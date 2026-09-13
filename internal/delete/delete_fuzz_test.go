package delete

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// FuzzTombstoneMatchesRow drives the tombstone predicate with arbitrary LogsQL
// and arbitrary row content.
//
// The predicate decides which rows a delete removes, and it is reached from
// four places (query filter, field enumeration, rewriter, compactor) with data
// that ultimately comes from user input: the query text is whatever the operator
// typed, and the field values are whatever was ingested. A panic here is a
// crash loop on every query touching the window; a disagreement between the two
// entry points means a row hidden by one path and copied forward by another.
func FuzzTombstoneMatchesRow(f *testing.F) {
	f.Add(`severity_text:="error"`, "severity_text", "error", int64(50))
	f.Add(`*`, "body", "anything", int64(0))
	f.Add(``, "body", "", int64(-1))
	f.Add(`service.name:="web" AND severity_text:="error"`, "service.name", "web", int64(100))
	f.Add(`broken:==`, "body", "x", int64(10))
	f.Add(`_time:[2026-01-01, 2026-01-02]`, "_time", "2026-01-01", int64(1))
	f.Add(strings.Repeat("a", 1024), "body", strings.Repeat("b", 1024), int64(1<<62))

	f.Fuzz(func(t *testing.T, query, fieldName, fieldValue string, tsNs int64) {
		ts := Tombstone{
			ID:      "fuzz",
			Query:   query,
			StartNs: 0,
			EndNs:   1 << 62,
		}

		row := map[string]string{fieldName: fieldValue}
		viaMap := ts.MatchesRow(row, tsNs)
		viaFields := ts.MatchesFields(fieldsFromMap(row), tsNs)

		// The two entry points must agree exactly. They exist only so callers
		// that already hold one shape do not have to convert.
		if viaMap != viaFields {
			t.Fatalf("MatchesRow=%v but MatchesFields=%v for query=%q field=%q=%q ts=%d",
				viaMap, viaFields, query, fieldName, fieldValue, tsNs)
		}

		// A row outside the tombstone's window can never match, whatever the
		// query says. Time bounding is what keeps a delete from spilling
		// outside the range the operator asked for.
		bounded := Tombstone{ID: "fuzz", Query: query, StartNs: 10, EndNs: 20}
		if tsNs < 10 || tsNs > 20 {
			if bounded.MatchesRow(row, tsNs) {
				t.Fatalf("a row at %d matched a tombstone bounded to [10, 20]", tsNs)
			}
		}

		// An inverted window matches nothing.
		inverted := Tombstone{ID: "fuzz", Query: query, StartNs: 20, EndNs: 10}
		if inverted.MatchesRow(row, tsNs) {
			t.Fatalf("a tombstone with StartNs > EndNs matched a row at %d", tsNs)
		}
	})
}

// FuzzRewritePartitionFromKey drives the key → partition derivation with arbitrary
// keys. It is the fallback the rewriter uses when the manifest does not know a
// key, so a wrong answer files a replacement under a partition no query looks
// in — the object exists, is manifested, and is invisible.
func FuzzRewritePartitionFromKey(f *testing.F) {
	f.Add("logs/dt=2026-01-01/hour=10/a.parquet")
	f.Add("1002/0/logs/dt=2026-01-01/hour=10/a.parquet")
	f.Add("")
	f.Add("/")
	f.Add("=")
	f.Add("////")
	f.Add(strings.Repeat("dt=x/", 500))

	f.Fuzz(func(t *testing.T, key string) {
		got := extractPartition(key)

		if got == "" {
			t.Fatalf("extractPartition(%q) returned an empty partition; an entry filed under it is unreachable", key)
		}
		// Every segment of the result must have come from the key, and must be
		// a partition-shaped segment.
		if got != "unknown" {
			for _, seg := range strings.Split(got, "/") {
				if !strings.Contains(seg, "=") {
					t.Fatalf("extractPartition(%q) = %q contains a non-partition segment %q", key, got, seg)
				}
				if !strings.Contains(key, seg) {
					t.Fatalf("extractPartition(%q) = %q invented the segment %q", key, got, seg)
				}
			}
		}
		// Deterministic: the rewriter and the orphan sweep must derive the same
		// partition from the same key.
		if again := extractPartition(key); again != got {
			t.Fatalf("extractPartition(%q) is not deterministic: %q then %q", key, got, again)
		}
	})
}

// FuzzTombstoneRoundTrip drives the durable encoding. Tombstones are restored
// from JSON written by another process (or another node), so a record that does
// not survive the round trip is a delete that silently stops being enforced.
func FuzzTombstoneRoundTrip(f *testing.F) {
	f.Add("id-1", `severity_text:="error"`, int64(0), int64(100), "permanent", "k1", true)
	f.Add("", "", int64(0), int64(0), "", "", false)
	f.Add("id-2", "*", int64(-1), int64(1<<62), "hide", "logs/dt=2026-01-01/hour=00/a.parquet", true)

	f.Fuzz(func(t *testing.T, id, query string, startNs, endNs int64, mode, key string, reaped bool) {
		original := Tombstone{
			ID:           id,
			Query:        query,
			StartNs:      startNs,
			EndNs:        endNs,
			AffectedKeys: []string{key},
			CreatedAt:    time.Unix(0, 0).UTC(),
			Mode:         mode,
			Reaped:       map[string]bool{key: reaped},
		}

		// A record the API would refuse cannot be expected to round-trip: it is
		// refused precisely because the durable encoding cannot carry it
		// faithfully (invalid UTF-8) or because it can never match a row.
		candidate := original
		if err := candidate.Validate(); err != nil {
			return
		}

		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var back Tombstone
		if err := json.Unmarshal(data, &back); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}

		if back.ID != original.ID || back.Query != original.Query ||
			back.StartNs != original.StartNs || back.EndNs != original.EndNs ||
			back.Mode != original.Mode {
			t.Fatalf("round trip changed the record:\n got %+v\nwant %+v", back, original)
		}
		if len(back.AffectedKeys) != 1 || back.AffectedKeys[0] != key {
			t.Fatalf("round trip lost the affected keys: %v", back.AffectedKeys)
		}
		// Rewrite progress is the field that must never be walked back: losing
		// a reaped mark makes the scheduler retry a key whose object is gone.
		if back.Reaped[key] != reaped {
			t.Fatalf("round trip changed the reaped mark for %q: %v -> %v", key, reaped, back.Reaped[key])
		}
		if back.FullyReaped() != original.FullyReaped() {
			t.Fatalf("round trip changed FullyReaped: %v -> %v", original.FullyReaped(), back.FullyReaped())
		}
	})
}

// FuzzNormalizeTombstonePrefix drives the S3 key prefix construction. The key
// layout is part of the durable contract — the writer, the startup restore, the
// orphan sweep's protected-substring match and anyone reading the documented
// "{prefix}_tombstones/" layout must all derive the same string — so every
// caller spelling (with or without trailing slashes) has to normalise to it.
func FuzzNormalizeTombstonePrefix(f *testing.F) {
	f.Add("")
	f.Add("/")
	f.Add("logs/")
	f.Add("1002/0/logs/")
	f.Add("1002/0/logs")
	f.Add("//")
	f.Add(strings.Repeat("/", 100))

	f.Fuzz(func(t *testing.T, prefix string) {
		got := normalizeTombstonePrefix(prefix)

		if !strings.HasSuffix(got, tombstonePrefixSegment) {
			t.Fatalf("normalizeTombstonePrefix(%q) = %q does not end in %q; the orphan sweep's protected-substring check would miss it",
				prefix, got, tombstonePrefixSegment)
		}
		// The segment must appear exactly once, and never doubled up against a
		// stray slash.
		if strings.Contains(got, "//") && !strings.Contains(strings.TrimSuffix(prefix, "/"), "//") {
			t.Fatalf("normalizeTombstonePrefix(%q) = %q introduced a doubled slash", prefix, got)
		}
		// Idempotent under the trailing-slash variants a caller might pass.
		if withSlash := normalizeTombstonePrefix(strings.TrimSuffix(prefix, "/") + "/"); withSlash != got {
			t.Fatalf("normalizeTombstonePrefix disagrees on the trailing slash: %q vs %q", got, withSlash)
		}
		// The exported spelling the docs and the sweep use must match.
		if TombstonePrefix(prefix) != got {
			t.Fatalf("TombstonePrefix(%q) = %q but normalizeTombstonePrefix = %q", prefix, TombstonePrefix(prefix), got)
		}
	})
}
