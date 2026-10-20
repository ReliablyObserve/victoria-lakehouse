package delete

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
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
			Tenants: []TenantRef{{}},
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
		bounded := Tombstone{Tenants: []TenantRef{{}}, ID: "fuzz", Query: query, StartNs: 10, EndNs: 20}
		if tsNs < 10 || tsNs > 20 {
			if bounded.MatchesRow(row, tsNs) {
				t.Fatalf("a row at %d matched a tombstone bounded to [10, 20]", tsNs)
			}
		}

		// An inverted window matches nothing.
		inverted := Tombstone{Tenants: []TenantRef{{}}, ID: "fuzz", Query: query, StartNs: 20, EndNs: 10}
		if inverted.MatchesRow(row, tsNs) {
			t.Fatalf("a tombstone with StartNs > EndNs matched a row at %d", tsNs)
		}
	})
}

// FuzzTombstoneKeyCodec drives the key codec with arbitrary ids and times. A key
// is how a pod finds a tombstone, so the codec must round-trip every id upstream
// accepts, produce one spelling per (kind, time, id), and keep the lexical order
// of keys equal to their time order (the poll lists "keys after T").
func FuzzTombstoneKeyCodec(f *testing.F) {
	f.Add("task-1", int64(1), int64(2), 0)
	f.Add("a.json", int64(1<<62), int64(1<<62+1), 1)
	f.Add("with/slash and space", int64(5), int64(4), 2)
	f.Add("%41", int64(1), int64(1<<63-1), 3)
	f.Add("\xff\xfe", int64(9), int64(10), 0)
	f.Add("-", int64(0x1000000000000000), int64(0x0fffffffffffffff), 1)

	prefix := TombstonePrefix("logs/")
	kinds := []string{kindDefinition, kindUndo}
	f.Fuzz(func(t *testing.T, id string, t1, t2 int64, kindIdx int) {
		if id == "" || t1 <= 0 || t2 <= 0 || kindIdx < 0 {
			return
		}
		k := tombKey{kind: kinds[kindIdx%len(kinds)], timeNs: t1, id: id}
		if k.kind == kindUndo {
			k.defNs = t2 // an undo names the definition key it undoes
		}
		key := formatKey(prefix, k)
		if len(escapeID(id)) > maxEscapedIDLen {
			return
		}
		back, ok := parseKey(prefix, key)
		if !ok {
			// Only an id that is not valid UTF-8 is refused by the parser.
			if strings.ToValidUTF8(id, "") == id {
				t.Fatalf("parseKey refused its own key %q (id %q)", key, id)
			}
			return
		}
		if back != k {
			t.Fatalf("round trip changed the key: %+v -> %q -> %+v", k, key, back)
		}
		if formatKey(prefix, back) != key {
			t.Fatalf("a key has two spellings: %q and %q", key, formatKey(prefix, back))
		}
		// Time order is key order for the same id and kind.
		other := formatKey(prefix, tombKey{kind: k.kind, timeNs: t2, id: id, defNs: k.defNs})
		if (t1 < t2) != (key < other) && t1 != t2 {
			t.Fatalf("key order disagrees with time order: %d vs %d -> %q vs %q", t1, t2, key, other)
		}
		// The listing resume point precedes every key at or after its time.
		if sa := startAfterKey(prefix, t1); sa >= key {
			t.Fatalf("startAfter %q is not before key %q", sa, key)
		}
	})
}

// FuzzParseKey drives the parser with arbitrary strings: it never panics, and a
// key it accepts is exactly the key formatKey would write.
func FuzzParseKey(f *testing.F) {
	prefix := TombstonePrefix("logs/")
	f.Add(prefix + "t0000000000000001-x.json")
	f.Add(prefix + "t0000000000000001-x.fate") // a key of an earlier design: rejected
	f.Add(prefix + "T0000000000000001-x.json")
	f.Add(prefix + "t0000000000000001-%2f.undo")
	f.Add(prefix + "t0000000000000001-%2F.done") // likewise
	f.Add(prefix + "t0000000000000000-x.json")
	f.Add(prefix + "tFFFFFFFFFFFFFFFF-x.json")
	f.Add(prefix + "_snap/abc.json")
	f.Add("garbage")
	f.Fuzz(func(t *testing.T, key string) {
		k, ok := parseKey(prefix, key)
		if !ok {
			return
		}
		if formatKey(prefix, k) != key {
			t.Fatalf("parseKey accepted %q but formatKey writes %q", key, formatKey(prefix, k))
		}
		if k.timeNs <= 0 || k.id == "" {
			t.Fatalf("parseKey accepted %q with time %d id %q", key, k.timeNs, k.id)
		}
	})
}

// FuzzDefinitionRoundTrip drives the definition encoding. Every pod reads the
// definition another pod wrote, so a definition that does not survive the round
// trip is a delete that silently stops being enforced, or two pods that disagree
// on its bytes.
func FuzzDefinitionRoundTrip(f *testing.F) {
	f.Add("id-1", `severity_text:="error"`, int64(0), int64(100), "permanent", "tenant", int64(7))
	f.Add("", "", int64(0), int64(0), "", "", int64(0))
	f.Add("id-2", "*", int64(-1), int64(1<<62), "hide", "", int64(1<<40))

	f.Fuzz(func(t *testing.T, id, query string, startNs, endNs int64, mode, by string, filterAt int64) {
		original := Tombstone{
			Tenants:   []TenantRef{{AccountID: 7, ProjectID: 3}, {AccountID: 1}},
			ID:        id,
			Query:     query,
			StartNs:   startNs,
			EndNs:     endNs,
			Mode:      mode,
			CreatedBy: by,
			FilterAt:  filterAt,
		}
		// A record the API would refuse cannot be expected to round-trip: it is
		// refused precisely because the encoding cannot carry it faithfully.
		if err := original.Validate(); err != nil {
			return
		}
		data, err := encodeDefinition(original)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		back, err := decodeDefinition(data)
		if err != nil {
			t.Fatalf("decode %s: %v", data, err)
		}
		again, err := encodeDefinition(back)
		if err != nil {
			t.Fatalf("encode again: %v", err)
		}
		if !bytes.Equal(data, again) {
			t.Fatalf("the encoding is not canonical:\n%s\n%s", data, again)
		}
		if back.ID != original.ID || back.Query != original.Query ||
			back.StartNs != original.StartNs || back.EndNs != original.EndNs ||
			back.Mode != original.Mode || back.CreatedBy != original.CreatedBy ||
			back.FilterAt != original.FilterAt || len(back.Tenants) != 2 {
			t.Fatalf("round trip changed the record:\n got %+v\nwant %+v", back, original)
		}
	})
}

// FuzzNamedTenants drives the lenient parse that scopes an unreadable
// definition's gate closure: it never panics, what it returns is normalised and
// non-empty, and for a body that decodes it names exactly the definition's
// tenants (a readable delete and its lenient parse never disagree).
func FuzzNamedTenants(f *testing.F) {
	f.Add([]byte(`{"v":1,"id":"a","tenants":[{"a":1,"p":0}],"filter":"*","filter_at":0,"start_ns":0,"end_ns":5,"mode":"hide"}`))
	f.Add([]byte(`{"v":99,"tenants":[{"a":7,"p":1},{"a":3,"p":0}]}`))
	f.Add([]byte(`{"tenants":[{"a":-1}]}`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, ok := namedTenants(data)
		if ok && len(got) == 0 {
			t.Fatal("namedTenants reported success with no tenant")
		}
		if again := NormalizeTenants(got); len(again) != len(got) {
			t.Fatalf("namedTenants returned a list that is not normalised: %v", got)
		}
		ts, err := decodeDefinition(data)
		if err != nil {
			return
		}
		if !ok || len(got) != len(ts.Tenants) {
			t.Fatalf("a readable definition names %v but namedTenants says %v (ok=%v)", ts.Tenants, got, ok)
		}
		for i := range got {
			if got[i] != ts.Tenants[i] {
				t.Fatalf("tenant %d differs: %v vs %v", i, got[i], ts.Tenants[i])
			}
		}
	})
}

// FuzzTombstoneSync drives the view's state machine with a random sequence of
// creates, un-deletes, polls, full listings, short and failing listings and clock
// moves over two pods and one bucket. Whatever the order:
//
//   - no pod ever issues a DELETE (only the retention GC may, and it is off);
//   - a pod's view never holds a tombstone nobody created;
//   - once a healthy full listing has run, the view is exactly the model: the
//     created tombstones minus the hide-mode ones that were undone (a permanent
//     or auto delete is never undone).
func FuzzTombstoneSync(f *testing.F) {
	f.Add([]byte{0, 3, 1, 3, 4, 3, 6, 2})
	f.Add([]byte{0x10, 0x11, 0x01, 5, 3, 7, 2, 3})
	f.Add([]byte{0, 0x20, 0x21, 0x22, 4, 2, 5, 3, 1, 3})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 64 {
			ops = ops[:64]
		}
		b := deletetest.NewBucket()
		writer, _ := newSyncedPod(t, b, "writer")
		reader, rpod := newSyncedPod(t, b, "reader")
		created := map[string]string{} // id -> mode
		undone := map[string]bool{}
		var ids []string
		modes := []string{"hide", "permanent", "auto"}
		for _, op := range ops {
			switch op & 7 {
			case 0: // create; the mode and the id come from the high bits
				id := fmt.Sprintf("d%d", len(ids))
				mode := modes[int(op>>3)%len(modes)]
				ts := hideTombstone(id)
				ts.Mode = mode
				if _, err := writer.Create(context.Background(), ts); err != nil {
					t.Fatalf("create %s: %v", id, err)
				}
				created[id] = mode
				ids = append(ids, id)
			case 1: // undelete one of the created
				if len(ids) == 0 {
					continue
				}
				id := ids[int(op>>3)%len(ids)]
				err := writer.Undelete(context.Background(), id)
				switch {
				case created[id] == "hide" && err != nil:
					t.Fatalf("undo of a hide delete %s: %v", id, err)
				case created[id] != "hide" && !errors.Is(err, ErrNotReversible):
					t.Fatalf("undo of a %s delete %s = %v, want ErrNotReversible", created[id], id, err)
				case created[id] == "hide":
					undone[id] = true
				}
			case 2:
				_ = reader.SyncNow(context.Background(), false)
			case 3:
				_ = reader.SyncNow(context.Background(), true)
			case 4:
				rpod.ShortListOnce(1 + int(op>>3)%3)
			case 5:
				rpod.FailNext("list", 1)
			case 6:
				b.Advance(time.Minute)
			case 7:
				b.Advance(20 * time.Minute)
			}
			for _, e := range reader.Active() {
				if _, ok := created[e.ID]; !ok {
					t.Fatalf("the view holds %s, which nobody created", e.ID)
				}
			}
		}
		// A healthy full listing settles the view.
		rpod.ShortListOnce(0)
		rpod.FailNext("list", 0)
		if err := reader.SyncNow(context.Background(), true); err != nil {
			t.Fatalf("final sync: %v", err)
		}
		for _, id := range ids {
			want := !undone[id]
			if got := has(reader, id); got != want {
				t.Fatalf("after a full listing %s in force = %v, want %v (mode %s)", id, got, want, created[id])
			}
		}
		for _, op := range b.Log() {
			if op.Kind == "DELETE" {
				t.Fatalf("%s deleted %s", op.Pod, op.Key)
			}
		}
	})
}

// FuzzTombstonePrefix drives the S3 key prefix construction. The key layout is
// part of the durable contract — every pod, the orphan sweep's protected-substring
// match and anyone reading the documented "{prefix}_tombstones/" layout must
// derive the same string — so every caller spelling (with or without trailing
// slashes) has to normalise to it.
func FuzzTombstonePrefix(f *testing.F) {
	f.Add("")
	f.Add("/")
	f.Add("logs/")
	f.Add("1002/0/logs/")
	f.Add("1002/0/logs")
	f.Add("//")
	f.Add(strings.Repeat("/", 100))

	f.Fuzz(func(t *testing.T, prefix string) {
		got := TombstonePrefix(prefix)
		if !strings.HasSuffix(got, tombstonePrefixSegment) {
			t.Fatalf("TombstonePrefix(%q) = %q does not end in %q; the orphan sweep's protected-substring check would miss it",
				prefix, got, tombstonePrefixSegment)
		}
		if strings.Contains(got, "//") && !strings.Contains(strings.TrimSuffix(prefix, "/"), "//") {
			t.Fatalf("TombstonePrefix(%q) = %q introduced a doubled slash", prefix, got)
		}
		if withSlash := TombstonePrefix(strings.TrimSuffix(prefix, "/") + "/"); withSlash != got {
			t.Fatalf("TombstonePrefix disagrees on the trailing slash: %q vs %q", got, withSlash)
		}
	})
}
