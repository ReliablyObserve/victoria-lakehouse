package storageinvariants

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Every storage test in the repository that asserts these invariants trusts
// this checker to speak up. A checker that silently reports nothing would turn
// all of those assertions into no-ops, so each violation is constructed here and
// the checker is required to name it.

type bucket []string

func (b bucket) Keys() []string { return b }

const part = "dt=2026-01-01/hour=00"

func key(name string) string { return "logs/" + part + "/" + name + ".parquet" }

func healthy() (*manifest.Manifest, bucket) {
	m := manifest.New("b", "")
	m.AddFile(part, manifest.FileInfo{Key: key("a"), RowCount: 3, MinTimeNs: 1, MaxTimeNs: 2,
		LabelAggregates: map[string]map[string]int64{"service.name": {"web": 2, "api": 1}}})
	return m, bucket{key("a")}
}

func kinds(v []Violation) map[string]bool {
	out := map[string]bool{}
	for _, x := range v {
		out[x.Invariant] = true
	}
	return out
}

func TestCheck_HealthyStateHasNoViolations(t *testing.T) {
	m, b := healthy()
	if v := Check(State{Manifest: m, Bucket: b}); len(v) != 0 {
		t.Fatalf("a consistent state must pass, got %v", v)
	}
	// Non-parquet objects (tombstone records, bundles) are not the checker's.
	if v := Check(State{Manifest: m, Bucket: append(b, "logs/_tombstones/x.json")}); len(v) != 0 {
		t.Fatalf("non-parquet objects must be ignored, got %v", v)
	}
}

func TestCheck_ReportsEveryViolationKind(t *testing.T) {
	cases := []struct {
		name  string
		state func() State
		want  string
	}{
		{"manifested key missing from the bucket", func() State {
			m, _ := healthy()
			return State{Manifest: m, Bucket: bucket{}}
		}, "manifested_key_missing_from_bucket"},
		{"unmanifested parquet in the bucket", func() State {
			m, b := healthy()
			return State{Manifest: m, Bucket: append(b, key("orphan"))}
		}, "unmanifested_parquet_in_bucket"},
		{"negative row count", func() State {
			m, b := healthy()
			m.AddFile(part, manifest.FileInfo{Key: key("neg"), RowCount: -1})
			return State{Manifest: m, Bucket: append(b, key("neg"))}
		}, "negative_row_count"},
		{"inverted time bounds", func() State {
			m, b := healthy()
			m.AddFile(part, manifest.FileInfo{Key: key("inv"), RowCount: 1, MinTimeNs: 9, MaxTimeNs: 3})
			return State{Manifest: m, Bucket: append(b, key("inv"))}
		}, "inverted_time_bounds"},
		{"label aggregate exceeding the row count", func() State {
			m, b := healthy()
			m.AddFile(part, manifest.FileInfo{Key: key("agg"), RowCount: 1, MinTimeNs: 1, MaxTimeNs: 2,
				LabelAggregates: map[string]map[string]int64{"service.name": {"web": 5}}})
			return State{Manifest: m, Bucket: append(b, key("agg"))}
		}, "label_aggregate_exceeds_row_count"},
		{"eternally active tombstone", func() State {
			m, b := healthy()
			return State{Manifest: m, Bucket: b, Tombstones: []TombstoneView{{
				ID: "ts", Mode: "permanent", AffectedKeys: []string{"gone"}, Reaped: map[string]bool{"gone": true},
			}}}
		}, "eternally_active_tombstone"},
		{"reaped key still manifested", func() State {
			m, b := healthy()
			return State{Manifest: m, Bucket: b, Tombstones: []TombstoneView{{
				ID: "ts", Mode: "permanent", AffectedKeys: []string{key("a"), "other"}, Reaped: map[string]bool{key("a"): true},
			}}}
		}, "reaped_key_still_manifested"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := kinds(Check(tc.state()))
			if !got[tc.want] {
				t.Fatalf("the checker did not report %s, got %v", tc.want, got)
			}
		})
	}
}

func TestCheck_HideModeTombstonesAreNeverEternal(t *testing.T) {
	m, b := healthy()
	v := Check(State{Manifest: m, Bucket: b, Tombstones: []TombstoneView{{
		ID: "h", Mode: "hide", AffectedKeys: []string{key("a")}, Reaped: map[string]bool{key("a"): true},
	}}})
	if len(v) != 0 {
		t.Fatalf("a hide-mode tombstone has no reap lifecycle and must not be flagged, got %v", v)
	}
}

func TestCheck_KeyFilterScopesTheComparison(t *testing.T) {
	m, _ := healthy()
	onlyOthers := func(k string) bool { return !strings.Contains(k, part) }
	if v := Check(State{Manifest: m, Bucket: bucket{}, KeyFilter: onlyOthers}); len(v) != 0 {
		t.Fatalf("keys outside the filter must not be compared, got %v", v)
	}
}

func TestCheck_ViolationsAreSortedAndPrintable(t *testing.T) {
	m, _ := healthy()
	v := Check(State{Manifest: m, Bucket: bucket{key("z-orphan"), key("b-orphan")}})
	if len(v) < 3 {
		t.Fatalf("expected at least three violations, got %v", v)
	}
	for i := 1; i < len(v); i++ {
		prev, cur := v[i-1], v[i]
		if prev.Invariant > cur.Invariant || (prev.Invariant == cur.Invariant && prev.Detail > cur.Detail) {
			t.Fatalf("violations are not sorted: %v", v)
		}
	}
	if s := v[0].String(); !strings.Contains(s, v[0].Invariant) || !strings.Contains(s, v[0].Detail) {
		t.Fatalf("String() = %q, want invariant and detail", s)
	}
}

func TestTombstoneViewFullyReaped(t *testing.T) {
	if (TombstoneView{}).FullyReaped() {
		t.Error("a view with no keys is not fully reaped")
	}
	if (TombstoneView{AffectedKeys: []string{"a", "b"}, Reaped: map[string]bool{"a": true}}).FullyReaped() {
		t.Error("one of two keys reaped is not fully reaped")
	}
	if !(TombstoneView{AffectedKeys: []string{"a"}, Reaped: map[string]bool{"a": true}}).FullyReaped() {
		t.Error("every key reaped is fully reaped")
	}
}

func TestManifestRowsAndKeys(t *testing.T) {
	m, _ := healthy()
	m.AddFile(part, manifest.FileInfo{Key: key("b"), RowCount: 4})
	if got := ManifestRows(m); got != 7 {
		t.Errorf("ManifestRows = %d, want 7", got)
	}
	keys := ManifestKeys(m)
	if len(keys) != 2 || keys[0] != key("a") || keys[1] != key("b") {
		t.Errorf("ManifestKeys = %v, want sorted [a b]", keys)
	}
}

// TestAssert_PassesQuietlyOnAHealthyState exercises the helper's success path;
// its failure path calls t.Fatalf, which the violation-kind tests above cover
// through Check.
func TestAssert_PassesQuietlyOnAHealthyState(t *testing.T) {
	m, b := healthy()
	Assert(t, "healthy", State{Manifest: m, Bucket: b})
}

func TestCheck_ObjectsAwaitingDeletionAreAccountedFor(t *testing.T) {
	m, b := healthy()
	retired := key("retired")
	m.Retire(retired, "x", true)
	withRetired := append(b, retired)

	if v := kinds(Check(State{Manifest: m, Bucket: withRetired})); !v["unmanifested_parquet_in_bucket"] {
		t.Fatalf("without the awaiting-deletion view every unmanifested object is a violation, got %v", v)
	}
	if v := Check(State{Manifest: m, Bucket: withRetired, AwaitingDeletion: AwaitingDeletionIn(m)}); len(v) != 0 {
		t.Fatalf("an object the manifest retired is expected residue, got %v", v)
	}
	pending := key("uploading")
	m.MarkPending(pending)
	if v := Check(State{Manifest: m, Bucket: append(withRetired, pending), AwaitingDeletion: AwaitingDeletionIn(m)}); len(v) != 0 {
		t.Fatalf("an unpublished upload is expected residue, got %v", v)
	}
	// An object neither manifested nor accounted for is still reported.
	if v := kinds(Check(State{Manifest: m, Bucket: append(withRetired, key("stray")), AwaitingDeletion: AwaitingDeletionIn(m)})); !v["unmanifested_parquet_in_bucket"] {
		t.Fatalf("a stray object must still be reported, got %v", v)
	}
}

func TestCheck_ManifestedKeyAwaitingDeletionIsAViolation(t *testing.T) {
	m, b := healthy()
	v := kinds(Check(State{Manifest: m, Bucket: b, AwaitingDeletion: func(k string) bool { return k == key("a") }}))
	if !v["manifested_key_awaiting_deletion"] {
		t.Fatalf("a key both served and awaiting deletion must be reported, got %v", v)
	}
}

func TestTombstoneViewFullyReaped_CleanAndUnfinished(t *testing.T) {
	v := TombstoneView{AffectedKeys: []string{"src", "repl"}, Reaped: map[string]bool{"src": true}, Clean: map[string]bool{"repl": true}}
	if !v.FullyReaped() {
		t.Error("a reaped source and a clean replacement are fully handled")
	}
	v.Unfinished = 1
	if v.FullyReaped() {
		t.Error("an unfinished rewrite keeps the tombstone working")
	}
	// A clean key is live by definition: it is not "reaped but still manifested".
	m, b := healthy()
	got := Check(State{Manifest: m, Bucket: b, Tombstones: []TombstoneView{{
		ID: "ts", Mode: "permanent", AffectedKeys: []string{key("a"), "other"}, Clean: map[string]bool{key("a"): true},
	}}})
	if len(got) != 0 {
		t.Fatalf("a clean manifested key is healthy, got %v", got)
	}
}
