// Package storageinvariants holds the properties that must hold of the cold
// tier at rest, expressed once so every test that moves objects around can
// assert the same set.
//
// The delete rewriter, the compactor, the orphan sweep and the retention loop
// all mutate the same two structures — the manifest and the bucket — and the
// bugs that hurt most are the ones where the two stop agreeing: a manifest
// entry whose object is gone (queries lose rows or, worse, synthesise them from
// stale metadata), or an object nothing manifests (the orphan sweep deletes it
// after its age gate, taking real rows with it).
//
// Checking those properties inline in each test drifted; each test asserted the
// two or three it happened to think of. This package is the single list, so a
// later change that breaks an invariant fails in every test that runs a
// storage-mutating path rather than only the one that remembered to look.
package storageinvariants

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Bucket is the object-store side of the comparison: the set of keys that
// actually exist. Implemented by the in-memory pools the tests already use.
type Bucket interface {
	Keys() []string
}

// State is one snapshot of the cold tier plus the tombstone state describing
// what is supposed to be hidden or gone.
type State struct {
	Manifest *manifest.Manifest
	Bucket   Bucket
	// Tombstones may be empty for checks that do not involve deletes. It is a
	// plain view rather than the store itself so this package stays free of a
	// dependency on internal/delete — which would make it unusable from that
	// package's own tests (import cycle), and those are exactly the tests that
	// need it most.
	Tombstones []TombstoneView
	// KeyFilter restricts the comparison to keys the test owns, so a fixture
	// holding unrelated objects (snapshots, sidecars) does not fail the
	// parquet-set checks. Nil means "every key ending in .parquet".
	KeyFilter func(key string) bool
	// AwaitingDeletion, when set, reports the objects the manifest deliberately
	// does not list while their deletes are outstanding — its retired and
	// pending keys (see AwaitingDeletionIn). Those are exempt from I2 (they are
	// the expected residue of a failed delete or an interrupted rewrite), and
	// I2b checks the converse. Nil means nothing may be outside the manifest.
	AwaitingDeletion func(key string) bool
}

// AwaitingDeletionIn is the AwaitingDeletion view of a manifest's retired and
// pending keys.
func AwaitingDeletionIn(m *manifest.Manifest) func(string) bool {
	return func(key string) bool { return m.IsRetired(key) || m.IsPending(key) }
}

// TombstoneView is the part of a tombstone the invariants care about.
type TombstoneView struct {
	ID           string
	Mode         string
	AffectedKeys []string
	// Reaped keys' objects are gone; Clean keys are live files free of the
	// tombstone's rows.
	Reaped map[string]bool
	Clean  map[string]bool
	// Unfinished is the number of rewrites whose objects are not settled yet;
	// a tombstone with any is still working.
	Unfinished int
}

// FullyReaped reports whether every key this tombstone covers is handled and
// no rewrite of its files is unfinished.
func (t TombstoneView) FullyReaped() bool {
	if len(t.AffectedKeys) == 0 || t.Unfinished > 0 {
		return false
	}
	for _, k := range t.AffectedKeys {
		if !t.Reaped[k] && !t.Clean[k] {
			return false
		}
	}
	return true
}

// Violation is one broken invariant.
type Violation struct {
	Invariant string
	Detail    string
}

func (v Violation) String() string { return v.Invariant + ": " + v.Detail }

// Check returns every violation found in the snapshot. The full list is
// returned rather than the first failure so a test reports everything that is
// wrong in one run.
func Check(st State) []Violation {
	var out []Violation

	manifestKeys := map[string]bool{}
	for _, files := range st.Manifest.AllFiles() {
		for _, fi := range files {
			manifestKeys[fi.Key] = true
		}
	}

	bucketKeys := map[string]bool{}
	if st.Bucket != nil {
		for _, k := range st.Bucket.Keys() {
			if !st.owns(k) {
				continue
			}
			bucketKeys[k] = true
		}
	}

	// I1 — every manifest entry has an object behind it. A violation means
	// queries hit a 404: with an active tombstone the file is skipped (kept
	// rows vanish), without one the recovery path synthesises RowCount blocks
	// from the stale entry (deleted rows reappear).
	for k := range manifestKeys {
		if !st.owns(k) {
			continue
		}
		if st.Bucket != nil && !bucketKeys[k] {
			out = append(out, Violation{"manifested_key_missing_from_bucket", k})
		}
	}

	// I2 — every object is manifested. A violation means the orphan sweep will
	// delete it once it passes the age gate, so any rows only it holds are on
	// a timer.
	for k := range bucketKeys {
		if !manifestKeys[k] && (st.AwaitingDeletion == nil || !st.AwaitingDeletion(k)) {
			out = append(out, Violation{"unmanifested_parquet_in_bucket", k})
		}
	}

	// I2b — nothing is both served and awaiting deletion. A key the manifest
	// lists AND remembers as retired or pending would be deleted (or skipped by
	// the refresh) while queries still read it.
	if st.AwaitingDeletion != nil {
		for k := range manifestKeys {
			if st.owns(k) && st.AwaitingDeletion(k) {
				out = append(out, Violation{"manifested_key_awaiting_deletion", k})
			}
		}
	}

	// I3 — no manifest entry claims rows it cannot have.
	for partition, files := range st.Manifest.AllFiles() {
		for _, fi := range files {
			if fi.RowCount < 0 {
				out = append(out, Violation{"negative_row_count", fmt.Sprintf("%s rows=%d", fi.Key, fi.RowCount)})
			}
			if fi.RowCount > 0 && fi.MinTimeNs > fi.MaxTimeNs {
				out = append(out, Violation{"inverted_time_bounds",
					fmt.Sprintf("%s min=%d max=%d", fi.Key, fi.MinTimeNs, fi.MaxTimeNs)})
			}
			// I3b — LabelAggregates are answered from metadata without opening
			// the file, so they may never exceed the file's own row count.
			for field, agg := range fi.LabelAggregates {
				var sum int64
				for _, c := range agg {
					sum += c
				}
				if sum > fi.RowCount {
					out = append(out, Violation{"label_aggregate_exceeds_row_count",
						fmt.Sprintf("%s field=%s sum=%d rows=%d (partition %s)", fi.Key, field, sum, fi.RowCount, partition)})
				}
			}
		}
	}

	// I4 — no tombstone is eternally active. A tombstone whose every affected
	// key is reaped has no work left; leaving it in Active() makes the
	// scheduler re-examine it forever and keeps the manifest-metadata query
	// fast paths disabled for the life of the process.
	for _, ts := range st.Tombstones {
		if ts.Mode == "hide" {
			continue
		}
		if ts.FullyReaped() {
			out = append(out, Violation{"eternally_active_tombstone",
				fmt.Sprintf("tombstone %s is fully reaped but still active", ts.ID)})
		}
		// I4b — a key recorded as reaped must be gone from the manifest (a
		// clean key is live by definition and exempt).
		for key, reaped := range ts.Reaped {
			if reaped && manifestKeys[key] {
				out = append(out, Violation{"reaped_key_still_manifested",
					fmt.Sprintf("tombstone %s reaped %s but the manifest still lists it", ts.ID, key)})
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Invariant != out[j].Invariant {
			return out[i].Invariant < out[j].Invariant
		}
		return out[i].Detail < out[j].Detail
	})
	return out
}

func (st State) owns(key string) bool {
	if st.KeyFilter != nil {
		return st.KeyFilter(key)
	}
	return strings.HasSuffix(key, ".parquet")
}

// Assert fails the test with every violation found. `stage` names the moment
// being checked so a failure says which step of a sequence broke the invariant.
func Assert(t *testing.T, stage string, st State) {
	t.Helper()
	v := Check(st)
	if len(v) == 0 {
		return
	}
	msgs := make([]string, len(v))
	for i := range v {
		msgs[i] = v[i].String()
	}
	t.Fatalf("storage invariants violated at %q:\n  %s", stage, strings.Join(msgs, "\n  "))
}

// ManifestRows sums RowCount across every manifest entry — the metadata-side
// answer to "how many rows does the cold tier hold", which must equal what a
// full scan returns.
func ManifestRows(m *manifest.Manifest) int64 {
	var total int64
	for _, files := range m.AllFiles() {
		for _, fi := range files {
			total += fi.RowCount
		}
	}
	return total
}

// ManifestKeys returns every key the manifest knows, sorted.
func ManifestKeys(m *manifest.Manifest) []string {
	var keys []string
	for _, files := range m.AllFiles() {
		for _, fi := range files {
			keys = append(keys, fi.Key)
		}
	}
	sort.Strings(keys)
	return keys
}
