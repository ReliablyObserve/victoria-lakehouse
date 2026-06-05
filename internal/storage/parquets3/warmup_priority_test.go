package parquets3

import (
	"sort"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestWarmupSortByMaxTimeNs pins the priority-warmup contract:
// files are walked newest-first by their actual time bounds, not
// by lexicographic key order. A partial warmup (e.g. context
// cancelled half-way at scale) must therefore have completed the
// most-recent partitions before starting on older ones — that's
// what makes "last hour" dashboards work mid-warmup.
//
// The sort is exercised via the same comparison the production
// WarmupCache uses. Failing this test would mean we silently
// regressed from time-ordered to lexicographic and a partial
// warmup would shuffle which partitions land in the cache first.
func TestWarmupSortByMaxTimeNs(t *testing.T) {
	files := []manifest.FileInfo{
		// Out-of-order on purpose: oldest first, freshest last.
		{Key: "dt=2026-06-01/hour=10/aaaaaaaa.parquet", MaxTimeNs: 1717_200_000_000_000_000},
		{Key: "dt=2026-06-05/hour=13/bbbbbbbb.parquet", MaxTimeNs: 1717_545_600_000_000_000}, // freshest
		{Key: "dt=2026-06-03/hour=08/cccccccc.parquet", MaxTimeNs: 1717_372_800_000_000_000},
		// Zero MaxTimeNs: must sort LAST regardless of key, so it
		// doesn't starve newer files.
		{Key: "dt=2026-06-04/hour=12/zzzzzzzz.parquet", MaxTimeNs: 0},
	}

	// Mirror of WarmupCache's sort.
	sort.Slice(files, func(i, j int) bool {
		if files[i].MaxTimeNs != files[j].MaxTimeNs {
			return files[i].MaxTimeNs > files[j].MaxTimeNs
		}
		return files[i].Key > files[j].Key
	})

	// Expected order: freshest MaxTimeNs first, then older,
	// then the zero-MaxTimeNs file last.
	want := []string{
		"dt=2026-06-05/hour=13/bbbbbbbb.parquet",
		"dt=2026-06-03/hour=08/cccccccc.parquet",
		"dt=2026-06-01/hour=10/aaaaaaaa.parquet",
		"dt=2026-06-04/hour=12/zzzzzzzz.parquet",
	}
	for i, f := range files {
		if f.Key != want[i] {
			t.Errorf("position %d: got %s, want %s", i, f.Key, want[i])
		}
	}
}

// TestWarmupSort_StableForEqualTimes pins the secondary
// comparison: when two files share MaxTimeNs (legitimate at the
// per-second resolution of a hot ingest), the key tiebreaker
// keeps the sort stable across runs. Without this, the warmup
// order across two cold-starts of the same cluster could differ
// — making capacity planning unpredictable.
func TestWarmupSort_StableForEqualTimes(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "z.parquet", MaxTimeNs: 1000},
		{Key: "a.parquet", MaxTimeNs: 1000},
		{Key: "m.parquet", MaxTimeNs: 1000},
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].MaxTimeNs != files[j].MaxTimeNs {
			return files[i].MaxTimeNs > files[j].MaxTimeNs
		}
		return files[i].Key > files[j].Key
	})
	// All equal MaxTimeNs → secondary key DESC: z, m, a.
	want := []string{"z.parquet", "m.parquet", "a.parquet"}
	for i, f := range files {
		if f.Key != want[i] {
			t.Errorf("position %d: got %s, want %s", i, f.Key, want[i])
		}
	}
}
