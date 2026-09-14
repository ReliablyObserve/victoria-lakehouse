package parquets3

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestDedupOverlappingFiles_PrefersCompactedOutput verifies that when a
// compacted file fully contains the time range of its source files (and
// has a higher CompactionLevel), the sources are filtered out. This
// prevents GetFieldValues from double-counting values during the brief
// window where both pre-compaction sources and the merged output appear
// in the manifest.
func TestDedupOverlappingFiles_PrefersCompactedOutput(t *testing.T) {
	files := []manifest.FileInfo{
		// Two source files at L0
		{Key: "src-a.parquet", Size: 100, RowCount: 10, MinTimeNs: 1000, MaxTimeNs: 4000, CompactionLevel: 0},
		{Key: "src-b.parquet", Size: 100, RowCount: 10, MinTimeNs: 5000, MaxTimeNs: 9000, CompactionLevel: 0},
		// Compacted L1 file that covers both sources.
		{Key: "merged.parquet", Size: 200, RowCount: 20, MinTimeNs: 1000, MaxTimeNs: 9000, CompactionLevel: 1},
	}

	got := dedupOverlappingFiles(files)
	if len(got) != 1 {
		t.Fatalf("got %d files, want 1; files=%v", len(got), got)
	}
	if got[0].Key != "merged.parquet" {
		t.Errorf("kept %q, want merged.parquet", got[0].Key)
	}
}

// TestDedupOverlappingFiles_NoOverlap leaves disjoint files untouched.
func TestDedupOverlappingFiles_NoOverlap(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a.parquet", Size: 100, MinTimeNs: 1000, MaxTimeNs: 2000},
		{Key: "b.parquet", Size: 100, MinTimeNs: 3000, MaxTimeNs: 4000},
		{Key: "c.parquet", Size: 100, MinTimeNs: 5000, MaxTimeNs: 6000},
	}
	got := dedupOverlappingFiles(files)
	if len(got) != 3 {
		t.Fatalf("got %d files, want 3", len(got))
	}
}

// TestDedupOverlappingFiles_PartialOverlapKept retains files whose
// overlap is smaller than the 90% threshold, since they likely carry
// data the larger file doesn't.
func TestDedupOverlappingFiles_PartialOverlapKept(t *testing.T) {
	files := []manifest.FileInfo{
		// Big file [1000, 10000]
		{Key: "big.parquet", Size: 1000, MinTimeNs: 1000, MaxTimeNs: 10000, CompactionLevel: 0},
		// Slightly-overlapping file: most of its range is OUTSIDE big.
		{Key: "edge.parquet", Size: 200, MinTimeNs: 9500, MaxTimeNs: 20000, CompactionLevel: 0},
	}
	got := dedupOverlappingFiles(files)
	if len(got) != 2 {
		t.Fatalf("expected both files preserved (partial overlap), got %d", len(got))
	}
}

// TestDedupOverlappingFiles_HigherLevelWins picks the higher
// CompactionLevel as the canonical file when two overlap heavily and
// both lack compaction-parent info.
func TestDedupOverlappingFiles_HigherLevelWins(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "l0.parquet", Size: 100, MinTimeNs: 1000, MaxTimeNs: 5000, CompactionLevel: 0},
		{Key: "l2.parquet", Size: 500, MinTimeNs: 1000, MaxTimeNs: 5000, CompactionLevel: 2},
	}
	got := dedupOverlappingFiles(files)
	if len(got) != 1 {
		t.Fatalf("got %d files, want 1", len(got))
	}
	if got[0].Key != "l2.parquet" {
		t.Errorf("kept %q, want l2.parquet (higher level)", got[0].Key)
	}
}

// TestDedupOverlappingFiles_NeverAcrossTenants: a larger file of one tenant
// covering the same seconds as a smaller file of another tenant is not its
// compacted output — both must survive, or a cross-tenant read (global read,
// tenant list) silently loses the smaller tenant's values.
func TestDedupOverlappingFiles_NeverAcrossTenants(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "0/0/logs/dt=2026-05-10/hour=14/big.parquet", Size: 5000, MinTimeNs: 1000, MaxTimeNs: 9000},
		{Key: "1001/0/logs/dt=2026-05-10/hour=14/small.parquet", Size: 100, MinTimeNs: 2000, MaxTimeNs: 3000},
		{Key: "3003/0/logs/dt=2026-05-10/hour=14/small.parquet", Size: 100, MinTimeNs: 2000, MaxTimeNs: 3000, CompactionLevel: 0},
	}
	if got := dedupOverlappingFiles(append([]manifest.FileInfo(nil), files...)); len(got) != 3 {
		t.Errorf("dedup across tenants kept %d of 3 files: %+v", len(got), got)
	}

	// Within one tenant partition the compacted output still subsumes its source.
	same := []manifest.FileInfo{
		{Key: "1001/0/logs/dt=2026-05-10/hour=14/src.parquet", Size: 100, MinTimeNs: 2000, MaxTimeNs: 3000},
		{Key: "1001/0/logs/dt=2026-05-10/hour=14/merged.parquet", Size: 900, MinTimeNs: 1000, MaxTimeNs: 9000, CompactionLevel: 1},
	}
	got := dedupOverlappingFiles(same)
	if len(got) != 1 || !strings.HasSuffix(got[0].Key, "merged.parquet") {
		t.Errorf("same-partition dedup kept %+v, want only the compacted output", got)
	}

	// Same tenant, different hour directories: not a compaction pair either.
	hours := []manifest.FileInfo{
		{Key: "1001/0/logs/dt=2026-05-10/hour=14/a.parquet", Size: 900, MinTimeNs: 1000, MaxTimeNs: 9000, CompactionLevel: 1},
		{Key: "1001/0/logs/dt=2026-05-10/hour=15/b.parquet", Size: 100, MinTimeNs: 2000, MaxTimeNs: 3000},
	}
	if got := dedupOverlappingFiles(hours); len(got) != 2 {
		t.Errorf("dedup across partitions kept %d of 2 files: %+v", len(got), got)
	}
}
