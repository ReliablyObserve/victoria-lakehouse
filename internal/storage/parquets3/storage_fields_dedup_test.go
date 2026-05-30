package parquets3

import (
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
