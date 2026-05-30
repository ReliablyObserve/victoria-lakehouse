package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestManifestFastPathSkipsUnpopulatedTime documents that MinTimeNs==0 is a
// sentinel meaning "time bounds not yet populated from Parquet footer". The
// fast path correctly skips such files, falling through to full S3 reads.
func TestManifestFastPathSkipsUnpopulatedTime(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "no-time.parquet", Size: 100, RowCount: 10, MinTimeNs: 0, MaxTimeNs: 1000},
		{Key: "normal.parquet", Size: 100, RowCount: 5, MinTimeNs: 500, MaxTimeNs: 1500},
	}

	var resolvedBlocks int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedBlocks++
	}

	remaining := s.manifestFastPath(files, 0, 2000, writeBlock)

	if resolvedBlocks != 1 {
		t.Errorf("resolvedBlocks = %d, want 1 (only normal file resolved)", resolvedBlocks)
	}
	if len(remaining) != 1 {
		t.Errorf("remaining = %d, want 1 (unpopulated-time file falls through)", len(remaining))
	}
	if len(remaining) > 0 && remaining[0].Key != "no-time.parquet" {
		t.Errorf("remaining file = %q, want no-time.parquet", remaining[0].Key)
	}
}

// TestManifestFastPathSkipsBothBoundsZero verifies files with both time
// bounds == 0 (fully unpopulated metadata) are correctly skipped.
func TestManifestFastPathSkipsBothBoundsZero(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "no-metadata.parquet", Size: 50, RowCount: 1, MinTimeNs: 0, MaxTimeNs: 0},
	}

	var resolvedBlocks int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedBlocks++
	}

	remaining := s.manifestFastPath(files, 0, 1000, writeBlock)

	if resolvedBlocks != 0 {
		t.Errorf("resolvedBlocks = %d, want 0 (no metadata = skip fast path)", resolvedBlocks)
	}
	if len(remaining) != 1 {
		t.Errorf("remaining = %d, want 1", len(remaining))
	}
}

// TestManifestFastPathNormalFile verifies a file with populated time bounds
// fully within the query range is resolved by the fast path.
func TestManifestFastPathNormalFile(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "good.parquet", Size: 100, RowCount: 42, MinTimeNs: 100, MaxTimeNs: 900},
	}

	var resolvedRows int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedRows += db.RowsCount()
	}

	remaining := s.manifestFastPath(files, 0, 1000, writeBlock)

	if resolvedRows != 42 {
		t.Errorf("resolvedRows = %d, want 42", resolvedRows)
	}
	if len(remaining) != 0 {
		t.Errorf("remaining = %d, want 0", len(remaining))
	}
}

// TestManifestFastPathPartialOverlap verifies a file NOT fully within the
// query range stays in remaining for full scan.
func TestManifestFastPathPartialOverlap(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "partial.parquet", Size: 100, RowCount: 10, MinTimeNs: 100, MaxTimeNs: 2000},
	}

	var resolvedBlocks int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedBlocks++
	}

	remaining := s.manifestFastPath(files, 500, 1500, writeBlock)

	if resolvedBlocks != 0 {
		t.Errorf("resolvedBlocks = %d, want 0 (partial overlap = no fast path)", resolvedBlocks)
	}
	if len(remaining) != 1 {
		t.Errorf("remaining = %d, want 1", len(remaining))
	}
}

// TestSyntheticManifestBlockEpochZero verifies that syntheticManifestBlock
// works correctly even for files with MinTimeNs==0.
func TestSyntheticManifestBlockEpochZero(t *testing.T) {
	s := testStorage()

	fi := manifest.FileInfo{
		Key:       "epoch-zero.parquet",
		Size:      100,
		RowCount:  10,
		MinTimeNs: 0,
		MaxTimeNs: 500,
	}

	db := s.syntheticManifestBlock(fi)
	if db == nil {
		t.Fatal("syntheticManifestBlock returned nil for MinTimeNs=0 file")
	}
	if db.RowsCount() != 10 {
		t.Errorf("RowsCount = %d, want 10", db.RowsCount())
	}
}
