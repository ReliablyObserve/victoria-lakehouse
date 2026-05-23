package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestManifestFastPath_ResolvesFilesFullyInRange(t *testing.T) {
	s := testStorage()

	// Query range: [1000, 5000]
	startNs := int64(1000)
	endNs := int64(5000)

	files := []manifest.FileInfo{
		// Fully within range -> should be resolved from manifest
		{Key: "full-inside.parquet", Size: 100, RowCount: 10, MinTimeNs: 1500, MaxTimeNs: 4500},
		// Partially overlapping (starts before query) -> should remain for S3
		{Key: "partial-left.parquet", Size: 200, RowCount: 20, MinTimeNs: 500, MaxTimeNs: 3000},
		// Fully within range (exact boundaries) -> should be resolved
		{Key: "exact-boundary.parquet", Size: 150, RowCount: 5, MinTimeNs: 1000, MaxTimeNs: 5000},
		// Partially overlapping (ends after query) -> should remain for S3
		{Key: "partial-right.parquet", Size: 300, RowCount: 30, MinTimeNs: 4000, MaxTimeNs: 6000},
		// Completely outside range -> should remain (MinTimeNs < startNs)
		{Key: "outside.parquet", Size: 50, RowCount: 3, MinTimeNs: 100, MaxTimeNs: 900},
	}

	var resolvedBlocks int
	var resolvedRows int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedBlocks++
		resolvedRows += db.RowsCount()
	}

	remaining := s.manifestFastPath(files, startNs, endNs, writeBlock)

	// Files fully in range: full-inside (10 rows) + exact-boundary (5 rows) = 2 files, 15 rows
	if resolvedBlocks != 2 {
		t.Errorf("resolvedBlocks = %d, want 2", resolvedBlocks)
	}
	if resolvedRows != 15 {
		t.Errorf("resolvedRows = %d, want 15", resolvedRows)
	}

	// Remaining: partial-left, partial-right, outside = 3 files
	if len(remaining) != 3 {
		t.Fatalf("remaining = %d files, want 3", len(remaining))
	}

	wantKeys := map[string]bool{
		"partial-left.parquet":  true,
		"partial-right.parquet": true,
		"outside.parquet":       true,
	}
	for _, fi := range remaining {
		if !wantKeys[fi.Key] {
			t.Errorf("unexpected remaining file: %s", fi.Key)
		}
		delete(wantKeys, fi.Key)
	}
	for k := range wantKeys {
		t.Errorf("expected remaining file not found: %s", k)
	}
}

func TestManifestFastPath_AllFilesResolved(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet", Size: 100, RowCount: 5, MinTimeNs: 2000, MaxTimeNs: 3000},
		{Key: "b.parquet", Size: 200, RowCount: 8, MinTimeNs: 3500, MaxTimeNs: 4500},
	}

	var resolvedRows int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedRows += db.RowsCount()
	}

	remaining := s.manifestFastPath(files, 1000, 5000, writeBlock)

	if len(remaining) != 0 {
		t.Errorf("remaining = %d, want 0 (all files fully in range)", len(remaining))
	}
	if resolvedRows != 13 {
		t.Errorf("resolvedRows = %d, want 13", resolvedRows)
	}
}

func TestManifestFastPath_NoFilesResolved(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet", Size: 100, RowCount: 5, MinTimeNs: 500, MaxTimeNs: 3000},
		{Key: "b.parquet", Size: 200, RowCount: 8, MinTimeNs: 3500, MaxTimeNs: 6000},
	}

	var resolvedBlocks int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedBlocks++
	}

	remaining := s.manifestFastPath(files, 1000, 5000, writeBlock)

	if resolvedBlocks != 0 {
		t.Errorf("resolvedBlocks = %d, want 0 (no files fully in range)", resolvedBlocks)
	}
	if len(remaining) != 2 {
		t.Errorf("remaining = %d, want 2", len(remaining))
	}
}

func TestManifestFastPath_ZeroRowCountSkipped(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		// Fully in range but zero row count -> should remain (no metadata to resolve)
		{Key: "empty.parquet", Size: 100, RowCount: 0, MinTimeNs: 2000, MaxTimeNs: 3000},
	}

	var resolvedBlocks int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedBlocks++
	}

	remaining := s.manifestFastPath(files, 1000, 5000, writeBlock)

	if resolvedBlocks != 0 {
		t.Errorf("resolvedBlocks = %d, want 0 (zero RowCount should not resolve)", resolvedBlocks)
	}
	if len(remaining) != 1 {
		t.Errorf("remaining = %d, want 1", len(remaining))
	}
}

func TestManifestFastPath_MissingTimestampsNotResolved(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		// Missing MinTimeNs -> should remain
		{Key: "no-min.parquet", Size: 100, RowCount: 5, MinTimeNs: 0, MaxTimeNs: 3000},
		// Missing MaxTimeNs -> should remain
		{Key: "no-max.parquet", Size: 100, RowCount: 5, MinTimeNs: 2000, MaxTimeNs: 0},
	}

	var resolvedBlocks int
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		resolvedBlocks++
	}

	remaining := s.manifestFastPath(files, 1000, 5000, writeBlock)

	if resolvedBlocks != 0 {
		t.Errorf("resolvedBlocks = %d, want 0 (missing timestamps should not resolve)", resolvedBlocks)
	}
	if len(remaining) != 2 {
		t.Errorf("remaining = %d, want 2", len(remaining))
	}
}

func TestSyntheticManifestBlock_CorrectRowCount(t *testing.T) {
	s := testStorage()

	tests := []struct {
		name     string
		fi       manifest.FileInfo
		wantRows int
	}{
		{
			name:     "single row",
			fi:       manifest.FileInfo{RowCount: 1, MinTimeNs: 1000, MaxTimeNs: 1000},
			wantRows: 1,
		},
		{
			name:     "multiple rows",
			fi:       manifest.FileInfo{RowCount: 100, MinTimeNs: 1000, MaxTimeNs: 2000},
			wantRows: 100,
		},
		{
			name:     "zero rows returns nil",
			fi:       manifest.FileInfo{RowCount: 0, MinTimeNs: 1000, MaxTimeNs: 2000},
			wantRows: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := s.syntheticManifestBlock(tc.fi)
			if tc.wantRows == 0 {
				if db != nil {
					t.Errorf("expected nil DataBlock for zero rows, got non-nil with %d rows", db.RowsCount())
				}
				return
			}
			if db == nil {
				t.Fatal("expected non-nil DataBlock")
			}
			if db.RowsCount() != tc.wantRows {
				t.Errorf("RowsCount() = %d, want %d", db.RowsCount(), tc.wantRows)
			}
		})
	}
}
