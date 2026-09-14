package parquets3

import (
	"context"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Mirror of internal/storage/parquets3/synthetic_block_stream_test.go.

// TestStreamConstTimeBlocks_ChunkSize verifies that metadata-only blocks are
// emitted in chunks of at most syntheticChunkSize rows even when the file row
// count is in the millions, so a downstream pipe never materializes a huge
// block.
func TestStreamConstTimeBlocks_ChunkSize(t *testing.T) {
	s := testStorage()

	// A file with 250,000 rows. With syntheticChunkSize=10_000 we expect
	// 25 chunks, each <= 10k rows.
	fi := manifest.FileInfo{
		RowCount:  250_000,
		MinTimeNs: 1_000_000_000,
		MaxTimeNs: 9_000_000_000,
	}

	var totalRows int
	var maxChunk int
	var chunks int
	s.streamConstTimeBlocks(context.Background(), fi, func(_ uint, db *logstorage.DataBlock) {
		chunks++
		n := db.RowsCount()
		totalRows += n
		if n > maxChunk {
			maxChunk = n
		}
	})

	if totalRows != int(fi.RowCount) {
		t.Errorf("totalRows = %d, want %d", totalRows, int(fi.RowCount))
	}
	if maxChunk > syntheticChunkSize {
		t.Errorf("maxChunk = %d exceeds syntheticChunkSize=%d", maxChunk, syntheticChunkSize)
	}
	wantChunks := (int(fi.RowCount) + syntheticChunkSize - 1) / syntheticChunkSize
	if chunks != wantChunks {
		t.Errorf("chunks = %d, want %d", chunks, wantChunks)
	}
}

// TestStreamConstTimeBlocks_SmallFile verifies a sub-chunk row count emits
// exactly one block of that size.
func TestStreamConstTimeBlocks_SmallFile(t *testing.T) {
	s := testStorage()

	fi := manifest.FileInfo{
		RowCount:  100,
		MinTimeNs: 1000,
		MaxTimeNs: 2000,
	}

	var chunks int
	var totalRows int
	s.streamConstTimeBlocks(context.Background(), fi, func(_ uint, db *logstorage.DataBlock) {
		chunks++
		totalRows += db.RowsCount()
	})

	if chunks != 1 {
		t.Errorf("chunks = %d, want 1", chunks)
	}
	if totalRows != 100 {
		t.Errorf("totalRows = %d, want 100", totalRows)
	}
}

// TestStreamConstTimeBlocks_NoRowCap is the regression for the under-count
// bug: the fast path used to stop at maxSyntheticRows = 1_000_000 rows per
// file, so any file above that silently reported fewer rows than it holds.
// Every row count must now be emitted in full.
func TestStreamConstTimeBlocks_NoRowCap(t *testing.T) {
	s := testStorage()

	for _, rowCount := range []int64{1_000_001, 1_500_000, 5_000_000} {
		fi := manifest.FileInfo{
			RowCount:  rowCount,
			MinTimeNs: 1_000_000_000,
			MaxTimeNs: 9_000_000_000,
		}
		var totalRows int64
		s.streamConstTimeBlocks(context.Background(), fi, func(_ uint, db *logstorage.DataBlock) {
			totalRows += int64(db.RowsCount())
		})
		if totalRows != rowCount {
			t.Errorf("RowCount=%d: emitted %d rows, want %d (the 1M cap must be gone)", rowCount, totalRows, rowCount)
		}
	}
}

// TestStreamConstTimeBlocks_StopsOnCancelledContext locks the safeguard that
// replaced the row cap: a runaway row count is bounded by the query's own
// budget, which cancels the context, not by silently truncating the answer.
func TestStreamConstTimeBlocks_StopsOnCancelledContext(t *testing.T) {
	s := testStorage()
	fi := manifest.FileInfo{RowCount: 10_000_000, MinTimeNs: 1_000_000_000, MaxTimeNs: 9_000_000_000}

	ctx, cancel := context.WithCancel(context.Background())
	var blocks int
	s.streamConstTimeBlocks(ctx, fi, func(_ uint, db *logstorage.DataBlock) {
		blocks++
		if blocks == 3 {
			cancel()
		}
	})
	if blocks != 3 {
		t.Errorf("blocks = %d, want 3 (emission must stop once the context is cancelled)", blocks)
	}
}
