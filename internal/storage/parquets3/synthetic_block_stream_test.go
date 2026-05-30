package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestStreamSyntheticManifestBlocks_ChunkSize verifies that synthetic
// blocks are emitted in chunks of at most syntheticChunkSize rows even
// when the file row count is in the millions. Avoids the previous
// pathology where a single []string of 50M elements was allocated for a
// large file's synthetic block.
func TestStreamSyntheticManifestBlocks_ChunkSize(t *testing.T) {
	s := testStorage()

	// A file with 250,000 rows. With syntheticChunkSize=10_000 we expect
	// 25 chunks, each ≤ 10k rows.
	fi := manifest.FileInfo{
		RowCount:  250_000,
		MinTimeNs: 1_000_000_000,
		MaxTimeNs: 9_000_000_000,
	}

	var totalRows int
	var maxChunk int
	var chunks int
	emit := func(db *logstorage.DataBlock) {
		chunks++
		n := db.RowsCount()
		totalRows += n
		if n > maxChunk {
			maxChunk = n
		}
	}

	s.streamSyntheticManifestBlocks(fi, emit)

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

// TestStreamSyntheticManifestBlocks_SmallFile verifies a sub-chunk
// row count emits exactly one block of that size.
func TestStreamSyntheticManifestBlocks_SmallFile(t *testing.T) {
	s := testStorage()

	fi := manifest.FileInfo{
		RowCount:  100,
		MinTimeNs: 1000,
		MaxTimeNs: 2000,
	}

	var chunks int
	var totalRows int
	s.streamSyntheticManifestBlocks(fi, func(db *logstorage.DataBlock) {
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

// TestStreamSyntheticManifestBlocks_CapsAtMaxRows verifies that the
// total row count is capped at maxSyntheticRows as a safety net.
func TestStreamSyntheticManifestBlocks_CapsAtMaxRows(t *testing.T) {
	s := testStorage()

	fi := manifest.FileInfo{
		RowCount:  int64(maxSyntheticRows) + 5000,
		MinTimeNs: 1_000_000_000,
		MaxTimeNs: 9_000_000_000,
	}

	var totalRows int
	s.streamSyntheticManifestBlocks(fi, func(db *logstorage.DataBlock) {
		totalRows += db.RowsCount()
	})
	if totalRows != maxSyntheticRows {
		t.Errorf("totalRows = %d, want capped at %d", totalRows, maxSyntheticRows)
	}
}
