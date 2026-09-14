package parquets3

import (
	"context"
	"sort"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

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

// groupCounts tallies how many synthetic rows carry each value of the resolved
// field column across all emitted blocks — the distribution a downstream
// `stats by (field) count()` would group on.
func (s *Storage) groupCounts(t *testing.T, fi manifest.FileInfo, field string) (perValue map[string]int64, total int64, served bool) {
	t.Helper()
	fieldCol := field
	if m := s.registry.ResolveFromParquet(field); m != nil {
		fieldCol = m.InternalName
	}
	perValue = map[string]int64{}
	served = s.streamSyntheticAggBlocks(fi, field, func(db *logstorage.DataBlock) {
		total += int64(db.RowsCount())
		c := db.GetColumnByName(fieldCol)
		if c == nil {
			t.Fatalf("emitted block missing field column %q", fieldCol)
		}
		for _, v := range c.Values {
			perValue[v]++
		}
	})
	return perValue, total, served
}

// TestStreamSyntheticAggBlocks_DistributionAndEmptyGroup locks the count-by-field
// pushdown's arithmetic: the synthetic blocks must reproduce LabelAggregates'
// per-value counts EXACTLY, plus an empty-value group equal to RowCount-sum, so
// `* | stats by (field) count()` answered from the manifest returns the same
// numbers a full S3 scan would. A wrong empty-group calc = silently wrong counts
// on every recent-window groupby — the exact failure this guards against.
func TestStreamSyntheticAggBlocks_DistributionAndEmptyGroup(t *testing.T) {
	s := testStorage()
	const field = "service.name"
	fi := manifest.FileInfo{
		RowCount:  10,
		MinTimeNs: 1000,
		MaxTimeNs: 2000,
		LabelAggregates: map[string]map[string]int64{
			field: {"api": 3, "web": 2}, // sum 5 → empty group = 10-5 = 5
		},
	}

	perValue, total, served := s.groupCounts(t, fi, field)
	if !served {
		t.Fatal("streamSyntheticAggBlocks returned false for a file with aggregates")
	}
	if total != fi.RowCount {
		t.Errorf("total synthetic rows = %d, want RowCount %d", total, fi.RowCount)
	}
	if len(perValue) != 3 {
		t.Errorf("distinct field groups = %d, want 3 (api, web, empty): %v", len(perValue), perValue)
	}
	got := make([]int64, 0, len(perValue))
	for _, n := range perValue {
		got = append(got, n)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := []int64{2, 3, 5} // web=2, api=3, empty=RowCount-sum=5
	if len(got) != len(want) {
		t.Fatalf("group-count multiset = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("group-count multiset = %v, want %v (empty group must be RowCount-sum)", got, want)
			break
		}
	}
}

// TestStreamSyntheticAggBlocks_NoAggregateScans verifies the pushdown DECLINES
// (returns false, emitting nothing) when the file carries no aggregate for the
// field — the caller must then scan it from S3 rather than report zero rows.
func TestStreamSyntheticAggBlocks_NoAggregateScans(t *testing.T) {
	s := testStorage()
	fi := manifest.FileInfo{RowCount: 10, MinTimeNs: 1000, MaxTimeNs: 2000}
	emitted := false
	if s.streamSyntheticAggBlocks(fi, "service.name", func(*logstorage.DataBlock) { emitted = true }) {
		t.Error("pushdown claimed to serve a file with no aggregate for the field")
	}
	if emitted {
		t.Error("pushdown emitted blocks despite having no aggregate")
	}
}

// TestManifestCountFastPath_ContainmentGate locks the safety gate: only files
// FULLY inside [start,end] may be answered from manifest aggregates. A file that
// spills past the window (or lacks aggregates) must fall through to `remaining`
// for a real scan — otherwise the pushdown would count rows outside the query
// window and over-report.
func TestManifestCountFastPath_ContainmentGate(t *testing.T) {
	s := testStorage()
	const field = "service.name"
	agg := map[string]map[string]int64{field: {"api": 4}}

	start, end := int64(1000), int64(5000)
	contained := manifest.FileInfo{Key: "contained", RowCount: 4, MinTimeNs: 1500, MaxTimeNs: 4500, LabelAggregates: agg}
	spillsRight := manifest.FileInfo{Key: "spills", RowCount: 4, MinTimeNs: 4000, MaxTimeNs: 6000, LabelAggregates: agg}
	noAgg := manifest.FileInfo{Key: "noagg", RowCount: 4, MinTimeNs: 1500, MaxTimeNs: 4500}

	emittedRows := 0
	remaining := s.manifestCountFastPath(
		[]manifest.FileInfo{contained, spillsRight, noAgg},
		start, end, field,
		func(_ uint, db *logstorage.DataBlock) { emittedRows += db.RowsCount() },
	)

	remKeys := map[string]bool{}
	for _, fi := range remaining {
		remKeys[fi.Key] = true
	}
	if remKeys["contained"] {
		t.Error("fully-contained file with aggregates was not served from the manifest")
	}
	if !remKeys["spills"] {
		t.Error("file spilling past the window was served from aggregates (would over-count out-of-window rows)")
	}
	if !remKeys["noagg"] {
		t.Error("file without aggregates was not deferred to a scan")
	}
	if emittedRows != int(contained.RowCount) {
		t.Errorf("emitted %d rows, want %d (only the contained file)", emittedRows, contained.RowCount)
	}
}
