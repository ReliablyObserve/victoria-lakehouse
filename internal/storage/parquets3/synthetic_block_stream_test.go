package parquets3

import (
	"sort"
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
