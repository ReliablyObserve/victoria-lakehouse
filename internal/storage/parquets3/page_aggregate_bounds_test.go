package parquets3

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Trap 3 regression tests (parquet-compression-research.md, "The three
// correctness traps under item 1"): every helper that derives a row group's
// timestamp bounds from the page index must aggregate across ALL pages
// (columnIndexTimeBounds) — never MinValue(0)/MaxValue(N-1). The fixture
// writes rows grouped by stream with interleaved time blocks and a tiny
// PageBufferSize, so the true MAX lives in the FIRST pages and the true MIN
// in LATER pages — exactly the page layout the (stream_id, timestamp) row
// sort produces.
//
// True bounds are [1000, 5000]; the positional approximation reads
// (~4500, ~1500). Critically, rowGroupFullyInRange under positional bounds
// wrongly declares FULL containment for windows like [0, 3000], emitting
// rows OUTSIDE the query range.
//
// To verify these are real regression locks: revert columnIndexTimeBounds to
// the positional two-liner — every subtest below MUST fail.

const (
	oooTrueMinNs = int64(1000)
	oooTrueMaxNs = int64(5000)
)

// writeOutOfOrderPagesParquet builds the fixture file and returns it opened,
// together with the timestamp column position and its column index. Skips the
// test when the writer produced fewer than 2 pages (the bug needs multiple
// pages to surface).
func writeOutOfOrderPagesParquet(t *testing.T) (*parquet.File, parquet.RowGroup, int, parquet.ColumnIndex) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ooo_pages.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Small PageBufferSize without compression forces a new page every few
	// values, producing many pages per column.
	w := parquet.NewGenericWriter[pushdownTestRow](f, parquet.PageBufferSize(64))

	// Stream A (first pages): timestamps around 4500 with the TRUE MAX
	// (5000) in the very first page.
	rows := make([]pushdownTestRow, 0, 256)
	for i := 0; i < 128; i++ {
		ts := int64(4500)
		if i == 5 {
			ts = oooTrueMaxNs
		} else if i%3 == 0 {
			ts = 4500 + int64(i%500)
		}
		rows = append(rows, pushdownTestRow{
			TimestampUnixNano: ts, Body: "a", SeverityText: "info", ServiceName: "stream-a",
		})
	}
	// Stream B (later pages): timestamps around 1500 with the TRUE MIN
	// (1000) in a middle page.
	for i := 0; i < 128; i++ {
		ts := int64(1500)
		if i == 5 {
			ts = oooTrueMinNs
		} else if i%3 == 0 {
			ts = 1000 + int64(i%500)
		}
		rows = append(rows, pushdownTestRow{
			TimestampUnixNano: ts, Body: "b", SeverityText: "info", ServiceName: "stream-b",
		})
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rgs := pf.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups produced")
	}
	tsIdx := findColumnIndex(pf.Root(), "timestamp_unix_nano")
	if tsIdx < 0 {
		t.Fatal("timestamp_unix_nano column not found")
	}
	idx, err := rgs[0].ColumnChunks()[tsIdx].ColumnIndex()
	if err != nil {
		t.Fatalf("ColumnIndex: %v", err)
	}
	if idx == nil || idx.NumPages() < 2 {
		t.Skip("parquet writer produced <2 pages; need 2+ to exercise the bug")
	}

	// Fixture sanity: the positional approximation MUST disagree with the
	// true bounds, otherwise none of the assertions below discriminate.
	posMin := idx.MinValue(0).Int64()
	posMax := idx.MaxValue(idx.NumPages() - 1).Int64()
	if posMin == oooTrueMinNs || posMax == oooTrueMaxNs {
		t.Fatalf("fixture regressed: positional bounds (%d, %d) match true bounds (%d, %d)",
			posMin, posMax, oooTrueMinNs, oooTrueMaxNs)
	}
	return pf, rgs[0], tsIdx, idx
}

func TestPageAggregateBounds_RowGroupFullyInRange_OutOfOrderPages(t *testing.T) {
	_, rg, tsIdx, idx := writeOutOfOrderPagesParquet(t)
	posMin := idx.MinValue(0).Int64()
	posMax := idx.MaxValue(idx.NumPages() - 1).Int64()

	tests := []struct {
		name    string
		startNs int64
		endNs   int64
		want    bool
	}{
		// THE critical cases: positional bounds (posMin≈4500, posMax≈1500)
		// satisfy posMin>=start && posMax<=end → the old code declared FULL
		// containment and emitted rows outside the window.
		{"window excludes true MAX (regression: rows at 5000 are out of range)", 0, 3000, false},
		{"window excludes true MIN (regression: rows at 1000 are out of range)", 4000, 6000, false},
		{"window equals true bounds", oooTrueMinNs, oooTrueMaxNs, true},
		{"window strictly containing true bounds", 999, 5001, true},
		{"window clipping true MIN", 1001, 6000, false},
		{"window clipping true MAX", 0, 4999, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rowGroupFullyInRange(rg, tsIdx, tt.startNs, tt.endNs); got != tt.want {
				t.Errorf("rowGroupFullyInRange(rg, %d, %d, %d) = %v, want %v (posBounds=(%d,%d), numPages=%d)",
					tsIdx, tt.startNs, tt.endNs, got, tt.want, posMin, posMax, idx.NumPages())
			}
		})
	}
}

func TestPageAggregateBounds_SyntheticTimestampBlock_OutOfOrderPages(t *testing.T) {
	_, rg, tsIdx, idx := writeOutOfOrderPagesParquet(t)
	s := testStorage()

	db := s.syntheticTimestampBlock(rg, tsIdx, 0, 10_000)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	n := int(rg.NumRows())
	if db.RowsCount() != n {
		t.Fatalf("RowsCount = %d, want %d", db.RowsCount(), n)
	}

	internalName := s.registry.TimestampColumn()
	if m := s.registry.ResolveFromParquet(internalName); m != nil {
		internalName = m.InternalName
	}
	col := db.GetColumnByName(internalName)
	if col == nil || len(col.Values) != n {
		t.Fatalf("timestamp column missing or wrong length")
	}

	// The synthetic series must span the TRUE bounds: first value at the true
	// MIN, last value at trueMin + (n-1)*step computed from the true range.
	wantFirst := s.registry.FormatField(internalName, oooTrueMinNs)
	step := (oooTrueMaxNs - oooTrueMinNs) / int64(n-1)
	if step == 0 {
		step = 1
	}
	wantLast := s.registry.FormatField(internalName, oooTrueMinNs+int64(n-1)*step)
	if col.Values[0] != wantFirst {
		t.Errorf("values[0] = %q, want %q (true min %d)", col.Values[0], wantFirst, oooTrueMinNs)
	}
	if col.Values[n-1] != wantLast {
		t.Errorf("values[n-1] = %q, want %q", col.Values[n-1], wantLast)
	}
	// Absent-value guard: the positional derivation would seed the series at
	// page 0's min (≈4500) instead of the true min.
	if posFirst := s.registry.FormatField(internalName, idx.MinValue(0).Int64()); col.Values[0] == posFirst {
		t.Errorf("values[0] = %q equals the positional page-0 seed — positional derivation regressed", posFirst)
	}
}

func TestPageAggregateBounds_EnrichManifestFromFooter_OutOfOrderPages(t *testing.T) {
	pf, _, _, idx := writeOutOfOrderPagesParquet(t)
	s := testStorage()

	partition := "dt=2026-05-10/hour=14"
	key := "logs/dt=2026-05-10/hour=14/ooo.parquet"
	fi := manifest.FileInfo{Key: key, Size: pf.Size()}
	s.manifest.AddFile(partition, fi)

	s.enrichManifestFromFooter(fi, pf)

	assertTrueBoundsInManifest(t, s, key, idx)
}

func TestPageAggregateBounds_EnrichFromCachedFooter_OutOfOrderPages(t *testing.T) {
	pf, _, _, idx := writeOutOfOrderPagesParquet(t)
	s := testStorage()

	partition := "dt=2026-05-10/hour=14"
	key := "logs/dt=2026-05-10/hour=14/ooo-cached.parquet"
	fi := manifest.FileInfo{Key: key, Size: pf.Size()}
	s.manifest.AddFile(partition, fi)

	if !s.enrichFromCachedFooter(fi, &CachedFooter{File: pf, FileSize: pf.Size()}) {
		t.Fatal("enrichFromCachedFooter returned false")
	}

	assertTrueBoundsInManifest(t, s, key, idx)
}

// assertTrueBoundsInManifest verifies the manifest entry for key carries the
// TRUE file bounds — and explicitly NOT the positional page-0/last-page
// approximation (absent-value guard).
func assertTrueBoundsInManifest(t *testing.T, s *Storage, key string, idx parquet.ColumnIndex) {
	t.Helper()
	var got *manifest.FileInfo
	for _, f := range s.manifest.GetFilesForRange(0, math.MaxInt64) {
		if f.Key == key {
			fc := f
			got = &fc
			break
		}
	}
	if got == nil {
		t.Fatal("enriched file not found in manifest")
	}
	if got.MinTimeNs != oooTrueMinNs || got.MaxTimeNs != oooTrueMaxNs {
		t.Errorf("manifest bounds = (%d, %d), want true bounds (%d, %d)",
			got.MinTimeNs, got.MaxTimeNs, oooTrueMinNs, oooTrueMaxNs)
	}
	posMin := idx.MinValue(0).Int64()
	posMax := idx.MaxValue(idx.NumPages() - 1).Int64()
	if got.MinTimeNs == posMin {
		t.Errorf("MinTimeNs %d equals page-0 min — positional derivation regressed", got.MinTimeNs)
	}
	if got.MaxTimeNs == posMax {
		t.Errorf("MaxTimeNs %d equals last-page max — positional derivation regressed", got.MaxTimeNs)
	}
}

// TestRowGroupMatchesFilter_StringMaxInFirstPage locks the string-branch fix
// in rowGroupMatchesFilter: the aggregate max must include page 0 (the old
// code seeded rgMax from the LAST page and looped from page 1, dropping
// page 0's max). A service.name whose lexicographically largest value lives
// only in the first page must still satisfy a > predicate.
func TestRowGroupMatchesFilter_StringMaxInFirstPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "string_max_first_page.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[pushdownTestRow](f, parquet.PageBufferSize(64))

	rows := make([]pushdownTestRow, 0, 256)
	for i := 0; i < 256; i++ {
		// The lexicographic MAX ("zzz-stream") exists ONLY in row 0, i.e.
		// only in page 0 — every later page holds strictly smaller values.
		svc := "mmm-stream"
		if i == 0 {
			svc = "zzz-stream"
		} else if i >= 128 {
			svc = "aaa-stream"
		}
		rows = append(rows, pushdownTestRow{
			TimestampUnixNano: int64(1000 + i), Body: "a", SeverityText: "info", ServiceName: svc,
		})
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rg := pf.RowGroups()[0]

	svcIdx := findColumnIndex(pf.Root(), "service.name")
	if svcIdx < 0 {
		t.Fatal("service.name column not found")
	}
	cidx, err := rg.ColumnChunks()[svcIdx].ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	if cidx == nil || cidx.NumPages() < 2 {
		t.Skip("parquet writer produced <2 pages; need 2+ to exercise the bug")
	}
	// Fixture sanity: the max must live in page 0 ONLY — if any later page
	// also reaches it, the buggy 1..N-1 loop would still find it and the
	// test stops discriminating.
	if got := valueToString(cidx.MaxValue(0)); got != "zzz-stream" {
		t.Fatalf("fixture regressed: page 0 max = %q, want %q", got, "zzz-stream")
	}
	for p := 1; p < cidx.NumPages(); p++ {
		if got := valueToString(cidx.MaxValue(p)); got >= "zzz-stream" {
			t.Fatalf("fixture regressed: page %d max %q must be < the page-0 max", p, got)
		}
	}

	pdf := &PushDownFilter{Checks: []PushDownCheck{
		// Matches only "zzz-stream" (> "yyy") — present solely in page 0.
		{Column: "service.name", Op: PushDownGreaterThan, Value: "yyy", ColIdx: -1},
	}}
	if !rowGroupMatchesFilter(pf, rg, pdf) {
		t.Error("row group wrongly skipped: page-0 string max was dropped from the aggregate bounds")
	}
}
