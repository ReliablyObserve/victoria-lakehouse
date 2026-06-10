package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// ---------------------------------------------------------------------------
// openRangedParquet
// ---------------------------------------------------------------------------

// TestOpenRangedParquet_ReadsRowsOverRanges: the ranged open must yield
// a readable parquet file without a full-body download, in both async
// (default) and sync read modes, with and without a cached schema.
func TestOpenRangedParquet_ReadsRowsOverRanges(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "alpha", SeverityText: "INFO", ServiceName: "svc-a"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "beta", SeverityText: "ERROR", ServiceName: "svc-b"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-06-01/hour=10/ranged.parquet"
	mock.putFile(key, data)
	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	t.Run("async mode without cached schema", func(t *testing.T) {
		f, err := s.openRangedParquet(context.Background(), fi, nil)
		if err != nil {
			t.Fatalf("openRangedParquet: %v", err)
		}
		var total int64
		for _, rg := range f.RowGroups() {
			total += rg.NumRows()
		}
		if total != 2 {
			t.Errorf("rows = %d, want 2", total)
		}
	})

	t.Run("sync mode with cached schema", func(t *testing.T) {
		s.cfg.S3.ParquetReadMode = "sync"
		defer func() { s.cfg.S3.ParquetReadMode = "" }()
		cached, _, err := ParseFooterFromData(key, data)
		if err != nil {
			t.Fatal(err)
		}
		f, err := s.openRangedParquet(context.Background(), fi, cached.File.Schema())
		if err != nil {
			t.Fatalf("openRangedParquet(sync, cached schema): %v", err)
		}
		var total int64
		for _, rg := range f.RowGroups() {
			total += rg.NumRows()
		}
		if total != 2 {
			t.Errorf("rows = %d, want 2", total)
		}
	})

	t.Run("read buffer clamped by small file size", func(t *testing.T) {
		s.cfg.S3.ReadBufferSize = 64 * 1024 * 1024 // absurdly large — must be clamped
		defer func() { s.cfg.S3.ReadBufferSize = 0 }()
		if _, err := s.openRangedParquet(context.Background(), fi, nil); err != nil {
			t.Fatalf("openRangedParquet with oversized buffer config: %v", err)
		}
	})

	t.Run("missing object surfaces an error", func(t *testing.T) {
		ghost := manifest.FileInfo{Key: "logs/dt=2026-06-01/hour=10/ghost.parquet", Size: int64(len(data))}
		if _, err := s.openRangedParquet(context.Background(), ghost, nil); err == nil {
			t.Fatal("expected error for missing S3 object")
		}
	})
}

func TestMax64(t *testing.T) {
	if max64(3, 5) != 5 || max64(5, 3) != 5 || max64(-1, -2) != -1 || max64(7, 7) != 7 {
		t.Error("max64 broken")
	}
}

// ---------------------------------------------------------------------------
// Bloom OR-branch pre-filter
// ---------------------------------------------------------------------------

// orTestStorage builds a storage whose bloom index tags each file with
// one service value (svc-0..svc-N) plus a shared env value.
func orTestStorage(t *testing.T, nFiles int) (*Storage, []manifest.FileInfo) {
	t.Helper()
	s := testStorage()
	s.bloomIdx = bloomindex.New()
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("dt=2026-06-01/hour=10/file%02d.parquet", i)
		bf := bloomindex.NewFilter(8, 0.01)
		bf.Add(fmt.Sprintf("svc-%d", i))
		s.bloomIdx.Add(key, "service.name", bf)
		files[i] = manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.UnixNano(),
		}
	}
	return s, files
}

// TestFilterFilesByBloomIndexOR_UnionsBranches is the Grafana-drilldown
// shape: (svc_a OR svc_b OR svc_c) must keep the union of per-branch
// bloom matches and prune the rest — the previous behaviour returned
// ALL files for any OR query.
func TestFilterFilesByBloomIndexOR_UnionsBranches(t *testing.T) {
	s, files := orTestStorage(t, 20)

	queryStr := `service.name:="svc-3" OR service.name:="svc-7" OR service.name:="svc-15"`
	result, ok := s.filterFilesByBloomIndexOR(files, queryStr)
	if !ok {
		t.Fatal("expected OR shape to be handled")
	}
	got := map[string]bool{}
	for _, fi := range result {
		got[fi.Key] = true
	}
	for _, idx := range []int{3, 7, 15} {
		k := fmt.Sprintf("dt=2026-06-01/hour=10/file%02d.parquet", idx)
		if !got[k] {
			t.Errorf("true-positive file %s pruned (bloom contains its branch value)", k)
		}
	}
	if len(result) > 8 {
		t.Errorf("OR pre-filter too permissive: kept %d/20 files for 3 branches", len(result))
	}
}

// TestFilterFilesByBloomIndexOR_FallbackShapes pins every (files, false)
// bail-out: the caller must fall back rather than over-filter.
func TestFilterFilesByBloomIndexOR_FallbackShapes(t *testing.T) {
	s, files := orTestStorage(t, 5)

	cases := []struct {
		name     string
		queryStr string
	}{
		{"empty query", ""},
		{"wildcard", "*"},
		{"no OR", `service.name:="svc-1"`},
		// severity_text has no bloom column → branch can't be evaluated.
		{"unindexed field branch", `service.name:="svc-1" OR severity_text:="ERROR"`},
		// phrase predicate in a branch — not a simple exact match.
		{"non-exact branch", `service.name:="svc-1" OR body:hello*`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := s.filterFilesByBloomIndexOR(files, tc.queryStr)
			if ok {
				t.Fatalf("shape %q must not be claimed as handled", tc.queryStr)
			}
			if len(result) != len(files) {
				t.Errorf("fallback must return all %d files, got %d", len(files), len(result))
			}
		})
	}
}

// TestFilterFilesByBloomIndex_RoutesOrShape verifies the dispatcher:
// an OR query goes through the branch path end-to-end (pruning), and
// an unsupported OR shape explicitly returns every file.
func TestFilterFilesByBloomIndex_RoutesOrShape(t *testing.T) {
	s, files := orTestStorage(t, 10)

	pruned := s.filterFilesByBloomIndex(files, `service.name:="svc-2" OR service.name:="svc-4"`)
	if len(pruned) >= len(files) {
		t.Errorf("OR query through the dispatcher did not prune: %d files kept", len(pruned))
	}

	kept := s.filterFilesByBloomIndex(files, `service.name:="svc-2" OR body:oops*`)
	if len(kept) != len(files) {
		t.Errorf("unsupported OR shape must keep all files, got %d/%d", len(kept), len(files))
	}
}

func TestResolveBloomColumn(t *testing.T) {
	s := testStorage() // logs profile: service.name and trace_id carry blooms

	if got := s.resolveBloomColumn("service.name"); got == "" {
		t.Error("service.name should resolve to a bloom-enabled parquet column")
	}
	if got := s.resolveBloomColumn("trace_id"); got == "" {
		t.Error("trace_id should resolve to a bloom-enabled parquet column")
	}
	// Low-cardinality field — bloom intentionally not configured.
	if got := s.resolveBloomColumn("severity_text"); got != "" {
		t.Errorf("severity_text must have no bloom column, got %q", got)
	}
	if got := s.resolveBloomColumn("no.such.field"); got != "" {
		t.Errorf("unknown field must resolve to \"\", got %q", got)
	}
}

// ---------------------------------------------------------------------------
// manifestCountFastPath / streamSyntheticAggBlocks
// ---------------------------------------------------------------------------

// collectAggCounts replays emitted synthetic blocks into value→count.
func collectAggCounts(t *testing.T, blocks []*logstorage.DataBlock, fieldCol string) map[string]int64 {
	t.Helper()
	counts := map[string]int64{}
	for _, db := range blocks {
		col := db.GetColumnByName(fieldCol)
		if col == nil {
			t.Fatalf("emitted block lacks the %q column; got %v", fieldCol, db.GetColumns(false))
		}
		for _, v := range col.Values {
			counts[v]++
		}
	}
	return counts
}

// TestManifestCountFastPath_ServesContainedFiles: a file fully inside
// the window with a LabelAggregate is served synthetically (zero S3
// reads); a boundary-straddling file must remain for the scan.
func TestManifestCountFastPath_ServesContainedFiles(t *testing.T) {
	s := testStorage()

	startNs := int64(10_000)
	endNs := int64(20_000)
	contained := manifest.FileInfo{
		Key:       "logs/dt=2026-06-01/hour=10/in.parquet",
		RowCount:  5,
		MinTimeNs: 11_000,
		MaxTimeNs: 19_000,
		LabelAggregates: map[string]map[string]int64{
			"severity_text": {"ERROR": 2, "INFO": 1},
		},
	}
	boundary := manifest.FileInfo{
		Key:       "logs/dt=2026-06-01/hour=10/edge.parquet",
		RowCount:  3,
		MinTimeNs: 9_000, // starts before the window — must be scanned
		MaxTimeNs: 19_000,
		LabelAggregates: map[string]map[string]int64{
			"severity_text": {"WARN": 3},
		},
	}

	var blocks []*logstorage.DataBlock
	remaining := s.manifestCountFastPath(
		[]manifest.FileInfo{contained, boundary}, startNs, endNs, "severity_text",
		func(_ uint, db *logstorage.DataBlock) { blocks = append(blocks, db) })

	if len(remaining) != 1 || remaining[0].Key != boundary.Key {
		t.Fatalf("boundary file must remain for scanning, got remaining=%v", remaining)
	}

	fieldCol := "severity_text"
	if m := s.registry.ResolveFromParquet("severity_text"); m != nil {
		fieldCol = m.InternalName
	}
	counts := collectAggCounts(t, blocks, fieldCol)
	// 2 ERROR + 1 INFO + 2 empty (RowCount 5 - sum 3): the EMPTY group
	// must be reproduced or `count() by (field)` over-counts named values.
	wantErr := s.registry.FormatField(fieldCol, "ERROR")
	wantInfo := s.registry.FormatField(fieldCol, "INFO")
	if counts[wantErr] != 2 || counts[wantInfo] != 1 || counts[""] != 2 {
		t.Errorf("synthetic distribution = %v, want {%s:2 %s:1 \"\":2}", counts, wantErr, wantInfo)
	}
	var total int64
	for _, c := range counts {
		total += c
	}
	if total != 5 {
		t.Errorf("synthetic rows = %d, want RowCount 5", total)
	}
}

// TestStreamSyntheticAggBlocks_Refusals pins every "emit nothing,
// return false" guard — the caller then scans the file normally.
func TestStreamSyntheticAggBlocks_Refusals(t *testing.T) {
	s := testStorage()
	emit := func(*logstorage.DataBlock) {}

	base := manifest.FileInfo{
		Key:       "k",
		RowCount:  10,
		MinTimeNs: 1,
		MaxTimeNs: 2,
		LabelAggregates: map[string]map[string]int64{
			"severity_text": {"ERROR": 10},
		},
	}

	noAgg := base
	noAgg.LabelAggregates = nil
	if s.streamSyntheticAggBlocks(noAgg, "severity_text", emit) {
		t.Error("file without the aggregate must not be served")
	}

	wrongField := base
	if s.streamSyntheticAggBlocks(wrongField, "other_field", emit) {
		t.Error("aggregate for a different field must not be served")
	}

	zeroRows := base
	zeroRows.RowCount = 0
	if s.streamSyntheticAggBlocks(zeroRows, "severity_text", emit) {
		t.Error("RowCount<=0 must not be served")
	}

	tooBig := base
	tooBig.RowCount = maxSyntheticAggRows + 1
	if s.streamSyntheticAggBlocks(tooBig, "severity_text", emit) {
		t.Error("oversized file must not be served synthetically")
	}

	if s.streamSyntheticAggBlocks(base, "severity_text", nil) {
		t.Error("nil emit must refuse")
	}
}

// TestStreamSyntheticAggBlocks_TimestampsMonotoneWithinRange: synthetic
// timestamps must stay inside [MinTimeNs, MaxTimeNs] so a downstream
// time filter never drops or miscounts the synthetic rows.
func TestStreamSyntheticAggBlocks_TimestampsMonotoneWithinRange(t *testing.T) {
	s := testStorage()
	fi := manifest.FileInfo{
		Key:       "k",
		RowCount:  4,
		MinTimeNs: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC).UnixNano(),
		MaxTimeNs: time.Date(2026, 6, 1, 10, 0, 3, 0, time.UTC).UnixNano(),
		LabelAggregates: map[string]map[string]int64{
			"severity_text": {"A": 2, "B": 2},
		},
	}
	tsCol := s.registry.TimestampColumn()
	tsInternal := tsCol
	if m := s.registry.ResolveFromParquet(tsCol); m != nil {
		tsInternal = m.InternalName
	}

	var tsVals []string
	ok := s.streamSyntheticAggBlocks(fi, "severity_text", func(db *logstorage.DataBlock) {
		if col := db.GetColumnByName(tsInternal); col != nil {
			tsVals = append(tsVals, col.Values...)
		}
	})
	if !ok {
		t.Fatal("expected synthetic serve")
	}
	if len(tsVals) != 4 {
		t.Fatalf("expected 4 timestamps, got %d", len(tsVals))
	}
	minStr := s.registry.FormatField(tsInternal, fi.MinTimeNs)
	maxStr := s.registry.FormatField(tsInternal, fi.MaxTimeNs)
	for _, v := range tsVals {
		if v < minStr || v > maxStr {
			t.Errorf("synthetic timestamp %q outside [%q, %q]", v, minStr, maxStr)
		}
	}
}

// ---------------------------------------------------------------------------
// file budget accounting
// ---------------------------------------------------------------------------

// TestFileBudgetOutstanding asserts the reserve/release accounting the
// metrics-export surface reads: acquire moves the outstanding counters,
// release returns them to the baseline.
func TestFileBudgetOutstanding(t *testing.T) {
	baseBytes, baseCount := fileBudgetOutstanding()

	release, err := acquireFileBudget(context.Background(), 1024)
	if err != nil {
		t.Fatalf("acquireFileBudget: %v", err)
	}
	gotBytes, gotCount := fileBudgetOutstanding()
	if gotBytes != baseBytes+1024 || gotCount != baseCount+1 {
		t.Errorf("outstanding after acquire = (%d, %d), want (%d, %d)",
			gotBytes, gotCount, baseBytes+1024, baseCount+1)
	}

	release()
	gotBytes, gotCount = fileBudgetOutstanding()
	if gotBytes != baseBytes || gotCount != baseCount {
		t.Errorf("outstanding after release = (%d, %d), want baseline (%d, %d)",
			gotBytes, gotCount, baseBytes, baseCount)
	}
}

// ---------------------------------------------------------------------------
// extractLogBloomValues
// ---------------------------------------------------------------------------

// TestExtractLogBloomValues: the UNCAPPED bloom feed must carry every
// distinct trace_id / service.name (a capped feed false-negatives on
// values past the cap) and skip empties.
func TestExtractLogBloomValues(t *testing.T) {
	if got := extractLogBloomValues(nil); got != nil {
		t.Errorf("nil rows must yield nil, got %v", got)
	}

	got := extractLogBloomValues([]schema.LogRow{
		{TraceID: "t1", ServiceName: "svc-a"},
		{TraceID: "t2", ServiceName: "svc-a"},
		{TraceID: "t1"},        // dup trace, empty service
		{ServiceName: "svc-b"}, // empty trace
		{},                     // both empty
	})
	if got == nil {
		t.Fatal("expected bloom values")
	}
	if len(got["trace_id"]) != 2 {
		t.Errorf("trace_id values = %v, want exactly {t1, t2} (deduped, empties skipped)", got["trace_id"])
	}
	if len(got["service.name"]) != 2 {
		t.Errorf("service.name values = %v, want exactly {svc-a, svc-b}", got["service.name"])
	}

	// All-empty fields → nil map (no useless bloom entries).
	if got := extractLogBloomValues([]schema.LogRow{{}, {}}); got != nil {
		t.Errorf("rows without bloomable values must yield nil, got %v", got)
	}
}
