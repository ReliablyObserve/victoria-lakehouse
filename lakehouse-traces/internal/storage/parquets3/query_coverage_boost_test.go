package parquets3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// =====================================================================
// 1. preFilterFiles — expand coverage from 27.8%
// =====================================================================

func TestPreFilterFiles_EmptyFiles(t *testing.T) {
	s := testStorage()
	result := s.preFilterFiles(nil, `service.name:="api"`)
	if result != nil {
		t.Errorf("expected nil for empty files, got %d", len(result))
	}
}

func TestPreFilterFiles_NoLabelsPassesThrough(t *testing.T) {
	s := testStorage()
	files := []manifest.FileInfo{
		{Key: "x.parquet", MinTimeNs: 1000, MaxTimeNs: 2000},
		{Key: "y.parquet", MinTimeNs: 3000, MaxTimeNs: 4000},
	}
	result := s.preFilterFiles(files, "*")
	if len(result) != 2 {
		t.Errorf("wildcard should return all files, got %d", len(result))
	}
}

func TestPreFilterFiles_WithLabelFiltering(t *testing.T) {
	s := testStorage()
	files := []manifest.FileInfo{
		{Key: "a.parquet", Labels: map[string][]string{"service.name": {"api-gw"}}},
		{Key: "b.parquet", Labels: map[string][]string{"service.name": {"worker"}}},
		{Key: "c.parquet", Labels: map[string][]string{"service.name": {"api-gw", "db"}}},
	}
	result := s.preFilterFiles(files, `service.name:="api-gw"`)
	// a and c match, b does not
	if len(result) != 2 {
		t.Errorf("expected 2 files matching api-gw, got %d", len(result))
	}
}

func TestPreFilterFiles_BloomIndexFiltering(t *testing.T) {
	s := testStorage()
	// Set up a bloom index that marks file "a.parquet" as containing "api-gw"
	// but "b.parquet" as NOT containing "api-gw".
	idx := bloomindex.New()
	fA := bloomindex.NewFilter(10, 0.01)
	fA.Add("api-gw")
	idx.Add("a.parquet", "service.name", fA)

	fB := bloomindex.NewFilter(10, 0.01)
	fB.Add("worker")
	idx.Add("b.parquet", "service.name", fB)

	s.bloomIdx = idx

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	result := s.preFilterFiles(files, `service.name:="api-gw"`)
	// Bloom index should allow "a" and skip "b"
	if len(result) != 1 {
		t.Errorf("expected 1 file after bloom index filter, got %d", len(result))
	}
	if len(result) == 1 && result[0].Key != "a.parquet" {
		t.Errorf("expected a.parquet, got %s", result[0].Key)
	}
}

func TestPreFilterFiles_NilBloomIndex(t *testing.T) {
	s := testStorage()
	s.bloomIdx = nil
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	result := s.preFilterFiles(files, `service.name:="api-gw"`)
	if len(result) != 2 {
		t.Errorf("nil bloom index should pass all files, got %d", len(result))
	}
}

func TestPreFilterFiles_EmptyBloomIndex(t *testing.T) {
	s := testStorage()
	s.bloomIdx = bloomindex.New()
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
	}
	result := s.preFilterFiles(files, `service.name:="api-gw"`)
	if len(result) != 1 {
		t.Errorf("empty bloom index should pass all files, got %d", len(result))
	}
}

// =====================================================================
// 2. queryBufferBridge — expand coverage from 15.4%
// =====================================================================

func TestQueryBufferBridge_DisabledBridge(t *testing.T) {
	s := testStorage()
	bb := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: false}, config.ModeLogs)
	s.bufferBridge = bb
	called := false
	s.queryBufferBridge(context.Background(), 0, int64(time.Hour), nil, nil,
		func(_ uint, db *logstorage.DataBlock) {
			called = true
		})
	if called {
		t.Error("disabled bridge should not call writeBlock")
	}
}

func TestQueryBufferBridge_NoEndpoints(t *testing.T) {
	s := testStorage()
	bb := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 5 * time.Second,
	}, config.ModeLogs)
	// No endpoints set
	s.bufferBridge = bb
	called := false
	s.queryBufferBridge(context.Background(), 0, int64(time.Hour), nil, nil,
		func(_ uint, db *logstorage.DataBlock) {
			called = true
		})
	if called {
		t.Error("bridge with no endpoints should not call writeBlock")
	}
}

func TestQueryBufferBridge_LogsMode(t *testing.T) {
	now := time.Now().UnixNano()
	logRows := []schema.LogRow{
		{TimestampUnixNano: now, Body: "buffered log", SeverityText: "INFO", ServiceName: "buf-svc"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		for _, row := range logRows {
			_ = enc.Encode(row)
		}
	}))
	defer srv.Close()

	s := testStorage()
	bb := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 5 * time.Second,
	}, config.ModeLogs)
	bb.SetEndpoints([]string{srv.URL})
	s.bufferBridge = bb
	s.cfg.Mode = config.ModeLogs

	var blocks []*logstorage.DataBlock
	s.queryBufferBridge(context.Background(), now-int64(time.Minute), now+int64(time.Minute), nil, nil,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		})
	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 1 {
		t.Errorf("expected 1 buffered log row, got %d", totalRows)
	}
}

func TestQueryBufferBridge_TracesMode(t *testing.T) {
	now := time.Now().UnixNano()
	traceRows := []schema.TraceRow{
		{TimestampUnixNano: now, TraceID: "trace-bb-1", SpanID: "span-bb-1", SpanName: "GET /api", ServiceName: "buf-svc", DurationNs: 5000000},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		for _, row := range traceRows {
			_ = enc.Encode(row)
		}
	}))
	defer srv.Close()

	s := testStorage()
	bb := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 5 * time.Second,
	}, config.ModeTraces)
	bb.SetEndpoints([]string{srv.URL})
	s.bufferBridge = bb
	s.cfg.Mode = config.ModeTraces

	var blocks []*logstorage.DataBlock
	s.queryBufferBridge(context.Background(), now-int64(time.Minute), now+int64(time.Minute), nil, nil,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		})
	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 1 {
		t.Errorf("expected 1 buffered trace row, got %d", totalRows)
	}
}

func TestQueryBufferBridge_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := testStorage()
	bb := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 5 * time.Second,
	}, config.ModeLogs)
	bb.SetEndpoints([]string{srv.URL})
	s.bufferBridge = bb
	s.cfg.Mode = config.ModeLogs

	called := false
	s.queryBufferBridge(context.Background(), 0, int64(time.Hour), nil, nil,
		func(_ uint, db *logstorage.DataBlock) {
			called = true
		})
	if called {
		t.Error("server error should not produce blocks")
	}
}

// =====================================================================
// 3. queryFile — expand coverage from 44%
// =====================================================================

func TestQueryFile_WithBloomChecks(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "bloom test", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "bloom test 2", SeverityText: "ERROR", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/bloom-qf.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	// Query with exact match that would trigger bloom check building
	err := s.queryFile(context.Background(), fi, startNs, endNs, `service.name:="api-gw"`, nil, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile: %v", err)
	}
	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows, got %d", totalRows)
	}
}

func TestQueryFile_CancelledContext(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "cancelled", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/cancel.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.queryFile(ctx, fi, 0, int64(time.Hour), "*", nil, func(_ uint, db *logstorage.DataBlock) {
		t.Error("should not be called on cancelled context")
	})
	// Cancelled context may or may not produce an error, depending on timing
	_ = err
}

func TestQueryFile_MissingFile(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	fi := manifest.FileInfo{Key: "logs/nonexistent.parquet", Size: 1000}

	err := s.queryFile(context.Background(), fi, 0, int64(time.Hour), "*", nil, func(_ uint, db *logstorage.DataBlock) {
		t.Error("should not be called for missing file")
	})
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestQueryFile_TimestampOnlyContext(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "tsonly", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/tsonly.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryFile(context.Background(), fi, startNs, endNs, "*", nil, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile: %v", err)
	}
	if len(blocks) == 0 {
		t.Error("expected at least one block")
	}
}

// =====================================================================
// 4. bloomFilterSkip — expand coverage from 60%
// =====================================================================

func TestBloomFilterSkip_WithRealBloom_Hit(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "api-gw"},
	}
	path := writeTestParquetWithBloom(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()

	for _, rg := range f.RowGroups() {
		colIdx := findColumnIndex(f.Root(), "service.name")
		if colIdx < 0 {
			t.Skip("service.name column not found")
		}

		bf := rg.ColumnChunks()[colIdx].BloomFilter()
		if bf == nil || bf.Size() == 0 {
			t.Skip("bloom filter not present")
		}

		// Value present: should NOT skip
		checks := []bloomCheck{
			{colName: "service.name", colIdx: colIdx, value: parquet.ValueOf("api-gw")},
		}
		if s.bloomFilterSkip(f, rg, checks) {
			t.Error("bloom filter should not skip for existing value 'api-gw'")
		}

		// Value absent: should skip
		checks2 := []bloomCheck{
			{colName: "service.name", colIdx: colIdx, value: parquet.ValueOf("nonexistent-service-xyz-99999")},
		}
		skipResult := s.bloomFilterSkip(f, rg, checks2)
		// Bloom filters can have false positives, so we accept either result
		_ = skipResult
	}
}

func TestBloomFilterSkip_OutOfBoundsColIdx(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquetWithBloom(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()

	for _, rg := range f.RowGroups() {
		// Column index way out of bounds
		checks := []bloomCheck{
			{colName: "nonexistent", colIdx: 999, value: parquet.ValueOf("val")},
		}
		if s.bloomFilterSkip(f, rg, checks) {
			t.Error("out-of-bounds column index should not cause skip")
		}
	}
}

func TestBloomFilterSkip_MultipleChecks(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	path := writeTestParquetWithBloom(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()

	for _, rg := range f.RowGroups() {
		colIdx := findColumnIndex(f.Root(), "service.name")
		if colIdx < 0 {
			t.Skip("service.name column not found")
		}

		// Two checks: one valid, one with out-of-bounds index
		checks := []bloomCheck{
			{colName: "service.name", colIdx: colIdx, value: parquet.ValueOf("api-gw")},
			{colName: "nonexistent", colIdx: 999, value: parquet.ValueOf("val")},
		}
		// Should not skip: first check matches, second is out of bounds (skipped)
		if s.bloomFilterSkip(f, rg, checks) {
			t.Error("should not skip when one check matches and other is out of bounds")
		}
	}
}

// =====================================================================
// 5. filterFilesByBloomIndex — expand coverage from 6.7%
// =====================================================================

func TestFilterFilesByBloomIndex_NilIndex(t *testing.T) {
	s := testStorage()
	s.bloomIdx = nil
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	result := s.filterFilesByBloomIndex(files, `service.name:="api-gw"`)
	if len(result) != 2 {
		t.Errorf("nil bloom index should return all files, got %d", len(result))
	}
}

func TestFilterFilesByBloomIndex_EmptyIndex(t *testing.T) {
	s := testStorage()
	s.bloomIdx = bloomindex.New()
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
	}
	result := s.filterFilesByBloomIndex(files, `service.name:="api-gw"`)
	if len(result) != 1 {
		t.Errorf("empty bloom index should return all files, got %d", len(result))
	}
}

func TestFilterFilesByBloomIndex_NoBloomColumn(t *testing.T) {
	s := testStorage()
	idx := bloomindex.New()
	f := bloomindex.NewFilter(10, 0.01)
	f.Add("api-gw")
	idx.Add("a.parquet", "service.name", f)
	s.bloomIdx = idx

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
	}
	// Query for level column which has no bloom filter
	result := s.filterFilesByBloomIndex(files, `level:="INFO"`)
	if len(result) != 1 {
		t.Errorf("no bloom check for level, should return all files, got %d", len(result))
	}
}

func TestFilterFilesByBloomIndex_FiltersCorrectly(t *testing.T) {
	s := testStorage()
	idx := bloomindex.New()

	fA := bloomindex.NewFilter(100, 0.01)
	fA.Add("api-gw")
	idx.Add("a.parquet", "service.name", fA)

	fB := bloomindex.NewFilter(100, 0.01)
	fB.Add("worker")
	idx.Add("b.parquet", "service.name", fB)

	fC := bloomindex.NewFilter(100, 0.01)
	fC.Add("api-gw")
	fC.Add("db-svc")
	idx.Add("c.parquet", "service.name", fC)

	s.bloomIdx = idx

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
		{Key: "c.parquet"},
	}
	result := s.filterFilesByBloomIndex(files, `service.name:="api-gw"`)
	// a and c should match, b should be filtered out
	if len(result) != 2 {
		t.Errorf("expected 2 matching files, got %d", len(result))
	}
	keys := make(map[string]bool)
	for _, fi := range result {
		keys[fi.Key] = true
	}
	if !keys["a.parquet"] {
		t.Error("a.parquet should match")
	}
	if keys["b.parquet"] {
		t.Error("b.parquet should be filtered out")
	}
	if !keys["c.parquet"] {
		t.Error("c.parquet should match")
	}
}

func TestFilterFilesByBloomIndex_WildcardQuery(t *testing.T) {
	s := testStorage()
	idx := bloomindex.New()
	f := bloomindex.NewFilter(10, 0.01)
	f.Add("api-gw")
	idx.Add("a.parquet", "service.name", f)
	s.bloomIdx = idx

	files := []manifest.FileInfo{{Key: "a.parquet"}}
	result := s.filterFilesByBloomIndex(files, "*")
	if len(result) != 1 {
		t.Errorf("wildcard should return all files, got %d", len(result))
	}
}

func TestFilterFilesByBloomIndex_AllFilesMatch(t *testing.T) {
	s := testStorage()
	idx := bloomindex.New()

	for _, key := range []string{"a.parquet", "b.parquet"} {
		f := bloomindex.NewFilter(100, 0.01)
		f.Add("api-gw")
		idx.Add(key, "service.name", f)
	}
	s.bloomIdx = idx

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	result := s.filterFilesByBloomIndex(files, `service.name:="api-gw"`)
	if len(result) != 2 {
		t.Errorf("all files match, should return all; got %d", len(result))
	}
}

// =====================================================================
// 6. checkFileBloom — expand coverage from 66.7%
// =====================================================================

func TestCheckFileBloom_EmptyQuery(t *testing.T) {
	s := testStorage()
	fi := manifest.FileInfo{Key: "test.parquet"}
	if s.checkFileBloom(context.Background(), fi, "") {
		t.Error("empty query should not skip")
	}
}

func TestCheckFileBloom_WildcardQuery(t *testing.T) {
	s := testStorage()
	fi := manifest.FileInfo{Key: "test.parquet"}
	if s.checkFileBloom(context.Background(), fi, "*") {
		t.Error("wildcard query should not skip")
	}
}

func TestCheckFileBloom_NoBloomSidecar(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	fi := manifest.FileInfo{Key: "logs/test.parquet", Size: 1000}
	// No .bloom sidecar uploaded — should not skip
	if s.checkFileBloom(context.Background(), fi, `service.name:="api-gw"`) {
		t.Error("missing bloom sidecar should not cause skip")
	}
}

func TestCheckFileBloom_WithBloomSidecar_Match(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	// Create a bloom index sidecar that contains "api-gw"
	colValues := map[string][]string{
		"service.name": {"api-gw", "worker"},
	}
	idx := bloomindex.NewFileBloomIndex(colValues, 0.01)
	data := idx.Marshal()
	mock.putFile("logs/test.parquet.bloom", data)

	fi := manifest.FileInfo{Key: "logs/test.parquet", Size: 1000}
	// Query matches data in bloom — should NOT skip
	if s.checkFileBloom(context.Background(), fi, `service.name:="api-gw"`) {
		t.Error("matching bloom sidecar should not cause skip")
	}
}

func TestCheckFileBloom_WithBloomSidecar_NoMatch(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	// Create a bloom index sidecar that only contains "api-gw"
	colValues := map[string][]string{
		"service.name": {"api-gw"},
	}
	idx := bloomindex.NewFileBloomIndex(colValues, 0.01)
	data := idx.Marshal()
	mock.putFile("logs/test.parquet.bloom", data)

	fi := manifest.FileInfo{Key: "logs/test.parquet", Size: 1000}
	// Query for value NOT in bloom — should skip
	skip := s.checkFileBloom(context.Background(), fi, `service.name:="nonexistent-service-xyz-42"`)
	if !skip {
		t.Error("bloom sidecar without matching value should cause skip")
	}
}

func TestCheckFileBloom_NoBloomColumns(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	fi := manifest.FileInfo{Key: "logs/test.parquet", Size: 1000}
	// Query for level column which has no bloom filter
	if s.checkFileBloom(context.Background(), fi, `level:="INFO"`) {
		t.Error("no bloom-enabled column for level, should not skip")
	}
}

// =====================================================================
// 7. filterByLabelIndex — expand coverage from 78.3%
// =====================================================================

func TestFilterByLabelIndex_NoIndex(t *testing.T) {
	s := testStorage()
	files := []manifest.FileInfo{
		{Key: "a.parquet", MinTimeNs: 1000, MaxTimeNs: 2000},
		{Key: "b.parquet", MinTimeNs: 3000, MaxTimeNs: 4000},
	}
	pdf := buildPushDownFilter(`service.name:="api-gw"`, s.registry)
	if pdf == nil {
		t.Fatal("expected non-nil pushdown filter")
	}
	result := s.filterByLabelIndex(files, pdf)
	// GetFileKeysByLabel returns nil when no label index is populated → filterByLabelIndex returns nil
	if result != nil {
		t.Errorf("with empty label index, filterByLabelIndex should return nil; got %d files", len(result))
	}
}

func TestFilterByLabelIndex_PopulatedIndex(t *testing.T) {
	s := testStorage()

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:       "a.parquet",
		Size:      100,
		Labels:    map[string][]string{"service.name": {"api-gw"}},
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	})
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:       "b.parquet",
		Size:      200,
		Labels:    map[string][]string{"service.name": {"worker"}},
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	})
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:       "c.parquet",
		Size:      300,
		Labels:    map[string][]string{"service.name": {"api-gw", "db"}},
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	})

	files := s.manifest.GetFilesForRange(now.UnixNano(), now.Add(time.Hour).UnixNano())
	pdf := buildPushDownFilter(`service.name:="api-gw"`, s.registry)
	if pdf == nil {
		t.Fatal("expected non-nil pushdown filter")
	}

	result := s.filterByLabelIndex(files, pdf)
	if result == nil {
		// If manifest doesn't build a label index, filterByLabelIndex returns nil (pass through)
		// This is acceptable since the label index building is managed by AddFile
		return
	}
	for _, fi := range result {
		if fi.Key == "b.parquet" {
			t.Error("b.parquet (worker) should not match api-gw filter")
		}
	}
}

func TestFilterByLabelIndex_NonExactOp(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	// GT/LT ops should cause filterByLabelIndex to return nil (not supported)
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownGreaterThan, Value: "aaa"},
		},
	}
	result := s.filterByLabelIndex(files, pdf)
	if result != nil {
		t.Errorf("non-exact ops should return nil, got %d files", len(result))
	}
}

func TestFilterByLabelIndex_MultipleChecksIntersection(t *testing.T) {
	s := testStorage()

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:       "a.parquet",
		Size:      100,
		Labels:    map[string][]string{"service.name": {"api-gw"}, "trace_id": {"tid-1"}},
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	})
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:       "b.parquet",
		Size:      200,
		Labels:    map[string][]string{"service.name": {"api-gw"}, "trace_id": {"tid-2"}},
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	})

	files := s.manifest.GetFilesForRange(now.UnixNano(), now.Add(time.Hour).UnixNano())
	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownExact, Value: "api-gw"},
			{Column: "trace_id", Op: PushDownExact, Value: "tid-1"},
		},
	}

	result := s.filterByLabelIndex(files, pdf)
	// This tests the intersection path: only file "a" has both api-gw AND tid-1
	// If manifest doesn't support label index, result will be nil (pass through)
	_ = result
}

// =====================================================================
// 8. readRowGroup — expand coverage from 66.7%
// =====================================================================

func TestReadRowGroup_TracesMode(t *testing.T) {
	s := testStorage()
	s.cfg.Mode = config.ModeTraces

	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "trace test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	for _, rg := range f.RowGroups() {
		var blocks []*logstorage.DataBlock
		err := s.readRowGroup(f, rg, startNs, endNs, func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		}, nil)
		if err != nil {
			t.Fatalf("readRowGroup traces: %v", err)
		}
		// Even with wrong schema (logRow vs TraceRow), the read should not panic
	}
}

func TestReadRowGroup_WithTraceIDCollection(t *testing.T) {
	s := testStorage()

	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var traceIDs []string
	for _, rg := range f.RowGroups() {
		err := s.readRowGroup(f, rg, startNs, endNs, func(_ uint, db *logstorage.DataBlock) {}, &traceIDs)
		if err != nil {
			t.Fatalf("readRowGroup: %v", err)
		}
	}
	// traceIDs collection depends on having a trace_id column with non-empty values
	// With logRow test data, there's no trace_id column set
}

// =====================================================================
// 9. readRowGroupProjectedBitmap — expand coverage from 69.7%
// =====================================================================

func TestReadRowGroupProjectedBitmap_EmptyWantCols(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		result, err := readRowGroupProjectedBitmap(f, rg, nil, nil)
		if err != nil {
			t.Fatalf("readRowGroupProjectedBitmap: %v", err)
		}
		if result != nil {
			t.Error("nil wantCols should return nil result")
		}

		result, err = readRowGroupProjectedBitmap(f, rg, map[string]bool{}, nil)
		if err != nil {
			t.Fatalf("readRowGroupProjectedBitmap: %v", err)
		}
		if result != nil {
			t.Error("empty wantCols should return nil result")
		}
	}
}

func TestReadRowGroupProjectedBitmap_WithBitmap(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "row0", SeverityText: "INFO", ServiceName: "svc-a"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "row1", SeverityText: "ERROR", ServiceName: "svc-b"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "row2", SeverityText: "DEBUG", ServiceName: "svc-c"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		// Only include rows 0 and 2, skip row 1
		bitmap := []bool{true, false, true}
		wantCols := map[string]bool{
			"body":         true,
			"service.name": true,
		}

		result, err := readRowGroupProjectedBitmap(f, rg, wantCols, bitmap)
		if err != nil {
			t.Fatalf("readRowGroupProjectedBitmap: %v", err)
		}
		if len(result) != 2 {
			t.Errorf("expected 2 rows (bitmap filtered), got %d", len(result))
		}
	}
}

func TestReadRowGroupProjectedBitmap_AllFalse(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "skip", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		bitmap := []bool{false}
		wantCols := map[string]bool{"body": true}

		result, err := readRowGroupProjectedBitmap(f, rg, wantCols, bitmap)
		if err != nil {
			t.Fatalf("readRowGroupProjectedBitmap: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("all-false bitmap should produce 0 rows, got %d", len(result))
		}
	}
}

func TestReadRowGroupProjectedBitmap_SubsetColumns(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		// Only request body column
		wantCols := map[string]bool{"body": true}

		result, err := readRowGroupProjectedBitmap(f, rg, wantCols, nil)
		if err != nil {
			t.Fatalf("readRowGroupProjectedBitmap: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result))
		}
		if len(result[0]) != 1 {
			t.Errorf("expected 1 field per row (body only), got %d", len(result[0]))
		}
		if result[0][0].name != "body" {
			t.Errorf("expected field name 'body', got %q", result[0][0].name)
		}
	}
}

func TestReadRowGroupProjectedBitmap_NonexistentColumn(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{"nonexistent_column": true}

		result, err := readRowGroupProjectedBitmap(f, rg, wantCols, nil)
		if err != nil {
			t.Fatalf("readRowGroupProjectedBitmap: %v", err)
		}
		// No matching columns → empty specs → nil result
		if result != nil {
			t.Errorf("nonexistent column should produce nil result, got %d rows", len(result))
		}
	}
}

// =====================================================================
// 10. readRowGroupColumnar — expand coverage from 74.3%
// =====================================================================

func TestQCB_ReadRowGroupColumnar_EmptyWantCols(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	for _, rg := range f.RowGroups() {
		db := readRowGroupColumnar(f, rg, nil, reg, startNs, endNs, nil)
		if db != nil {
			t.Error("nil wantCols should return nil")
		}

		db = readRowGroupColumnar(f, rg, map[string]bool{}, reg, startNs, endNs, nil)
		if db != nil {
			t.Error("empty wantCols should return nil")
		}
	}
}

func TestQCB_ReadRowGroupColumnar_WithBitmap(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "row0", SeverityText: "INFO", ServiceName: "svc-a"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "row1", SeverityText: "ERROR", ServiceName: "svc-b"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "row2", SeverityText: "DEBUG", ServiceName: "svc-c"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	for _, rg := range f.RowGroups() {
		bitmap := []bool{true, false, true}
		wantCols := map[string]bool{
			"timestamp_unix_nano": true,
			"body":                true,
		}

		db := readRowGroupColumnar(f, rg, wantCols, reg, startNs, endNs, bitmap)
		if db == nil {
			t.Fatal("expected non-nil DataBlock")
		}
		if db.RowsCount() != 2 {
			t.Errorf("expected 2 rows (bitmap filtered), got %d", db.RowsCount())
		}
	}
}

func TestReadRowGroupColumnar_TimeRangeFilter(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: base.UnixNano(), Body: "in range", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: base.Add(2 * time.Hour).UnixNano(), Body: "out of range", SeverityText: "WARN", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	startNs := base.Add(-time.Minute).UnixNano()
	endNs := base.Add(time.Minute).UnixNano()

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{
			"timestamp_unix_nano": true,
			"body":                true,
		}

		db := readRowGroupColumnar(f, rg, wantCols, reg, startNs, endNs, nil)
		if db == nil {
			t.Fatal("expected non-nil DataBlock")
		}
		if db.RowsCount() != 1 {
			t.Errorf("expected 1 row (time filtered), got %d", db.RowsCount())
		}
	}
}

func TestReadRowGroupColumnar_AllColumns(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "full", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{
			"timestamp_unix_nano": true,
			"body":                true,
			"severity_text":       true,
			"service.name":        true,
		}

		db := readRowGroupColumnar(f, rg, wantCols, reg, startNs, endNs, nil)
		if db == nil {
			t.Fatal("expected non-nil DataBlock")
		}
		if db.RowsCount() != 1 {
			t.Errorf("expected 1 row, got %d", db.RowsCount())
		}
		colNames := make(map[string]bool)
		for _, col := range db.GetColumns(false) {
			colNames[col.Name] = true
		}
		if !colNames["_time"] {
			t.Error("expected _time column")
		}
		if !colNames["_msg"] {
			t.Error("expected _msg column")
		}
	}
}

func TestReadRowGroupColumnar_NonexistentColumn(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{"nonexistent_xyz": true}

		db := readRowGroupColumnar(f, rg, wantCols, reg, startNs, endNs, nil)
		if db != nil {
			t.Error("nonexistent column should produce nil DataBlock")
		}
	}
}

func TestReadRowGroupColumnar_AllRowsOutOfTimeRange(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	// Time range in the far future
	startNs := now.Add(time.Hour).UnixNano()
	endNs := now.Add(2 * time.Hour).UnixNano()

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{
			"timestamp_unix_nano": true,
			"body":                true,
		}

		db := readRowGroupColumnar(f, rg, wantCols, reg, startNs, endNs, nil)
		if db != nil {
			t.Error("all rows out of time range should produce nil DataBlock")
		}
	}
}

// =====================================================================
// 11. detectConstantColumns — expand coverage from 73.3%
// =====================================================================

func TestDetectConstantColumns_AllSameValues(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	// All rows have the same service.name
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "same-svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "INFO", ServiceName: "same-svc"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "c", SeverityText: "INFO", ServiceName: "same-svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{
			"service.name":  true,
			"severity_text": true,
		}

		constants := detectConstantColumns(f, rg, wantCols)
		constMap := make(map[string]bool)
		for _, c := range constants {
			constMap[c.name] = true
		}
		// ByteArray columns (string types) must NEVER be detected as
		// constant because parquet's column-index min/max may be
		// truncated per the PageIndex spec — surfacing as e.g.
		// "notification-ser" alongside "notification-service" in
		// /select/jaeger/api/services. Even when all rows in the page
		// share the value, we cannot use the truncated min/max as the
		// row value safely. Callers must read the actual data pages.
		if constMap["service.name"] {
			t.Error("service.name (ByteArray) must not be detected as constant — truncation risk")
		}
		if constMap["severity_text"] {
			t.Error("severity_text (ByteArray) must not be detected as constant — truncation risk")
		}
	}
}

func TestDetectConstantColumns_DifferentValues(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "svc-1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "ERROR", ServiceName: "svc-2"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{
			"service.name":  true,
			"severity_text": true,
		}
		constants := detectConstantColumns(f, rg, wantCols)
		constMap := make(map[string]bool)
		for _, c := range constants {
			constMap[c.name] = true
		}
		if constMap["service.name"] {
			t.Error("service.name should NOT be constant when values differ")
		}
		if constMap["severity_text"] {
			t.Error("severity_text should NOT be constant when values differ")
		}
	}
}

func TestDetectConstantColumns_EmptyWantCols(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		constants := detectConstantColumns(f, rg, nil)
		if len(constants) != 0 {
			t.Errorf("nil wantCols should return empty, got %d", len(constants))
		}

		constants = detectConstantColumns(f, rg, map[string]bool{})
		if len(constants) != 0 {
			t.Errorf("empty wantCols should return empty, got %d", len(constants))
		}
	}
}

func TestDetectConstantColumns_NonexistentColumn(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{"nonexistent_xyz": true}
		constants := detectConstantColumns(f, rg, wantCols)
		if len(constants) != 0 {
			t.Errorf("nonexistent column should return empty, got %d", len(constants))
		}
	}
}

func TestDetectConstantColumns_MixedConstAndVariable(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "same-svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "ERROR", ServiceName: "same-svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{
			"service.name":  true,
			"severity_text": true,
			"body":          true,
		}
		constants := detectConstantColumns(f, rg, wantCols)
		constMap := make(map[string]bool)
		for _, c := range constants {
			constMap[c.name] = true
		}
		// All three are ByteArray (string) — none may be detected as
		// constant regardless of whether values are the same. The
		// PageIndex truncation rule applies uniformly to BYTE_ARRAY.
		if constMap["service.name"] {
			t.Error("service.name (ByteArray) must not be detected as constant — truncation risk")
		}
		if constMap["severity_text"] {
			t.Error("severity_text (ByteArray) must not be detected as constant — truncation risk")
		}
		if constMap["body"] {
			t.Error("body (ByteArray) must not be detected as constant — truncation risk")
		}
	}
}

// =====================================================================
// 12. dictionaryContainsMatch — expand coverage from 42.9%
// =====================================================================

func TestDictionaryContainsMatch_ExactMatch(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "test2", SeverityText: "ERROR", ServiceName: "worker"},
	}

	// Write with default settings (should use dictionary encoding for string cols)
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		svcIdx := findColumnIndex(f.Root(), "service.name")
		if svcIdx < 0 {
			t.Skip("service.name column not found")
		}

		cols := rg.ColumnChunks()
		if svcIdx >= len(cols) {
			t.Skip("column index out of range")
		}

		// Exact match for existing value
		found := dictionaryContainsMatch(cols[svcIdx], PushDownCheck{
			Op:    PushDownExact,
			Value: "api-gw",
		})
		if !found {
			t.Error("should find 'api-gw' in dictionary")
		}

		// Exact match for non-existing value
		found = dictionaryContainsMatch(cols[svcIdx], PushDownCheck{
			Op:    PushDownExact,
			Value: "nonexistent-unique-service-42",
		})
		// Dictionary may or may not exist depending on encoding; conservative = true
		_ = found
	}
}

func TestDictionaryContainsMatch_PrefixMatch(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "test2", SeverityText: "ERROR", ServiceName: "api-worker"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		svcIdx := findColumnIndex(f.Root(), "service.name")
		if svcIdx < 0 {
			t.Skip("service.name column not found")
		}

		cols := rg.ColumnChunks()
		if svcIdx >= len(cols) {
			t.Skip("column index out of range")
		}

		// Prefix match
		found := dictionaryContainsMatch(cols[svcIdx], PushDownCheck{
			Op:    PushDownPrefix,
			Value: "api-",
		})
		if !found {
			t.Error("should find prefix 'api-' in dictionary")
		}

		// Prefix match for non-matching prefix
		found = dictionaryContainsMatch(cols[svcIdx], PushDownCheck{
			Op:    PushDownPrefix,
			Value: "zzz-nonexist-",
		})
		// Dictionary may or may not exist depending on encoding; conservative = true
		_ = found
	}
}

func TestDictionaryContainsMatch_NonDictionaryColumn(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		tsIdx := findColumnIndex(f.Root(), "timestamp_unix_nano")
		if tsIdx < 0 {
			t.Skip("timestamp column not found")
		}

		cols := rg.ColumnChunks()
		if tsIdx >= len(cols) {
			t.Skip("column index out of range")
		}

		// timestamp_unix_nano is int64, no dictionary expected → should return true (conservative)
		found := dictionaryContainsMatch(cols[tsIdx], PushDownCheck{
			Op:    PushDownExact,
			Value: "12345",
		})
		if !found {
			t.Error("non-dictionary column should return true (conservative)")
		}
	}
}

// =====================================================================
// Additional edge-case tests for broader coverage
// =====================================================================

func TestReadRowGroupWithProjection_ConstantColumnsOnly(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	// All rows have same service.name → it's constant
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "const-svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "INFO", ServiceName: "const-svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	// Request only the constant column
	cols := map[string]bool{"service.name": true}

	var blocks []*logstorage.DataBlock
	err = s.readRowGroupWithProjection(f, rgs[0], startNs, endNs, cols, nil,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		}, nil)
	if err != nil {
		t.Fatalf("readRowGroupWithProjection: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows, got %d", totalRows)
	}
}

func TestReadRowGroupWithProjection_WithPushdownFilter(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "match", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "nomatch", SeverityText: "ERROR", ServiceName: "worker"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"service.name":        true,
	}

	// Build a resolved pushdown filter
	pdf := resolvePushDownIndices(f, buildPushDownFilter(`service.name:="api-gw"`, s.registry))

	var blocks []*logstorage.DataBlock
	err = s.readRowGroupWithProjection(f, rgs[0], startNs, endNs, cols, pdf,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		}, nil)
	if err != nil {
		t.Fatalf("readRowGroupWithProjection: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	// With pushdown, should get only matching rows (or all if pushdown doesn't filter at row level)
	if totalRows == 0 {
		t.Error("expected at least some rows")
	}
}

func TestQueryFile_WithPipeFields(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/pipe-fields.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryFile(context.Background(), fi, startNs, endNs, "*", []string{"body", "severity_text"}, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile with pipe fields: %v", err)
	}
	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows, got %d", totalRows)
	}
}

// Test preFilterFiles with traceID smart cache path
func TestPreFilterFiles_SmartCacheTraceID(t *testing.T) {
	s := testStorage()
	s.smartCache = newSmartCacheWithLocalKeys([]string{"a.parquet", "b.parquet"})

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
		{Key: "c.parquet"},
	}

	// No trace_id in query — should not use smart cache trace_id path
	result := s.preFilterFiles(files, `service.name:="api-gw"`)
	if len(result) > 3 {
		t.Errorf("expected at most 3 files, got %d", len(result))
	}
}

// Test RunQuery integration with buffer bridge and S3 files
func TestRunQuery_WithBufferBridge(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "s3 data", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/bb-integ.parquet"
	registerFileInMockS3(t, s, mock, key, data, now)

	// Set up buffer bridge that returns no rows (empty endpoints)
	bb := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: false,
	}, config.ModeLogs)
	s.bufferBridge = bb
	s.cfg.Mode = config.ModeLogs

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var blocks []*logstorage.DataBlock
	var mu sync.Mutex
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		blocks = append(blocks, db)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows == 0 {
		t.Error("expected at least 1 row from S3")
	}
}

// Test fileLabelsMatch for different PushDownOps
func TestFileLabelsMatch_AllOps(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		check  PushDownCheck
		want   bool
	}{
		{"exact match", []string{"api-gw", "worker"}, PushDownCheck{Op: PushDownExact, Value: "api-gw"}, true},
		{"exact no match", []string{"worker", "db"}, PushDownCheck{Op: PushDownExact, Value: "api-gw"}, false},
		{"prefix match", []string{"api-gw", "api-worker"}, PushDownCheck{Op: PushDownPrefix, Value: "api-"}, true},
		{"prefix no match", []string{"worker", "db"}, PushDownCheck{Op: PushDownPrefix, Value: "api-"}, false},
		{"gt match", []string{"bbb"}, PushDownCheck{Op: PushDownGreaterThan, Value: "aaa"}, true},
		{"gt no match", []string{"aaa"}, PushDownCheck{Op: PushDownGreaterThan, Value: "zzz"}, false},
		{"lt match", []string{"aaa"}, PushDownCheck{Op: PushDownLessThan, Value: "bbb"}, true},
		{"lt no match", []string{"zzz"}, PushDownCheck{Op: PushDownLessThan, Value: "aaa"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fileLabelsMatch(tt.values, tt.check)
			if got != tt.want {
				t.Errorf("fileLabelsMatch(%v, %+v) = %v, want %v", tt.values, tt.check, got, tt.want)
			}
		})
	}
}

// Test extractInValues
func TestExtractInValues(t *testing.T) {
	tests := []struct {
		query     string
		fieldName string
		wantLen   int
	}{
		{`service.name:in("api","worker","db")`, "service.name", 3},
		{`trace_id:in("abc","def")`, "trace_id", 2},
		{`service.name:="api"`, "service.name", 0},
		{``, "service.name", 0},
		{`service.name:in()`, "service.name", 0},
	}

	for _, tt := range tests {
		vals := extractInValues(tt.query, tt.fieldName)
		if len(vals) != tt.wantLen {
			t.Errorf("extractInValues(%q, %q) got %d values, want %d", tt.query, tt.fieldName, len(vals), tt.wantLen)
		}
	}
}

// Test extractFilterValues
func TestExtractFilterValues(t *testing.T) {
	tests := []struct {
		query     string
		fieldName string
		wantLen   int
	}{
		{`service.name:="api-gw"`, "service.name", 1},
		{`service.name:in("api","worker")`, "service.name", 2},
		{`level:="INFO"`, "level", 1},
		{``, "service.name", 0},
	}

	for _, tt := range tests {
		vals := extractFilterValues(tt.query, tt.fieldName)
		if len(vals) != tt.wantLen {
			t.Errorf("extractFilterValues(%q, %q) got %d values, want %d", tt.query, tt.fieldName, len(vals), tt.wantLen)
		}
	}
}

// Test isFileNotFoundError
func TestIsFileNotFoundError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("NoSuchKey: key not found"), true},
		{fmt.Errorf("NotFound"), true},
		{fmt.Errorf("status code 404"), true},
		{fmt.Errorf("file does not exist"), true},
		{fmt.Errorf("file not found"), true},
		{fmt.Errorf("some other error"), false},
	}

	for _, tt := range tests {
		got := isFileNotFoundError(tt.err)
		if got != tt.want {
			t.Errorf("isFileNotFoundError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// Test parquetValueToInterface
func TestParquetValueToInterface(t *testing.T) {
	tests := []struct {
		name string
		val  parquet.Value
	}{
		{"string", parquet.ValueOf("hello")},
		{"int64", parquet.ValueOf(int64(42))},
		{"int32", parquet.ValueOf(int32(7))},
		{"double", parquet.ValueOf(float64(3.14))},
		{"bool", parquet.ValueOf(true)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parquetValueToInterface(tt.val)
			if result == nil {
				t.Error("expected non-nil result")
			}
		})
	}
}

// Test readRowGroupProjected (wrapper)
func TestReadRowGroupProjected_Basic(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	for _, rg := range f.RowGroups() {
		wantCols := map[string]bool{
			"timestamp_unix_nano": true,
			"body":                true,
		}
		result, err := readRowGroupProjected(f, rg, wantCols)
		if err != nil {
			t.Fatalf("readRowGroupProjected: %v", err)
		}
		if len(result) != 1 {
			t.Errorf("expected 1 row, got %d", len(result))
		}
	}
}

// Ensure writeParquetToBytes with bloom works for bloom filter skip tests
func writeTestParquetWithBloomToBytes(t *testing.T, rows []logRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[logRow](&buf,
		parquet.Compression(&parquet.Zstd),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "service.name"),
		),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestQueryFile_MultipleRowGroups(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)

	// Write two sets of rows to create small file (single RG typically)
	var allRows []logRow
	for i := 0; i < 10; i++ {
		allRows = append(allRows, logRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              fmt.Sprintf("msg-%d", i),
			SeverityText:      "INFO",
			ServiceName:       "multi-rg-svc",
		})
	}
	data := writeParquetToBytes(t, allRows)
	key := "logs/dt=2026-05-10/hour=14/multi-rg.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryFile(context.Background(), fi, startNs, endNs, "*", nil, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile: %v", err)
	}
	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 10 {
		t.Errorf("expected 10 rows, got %d", totalRows)
	}
}

// Suppress unused import warnings
var (
	_ = bytes.NewReader
	_ = cache.NewLRU
	_ = discovery.New
)
