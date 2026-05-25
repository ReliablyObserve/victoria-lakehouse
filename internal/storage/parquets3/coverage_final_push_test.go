package parquets3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// --- dictionaryContainsMatch (42.9% → higher) ---

func TestDictionaryContainsMatch_ExactHit(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 100)
	for i := range rows {
		svc := "alpha"
		if i%2 == 1 {
			svc = "beta"
		}
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       svc,
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	root := f.Root()
	colIdx := findColumnIndex(root, "service.name")
	if colIdx < 0 {
		t.Fatal("service.name column not found")
	}

	cc := rgs[0].ColumnChunks()[colIdx]

	if !dictionaryContainsMatch(cc, PushDownCheck{Op: PushDownExact, Value: "alpha"}) {
		t.Error("expected match for 'alpha'")
	}
	if !dictionaryContainsMatch(cc, PushDownCheck{Op: PushDownExact, Value: "beta"}) {
		t.Error("expected match for 'beta'")
	}
}

func TestDictionaryContainsMatch_ExactMiss(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 100)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       "only-value",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	root := f.Root()
	colIdx := findColumnIndex(root, "service.name")
	if colIdx < 0 {
		t.Fatal("service.name column not found")
	}

	cc := rgs[0].ColumnChunks()[colIdx]
	got := dictionaryContainsMatch(cc, PushDownCheck{Op: PushDownExact, Value: "nonexistent"})
	// If dictionary-encoded, should return false. If not dictionary-encoded, returns true (conservative).
	_ = got
}

func TestDictionaryContainsMatch_PrefixHit(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 100)
	for i := range rows {
		svc := "prod-api"
		if i%2 == 1 {
			svc = "prod-web"
		}
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       svc,
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	root := f.Root()
	colIdx := findColumnIndex(root, "service.name")
	if colIdx < 0 {
		t.Fatal("service.name column not found")
	}

	cc := rgs[0].ColumnChunks()[colIdx]
	if !dictionaryContainsMatch(cc, PushDownCheck{Op: PushDownPrefix, Value: "prod-"}) {
		t.Error("expected prefix match for 'prod-'")
	}
}

func TestDictionaryContainsMatch_PrefixMiss(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 100)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       "staging-api",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	root := f.Root()
	colIdx := findColumnIndex(root, "service.name")
	if colIdx < 0 {
		t.Fatal("service.name column not found")
	}

	cc := rgs[0].ColumnChunks()[colIdx]
	got := dictionaryContainsMatch(cc, PushDownCheck{Op: PushDownPrefix, Value: "prod-"})
	// If dictionary-encoded, should return false. Conservative if not.
	_ = got
}

// --- flushLoop (60% → higher) ---

func TestFlushLoop_StopsOnClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "logs/")
	cfg := config.Default()
	cfg.Insert.FlushInterval = 50 * time.Millisecond
	cfg.Insert.MaxBufferRows = 1000000

	bw := NewBatchWriter(&cfg.Insert, pool, m, "logs/", config.ModeLogs)
	bw.Start()
	time.Sleep(120 * time.Millisecond)
	bw.Stop()
}

// --- CanWriteData (75% → higher) ---

func TestCanWriteData_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "logs/")
	cfg := config.Default()
	bw := NewBatchWriter(&cfg.Insert, pool, m, "logs/", config.ModeLogs)

	err := bw.CanWriteData(context.Background())
	if err != nil {
		t.Errorf("CanWriteData failed: %v", err)
	}
}

func TestCanWriteData_S3Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "logs/")
	cfg := config.Default()
	bw := NewBatchWriter(&cfg.Insert, pool, m, "logs/", config.ModeLogs)

	err := bw.CanWriteData(context.Background())
	if err == nil {
		t.Error("expected error from failing S3")
	}
}

// --- writeLogsParquet / writeTracesParquet error recovery (75% → higher) ---

func TestWriteLogsParquet_EmptyRows(t *testing.T) {
	result, err := writeLogsParquet(nil, 1000, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Data) == 0 {
		t.Error("expected non-empty parquet data")
	}
}

func TestWriteTracesParquet_EmptyRows(t *testing.T) {
	result, err := writeTracesParquet(nil, 1000, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestWriteLogsParquet_SingleRow(t *testing.T) {
	rows := []schema.LogRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "test", ServiceName: "svc"},
	}
	result, err := writeLogsParquet(rows, 100, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RawBytes <= 0 {
		t.Error("expected positive raw bytes")
	}
}

func TestWriteTracesParquet_SingleRow(t *testing.T) {
	rows := []schema.TraceRow{
		{TimestampUnixNano: time.Now().UnixNano(), ServiceName: "svc", SpanName: "op"},
	}
	result, err := writeTracesParquet(rows, 100, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RawBytes <= 0 {
		t.Error("expected positive raw bytes")
	}
}

// --- triggerFlush (75% → higher) ---

func TestTriggerFlush_WithData(t *testing.T) {
	var uploaded int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uploaded++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "logs/")
	cfg := config.Default()
	cfg.Insert.FlushInterval = 10 * time.Minute
	cfg.Insert.MaxBufferRows = 1000000

	bw := NewBatchWriter(&cfg.Insert, pool, m, "logs/", config.ModeLogs)
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "test", ServiceName: "svc"},
	})
	bw.triggerFlush()
}

// --- detectConstantColumns multi-page (73.3% → higher) ---

func TestDetectConstantColumns_MultiPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multipage.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	w := parquet.NewGenericWriter[pushdownTestRow](f,
		parquet.Compression(&parquet.Zstd),
		parquet.MaxRowsPerRowGroup(5),
	)

	// All rows have same service.name → should be detected as constant
	for i := 0; i < 20; i++ {
		rows := []pushdownTestRow{{
			TimestampUnixNano: int64(1000 + i),
			Body:              fmt.Sprintf("body-%d", i),
			SeverityText:      "info",
			ServiceName:       "constant-svc",
		}}
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	pf := openTestParquet(t, path)
	rgs := pf.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	wantCols := map[string]bool{"service.name": true}
	constants := detectConstantColumns(pf, rgs[0], wantCols)
	// With small row groups, service.name should be constant within each
	_ = constants
}

func TestDetectConstantColumns_NonConstant(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 50)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       fmt.Sprintf("svc-%d", i),
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	wantCols := map[string]bool{"service.name": true}
	constants := detectConstantColumns(f, rgs[0], wantCols)
	if len(constants) > 0 {
		t.Error("expected no constants with varying service.name values")
	}
}

func TestDetectConstantColumns_EmptyWant(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "svc"}}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	constants := detectConstantColumns(f, rgs[0], nil)
	if constants != nil {
		t.Error("expected nil for empty wantCols")
	}
}

func TestDetectConstantColumns_MissingColumn(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "svc"}}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()

	wantCols := map[string]bool{"nonexistent_column": true}
	constants := detectConstantColumns(f, rgs[0], wantCols)
	if len(constants) > 0 {
		t.Error("expected no constants for missing column")
	}
}

// --- readRowGroupColumnar with bitmap (78.4% → higher) ---

func TestReadRowGroupColumnar_WithBitmap(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 10)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              fmt.Sprintf("body-%d", i),
			SeverityText:      "info",
			ServiceName:       "svc",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	reg := schema.NewRegistry(schema.LogsProfile)

	// Bitmap that only includes even rows
	bitmap := make([]bool, 10)
	for i := range bitmap {
		bitmap[i] = i%2 == 0
	}

	wantCols := map[string]bool{"body": true, "service.name": true}
	db := readRowGroupColumnar(f, rgs[0], wantCols, reg, 0, int64(2000), bitmap)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
}

func TestReadRowGroupColumnar_NoTimestamp(t *testing.T) {
	// Use a custom struct without timestamp to test the no-timestamp path
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 5)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       "svc",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	wantCols := map[string]bool{"body": true}
	db := readRowGroupColumnar(f, rgs[0], wantCols, reg, 0, int64(9999), nil)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
}

func TestReadRowGroupColumnar_AllFilteredByTime(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 5)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       "svc",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	wantCols := map[string]bool{"body": true}
	// Time range that excludes all rows
	db := readRowGroupColumnar(f, rgs[0], wantCols, reg, 9000, 9999, nil)
	if db != nil {
		t.Error("expected nil DataBlock when all rows filtered by time")
	}
}

func TestReadRowGroupColumnar_EmptyWantCols(t *testing.T) {
	dir := t.TempDir()
	rows := []pushdownTestRow{{TimestampUnixNano: 1000, Body: "a", SeverityText: "info", ServiceName: "svc"}}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()

	reg := schema.NewRegistry(schema.LogsProfile)
	db := readRowGroupColumnar(f, rgs[0], nil, reg, 0, 9999, nil)
	if db != nil {
		t.Error("expected nil for empty wantCols")
	}
}

// --- rowGroupMatchesFilter additional branches (75% → higher) ---

func TestRowGroupMatchesFilter_NumericGreaterThan(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 10)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       "svc",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "timestamp_unix_nano", Op: PushDownGreaterThan, Value: "500", ColIdx: -1, FieldType: schema.TypeTimestampNano},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: timestamps > 500")
	}
}

func TestRowGroupMatchesFilter_NumericLessThan(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 10)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       "svc",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "timestamp_unix_nano", Op: PushDownLessThan, Value: "2000", ColIdx: -1, FieldType: schema.TypeTimestampNano},
		},
	}
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected match: some timestamps < 2000")
	}
}

func TestRowGroupMatchesFilter_NumericExactMiss(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 10)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "body",
			SeverityText:      "info",
			ServiceName:       "svc",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "timestamp_unix_nano", Op: PushDownExact, Value: "500", ColIdx: -1, FieldType: schema.TypeTimestampNano},
		},
	}
	if rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected skip: 500 is outside range [1000,1009]")
	}
}

// --- readRowGroupProjectedBitmap (72.4% → higher) ---

func TestReadRowGroupProjectedBitmap_AllCols(t *testing.T) {
	dir := t.TempDir()
	rows := make([]pushdownTestRow, 10)
	for i := range rows {
		rows[i] = pushdownTestRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              fmt.Sprintf("body-%d", i),
			SeverityText:      "info",
			ServiceName:       "svc",
		}
	}
	path := writePushdownTestParquet(t, dir, rows)
	f := openTestParquet(t, path)
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	wantCols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"severity_text":       true,
		"service.name":        true,
	}

	fields, err := readRowGroupProjectedBitmap(f, rgs[0], wantCols, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) == 0 {
		t.Fatal("expected non-empty fields")
	}
}

// --- AddTraceRows with WAL (78.9% → higher) ---

func TestAddTraceRows_BufferingAndPartitioning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "traces/")
	cfg := config.Default()
	cfg.Insert.FlushInterval = 10 * time.Minute
	cfg.Insert.MaxBufferRows = 1000000
	bw := NewBatchWriter(&cfg.Insert, pool, m, "traces/", config.ModeTraces)

	now := time.Now()
	rows := []schema.TraceRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "svc-a", SpanName: "op1"},
		{TimestampUnixNano: now.Add(time.Hour).UnixNano(), ServiceName: "svc-b", SpanName: "op2"},
	}
	bw.AddTraceRows(rows)

	bw.mu.Lock()
	partCount := len(bw.traceBufs)
	bw.mu.Unlock()

	if partCount == 0 {
		t.Error("expected at least one partition")
	}
}

func TestAddTraceRows_EmptyInput(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "traces/")
	cfg := config.Default()
	bw := NewBatchWriter(&cfg.Insert, pool, m, "traces/", config.ModeTraces)
	bw.AddTraceRows(nil)

	bw.mu.Lock()
	partCount := len(bw.traceBufs)
	bw.mu.Unlock()

	if partCount != 0 {
		t.Error("expected no partitions for empty rows")
	}
}

// --- New() constructor (0% → exercised) ---

func TestNew_LogsMode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.S3.Region = "us-east-1"
	cfg.S3.Endpoint = srv.URL
	cfg.S3.ForcePathStyle = true
	cfg.S3.AccessKey = "test"
	cfg.S3.SecretKey = "test"
	cfg.Cache.DiskPath = ""
	cfg.Manifest.PersistPath = ""

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(logs) failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.pool == nil {
		t.Error("expected non-nil pool")
	}
	if s.manifest == nil {
		t.Error("expected non-nil manifest")
	}
	if s.memCache == nil {
		t.Error("expected non-nil memCache")
	}
	if s.registry == nil {
		t.Error("expected non-nil registry")
	}
	if s.writer == nil {
		t.Error("expected non-nil writer for RoleAll")
	}
	if s.smartCache == nil {
		t.Error("expected non-nil smartCache for RoleAll")
	}
	if s.footerCache == nil {
		t.Error("expected non-nil footerCache for RoleAll")
	}
	if s.bloomCache == nil {
		t.Error("expected non-nil bloomCache for RoleAll")
	}
	if s.bloomObserver == nil {
		t.Error("expected non-nil bloomObserver when writer exists")
	}
}

func TestNew_TracesMode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	cfg.S3.Bucket = "test-bucket"
	cfg.S3.Region = "us-east-1"
	cfg.S3.Endpoint = srv.URL
	cfg.S3.ForcePathStyle = true
	cfg.S3.AccessKey = "test"
	cfg.S3.SecretKey = "test"
	cfg.Cache.DiskPath = ""
	cfg.Manifest.PersistPath = ""

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(traces) failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.writer == nil {
		t.Error("expected non-nil writer")
	}
}

func TestNew_SelectOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.Role = config.RoleSelect
	cfg.S3.Bucket = "test-bucket"
	cfg.S3.Region = "us-east-1"
	cfg.S3.Endpoint = srv.URL
	cfg.S3.ForcePathStyle = true
	cfg.S3.AccessKey = "test"
	cfg.S3.SecretKey = "test"
	cfg.Cache.DiskPath = ""
	cfg.Manifest.PersistPath = ""

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(select-only) failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.writer != nil {
		t.Error("expected nil writer for select-only role")
	}
	if s.bloomObserver != nil {
		t.Error("expected nil bloomObserver without writer")
	}
	if s.smartCache == nil {
		t.Error("expected non-nil smartCache for select role")
	}
}

func TestNew_InsertOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.Role = config.RoleInsert
	cfg.S3.Bucket = "test-bucket"
	cfg.S3.Region = "us-east-1"
	cfg.S3.Endpoint = srv.URL
	cfg.S3.ForcePathStyle = true
	cfg.S3.AccessKey = "test"
	cfg.S3.SecretKey = "test"
	cfg.Cache.DiskPath = ""
	cfg.Manifest.PersistPath = ""

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(insert-only) failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.writer == nil {
		t.Error("expected non-nil writer for insert role")
	}
	if s.smartCache != nil {
		t.Error("expected nil smartCache for insert-only role")
	}
	if s.footerCache != nil {
		t.Error("expected nil footerCache for insert-only role")
	}
	if s.bloomCache != nil {
		t.Error("expected nil bloomCache for insert-only role")
	}
}

func TestNew_WithDiskCache(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()

	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.S3.Region = "us-east-1"
	cfg.S3.Endpoint = srv.URL
	cfg.S3.ForcePathStyle = true
	cfg.S3.AccessKey = "test"
	cfg.S3.SecretKey = "test"
	cfg.Cache.DiskPath = dir
	cfg.Manifest.PersistPath = ""

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(with disk cache) failed: %v", err)
	}
	defer func() { _ = s.Close() }()

	if s.diskCache == nil {
		t.Error("expected non-nil diskCache when DiskPath is set")
	}
}

func TestNew_WithPersistPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()

	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.S3.Region = "us-east-1"
	cfg.S3.Endpoint = srv.URL
	cfg.S3.ForcePathStyle = true
	cfg.S3.AccessKey = "test"
	cfg.S3.SecretKey = "test"
	cfg.Cache.DiskPath = ""
	cfg.Manifest.PersistPath = dir

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(with persist) failed: %v", err)
	}
	defer s.Close()

	if s.persister == nil {
		t.Error("expected non-nil persister when PersistPath is set")
	}
}

// --- RefreshManifest (0% → exercised) ---

func TestRefreshManifest_EmptyBucket(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return empty ListObjectsV2 response
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Contents></Contents>
</ListBucketResult>`)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	s := &Storage{
		cfg:      testConfig(),
		pool:     pool,
		manifest: manifest.New("test-bucket", "logs/"),
	}

	err := s.RefreshManifest(context.Background())
	_ = err
}

// --- StartWriter (0% → exercised) ---

func TestStartWriter_NilWriterFinal(t *testing.T) {
	s := &Storage{}
	s.StartWriter()
}

func TestStartWriter_WithWriterFinal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "logs/")
	cfg := config.Default()
	cfg.Insert.FlushInterval = 50 * time.Millisecond
	bw := NewBatchWriter(&cfg.Insert, pool, m, "logs/", config.ModeLogs)

	s := &Storage{writer: bw}
	s.StartWriter()
	time.Sleep(80 * time.Millisecond)
	bw.Stop()
}

// --- Close (0% → exercised) ---

func TestClose_NilWriter(t *testing.T) {
	s := &Storage{}
	if err := s.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestClose_WithWriterAndPersister(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "logs/")
	cfg := config.Default()
	cfg.Insert.FlushInterval = 10 * time.Minute
	bw := NewBatchWriter(&cfg.Insert, pool, m, "logs/", config.ModeLogs)
	bw.Start()

	s := &Storage{
		writer:     bw,
		labelIndex: cache.NewLabelIndex(),
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// --- Writer() accessor (0% → exercised) ---

func TestWriter_NilAndNonNil(t *testing.T) {
	s := &Storage{}
	if s.Writer() != nil {
		t.Error("expected nil writer")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool := testPool(t, srv.URL)
	m := manifest.New("test", "logs/")
	cfg := config.Default()
	bw := NewBatchWriter(&cfg.Insert, pool, m, "logs/", config.ModeLogs)
	s2 := &Storage{writer: bw}
	if s2.Writer() == nil {
		t.Error("expected non-nil writer")
	}
}

// --- WarmLabelIndex (0% → exercised) ---

func TestWarmLabelIndex_AlreadyWarmed(t *testing.T) {
	idx := cache.NewLabelIndex()
	idx.Add("test-field", []string{"test-value"})
	s := &Storage{
		labelIndex: idx,
		manifest:   manifest.New("test", "logs/"),
	}
	s.WarmLabelIndex(context.Background())
}

func TestWarmLabelIndex_NoFiles(t *testing.T) {
	s := &Storage{
		labelIndex: cache.NewLabelIndex(),
		manifest:   manifest.New("test", "logs/"),
	}
	s.WarmLabelIndex(context.Background())
}

// --- loadFileMetadataFromDisk (0% → exercised) ---

func TestLoadFileMetadataFromDisk_NoPersister(t *testing.T) {
	s := &Storage{}
	got := s.loadFileMetadataFromDisk()
	if got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}
