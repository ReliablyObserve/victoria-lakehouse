package parquets3

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type logRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	SeverityText      string `parquet:"severity_text"`
	ServiceName       string `parquet:"service.name"`
}

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	return cfg
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func testStorage() *Storage {
	return &Storage{
		cfg:      testConfig(),
		logger:   testLogger(),
		manifest: manifest.New("test", "logs/", testLogger()),
		registry: schema.NewRegistry(schema.LogsProfile),
	}
}

func writeTestParquet(t *testing.T, dir string, rows []logRow) string {
	t.Helper()
	path := filepath.Join(dir, "test.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[logRow](f, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeTestParquetWithBloom(t *testing.T, dir string, rows []logRow) string {
	t.Helper()
	path := filepath.Join(dir, "bloom_test.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[logRow](f,
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
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// --- Interface compliance ---

func TestStorage_ImplementsInterface(t *testing.T) {
	var _ storage.Storage = (*Storage)(nil)
}

// --- RunQuery tests ---

func TestRunQuery_EmptyManifest(t *testing.T) {
	s := testStorage()
	called := false
	err := s.RunQuery(context.Background(), &storage.QueryContext{
		StartNs: time.Now().Add(-time.Hour).UnixNano(),
		EndNs:   time.Now().UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		called = true
	})
	if err != nil {
		t.Errorf("RunQuery: %v", err)
	}
	if called {
		t.Error("empty manifest should not call writeBlock")
	}
}

func TestRunQuery_CancelledContext(t *testing.T) {
	s := testStorage()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.RunQuery(ctx, &storage.QueryContext{
		StartNs: time.Now().Add(-time.Hour).UnixNano(),
		EndNs:   time.Now().UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		t.Error("should not be called on cancelled context")
	})
	if err != nil && err != context.Canceled {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- queryLocalFile tests ---

func TestQueryFile_ReadsParquet(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello world", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "error occurred", SeverityText: "ERROR", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "debug msg", SeverityText: "DEBUG", ServiceName: "worker"},
	}
	path := writeTestParquet(t, dir, rows)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()

	var blocks []*storage.DataBlock
	err = s.queryLocalFile(path, info.Size(), &storage.QueryContext{
		StartNs: now.Add(-time.Minute).UnixNano(),
		EndNs:   now.Add(time.Minute).UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount
	}
	if totalRows != 3 {
		t.Errorf("expected 3 rows, got %d", totalRows)
	}

	if len(blocks) > 0 {
		colNames := make(map[string]bool)
		for _, col := range blocks[0].Columns {
			colNames[col.Name] = true
		}
		if !colNames["_time"] {
			t.Error("expected _time column (mapped from timestamp_unix_nano)")
		}
		if !colNames["_msg"] {
			t.Error("expected _msg column (mapped from body)")
		}
		if !colNames["level"] {
			t.Error("expected level column (mapped from severity_text)")
		}
		if !colNames["service.name"] {
			t.Error("expected service.name column")
		}
	}
}

func TestQueryFile_TimeRangeFilter(t *testing.T) {
	dir := t.TempDir()

	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: base.UnixNano(), Body: "hour 10", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: base.Add(time.Hour).UnixNano(), Body: "hour 11", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: base.Add(2 * time.Hour).UnixNano(), Body: "hour 12", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	s := testStorage()

	var blocks []*storage.DataBlock
	err := s.queryLocalFile(path, info.Size(), &storage.QueryContext{
		StartNs: base.Add(30 * time.Minute).UnixNano(),
		EndNs:   base.Add(90 * time.Minute).UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount
	}
	if totalRows != 1 {
		t.Errorf("expected 1 row in time range, got %d", totalRows)
	}
}

func TestQueryFile_EmptyTimeRange(t *testing.T) {
	dir := t.TempDir()

	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: base.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	s := testStorage()

	var blocks []*storage.DataBlock
	err := s.queryLocalFile(path, info.Size(), &storage.QueryContext{
		StartNs: base.Add(time.Hour).UnixNano(),
		EndNs:   base.Add(2 * time.Hour).UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount
	}
	if totalRows != 0 {
		t.Errorf("expected 0 rows outside time range, got %d", totalRows)
	}
}

// --- Column projection tests ---

func TestQueryFile_ColumnProjection(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	s := testStorage()

	var blocks []*storage.DataBlock
	err := s.queryLocalFile(path, info.Size(), &storage.QueryContext{
		StartNs:          now.Add(-time.Minute).UnixNano(),
		EndNs:            now.Add(time.Minute).UnixNano(),
		RequestedColumns: []string{"_msg", "level"},
	}, func(workerID uint, db *storage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(blocks) == 0 {
		t.Fatal("expected at least one block")
	}

	colNames := make(map[string]bool)
	for _, col := range blocks[0].Columns {
		colNames[col.Name] = true
	}

	if !colNames["_time"] {
		t.Error("_time should always be included in projection")
	}
	if !colNames["_msg"] {
		t.Error("_msg should be in projection")
	}
	if !colNames["level"] {
		t.Error("level should be in projection")
	}
	if colNames["service.name"] {
		t.Error("service.name should NOT be in projection when not requested")
	}
}

func TestQueryFile_ColumnProjection_AllColumns(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	s := testStorage()

	var blocks []*storage.DataBlock
	err := s.queryLocalFile(path, info.Size(), &storage.QueryContext{
		StartNs: now.Add(-time.Minute).UnixNano(),
		EndNs:   now.Add(time.Minute).UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(blocks) == 0 {
		t.Fatal("expected at least one block")
	}

	if len(blocks[0].Columns) != 4 {
		t.Errorf("expected 4 columns (all), got %d", len(blocks[0].Columns))
	}
}

// --- Bloom filter tests ---

func TestBloomFilterSkip_NoChecks(t *testing.T) {
	s := testStorage()
	if s.bloomFilterSkip(nil, nil, nil) {
		t.Error("nil checks should not skip")
	}
	if s.bloomFilterSkip(nil, nil, []bloomCheck{}) {
		t.Error("empty checks should not skip")
	}
}

func TestBuildBloomChecks_EmptyQuery(t *testing.T) {
	s := testStorage()
	checks := s.buildBloomChecks(&storage.QueryContext{})
	if len(checks) != 0 {
		t.Errorf("empty query should produce no bloom checks, got %d", len(checks))
	}
}

func TestBuildBloomChecks_ExactMatch(t *testing.T) {
	s := testStorage()
	checks := s.buildBloomChecks(&storage.QueryContext{
		Query: `service.name:="api-gw"`,
	})
	if len(checks) != 1 {
		t.Fatalf("expected 1 bloom check, got %d", len(checks))
	}
	if checks[0].colName != "service.name" {
		t.Errorf("expected column service.name, got %s", checks[0].colName)
	}
}

func TestBuildBloomChecks_TraceID(t *testing.T) {
	s := testStorage()
	checks := s.buildBloomChecks(&storage.QueryContext{
		Query: `trace_id:="abc123"`,
	})
	if len(checks) != 1 {
		t.Fatalf("expected 1 bloom check for trace_id, got %d", len(checks))
	}
	if checks[0].colName != "trace_id" {
		t.Errorf("expected column trace_id, got %s", checks[0].colName)
	}
}

func TestBuildBloomChecks_NoBloomColumn(t *testing.T) {
	s := testStorage()
	checks := s.buildBloomChecks(&storage.QueryContext{
		Query: `level:="INFO"`,
	})
	if len(checks) != 0 {
		t.Errorf("level has no bloom filter, expected 0 checks, got %d", len(checks))
	}
}

func TestExtractExactMatch(t *testing.T) {
	tests := []struct {
		query string
		field string
		want  string
	}{
		{`service.name:="api-gw"`, "service.name", "api-gw"},
		{`trace_id:="abc123"`, "trace_id", "abc123"},
		{`service.name:"api-gw"`, "service.name", "api-gw"},
		{`body:~"error.*"`, "body", ""},
		{``, "service.name", ""},
		{`level:INFO`, "level", ""},
		{`service.name:="api-gw" AND trace_id:="xyz"`, "service.name", "api-gw"},
		{`service.name:="api-gw" AND trace_id:="xyz"`, "trace_id", "xyz"},
	}
	for _, tt := range tests {
		got := extractExactMatch(tt.query, tt.field)
		if got != tt.want {
			t.Errorf("extractExactMatch(%q, %q) = %q, want %q", tt.query, tt.field, got, tt.want)
		}
	}
}

func TestBloomFilter_WithRealParquet(t *testing.T) {
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
			t.Fatal("service.name column not found")
		}

		bf := rg.ColumnChunks()[colIdx].BloomFilter()
		if bf == nil || bf.Size() == 0 {
			t.Skip("bloom filter not present in test file")
		}

		found, err := bf.Check(parquet.ValueOf("api-gw"))
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Error("bloom filter should find 'api-gw'")
		}

		skipResult := s.bloomFilterSkip(f, rg, []bloomCheck{
			{colName: "service.name", value: parquet.ValueOf("nonexistent-service-xyz-123")},
		})
		_ = skipResult
	}
}

// --- GetFieldNames tests ---

func TestGetFieldNames_FromParquet(t *testing.T) {
	dir := t.TempDir()

	rows := []logRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	m := manifest.New("test", "logs/", testLogger())
	m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{Key: path, Size: info.Size()})

	s := &Storage{
		cfg:      testConfig(),
		logger:   testLogger(),
		manifest: m,
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	fields, err := s.getFieldNamesLocal(context.Background(), &storage.QueryContext{
		StartNs: time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		EndNs:   time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(fields) == 0 {
		t.Fatal("expected field names")
	}

	names := make(map[string]bool)
	for _, f := range fields {
		names[f.Value] = true
	}

	for _, expected := range []string{"_time", "_msg", "level", "service.name"} {
		if !names[expected] {
			t.Errorf("missing expected field %q", expected)
		}
	}
}

func TestGetFieldNames_EmptyManifest(t *testing.T) {
	s := testStorage()
	fields, err := s.getFieldNamesLocal(context.Background(), &storage.QueryContext{
		StartNs: time.Now().Add(-time.Hour).UnixNano(),
		EndNs:   time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 0 {
		t.Errorf("expected 0 fields from empty manifest, got %d", len(fields))
	}
}

// --- GetStreamFieldNames tests ---

func TestGetStreamFieldNames_Logs(t *testing.T) {
	s := testStorage()
	fields, err := s.GetStreamFieldNames(context.Background(), &storage.QueryContext{})
	if err != nil {
		t.Fatal(err)
	}

	names := make(map[string]bool)
	for _, f := range fields {
		names[f.Value] = true
	}

	if !names["service.name"] {
		t.Error("expected service.name in stream fields")
	}
	if !names["k8s.namespace.name"] {
		t.Error("expected k8s.namespace.name in stream fields")
	}
	if !names["k8s.pod.name"] {
		t.Error("expected k8s.pod.name in stream fields")
	}
}

func TestGetStreamFieldNames_Traces(t *testing.T) {
	s := &Storage{
		cfg:      testConfig(),
		logger:   testLogger(),
		manifest: manifest.New("test", "traces/", testLogger()),
		registry: schema.NewRegistry(schema.TracesProfile),
	}

	fields, err := s.GetStreamFieldNames(context.Background(), &storage.QueryContext{})
	if err != nil {
		t.Fatal(err)
	}

	names := make(map[string]bool)
	for _, f := range fields {
		names[f.Value] = true
	}

	if !names["resource_attr:service.name"] {
		t.Error("expected resource_attr:service.name in trace stream fields")
	}
	if !names["name"] {
		t.Error("expected name in trace stream fields")
	}
}

// --- Schema registry integration tests ---

func TestTracesProfile_SchemaMapping(t *testing.T) {
	r := schema.NewRegistry(schema.TracesProfile)

	tests := []struct {
		internal string
		wantCol  string
	}{
		{"_time", "timestamp_unix_nano"},
		{"trace_id", "trace_id"},
		{"span_id", "span_id"},
		{"name", "span.name"},
		{"status_code", "status.code"},
		{"duration", "duration_ns"},
		{"resource_attr:service.name", "service.name"},
	}

	for _, tt := range tests {
		m := r.ResolveToParquet(tt.internal)
		if m == nil {
			t.Errorf("ResolveToParquet(%q) = nil", tt.internal)
			continue
		}
		if m.ParquetColumn != tt.wantCol {
			t.Errorf("ResolveToParquet(%q).ParquetColumn = %q, want %q", tt.internal, m.ParquetColumn, tt.wantCol)
		}
	}
}

func TestStreamFields_Logs(t *testing.T) {
	r := schema.NewRegistry(schema.LogsProfile)
	fields := r.StreamFields()
	if len(fields) != 3 {
		t.Errorf("expected 3 stream fields for logs, got %d", len(fields))
	}
}

func TestStreamFields_Traces(t *testing.T) {
	r := schema.NewRegistry(schema.TracesProfile)
	fields := r.StreamFields()
	if len(fields) != 2 {
		t.Errorf("expected 2 stream fields for traces, got %d", len(fields))
	}
}

// --- projectColumns tests ---

func TestProjectColumns_NoRequestedReturnsAll(t *testing.T) {
	s := testStorage()
	cols := []string{"a", "b", "c"}
	indices := s.projectColumns(cols, nil)
	if len(indices) != 3 {
		t.Errorf("expected 3 indices, got %d", len(indices))
	}
}

func TestProjectColumns_RequestedSubset(t *testing.T) {
	s := testStorage()
	cols := []string{"timestamp_unix_nano", "body", "severity_text", "service.name"}
	indices := s.projectColumns(cols, []string{"_msg"})

	if len(indices) != 2 {
		t.Errorf("expected 2 indices (_time + _msg), got %d", len(indices))
	}

	hasTimestamp := false
	hasBody := false
	for _, idx := range indices {
		if cols[idx] == "timestamp_unix_nano" {
			hasTimestamp = true
		}
		if cols[idx] == "body" {
			hasBody = true
		}
	}
	if !hasTimestamp {
		t.Error("timestamp_unix_nano should always be included")
	}
	if !hasBody {
		t.Error("body should be included when _msg is requested")
	}
}

// --- Helper function tests ---

func TestValueToString(t *testing.T) {
	tests := []struct {
		name string
		val  parquet.Value
		want string
	}{
		{"int32", parquet.ValueOf(int32(42)), "42"},
		{"int64", parquet.ValueOf(int64(1234567890)), "1234567890"},
		{"string", parquet.ValueOf("hello"), "hello"},
		{"bool_true", parquet.ValueOf(true), "true"},
		{"bool_false", parquet.ValueOf(false), "false"},
		{"float", parquet.ValueOf(float32(3.14)), "3.14"},
		{"double", parquet.ValueOf(float64(2.718)), "2.718"},
		{"null", parquet.Value{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valueToString(tt.val)
			if got != tt.want {
				t.Errorf("valueToString(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

func TestValueToInt64(t *testing.T) {
	tests := []struct {
		name string
		val  parquet.Value
		want int64
	}{
		{"int64", parquet.ValueOf(int64(12345)), 12345},
		{"int32", parquet.ValueOf(int32(42)), 42},
		{"null", parquet.Value{}, 0},
		{"string", parquet.ValueOf("not a number"), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valueToInt64(tt.val)
			if got != tt.want {
				t.Errorf("valueToInt64(%v) = %d, want %d", tt.val, got, tt.want)
			}
		})
	}
}

func TestFindColumnIndex(t *testing.T) {
	dir := t.TempDir()
	rows := []logRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	if idx := findColumnIndex(f.Root(), "timestamp_unix_nano"); idx < 0 {
		t.Error("timestamp_unix_nano column should exist")
	}
	if idx := findColumnIndex(f.Root(), "nonexistent"); idx != -1 {
		t.Errorf("nonexistent column should return -1, got %d", idx)
	}
}

func TestColumnNames(t *testing.T) {
	dir := t.TempDir()
	rows := []logRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	names := columnNames(f.Root())
	if len(names) != 4 {
		t.Errorf("expected 4 column names, got %d: %v", len(names), names)
	}

	expected := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"severity_text":       true,
		"service.name":        true,
	}
	for _, name := range names {
		if !expected[name] {
			t.Errorf("unexpected column name: %s", name)
		}
	}
}

func TestIsPrintable(t *testing.T) {
	if !isPrintable([]byte("hello world")) {
		t.Error("ASCII should be printable")
	}
	if !isPrintable([]byte("line1\nline2\ttab")) {
		t.Error("newlines and tabs should be printable")
	}
	if isPrintable([]byte{0x00, 0x01, 0x02}) {
		t.Error("control chars should not be printable")
	}
	if !isPrintable([]byte{}) {
		t.Error("empty bytes should be printable")
	}
}

// --- Close test ---

func TestClose(t *testing.T) {
	s := testStorage()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- Test helpers for local file access ---

func (s *Storage) queryLocalFile(path string, size int64, qctx *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
	f, err := parquet.OpenFile(newLocalReaderAt(path), size)
	if err != nil {
		return err
	}

	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())
	bloomChecks := s.buildBloomChecks(qctx)

	for _, rg := range f.RowGroups() {
		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, qctx.StartNs, qctx.EndNs) {
			continue
		}
		if s.bloomFilterSkip(f, rg, bloomChecks) {
			continue
		}
		if err := s.readRowGroup(f, rg, qctx, writeBlock); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) getFieldNamesLocal(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	files := s.manifest.GetFilesForRange(qctx.StartNs, qctx.EndNs)
	if len(files) == 0 {
		return nil, nil
	}

	fi := files[0]
	f, err := parquet.OpenFile(newLocalReaderAt(fi.Key), fi.Size)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var result []storage.ValueWithHits
	for _, name := range columnNames(f.Root()) {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		if !seen[internalName] {
			seen[internalName] = true
			result = append(result, storage.ValueWithHits{Value: internalName, Hits: 1})
		}
	}
	return result, nil
}

type localReaderAt struct {
	f *os.File
}

func newLocalReaderAt(path string) *localReaderAt {
	f, _ := os.Open(path)
	return &localReaderAt{f: f}
}

func (r *localReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return r.f.ReadAt(p, off)
}
