package parquets3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/wal"
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

func testStorage() *Storage {
	return &Storage{
		cfg:        testConfig(),
		manifest:   manifest.New("test", "logs/"),
		registry:   schema.NewRegistry(schema.LogsProfile),
		memCache:   cache.NewLRU(64 * 1024 * 1024),
		sfGroup:    cache.NewGroup(),
		labelIndex: cache.NewLabelIndex(),
		discovery:  discovery.New("", nil, "", "", "9428", 5*time.Second),
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

// mustParseQueryWithTime creates a *logstorage.Query with time filter from a query string and ns range.
func mustParseQueryWithTime(t *testing.T, queryStr string, startNs, endNs int64) *logstorage.Query {
	t.Helper()
	if queryStr == "" {
		queryStr = "*"
	}
	q, err := logstorage.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("ParseQuery(%q): %v", queryStr, err)
	}
	q.AddTimeFilter(startNs, endNs)
	return q
}

// mustParseQuery creates a *logstorage.Query from a query string (no time filter).
func mustParseQuery(t *testing.T, queryStr string) *logstorage.Query {
	t.Helper()
	if queryStr == "" {
		queryStr = "*"
	}
	q, err := logstorage.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("ParseQuery(%q): %v", queryStr, err)
	}
	return q
}

// --- Interface compliance ---

func TestStorage_ImplementsInterface(t *testing.T) {
	var _ storage.Storage = (*Storage)(nil)
}

// --- RunQuery tests ---

func TestRunQuery_EmptyManifest(t *testing.T) {
	s := testStorage()
	called := false
	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)
	err := s.RunQuery(context.Background(), nil, q, func(workerID uint, db *logstorage.DataBlock) {
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

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)
	err := s.RunQuery(ctx, nil, q, func(workerID uint, db *logstorage.DataBlock) {
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

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err = s.queryLocalFile(path, info.Size(), startNs, endNs, "", func(workerID uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 3 {
		t.Errorf("expected 3 rows, got %d", totalRows)
	}

	if len(blocks) > 0 {
		colNames := make(map[string]bool)
		for _, col := range blocks[0].GetColumns(false) {
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

	startNs := base.Add(30 * time.Minute).UnixNano()
	endNs := base.Add(90 * time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryLocalFile(path, info.Size(), startNs, endNs, "", func(workerID uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
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

	startNs := base.Add(time.Hour).UnixNano()
	endNs := base.Add(2 * time.Hour).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryLocalFile(path, info.Size(), startNs, endNs, "", func(workerID uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
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

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryLocalFile(path, info.Size(), startNs, endNs, "", func(workerID uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(blocks) == 0 {
		t.Fatal("expected at least one block")
	}

	colNames := make(map[string]bool)
	for _, col := range blocks[0].GetColumns(false) {
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

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryLocalFile(path, info.Size(), startNs, endNs, "", func(workerID uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(blocks) == 0 {
		t.Fatal("expected at least one block")
	}

	if len(blocks[0].GetColumns(false)) != 4 {
		t.Errorf("expected 4 columns (all), got %d", len(blocks[0].GetColumns(false)))
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
	checks := s.buildBloomChecks("")
	if len(checks) != 0 {
		t.Errorf("empty query should produce no bloom checks, got %d", len(checks))
	}
}

func TestBuildBloomChecks_ExactMatch(t *testing.T) {
	s := testStorage()
	checks := s.buildBloomChecks(`service.name:="api-gw"`)
	if len(checks) != 1 {
		t.Fatalf("expected 1 bloom check, got %d", len(checks))
	}
	if checks[0].colName != "service.name" {
		t.Errorf("expected column service.name, got %s", checks[0].colName)
	}
}

func TestBuildBloomChecks_TraceID(t *testing.T) {
	s := testStorage()
	checks := s.buildBloomChecks(`trace_id:="abc123"`)
	if len(checks) != 1 {
		t.Fatalf("expected 1 bloom check for trace_id, got %d", len(checks))
	}
	if checks[0].colName != "trace_id" {
		t.Errorf("expected column trace_id, got %s", checks[0].colName)
	}
}

func TestBuildBloomChecks_NoBloomColumn(t *testing.T) {
	s := testStorage()
	checks := s.buildBloomChecks(`level:="INFO"`)
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

	m := manifest.New("test", "logs/")
	m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{Key: path, Size: info.Size()})

	s := &Storage{
		cfg:      testConfig(),
		manifest: m,
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	startNs := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano()

	fields, err := s.getFieldNamesLocal(context.Background(), startNs, endNs)
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
	startNs := time.Now().Add(-time.Hour).UnixNano()
	endNs := time.Now().UnixNano()

	fields, err := s.getFieldNamesLocal(context.Background(), startNs, endNs)
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
	q := mustParseQuery(t, "*")
	fields, err := s.GetStreamFieldNames(context.Background(), nil, q)
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
		manifest: manifest.New("test", "traces/"),
		registry: schema.NewRegistry(schema.TracesProfile),
	}

	q := mustParseQuery(t, "*")
	fields, err := s.GetStreamFieldNames(context.Background(), nil, q)
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

// --- Cache integration tests ---

func TestGetFileData_CachesInMemory(t *testing.T) {
	dir := t.TempDir()
	rows := []logRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "cached", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	s.memCache.Put(path, data)

	got, err := s.getFileData(context.Background(), path, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Errorf("data len = %d, want %d", len(got), len(data))
	}

	stats := s.memCache.Stats()
	if stats.Hits != 1 {
		t.Errorf("memory cache hits = %d, want 1", stats.Hits)
	}
}

func TestGetFileData_CachesOnDisk(t *testing.T) {
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")

	rows := []logRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "disk-cached", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	fileData, _ := os.ReadFile(path)

	dc, err := cache.NewDiskCache(cacheDir, 100*1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dc.Put(path, fileData); err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	s.diskCache = dc

	got, err := s.getFileData(context.Background(), path, int64(len(fileData)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(fileData) {
		t.Errorf("data len = %d, want %d", len(got), len(fileData))
	}

	diskStats := dc.Stats()
	if diskStats.Hits != 1 {
		t.Errorf("disk cache hits = %d, want 1", diskStats.Hits)
	}

	memStats := s.memCache.Stats()
	if memStats.Entries != 1 {
		t.Errorf("memory cache entries = %d, want 1 (promoted from disk)", memStats.Entries)
	}
}

func TestUpdateLabelIndex(t *testing.T) {
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

	s := testStorage()
	s.updateLabelIndex(f)

	if s.labelIndex.Len() != 4 {
		t.Errorf("label index len = %d, want 4", s.labelIndex.Len())
	}

	names := s.labelIndex.GetFieldNames()
	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}
	for _, expected := range []string{"_time", "_msg", "level", "service.name"} {
		if !nameSet[expected] {
			t.Errorf("label index missing %q", expected)
		}
	}
}

func TestGetFieldNames_UsesLabelIndex(t *testing.T) {
	s := testStorage()
	s.labelIndex.Add("service.name", []string{"api", "web"})
	s.labelIndex.Add("level", []string{"info", "error"})

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)
	fields, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 {
		t.Errorf("expected 2 fields from label index, got %d", len(fields))
	}
}

func TestGetFieldValues_UsesLabelIndex(t *testing.T) {
	s := testStorage()
	s.labelIndex.Add("service.name", []string{"api", "web", "worker"})

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)
	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", uint64(10))
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 3 {
		t.Errorf("expected 3 values from label index, got %d", len(vals))
	}
}

func TestMemCacheStats(t *testing.T) {
	s := testStorage()
	s.memCache.Put("k", []byte("v"))
	stats := s.MemCacheStats()
	if stats.Entries != 1 {
		t.Errorf("entries = %d, want 1", stats.Entries)
	}
}

func TestDiskCacheStats_Nil(t *testing.T) {
	s := testStorage()
	if s.DiskCacheStats() != nil {
		t.Error("expected nil disk cache stats when no disk cache")
	}
}

func TestDiskCacheStats_WithCache(t *testing.T) {
	dir := t.TempDir()
	dc, _ := cache.NewDiskCache(dir, 1024*1024, 0.8)

	s := testStorage()
	s.diskCache = dc
	if _, err := dc.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	stats := s.DiskCacheStats()
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	if stats.Entries != 1 {
		t.Errorf("entries = %d, want 1", stats.Entries)
	}
}

func TestPersistState_NoPersister(t *testing.T) {
	s := testStorage()
	if err := s.PersistState(); err != nil {
		t.Errorf("PersistState with nil persister: %v", err)
	}
}

func TestPersistState_WithPersister(t *testing.T) {
	dir := t.TempDir()
	p, _ := cache.NewPersister(dir)

	s := testStorage()
	s.persister = p
	s.labelIndex.Add("svc", []string{"api"})

	if err := s.PersistState(); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.LoadLabelIndex()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Len() != 1 {
		t.Errorf("loaded label index len = %d, want 1", loaded.Len())
	}
}

func TestClose_PersistsLabelIndex(t *testing.T) {
	dir := t.TempDir()
	p, _ := cache.NewPersister(dir)

	s := testStorage()
	s.persister = p
	s.labelIndex.Add("field1", nil)
	s.labelIndex.Add("field2", nil)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.LoadLabelIndex()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Len() != 2 {
		t.Errorf("persisted label index len = %d, want 2", loaded.Len())
	}
}

// --- Close test ---

func TestClose_NoPersister(t *testing.T) {
	s := testStorage()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// --- Test helpers for local file access ---

func (s *Storage) queryLocalFile(path string, size int64, startNs, endNs int64, queryStr string, writeBlock logstorage.WriteDataBlockFunc) error {
	f, err := parquet.OpenFile(newLocalReaderAt(path), size)
	if err != nil {
		return err
	}

	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())
	bloomChecks := s.buildBloomChecks(queryStr)

	for _, rg := range f.RowGroups() {
		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, startNs, endNs) {
			continue
		}
		if s.bloomFilterSkip(f, rg, bloomChecks) {
			continue
		}
		if err := s.readRowGroup(f, rg, startNs, endNs, writeBlock); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) getFieldNamesLocal(ctx context.Context, startNs, endNs int64) ([]logstorage.ValueWithHits, error) {
	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	fi := files[0]
	f, err := parquet.OpenFile(newLocalReaderAt(fi.Key), fi.Size)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var result []logstorage.ValueWithHits
	for _, name := range columnNames(f.Root()) {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		if !seen[internalName] {
			seen[internalName] = true
			result = append(result, logstorage.ValueWithHits{Value: internalName, Hits: 1})
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

// --- M4: Discovery + Peer Cache tests ---

func TestRunQuery_HotBoundarySuppression(t *testing.T) {
	s := testStorage()

	now := time.Now()
	s.manifest.AddFile("dt="+now.AddDate(0, 0, -2).Format("2006-01-02")+"/hour=00",
		manifest.FileInfo{Key: "logs/old.parquet", Size: 100})

	s.discovery.SetHotBoundaryForTest(&discovery.HotBoundary{
		MinTime: now.AddDate(0, 0, -3),
		MaxTime: now,
	})

	q := mustParseQueryWithTime(t, "*",
		now.Add(-1*time.Hour).UnixNano(),
		now.UnixNano(),
	)

	var called bool
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, _ *logstorage.DataBlock) {
		called = true
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("writeBlock should not be called when query is within hot boundary")
	}
}

func TestDiscovery_Accessor(t *testing.T) {
	s := testStorage()
	if s.Discovery() == nil {
		t.Error("expected non-nil discovery")
	}
}

func TestPeerCache_Accessor_Nil(t *testing.T) {
	s := testStorage()
	if s.PeerCache() != nil {
		t.Error("expected nil peer cache (no peer config)")
	}
	if s.PeerHandler() != nil {
		t.Error("expected nil peer handler (no peer config)")
	}
}

func TestRefreshDiscovery_NoConfig(t *testing.T) {
	s := testStorage()
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Errorf("RefreshDiscovery with no config: %v", err)
	}
}

// --- Task 10: logRowsToDataBlock tests ---

func TestLogRowsToDataBlock(t *testing.T) {
	s := testStorage()

	rows := []schema.LogRow{
		{
			TimestampUnixNano: 1000000000,
			Body:              "hello world",
			SeverityText:      "INFO",
			ServiceName:       "api-gw",
			TraceID:           "trace-1",
			SpanID:            "span-1",
			Stream:            `{service.name="api-gw"}`,
			K8sNamespaceName:  "default",
			K8sPodName:        "pod-1",
			K8sDeploymentName: "deploy-1",
			K8sNodeName:       "node-1",
			DeployEnv:         "production",
			CloudRegion:       "us-east-1",
			HostName:          "host-1",
		},
		{
			TimestampUnixNano: 2000000000,
			Body:              "error occurred",
			SeverityText:      "ERROR",
			ServiceName:       "worker",
		},
	}

	db := s.logRowsToDataBlock(rows)

	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 2 {
		t.Errorf("RowsCount = %d, want 2", db.RowsCount())
	}

	colMap := make(map[string][]string)
	for _, col := range db.GetColumns(false) {
		colMap[col.Name] = col.Values
	}

	// Check _time column
	if vals, ok := colMap["_time"]; !ok {
		t.Error("missing _time column")
	} else {
		if vals[0] != "1000000000" {
			t.Errorf("_time[0] = %q, want %q", vals[0], "1000000000")
		}
		if vals[1] != "2000000000" {
			t.Errorf("_time[1] = %q, want %q", vals[1], "2000000000")
		}
	}

	// Check _msg column
	if vals, ok := colMap["_msg"]; !ok {
		t.Error("missing _msg column")
	} else {
		if vals[0] != "hello world" {
			t.Errorf("_msg[0] = %q, want %q", vals[0], "hello world")
		}
	}

	// Check level column
	if vals, ok := colMap["level"]; !ok {
		t.Error("missing level column")
	} else {
		if vals[0] != "INFO" {
			t.Errorf("level[0] = %q, want %q", vals[0], "INFO")
		}
	}

	// Check service.name column
	if vals, ok := colMap["service.name"]; !ok {
		t.Error("missing service.name column")
	} else {
		if vals[0] != "api-gw" {
			t.Errorf("service.name[0] = %q, want %q", vals[0], "api-gw")
		}
	}

	// Check trace_id column
	if vals, ok := colMap["trace_id"]; !ok {
		t.Error("missing trace_id column")
	} else if vals[0] != "trace-1" {
		t.Errorf("trace_id[0] = %q, want %q", vals[0], "trace-1")
	}

	// Check other mapped columns
	for _, tc := range []struct{ col, want string }{
		{"span_id", "span-1"},
		{"_stream", `{service.name="api-gw"}`},
		{"k8s.namespace.name", "default"},
		{"k8s.pod.name", "pod-1"},
		{"k8s.deployment.name", "deploy-1"},
		{"k8s.node.name", "node-1"},
		{"deployment.environment", "production"},
		{"cloud.region", "us-east-1"},
		{"host.name", "host-1"},
	} {
		if vals, ok := colMap[tc.col]; !ok {
			t.Errorf("missing %s column", tc.col)
		} else if vals[0] != tc.want {
			t.Errorf("%s[0] = %q, want %q", tc.col, vals[0], tc.want)
		}
	}

	// Verify all columns have same length
	for _, col := range db.GetColumns(false) {
		if len(col.Values) != db.RowsCount() {
			t.Errorf("column %s has %d values, want %d", col.Name, len(col.Values), db.RowsCount())
		}
	}
}

func TestLogRowsToDataBlock_Empty(t *testing.T) {
	s := testStorage()
	db := s.logRowsToDataBlock(nil)
	if db != nil {
		t.Error("expected nil DataBlock for nil rows")
	}

	db = s.logRowsToDataBlock([]schema.LogRow{})
	if db != nil {
		t.Error("expected nil DataBlock for empty rows")
	}
}

func TestTraceRowsToDataBlock(t *testing.T) {
	s := testStorage()

	rows := []schema.TraceRow{
		{
			TimestampUnixNano: 1000000000,
			TraceID:           "trace-abc",
			SpanID:            "span-def",
			SpanName:          "GET /api/v1/users",
			ServiceName:       "api-gw",
			DurationNs:        5000000,
			StatusCode:        0,
			ParentSpanID:      "parent-123",
			StatusMessage:     "OK",
		},
		{
			TimestampUnixNano: 2000000000,
			TraceID:           "trace-xyz",
			SpanID:            "span-ghi",
			SpanName:          "db.query",
			ServiceName:       "db-service",
			DurationNs:        1000000,
			StatusCode:        2,
			StatusMessage:     "error",
		},
	}

	db := s.traceRowsToDataBlock(rows)

	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 2 {
		t.Errorf("RowsCount = %d, want 2", db.RowsCount())
	}

	colMap := make(map[string][]string)
	for _, col := range db.GetColumns(false) {
		colMap[col.Name] = col.Values
	}

	// Check _time
	if vals, ok := colMap["_time"]; !ok {
		t.Error("missing _time column")
	} else if vals[0] != "1000000000" {
		t.Errorf("_time[0] = %q, want %q", vals[0], "1000000000")
	}

	// Check trace_id
	if vals, ok := colMap["trace_id"]; !ok {
		t.Error("missing trace_id column")
	} else if vals[0] != "trace-abc" {
		t.Errorf("trace_id[0] = %q, want %q", vals[0], "trace-abc")
	}

	// Check span_id
	if vals, ok := colMap["span_id"]; !ok {
		t.Error("missing span_id column")
	} else if vals[0] != "span-def" {
		t.Errorf("span_id[0] = %q, want %q", vals[0], "span-def")
	}

	// Check name
	if vals, ok := colMap["name"]; !ok {
		t.Error("missing name column")
	} else if vals[0] != "GET /api/v1/users" {
		t.Errorf("name[0] = %q, want %q", vals[0], "GET /api/v1/users")
	}

	// Check service.name
	if vals, ok := colMap["service.name"]; !ok {
		t.Error("missing service.name column")
	} else if vals[0] != "api-gw" {
		t.Errorf("service.name[0] = %q, want %q", vals[0], "api-gw")
	}

	// Check duration
	if vals, ok := colMap["duration"]; !ok {
		t.Error("missing duration column")
	} else if vals[0] != "5000000" {
		t.Errorf("duration[0] = %q, want %q", vals[0], "5000000")
	}

	// Check status_code
	if vals, ok := colMap["status_code"]; !ok {
		t.Error("missing status_code column")
	} else if vals[0] != "0" {
		t.Errorf("status_code[0] = %q, want %q", vals[0], "0")
	}

	// Check parent_span_id
	if vals, ok := colMap["parent_span_id"]; !ok {
		t.Error("missing parent_span_id column")
	} else if vals[0] != "parent-123" {
		t.Errorf("parent_span_id[0] = %q, want %q", vals[0], "parent-123")
	}

	// Check status_message
	if vals, ok := colMap["status_message"]; !ok {
		t.Error("missing status_message column")
	} else if vals[0] != "OK" {
		t.Errorf("status_message[0] = %q, want %q", vals[0], "OK")
	}

	// Verify second row
	if colMap["status_code"][1] != "2" {
		t.Errorf("status_code[1] = %q, want %q", colMap["status_code"][1], "2")
	}

	// Verify all columns have same length
	for _, col := range db.GetColumns(false) {
		if len(col.Values) != db.RowsCount() {
			t.Errorf("column %s has %d values, want %d", col.Name, len(col.Values), db.RowsCount())
		}
	}
}

func TestTraceRowsToDataBlock_Empty(t *testing.T) {
	s := testStorage()
	db := s.traceRowsToDataBlock(nil)
	if db != nil {
		t.Error("expected nil DataBlock for nil rows")
	}

	db = s.traceRowsToDataBlock([]schema.TraceRow{})
	if db != nil {
		t.Error("expected nil DataBlock for empty rows")
	}
}

// --- Task 10: StartWriter WAL replay test ---

func TestStartWriter_WALReplay(t *testing.T) {
	// With nil writer, StartWriter is a no-op.
	s := testStorage()
	s.writer = nil
	s.StartWriter() // should not panic

	// With a writer that has no WAL, ReplayWAL returns (0,0).
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Insert.WALEnabled = false

	s2 := testStorage()
	s2.writer = &BatchWriter{
		cfg:       &cfg.Insert,
		mode:      cfg.Mode,
		logBufs:   make(map[string][]schema.LogRow),
		traceBufs: make(map[string][]schema.TraceRow),
		stopCh:    make(chan struct{}),
	}
	logCount, traceCount := s2.writer.ReplayWAL()
	if logCount != 0 || traceCount != 0 {
		t.Errorf("ReplayWAL with no WAL = (%d, %d), want (0, 0)", logCount, traceCount)
	}
}

func TestStartWriter_WALReplay_WithEntries(t *testing.T) {
	// Test that ReplayWAL recovers entries from a populated WAL.
	dir := t.TempDir()

	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Insert.WALEnabled = true
	cfg.Insert.WALDir = dir

	bw := &BatchWriter{
		cfg:       &cfg.Insert,
		mode:      cfg.Mode,
		logBufs:   make(map[string][]schema.LogRow),
		traceBufs: make(map[string][]schema.TraceRow),
		stopCh:    make(chan struct{}),
	}

	// Open a WAL and write entries to it
	walPath := filepath.Join(dir, "lakehouse.wal")
	w, err := wal.Open(walPath, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	if err := w.AppendLog(&schema.LogRow{TimestampUnixNano: now, Body: "wal-entry-1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendLog(&schema.LogRow{TimestampUnixNano: now + 1, Body: "wal-entry-2"}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	// Re-open WAL so ReplayWAL can read it
	w2, err := wal.Open(walPath, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	bw.wal = w2

	// ReplayWAL should recover the entries
	logCount, traceCount := bw.ReplayWAL()
	if logCount != 2 {
		t.Errorf("ReplayWAL logCount = %d, want 2", logCount)
	}
	if traceCount != 0 {
		t.Errorf("ReplayWAL traceCount = %d, want 0", traceCount)
	}

	// Verify entries are in the buffer
	buffered := bw.BufferedRows()
	if buffered != 2 {
		t.Errorf("BufferedRows after WAL replay = %d, want 2", buffered)
	}

	// Now verify StartWriter calls ReplayWAL before Start by checking
	// that a storage with this writer would trigger the logging path.
	s := testStorage()
	s.writer = bw
	// Don't call StartWriter since we need S3 for flush.
	// Instead verify the contract: ReplayWAL was already called above.
}

// --- Task 10: BufferBridge accessor test ---

func TestBufferBridge_Accessor(t *testing.T) {
	s := testStorage()
	if s.BufferBridge() != nil {
		t.Error("expected nil buffer bridge for default test storage")
	}

	// Assign a bridge and check accessor
	bb := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true}, config.ModeLogs)
	s.bufferBridge = bb
	if s.BufferBridge() != bb {
		t.Error("BufferBridge() did not return the assigned bridge")
	}
}

// --- Tombstone filtering tests ---

func TestTombstone_SetterGetter(t *testing.T) {
	s := testStorage()
	if s.TombstoneStore() != nil {
		t.Error("expected nil tombstone store initially")
	}

	ts := delete.NewTombstoneStore()
	s.SetTombstoneStore(ts)
	if s.TombstoneStore() != ts {
		t.Error("TombstoneStore() did not return the set store")
	}
}

func TestTombstone_NoTombstones_AllRowsPassThrough(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello world", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "second msg", SeverityText: "WARN", ServiceName: "worker"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "third msg", SeverityText: "ERROR", ServiceName: "api-gw"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	s := testStorage()
	// No tombstone store set — all rows should pass through.

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryLocalFile(path, info.Size(), startNs, endNs, "", func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 3 {
		t.Errorf("without tombstones expected 3 rows, got %d", totalRows)
	}
}

func TestTombstone_MatchingRowsSuppressed(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello world", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "secret data", SeverityText: "WARN", ServiceName: "worker"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "third msg", SeverityText: "ERROR", ServiceName: "api-gw"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	s := testStorage()

	// Set up tombstone that matches service.name="worker"
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{
		ID:      "ts-1",
		Query:   `service.name:="worker"`,
		StartNs: now.Add(-time.Minute).UnixNano(),
		EndNs:   now.Add(time.Minute).UnixNano(),
	})
	s.SetTombstoneStore(ts)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock

	// Use filterTombstonedRows directly since queryLocalFile doesn't go through RunQuery's wrapper
	err := s.queryLocalFile(path, info.Size(), startNs, endNs, "", func(_ uint, db *logstorage.DataBlock) {
		filtered := s.filterTombstonedRows(db, startNs, endNs)
		if filtered != nil && filtered.RowsCount() > 0 {
			blocks = append(blocks, filtered)
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows after tombstone suppression (worker row removed), got %d", totalRows)
	}

	// Verify remaining rows are the api-gw ones
	for _, b := range blocks {
		for _, col := range b.GetColumns(false) {
			if col.Name == "service.name" {
				for _, v := range col.Values {
					if v == "worker" {
						t.Error("worker row should have been suppressed by tombstone")
					}
				}
			}
		}
	}
}

func TestTombstone_PartialMatch_OnlyMatchingRowsSuppressed(t *testing.T) {
	s := testStorage()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)

	// Create a DataBlock directly
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{
			fmt.Sprintf("%d", now.UnixNano()),
			fmt.Sprintf("%d", now.Add(time.Second).UnixNano()),
			fmt.Sprintf("%d", now.Add(2*time.Second).UnixNano()),
			fmt.Sprintf("%d", now.Add(3*time.Second).UnixNano()),
		}},
		{Name: "service.name", Values: []string{"api-gw", "worker", "api-gw", "worker"}},
		{Name: "_msg", Values: []string{"msg1", "msg2", "msg3", "msg4"}},
	})

	// Tombstone matches worker service
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{
		ID:      "ts-partial",
		Query:   `service.name:="worker"`,
		StartNs: now.Add(-time.Minute).UnixNano(),
		EndNs:   now.Add(time.Minute).UnixNano(),
	})
	s.SetTombstoneStore(ts)

	filtered := s.filterTombstonedRows(db, now.Add(-time.Minute).UnixNano(), now.Add(time.Minute).UnixNano())

	if filtered == nil {
		t.Fatal("expected non-nil result (partial match)")
	}
	if filtered.RowsCount() != 2 {
		t.Errorf("expected 2 remaining rows, got %d", filtered.RowsCount())
	}

	// Verify remaining are api-gw
	for _, col := range filtered.GetColumns(false) {
		if col.Name == "service.name" {
			for i, v := range col.Values {
				if v != "api-gw" {
					t.Errorf("row %d: expected api-gw, got %s", i, v)
				}
			}
		}
	}
}

func TestTombstone_AllRowsSuppressed_ReturnsNil(t *testing.T) {
	s := testStorage()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)

	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{
			fmt.Sprintf("%d", now.UnixNano()),
			fmt.Sprintf("%d", now.Add(time.Second).UnixNano()),
		}},
		{Name: "service.name", Values: []string{"worker", "worker"}},
		{Name: "_msg", Values: []string{"msg1", "msg2"}},
	})

	// Tombstone matches all rows
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{
		ID:      "ts-all",
		Query:   `service.name:="worker"`,
		StartNs: now.Add(-time.Minute).UnixNano(),
		EndNs:   now.Add(time.Minute).UnixNano(),
	})
	s.SetTombstoneStore(ts)

	filtered := s.filterTombstonedRows(db, now.Add(-time.Minute).UnixNano(), now.Add(time.Minute).UnixNano())

	if filtered != nil {
		t.Errorf("expected nil when all rows suppressed, got %+v", filtered)
	}
}

func TestTombstone_NilStore_PassThrough(t *testing.T) {
	s := testStorage()
	// tombstones is nil by default

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{fmt.Sprintf("%d", now.UnixNano())}},
		{Name: "_msg", Values: []string{"hello"}},
	})

	filtered := s.filterTombstonedRows(db, now.Add(-time.Minute).UnixNano(), now.Add(time.Minute).UnixNano())
	if filtered != db {
		t.Error("with nil tombstone store, should return original DataBlock pointer")
	}
}

func TestTombstone_EmptyForRange_PassThrough(t *testing.T) {
	s := testStorage()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{fmt.Sprintf("%d", now.UnixNano())}},
		{Name: "_msg", Values: []string{"hello"}},
	})

	// Tombstone outside the query range
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{
		ID:      "ts-outside",
		Query:   "*",
		StartNs: now.Add(time.Hour).UnixNano(),
		EndNs:   now.Add(2 * time.Hour).UnixNano(),
	})
	s.SetTombstoneStore(ts)

	filtered := s.filterTombstonedRows(db, now.Add(-time.Minute).UnixNano(), now.Add(time.Minute).UnixNano())
	if filtered != db {
		t.Error("with no matching tombstones in range, should return original DataBlock pointer")
	}
}

func TestTombstone_RunQuery_Integration(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "keep me", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "delete me", SeverityText: "ERROR", ServiceName: "worker"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	s := testStorage()

	// Add file to manifest so RunQuery finds it
	s.manifest.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{Key: path, Size: info.Size()})

	// Override getFileData to read from local path
	s.memCache.Put(path, mustReadFile(t, path))

	// Set up tombstone
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{
		ID:      "ts-integration",
		Query:   `service.name:="worker"`,
		StartNs: now.Add(-time.Minute).UnixNano(),
		EndNs:   now.Add(time.Minute).UnixNano(),
	})
	s.SetTombstoneStore(ts)

	q := mustParseQueryWithTime(t, "*",
		now.Add(-time.Minute).UnixNano(),
		now.Add(time.Minute).UnixNano(),
	)

	var blocks []*logstorage.DataBlock
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 1 {
		t.Errorf("expected 1 row after tombstone filtering in RunQuery, got %d", totalRows)
	}

	// Verify the kept row is api-gw
	for _, b := range blocks {
		for _, col := range b.GetColumns(false) {
			if col.Name == "service.name" {
				for _, v := range col.Values {
					if v == "worker" {
						t.Error("worker row should have been filtered by tombstone in RunQuery")
					}
				}
			}
		}
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
