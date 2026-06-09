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
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fullLogRow has all columns needed for field/stream testing
type fullLogRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	SeverityText      string `parquet:"severity_text"`
	ServiceName       string `parquet:"service.name"`
	Stream            string `parquet:"_stream"`
	StreamID          string `parquet:"_stream_id"`
	TraceID           string `parquet:"trace_id"`
	SpanID            string `parquet:"span_id"`
}

func writeFullLogParquet(t *testing.T, dir, name string, rows []fullLogRow) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[fullLogRow](f, parquet.Compression(&parquet.Zstd))
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

// testFieldStorage creates a Storage with a parquet file loaded into memCache and registered in manifest.
func testFieldStorage(t *testing.T, rows []fullLogRow) (*Storage, string) {
	t.Helper()
	dir := t.TempDir()
	path := writeFullLogParquet(t, dir, "fields_test.parquet", rows)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	m := manifest.New("test", "logs/")
	m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{Key: path, Size: int64(len(data))})

	s := &Storage{
		cfg:        testConfig(),
		manifest:   m,
		registry:   schema.NewRegistry(schema.LogsProfile),
		memCache:   cache.NewLRU(64 * 1024 * 1024),
		sfGroup:    cache.NewGroup(),
		labelIndex: cache.NewLabelIndex(),
		discovery:  discovery.New("", nil, "", "", "9428", 5*time.Second),
	}
	s.memCache.Put(path, data)

	return s, path
}

// --- GetFieldNames tests ---

func TestGetFieldNames_FromLabelIndex(t *testing.T) {
	s := testStorage()
	s.labelIndex.Add("service.name", []string{"api", "web"})
	s.labelIndex.Add("level", []string{"info", "error"})
	s.labelIndex.Add("host.name", nil)

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	fields, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(fields))
	}

	nameSet := make(map[string]bool)
	for _, f := range fields {
		nameSet[f.Value] = true
	}
	for _, expected := range []string{"service.name", "level", "host.name"} {
		if !nameSet[expected] {
			t.Errorf("missing field %q", expected)
		}
	}
}

func TestGetFieldNames_FromParquetFile(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api",
			Stream: `{svc="api"}`, StreamID: "sid1", TraceID: "t1", SpanID: "s1"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, `service.name:="api"`,
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	fields, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) == 0 {
		t.Fatal("expected field names from parquet scan")
	}

	nameSet := make(map[string]bool)
	for _, f := range fields {
		nameSet[f.Value] = true
	}

	// Check mapped names
	if !nameSet["_time"] {
		t.Error("missing _time (mapped from timestamp_unix_nano)")
	}
	if !nameSet["_msg"] {
		t.Error("missing _msg (mapped from body)")
	}
	if !nameSet["level"] {
		t.Error("missing level (mapped from severity_text)")
	}
	if !nameSet["service.name"] {
		t.Error("missing service.name")
	}
}

func TestGetFieldNames_EmptyManifest_WithFilter(t *testing.T) {
	s := testStorage()
	q := mustParseQueryWithTime(t, `service.name:="api"`,
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	fields, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 0 {
		t.Errorf("expected 0 fields from empty manifest, got %d", len(fields))
	}
}

func TestGetFieldNames_CancelledContext(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	s, _ := testFieldStorage(t, rows)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := mustParseQueryWithTime(t, `service.name:="x"`,
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	// Should still work since GetFieldNames reads the first file regardless of context
	// (the context is checked in the loop in GetFieldValues, not GetFieldNames)
	_, err := s.GetFieldNames(ctx, nil, q)
	// Might error or might not, depending on implementation
	_ = err
}

// --- GetFieldValues tests ---

func TestGetFieldValues_FromLabelIndex(t *testing.T) {
	s := testStorage()
	s.labelIndex.Add("service.name", []string{"api", "web", "worker"})

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 3 {
		t.Errorf("expected 3 values, got %d", len(vals))
	}
}

func TestGetFieldValues_FromLabelIndex_WithLimit(t *testing.T) {
	s := testStorage()
	s.labelIndex.Add("level", []string{"info", "warn", "error", "debug", "trace"})

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "level", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 values (limited), got %d", len(vals))
	}
}

func TestGetFieldValues_FromParquetScan(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "msg3", SeverityText: "WARN", ServiceName: "api"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatal(err)
	}

	// After first file scan, label index is populated, so subsequent calls hit cache.
	// But the first scan should return values from parquet.
	if len(vals) == 0 {
		t.Fatal("expected values from parquet scan")
	}

	valSet := make(map[string]bool)
	for _, v := range vals {
		valSet[v.Value] = true
	}
	if !valSet["api"] {
		t.Error("missing value 'api'")
	}
	if !valSet["web"] {
		t.Error("missing value 'web'")
	}
}

func TestGetFieldValues_EmptyManifest(t *testing.T) {
	s := testStorage()
	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 0 {
		t.Errorf("expected 0 values from empty manifest, got %d", len(vals))
	}
}

func TestGetFieldValues_UnknownField(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg", SeverityText: "INFO", ServiceName: "svc"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	// For a completely unknown field that doesn't resolve through the registry,
	// it will try resource.attributes MAP column which doesn't exist in our test files
	vals, err := s.GetFieldValues(context.Background(), nil, q, "completely_unknown_field_xyz", 10)
	if err != nil {
		t.Fatal(err)
	}
	// Should return empty since the column doesn't exist in the parquet file
	if len(vals) != 0 {
		t.Errorf("expected 0 values for unknown field, got %d", len(vals))
	}
}

func TestGetFieldValues_WithFilter(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "msg3", SeverityText: "INFO", ServiceName: "worker"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, `level:="INFO"`,
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatal(err)
	}

	// With filter level:="INFO", only api and worker should match
	valSet := make(map[string]bool)
	for _, v := range vals {
		valSet[v.Value] = true
	}
	if valSet["web"] {
		t.Error("web should not be in results (filtered by level:=INFO)")
	}
}

func TestGetFieldValues_WithLimit(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := make([]fullLogRow, 20)
	for i := 0; i < 20; i++ {
		rows[i] = fullLogRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "msg",
			SeverityText:      "INFO",
			ServiceName:       "svc-unique-" + string(rune('a'+i)),
		}
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) > 5 {
		t.Errorf("expected at most 5 values (limit), got %d", len(vals))
	}
}

func TestGetFieldValues_CancelledContext(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	s, _ := testFieldStorage(t, rows)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := mustParseQueryWithTime(t, `level:="INFO"`,
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	// Context cancellation may or may not cause an error depending on timing.
	// The important thing is it doesn't hang.
	_, _ = s.GetFieldValues(ctx, nil, q, "service.name", 10)
}

// --- GetStreamFieldNames tests ---

func TestGetStreamFieldNames_Logs_ReturnsRegistryFields(t *testing.T) {
	s := testStorage()
	q := mustParseQuery(t, "*")

	fields, err := s.GetStreamFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}

	nameSet := make(map[string]bool)
	for _, f := range fields {
		nameSet[f.Value] = true
	}

	expected := []string{
		"service.name", "k8s.namespace.name", "k8s.pod.name",
		"k8s.deployment.name", "deployment.environment", "cloud.region",
		"host.name", "k8s.node.name", "level",
	}
	for _, e := range expected {
		if !nameSet[e] {
			t.Errorf("missing stream field %q", e)
		}
	}
	if len(fields) != len(expected) {
		t.Errorf("expected %d stream fields for logs, got %d", len(expected), len(fields))
	}
}

func TestGetStreamFieldNames_Traces_ReturnsRegistryFields(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	s := &Storage{
		cfg:        cfg,
		manifest:   manifest.New("test", "traces/"),
		registry:   schema.NewRegistry(schema.TracesProfile),
		memCache:   cache.NewLRU(64 * 1024 * 1024),
		sfGroup:    cache.NewGroup(),
		labelIndex: cache.NewLabelIndex(),
		discovery:  discovery.New("", nil, "", "", "9428", 5*time.Second),
	}

	q := mustParseQuery(t, "*")
	fields, err := s.GetStreamFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}

	nameSet := make(map[string]bool)
	for _, f := range fields {
		nameSet[f.Value] = true
	}

	if !nameSet["resource_attr:service.name"] {
		t.Error("missing resource_attr:service.name in trace stream fields")
	}
	if !nameSet["name"] {
		t.Error("missing name in trace stream fields")
	}
	if len(fields) != 2 {
		t.Errorf("expected 2 stream fields for traces, got %d", len(fields))
	}
}

// --- GetStreamFieldValues tests ---

func TestGetStreamFieldValues_DelegatesToGetFieldValues(t *testing.T) {
	s := testStorage()
	s.labelIndex.Add("service.name", []string{"api", "web"})

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	vals, err := s.GetStreamFieldValues(context.Background(), nil, q, "service.name", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 values, got %d", len(vals))
	}
}

// --- GetStreams tests ---

func TestGetStreams_EmptyManifest(t *testing.T) {
	s := testStorage()
	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	vals, err := s.GetStreams(context.Background(), nil, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 0 {
		t.Errorf("expected 0 streams from empty manifest, got %d", len(vals))
	}
}

func TestGetStreams_FromParquet(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api",
			Stream: `{service.name="api"}`, StreamID: "sid1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web",
			Stream: `{service.name="web"}`, StreamID: "sid2"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "msg3", SeverityText: "INFO", ServiceName: "api",
			Stream: `{service.name="api"}`, StreamID: "sid1"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	streams, err := s.GetStreams(context.Background(), nil, q, 100)
	if err != nil {
		t.Fatal(err)
	}

	if len(streams) == 0 {
		t.Fatal("expected streams from parquet")
	}

	valSet := make(map[string]bool)
	for _, v := range streams {
		valSet[v.Value] = true
	}
	if !valSet[`{service.name="api"}`] {
		t.Error("missing stream {service.name=\"api\"}")
	}
	if !valSet[`{service.name="web"}`] {
		t.Error("missing stream {service.name=\"web\"}")
	}
}

func TestGetStreams_WithLimit(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := make([]fullLogRow, 20)
	for i := 0; i < 20; i++ {
		rows[i] = fullLogRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "msg",
			SeverityText:      "INFO",
			ServiceName:       "svc",
			Stream:            `{service.name="` + string(rune('a'+i)) + `"}`,
			StreamID:          "sid",
		}
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	streams, err := s.GetStreams(context.Background(), nil, q, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) > 5 {
		t.Errorf("expected at most 5 streams (limit), got %d", len(streams))
	}
}

func TestGetStreams_CancelledContext(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg", SeverityText: "INFO", ServiceName: "svc",
			Stream: `{svc="api"}`, StreamID: "sid"},
	}
	s, _ := testFieldStorage(t, rows)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	// Context may or may not produce error depending on timing.
	_, _ = s.GetStreams(ctx, nil, q, 10)
}

func TestGetStreams_WithFilter(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api",
			Stream: `{service.name="api"}`, StreamID: "sid1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web",
			Stream: `{service.name="web"}`, StreamID: "sid2"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, `level:="INFO"`,
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	streams, err := s.GetStreams(context.Background(), nil, q, 100)
	if err != nil {
		t.Fatal(err)
	}

	// With filter level:="INFO", only the api stream should appear
	for _, v := range streams {
		if v.Value == `{service.name="web"}` {
			t.Error("web stream should be filtered out (level is ERROR, not INFO)")
		}
	}
}

// --- GetStreamIDs tests ---

func TestGetStreamIDs_EmptyManifest(t *testing.T) {
	s := testStorage()
	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	ids, err := s.GetStreamIDs(context.Background(), nil, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Errorf("expected 0 stream IDs from empty manifest, got %d", len(ids))
	}
}

func TestGetStreamIDs_FromParquet(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api",
			Stream: `{svc="api"}`, StreamID: "stream-id-001"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web",
			Stream: `{svc="web"}`, StreamID: "stream-id-002"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "msg3", SeverityText: "INFO", ServiceName: "api",
			Stream: `{svc="api"}`, StreamID: "stream-id-001"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	ids, err := s.GetStreamIDs(context.Background(), nil, q, 100)
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) == 0 {
		t.Fatal("expected stream IDs from parquet")
	}

	valSet := make(map[string]bool)
	for _, v := range ids {
		valSet[v.Value] = true
	}
	if !valSet["stream-id-001"] {
		t.Error("missing stream-id-001")
	}
	if !valSet["stream-id-002"] {
		t.Error("missing stream-id-002")
	}
}

func TestGetStreamIDs_WithLimit(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := make([]fullLogRow, 20)
	for i := 0; i < 20; i++ {
		rows[i] = fullLogRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "msg",
			SeverityText:      "INFO",
			ServiceName:       "svc",
			Stream:            `{svc="svc"}`,
			StreamID:          "sid-unique-" + string(rune('a'+i)),
		}
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	ids, err := s.GetStreamIDs(context.Background(), nil, q, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) > 5 {
		t.Errorf("expected at most 5 stream IDs (limit), got %d", len(ids))
	}
}

func TestGetStreamIDs_CancelledContext(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg", SeverityText: "INFO", ServiceName: "svc",
			Stream: `{svc="api"}`, StreamID: "sid1"},
	}
	s, _ := testFieldStorage(t, rows)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	// Context may or may not produce error.
	_, _ = s.GetStreamIDs(ctx, nil, q, 10)
}

func TestGetStreamIDs_WithFilter(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api",
			Stream: `{svc="api"}`, StreamID: "sid-api"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web",
			Stream: `{svc="web"}`, StreamID: "sid-web"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, `level:="ERROR"`,
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	ids, err := s.GetStreamIDs(context.Background(), nil, q, 100)
	if err != nil {
		t.Fatal(err)
	}

	// Only web row has ERROR level
	for _, v := range ids {
		if v.Value == "sid-api" {
			t.Error("sid-api should be filtered out (level is INFO)")
		}
	}
}

// --- collectFilteredValues tests ---

func TestCollectFilteredValues_NilFilter(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web"},
	}
	path := writeFullLogParquet(t, dir, "collect_test.parquet", rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	colNames := columnNames(f.Root())
	svcIdx := findColumnIndex(f.Root(), "service.name")
	if svcIdx < 0 {
		t.Fatal("service.name not found")
	}

	seen := make(map[string]uint64)
	for _, rg := range f.RowGroups() {
		rRows := rg.Rows()
		buf := make([]parquet.Row, 256)
		for {
			n, readErr := rRows.ReadRows(buf)
			if n > 0 {
				collectFilteredValues(buf[:n], colNames, svcIdx, nil, s, seen)
			}
			if readErr != nil {
				break
			}
		}
		_ = rRows.Close()
	}

	if len(seen) != 2 {
		t.Errorf("expected 2 distinct values, got %d: %v", len(seen), seen)
	}
	if seen["api"] == 0 {
		t.Error("missing 'api'")
	}
	if seen["web"] == 0 {
		t.Error("missing 'web'")
	}
}

func TestCollectFilteredValues_WithFilter(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "msg3", SeverityText: "INFO", ServiceName: "worker"},
	}
	path := writeFullLogParquet(t, dir, "filter_test.parquet", rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	colNames := columnNames(f.Root())
	svcIdx := findColumnIndex(f.Root(), "service.name")
	if svcIdx < 0 {
		t.Fatal("service.name not found")
	}

	// Filter for level:="INFO" (severity_text maps to "level")
	filter, parseErr := logstorage.ParseFilter(`level:="INFO"`)
	if parseErr != nil {
		t.Fatal(parseErr)
	}

	seen := make(map[string]uint64)
	for _, rg := range f.RowGroups() {
		rRows := rg.Rows()
		buf := make([]parquet.Row, 256)
		for {
			n, readErr := rRows.ReadRows(buf)
			if n > 0 {
				collectFilteredValues(buf[:n], colNames, svcIdx, filter, s, seen)
			}
			if readErr != nil {
				break
			}
		}
		_ = rRows.Close()
	}

	// Only INFO rows should match: api and worker
	if seen["web"] > 0 {
		t.Error("web should be filtered out (ERROR level)")
	}
}

func TestCollectFilteredValues_EmptyRows(t *testing.T) {
	seen := make(map[string]uint64)
	collectFilteredValues(nil, nil, 0, nil, nil, seen)
	if len(seen) != 0 {
		t.Errorf("expected empty map for nil rows, got %v", seen)
	}
}

func TestCollectFilteredValues_OutOfBoundsColumn(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeFullLogParquet(t, dir, "oob_test.parquet", rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	colNames := columnNames(f.Root())

	seen := make(map[string]uint64)
	for _, rg := range f.RowGroups() {
		rRows := rg.Rows()
		buf := make([]parquet.Row, 256)
		for {
			n, readErr := rRows.ReadRows(buf)
			if n > 0 {
				// targetColIdx way out of bounds
				collectFilteredValues(buf[:n], colNames, 999, nil, s, seen)
			}
			if readErr != nil {
				break
			}
		}
		_ = rRows.Close()
	}

	if len(seen) != 0 {
		t.Errorf("expected empty for out-of-bounds target col, got %v", seen)
	}
}

// --- parquetRowToFields tests ---

func TestParquetRowToFields_Basic(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api",
			Stream: `{svc="api"}`, StreamID: "sid1", TraceID: "t1", SpanID: "s1"},
	}
	path := writeFullLogParquet(t, dir, "fields_basic.parquet", rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	colNames := columnNames(f.Root())

	tsColIdx := -1
	for i, name := range colNames {
		if name == "timestamp_unix_nano" {
			tsColIdx = i
			break
		}
	}

	if rgs := f.RowGroups(); len(rgs) > 0 {
		rg := rgs[0]
		rRows := rg.Rows()
		buf := make([]parquet.Row, 1)
		n, _ := rRows.ReadRows(buf)
		if n == 0 {
			t.Fatal("expected at least 1 row")
		}

		fields := parquetRowToFields(buf[0], colNames, tsColIdx, s)
		if len(fields) == 0 {
			t.Fatal("expected non-empty fields")
		}

		fieldMap := make(map[string]string)
		for _, fld := range fields {
			fieldMap[fld.Name] = fld.Value
		}

		// Check that the timestamp column is formatted as RFC3339Nano
		if tsVal, ok := fieldMap["_time"]; ok {
			// It should be a valid RFC3339Nano timestamp
			if _, parseErr := time.Parse(time.RFC3339Nano, tsVal); parseErr != nil {
				t.Errorf("_time value %q is not valid RFC3339Nano: %v", tsVal, parseErr)
			}
		} else {
			t.Error("missing _time field")
		}

		// Check that body is mapped to _msg
		if msgVal, ok := fieldMap["_msg"]; ok {
			if msgVal != "hello" {
				t.Errorf("_msg = %q, want %q", msgVal, "hello")
			}
		} else {
			t.Error("missing _msg field")
		}

		// Check service.name
		if svcVal, ok := fieldMap["service.name"]; ok {
			if svcVal != "api" {
				t.Errorf("service.name = %q, want %q", svcVal, "api")
			}
		} else {
			t.Error("missing service.name field")
		}

		_ = rRows.Close()
	}
}

func TestParquetRowToFields_NilStorage(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeFullLogParquet(t, dir, "nil_storage.parquet", rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	colNames := columnNames(f.Root())

	if rgs := f.RowGroups(); len(rgs) > 0 {
		rg := rgs[0]
		rRows := rg.Rows()
		buf := make([]parquet.Row, 1)
		n, _ := rRows.ReadRows(buf)
		if n == 0 {
			t.Fatal("expected at least 1 row")
		}

		// Pass nil for storage - should use raw column names
		fields := parquetRowToFields(buf[0], colNames, -1, nil)
		if len(fields) == 0 {
			t.Fatal("expected non-empty fields")
		}

		fieldMap := make(map[string]string)
		for _, fld := range fields {
			fieldMap[fld.Name] = fld.Value
		}

		// Without storage, names should be raw parquet names
		if _, ok := fieldMap["timestamp_unix_nano"]; !ok {
			t.Error("expected raw parquet name 'timestamp_unix_nano' when storage is nil")
		}
		if _, ok := fieldMap["body"]; !ok {
			t.Error("expected raw parquet name 'body' when storage is nil")
		}

		_ = rRows.Close()
	}
}

func TestParquetRowToFields_EmptyRow(t *testing.T) {
	fields := parquetRowToFields(nil, nil, -1, nil)
	if len(fields) != 0 {
		t.Errorf("expected empty fields for nil row, got %d", len(fields))
	}
}

func TestParquetRowToFields_ShorterRowThanColumns(t *testing.T) {
	// Create a row with fewer values than columns
	colNames := []string{"col1", "col2", "col3"}
	row := parquet.Row{parquet.ValueOf("val1")}

	fields := parquetRowToFields(row, colNames, -1, nil)
	if len(fields) != 1 {
		t.Errorf("expected 1 field (row shorter than colNames), got %d", len(fields))
	}
}

// --- Multiple files in manifest ---

func TestGetFieldValues_MultipleFiles(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)

	// File 1
	rows1 := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api"},
	}
	path1 := writeFullLogParquet(t, dir, "file1.parquet", rows1)
	data1, _ := os.ReadFile(path1)

	// File 2
	rows2 := []fullLogRow{
		{TimestampUnixNano: now.Add(time.Minute).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web"},
	}
	path2 := writeFullLogParquet(t, dir, "file2.parquet", rows2)
	data2, _ := os.ReadFile(path2)

	m := manifest.New("test", "logs/")
	m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{Key: path1, Size: int64(len(data1))})
	m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{Key: path2, Size: int64(len(data2))})

	s := &Storage{
		cfg:        testConfig(),
		manifest:   m,
		registry:   schema.NewRegistry(schema.LogsProfile),
		memCache:   cache.NewLRU(64 * 1024 * 1024),
		sfGroup:    cache.NewGroup(),
		labelIndex: cache.NewLabelIndex(),
		discovery:  discovery.New("", nil, "", "", "9428", 5*time.Second),
	}
	s.memCache.Put(path1, data1)
	s.memCache.Put(path2, data2)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatal(err)
	}

	valSet := make(map[string]bool)
	for _, v := range vals {
		valSet[v.Value] = true
	}
	if !valSet["api"] {
		t.Error("missing 'api' from file1")
	}
	if !valSet["web"] {
		t.Error("missing 'web' from file2")
	}
}

// --- GetFieldValues with zero limit ---

func TestGetFieldValues_ZeroLimit(t *testing.T) {
	s := testStorage()
	s.labelIndex.Add("service.name", []string{"a", "b", "c"})

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	// limit=0 (no limit — what a Grafana dropdown sends) must serve from the in-RAM
	// label index, NOT fall through to a Parquet scan. The index is self-bounded, so
	// serving all its values is correct. (Previously the fast-path was gated on
	// limit > 0, which sent no-limit requests to a full scan — the dropdown slowness.)
	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 3 {
		t.Errorf("limit=0 should serve all 3 index values (a,b,c), got %d", len(vals))
	}
}

// --- GetFieldValues for field not in label index ---

func TestGetFieldValues_FieldNotInLabelIndex(t *testing.T) {
	s := testStorage()
	s.labelIndex.Add("service.name", []string{"api"})
	// "level" is NOT in label index

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "level", 10)
	if err != nil {
		t.Fatal(err)
	}
	// Label index has no values for "level", so it falls through to parquet scan
	// Empty manifest returns nil
	if len(vals) != 0 {
		t.Errorf("expected 0 values, got %d", len(vals))
	}
}

// --- Edge cases ported from traces ---

func TestCollectFilteredValues_EmptyValues_NotIncluded(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "alpha",
			Stream: `{svc="alpha"}`, StreamID: "id1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "WARN", ServiceName: "",
			Stream: `{svc=""}`, StreamID: "id2"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		now.Add(-time.Hour).UnixNano(),
		now.Add(time.Hour).UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vals {
		if v.Value == "" {
			t.Error("empty string values should not be included in results")
		}
	}
}

func TestGetFieldValues_LimitRespected(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := make([]fullLogRow, 0, 50)
	for i := 0; i < 50; i++ {
		rows = append(rows, fullLogRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              fmt.Sprintf("msg%d", i),
			SeverityText:      "INFO",
			ServiceName:       fmt.Sprintf("svc-%d", i),
			Stream:            fmt.Sprintf(`{svc="svc-%d"}`, i),
			StreamID:          fmt.Sprintf("sid-%d", i),
		})
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		now.Add(-time.Hour).UnixNano(),
		now.Add(time.Hour).UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 5)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(vals)) > 5 {
		t.Errorf("expected at most 5 values, got %d", len(vals))
	}
}

func TestGetStreams_DuplicatesDeduped(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api",
			Stream: `{service.name="api"}`, StreamID: "s1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "WARN", ServiceName: "api",
			Stream: `{service.name="api"}`, StreamID: "s1"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "msg3", SeverityText: "ERROR", ServiceName: "api",
			Stream: `{service.name="api"}`, StreamID: "s1"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		now.Add(-time.Hour).UnixNano(),
		now.Add(time.Hour).UnixNano(),
	)

	streams, err := s.GetStreams(context.Background(), nil, q, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 {
		t.Errorf("expected 1 unique stream, got %d: %v", len(streams), streams)
	}
	if streams[0].Hits != 3 {
		t.Errorf("expected 3 hits for deduplicated stream, got %d", streams[0].Hits)
	}
}

func TestGetStreamIDs_LimitZero_ReturnsAll(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO", ServiceName: "api",
			Stream: `{svc="api"}`, StreamID: "sid-1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "ERROR", ServiceName: "web",
			Stream: `{svc="web"}`, StreamID: "sid-2"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		now.Add(-time.Hour).UnixNano(),
		now.Add(time.Hour).UnixNano(),
	)

	ids, err := s.GetStreamIDs(context.Background(), nil, q, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("limit=0 should return all stream IDs, got %d", len(ids))
	}
}

func TestGetFieldNames_NoDuplicatesFromPromotedAndMap(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg", SeverityText: "INFO", ServiceName: "api",
			Stream: `{svc="api"}`, StreamID: "s1"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		now.Add(-time.Hour).UnixNano(),
		now.Add(time.Hour).UnixNano(),
	)

	names, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int)
	for _, n := range names {
		seen[n.Value]++
	}
	for name, count := range seen {
		if count > 1 {
			t.Errorf("field %q appears %d times (should be unique)", name, count)
		}
	}
}
