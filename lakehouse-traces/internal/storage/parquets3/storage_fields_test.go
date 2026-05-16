package parquets3

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type fullTraceRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	SeverityText      string `parquet:"severity_text"`
	ServiceName       string `parquet:"service.name"`
	Stream            string `parquet:"_stream"`
	StreamID          string `parquet:"_stream_id"`
	TraceID           string `parquet:"trace_id"`
	SpanID            string `parquet:"span_id"`
	SpanName          string `parquet:"span_name"`
	Duration          int64  `parquet:"duration"`
}

func writeFullTraceParquet(t *testing.T, dir, name string, rows []fullTraceRow) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[fullTraceRow](f, parquet.Compression(&parquet.Zstd))
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

func testFieldStorageTraces(t *testing.T, rows []fullTraceRow) *Storage {
	t.Helper()
	dir := t.TempDir()
	path := writeFullTraceParquet(t, dir, "fields_test.parquet", rows)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	m := manifest.New("test", "traces/")
	m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{Key: path, Size: int64(len(data))})

	cfg := config.Default()
	cfg.Mode = config.ModeTraces

	s := &Storage{
		cfg:        cfg,
		manifest:   m,
		registry:   schema.NewRegistry(schema.TracesProfile),
		memCache:   cache.NewLRU(64 * 1024 * 1024),
		sfGroup:    cache.NewGroup(),
		labelIndex: cache.NewLabelIndex(),
		discovery:  discovery.New("", nil, "", "", "9428", 5*time.Second),
	}
	s.memCache.Put(path, data)

	return s
}

func TestGetFieldNames_FromLabelIndex(t *testing.T) {
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
	s.labelIndex.Add("service.name", []string{"api", "web"})
	s.labelIndex.Add("span_name", []string{"GET /", "POST /api"})
	s.labelIndex.Add("trace_id", nil)

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
	for _, expected := range []string{"service.name", "span_name", "trace_id"} {
		if !nameSet[expected] {
			t.Errorf("missing field %q", expected)
		}
	}
}

func TestGetFieldNames_FromParquetFile(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullTraceRow{
		{TimestampUnixNano: now.UnixNano(), Body: "span", SeverityText: "INFO", ServiceName: "api",
			Stream: `{svc="api"}`, StreamID: "sid1", TraceID: "t1", SpanID: "s1", SpanName: "GET /", Duration: 150000000},
	}
	s := testFieldStorageTraces(t, rows)

	q := mustParseQueryWithTime(t, `resource_attr:service.name:="api"`,
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

	// TracesProfile maps: service.name parquet → resource_attr:service.name internal
	if !nameSet["resource_attr:service.name"] {
		t.Errorf("missing resource_attr:service.name, got fields: %v", nameSet)
	}
	if !nameSet["trace_id"] {
		t.Error("missing trace_id")
	}
}

func TestGetFieldNames_EmptyManifest_WithFilter(t *testing.T) {
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

func TestGetFieldValues_FromLabelIndex(t *testing.T) {
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
	s.labelIndex.Add("span_name", []string{"GET /", "POST /", "PUT /", "DELETE /", "PATCH /"})

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "span_name", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 values (limited), got %d", len(vals))
	}
}

func TestGetFieldValues_FromParquetScan(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullTraceRow{
		{TimestampUnixNano: now.UnixNano(), Body: "span1", ServiceName: "api", SpanName: "GET /", TraceID: "t1", SpanID: "s1", Duration: 100000000},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "span2", ServiceName: "web", SpanName: "POST /api", TraceID: "t2", SpanID: "s2", Duration: 200000000},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "span3", ServiceName: "api", SpanName: "GET /health", TraceID: "t3", SpanID: "s3", Duration: 50000000},
	}
	s := testFieldStorageTraces(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	// Use internal name — TracesProfile maps service.name parquet → resource_attr:service.name
	vals, err := s.GetFieldValues(context.Background(), nil, q, "resource_attr:service.name", 100)
	if err != nil {
		t.Fatal(err)
	}
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
	rows := []fullTraceRow{
		{TimestampUnixNano: now.UnixNano(), Body: "span", ServiceName: "svc", SpanName: "op", TraceID: "t1", SpanID: "s1"},
	}
	s := testFieldStorageTraces(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "completely_unknown_xyz", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 0 {
		t.Errorf("expected 0 values for unknown field, got %d", len(vals))
	}
}

func TestGetStreams_FromParquetScan(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullTraceRow{
		{TimestampUnixNano: now.UnixNano(), Body: "span1", ServiceName: "api", Stream: `{svc="api"}`, StreamID: "s1", TraceID: "t1", SpanID: "sp1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "span2", ServiceName: "web", Stream: `{svc="web"}`, StreamID: "s2", TraceID: "t2", SpanID: "sp2"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "span3", ServiceName: "api", Stream: `{svc="api"}`, StreamID: "s1", TraceID: "t3", SpanID: "sp3"},
	}
	s := testFieldStorageTraces(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	streams, err := s.GetStreams(context.Background(), nil, q, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) == 0 {
		t.Fatal("expected streams from parquet scan")
	}

	streamSet := make(map[string]bool)
	for _, s := range streams {
		streamSet[s.Value] = true
	}
	if !streamSet[`{svc="api"}`] {
		t.Error("missing stream {svc=\"api\"}")
	}
	if !streamSet[`{svc="web"}`] {
		t.Error("missing stream {svc=\"web\"}")
	}
}

func TestGetStreams_EmptyManifest(t *testing.T) {
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
	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	streams, err := s.GetStreams(context.Background(), nil, q, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 0 {
		t.Errorf("expected 0 streams, got %d", len(streams))
	}
}

func TestGetStreams_WithLimit(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullTraceRow{
		{TimestampUnixNano: now.UnixNano(), Body: "s1", ServiceName: "a", Stream: `{svc="a"}`, StreamID: "id1", TraceID: "t1", SpanID: "sp1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "s2", ServiceName: "b", Stream: `{svc="b"}`, StreamID: "id2", TraceID: "t2", SpanID: "sp2"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "s3", ServiceName: "c", Stream: `{svc="c"}`, StreamID: "id3", TraceID: "t3", SpanID: "sp3"},
	}
	s := testFieldStorageTraces(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	streams, err := s.GetStreams(context.Background(), nil, q, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) > 1 {
		t.Errorf("expected at most 1 stream with limit=1, got %d", len(streams))
	}
}

func TestGetStreamIDs_FromParquetScan(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullTraceRow{
		{TimestampUnixNano: now.UnixNano(), Body: "s1", ServiceName: "api", Stream: `{svc="api"}`, StreamID: "stream-001", TraceID: "t1", SpanID: "sp1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "s2", ServiceName: "web", Stream: `{svc="web"}`, StreamID: "stream-002", TraceID: "t2", SpanID: "sp2"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "s3", ServiceName: "api", Stream: `{svc="api"}`, StreamID: "stream-001", TraceID: "t3", SpanID: "sp3"},
	}
	s := testFieldStorageTraces(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	ids, err := s.GetStreamIDs(context.Background(), nil, q, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) == 0 {
		t.Fatal("expected stream IDs from parquet scan")
	}

	idSet := make(map[string]bool)
	for _, id := range ids {
		idSet[id.Value] = true
	}
	if !idSet["stream-001"] {
		t.Error("missing stream-001")
	}
	if !idSet["stream-002"] {
		t.Error("missing stream-002")
	}
}

func TestGetStreamIDs_CancelledContext(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []fullTraceRow{
		{TimestampUnixNano: now.UnixNano(), Body: "s1", ServiceName: "api", Stream: `{}`, StreamID: "id1", TraceID: "t1", SpanID: "sp1"},
	}
	s := testFieldStorageTraces(t, rows)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	_, err := s.GetStreamIDs(ctx, nil, q, 100)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestGetStreamFieldValues_DelegatesToGetFieldValues(t *testing.T) {
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
	s.labelIndex.Add("service.name", []string{"api"})

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	vals, err := s.GetStreamFieldValues(context.Background(), nil, q, "service.name", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0].Value != "api" {
		t.Errorf("expected [{api 1}], got %v", vals)
	}
}

func TestGetStreamFieldNames_ReturnsRegisteredStreamFields(t *testing.T) {
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

	q := mustParseQueryWithTime(t, "*",
		time.Now().Add(-time.Hour).UnixNano(),
		time.Now().UnixNano(),
	)

	fields, err := s.GetStreamFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) == 0 {
		t.Fatal("expected stream field names from registry")
	}
}
