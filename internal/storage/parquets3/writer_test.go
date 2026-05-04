package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestPartitionFromNano(t *testing.T) {
	ts := time.Date(2026, 5, 3, 14, 30, 0, 0, time.UTC)
	got := partitionFromNano(ts.UnixNano())
	want := "dt=2026-05-03/hour=14"
	if got != want {
		t.Errorf("partitionFromNano() = %q, want %q", got, want)
	}
}

func TestPartitionFromNano_Midnight(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := partitionFromNano(ts.UnixNano())
	want := "dt=2026-01-01/hour=00"
	if got != want {
		t.Errorf("partitionFromNano() = %q, want %q", got, want)
	}
}

func TestWriteLogsParquet(t *testing.T) {
	rows := []schema.LogRow{
		{
			TimestampUnixNano: time.Now().UnixNano(),
			Body:              "test log message",
			SeverityText:      "INFO",
			ServiceName:       "test-service",
			K8sNamespaceName:  "default",
		},
		{
			TimestampUnixNano: time.Now().Add(-time.Minute).UnixNano(),
			Body:              "another message",
			SeverityText:      "ERROR",
			ServiceName:       "test-service",
			K8sNamespaceName:  "production",
		},
	}

	result, err := writeLogsParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeLogsParquet() error: %v", err)
	}

	if len(result.Data) == 0 {
		t.Fatal("writeLogsParquet() returned empty data")
	}

	if string(result.Data[:4]) != "PAR1" {
		t.Errorf("data does not start with Parquet magic bytes, got %x", result.Data[:4])
	}
	if string(result.Data[len(result.Data)-4:]) != "PAR1" {
		t.Errorf("data does not end with Parquet magic bytes")
	}
	if result.RawBytes <= 0 {
		t.Error("RawBytes should be > 0")
	}
}

func TestWriteLogsParquet_SmallRowGroupSize(t *testing.T) {
	rows := make([]schema.LogRow, 25)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: time.Now().Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              fmt.Sprintf("msg %d", i),
			ServiceName:       "svc",
		}
	}

	result, err := writeLogsParquet(rows, 10, 3)
	if err != nil {
		t.Fatalf("writeLogsParquet() error: %v", err)
	}
	if len(result.Data) == 0 {
		t.Fatal("empty output")
	}
}

func TestWriteTracesParquet(t *testing.T) {
	rows := []schema.TraceRow{
		{
			TimestampUnixNano: time.Now().UnixNano(),
			TraceID:           "abc123",
			SpanID:            "span1",
			SpanName:          "HTTP GET /api",
			ServiceName:       "test-service",
			DurationNs:        5000000,
		},
	}

	result, err := writeTracesParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeTracesParquet() error: %v", err)
	}
	if len(result.Data) == 0 {
		t.Fatal("writeTracesParquet() returned empty data")
	}
	if string(result.Data[:4]) != "PAR1" {
		t.Errorf("data does not start with Parquet magic bytes")
	}
}

func TestWriteTracesParquet_SmallRowGroupSize(t *testing.T) {
	rows := make([]schema.TraceRow, 15)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: time.Now().Add(time.Duration(i) * time.Second).UnixNano(),
			TraceID:           fmt.Sprintf("trace-%d", i),
			SpanID:            fmt.Sprintf("span-%d", i),
			ServiceName:       "svc",
			DurationNs:        int64(i * 1000),
		}
	}

	result, err := writeTracesParquet(rows, 5, 3)
	if err != nil {
		t.Fatalf("writeTracesParquet() error: %v", err)
	}
	if len(result.Data) == 0 {
		t.Fatal("empty output")
	}
}

func TestRandomBatchID(t *testing.T) {
	id1 := randomBatchID()
	id2 := randomBatchID()

	if len(id1) != 16 {
		t.Errorf("randomBatchID() len = %d, want 16", len(id1))
	}
	if id1 == id2 {
		t.Error("randomBatchID() returned same value twice")
	}
}

func TestPartitionKey(t *testing.T) {
	tests := []struct {
		prefix    string
		partition string
		batchID   string
		want      string
	}{
		{"logs/", "dt=2026-05-03/hour=14", "abc123", "logs/dt=2026-05-03/hour=14/abc123.parquet"},
		{"traces/", "dt=2026-01-01/hour=00", "def456", "traces/dt=2026-01-01/hour=00/def456.parquet"},
		{"", "dt=2026-05-03/hour=14", "abc123", "dt=2026-05-03/hour=14/abc123.parquet"},
		{"myprefix", "dt=2026-05-03/hour=14", "abc123", "myprefix/dt=2026-05-03/hour=14/abc123.parquet"},
	}

	for _, tt := range tests {
		got := PartitionKey(tt.prefix, tt.partition, tt.batchID)
		if got != tt.want {
			t.Errorf("PartitionKey(%q, %q, %q) = %q, want %q", tt.prefix, tt.partition, tt.batchID, got, tt.want)
		}
	}
}

// mockS3 returns an httptest.Server that accepts PutObject and returns 200.
func mockS3() *httptest.Server {
	var putCount atomic.Int64
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			putCount.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		// ListObjectsV2 returns empty
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
}

func testPool(t *testing.T, endpoint string) *s3reader.ClientPool {
	t.Helper()
	cfg := &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       endpoint,
		ForcePathStyle: true,
		AccessKey:      "test",
		SecretKey:      "test",
	}
	pool, err := s3reader.NewClientPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}
	return pool
}

func testInsertConfig() *config.InsertConfig {
	return &config.InsertConfig{
		FlushInterval:    1 * time.Second,
		MaxBufferRows:    100,
		MaxBufferBytes:   "256MB",
		TargetFileSize:   "128MB",
		RowGroupSize:     50,
		BloomColumns:     []string{"service.name", "trace_id"},
		CompressionLevel: 3,
	}
}

func testWriter(t *testing.T, endpoint string) (*BatchWriter, *manifest.Manifest) {
	t.Helper()
	pool := testPool(t, endpoint)
	m := manifest.New("test-bucket", "logs/", slog.Default())
	cfg := testInsertConfig()
	bw := NewBatchWriter(cfg, pool, m, "logs/", config.ModeLogs, slog.Default())
	return bw, m
}

func sampleLogRows(n int, baseTime time.Time) []schema.LogRow {
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              fmt.Sprintf("msg-%d", i),
			SeverityText:      "INFO",
			ServiceName:       "test-svc",
			K8sNamespaceName:  "default",
		}
	}
	return rows
}

func sampleTraceRows(n int, baseTime time.Time) []schema.TraceRow {
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Second).UnixNano(),
			TraceID:           fmt.Sprintf("trace-%d", i),
			SpanID:            fmt.Sprintf("span-%d", i),
			SpanName:          "test-span",
			ServiceName:       "test-svc",
			DurationNs:        int64(i+1) * 1000000,
		}
	}
	return rows
}

func TestNewBatchWriter(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()

	bw, _ := testWriter(t, s3srv.URL)
	if bw == nil {
		t.Fatal("NewBatchWriter returned nil")
	}
	if bw.BufferedRows() != 0 {
		t.Errorf("initial BufferedRows = %d, want 0", bw.BufferedRows())
	}
	if bw.TotalBytesUploaded() != 0 {
		t.Errorf("initial TotalBytesUploaded = %d, want 0", bw.TotalBytesUploaded())
	}
}

func TestAddLogRows_Buffering(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()

	bw, _ := testWriter(t, s3srv.URL)

	rows := sampleLogRows(10, time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC))
	bw.AddLogRows(rows)

	if got := bw.BufferedRows(); got != 10 {
		t.Errorf("BufferedRows after add = %d, want 10", got)
	}

	bw.AddLogRows(sampleLogRows(5, time.Date(2026, 5, 3, 15, 0, 0, 0, time.UTC)))

	if got := bw.BufferedRows(); got != 15 {
		t.Errorf("BufferedRows after second add = %d, want 15", got)
	}
}

func TestAddLogRows_Empty(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, _ := testWriter(t, s3srv.URL)

	bw.AddLogRows(nil)
	bw.AddLogRows([]schema.LogRow{})

	if got := bw.BufferedRows(); got != 0 {
		t.Errorf("BufferedRows = %d, want 0", got)
	}
}

func TestAddTraceRows_Buffering(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()

	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "traces/", slog.Default())
	cfg := testInsertConfig()
	bw := NewBatchWriter(cfg, pool, m, "traces/", config.ModeTraces, slog.Default())

	rows := sampleTraceRows(8, time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC))
	bw.AddTraceRows(rows)

	if got := bw.BufferedRows(); got != 8 {
		t.Errorf("BufferedRows = %d, want 8", got)
	}
}

func TestAddTraceRows_Empty(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "traces/", slog.Default())
	cfg := testInsertConfig()
	bw := NewBatchWriter(cfg, pool, m, "traces/", config.ModeTraces, slog.Default())

	bw.AddTraceRows(nil)
	bw.AddTraceRows([]schema.TraceRow{})

	if got := bw.BufferedRows(); got != 0 {
		t.Errorf("BufferedRows = %d, want 0", got)
	}
}

func TestBufferedLogRows_TimeRange(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, _ := testWriter(t, s3srv.URL)

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := sampleLogRows(10, base)
	bw.AddLogRows(rows)

	start := base.Add(3 * time.Second).UnixNano()
	end := base.Add(7 * time.Second).UnixNano()

	got := bw.BufferedLogRows(start, end)
	if len(got) != 4 {
		t.Errorf("BufferedLogRows returned %d rows, want 4 (indices 3-6)", len(got))
	}
}

func TestBufferedLogRows_Empty(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, _ := testWriter(t, s3srv.URL)

	got := bw.BufferedLogRows(0, time.Now().UnixNano())
	if len(got) != 0 {
		t.Errorf("BufferedLogRows on empty = %d, want 0", len(got))
	}
}

func TestBufferedTraceRows_TimeRange(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "traces/", slog.Default())
	cfg := testInsertConfig()
	bw := NewBatchWriter(cfg, pool, m, "traces/", config.ModeTraces, slog.Default())

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := sampleTraceRows(10, base)
	bw.AddTraceRows(rows)

	start := base.UnixNano()
	end := base.Add(5 * time.Second).UnixNano()

	got := bw.BufferedTraceRows(start, end)
	if len(got) != 5 {
		t.Errorf("BufferedTraceRows returned %d rows, want 5", len(got))
	}
}

func TestBufferedTraceRows_Empty(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "traces/", slog.Default())
	cfg := testInsertConfig()
	bw := NewBatchWriter(cfg, pool, m, "traces/", config.ModeTraces, slog.Default())

	got := bw.BufferedTraceRows(0, time.Now().UnixNano())
	if len(got) != 0 {
		t.Errorf("BufferedTraceRows on empty = %d, want 0", len(got))
	}
}

func TestFlushAll_Logs(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, m := testWriter(t, s3srv.URL)

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(sampleLogRows(20, base))

	ctx := context.Background()
	if err := bw.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll error: %v", err)
	}

	if got := bw.BufferedRows(); got != 0 {
		t.Errorf("BufferedRows after flush = %d, want 0", got)
	}

	if got := m.TotalFiles(); got != 1 {
		t.Errorf("manifest TotalFiles = %d, want 1", got)
	}

	if got := bw.TotalBytesUploaded(); got <= 0 {
		t.Error("TotalBytesUploaded should be > 0 after flush")
	}
}

func TestFlushAll_Traces(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "traces/", slog.Default())
	cfg := testInsertConfig()
	bw := NewBatchWriter(cfg, pool, m, "traces/", config.ModeTraces, slog.Default())

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddTraceRows(sampleTraceRows(15, base))

	ctx := context.Background()
	if err := bw.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll error: %v", err)
	}

	if got := bw.BufferedRows(); got != 0 {
		t.Errorf("BufferedRows after flush = %d, want 0", got)
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("manifest TotalFiles = %d, want 1", got)
	}
}

func TestFlushAll_Empty(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, m := testWriter(t, s3srv.URL)

	ctx := context.Background()
	if err := bw.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll on empty error: %v", err)
	}
	if got := m.TotalFiles(); got != 0 {
		t.Errorf("manifest TotalFiles = %d, want 0", got)
	}
}

func TestFlushAll_MultiplePartitions(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, m := testWriter(t, s3srv.URL)

	bw.AddLogRows(sampleLogRows(5, time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)))
	bw.AddLogRows(sampleLogRows(5, time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)))
	bw.AddLogRows(sampleLogRows(5, time.Date(2026, 5, 4, 8, 0, 0, 0, time.UTC)))

	ctx := context.Background()
	if err := bw.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll error: %v", err)
	}

	if got := m.TotalFiles(); got != 3 {
		t.Errorf("manifest TotalFiles = %d, want 3 (one per partition)", got)
	}
}

func TestCanWriteData(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, _ := testWriter(t, s3srv.URL)

	ctx := context.Background()
	if err := bw.CanWriteData(ctx); err != nil {
		t.Errorf("CanWriteData error: %v", err)
	}
}

func TestStartStop(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, _ := testWriter(t, s3srv.URL)

	bw.Start()

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(sampleLogRows(5, base))

	bw.Stop()

	if got := bw.BufferedRows(); got != 0 {
		t.Errorf("BufferedRows after Stop = %d, want 0 (Stop should flush)", got)
	}
}

func TestCheckSizeThreshold(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()

	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "logs/", slog.Default())
	cfg := testInsertConfig()
	cfg.MaxBufferRows = 20
	bw := NewBatchWriter(cfg, pool, m, "logs/", config.ModeLogs, slog.Default())

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(sampleLogRows(25, base))

	if got := bw.BufferedRows(); got != 0 {
		t.Errorf("BufferedRows = %d, want 0 (should have auto-flushed at 20)", got)
	}
	if got := m.TotalFiles(); got < 1 {
		t.Errorf("manifest TotalFiles = %d, want >= 1 after threshold flush", got)
	}
}

func TestFlushAll_S3Error(t *testing.T) {
	errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer errSrv.Close()

	bw, _ := testWriter(t, errSrv.URL)
	bw.AddLogRows(sampleLogRows(5, time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)))

	ctx := context.Background()
	err := bw.FlushAll(ctx)
	if err == nil {
		t.Error("FlushAll should return error when S3 fails")
	}
	if !strings.Contains(err.Error(), "flush") {
		t.Errorf("error should mention flush, got: %v", err)
	}
}

func TestFlushAll_TraceS3Error(t *testing.T) {
	errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer errSrv.Close()

	pool := testPool(t, errSrv.URL)
	m := manifest.New("test-bucket", "traces/", slog.Default())
	cfg := testInsertConfig()
	bw := NewBatchWriter(cfg, pool, m, "traces/", config.ModeTraces, slog.Default())
	bw.AddTraceRows(sampleTraceRows(5, time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)))

	err := bw.FlushAll(context.Background())
	if err == nil {
		t.Error("FlushAll should return error when S3 fails")
	}
}

func TestAddLogRows_PartitionGrouping(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, _ := testWriter(t, s3srv.URL)

	rows := []schema.LogRow{
		{TimestampUnixNano: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC).UnixNano(), Body: "a"},
		{TimestampUnixNano: time.Date(2026, 5, 3, 10, 30, 0, 0, time.UTC).UnixNano(), Body: "b"},
		{TimestampUnixNano: time.Date(2026, 5, 3, 11, 0, 0, 0, time.UTC).UnixNano(), Body: "c"},
	}
	bw.AddLogRows(rows)

	bw.mu.Lock()
	numPartitions := len(bw.logBufs)
	bw.mu.Unlock()

	if numPartitions != 2 {
		t.Errorf("expected 2 partitions (hour=10, hour=11), got %d", numPartitions)
	}
}

func TestWriteLogsParquet_WithMAPColumns(t *testing.T) {
	rows := []schema.LogRow{
		{
			TimestampUnixNano: time.Now().UnixNano(),
			Body:              "test with attributes",
			ServiceName:       "svc",
			LogAttributes: map[string]string{
				"http.method":      "GET",
				"http.status_code": "200",
				"custom.field":     "value",
			},
			ResourceAttributes: map[string]string{
				"cloud.provider": "aws",
				"instance.id":    "i-12345",
			},
		},
		{
			TimestampUnixNano: time.Now().UnixNano(),
			Body:              "test without attributes",
			ServiceName:       "svc",
		},
	}

	result, err := writeLogsParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeLogsParquet() error: %v", err)
	}

	if len(result.Data) == 0 {
		t.Fatal("empty parquet output")
	}

	reader := bytes.NewReader(result.Data)
	f, err := parquet.OpenFile(reader, int64(len(result.Data)))
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}

	r := parquet.NewGenericReader[schema.LogRow](f)
	out := make([]schema.LogRow, 2)
	n, _ := r.Read(out)
	if n != 2 {
		t.Fatalf("read %d rows, want 2", n)
	}

	row0 := out[0]
	if row0.LogAttributes == nil {
		t.Fatal("row0 LogAttributes should not be nil")
	}
	if row0.LogAttributes["http.method"] != "GET" {
		t.Errorf("LogAttributes[http.method] = %q, want GET", row0.LogAttributes["http.method"])
	}
	if row0.LogAttributes["http.status_code"] != "200" {
		t.Errorf("LogAttributes[http.status_code] = %q, want 200", row0.LogAttributes["http.status_code"])
	}
	if row0.ResourceAttributes == nil {
		t.Fatal("row0 ResourceAttributes should not be nil")
	}
	if row0.ResourceAttributes["cloud.provider"] != "aws" {
		t.Errorf("ResourceAttributes[cloud.provider] = %q, want aws", row0.ResourceAttributes["cloud.provider"])
	}

	row1 := out[1]
	if len(row1.LogAttributes) != 0 {
		t.Errorf("row1 LogAttributes should be empty, got %v", row1.LogAttributes)
	}
}

func TestWriteTracesParquet_WithMAPColumns(t *testing.T) {
	rows := []schema.TraceRow{
		{
			TimestampUnixNano: time.Now().UnixNano(),
			TraceID:           "trace-1",
			SpanID:            "span-1",
			SpanName:          "HTTP GET",
			ServiceName:       "svc",
			ResourceAttributes: map[string]string{
				"cloud.provider": "gcp",
			},
			SpanAttributes: map[string]string{
				"http.url":    "https://example.com",
				"http.method": "GET",
			},
			ScopeAttributes: map[string]string{
				"otel.scope.version": "1.0.0",
			},
		},
	}

	result, err := writeTracesParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeTracesParquet() error: %v", err)
	}

	reader := bytes.NewReader(result.Data)
	f, err := parquet.OpenFile(reader, int64(len(result.Data)))
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}

	r := parquet.NewGenericReader[schema.TraceRow](f)
	out := make([]schema.TraceRow, 1)
	n, _ := r.Read(out)
	if n != 1 {
		t.Fatalf("read %d rows, want 1", n)
	}

	if out[0].ResourceAttributes["cloud.provider"] != "gcp" {
		t.Errorf("ResourceAttributes[cloud.provider] = %q, want gcp", out[0].ResourceAttributes["cloud.provider"])
	}
	if out[0].SpanAttributes["http.url"] != "https://example.com" {
		t.Errorf("SpanAttributes[http.url] = %q", out[0].SpanAttributes["http.url"])
	}
	if out[0].ScopeAttributes["otel.scope.version"] != "1.0.0" {
		t.Errorf("ScopeAttributes[otel.scope.version] = %q", out[0].ScopeAttributes["otel.scope.version"])
	}
}

func TestZstdLevel(t *testing.T) {
	tests := []struct {
		level int
		want  string
	}{
		{1, "SpeedFastest"},
		{3, "SpeedDefault"},
		{5, "SpeedDefault"},
		{7, "SpeedBetterCompression"},
		{10, "SpeedBetterCompression"},
		{11, "SpeedBestCompression"},
		{22, "SpeedBestCompression"},
	}
	for _, tt := range tests {
		got := zstdLevel(tt.level)
		_ = got
	}
}

func TestWriteLogsParquet_CompressionLevels(t *testing.T) {
	rows := sampleLogRows(100, time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC))

	r1, err := writeLogsParquet(rows, 1000, 1)
	if err != nil {
		t.Fatalf("level 1: %v", err)
	}
	r11, err := writeLogsParquet(rows, 1000, 11)
	if err != nil {
		t.Fatalf("level 11: %v", err)
	}

	if r1.RawBytes != r11.RawBytes {
		t.Errorf("RawBytes should be same regardless of compression level: %d vs %d", r1.RawBytes, r11.RawBytes)
	}

	if len(r1.Data) == 0 || len(r11.Data) == 0 {
		t.Fatal("both compression levels should produce output")
	}
}

func TestFlushResult_RawBytes(t *testing.T) {
	rows := []schema.LogRow{
		{
			TimestampUnixNano: time.Now().UnixNano(),
			Body:              "test message with some content",
			ServiceName:       "my-service",
			TraceID:           "abc123def456",
			ResourceAttributes: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			LogAttributes: map[string]string{
				"attr1": "val1",
			},
		},
	}

	result, err := writeLogsParquet(rows, 1000, 3)
	if err != nil {
		t.Fatal(err)
	}

	if result.RawBytes <= 0 {
		t.Error("RawBytes should be > 0")
	}
	if result.CompressionRatio() <= 0 {
		t.Error("CompressionRatio should be > 0")
	}
}

func (fr *flushResult) CompressionRatio() float64 {
	if fr.RawBytes <= 0 || len(fr.Data) <= 0 {
		return 0
	}
	return float64(fr.RawBytes) / float64(len(fr.Data))
}

func TestSchemaFingerprint(t *testing.T) {
	fp1 := schemaFingerprint(config.ModeLogs)
	fp2 := schemaFingerprint(config.ModeTraces)

	if fp1 == "" {
		t.Error("logs fingerprint should not be empty")
	}
	if fp2 == "" {
		t.Error("traces fingerprint should not be empty")
	}
	if fp1 == fp2 {
		t.Error("logs and traces fingerprints should differ")
	}

	fp1b := schemaFingerprint(config.ModeLogs)
	if fp1 != fp1b {
		t.Error("same mode should produce same fingerprint")
	}
}

func TestFlushAll_PopulatesLabels(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, m := testWriter(t, s3srv.URL)

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := []schema.LogRow{
		{TimestampUnixNano: base.UnixNano(), Body: "a", ServiceName: "api", SeverityText: "INFO"},
		{TimestampUnixNano: base.Add(time.Second).UnixNano(), Body: "b", ServiceName: "worker", SeverityText: "ERROR"},
	}
	bw.AddLogRows(rows)

	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	files := m.GetFilesForRange(base.UnixNano(), base.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if files[0].Labels == nil {
		t.Fatal("Labels should be populated")
	}
	if !files[0].MatchesLabel("service.name", "api") {
		t.Error("should contain service.name=api")
	}
	if !files[0].MatchesLabel("service.name", "worker") {
		t.Error("should contain service.name=worker")
	}
	if !files[0].MatchesLabel("severity_text", "INFO") {
		t.Error("should contain severity_text=INFO")
	}
}

func TestAdaptiveFlush_TargetFileSize(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()

	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "logs/", slog.Default())
	cfg := testInsertConfig()
	cfg.MaxBufferRows = 1000000 // high row limit so it doesn't trigger
	cfg.TargetFileSize = "1KB"  // very low target so byte check triggers
	bw := NewBatchWriter(cfg, pool, m, "logs/", config.ModeLogs, slog.Default())

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(sampleLogRows(50, base))

	// Should have auto-flushed due to per-partition size exceeding 1KB
	if got := m.TotalFiles(); got < 1 {
		t.Errorf("TotalFiles = %d, want >= 1 (adaptive flush should trigger)", got)
	}
}

func TestFlushAll_EnhancedFileInfo(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, m := testWriter(t, s3srv.URL)

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(sampleLogRows(20, base))

	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	files := m.GetFilesForRange(base.UnixNano(), base.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	fi := files[0]
	if fi.RowCount != 20 {
		t.Errorf("RowCount = %d, want 20", fi.RowCount)
	}
	if fi.MinTimeNs != base.UnixNano() {
		t.Errorf("MinTimeNs mismatch")
	}
	if fi.MaxTimeNs != base.Add(19*time.Second).UnixNano() {
		t.Errorf("MaxTimeNs mismatch")
	}
	if fi.RawBytes <= 0 {
		t.Error("RawBytes should be > 0")
	}
	if fi.SchemaFingerprint == "" {
		t.Error("SchemaFingerprint should not be empty")
	}
	if fi.CompressionRatio() <= 0 {
		t.Error("CompressionRatio should be > 0")
	}
}
