package parquets3

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type testRow struct {
	Timestamp int64  `parquet:"timestamp_ns"`
	Message   string `parquet:"message"`
	Level     string `parquet:"level"`
}

// testRowWide matches a wide-column layout similar to real lakehouse parquet files.
type testRowWide struct {
	TimestampNs int64  `parquet:"timestamp_ns"`
	Message     string `parquet:"_msg"`
	Stream      string `parquet:"_stream"`
	Level       string `parquet:"level"`
	ServiceName string `parquet:"service_name"`
	Hostname    string `parquet:"hostname"`
	TraceID     string `parquet:"trace_id"`
	SpanID      string `parquet:"span_id"`
	Method      string `parquet:"method"`
	Path        string `parquet:"path"`
	StatusCode  int64  `parquet:"status_code"`
	Duration    int64  `parquet:"duration_ms"`
	UserID      string `parquet:"user_id"`
	RequestID   string `parquet:"request_id"`
	ErrorType   string `parquet:"error_type"`
	ErrorMsg    string `parquet:"error_message"`
	Component   string `parquet:"component"`
	Version     string `parquet:"version"`
	Env         string `parquet:"env"`
	Region      string `parquet:"region"`
	Cluster     string `parquet:"cluster"`
	Namespace   string `parquet:"namespace"`
	PodName     string `parquet:"pod_name"`
}

func TestParseFooterFromBytes_SkipsPageIndex(t *testing.T) {
	// Create a real parquet file in memory.
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[testRow](&buf)
	rows := make([]testRow, 100)
	for i := range rows {
		rows[i] = testRow{
			Timestamp: int64(1000 + i),
			Message:   "test message",
			Level:     "INFO",
		}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	fileSize := int64(len(data))

	// Read footer length from the last 8 bytes.
	footerLen, err := FooterLength(data[len(data)-8:])
	if err != nil {
		t.Fatal(err)
	}

	// Extract footer slice (footerLen + 8 bytes from end).
	totalFooterBytes := footerLen + 8
	footerSlice := data[len(data)-totalFooterBytes:]

	// Parse using our ParseFooterFromBytes which uses footerReaderAt + SkipPageIndex.
	cached, f, err := ParseFooterFromBytes("test.parquet", footerSlice, fileSize)
	if err != nil {
		t.Fatalf("ParseFooterFromBytes failed: %v", err)
	}
	if cached == nil || f == nil {
		t.Fatal("expected non-nil results")
	}
	if f.NumRows() != 100 {
		t.Fatalf("expected 100 rows, got %d", f.NumRows())
	}
	t.Logf("success: parsed footer with %d rows, %d row groups", f.NumRows(), len(f.RowGroups()))
}

func TestParseFooterFromBytes_WideSchema23Cols(t *testing.T) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[testRowWide](&buf)
	rows := make([]testRowWide, 500)
	for i := range rows {
		rows[i] = testRowWide{
			TimestampNs: int64(1000000000 + i*1000000),
			Message:     "GET /api/v1/query 200 OK",
			Stream:      "stream-1",
			Level:       "INFO",
			ServiceName: "api-gateway",
			Hostname:    "node-1",
			TraceID:     "abc123",
			SpanID:      "def456",
			Method:      "GET",
			Path:        "/api/v1/query",
			StatusCode:  200,
			Duration:    42,
			UserID:      "user-1",
			RequestID:   "req-1",
			ErrorType:   "",
			ErrorMsg:    "",
			Component:   "http",
			Version:     "1.0.0",
			Env:         "production",
			Region:      "us-east-1",
			Cluster:     "prod-1",
			Namespace:   "default",
			PodName:     "api-gateway-abc123",
		}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	fileSize := int64(len(data))
	t.Logf("generated parquet file: %d bytes, 23 columns, 500 rows", len(data))

	footerLen, err := FooterLength(data[len(data)-8:])
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("footer length: %d bytes", footerLen)

	totalFooterBytes := footerLen + 8
	footerSlice := data[len(data)-totalFooterBytes:]

	cached, f, err := ParseFooterFromBytes("test-wide.parquet", footerSlice, fileSize)
	if err != nil {
		t.Fatalf("ParseFooterFromBytes with 23 columns failed: %v", err)
	}
	if f.NumRows() != 500 {
		t.Fatalf("expected 500 rows, got %d", f.NumRows())
	}
	rgs := f.RowGroups()
	t.Logf("success: %d rows, %d row groups, %d columns, footerSize=%d",
		f.NumRows(), len(rgs), len(rgs[0].ColumnChunks()), cached.footerSize)
}
