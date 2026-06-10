package parquets3

import (
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestExtractLogBloomValues(t *testing.T) {
	rows := []schema.LogRow{
		{TraceID: "trace-aaa", ServiceName: "api-gw"},
		{TraceID: "trace-bbb", ServiceName: "api-gw"},
		{TraceID: "trace-ccc", ServiceName: "order-svc"},
		{TraceID: "", ServiceName: ""},
	}

	vals := extractLogBloomValues(rows)
	if vals == nil {
		t.Fatal("expected non-nil bloom values")
	}

	traceIDs := vals["trace_id"]
	if len(traceIDs) != 3 {
		t.Errorf("want 3 trace_ids, got %d", len(traceIDs))
	}

	services := vals["service.name"]
	if len(services) != 2 {
		t.Errorf("want 2 services, got %d", len(services))
	}
}

func TestExtractLogBloomValues_Empty(t *testing.T) {
	vals := extractLogBloomValues(nil)
	if vals != nil {
		t.Error("expected nil for empty rows")
	}
}

func TestExtractTraceBloomValues(t *testing.T) {
	rows := []schema.TraceRow{
		{TraceID: "trace-111", ServiceName: "user-svc"},
		{TraceID: "trace-222", ServiceName: "user-svc"},
		{TraceID: "trace-333", ServiceName: "payment-svc"},
	}

	vals := extractTraceBloomValues(rows)
	if vals == nil {
		t.Fatal("expected non-nil bloom values")
	}

	traceIDs := vals["trace_id"]
	if len(traceIDs) != 3 {
		t.Errorf("want 3 trace_ids, got %d", len(traceIDs))
	}

	services := vals["service.name"]
	if len(services) != 2 {
		t.Errorf("want 2 services, got %d", len(services))
	}
}

func TestExtractTraceBloomValues_Empty(t *testing.T) {
	vals := extractTraceBloomValues(nil)
	if vals != nil {
		t.Error("expected nil for empty rows")
	}
}

func TestBloomS3Loader_NonExistent(t *testing.T) {
	loader := bloomS3Loader(nil, "prefix/")
	idx, err := loader(context.Background(), "nonexistent")
	if err != nil {
		t.Errorf("non-existent bloom should return nil, not error: %v", err)
	}
	if idx != nil {
		t.Error("non-existent bloom should return nil index")
	}
}
