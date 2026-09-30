package main

import (
	"context"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

type lookupStore struct {
	storage.Storage
	ran    int
	lookup int
}

func (s *lookupStore) RunQuery(context.Context, []logstorage.TenantID, *logstorage.Query, logstorage.WriteDataBlockFunc) error {
	s.ran++
	return nil
}

func (s *lookupStore) LookupTraceIndex(context.Context, []logstorage.TenantID, string) (int64, int64, bool, error) {
	s.lookup++
	return 1, 2, true, nil
}

// With telemetry on, the adapter's store is the traced decorator: a LogsQL
// query keeps its storage.run_query span, and the trace-index lookup capability
// the adapter finds by type assertion survives the wrapping.
func TestAdapterStorage_TelemetryKeepsSpanAndLookup(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	inner := &lookupStore{}
	got := adapterStorage(inner, true)
	if _, ok := got.(*internalvlstorage.TracedStorage); !ok {
		t.Fatalf("telemetry on: adapter store is %T, want *TracedStorage", got)
	}
	q, err := logstorage.ParseQueryAtTimestamp("*", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := got.RunQuery(context.Background(), nil, q, func(uint, *logstorage.DataBlock) {}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, sp := range rec.Ended() {
		if sp.Name() == "storage.run_query" {
			found = true
		}
	}
	if !found || inner.ran != 1 {
		t.Fatalf("storage.run_query span missing (found=%v, inner runs=%d)", found, inner.ran)
	}

	l, ok := got.(interface {
		LookupTraceIndex(context.Context, []logstorage.TenantID, string) (int64, int64, bool, error)
	})
	if !ok {
		t.Fatal("traced store dropped LookupTraceIndex")
	}
	if s, e, f, _ := l.LookupTraceIndex(context.Background(), nil, "x"); !f || s != 1 || e != 2 || inner.lookup != 1 {
		t.Fatalf("lookup not forwarded: %d %d %v (inner calls %d)", s, e, f, inner.lookup)
	}
}

func TestAdapterStorage_TelemetryOffIsTheStore(t *testing.T) {
	inner := &lookupStore{}
	if got := adapterStorage(inner, false); got != storage.Storage(inner) {
		t.Fatalf("telemetry off: got %T, want the store itself", got)
	}
}
