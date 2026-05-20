package telemetry

import (
	"context"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// mockStorage implements storage.Storage with no-op methods.
// Bool fields track which methods were called.
type mockStorage struct {
	runQueryCalled             bool
	getFieldNamesCalled        bool
	getFieldValuesCalled       bool
	getStreamFieldNamesCalled  bool
	getStreamFieldValuesCalled bool
	getStreamsCalled           bool
	getStreamIDsCalled         bool
	hasDataForRangeCalled      bool
	closeCalled                bool
}

var _ storage.Storage = (*mockStorage)(nil)

func (m *mockStorage) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	m.runQueryCalled = true
	return nil
}

func (m *mockStorage) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	m.getFieldNamesCalled = true
	return nil, nil
}

func (m *mockStorage) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	m.getFieldValuesCalled = true
	return nil, nil
}

func (m *mockStorage) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	m.getStreamFieldNamesCalled = true
	return nil, nil
}

func (m *mockStorage) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamFieldValuesCalled = true
	return nil, nil
}

func (m *mockStorage) GetStreams(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamsCalled = true
	return nil, nil
}

func (m *mockStorage) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamIDsCalled = true
	return nil, nil
}

func (m *mockStorage) HasDataForRange(_, _ int64) bool {
	m.hasDataForRangeCalled = true
	return false
}

func (m *mockStorage) Close() error {
	m.closeCalled = true
	return nil
}

// setupTracer configures an in-memory exporter and returns it for span inspection.
func setupTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return exporter
}

func TestTracedStorage_RunQuery_CreatesSpan(t *testing.T) {
	exporter := setupTracer(t)
	mock := &mockStorage{}
	ts := NewTracedStorage(mock)

	tenants := []logstorage.TenantID{{AccountID: 1, ProjectID: 2}, {AccountID: 3, ProjectID: 4}}
	err := ts.RunQuery(context.Background(), tenants, nil, nil)
	if err != nil {
		t.Fatalf("RunQuery returned error: %v", err)
	}

	if !mock.runQueryCalled {
		t.Fatal("expected inner RunQuery to be called")
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "storage.run_query" {
		t.Errorf("expected span name 'storage.run_query', got %q", spans[0].Name)
	}

	// Check tenant_count attribute.
	found := false
	for _, attr := range spans[0].Attributes {
		if attr.Key == attribute.Key("tenant_count") && attr.Value.AsInt64() == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected tenant_count=2 attribute, got attributes: %v", spans[0].Attributes)
	}
}

func TestTracedStorage_GetFieldNames_CreatesSpan(t *testing.T) {
	exporter := setupTracer(t)
	mock := &mockStorage{}
	ts := NewTracedStorage(mock)

	_, err := ts.GetFieldNames(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("GetFieldNames returned error: %v", err)
	}

	if !mock.getFieldNamesCalled {
		t.Fatal("expected inner GetFieldNames to be called")
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "storage.get_field_names" {
		t.Errorf("expected span name 'storage.get_field_names', got %q", spans[0].Name)
	}
}

func TestTracedStorage_GetFieldValues_HasFieldAttribute(t *testing.T) {
	exporter := setupTracer(t)
	mock := &mockStorage{}
	ts := NewTracedStorage(mock)

	_, err := ts.GetFieldValues(context.Background(), nil, nil, "my_field", 100)
	if err != nil {
		t.Fatalf("GetFieldValues returned error: %v", err)
	}

	if !mock.getFieldValuesCalled {
		t.Fatal("expected inner GetFieldValues to be called")
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "storage.get_field_values" {
		t.Errorf("expected span name 'storage.get_field_values', got %q", spans[0].Name)
	}

	found := false
	for _, attr := range spans[0].Attributes {
		if attr.Key == attribute.Key("field") && attr.Value.AsString() == "my_field" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected field='my_field' attribute, got attributes: %v", spans[0].Attributes)
	}
}

func TestTracedStorage_HasDataForRange_NoSpan(t *testing.T) {
	exporter := setupTracer(t)
	mock := &mockStorage{}
	ts := NewTracedStorage(mock)

	result := ts.HasDataForRange(0, 100)
	if result {
		t.Error("expected false from mock")
	}

	if !mock.hasDataForRangeCalled {
		t.Fatal("expected inner HasDataForRange to be called")
	}

	spans := exporter.GetSpans()
	if len(spans) != 0 {
		t.Errorf("expected 0 spans for HasDataForRange, got %d", len(spans))
	}
}

func TestTracedStorage_ImplementsInterface(t *testing.T) {
	// Compile-time check (also in traced_storage.go).
	var _ storage.Storage = (*TracedStorage)(nil)
}
