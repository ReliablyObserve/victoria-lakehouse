package telemetry

import (
	"errors"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"

	"go.opentelemetry.io/otel/attribute"
)

type mockWriter struct {
	addedRows int
	canWrite  error
}

func (m *mockWriter) MustAddLogRows(rows []schema.LogRow) { m.addedRows += len(rows) }
func (m *mockWriter) CanWriteData() error                 { return m.canWrite }

func TestTracedWriter_MustAddLogRows_CreatesSpan(t *testing.T) {
	exporter := setupTracer(t)
	mock := &mockWriter{}
	tw := NewTracedWriter(mock)

	rows := []schema.LogRow{
		{Body: "test1"},
		{Body: "test2"},
		{Body: "test3"},
	}
	tw.MustAddLogRows(rows)

	if mock.addedRows != 3 {
		t.Fatalf("expected 3 rows added, got %d", mock.addedRows)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "storage.add_rows" {
		t.Errorf("expected span name 'storage.add_rows', got %q", spans[0].Name)
	}

	found := false
	for _, attr := range spans[0].Attributes {
		if attr.Key == attribute.Key("row_count") && attr.Value.AsInt64() == 3 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected row_count=3 attribute, got attributes: %v", spans[0].Attributes)
	}
}

func TestTracedWriter_CanWriteData_Delegates(t *testing.T) {
	// Test nil error (success case).
	mock := &mockWriter{canWrite: nil}
	tw := NewTracedWriter(mock)
	if err := tw.CanWriteData(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// Test non-nil error propagation.
	expectedErr := errors.New("disk full")
	mock2 := &mockWriter{canWrite: expectedErr}
	tw2 := NewTracedWriter(mock2)
	if err := tw2.CanWriteData(); !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}
