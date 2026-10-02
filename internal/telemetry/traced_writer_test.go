package telemetry

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"go.opentelemetry.io/otel/attribute"
)

type mockBuffer struct {
	addedRows int
	readOnly  bool
}

func (m *mockBuffer) MustAddRows(lr *logstorage.LogRows) { m.addedRows += lr.RowsCount() }
func (m *mockBuffer) IsReadOnly() bool                   { return m.readOnly }

func TestTracedBuffer_MustAddRows_CreatesSpan(t *testing.T) {
	exporter := setupTracer(t)
	mock := &mockBuffer{}
	tb := NewTracedBuffer(mock)

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	for i := 0; i < 3; i++ {
		lr.MustAdd(logstorage.TenantID{}, int64(i+1), []logstorage.Field{{Name: "_msg", Value: "test"}}, -1)
	}
	tb.MustAddRows(lr)

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

func TestTracedBuffer_IsReadOnly_Delegates(t *testing.T) {
	if NewTracedBuffer(&mockBuffer{}).IsReadOnly() {
		t.Fatal("expected false")
	}
	if !NewTracedBuffer(&mockBuffer{readOnly: true}).IsReadOnly() {
		t.Fatal("expected true")
	}
}
