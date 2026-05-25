package vlstorage

import (
	"context"
	"errors"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type mockStorage struct {
	runQueryCalled           bool
	getFieldNamesCalled      bool
	getFieldValuesCalled     bool
	getStreamFieldNames      bool
	getStreamFieldValues     bool
	getStreamsCalled         bool
	getStreamIDsCalled       bool
	hasDataCalled            bool
	closeCalled              bool
	returnErr                error
	fieldNameArg             string
	limitArg                 uint64
	hasDataStart, hasDataEnd int64
}

func (m *mockStorage) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	m.runQueryCalled = true
	return m.returnErr
}
func (m *mockStorage) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	m.getFieldNamesCalled = true
	return nil, m.returnErr
}
func (m *mockStorage) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getFieldValuesCalled = true
	m.fieldNameArg = fieldName
	m.limitArg = limit
	return nil, m.returnErr
}
func (m *mockStorage) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	m.getStreamFieldNames = true
	return nil, m.returnErr
}
func (m *mockStorage) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamFieldValues = true
	m.fieldNameArg = fieldName
	m.limitArg = limit
	return nil, m.returnErr
}
func (m *mockStorage) GetStreams(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamsCalled = true
	m.limitArg = limit
	return nil, m.returnErr
}
func (m *mockStorage) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamIDsCalled = true
	m.limitArg = limit
	return nil, m.returnErr
}
func (m *mockStorage) HasDataForRange(startNs, endNs int64) bool {
	m.hasDataCalled = true
	m.hasDataStart = startNs
	m.hasDataEnd = endNs
	return startNs < endNs
}
func (m *mockStorage) Close() error {
	m.closeCalled = true
	return m.returnErr
}

func TestTracedStorage_DelegatesToInner(t *testing.T) {
	m := &mockStorage{}
	ts := NewTracedStorage(m)
	ctx := context.Background()

	if err := ts.RunQuery(ctx, nil, nil, nil); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if !m.runQueryCalled {
		t.Error("RunQuery not delegated")
	}

	if _, err := ts.GetFieldNames(ctx, nil, nil); err != nil {
		t.Fatalf("GetFieldNames: %v", err)
	}
	if !m.getFieldNamesCalled {
		t.Error("GetFieldNames not delegated")
	}

	if _, err := ts.GetFieldValues(ctx, nil, nil, "test_field", 100); err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	if !m.getFieldValuesCalled {
		t.Error("GetFieldValues not delegated")
	}
	if m.fieldNameArg != "test_field" || m.limitArg != 100 {
		t.Errorf("GetFieldValues args: field=%q limit=%d", m.fieldNameArg, m.limitArg)
	}

	if _, err := ts.GetStreamFieldNames(ctx, nil, nil); err != nil {
		t.Fatalf("GetStreamFieldNames: %v", err)
	}
	if !m.getStreamFieldNames {
		t.Error("GetStreamFieldNames not delegated")
	}

	if _, err := ts.GetStreamFieldValues(ctx, nil, nil, "stream_f", 50); err != nil {
		t.Fatalf("GetStreamFieldValues: %v", err)
	}
	if !m.getStreamFieldValues {
		t.Error("GetStreamFieldValues not delegated")
	}

	if _, err := ts.GetStreams(ctx, nil, nil, 10); err != nil {
		t.Fatalf("GetStreams: %v", err)
	}
	if !m.getStreamsCalled {
		t.Error("GetStreams not delegated")
	}

	if _, err := ts.GetStreamIDs(ctx, nil, nil, 20); err != nil {
		t.Fatalf("GetStreamIDs: %v", err)
	}
	if !m.getStreamIDsCalled {
		t.Error("GetStreamIDs not delegated")
	}

	if !ts.HasDataForRange(1, 100) {
		t.Error("HasDataForRange: expected true for 1 < 100")
	}
	if !m.hasDataCalled {
		t.Error("HasDataForRange not delegated")
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !m.closeCalled {
		t.Error("Close not delegated")
	}
}

func TestTracedStorage_PropagatesErrors(t *testing.T) {
	sentinel := errors.New("test error")
	m := &mockStorage{returnErr: sentinel}
	ts := NewTracedStorage(m)
	ctx := context.Background()

	if err := ts.RunQuery(ctx, nil, nil, nil); !errors.Is(err, sentinel) {
		t.Errorf("RunQuery: got %v, want %v", err, sentinel)
	}
	if _, err := ts.GetFieldNames(ctx, nil, nil); !errors.Is(err, sentinel) {
		t.Errorf("GetFieldNames: got %v, want %v", err, sentinel)
	}
	if _, err := ts.GetFieldValues(ctx, nil, nil, "f", 0); !errors.Is(err, sentinel) {
		t.Errorf("GetFieldValues: got %v, want %v", err, sentinel)
	}
	if _, err := ts.GetStreamFieldNames(ctx, nil, nil); !errors.Is(err, sentinel) {
		t.Errorf("GetStreamFieldNames: got %v, want %v", err, sentinel)
	}
	if _, err := ts.GetStreamFieldValues(ctx, nil, nil, "f", 0); !errors.Is(err, sentinel) {
		t.Errorf("GetStreamFieldValues: got %v, want %v", err, sentinel)
	}
	if _, err := ts.GetStreams(ctx, nil, nil, 0); !errors.Is(err, sentinel) {
		t.Errorf("GetStreams: got %v, want %v", err, sentinel)
	}
	if _, err := ts.GetStreamIDs(ctx, nil, nil, 0); !errors.Is(err, sentinel) {
		t.Errorf("GetStreamIDs: got %v, want %v", err, sentinel)
	}
	if err := ts.Close(); !errors.Is(err, sentinel) {
		t.Errorf("Close: got %v, want %v", err, sentinel)
	}
}

func TestTracedStorage_HasDataForRange_False(t *testing.T) {
	m := &mockStorage{}
	ts := NewTracedStorage(m)
	if ts.HasDataForRange(100, 1) {
		t.Error("expected false when start > end")
	}
}
