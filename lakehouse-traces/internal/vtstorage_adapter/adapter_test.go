package vtstorageadapter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// mockStorage is a test double for storage.Storage that records method calls
// and returns configurable results.
type mockStorage struct {
	// method call trackers
	runQueryCalled             bool
	getFieldNamesCalled        bool
	getFieldValuesCalled       bool
	getStreamFieldNamesCalled  bool
	getStreamFieldValuesCalled bool
	getStreamsCalled           bool
	getStreamIDsCalled         bool
	hasDataForRangeCalled      bool

	// recorded parameters for verification
	lastCtx       context.Context
	lastTenantIDs []logstorage.TenantID
	lastQuery     *logstorage.Query
	lastFieldName string
	lastLimit     uint64
	lastStart     int64
	lastEnd       int64

	// configurable return values
	returnValues          []logstorage.ValueWithHits
	returnErr             error
	returnHasDataForRange bool
}

func (m *mockStorage) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	m.runQueryCalled = true
	m.lastCtx = ctx
	m.lastTenantIDs = tenantIDs
	m.lastQuery = q
	return m.returnErr
}

func (m *mockStorage) GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	m.getFieldNamesCalled = true
	m.lastCtx = ctx
	m.lastTenantIDs = tenantIDs
	m.lastQuery = q
	return m.returnValues, m.returnErr
}

func (m *mockStorage) GetFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getFieldValuesCalled = true
	m.lastCtx = ctx
	m.lastTenantIDs = tenantIDs
	m.lastQuery = q
	m.lastFieldName = fieldName
	m.lastLimit = limit
	return m.returnValues, m.returnErr
}

func (m *mockStorage) GetStreamFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	m.getStreamFieldNamesCalled = true
	m.lastCtx = ctx
	m.lastTenantIDs = tenantIDs
	m.lastQuery = q
	return m.returnValues, m.returnErr
}

func (m *mockStorage) GetStreamFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamFieldValuesCalled = true
	m.lastCtx = ctx
	m.lastTenantIDs = tenantIDs
	m.lastQuery = q
	m.lastFieldName = fieldName
	m.lastLimit = limit
	return m.returnValues, m.returnErr
}

func (m *mockStorage) GetStreams(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamsCalled = true
	m.lastCtx = ctx
	m.lastTenantIDs = tenantIDs
	m.lastQuery = q
	m.lastLimit = limit
	return m.returnValues, m.returnErr
}

func (m *mockStorage) GetStreamIDs(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamIDsCalled = true
	m.lastCtx = ctx
	m.lastTenantIDs = tenantIDs
	m.lastQuery = q
	m.lastLimit = limit
	return m.returnValues, m.returnErr
}

func (m *mockStorage) HasDataForRange(startNs, endNs int64) bool {
	m.hasDataForRangeCalled = true
	m.lastStart = startNs
	m.lastEnd = endNs
	return m.returnHasDataForRange
}

func (m *mockStorage) Close() error {
	return nil
}

// newTestAdapter creates a fresh Adapter wrapping a mockStorage.
func newTestAdapter(mock *mockStorage) *Adapter {
	return &Adapter{store: mock}
}

// newTestQctx creates a minimal QueryContext for testing.
func newTestQctx(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) *logstorage.QueryContext {
	return &logstorage.QueryContext{
		Context:   ctx,
		TenantIDs: tenantIDs,
		Query:     q,
	}
}

func TestRunQuery_NilQueryReturnsNil(t *testing.T) {
	mock := &mockStorage{}
	a := newTestAdapter(mock)

	ctx := context.Background()
	tenantIDs := []logstorage.TenantID{{AccountID: 1, ProjectID: 2}}
	qctx := newTestQctx(ctx, tenantIDs, nil)

	err := a.RunQuery(qctx, nil)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRunQuery_DelegatesToStorage(t *testing.T) {
	mock := &mockStorage{}
	a := newTestAdapter(mock)

	ctx := context.Background()
	tenantIDs := []logstorage.TenantID{{AccountID: 1, ProjectID: 2}}
	q, err := logstorage.ParseQueryAtTimestamp("*", 1000000000)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	qctx := newTestQctx(ctx, tenantIDs, q)

	err = a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !mock.runQueryCalled {
		t.Fatal("expected RunQuery to be called on storage")
	}
	if mock.lastCtx != ctx {
		t.Error("context not forwarded correctly")
	}
	if len(mock.lastTenantIDs) != 1 || mock.lastTenantIDs[0] != tenantIDs[0] {
		t.Error("tenantIDs not forwarded correctly")
	}
}

func TestRunQuery_PropagatesError(t *testing.T) {
	wantErr := errors.New("storage error")
	mock := &mockStorage{returnErr: wantErr}
	a := newTestAdapter(mock)

	q, err := logstorage.ParseQueryAtTimestamp("*", 1000000000)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	qctx := newTestQctx(context.Background(), nil, q)
	err = a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {})

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestGetFieldNames_DelegatesToStorage(t *testing.T) {
	want := []logstorage.ValueWithHits{{Value: "field1", Hits: 10}}
	mock := &mockStorage{returnValues: want}
	a := newTestAdapter(mock)

	ctx := context.Background()
	tenantIDs := []logstorage.TenantID{{AccountID: 5, ProjectID: 6}}
	qctx := newTestQctx(ctx, tenantIDs, nil)

	got, err := a.GetFieldNames(qctx)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.getFieldNamesCalled {
		t.Fatal("expected GetFieldNames to be called on storage")
	}
	if len(got) != 1 || got[0].Value != "field1" {
		t.Errorf("unexpected result: %v", got)
	}
	if mock.lastCtx != ctx {
		t.Error("context not forwarded correctly")
	}
}

func TestGetFieldValues_ForwardsFieldNameAndLimit(t *testing.T) {
	want := []logstorage.ValueWithHits{{Value: "val1", Hits: 3}}
	mock := &mockStorage{returnValues: want}
	a := newTestAdapter(mock)

	qctx := newTestQctx(context.Background(), nil, nil)
	got, err := a.GetFieldValues(qctx, "myField", 42)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.getFieldValuesCalled {
		t.Fatal("expected GetFieldValues to be called on storage")
	}
	if mock.lastFieldName != "myField" {
		t.Errorf("expected fieldName %q, got %q", "myField", mock.lastFieldName)
	}
	if mock.lastLimit != 42 {
		t.Errorf("expected limit 42, got %d", mock.lastLimit)
	}
	if len(got) != 1 || got[0].Value != "val1" {
		t.Errorf("unexpected result: %v", got)
	}
}

func TestGetStreamFieldNames_DelegatesToStorage(t *testing.T) {
	want := []logstorage.ValueWithHits{{Value: "streamField", Hits: 7}}
	mock := &mockStorage{returnValues: want}
	a := newTestAdapter(mock)

	ctx := context.Background()
	qctx := newTestQctx(ctx, nil, nil)

	got, err := a.GetStreamFieldNames(qctx)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.getStreamFieldNamesCalled {
		t.Fatal("expected GetStreamFieldNames to be called on storage")
	}
	if len(got) != 1 || got[0].Value != "streamField" {
		t.Errorf("unexpected result: %v", got)
	}
}

func TestGetStreamFieldValues_ForwardsFieldNameAndLimit(t *testing.T) {
	mock := &mockStorage{returnValues: []logstorage.ValueWithHits{{Value: "sv", Hits: 1}}}
	a := newTestAdapter(mock)

	qctx := newTestQctx(context.Background(), nil, nil)
	_, err := a.GetStreamFieldValues(qctx, "streamKey", 100)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.getStreamFieldValuesCalled {
		t.Fatal("expected GetStreamFieldValues to be called on storage")
	}
	if mock.lastFieldName != "streamKey" {
		t.Errorf("expected fieldName %q, got %q", "streamKey", mock.lastFieldName)
	}
	if mock.lastLimit != 100 {
		t.Errorf("expected limit 100, got %d", mock.lastLimit)
	}
}

func TestGetStreams_ForwardsLimit(t *testing.T) {
	mock := &mockStorage{returnValues: []logstorage.ValueWithHits{{Value: "stream1", Hits: 5}}}
	a := newTestAdapter(mock)

	qctx := newTestQctx(context.Background(), nil, nil)
	got, err := a.GetStreams(qctx, 50)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.getStreamsCalled {
		t.Fatal("expected GetStreams to be called on storage")
	}
	if mock.lastLimit != 50 {
		t.Errorf("expected limit 50, got %d", mock.lastLimit)
	}
	if len(got) != 1 || got[0].Value != "stream1" {
		t.Errorf("unexpected result: %v", got)
	}
}

func TestGetStreamIDs_ForwardsLimit(t *testing.T) {
	mock := &mockStorage{returnValues: []logstorage.ValueWithHits{{Value: "id1", Hits: 2}}}
	a := newTestAdapter(mock)

	qctx := newTestQctx(context.Background(), nil, nil)
	got, err := a.GetStreamIDs(qctx, 25)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mock.getStreamIDsCalled {
		t.Fatal("expected GetStreamIDs to be called on storage")
	}
	if mock.lastLimit != 25 {
		t.Errorf("expected limit 25, got %d", mock.lastLimit)
	}
	if len(got) != 1 || got[0].Value != "id1" {
		t.Errorf("unexpected result: %v", got)
	}
}

func TestGetTenantIDs_ReturnsNilWhenNoData(t *testing.T) {
	mock := &mockStorage{returnHasDataForRange: false}
	a := newTestAdapter(mock)

	tenants, err := a.GetTenantIDs(context.Background(), 1000, 2000)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenants != nil {
		t.Errorf("expected nil tenant list when no data, got %v", tenants)
	}
	if !mock.hasDataForRangeCalled {
		t.Fatal("expected HasDataForRange to be called")
	}
	if mock.lastStart != 1000 {
		t.Errorf("expected start 1000, got %d", mock.lastStart)
	}
	if mock.lastEnd != 2000 {
		t.Errorf("expected end 2000, got %d", mock.lastEnd)
	}
}

func TestGetTenantIDs_ReturnsDefaultTenantWhenDataExists(t *testing.T) {
	mock := &mockStorage{returnHasDataForRange: true}
	a := newTestAdapter(mock)

	tenants, err := a.GetTenantIDs(context.Background(), 500, 1500)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tenants) != 1 {
		t.Fatalf("expected 1 tenant, got %d", len(tenants))
	}
	if tenants[0].AccountID != 0 || tenants[0].ProjectID != 0 {
		t.Errorf("expected default tenant {0,0}, got %+v", tenants[0])
	}
	if mock.lastStart != 500 || mock.lastEnd != 1500 {
		t.Errorf("range not forwarded correctly: start=%d end=%d", mock.lastStart, mock.lastEnd)
	}
}

func TestGetTenantIDs_IgnoresContext(t *testing.T) {
	// GetTenantIDs takes a context but the adapter ignores it (passes _ parameter).
	// Verify it still works correctly regardless of context value.
	mock := &mockStorage{returnHasDataForRange: true}
	a := newTestAdapter(mock)

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "sentinel")

	tenants, err := a.GetTenantIDs(ctx, 0, 0)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tenants) != 1 {
		t.Fatalf("expected 1 tenant, got %d", len(tenants))
	}
}

func TestExtractTraceIDFromIndexQuery(t *testing.T) {
	tests := []struct {
		name     string
		queryStr string
		want     string
	}{
		{
			name:     "quoted index query",
			queryStr: `{trace_id_idx_stream="42"} AND trace_id_idx:="abc123def456"`,
			want:     "abc123def456",
		},
		{
			name:     "unquoted index query (VL serialized format)",
			queryStr: `{trace_id_idx_stream="42"} trace_id_idx:=abc123def456 | stats min(_time)`,
			want:     "abc123def456",
		},
		{
			name:     "with time filter and pipes",
			queryStr: `_time:[2026-05-26T10:00:00Z,2026-05-26T11:00:00Z] {trace_id_idx_stream="99"} trace_id_idx:=deadbeef | stats min(_time) _time`,
			want:     "deadbeef",
		},
		{
			name:     "no index query (search)",
			queryStr: `{trace_id_idx_stream=""} AND * | last 1 by (_time) partition by (trace_id)`,
			want:     "",
		},
		{
			name:     "empty string",
			queryStr: "",
			want:     "",
		},
		{
			name:     "regular query without index",
			queryStr: `trace_id:="abc123"`,
			want:     "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTraceIDFromIndexQuery(tt.queryStr)
			if got != tt.want {
				t.Errorf("extractTraceIDFromIndexQuery(%q) = %q, want %q", tt.queryStr, got, tt.want)
			}
		})
	}
}

func TestRewriteTraceIndexQuery(t *testing.T) {
	t.Run("rewrites index query", func(t *testing.T) {
		qStr := `{trace_id_idx_stream="42"} AND trace_id_idx:="abc123" | stats min(_time) _time, min(start_time) start_time, max(end_time) end_time`
		q, err := logstorage.ParseQueryAtTimestamp(qStr, 1000000000)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		rewritten, ok := rewriteTraceIndexQuery(q)
		if !ok {
			t.Fatal("expected rewrite to succeed")
		}
		rewrittenStr := rewritten.String()
		if !strings.Contains(rewrittenStr, "trace_id:=") {
			t.Errorf("expected rewritten query to contain trace_id filter, got: %s", rewrittenStr)
		}
		if !strings.Contains(rewrittenStr, "start_time_unix_nano") {
			t.Errorf("expected rewritten query to reference start_time_unix_nano, got: %s", rewrittenStr)
		}
		if !strings.Contains(rewrittenStr, "end_time_unix_nano") {
			t.Errorf("expected rewritten query to reference end_time_unix_nano, got: %s", rewrittenStr)
		}
		if strings.Contains(rewrittenStr, "trace_id_idx_stream") {
			t.Errorf("expected rewritten query to NOT contain trace_id_idx_stream, got: %s", rewrittenStr)
		}
	})

	t.Run("does not rewrite search query", func(t *testing.T) {
		qStr := `{trace_id_idx_stream=""} AND * | last 1 by (_time) partition by (trace_id) | fields _time, trace_id`
		q, err := logstorage.ParseQueryAtTimestamp(qStr, 1000000000)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := rewriteTraceIndexQuery(q)
		if ok {
			t.Fatal("expected rewrite to NOT trigger for search query")
		}
	})

	t.Run("does not rewrite regular query", func(t *testing.T) {
		qStr := `trace_id:="abc123"`
		q, err := logstorage.ParseQueryAtTimestamp(qStr, 1000000000)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, ok := rewriteTraceIndexQuery(q)
		if ok {
			t.Fatal("expected rewrite to NOT trigger for regular query")
		}
	})
}

func TestInterfaceCompliance(t *testing.T) {
	// Compile-time check is already enforced by the var _ declaration in adapter.go.
	// This test is a runtime sanity check that the adapter can be created and used.
	mock := &mockStorage{}
	a := newTestAdapter(mock)
	if a == nil {
		t.Fatal("adapter should not be nil")
	}
	if a.store == nil {
		t.Fatal("adapter.store should not be nil")
	}
}
