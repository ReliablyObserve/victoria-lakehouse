package vlstorage

import (
	"context"
	"errors"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"
)

// --- Additional mock stores for coverage ---

// mockStoreWithFieldValues returns predefined values for GetFieldValues.
type mockStoreWithFieldValues struct {
	mockStore
	fieldValues []logstorage.ValueWithHits
}

var _ storage.Storage = (*mockStoreWithFieldValues)(nil)

func (m mockStoreWithFieldValues) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return m.fieldValues, nil
}

// mockStoreWithStreamFields returns predefined stream field names and values.
type mockStoreWithStreamFields struct {
	mockStore
	streamFieldNames  []logstorage.ValueWithHits
	streamFieldValues []logstorage.ValueWithHits
}

var _ storage.Storage = (*mockStoreWithStreamFields)(nil)

func (m mockStoreWithStreamFields) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return m.streamFieldNames, nil
}

func (m mockStoreWithStreamFields) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return m.streamFieldValues, nil
}

// mockStoreWithStreams returns predefined streams and stream IDs.
type mockStoreWithStreams struct {
	mockStore
	streams   []logstorage.ValueWithHits
	streamIDs []logstorage.ValueWithHits
}

var _ storage.Storage = (*mockStoreWithStreams)(nil)

func (m mockStoreWithStreams) GetStreams(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return m.streams, nil
}

func (m mockStoreWithStreams) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return m.streamIDs, nil
}

// mockStoreWithRunQuery records calls and invokes writeBlock with predefined data.
type mockStoreWithRunQuery struct {
	mockStore
	columns []logstorage.BlockColumn
}

var _ storage.Storage = (*mockStoreWithRunQuery)(nil)

func (m mockStoreWithRunQuery) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	if len(m.columns) > 0 {
		db := &logstorage.DataBlock{}
		db.SetColumns(m.columns)
		writeBlock(0, db)
	}
	return nil
}

// --- Coverage tests ---

func TestGetFieldValues_DelegatesToStore(t *testing.T) {
	store := mockStoreWithFieldValues{
		fieldValues: []logstorage.ValueWithHits{
			{Value: "value-a", Hits: 10},
			{Value: "value-b", Hits: 5},
			{Value: "value-c", Hits: 3},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context: context.Background(),
	}

	results, err := a.GetFieldValues(qctx, "fieldName", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	if results[0].Value != "value-a" {
		t.Errorf("expected first value 'value-a', got %q", results[0].Value)
	}
}

func TestGetStreamFieldNames_DelegatesToStore(t *testing.T) {
	store := mockStoreWithStreamFields{
		streamFieldNames: []logstorage.ValueWithHits{
			{Value: "host", Hits: 10},
			{Value: "k8s.namespace", Hits: 5},
			{Value: "service", Hits: 3},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context: context.Background(),
	}

	results, err := a.GetStreamFieldNames(qctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestGetStreamFieldNames_WithHiddenFieldsFilter(t *testing.T) {
	store := mockStoreWithStreamFields{
		streamFieldNames: []logstorage.ValueWithHits{
			{Value: "host", Hits: 10},
			{Value: "k8s.namespace", Hits: 5},
			{Value: "k8s.pod", Hits: 3},
			{Value: "service", Hits: 2},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context:             context.Background(),
		HiddenFieldsFilters: []string{"k8s.*"},
	}

	results, err := a.GetStreamFieldNames(qctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results (k8s.* filtered), got %d", len(results))
	}
	for _, r := range results {
		if r.Value == "k8s.namespace" || r.Value == "k8s.pod" {
			t.Errorf("hidden field %q should have been filtered", r.Value)
		}
	}
}

func TestGetStreamFieldValues_DelegatesToStore(t *testing.T) {
	store := mockStoreWithStreamFields{
		streamFieldValues: []logstorage.ValueWithHits{
			{Value: "prod", Hits: 10},
			{Value: "staging", Hits: 5},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context: context.Background(),
	}

	results, err := a.GetStreamFieldValues(qctx, "host", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
	if results[0].Value != "prod" {
		t.Errorf("expected first value 'prod', got %q", results[0].Value)
	}
}

func TestGetStreams_DelegatesToStore(t *testing.T) {
	store := mockStoreWithStreams{
		streams: []logstorage.ValueWithHits{
			{Value: `{host="web-1"}`, Hits: 100},
			{Value: `{host="web-2"}`, Hits: 50},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context: context.Background(),
	}

	results, err := a.GetStreams(qctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 streams, got %d", len(results))
	}
}

func TestGetStreamIDs_DelegatesToStore(t *testing.T) {
	store := mockStoreWithStreams{
		streamIDs: []logstorage.ValueWithHits{
			{Value: "stream-001", Hits: 100},
			{Value: "stream-002", Hits: 50},
			{Value: "stream-003", Hits: 25},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context: context.Background(),
	}

	results, err := a.GetStreamIDs(qctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 stream IDs, got %d", len(results))
	}
}

func TestRunQuery_NoPipes_DelegatesToStore(t *testing.T) {
	store := mockStoreWithRunQuery{
		columns: []logstorage.BlockColumn{
			{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
			{Name: "msg", Values: []string{"hello"}},
		},
	}
	a := &adapter{store: store}

	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}

	qctx := &logstorage.QueryContext{
		Context:    context.Background(),
		Query:      q,
		QueryStats: &logstorage.QueryStats{},
	}

	var received *logstorage.DataBlock
	err = a.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		received = db
	})
	if err != nil {
		t.Fatal(err)
	}
	if received == nil {
		t.Fatal("expected DataBlock, got nil")
	}
	cols := received.GetColumns(false)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
}

func TestRunQuery_NoPipes_WithHiddenFields(t *testing.T) {
	store := mockStoreWithRunQuery{
		columns: []logstorage.BlockColumn{
			{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
			{Name: "secret", Values: []string{"hidden-value"}},
			{Name: "msg", Values: []string{"hello"}},
		},
	}
	a := &adapter{store: store}

	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}

	qctx := &logstorage.QueryContext{
		Context:             context.Background(),
		Query:               q,
		QueryStats:          &logstorage.QueryStats{},
		HiddenFieldsFilters: []string{"secret"},
	}

	var received *logstorage.DataBlock
	err = a.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		received = db
	})
	if err != nil {
		t.Fatal(err)
	}
	if received == nil {
		t.Fatal("expected DataBlock, got nil")
	}
	cols := received.GetColumns(false)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns (secret hidden), got %d", len(cols))
	}
	for _, c := range cols {
		if c.Name == "secret" {
			t.Error("column 'secret' should have been hidden")
		}
	}
}

func TestRunQuery_WithPipes_CallsRunQueryExternal(t *testing.T) {
	store := mockStoreWithRunQuery{
		columns: []logstorage.BlockColumn{
			{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
			{Name: "_msg", Values: []string{"hello world"}},
		},
	}
	a := &adapter{store: store}

	// A query with a pipe operator (| limit 10) triggers RunQueryExternal path
	q, err := logstorage.ParseQuery("* | limit 10")
	if err != nil {
		t.Fatalf("parse query with pipe: %v", err)
	}

	if !logstorage.QueryHasPipes(q) {
		t.Fatal("expected query to have pipes")
	}

	qctx := &logstorage.QueryContext{
		Context:    context.Background(),
		Query:      q,
		QueryStats: &logstorage.QueryStats{},
	}

	var callCount int
	err = a.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		callCount++
	})
	if err != nil {
		t.Fatalf("RunQuery with pipes: %v", err)
	}
	// RunQueryExternal processes the pipe; we just verify no error
}

func TestWrapHiddenFields_AllColumnsHidden(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "secret1", Values: []string{"a"}},
		{Name: "secret2", Values: []string{"b"}},
	})

	var writeBlockCalled bool
	wrapped := wrapHiddenFields(func(_ uint, d *logstorage.DataBlock) {
		writeBlockCalled = true
	}, []string{"secret1", "secret2"})
	wrapped(0, db)

	if writeBlockCalled {
		t.Error("writeBlock should not be called when all columns are hidden")
	}
}

func TestWrapHiddenFields_NoColumnsHidden(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
		{Name: "msg", Values: []string{"hello"}},
	})

	var received *logstorage.DataBlock
	wrapped := wrapHiddenFields(func(_ uint, d *logstorage.DataBlock) {
		received = d
	}, []string{"nonexistent"})
	wrapped(0, db)

	if received == nil {
		t.Fatal("expected original DataBlock to be passed through")
	}
	// When no columns match the filter, the original DataBlock is passed
	cols := received.GetColumns(false)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
}

func TestDeleteStopTask_NilTombstones_NoPanic(t *testing.T) {
	a := &adapter{store: mockStore{}, tombstones: nil}
	err := a.DeleteStopTask(context.Background(), "task-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetFieldNames_NoHiddenFilters(t *testing.T) {
	store := mockStoreWithFields{
		fieldNames: []logstorage.ValueWithHits{
			{Value: "service.name", Hits: 10},
			{Value: "level", Hits: 5},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context: context.Background(),
		// No HiddenFieldsFilters
	}

	results, err := a.GetFieldNames(qctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

// --- Error propagation tests for all methods ---

// errMockStore returns an error from every method.
type errMockStore struct {
	mockStore
	err error
}

func (e errMockStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	return e.err
}
func (e errMockStore) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, e.err
}
func (e errMockStore) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, e.err
}
func (e errMockStore) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, e.err
}
func (e errMockStore) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, e.err
}
func (e errMockStore) GetStreams(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, e.err
}
func (e errMockStore) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, e.err
}

func TestGetFieldValues_Error(t *testing.T) {
	a := &adapter{store: errMockStore{err: errors.New("field values error")}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	_, err := a.GetFieldValues(qctx, "field", "", 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetStreamFieldNames_Error(t *testing.T) {
	a := &adapter{store: errMockStore{err: errors.New("stream field names error")}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	_, err := a.GetStreamFieldNames(qctx, "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetStreamFieldValues_Error(t *testing.T) {
	a := &adapter{store: errMockStore{err: errors.New("stream field values error")}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	_, err := a.GetStreamFieldValues(qctx, "host", "", 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetStreams_Error(t *testing.T) {
	a := &adapter{store: errMockStore{err: errors.New("streams error")}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	_, err := a.GetStreams(qctx, 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetStreamIDs_Error(t *testing.T) {
	a := &adapter{store: errMockStore{err: errors.New("stream IDs error")}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	_, err := a.GetStreamIDs(qctx, 100)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetFieldNames_Error(t *testing.T) {
	a := &adapter{store: errMockStore{err: errors.New("field names error")}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	_, err := a.GetFieldNames(qctx, "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestRunQuery_Error(t *testing.T) {
	a := &adapter{store: errMockStore{err: errors.New("query error")}}
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatal(err)
	}
	qctx := &logstorage.QueryContext{
		Context:    context.Background(),
		Query:      q,
		QueryStats: &logstorage.QueryStats{},
	}
	err = a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- Additional edge case tests ---

func TestFilterHiddenValues_EmptyInput(t *testing.T) {
	result := filterHiddenValues(nil, []string{"something"})
	if len(result) != 0 {
		t.Errorf("expected empty result for nil input, got %d", len(result))
	}
}

func TestFilterHiddenValues_MultipleExactFilters(t *testing.T) {
	values := []logstorage.ValueWithHits{
		{Value: "a", Hits: 10},
		{Value: "b", Hits: 5},
		{Value: "c", Hits: 3},
	}
	result := filterHiddenValues(values, []string{"a", "b"})
	if len(result) != 1 {
		t.Errorf("expected 1 result, got %d", len(result))
	}
	if result[0].Value != "c" {
		t.Errorf("expected c, got %q", result[0].Value)
	}
}

func TestGetTenantIDs_WithNilTombstones(t *testing.T) {
	a := &adapter{store: mockStore{}, tombstones: nil}
	ids, err := a.GetTenantIDs(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 tenant, got %d", len(ids))
	}
}
