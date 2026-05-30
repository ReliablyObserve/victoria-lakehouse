package vlstorage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// mockStore implements storage.Storage with no-op methods.
type mockStore struct{}

var _ storage.Storage = (*mockStore)(nil)

func (mockStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	return nil
}
func (mockStore) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetStreams(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) HasDataForRange(_, _ int64) bool { return true }
func (mockStore) Close() error                    { return nil }

func TestSetStorage_NoPanic(t *testing.T) {
	// SetStorage registers an adapter with VL's vlstorage global state.
	// It should not panic even with a nil tombstone store.
	SetStorage(mockStore{}, nil)
}

func TestGetTenantIDs_ReturnsDefault(t *testing.T) {
	a := &adapter{store: mockStore{}}
	ids, err := a.GetTenantIDs(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 tenant ID, got %d", len(ids))
	}
	if ids[0].AccountID != 0 || ids[0].ProjectID != 0 {
		t.Errorf("expected default tenant {0,0}, got {%d,%d}", ids[0].AccountID, ids[0].ProjectID)
	}
}

type mockStoreNoData struct{ mockStore }

func (mockStoreNoData) HasDataForRange(_, _ int64) bool { return false }

func TestGetTenantIDs_NoDataReturnsEmpty(t *testing.T) {
	a := &adapter{store: mockStoreNoData{}}
	ids, err := a.GetTenantIDs(context.Background(), 1000, 2000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected 0 tenant IDs for range with no data, got %d", len(ids))
	}
}

func TestDeleteRunTask_NilTombstones_NoPanic(t *testing.T) {
	a := &adapter{store: mockStore{}, tombstones: nil}
	err := a.DeleteRunTask(context.Background(), "task-1", time.Now().UnixNano(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteRunTask_AddsTombstone(t *testing.T) {
	ts := delete.NewTombstoneStore()
	a := &adapter{store: mockStore{}, tombstones: ts}

	taskID := "delete-task-42"
	timestamp := time.Now().UnixNano()

	// Pass nil filter -- DeleteRunTask calls f.String() which returns "*" for nil.
	err := a.DeleteRunTask(context.Background(), taskID, timestamp, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ts.Count() != 1 {
		t.Fatalf("expected 1 tombstone, got %d", ts.Count())
	}

	stored, ok := ts.Get(taskID)
	if !ok {
		t.Fatal("tombstone not found by task ID")
	}
	if stored.EndNs != timestamp {
		t.Errorf("EndNs = %d, want %d", stored.EndNs, timestamp)
	}
	if stored.Mode != "auto" {
		t.Errorf("Mode = %q, want %q", stored.Mode, "auto")
	}
}

func TestDeleteStopTask_NilTombstones_NoPanic(t *testing.T) {
	a := &adapter{store: mockStore{}, tombstones: nil}
	err := a.DeleteStopTask(context.Background(), "task-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteStopTask_RemovesTombstone(t *testing.T) {
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{ID: "task-99", Query: "*", Mode: "auto"})
	a := &adapter{store: mockStore{}, tombstones: ts}

	if ts.Count() != 1 {
		t.Fatalf("precondition: expected 1 tombstone, got %d", ts.Count())
	}

	err := a.DeleteStopTask(context.Background(), "task-99")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ts.Count() != 0 {
		t.Errorf("expected 0 tombstones after stop, got %d", ts.Count())
	}
}

func TestDeleteActiveTasks_NilTombstones(t *testing.T) {
	a := &adapter{store: mockStore{}, tombstones: nil}
	tasks, err := a.DeleteActiveTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tasks != nil {
		t.Errorf("expected nil tasks, got %v", tasks)
	}
}

func TestFilterValuesBySubstring(t *testing.T) {
	values := []logstorage.ValueWithHits{
		{Value: "service.name", Hits: 10},
		{Value: "level", Hits: 5},
		{Value: "service.version", Hits: 3},
		{Value: "host.name", Hits: 2},
	}

	t.Run("empty filter returns all", func(t *testing.T) {
		result := filterValuesBySubstring(values, "")
		if len(result) != 4 {
			t.Errorf("expected 4 results, got %d", len(result))
		}
	})

	t.Run("filter by substring", func(t *testing.T) {
		result := filterValuesBySubstring(values, "service")
		if len(result) != 2 {
			t.Errorf("expected 2 results matching 'service', got %d", len(result))
		}
	})

	t.Run("filter matches none", func(t *testing.T) {
		result := filterValuesBySubstring(values, "nonexistent")
		if len(result) != 0 {
			t.Errorf("expected 0 results, got %d", len(result))
		}
	})

	t.Run("filter matches one", func(t *testing.T) {
		result := filterValuesBySubstring(values, "level")
		if len(result) != 1 {
			t.Errorf("expected 1 result, got %d", len(result))
		}
	})
}

// mockStoreWithFields returns predefined field names/values for testing filter passthrough.
type mockStoreWithFields struct {
	mockStore
	fieldNames []logstorage.ValueWithHits
}

func (m mockStoreWithFields) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return m.fieldNames, nil
}

func TestGetFieldNames_FilterSubstring(t *testing.T) {
	store := mockStoreWithFields{
		fieldNames: []logstorage.ValueWithHits{
			{Value: "service.name", Hits: 10},
			{Value: "level", Hits: 5},
			{Value: "service.version", Hits: 3},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{Context: context.Background()}

	t.Run("no filter", func(t *testing.T) {
		results, err := a.GetFieldNames(qctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 results, got %d", len(results))
		}
	})

	t.Run("with filter", func(t *testing.T) {
		results, err := a.GetFieldNames(qctx, "service")
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results matching 'service', got %d", len(results))
		}
	})
}

func TestWrapHiddenFields_NoFilters(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
		{Name: "_msg", Values: []string{"hello"}},
		{Name: "service.name", Values: []string{"api"}},
	})

	var received *logstorage.DataBlock
	wrapped := wrapHiddenFields(func(_ uint, d *logstorage.DataBlock) {
		received = d
	}, nil)
	wrapped(0, db)

	if received == nil {
		t.Fatal("expected DataBlock, got nil")
	}
	cols := received.GetColumns(false)
	if len(cols) != 3 {
		t.Errorf("expected 3 columns, got %d", len(cols))
	}
}

func TestWrapHiddenFields_ExactMatch(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
		{Name: "_msg", Values: []string{"hello"}},
		{Name: "secret_field", Values: []string{"hidden"}},
	})

	var received *logstorage.DataBlock
	wrapped := wrapHiddenFields(func(_ uint, d *logstorage.DataBlock) {
		received = d
	}, []string{"secret_field"})
	wrapped(0, db)

	if received == nil {
		t.Fatal("expected DataBlock, got nil")
	}
	cols := received.GetColumns(false)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns after hiding secret_field, got %d", len(cols))
	}
	for _, col := range cols {
		if col.Name == "secret_field" {
			t.Error("secret_field should have been hidden")
		}
	}
}

func TestWrapHiddenFields_WildcardPrefix(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
		{Name: "k8s.namespace.name", Values: []string{"prod"}},
		{Name: "k8s.pod.name", Values: []string{"api-123"}},
		{Name: "service.name", Values: []string{"api"}},
	})

	var received *logstorage.DataBlock
	wrapped := wrapHiddenFields(func(_ uint, d *logstorage.DataBlock) {
		received = d
	}, []string{"k8s.*"})
	wrapped(0, db)

	if received == nil {
		t.Fatal("expected DataBlock, got nil")
	}
	cols := received.GetColumns(false)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns after hiding k8s.*, got %d", len(cols))
	}
	for _, col := range cols {
		if col.Name == "k8s.namespace.name" || col.Name == "k8s.pod.name" {
			t.Errorf("column %q should have been hidden", col.Name)
		}
	}
}

func TestFilterHiddenValues(t *testing.T) {
	values := []logstorage.ValueWithHits{
		{Value: "service.name", Hits: 10},
		{Value: "k8s.namespace.name", Hits: 5},
		{Value: "k8s.pod.name", Hits: 3},
		{Value: "level", Hits: 2},
	}

	t.Run("no filters", func(t *testing.T) {
		result := filterHiddenValues(values, nil)
		if len(result) != 4 {
			t.Errorf("expected 4 results, got %d", len(result))
		}
	})

	t.Run("exact match", func(t *testing.T) {
		result := filterHiddenValues(values, []string{"level"})
		if len(result) != 3 {
			t.Errorf("expected 3 results, got %d", len(result))
		}
	})

	t.Run("wildcard prefix", func(t *testing.T) {
		result := filterHiddenValues(values, []string{"k8s.*"})
		if len(result) != 2 {
			t.Errorf("expected 2 results after hiding k8s.*, got %d", len(result))
		}
	})
}

func TestRunQuery_PipeProcessing(t *testing.T) {
	store := mockStore{}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context:   context.Background(),
		TenantIDs: []logstorage.TenantID{{AccountID: 0, ProjectID: 0}},
	}

	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	qctx.Query = q

	var called bool
	writeBlock := logstorage.WriteDataBlockFunc(func(_ uint, _ *logstorage.DataBlock) {
		called = true
	})

	err = a.RunQuery(qctx, writeBlock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// mockStore returns no data, so writeBlock shouldn't be called
	if called {
		t.Error("writeBlock should not have been called for empty storage")
	}
}

func TestGetFieldNames_HiddenFieldsFilters(t *testing.T) {
	store := mockStoreWithFields{
		fieldNames: []logstorage.ValueWithHits{
			{Value: "service.name", Hits: 10},
			{Value: "k8s.namespace.name", Hits: 5},
			{Value: "k8s.pod.name", Hits: 3},
			{Value: "level", Hits: 2},
		},
	}
	a := &adapter{store: store}

	qctx := &logstorage.QueryContext{
		Context:             context.Background(),
		HiddenFieldsFilters: []string{"k8s.*"},
	}

	results, err := a.GetFieldNames(qctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results after hiding k8s.*, got %d", len(results))
	}
	for _, r := range results {
		if r.Value == "k8s.namespace.name" || r.Value == "k8s.pod.name" {
			t.Errorf("field %q should have been hidden", r.Value)
		}
	}
}

func TestDeleteActiveTasks_ReturnsTombstones(t *testing.T) {
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{ID: "t-1", Query: "*", Mode: "auto"})
	ts.Add(delete.Tombstone{ID: "t-2", Query: "*", Mode: "hide"})

	a := &adapter{store: mockStore{}, tombstones: ts}
	tasks, err := a.DeleteActiveTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 active tasks, got %d", len(tasks))
	}

	// Verify task IDs are present (order is map-iteration-dependent).
	ids := make(map[string]bool)
	for _, task := range tasks {
		ids[task.TaskID] = true
	}
	if !ids["t-1"] || !ids["t-2"] {
		t.Errorf("expected task IDs t-1 and t-2, got %v", ids)
	}
}

// mockStoreWithValues returns predefined results for stream/field value calls.
type mockStoreWithValues struct {
	mockStore
	fieldValues       []logstorage.ValueWithHits
	streamFieldNames  []logstorage.ValueWithHits
	streamFieldValues []logstorage.ValueWithHits
	streams           []logstorage.ValueWithHits
	streamIDs         []logstorage.ValueWithHits
}

func (m mockStoreWithValues) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return m.fieldValues, nil
}
func (m mockStoreWithValues) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return m.streamFieldNames, nil
}
func (m mockStoreWithValues) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return m.streamFieldValues, nil
}
func (m mockStoreWithValues) GetStreams(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return m.streams, nil
}
func (m mockStoreWithValues) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return m.streamIDs, nil
}

// TestGetFieldValues exercises adapter.GetFieldValues (previously 0%).
func TestGetFieldValues(t *testing.T) {
	store := mockStoreWithValues{
		fieldValues: []logstorage.ValueWithHits{
			{Value: "info", Hits: 100},
			{Value: "error", Hits: 50},
			{Value: "warn", Hits: 25},
		},
	}
	a := &adapter{store: store}
	qctx := &logstorage.QueryContext{Context: context.Background()}

	t.Run("no filter", func(t *testing.T) {
		results, err := a.GetFieldValues(qctx, "level", "", 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 results, got %d", len(results))
		}
	})

	t.Run("substring filter", func(t *testing.T) {
		results, err := a.GetFieldValues(qctx, "level", "err", 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result matching 'err', got %d", len(results))
		}
		if results[0].Value != "error" {
			t.Errorf("expected 'error', got %q", results[0].Value)
		}
	})
}

// TestGetStreamFieldNames exercises adapter.GetStreamFieldNames (previously 0%).
func TestGetStreamFieldNames(t *testing.T) {
	store := mockStoreWithValues{
		streamFieldNames: []logstorage.ValueWithHits{
			{Value: "service.name", Hits: 200},
			{Value: "k8s.namespace.name", Hits: 150},
		},
	}
	a := &adapter{store: store}
	qctx := &logstorage.QueryContext{Context: context.Background()}

	results, err := a.GetStreamFieldNames(qctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

// TestGetStreamFieldValues exercises adapter.GetStreamFieldValues (previously 0%).
func TestGetStreamFieldValues(t *testing.T) {
	store := mockStoreWithValues{
		streamFieldValues: []logstorage.ValueWithHits{
			{Value: "api-gateway", Hits: 300},
			{Value: "auth-service", Hits: 100},
		},
	}
	a := &adapter{store: store}
	qctx := &logstorage.QueryContext{Context: context.Background()}

	t.Run("no filter", func(t *testing.T) {
		results, err := a.GetStreamFieldValues(qctx, "service.name", "", 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("substring filter", func(t *testing.T) {
		results, err := a.GetStreamFieldValues(qctx, "service.name", "api", 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Errorf("expected 1 result matching 'api', got %d", len(results))
		}
	})
}

// TestGetStreams exercises adapter.GetStreams (previously 0%).
func TestGetStreams(t *testing.T) {
	store := mockStoreWithValues{
		streams: []logstorage.ValueWithHits{
			{Value: `{service.name="api"}`, Hits: 500},
			{Value: `{service.name="auth"}`, Hits: 200},
		},
	}
	a := &adapter{store: store}
	qctx := &logstorage.QueryContext{Context: context.Background()}

	results, err := a.GetStreams(qctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 streams, got %d", len(results))
	}
}

// TestGetStreamIDs exercises adapter.GetStreamIDs (previously 0%).
func TestGetStreamIDs(t *testing.T) {
	store := mockStoreWithValues{
		streamIDs: []logstorage.ValueWithHits{
			{Value: "stream-id-001", Hits: 100},
		},
	}
	a := &adapter{store: store}
	qctx := &logstorage.QueryContext{Context: context.Background()}

	results, err := a.GetStreamIDs(qctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 stream ID, got %d", len(results))
	}
}

// TestSetInsertStorage_NoPanic exercises SetInsertStorage (previously 0%).
func TestSetInsertStorage_NoPanic(t *testing.T) {
	// SetInsertStorage registers with VL global state — just ensure no panic.
	w := &mockLogWriter{}
	SetInsertStorage(w)
}

// errStoreForFieldValues returns an error from GetFieldValues and GetStreamFieldValues.
type errStoreForFieldValues struct {
	mockStore
	err error
}

func (e errStoreForFieldValues) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, e.err
}
func (e errStoreForFieldValues) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, e.err
}

// TestGetFieldValues_Error exercises the error branch of GetFieldValues.
func TestGetFieldValues_Error(t *testing.T) {
	a := &adapter{store: errStoreForFieldValues{err: errors.New("storage error")}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	_, err := a.GetFieldValues(qctx, "level", "", 100)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

// TestGetStreamFieldValues_Error exercises the error branch of GetStreamFieldValues.
func TestGetStreamFieldValues_Error(t *testing.T) {
	a := &adapter{store: errStoreForFieldValues{err: errors.New("storage error")}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	_, err := a.GetStreamFieldValues(qctx, "host", "", 100)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

// TestRunQuery_WithPipes exercises the pipe branch of adapter.RunQuery.
func TestRunQuery_WithPipes(t *testing.T) {
	store := mockStore{}
	a := &adapter{store: store}

	// Query with a pipe triggers RunQueryExternal path.
	q, err := logstorage.ParseQuery("* | limit 10")
	if err != nil {
		t.Fatalf("parse pipe query: %v", err)
	}

	qctx := &logstorage.QueryContext{
		Context:    context.Background(),
		TenantIDs:  []logstorage.TenantID{{AccountID: 0, ProjectID: 0}},
		Query:      q,
		QueryStats: &logstorage.QueryStats{},
	}

	err = a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {})
	if err != nil {
		t.Fatalf("RunQuery with pipes: %v", err)
	}
}

// recordingStore wraps mockStore but captures the *logstorage.Query passed
// to RunQuery so tests can assert structural properties of what the
// storage backend actually received.
type recordingStore struct {
	mockStore
	lastQuery *logstorage.Query
	called    bool
}

func (r *recordingStore) RunQuery(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	r.called = true
	r.lastQuery = q
	return nil
}

// TestRunQuery_PreservesPipesToStorage is the structural regression lock for
// the "0 results with tag filter" bug (mirror of the lakehouse-traces fix).
// The adapter MUST pass the FULL query (with pipes intact) to a.store.RunQuery
// so the storage layer's column-projection planning can see fields referenced
// only by pipes (e.g. `| fields _time, trace_id`).
//
// If anyone re-introduces logstorage.CloneWithoutPipes here, the storage
// projection silently drops pipe-referenced columns, the emitted DataBlocks
// lack those fields, and downstream pipes yield zero rows.
//
// Negative-control procedure: in vlstorage.go, change the RunQuery call
// inside the QueryHasPipes branch back to use
// `filterOnly := logstorage.CloneWithoutPipes(qctx.Query)` and pass
// filterOnly. This test MUST fail. Then restore the fix and it MUST pass.
func TestRunQuery_PreservesPipesToStorage(t *testing.T) {
	tests := []struct {
		name     string
		queryStr string
	}{
		{
			name:     "fields pipe (logs trace-correlation shape)",
			queryStr: `service.name:="api-gateway" | fields _time, _msg, trace_id`,
		},
		{
			name:     "stats pipe",
			queryStr: `* | stats count() rows`,
		},
		{
			name:     "limit pipe",
			queryStr: `level:="error" | limit 100`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &recordingStore{}
			a := &adapter{store: rs}

			q, err := logstorage.ParseQuery(tt.queryStr)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			qctx := &logstorage.QueryContext{
				Context:    context.Background(),
				TenantIDs:  []logstorage.TenantID{{AccountID: 0, ProjectID: 0}},
				Query:      q,
				QueryStats: &logstorage.QueryStats{},
			}
			if err := a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {}); err != nil {
				t.Fatalf("RunQuery error: %v", err)
			}
			if !rs.called {
				t.Fatal("expected store.RunQuery to be called")
			}
			if rs.lastQuery == nil {
				t.Fatal("expected store.RunQuery to receive a non-nil query")
			}
			if !logstorage.QueryHasPipes(rs.lastQuery) {
				t.Fatalf("REGRESSION: storage received a pipe-stripped query for %q.\n"+
					"  Storage layer relies on logstorage.GetQueryPipeFields() to expand\n"+
					"  the parquet column projection. Without pipes, fields referenced only\n"+
					"  by pipes (e.g. `| fields trace_id`) are dropped from the projection.",
					tt.queryStr)
			}
		})
	}
}

// TestRunQuery_PipeReferencedFieldsReachProjection verifies the specific
// trace_id projection path: a query whose filter does NOT reference
// trace_id, but whose `| fields _time, trace_id` pipe does. After the fix,
// the storage must receive a query where GetQueryPipeFields includes
// trace_id, so the projection planner can include that parquet column.
func TestRunQuery_PipeReferencedFieldsReachProjection(t *testing.T) {
	rs := &recordingStore{}
	a := &adapter{store: rs}

	queryStr := `service.name:="api-gateway" | fields _time, _msg, trace_id`
	q, err := logstorage.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	qctx := &logstorage.QueryContext{
		Context:    context.Background(),
		TenantIDs:  []logstorage.TenantID{{AccountID: 0, ProjectID: 0}},
		Query:      q,
		QueryStats: &logstorage.QueryStats{},
	}
	if err := a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {}); err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}
	if rs.lastQuery == nil {
		t.Fatal("expected store.RunQuery to receive a non-nil query")
	}

	pipeFields := logstorage.GetQueryPipeFields(rs.lastQuery)
	var sawTraceID bool
	for _, f := range pipeFields {
		if f == "trace_id" {
			sawTraceID = true
		}
	}
	if !sawTraceID {
		t.Errorf("REGRESSION: trace_id missing from GetQueryPipeFields for query %q; "+
			"projection will drop trace_id column. Got pipeFields=%v",
			queryStr, pipeFields)
	}
}
