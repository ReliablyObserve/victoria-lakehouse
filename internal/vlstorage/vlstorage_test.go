package vlstorage

import (
	"context"
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

