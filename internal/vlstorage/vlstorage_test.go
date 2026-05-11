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
func (mockStore) Close() error { return nil }

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

