package vlstorage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

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

type mockStoreWithFields struct {
	mockStore
	fieldNames []logstorage.ValueWithHits
}

func (m mockStoreWithFields) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return m.fieldNames, nil
}

func TestSetStorage_NoPanic(t *testing.T) {
	SetStorage(mockStore{}, nil)
}

func TestGetTenantIDs_ReturnsDefault(t *testing.T) {
	a := &adapter{store: mockStore{}}
	ids, err := a.GetTenantIDs(context.Background(), 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 1 || ids[0].AccountID != 0 || ids[0].ProjectID != 0 {
		t.Errorf("expected default tenant {0,0}, got %v", ids)
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

func TestWrapHiddenFields_NoFilters(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
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
	if len(received.GetColumns(false)) != 2 {
		t.Errorf("expected 2 columns, got %d", len(received.GetColumns(false)))
	}
}

func TestWrapHiddenFields_ExactMatch(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
		{Name: "secret", Values: []string{"hidden"}},
		{Name: "service.name", Values: []string{"api"}},
	})

	var received *logstorage.DataBlock
	wrapped := wrapHiddenFields(func(_ uint, d *logstorage.DataBlock) {
		received = d
	}, []string{"secret"})
	wrapped(0, db)

	if received == nil {
		t.Fatal("expected DataBlock")
	}
	cols := received.GetColumns(false)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
	for _, c := range cols {
		if c.Name == "secret" {
			t.Error("secret should have been hidden")
		}
	}
}

func TestWrapHiddenFields_WildcardPrefix(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"2024-01-01T00:00:00Z"}},
		{Name: "k8s.namespace", Values: []string{"prod"}},
		{Name: "k8s.pod", Values: []string{"api-1"}},
		{Name: "service.name", Values: []string{"api"}},
	})

	var received *logstorage.DataBlock
	wrapped := wrapHiddenFields(func(_ uint, d *logstorage.DataBlock) {
		received = d
	}, []string{"k8s.*"})
	wrapped(0, db)

	if received == nil {
		t.Fatal("expected DataBlock")
	}
	cols := received.GetColumns(false)
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
}

func TestFilterHiddenValues(t *testing.T) {
	values := []logstorage.ValueWithHits{
		{Value: "service.name", Hits: 10},
		{Value: "k8s.namespace", Hits: 5},
		{Value: "k8s.pod", Hits: 3},
		{Value: "level", Hits: 2},
	}

	t.Run("no filters", func(t *testing.T) {
		result := filterHiddenValues(values, nil)
		if len(result) != 4 {
			t.Errorf("expected 4, got %d", len(result))
		}
	})

	t.Run("wildcard", func(t *testing.T) {
		result := filterHiddenValues(values, []string{"k8s.*"})
		if len(result) != 2 {
			t.Errorf("expected 2, got %d", len(result))
		}
	})
}

func TestGetFieldNames_HiddenFieldsFilters(t *testing.T) {
	store := mockStoreWithFields{
		fieldNames: []logstorage.ValueWithHits{
			{Value: "service.name", Hits: 10},
			{Value: "k8s.namespace", Hits: 5},
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
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestDeleteRunTask_RefusedWithoutStoreOrFilter(t *testing.T) {
	f, err := logstorage.ParseFilter("level:error")
	if err != nil {
		t.Fatal(err)
	}
	one := []logstorage.TenantID{{AccountID: 1}}
	a := &adapter{store: mockStore{}, tombstones: nil}
	if err := a.DeleteRunTask(context.Background(), "task-1", time.Now().UnixNano(), one, f); err == nil {
		t.Fatal("a task must be refused while the delete feature (the tombstone store) is off")
	}
	a = &adapter{store: mockStore{}, tombstones: delete.NewTombstoneStore()}
	if err := a.DeleteRunTask(context.Background(), "task-1", time.Now().UnixNano(), one, nil); err == nil {
		t.Fatal("a task without a filter must be refused")
	}
}

// keyListingStore is a storage that lists its objects by tenant, like
// parquets3.Storage.
type keyListingStore struct {
	mockStore
	asked []logstorage.TenantID
}

func (s *keyListingStore) TenantFileKeys(ids []logstorage.TenantID, _, _ int64) []string {
	s.asked = ids
	return []string{"7/3/logs/dt=2026-01-01/hour=00/a.parquet"}
}

// A delete task becomes a tombstone scoped to exactly the task's tenant_ids,
// recording only those tenants' objects; the default delete mode applies.
func TestDeleteRunTask_ScopesTheTombstoneToTheTaskTenants(t *testing.T) {
	SetDeleteTaskMode("permanent")
	t.Cleanup(func() { SetDeleteTaskMode("") })
	ts := delete.NewTombstoneStore()
	st := &keyListingStore{}
	a := &adapter{store: st, tombstones: ts}
	f, err := logstorage.ParseFilter("level:error")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	if err := a.DeleteRunTask(context.Background(), "task-42", now, []logstorage.TenantID{{AccountID: 7, ProjectID: 3}}, f); err != nil {
		t.Fatalf("DeleteRunTask: %v", err)
	}
	got, ok := ts.Get("task-42")
	if !ok {
		t.Fatal("no tombstone registered")
	}
	if !got.ScopedExactlyTo(7, 3) || got.Mode != "permanent" || got.EndNs != now || got.CreatedBy != "delete_task" {
		t.Fatalf("tombstone = %+v, want 7:3 only, mode permanent, ending at the task timestamp", got)
	}
	if len(got.AffectedKeys) != 1 || len(st.asked) != 1 || st.asked[0] != (logstorage.TenantID{AccountID: 7, ProjectID: 3}) {
		t.Fatalf("affected keys %v listed for tenants %v, want only 7:3's objects", got.AffectedKeys, st.asked)
	}

	// No tenants: accepted, nothing created (it would act on every tenant).
	if err := a.DeleteRunTask(context.Background(), "task-none", now, nil, f); err != nil {
		t.Fatalf("a task naming no tenant must be accepted like upstream: %v", err)
	}
	if ts.Count() != 1 {
		t.Fatalf("tombstones = %d, want 1", ts.Count())
	}
	// A registered id is refused, as upstream.
	if err := a.DeleteRunTask(context.Background(), "task-42", now, []logstorage.TenantID{{AccountID: 1}}, f); !errors.Is(err, delete.ErrTaskExists) {
		t.Fatalf("duplicate task id: err = %v, want ErrTaskExists", err)
	}
}

// active_tasks reports tombstones in upstream's DeleteTask shape.
func TestDeleteActiveTasks_UpstreamShape(t *testing.T) {
	ts := delete.NewTombstoneStore()
	created := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	ts.Add(delete.Tombstone{ID: "scoped", Query: "level:error", EndNs: 1, CreatedAt: created, Mode: "hide", Tenants: []delete.TenantRef{{AccountID: 7, ProjectID: 3}}})
	ts.Add(delete.Tombstone{ID: "legacy", Query: "*", EndNs: 1, CreatedAt: created.Add(time.Hour), Mode: "hide"})
	a := &adapter{store: mockStore{}, tombstones: ts}
	tasks, err := a.DeleteActiveTasks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 || tasks[0].TaskID != "scoped" || tasks[1].TaskID != "legacy" {
		t.Fatalf("tasks = %+v, want scoped then legacy (oldest first)", tasks)
	}
	if tasks[0].Filter != "level:error" || !tasks[0].StartTime.Equal(created) ||
		len(tasks[0].TenantIDs) != 1 || tasks[0].TenantIDs[0] != (logstorage.TenantID{AccountID: 7, ProjectID: 3}) {
		t.Fatalf("scoped task = %+v", tasks[0])
	}
	if len(tasks[1].TenantIDs) != 0 {
		t.Fatalf("an instance-wide legacy record has no tenants to list, got %v", tasks[1].TenantIDs)
	}
	data := string(logstorage.MarshalDeleteTasksToJSON(tasks))
	if !strings.Contains(data, `"task_id":"scoped"`) || !strings.Contains(data, `"filter":"level:error"`) {
		t.Fatalf("upstream JSON = %s", data)
	}
}

func TestDeleteStopTask_RemovesTombstone(t *testing.T) {
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{ID: "t-1", Query: "*", Mode: "auto"})
	a := &adapter{store: mockStore{}, tombstones: ts}

	err := a.DeleteStopTask(context.Background(), "t-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ts.Count() != 0 {
		t.Errorf("expected 0 tombstones, got %d", ts.Count())
	}
}

func TestDeleteActiveTasks_NilTombstones(t *testing.T) {
	a := &adapter{store: mockStore{}, tombstones: nil}
	tasks, err := a.DeleteActiveTasks(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tasks != nil {
		t.Errorf("expected nil, got %v", tasks)
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
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
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
// the "0 results with tag filter" bug (mirror of the vlstorage and
// vtstorage_adapter fixes — see those packages' tests).
// The adapter MUST pass the FULL query (with pipes intact) to a.store.RunQuery
// so the storage layer's column-projection planning can see fields referenced
// only by pipes.
//
// Negative-control procedure: in vlstorage.go (this package), change the
// RunQuery call inside the QueryHasPipes branch back to use
// `filterOnly := logstorage.CloneWithoutPipes(qctx.Query)` and pass
// filterOnly. This test MUST fail. Then restore the fix and it MUST pass.
func TestRunQuery_PreservesPipesToStorage(t *testing.T) {
	tests := []struct {
		name     string
		queryStr string
	}{
		{
			name:     "fields pipe (trace correlation shape)",
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
				Context:   context.Background(),
				TenantIDs: []logstorage.TenantID{{AccountID: 0, ProjectID: 0}},
				Query:     q,
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

// TestDeleteStopTask_RefusedWhileARewriteIsUnfinished: stopping a delete task
// removes its tombstone, and a tombstone carries the durable record of any
// rewrite of its files that has not finished. Dropping it mid-rewrite loses the
// only trace of a replacement object — the next manifest refresh would adopt it
// next to its source — so the stop is refused until the rewrite settles, the
// same as the delete API's un-delete.
func TestDeleteStopTask_RefusedWhileARewriteIsUnfinished(t *testing.T) {
	ts := delete.NewTombstoneStore()
	ts.Add(delete.Tombstone{ID: "task-busy", Query: "*", Mode: "auto",
		Superseded: map[string]delete.Supersession{
			"logs/dt=2026-03-01/hour=07/src.parquet": {NewKey: "logs/dt=2026-03-01/hour=07/repl.parquet", State: delete.SupersessionPublished},
		}})
	a := &adapter{store: mockStore{}, tombstones: ts}

	if err := a.DeleteStopTask(context.Background(), "task-busy"); err == nil {
		t.Fatal("stopping a task whose rewrite is unfinished must fail")
	}
	if _, ok := ts.Get("task-busy"); !ok {
		t.Fatal("a refused stop must keep the tombstone and its rewrite record")
	}
	// Stopping a task that does not exist stays a no-op.
	if err := a.DeleteStopTask(context.Background(), "no-such-task"); err != nil {
		t.Fatalf("stopping an unknown task: %v", err)
	}
}

// namesErrStore fails the field-name enumerations, so the adapter's error
// propagation is exercised.
type namesErrStore struct {
	mockStore
	err error
}

func (s namesErrStore) GetFieldNames(context.Context, []logstorage.TenantID, *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, s.err
}

func (s namesErrStore) GetStreamFieldNames(context.Context, []logstorage.TenantID, *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, s.err
}

func TestFieldNames_PropagateStorageErrors(t *testing.T) {
	want := errors.New("storage error")
	a := &adapter{store: namesErrStore{err: want}}
	qctx := &logstorage.QueryContext{Context: context.Background()}
	if got, err := a.GetFieldNames(qctx, ""); !errors.Is(err, want) || got != nil {
		t.Errorf("GetFieldNames = %v, %v; want the storage error", got, err)
	}
	if got, err := a.GetStreamFieldNames(qctx, ""); !errors.Is(err, want) || got != nil {
		t.Errorf("GetStreamFieldNames = %v, %v; want the storage error", got, err)
	}
}

func TestFilterValuesBySubstring(t *testing.T) {
	in := []logstorage.ValueWithHits{{Value: "checkout"}, {Value: "cart"}, {Value: "payments"}}
	if got := filterValuesBySubstring(in, ""); len(got) != 3 {
		t.Errorf("empty filter must keep every value, got %v", got)
	}
	got := filterValuesBySubstring(in, "ca")
	if len(got) != 1 || got[0].Value != "cart" {
		t.Errorf("filter %q = %v, want [cart]", "ca", got)
	}
}
