package delete

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestRunTask_RegistersATenantScopedTombstone(t *testing.T) {
	store := NewTombstoneStore()
	var asked []TenantRef
	files := func(tenants []TenantRef, startNs, endNs int64) []string {
		asked = tenants
		if startNs != math.MinInt64 || endNs != 5000 {
			t.Errorf("files listed for [%d, %d], want (-inf, task timestamp]", startNs, endNs)
		}
		return []string{"7/3/logs/dt=x/a.parquet"}
	}
	err := RunTask(store, files, "task-1", 5000, []TenantRef{{AccountID: 7, ProjectID: 3}, {AccountID: 7, ProjectID: 3}}, "level:error", "auto")
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	ts, ok := store.Get("task-1")
	if !ok {
		t.Fatal("no tombstone registered")
	}
	if !ts.ScopedExactlyTo(7, 3) || ts.Mode != "auto" || ts.Query != "level:error" || ts.EndNs != 5000 || ts.StartNs != math.MinInt64 {
		t.Fatalf("tombstone = %+v", ts)
	}
	if len(asked) != 1 || len(ts.AffectedKeys) != 1 {
		t.Fatalf("files were listed for %v (want the normalized 7:3), affected=%v", asked, ts.AffectedKeys)
	}
	if time.Since(ts.CreatedAt) > time.Minute {
		t.Errorf("CreatedAt %v is not this node's accept time", ts.CreatedAt)
	}
}

func TestRunTask_EdgeCases(t *testing.T) {
	if err := RunTask(nil, nil, "t", 1, []TenantRef{{}}, "*", ""); err == nil {
		t.Error("a task must be refused while the delete feature is off")
	}

	store := NewTombstoneStore()
	if err := RunTask(store, nil, "none", 1, nil, "*", ""); err != nil {
		t.Errorf("a task naming no tenant is accepted like upstream, got %v", err)
	}
	if store.Count() != 0 {
		t.Fatal("a task naming no tenant must create nothing: an unscoped tombstone acts on every tenant")
	}

	if err := RunTask(store, nil, "hide-default", 1, []TenantRef{{}}, "*", ""); err != nil {
		t.Fatal(err)
	}
	if ts, _ := store.Get("hide-default"); ts.Mode != "hide" || len(ts.AffectedKeys) != 0 {
		t.Errorf("no mode configured must mean hide (reversible), got %q; no lister must mean no keys, got %v", ts.Mode, ts.AffectedKeys)
	}

	err := RunTask(store, nil, "hide-default", 1, []TenantRef{{AccountID: 9}}, "*", "")
	if !errors.Is(err, ErrTaskExists) || err.Error() != `the delete task with task_id="hide-default" is already registered` {
		t.Errorf("duplicate id: err = %v, want upstream's refusal", err)
	}
	if ts, _ := store.Get("hide-default"); !ts.ScopedExactlyTo(0, 0) {
		t.Errorf("a refused duplicate changed the registered task: %v", ts.Tenants)
	}

	if err := RunTask(store, nil, "bad", 1, []TenantRef{{}}, "level:(", "hide"); err == nil {
		t.Error("an unparseable filter must be refused")
	}
	if err := RunTask(store, nil, "badmode", 1, []TenantRef{{}}, "*", "shred"); err == nil {
		t.Error("an unknown mode must be refused")
	}
}

// Two registrations of the same id race: exactly one wins, the other gets
// upstream's refusal, and the stored scope is the winner's.
func TestRunTask_ConcurrentSameIDRegistersOnce(t *testing.T) {
	store := NewTombstoneStore()
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := RunTask(store, nil, "same", 1, []TenantRef{{AccountID: uint32(i + 1)}}, "*", "hide"); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			} else if !errors.Is(err, ErrTaskExists) {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d registrations of one id succeeded, want 1", wins)
	}
	if ts, _ := store.Get("same"); len(ts.Tenants) != 1 {
		t.Fatalf("stored scope = %v, want one tenant", ts.Tenants)
	}
}

// A delete task survives a crash with its scope: it is persisted like any
// other tombstone.
func TestRunTask_PersistsTheScope(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()
	store := NewTombstoneStore()
	store.EnablePersistence(PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"})
	if err := RunTask(store, nil, "durable", 1, []TenantRef{{AccountID: 5}}, "*", "hide"); err != nil {
		t.Fatal(err)
	}
	store.FlushPending(context.Background())

	restored := NewTombstoneStore()
	if _, err := restored.Restore(context.Background(), PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if ts, ok := restored.Get("durable"); !ok || !ts.ScopedExactlyTo(5, 0) {
		t.Fatalf("restored task = %+v, want scoped to 5:0", ts)
	}
}

func TestActiveTasksAndStopTask(t *testing.T) {
	if ActiveTasks(nil) != nil || StopTask(nil, "x") != nil {
		t.Fatal("nil store: no tasks, stop is a no-op")
	}
	store := NewTombstoneStore()
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for i, id := range []string{"b", "a", "c"} {
		store.Add(Tombstone{ID: id, Query: "*", EndNs: 1, CreatedAt: base.Add(time.Duration(i%2) * time.Hour), Mode: "hide",
			Tenants: []TenantRef{{AccountID: uint32(i)}}})
	}
	tasks := ActiveTasks(store)
	var order string
	for _, task := range tasks {
		order += task.TaskID
	}
	if order != "bca" {
		t.Fatalf("task order = %q, want oldest first, ties by id (b, c, then a)", order)
	}
	if tasks[0].TenantIDs[0] != (logstorage.TenantID{AccountID: 0}) || tasks[2].TenantIDs[0] != (logstorage.TenantID{AccountID: 1}) {
		t.Fatalf("tenants not carried: %+v", tasks)
	}

	if err := StopTask(store, "unknown"); err != nil {
		t.Fatalf("stopping an unknown task is a no-op upstream, got %v", err)
	}
	if err := StopTask(store, "a"); err != nil || store.Count() != 2 {
		t.Fatalf("StopTask: err=%v count=%d", err, store.Count())
	}
	store.Update("b", func(ts *Tombstone) bool {
		ts.Superseded = map[string]Supersession{"k": {State: SupersessionPrepared, At: time.Now()}}
		return true
	})
	if err := StopTask(store, "b"); !errors.Is(err, ErrRewriteInProgress) {
		t.Fatalf("stopping a task mid-rewrite: err = %v, want ErrRewriteInProgress", err)
	}
}

func TestTenantConversions(t *testing.T) {
	if TenantRefsOf(nil) != nil || TenantIDsOf(nil) != nil {
		t.Fatal("empty in, nil out")
	}
	ids := []logstorage.TenantID{{AccountID: 1, ProjectID: 2}, {AccountID: 3}}
	back := TenantIDsOf(TenantRefsOf(ids))
	if len(back) != 2 || back[0] != ids[0] || back[1] != ids[1] {
		t.Fatalf("round trip = %v", back)
	}
}

type fakeLister struct{ calls int }

func (f *fakeLister) TenantFileKeys(ids []logstorage.TenantID, _, _ int64) []string {
	f.calls++
	return []string{strconv.Itoa(len(ids))}
}

func TestTaskFilesOf(t *testing.T) {
	if TaskFilesOf(struct{}{}) != nil {
		t.Fatal("a storage without a tenant listing gives no TaskFiles")
	}
	l := &fakeLister{}
	files := TaskFilesOf(l)
	if got := files([]TenantRef{{AccountID: 1}, {AccountID: 2}}, 0, 1); len(got) != 1 || got[0] != "2" || l.calls != 1 {
		t.Fatalf("TaskFilesOf did not delegate: %v", got)
	}
}

func TestAddIfAbsent(t *testing.T) {
	store := NewTombstoneStore()
	if !store.AddIfAbsent(sampleTombstone("x")) {
		t.Fatal("first add must succeed")
	}
	if store.AddIfAbsent(scopedSample("x", TenantRef{AccountID: 1})) {
		t.Fatal("second add of the same id must be refused")
	}
	if ts, _ := store.Get("x"); ts.Scoped() {
		t.Fatal("a refused add replaced the record")
	}
	// A removed id can be registered again: its removal marker no longer applies.
	store.Remove("x")
	if !store.AddIfAbsent(sampleTombstone("x")) {
		t.Fatal("re-registering a removed id must succeed")
	}
}
