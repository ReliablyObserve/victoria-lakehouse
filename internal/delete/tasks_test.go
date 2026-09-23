package delete

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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
	err := RunTask(store, files, false, "task-1", 5000, []TenantRef{{AccountID: 7, ProjectID: 3}, {AccountID: 7, ProjectID: 3}}, "level:error", "auto")
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
	if err := RunTask(nil, nil, false, "t", 1, []TenantRef{{}}, "*", ""); err == nil {
		t.Error("a task must be refused while the delete feature is off")
	}

	store := NewTombstoneStore()
	if err := RunTask(store, nil, false, "none", 1, nil, "*", ""); err != nil {
		t.Errorf("a task naming no tenant is accepted like upstream, got %v", err)
	}
	if store.Count() != 0 {
		t.Fatal("a task naming no tenant must create nothing: an unscoped tombstone acts on every tenant")
	}

	if err := RunTask(store, nil, false, "hide-default", 1, []TenantRef{{}}, "*", ""); err != nil {
		t.Fatal(err)
	}
	if ts, _ := store.Get("hide-default"); ts.Mode != "hide" || len(ts.AffectedKeys) != 0 {
		t.Errorf("no mode configured must mean hide (reversible), got %q; no lister must mean no keys, got %v", ts.Mode, ts.AffectedKeys)
	}

	err := RunTask(store, nil, false, "hide-default", 1, []TenantRef{{AccountID: 9}}, "*", "")
	if !errors.Is(err, ErrTaskExists) || err.Error() != `the delete task with task_id="hide-default" is already registered` {
		t.Errorf("duplicate id: err = %v, want upstream's refusal", err)
	}
	if ts, _ := store.Get("hide-default"); !ts.ScopedExactlyTo(0, 0) {
		t.Errorf("a refused duplicate changed the registered task: %v", ts.Tenants)
	}

	if err := RunTask(store, nil, false, "bad", 1, []TenantRef{{}}, "level:(", "hide"); err == nil {
		t.Error("an unparseable filter must be refused")
	}
	if err := RunTask(store, nil, false, "badmode", 1, []TenantRef{{}}, "*", "shred"); err == nil {
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
			if err := RunTask(store, nil, false, "same", 1, []TenantRef{{AccountID: uint32(i + 1)}}, "*", "hide"); err == nil {
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
	if err := RunTask(store, nil, false, "durable", 1, []TenantRef{{AccountID: 5}}, "*", "hide"); err != nil {
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
	ctx := context.Background() // no caller: the cluster protocol, unscoped
	if ActiveTasks(ctx, nil) != nil || StopTask(ctx, nil, "x") != nil {
		t.Fatal("nil store: no tasks, stop is a no-op")
	}
	store := NewTombstoneStore()
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for i, id := range []string{"b", "a", "c"} {
		store.Add(Tombstone{ID: id, Query: "*", EndNs: 1, CreatedAt: base.Add(time.Duration(i%2) * time.Hour), Mode: "hide",
			Tenants: []TenantRef{{AccountID: uint32(i)}}})
	}
	tasks := ActiveTasks(ctx, store)
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

	if err := StopTask(ctx, store, "unknown"); err != nil {
		t.Fatalf("stopping an unknown task is a no-op upstream, got %v", err)
	}
	if err := StopTask(ctx, store, "a"); err != nil || store.Count() != 2 {
		t.Fatalf("StopTask: err=%v count=%d", err, store.Count())
	}
	store.Update("b", func(ts *Tombstone) bool {
		ts.Superseded = map[string]Supersession{"k": {State: SupersessionPrepared, At: time.Now()}}
		return true
	})
	if err := StopTask(ctx, store, "b"); !errors.Is(err, ErrRewriteInProgress) {
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
	if ts, _ := store.Get("x"); ts.ScopedExactlyTo(1, 0) {
		t.Fatal("a refused add replaced the record")
	}
	// A removed id can be registered again: its removal marker no longer applies.
	store.Remove("x")
	if !store.AddIfAbsent(sampleTombstone("x")) {
		t.Fatal("re-registering a removed id must succeed")
	}
}

// publicTaskServer is the public delete API as the binaries mount it: the
// caller is put on the context, and the storage calls (here, StopTask /
// ActiveTasks directly, as the adapters make them) act for that caller.
func publicTaskServer(store *TombstoneStore) http.Handler {
	auth := func(r *http.Request) bool { return r.Header.Get("X-Global") == "yes" }
	return ScopeTaskRequests(auth, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/delete/active_tasks":
			_, _ = w.Write(logstorage.MarshalDeleteTasksToJSON(ActiveTasks(r.Context(), store)))
		case "/delete/stop_task":
			if err := StopTask(r.Context(), store, r.FormValue("task_id")); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	})
}

func taskRequest(h http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// Tenant B can neither list nor stop tenant A's task; stopping it answers
// byte for byte what stopping an unknown task answers. The operator sees and
// stops every task; an unparseable tenant sees none.
func TestPublicTasks_AreTenantScoped(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(scopedSample("task-a", TenantRef{AccountID: 1}))
	store.Add(scopedSample("task-b", TenantRef{AccountID: 2}))
	store.Add(scopedSample("task-ab", TenantRef{AccountID: 1}, TenantRef{AccountID: 2}))
	srv := publicTaskServer(store)
	asA := map[string]string{"AccountID": "1"}
	asB := map[string]string{"AccountID": "2"}

	if body := taskRequest(srv, "/delete/active_tasks", asB).Body.String(); !strings.Contains(body, `"task-b"`) ||
		strings.Contains(body, `"task-a"`) || strings.Contains(body, `"task-ab"`) {
		t.Fatalf("tenant 2 lists %s, want only its own task", body)
	}
	if body := taskRequest(srv, "/delete/active_tasks", map[string]string{"AccountID": "x"}).Body.String(); body != "[]" {
		t.Fatalf("an unparseable tenant lists %s, want nothing", body)
	}

	foreign := taskRequest(srv, "/delete/stop_task?task_id=task-a", asB)
	unknown := taskRequest(srv, "/delete/stop_task?task_id=no-such-task", asB)
	if foreign.Code != unknown.Code || foreign.Body.String() != unknown.Body.String() {
		t.Fatalf("foreign-id answer %d %q differs from unknown-id answer %d %q", foreign.Code, foreign.Body.String(), unknown.Code, unknown.Body.String())
	}
	if _, ok := store.Get("task-a"); !ok {
		t.Fatal("tenant 2 stopped tenant 1's task")
	}
	if taskRequest(srv, "/delete/stop_task?task_id=task-ab", asA); store.Count() != 3 {
		t.Fatal("a tenant stopped a task that spans another tenant")
	}
	if taskRequest(srv, "/delete/stop_task?task_id=task-a", asA); store.Count() != 2 {
		t.Fatal("tenant 1 could not stop its own task")
	}

	global := map[string]string{"X-Global": "yes"}
	if body := taskRequest(srv, "/delete/active_tasks", global).Body.String(); !strings.Contains(body, `"task-b"`) || !strings.Contains(body, `"task-ab"`) {
		t.Fatalf("the operator lists %s, want every task", body)
	}
	taskRequest(srv, "/delete/stop_task?task_id=task-ab", global)
	taskRequest(srv, "/delete/stop_task?task_id=task-b", global)
	if store.Count() != 0 {
		t.Fatalf("the operator could not stop every task; %d left", store.Count())
	}
}

// active_tasks reports a task's start_time as upstream does: the task's own
// timestamp, not the time this node accepted it.
func TestActiveTasks_StartTimeIsTheTaskTimestamp(t *testing.T) {
	store := NewTombstoneStore()
	at := time.Date(2026, 9, 1, 12, 0, 0, 123, time.UTC).UnixNano()
	if err := RunTask(store, nil, false, "t1", at, []TenantRef{{AccountID: 1}}, "*", "hide"); err != nil {
		t.Fatal(err)
	}
	tasks := ActiveTasks(context.Background(), store)
	if len(tasks) != 1 || !tasks[0].StartTime.Equal(time.Unix(0, at).UTC()) || tasks[0].StartTime.Location() != time.UTC {
		t.Fatalf("start_time = %v, want the task timestamp %v", tasks[0].StartTime, time.Unix(0, at).UTC())
	}
	if ts, _ := store.Get("t1"); time.Since(ts.CreatedAt) > time.Minute {
		t.Fatalf("CreatedAt %v must stay this node's accept time (the un-delete window runs from it)", ts.CreatedAt)
	}
}

// Upstream registers a task naming no tenant (it deletes nothing) and lists it
// in active_tasks; the lakehouse creates nothing for it, so it is not listed.
func TestRunTask_NoTenantsIsAcceptedButNotListed(t *testing.T) {
	store := NewTombstoneStore()
	if err := RunTask(store, nil, false, "none", 1, nil, "*", "hide"); err != nil {
		t.Fatalf("a task naming no tenant must be accepted: %v", err)
	}
	if tasks := ActiveTasks(context.Background(), store); len(tasks) != 0 {
		t.Fatalf("active_tasks = %+v, want nothing (no tombstone is created)", tasks)
	}
}

// A task's relative time filter is evaluated at the task's timestamp, as
// upstream does, and does not drift with the clock.
func TestRunTask_RelativeTimeFilterIsPinnedToTheTaskTimestamp(t *testing.T) {
	store := NewTombstoneStore()
	at := time.Now().Add(-2 * time.Hour).UnixNano()
	if err := RunTask(store, nil, false, "rel", at, []TenantRef{{}}, "_time:5m", "hide"); err != nil {
		t.Fatal(err)
	}
	ts, _ := store.Get("rel")
	if ts.FilterAt != at {
		t.Fatalf("FilterAt = %d, want the task timestamp %d", ts.FilterAt, at)
	}
	row := map[string]string{"_msg": "x"}
	inWindow := at - int64(time.Minute)
	if !ts.MatchesRow(withTime(row, inWindow), inWindow) {
		t.Error("a row one minute before the task must match _time:5m evaluated at the task timestamp")
	}
	stale := at - int64(10*time.Minute)
	if ts.MatchesRow(withTime(row, stale), stale) {
		t.Error("a row ten minutes before the task must not match _time:5m")
	}
	recent := time.Now().Add(-time.Minute).UnixNano()
	if ts.MatchesRow(withTime(row, recent), recent) {
		t.Error("_time:5m drifted to the current clock: a row written after the task matched")
	}

	// The same query issued through the lakehouse API is pinned too, and the
	// pin survives persistence.
	pinned := Tombstone{ID: "p", Query: "_time:5m", StartNs: 0, EndNs: time.Now().UnixNano(), Mode: "hide",
		Tenants: []TenantRef{{}}, FilterAt: at}
	if err := pinned.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if pinned.Filter() == nil || pinned.MatchesRow(withTime(row, recent), recent) {
		t.Error("a pinned tombstone matched outside its evaluation window")
	}
}

func withTime(row map[string]string, ns int64) map[string]string {
	out := make(map[string]string, len(row)+1)
	for k, v := range row {
		out[k] = v
	}
	out["_time"] = time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
	return out
}

type accountOnlyManifest struct{ only bool }

func (m accountOnlyManifest) AccountOnlyTenantKeys() bool { return m.only }

// Where object keys carry the account alone ({OrgID} layout), a delete scoped
// to a ProjectID other than 0 could not be applied to that project only, so it
// is refused instead of recorded with a scope it cannot honour.
func TestKeyLayout_AccountOnlyRefusesNonZeroProject(t *testing.T) {
	if !AccountOnlyKeys(accountOnlyManifest{only: true}) || AccountOnlyKeys(accountOnlyManifest{}) || AccountOnlyKeys(struct{}{}) {
		t.Fatal("AccountOnlyKeys must report the layout, false when unknown")
	}
	if err := CheckTenantsForKeyLayout([]TenantRef{{AccountID: 5, ProjectID: 3}}, true); !errors.Is(err, ErrProjectNotInKeyLayout) {
		t.Fatalf("err = %v, want ErrProjectNotInKeyLayout", err)
	}
	if err := CheckTenantsForKeyLayout([]TenantRef{{AccountID: 5}}, true); err != nil {
		t.Fatalf("ProjectID 0 must be accepted: %v", err)
	}
	if err := CheckTenantsForKeyLayout([]TenantRef{{AccountID: 5, ProjectID: 3}}, false); err != nil {
		t.Fatalf("the two-segment layout carries the project: %v", err)
	}
	store := NewTombstoneStore()
	if err := RunTask(store, nil, true, "t", 1, []TenantRef{{AccountID: 5, ProjectID: 3}}, "*", "hide"); !errors.Is(err, ErrProjectNotInKeyLayout) || store.Count() != 0 {
		t.Fatalf("RunTask err = %v (tombstones %d), want a refusal and nothing registered", err, store.Count())
	}
}
