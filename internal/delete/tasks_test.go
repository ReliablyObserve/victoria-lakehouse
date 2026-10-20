package delete

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

func TestRunTask_RegistersATenantScopedTombstone(t *testing.T) {
	b := deletetest.NewBucket()
	store, _ := newSyncedPod(t, b, "pod")
	const taskNs = 1792310275123456789
	const id = "1792310275123456789" // upstream's task id is its timestamp
	err := RunTask(context.Background(), store, false, id, taskNs, []TenantRef{{AccountID: 7, ProjectID: 3}, {AccountID: 7, ProjectID: 3}}, "level:error", "auto")
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	ts, ok := store.Get(id)
	if !ok {
		t.Fatal("no tombstone registered")
	}
	if !ts.ScopedExactlyTo(7, 3) || ts.Mode != "auto" || ts.Query != "level:error" || ts.EndNs != taskNs || ts.StartNs != math.MinInt64 || len(ts.Tenants) != 1 {
		t.Fatalf("tombstone = %+v", ts)
	}
	// The definition key is the task's own timestamp, so every node that
	// receives the task writes the same key.
	want := TombstonePrefix(testPrefix) + "t" + hexTime(taskNs) + "-" + id + ".json"
	if _, ok := b.Body(want); !ok {
		t.Fatalf("keys = %v, want %s", b.Keys(""), want)
	}
	if ts.CreatedAt.UnixNano() != taskNs {
		t.Errorf("CreatedAt = %v, want the task timestamp", ts.CreatedAt)
	}
}

func TestRunTask_EdgeCases(t *testing.T) {
	ctx := context.Background()
	if err := RunTask(ctx, nil, false, "t", 1, []TenantRef{{}}, "*", ""); err == nil {
		t.Error("a task must be refused while the delete feature is off")
	}

	store := newTestStore(t)
	if err := RunTask(ctx, store, false, "none", 1, nil, "*", ""); err != nil {
		t.Errorf("a task naming no tenant is accepted like upstream, got %v", err)
	}
	if store.Count() != 0 {
		t.Fatal("a task naming no tenant must create nothing: an unscoped tombstone acts on every tenant")
	}

	if err := RunTask(ctx, store, false, "hide-default", 1, []TenantRef{{}}, "*", ""); err != nil {
		t.Fatal(err)
	}
	if ts, _ := store.Get("hide-default"); ts.Mode != "hide" {
		t.Errorf("no mode configured must mean hide (reversible), got %q", ts.Mode)
	}

	err := RunTask(ctx, store, false, "hide-default", 1, []TenantRef{{AccountID: 9}}, "*", "")
	if !errors.Is(err, ErrTaskExists) || err.Error() != `the delete task with task_id="hide-default" is already registered` {
		t.Errorf("duplicate id with different content: err = %v, want upstream's refusal", err)
	}
	if ts, _ := store.Get("hide-default"); !ts.ScopedExactlyTo(0, 0) {
		t.Errorf("a refused duplicate changed the registered task: %v", ts.Tenants)
	}

	if err := RunTask(ctx, store, false, "bad", 1, []TenantRef{{}}, "level:(", "hide"); err == nil {
		t.Error("an unparseable filter must be refused")
	}
	if err := RunTask(ctx, store, false, "badmode", 1, []TenantRef{{}}, "*", "shred"); err == nil {
		t.Error("an unknown mode must be refused")
	}
}

// The same task delivered twice (the vlselect fan-out of one task to several
// nodes, or a retry) writes the same key with the same bytes: the second is a
// success, not an error. This is a documented difference from upstream, whose
// node-local registry refuses a repeated task id.
func TestRunTask_IdenticalTaskIsIdempotent(t *testing.T) {
	b := deletetest.NewBucket()
	podA, _ := newSyncedPod(t, b, "a")
	podB, _ := newSyncedPod(t, b, "b")
	ctx := context.Background()
	for _, s := range []*TombstoneStore{podA, podB, podA} {
		if err := RunTask(ctx, s, false, "fanout", 9_000, []TenantRef{{AccountID: 5}}, "level:error", "hide"); err != nil {
			t.Fatalf("RunTask: %v", err)
		}
	}
	if keys := b.Keys(TombstonePrefix(testPrefix)); len(keys) != 1 {
		t.Fatalf("keys = %v, want exactly one definition", keys)
	}
}

// Two registrations of the same id race: exactly one wins, the other gets
// upstream's refusal, and the stored scope is the winner's.
func TestRunTask_ConcurrentSameIDRegistersOnce(t *testing.T) {
	b := deletetest.NewBucket()
	var stores []*TombstoneStore
	for i := 0; i < 4; i++ {
		s, _ := newSyncedPod(t, b, "pod")
		stores = append(stores, s)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := RunTask(context.Background(), stores[i%len(stores)], false, "same", 1, []TenantRef{{AccountID: uint32(i + 1)}}, "*", "hide"); err == nil {
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
	if keys := b.Keys(TombstonePrefix(testPrefix)); len(keys) != 1 {
		t.Fatalf("keys = %v, want exactly one object", keys)
	}
}

// A delete task survives the loss of the pod and its disk: it is a key in the
// bucket, and a pod that starts empty learns it.
func TestRunTask_AFreshPodLearnsTheScope(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	if err := RunTask(context.Background(), writer, false, "durable", 1, []TenantRef{{AccountID: 5}}, "*", "hide"); err != nil {
		t.Fatal(err)
	}
	fresh, _ := newSyncedPod(t, b, "fresh")
	if ts, ok := fresh.Get("durable"); !ok || !ts.ScopedExactlyTo(5, 0) {
		t.Fatalf("task on a fresh pod = %+v, want scoped to 5:0", ts)
	}
}

// A Lakehouse task is complete once it is registered: active_tasks lists
// nothing (upstream's list holds pending and running tasks, which are gone a
// second after run_task) and stop_task is a no-op that answers ok.
func TestActiveTasksAndStopTask(t *testing.T) {
	ctx := context.Background()
	if got := ActiveTasks(ctx, nil); got == nil || len(got) != 0 {
		t.Fatalf("nil store: active tasks = %v, want an empty list", got)
	}
	if StopTask(ctx, nil, "x") != nil {
		t.Fatal("nil store: stop is a no-op")
	}
	store := newTestStore(t)
	for i, id := range []string{"b", "a", "c"} {
		mustCreate(t, store, Tombstone{ID: id, Query: "*", EndNs: 1, Mode: "hide", Tenants: []TenantRef{{AccountID: uint32(i)}}})
	}
	if tasks := ActiveTasks(ctx, store); tasks == nil || len(tasks) != 0 {
		t.Fatalf("active tasks = %v, want an empty list: a registered task is finished", tasks)
	}
	if body := logstorage.MarshalDeleteTasksToJSON(ActiveTasks(ctx, store)); string(body) != "[]" {
		t.Fatalf("active_tasks answers %s, want []", body)
	}
	for _, id := range []string{"unknown", "a", "a"} {
		if err := StopTask(ctx, store, id); err != nil {
			t.Fatalf("stop_task %s = %v, want ok", id, err)
		}
	}
	if store.Count() != 3 {
		t.Fatalf("stop_task changed the view: %d tombstones left of 3", store.Count())
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

// The public task endpoints answer the same to every caller: active_tasks is
// empty and stop_task answers ok and changes nothing, whether the id is the
// caller's own, another tenant's or unknown, so a task's existence leaks nowhere.
func TestPublicTasks_AnswerTheSameToEveryCaller(t *testing.T) {
	store := newTestStore(t)
	mustCreate(t, store, scopedSample("task-a", TenantRef{AccountID: 1}))
	mustCreate(t, store, scopedSample("task-b", TenantRef{AccountID: 2}))
	srv := publicTaskServer(store)
	asA := map[string]string{"AccountID": "1"}
	asB := map[string]string{"AccountID": "2"}
	global := map[string]string{"X-Global": "yes"}

	for name, h := range map[string]map[string]string{"tenant 1": asA, "tenant 2": asB, "operator": global, "bad tenant": {"AccountID": "x"}} {
		if body := taskRequest(srv, "/delete/active_tasks", h).Body.String(); body != "[]" {
			t.Fatalf("%s lists %s, want nothing", name, body)
		}
	}
	foreign := taskRequest(srv, "/delete/stop_task?task_id=task-a", asB)
	unknown := taskRequest(srv, "/delete/stop_task?task_id=no-such-task", asB)
	own := taskRequest(srv, "/delete/stop_task?task_id=task-a", asA)
	for name, rec := range map[string]*httptest.ResponseRecorder{"foreign": foreign, "own": own} {
		if rec.Code != unknown.Code || rec.Body.String() != unknown.Body.String() {
			t.Fatalf("%s-id answer %d %q differs from unknown-id answer %d %q", name, rec.Code, rec.Body.String(), unknown.Code, unknown.Body.String())
		}
	}
	if store.Count() != 2 {
		t.Fatalf("stop_task changed the view: %d tombstones left of 2", store.Count())
	}
}

// Upstream registers a task naming no tenant (it deletes nothing) and lists it
// in active_tasks; the lakehouse creates nothing for it, so it is not listed.
func TestRunTask_NoTenantsIsAcceptedButNotListed(t *testing.T) {
	store := newTestStore(t)
	if err := RunTask(context.Background(), store, false, "none", 1, nil, "*", "hide"); err != nil {
		t.Fatalf("a task naming no tenant must be accepted: %v", err)
	}
	if tasks := ActiveTasks(context.Background(), store); len(tasks) != 0 {
		t.Fatalf("active_tasks = %+v, want nothing (no tombstone is created)", tasks)
	}
}

// A task's relative time filter is evaluated at the task's timestamp, as
// upstream does, and does not drift with the clock.
func TestRunTask_RelativeTimeFilterIsPinnedToTheTaskTimestamp(t *testing.T) {
	store := newTestStore(t)
	at := time.Now().Add(-2 * time.Hour).UnixNano()
	if err := RunTask(context.Background(), store, false, "rel", at, []TenantRef{{}}, "_time:5m", "hide"); err != nil {
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
	store := newTestStore(t)
	if err := RunTask(context.Background(), store, true, "t", 1, []TenantRef{{AccountID: 5, ProjectID: 3}}, "*", "hide"); !errors.Is(err, ErrProjectNotInKeyLayout) || store.Count() != 0 {
		t.Fatalf("RunTask err = %v (tombstones %d), want a refusal and nothing registered", err, store.Count())
	}
}
