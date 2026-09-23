package vlstorage

import (
	"flag"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
)

// The cluster delete protocol end to end, as the logs binary serves it:
// upstream's own vlselect.RequestHandler (its -internaldelete.enable gate, then
// internalselect) behind the lakehouse delete.enabled check, dispatching into
// this adapter. This is the path a vlselect node takes when it fans a delete
// out to the cold tier.
func internalDeleteServer(t *testing.T, ts *delete.TombstoneStore, flagOn, deleteEnabled bool) http.HandlerFunc {
	t.Helper()
	internalselect.Init()
	t.Cleanup(internalselect.Stop)
	SetStorage(mockStore{}, ts)
	if err := flag.Set(internaldelete.FlagName, map[bool]string{true: "true", false: "false"}[flagOn]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(internaldelete.FlagName, "false") })
	return internaldelete.Handler(internaldelete.FlagEnabled, deleteEnabled, func(w http.ResponseWriter, r *http.Request) {
		vlselect.RequestHandler(w, r)
	})
}

// upstream's answer while -internaldelete.enable is off (vlselect/main.go).
const upstreamDisabled = "requests to /internal/delete/* are disabled; pass -internaldelete.enable command-line flag for enabling them; " +
	"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs\n"

func postForm(h http.HandlerFunc, path string, form url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func runTaskForm(tenants string) url.Values {
	return url.Values{
		"version":    {netselect.DeleteRunTaskProtocolVersion},
		"task_id":    {"task-from-vlselect"},
		"timestamp":  {"1700000000000000000"},
		"tenant_ids": {tenants},
		"filter":     {"level:error"},
	}
}

// Regression: /internal/delete/run_task used to be served unconditionally and
// wrote an instance-wide tombstone, so any client reaching the port could hide
// every tenant's matching rows. By default it now gets upstream's own answer.
func TestInternalDelete_DefaultAnswersLikeUpstreamAndHidesNothing(t *testing.T) {
	ts := delete.NewTombstoneStore()
	h := internalDeleteServer(t, ts, false, true)

	for _, path := range []string{"/internal/delete/run_task", "/internal/delete/stop_task", "/internal/delete/active_tasks"} {
		rec := postForm(h, path, runTaskForm(`[{"account_id":7,"project_id":3}]`))
		if rec.Code != http.StatusBadRequest || rec.Body.String() != upstreamDisabled {
			t.Fatalf("%s: got %d %q, want upstream's disabled answer", path, rec.Code, rec.Body.String())
		}
	}
	if n := ts.Count(); n != 0 {
		t.Fatalf("tombstones = %d, want 0", n)
	}
}

// With both switches on, /internal/delete/run_task registers a tombstone scoped
// to exactly the task's tenant_ids. A task naming no tenant deletes nothing, as
// upstream (its search has no tenant to match), and a task id that is already
// registered gets upstream's refusal.
func TestInternalDelete_EnabledRunTaskIsTenantScoped(t *testing.T) {
	ts := delete.NewTombstoneStore()
	h := internalDeleteServer(t, ts, true, true)

	rec := postForm(h, "/internal/delete/run_task", runTaskForm(`[{"account_id":7,"project_id":3},{"account_id":7,"project_id":3}]`))
	if rec.Code != http.StatusOK {
		t.Fatalf("run_task: got %d %q, want 200", rec.Code, rec.Body.String())
	}
	got, ok := ts.Get("task-from-vlselect")
	if !ok {
		t.Fatal("run_task registered no tombstone")
	}
	if !got.ScopedExactlyTo(7, 3) || got.AppliesToTenant(0, 0) {
		t.Fatalf("tombstone tenants = %v, want exactly 7:3", got.Tenants)
	}
	if got.Query != "level:error" || got.EndNs != 1700000000000000000 || got.StartNs != math.MinInt64 {
		t.Fatalf("tombstone = %+v, want filter level:error over (-inf, task timestamp]", got)
	}

	rec = postForm(h, "/internal/delete/run_task", runTaskForm(`[{"account_id":1,"project_id":0}]`))
	if rec.Code < http.StatusBadRequest || !strings.Contains(rec.Body.String(), `the delete task with task_id="task-from-vlselect" is already registered`) {
		t.Fatalf("duplicate task_id: got %d %q, want upstream's refusal", rec.Code, rec.Body.String())
	}
	if again, _ := ts.Get("task-from-vlselect"); !again.ScopedExactlyTo(7, 3) {
		t.Fatalf("a refused duplicate replaced the registered task's scope: %v", again.Tenants)
	}

	empty := runTaskForm(`[]`)
	empty.Set("task_id", "task-no-tenants")
	rec = postForm(h, "/internal/delete/run_task", empty)
	if rec.Code != http.StatusOK {
		t.Fatalf("run_task with no tenants: got %d %q, want 200 as upstream", rec.Code, rec.Body.String())
	}
	if _, ok := ts.Get("task-no-tenants"); ok || ts.Count() != 1 {
		t.Fatalf("a task naming no tenant must create nothing (it would act on every tenant); tombstones=%d", ts.Count())
	}
}

// With both switches on, the read and un-delete halves of the protocol still
// work: active_tasks lists and stop_task removes a tombstone.
func TestInternalDelete_EnabledStopAndListStillWork(t *testing.T) {
	ts := delete.NewTombstoneStore()
	h := internalDeleteServer(t, ts, true, true)
	ts.Add(delete.Tombstone{ID: "t1", Query: "level:error", EndNs: time.Now().UnixNano(), CreatedAt: time.Now(), Mode: "hide"})

	rec := postForm(h, "/internal/delete/active_tasks", url.Values{"version": {netselect.DeleteActiveTasksProtocolVersion}})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"t1"`) {
		t.Fatalf("active_tasks: got %d %q, want 200 listing t1", rec.Code, rec.Body.String())
	}
	rec = postForm(h, "/internal/delete/stop_task", url.Values{"version": {netselect.DeleteStopTaskProtocolVersion}, "task_id": {"t1"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("stop_task: got %d %q, want 200", rec.Code, rec.Body.String())
	}
	if n := ts.Count(); n != 0 {
		t.Fatalf("tombstones after stop_task = %d, want 0", n)
	}
}

func TestInternalDelete_DeleteFeatureOffIsRefused(t *testing.T) {
	ts := delete.NewTombstoneStore()
	h := internalDeleteServer(t, ts, true, false)
	rec := postForm(h, "/internal/delete/run_task", runTaskForm(`[]`))
	if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.DeleteDisabledMessage+"\n" {
		t.Fatalf("got %d %q, want the delete.enabled refusal", rec.Code, rec.Body.String())
	}
}
