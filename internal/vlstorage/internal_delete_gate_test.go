package vlstorage

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
)

// The cluster delete protocol end to end: the lakehouse gate in front of
// upstream's own internalselect handler, dispatching into this adapter. This
// is the path a vlselect node takes when it fans a delete out to the cold tier.
func internalDeleteServer(t *testing.T, ts *delete.TombstoneStore, enabled, deleteEnabled bool) http.HandlerFunc {
	t.Helper()
	internalselect.Init()
	t.Cleanup(internalselect.Stop)
	SetStorage(mockStore{}, ts)
	return internaldelete.Handler(enabled, deleteEnabled, func(w http.ResponseWriter, r *http.Request) {
		internalselect.RequestHandler(r.Context(), w, r)
	})
}

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
// every tenant's matching rows. By default it now answers exactly as upstream.
func TestInternalDelete_DefaultAnswersLikeUpstreamAndHidesNothing(t *testing.T) {
	ts := delete.NewTombstoneStore()
	h := internalDeleteServer(t, ts, false, true)

	for _, path := range []string{"/internal/delete/run_task", "/internal/delete/stop_task", "/internal/delete/active_tasks"} {
		rec := postForm(h, path, runTaskForm(`[{"account_id":7,"project_id":3}]`))
		if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.DisabledMessage+"\n" {
			t.Fatalf("%s: got %d %q, want upstream's disabled answer", path, rec.Code, rec.Body.String())
		}
	}
	if n := ts.Count(); n != 0 {
		t.Fatalf("tombstones = %d, want 0", n)
	}
}

func TestInternalDelete_EnabledRunTaskIsRefusedNotWidened(t *testing.T) {
	ts := delete.NewTombstoneStore()
	h := internalDeleteServer(t, ts, true, true)

	for _, tenants := range []string{`[{"account_id":7,"project_id":3}]`, `[{"account_id":0,"project_id":0}]`, `[]`} {
		rec := postForm(h, "/internal/delete/run_task", runTaskForm(tenants))
		// The status is upstream's for a failed storage call: 400 on the
		// traces pin, 502 on VL v1.52+ (internalselect.go wraps it so vlselect
		// propagates it). Either way an error, never a 2xx.
		if rec.Code < http.StatusBadRequest {
			t.Fatalf("tenant_ids=%s: status = %d, want an error status", tenants, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), internaldelete.ErrRunTaskNotTenantScoped.Error()) {
			t.Fatalf("tenant_ids=%s: body = %q, want the tenant-scope refusal", tenants, rec.Body.String())
		}
	}
	if n := ts.Count(); n != 0 {
		t.Fatalf("tombstones = %d, want 0: a refused task must not hide anything", n)
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
