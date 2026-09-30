package main

import (
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// /delete/run_task is POST-only, as on VictoriaLogs master (the v1.52.0 pin
// lacks the check; internaldelete.RunTaskPOSTOnly adds it). Twin of
// lakehouse-traces/public_delete_post_only_test.go.

func postOnlyServer(t *testing.T, deleteEnabled bool) (*delete.TombstoneStore, http.Handler) {
	t.Helper()
	store := delete.NewTombstoneStore()
	internalvlstorage.SetStorage(nopStorage{}, store)
	mux := http.NewServeMux()
	mountPublicDelete(mux, deleteEnabled, testGlobalRead)
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	if err := resolver.AddAlias("acme", tenant.TenantID{AccountID: 2002}); err != nil {
		t.Fatal(err)
	}
	return store, resolver.Middleware(mux)
}

func TestMountPublicDelete_RunTaskIsPOSTOnly(t *testing.T) {
	enablePublicDelete(t)
	// Both tenant forms: int AccountID/ProjectID and string OrgID (alias).
	for name, headers := range map[string]map[string]string{
		"int":    {"AccountID": "7", "ProjectID": "3"},
		"string": {"X-Scope-OrgID": "acme"},
		"none":   nil,
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete} {
			store, srv := postOnlyServer(t, true)
			rec := publicDeleteRequest(srv, method, "/delete/run_task", url.Values{"filter": {"level:error"}}, headers)
			want := "Only POST method is allowed; got " + method + ".\n"
			if rec.Code != http.StatusMethodNotAllowed || (method != http.MethodHead && rec.Body.String() != want) {
				t.Errorf("%s tenant %s /delete/run_task: got %d %q, want 405 %q", name, method, rec.Code, rec.Body.String(), want)
			}
			if store.Count() != 0 {
				t.Errorf("%s tenant %s: a delete task was created (%d tombstones)", name, method, store.Count())
			}
		}
		store, srv := postOnlyServer(t, true)
		rec := publicDeleteRequest(srv, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}}, headers)
		if rec.Code != http.StatusOK || store.Count() != 1 {
			t.Errorf("%s tenant POST: got %d %q, %d tombstones; want 200 and one task", name, rec.Code, rec.Body.String(), store.Count())
		}
	}
}

// The POST check sits after the disabled answers, as on upstream: a GET while
// -delete.enable is off still gets upstream's disabled answer, and while the
// lakehouse delete feature is off the lakehouse refusal.
func TestMountPublicDelete_RunTaskGETKeepsDisabledAnswers(t *testing.T) {
	_, srv := postOnlyServer(t, true)
	rec := publicDeleteRequest(srv, http.MethodGet, "/delete/run_task", url.Values{}, nil)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != upstreamPublicDeleteDisabled {
		t.Errorf("flag off: got %d %q, want upstream's disabled answer", rec.Code, rec.Body.String())
	}
	enablePublicDelete(t)
	_, srv = postOnlyServer(t, false)
	rec = publicDeleteRequest(srv, http.MethodGet, "/delete/run_task", url.Values{}, nil)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "need the lakehouse delete feature") {
		t.Errorf("delete.enabled off: got %d %q, want the lakehouse refusal", rec.Code, rec.Body.String())
	}
}

// stop_task and active_tasks accept GET as before (upstream is unchanged there).
func TestMountPublicDelete_OtherEndpointsStillAcceptGET(t *testing.T) {
	enablePublicDelete(t)
	store, srv := postOnlyServer(t, true)
	store.Add(delete.Tombstone{ID: "t1", Query: "*", EndNs: 1, FilterAt: 1, Mode: "hide", Tenants: []delete.TenantRef{{AccountID: 7}}})
	h := map[string]string{"AccountID": "7"}
	if rec := publicDeleteRequest(srv, http.MethodGet, "/delete/active_tasks", url.Values{}, h); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "t1") {
		t.Errorf("GET active_tasks: %d %q", rec.Code, rec.Body.String())
	}
	if rec := publicDeleteRequest(srv, http.MethodGet, "/delete/stop_task?task_id=t1", url.Values{}, h); rec.Code != http.StatusOK || store.Count() != 0 {
		t.Errorf("GET stop_task: %d %q, %d tombstones", rec.Code, rec.Body.String(), store.Count())
	}
}

// internaldelete.RunTaskPOSTOnly duplicates a check VictoriaLogs master has and
// the pinned v1.52.0 lacks. Fail the moment the vendored source has it, so the
// duplicate gets dropped (the wrapper in mountPublicDelete, RunTaskPOSTOnly and
// its tests, and this test).
func TestUpstreamRunTaskStillLacksMethodCheck(t *testing.T) {
	src, err := os.ReadFile("../../deps/VictoriaLogs/app/vlselect/main.go")
	if err != nil {
		t.Fatalf("vendored VictoriaLogs source missing (run make deps-logs): %v", err)
	}
	s := string(src)
	i := strings.Index(s, "func processDeleteRunTaskRequest(")
	if i < 0 {
		t.Fatal("vendored VictoriaLogs no longer has processDeleteRunTaskRequest; re-check internaldelete.RunTaskPOSTOnly")
	}
	body := s[i:]
	if j := strings.Index(body[1:], "\nfunc "); j >= 0 {
		body = body[:j+1]
	}
	if strings.Contains(body, "http.MethodPost") || strings.Contains(body, "Only POST method is allowed") {
		t.Fatal("the vendored VictoriaLogs processDeleteRunTaskRequest now enforces POST itself: " +
			"drop internaldelete.RunTaskPOSTOnly, its use in mountPublicDelete and these tests, " +
			"and the differ status of the vl.delete.run_task.non_post.differ registry row")
	}
}
