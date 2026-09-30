package main

import (
	"bytes"
	"context"
	"flag"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	vmmetrics "github.com/VictoriaMetrics/metrics"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// /delete/run_task, /internal/delete/* and /internal/select/* are POST-only, as
// on VictoriaLogs master (the v1.52.0 pin lacks the checks;
// internaldelete.RunTaskPOSTOnly and internaldelete.POSTOnly add them). Twin of
// lakehouse-traces/public_delete_post_only_test.go.

// queryRequest is the SSRF shape of an attack: every argument in the URL query
// and no body (Go's ParseForm ignores the body of a GET, HEAD or DELETE, so a
// form in the body would not reach the handler and prove nothing).
func queryRequest(mux http.Handler, method, path string, form url.Values, headers map[string]string) *httptest.ResponseRecorder {
	target := path
	if len(form) > 0 {
		target += "?" + form.Encode()
	}
	req := httptest.NewRequest(method, target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

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

var tenantForms = map[string]map[string]string{
	"int":    {"AccountID": "7", "ProjectID": "3"},
	"string": {"X-Scope-OrgID": "acme"},
	"none":   nil,
}

var nonPOST = []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete}

func TestMountPublicDelete_RunTaskIsPOSTOnly(t *testing.T) {
	enablePublicDelete(t)
	for name, headers := range tenantForms {
		for _, method := range nonPOST {
			store, srv := postOnlyServer(t, true)
			rec := queryRequest(srv, method, "/delete/run_task", url.Values{"filter": {"level:error"}}, headers)
			want := "Only POST method is allowed; got " + method + ".\n"
			if rec.Code != http.StatusMethodNotAllowed || (method != http.MethodHead && rec.Body.String() != want) {
				t.Errorf("%s tenant %s /delete/run_task?filter=..: got %d %q, want 405 %q", name, method, rec.Code, rec.Body.String(), want)
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
	rec := queryRequest(srv, http.MethodGet, "/delete/run_task", url.Values{"filter": {"*"}}, nil)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != upstreamPublicDeleteDisabled {
		t.Errorf("flag off: got %d %q, want upstream's disabled answer", rec.Code, rec.Body.String())
	}
	enablePublicDelete(t)
	_, srv = postOnlyServer(t, false)
	rec = queryRequest(srv, http.MethodGet, "/delete/run_task", url.Values{"filter": {"*"}}, nil)
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
	if rec := queryRequest(srv, http.MethodGet, "/delete/active_tasks", nil, h); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "t1") {
		t.Errorf("GET active_tasks: %d %q", rec.Code, rec.Body.String())
	}
	if rec := queryRequest(srv, http.MethodGet, "/delete/stop_task", url.Values{"task_id": {"t1"}}, h); rec.Code != http.StatusOK || store.Count() != 0 {
		t.Errorf("GET stop_task: %d %q, %d tombstones", rec.Code, rec.Body.String(), store.Count())
	}
}

// VictoriaLogs master counts /delete/run_task before refusing it; the wrapper
// increments upstream's own counter (same instance), once per request, for the
// refused GET and the served POST alike.
func TestMountPublicDelete_RunTaskCounterParity(t *testing.T) {
	enablePublicDelete(t)
	c := vmmetrics.GetOrCreateCounter(internaldelete.RunTaskRequestsCounter)
	_, srv := postOnlyServer(t, true)
	before := c.Get()
	queryRequest(srv, http.MethodGet, "/delete/run_task", url.Values{"filter": {"*"}}, nil)
	if got := c.Get() - before; got != 1 {
		t.Errorf("GET moved %s by %d, want 1", internaldelete.RunTaskRequestsCounter, got)
	}
	before = c.Get()
	publicDeleteRequest(srv, http.MethodPost, "/delete/run_task", url.Values{"filter": {"*"}}, nil)
	if got := c.Get() - before; got != 1 {
		t.Errorf("POST moved %s by %d, want 1 (upstream counts it once)", internaldelete.RunTaskRequestsCounter, got)
	}
}

// The pinned VictoriaLogs v1.52.0 accepts a GET on /delete/run_task.
// internaldelete.RunTaskPOSTOnly duplicates a check VictoriaLogs master has:
// call the vendored upstream handler with the attack shape and fail the moment
// it refuses the GET itself, so the duplicate gets dropped.
func TestUpstreamRunTaskStillLacksMethodCheck(t *testing.T) {
	enablePublicDelete(t)
	internalvlstorage.SetStorage(nopStorage{}, delete.NewTombstoneStore())
	rec := httptest.NewRecorder()
	vlselect.RequestHandler(rec, httptest.NewRequest(http.MethodGet, "/delete/run_task?filter=*", nil))
	if rec.Code == http.StatusMethodNotAllowed {
		t.Fatal("the vendored VictoriaLogs /delete/run_task now answers 405 to a GET itself: " +
			"drop internaldelete.RunTaskPOSTOnly, its use in mountPublicDelete and its tests, and flip " +
			"vl.delete.run_task.non_post.differ to pass")
	}
}

// ---- /internal/delete/* and /internal/select/* ----

func enableInternalDelete(t *testing.T) {
	t.Helper()
	if err := flag.Set(internaldelete.FlagName, "true"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(internaldelete.FlagName, "false") })
}

func internalServer(t *testing.T, deleteEnabled bool) (*delete.TombstoneStore, http.Handler) {
	t.Helper()
	store := delete.NewTombstoneStore()
	internalvlstorage.SetStorage(nopStorage{}, store)
	internalselect.Init()
	t.Cleanup(internalselect.Stop)
	mux := http.NewServeMux()
	mountInternalProtocol(mux, deleteEnabled)
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	if err := resolver.AddAlias("acme", tenant.TenantID{AccountID: 2002}); err != nil {
		t.Fatal(err)
	}
	return store, resolver.Middleware(mux)
}

// internalRunTaskArgs is what upstream's netselect client sends to
// /internal/delete/run_task: two tenants, a filter, the protocol version.
func internalRunTaskArgs() url.Values {
	tenants := []logstorage.TenantID{{AccountID: 7, ProjectID: 3}, {AccountID: 8}}
	return url.Values{
		"version":    {"v2"},
		"task_id":    {"forged"},
		"timestamp":  {"1"},
		"tenant_ids": {string(logstorage.MarshalTenantIDsToJSON(tenants))},
		"filter":     {"*"},
	}
}

func multipartPOST(t *testing.T, srv http.Handler, path string, args url.Values) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, vs := range args {
		for _, v := range vs {
			_ = mw.WriteField(k, v)
		}
	}
	_ = mw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, path, &buf).WithContext(ctx)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func TestMountInternalProtocol_NonPOSTRefused(t *testing.T) {
	enableInternalDelete(t)
	for name, headers := range tenantForms {
		for _, method := range nonPOST {
			store, srv := internalServer(t, true)
			rec := queryRequest(srv, method, "/internal/delete/run_task", internalRunTaskArgs(), headers)
			if rec.Code != http.StatusMethodNotAllowed || rec.Body.Len() != 0 {
				t.Errorf("%s tenant %s /internal/delete/run_task: got %d %q, want a bare 405", name, method, rec.Code, rec.Body.String())
			}
			if store.Count() != 0 {
				t.Errorf("%s tenant %s: a forged internal delete created %d tombstone(s)", name, method, store.Count())
			}
			for _, path := range []string{"/internal/delete/stop_task", "/internal/delete/active_tasks"} {
				if rec := queryRequest(srv, method, path, url.Values{"version": {"v2"}}, headers); rec.Code != http.StatusMethodNotAllowed || rec.Body.Len() != 0 {
					t.Errorf("%s tenant %s %s: got %d %q, want a bare 405", name, method, path, rec.Code, rec.Body.String())
				}
			}
			rec = queryRequest(srv, method, "/internal/select/tenant_ids", url.Values{"version": {"v5"}, "start": {"0"}, "end": {"9223372036854775807"}}, headers)
			if rec.Code != http.StatusMethodNotAllowed || rec.Body.Len() != 0 {
				t.Errorf("%s tenant %s /internal/select/tenant_ids: got %d %q, want a bare 405 and no data", name, method, rec.Code, rec.Body.String())
			}
		}
		// POST still works: the way upstream's client sends it (multipart).
		store, srv := internalServer(t, true)
		rec := multipartPOST(t, srv, "/internal/delete/run_task", internalRunTaskArgs())
		if rec.Code != http.StatusOK || store.Count() != 1 {
			t.Errorf("%s tenant POST run_task: got %d %q, %d tombstones; want 200 and one task", name, rec.Code, rec.Body.String(), store.Count())
		}
		rec = multipartPOST(t, srv, "/internal/select/tenant_ids", url.Values{"version": {"v5"}, "start": {"0"}, "end": {"9223372036854775807"}})
		if rec.Code == http.StatusMethodNotAllowed {
			t.Errorf("%s tenant POST tenant_ids: got 405", name)
		}
	}
}

// The POST check sits after the gates, as on upstream: with the flag off a GET
// gets upstream's disabled answer, with delete.enabled off the lakehouse one.
// /internal/select/* has no gate, so it answers 405 whatever the flags say.
func TestMountInternalProtocol_GatesAnswerBeforeMethodCheck(t *testing.T) {
	_, srv := internalServer(t, true)
	rec := queryRequest(srv, http.MethodGet, "/internal/delete/run_task", internalRunTaskArgs(), nil)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != upstreamInternalDeleteDisabled {
		t.Errorf("flag off: got %d %q, want upstream's disabled answer", rec.Code, rec.Body.String())
	}
	if rec := queryRequest(srv, http.MethodGet, "/internal/select/tenant_ids", nil, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("select, flag off: got %d, want 405", rec.Code)
	}
	enableInternalDelete(t)
	_, srv = internalServer(t, false)
	rec = queryRequest(srv, http.MethodGet, "/internal/delete/run_task", internalRunTaskArgs(), nil)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.DeleteDisabledMessage+"\n" {
		t.Errorf("delete.enabled off: got %d %q, want the lakehouse refusal", rec.Code, rec.Body.String())
	}
}

// Behavioural drift guards: the pinned VictoriaLogs answers a GET on its
// cluster protocol; internaldelete.POSTOnly duplicates a check VictoriaLogs
// master has. Fail the moment the vendored handlers refuse the GET themselves.
func TestUpstreamInternalProtocolStillAcceptsGET(t *testing.T) {
	enableInternalDelete(t)
	internalvlstorage.SetStorage(nopStorage{}, delete.NewTombstoneStore())
	internalselect.Init()
	t.Cleanup(internalselect.Stop)
	const msg = "the vendored VictoriaLogs %s now answers 405 to a GET itself: drop internaldelete.POSTOnly " +
		"(its use in mountInternalProtocol) and its tests, and flip vl.internal.delete.non_post.differ / " +
		"vl.internal.select.non_post.differ to pass"
	newReq := func(path string) *http.Request {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		return httptest.NewRequest(http.MethodGet, path+"?"+internalRunTaskArgs().Encode(), nil).WithContext(ctx)
	}
	rec := httptest.NewRecorder()
	vlselect.RequestHandler(rec, newReq("/internal/delete/run_task"))
	if rec.Code == http.StatusMethodNotAllowed {
		t.Fatalf(msg, "/internal/delete/*")
	}
	rec = httptest.NewRecorder()
	internalselect.RequestHandler(context.Background(), rec, newReq("/internal/select/tenant_ids"))
	if rec.Code == http.StatusMethodNotAllowed {
		t.Fatalf(msg, "/internal/select/*")
	}
}

// Every client of the cluster protocol we run is upstream's netselect, which
// sends every request through one helper that uses POST. Fail if that changes:
// a GET client would break against the POST-only handlers above.
func TestNetselectClientUsesPOST(t *testing.T) {
	src, err := os.ReadFile("../../deps/VictoriaLogs/app/vlstorage/netselect/netselect.go")
	if err != nil {
		t.Fatalf("vendored VictoriaLogs source missing (run make deps-logs): %v", err)
	}
	if n := strings.Count(string(src), "http.NewRequestWithContext("); n != 1 || !strings.Contains(string(src), `http.NewRequestWithContext(ctx, "POST", reqURL, reqBody)`) {
		t.Fatalf("netselect no longer has exactly one request helper sending POST (found %d constructors); audit the /internal/select and /internal/delete clients", n)
	}
}
