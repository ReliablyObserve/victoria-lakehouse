package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// Both tenant forms on upstream's delete API. Integer tenants (AccountID /
// ProjectID) are upstream's form and answer exactly as upstream; string tenants
// (X-Scope-OrgID through the aliases) are the lakehouse extension, resolved by
// the tenant middleware the binary puts in front of every route.
// Twin of cmd/lakehouse-logs/public_delete_tenant_forms_test.go.

// publicDeleteServer is the public delete API, behind the tenant middleware
// when aliases are configured (acme → 2002:0) — as the binary installs it only
// then — with a stand-in for the select path behind the same middleware.
func publicDeleteServer(t *testing.T, aliases bool) (*delete.TombstoneStore, http.Handler) {
	t.Helper()
	store := delete.NewTombstoneStore()
	internalvlstorage.SetStorage(nopStorage{}, store)
	mux := http.NewServeMux()
	mountPublicDelete(mux, true, testGlobalRead)
	mux.HandleFunc("/select/logsql/query", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if !aliases {
		return store, mux
	}
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	if err := resolver.AddAlias("acme", tenant.TenantID{AccountID: 2002}); err != nil {
		t.Fatal(err)
	}
	return store, resolver.Middleware(mux)
}

// A task registered through a string OrgID belongs to the aliased tenant, the
// integer form of that tenant sees the same task, and an unknown OrgID gets the
// select path's answer.
func TestMountPublicDelete_StringTenants(t *testing.T) {
	enablePublicDelete(t)
	store, srv := publicDeleteServer(t, true)
	acme := map[string]string{"X-Scope-OrgID": "acme"}

	rec := publicDeleteRequest(srv, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}}, acme)
	var resp struct {
		TaskID string `json:"task_id"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil {
		t.Fatalf("run_task via alias: %d %q", rec.Code, rec.Body.String())
	}
	if ts, ok := store.Get(resp.TaskID); !ok || !ts.ScopedExactlyTo(2002, 0) {
		t.Fatalf("task = %+v, want scoped to the alias's tenant 2002:0", ts)
	}
	for name, h := range map[string]map[string]string{"alias": acme, "int": {"AccountID": "2002"}} {
		if got := publicDeleteRequest(srv, http.MethodGet, "/delete/active_tasks", url.Values{}, h).Body.String(); !strings.Contains(got, resp.TaskID) {
			t.Errorf("active_tasks as %s = %s, want the task", name, got)
		}
	}
	if got := publicDeleteRequest(srv, http.MethodGet, "/delete/active_tasks", url.Values{}, map[string]string{"AccountID": "1001"}).Body.String(); got != "[]" {
		t.Errorf("another tenant lists %s, want []", got)
	}

	unknown := map[string]string{"X-Scope-OrgID": "nobody"}
	sel := publicDeleteRequest(srv, http.MethodGet, "/select/logsql/query", url.Values{}, unknown)
	for _, path := range []string{"/delete/run_task", "/delete/stop_task", "/delete/active_tasks"} {
		rec := publicDeleteRequest(srv, http.MethodPost, path, url.Values{"filter": {"*"}, "task_id": {resp.TaskID}}, unknown)
		if rec.Code != sel.Code || rec.Body.String() != sel.Body.String() || rec.Code != http.StatusBadRequest {
			t.Errorf("%s with an unknown OrgID = %d %q, want the select path's %d %q", path, rec.Code, rec.Body.String(), sel.Code, sel.Body.String())
		}
	}

	rec = publicDeleteRequest(srv, http.MethodPost, "/delete/stop_task", url.Values{"task_id": {resp.TaskID}}, acme)
	if rec.Code != http.StatusOK || store.Count() != 0 {
		t.Fatalf("stop_task via alias: %d %q, %d tasks left", rec.Code, rec.Body.String(), store.Count())
	}
}

// Integer-tenant answers on upstream's delete API are the same whether or not
// string-tenant aliases are configured.
func TestMountPublicDelete_IntTenantAnswerIndependentOfAliases(t *testing.T) {
	enablePublicDelete(t)
	answers := func(aliases bool) []string {
		store, srv := publicDeleteServer(t, aliases)
		store.Add(delete.Tombstone{ID: "own", Query: "*", EndNs: 1, FilterAt: 1, Mode: "hide", Tenants: []delete.TenantRef{{AccountID: 2002}}})
		store.Add(delete.Tombstone{ID: "foreign", Query: "*", EndNs: 1, FilterAt: 1, Mode: "hide", Tenants: []delete.TenantRef{{AccountID: 1001}}})
		h := map[string]string{"AccountID": "2002"}
		var out []string
		for _, r := range []struct {
			path string
			form url.Values
		}{
			{"/delete/active_tasks", url.Values{}},
			{"/delete/stop_task", url.Values{"task_id": {"foreign"}}},
			{"/delete/stop_task", url.Values{"task_id": {"no-such-task"}}},
			{"/delete/run_task", url.Values{"filter": {"level:("}}},
		} {
			rec := publicDeleteRequest(srv, http.MethodPost, r.path, r.form, h)
			out = append(out, r.path+" "+rec.Header().Get("Content-Type")+" "+http.StatusText(rec.Code)+" "+rec.Body.String())
		}
		return out
	}
	with, without := answers(true), answers(false)
	for i := range with {
		if with[i] != without[i] {
			t.Errorf("int-tenant answer changed with aliases configured:\n with:    %s\n without: %s", with[i], without[i])
		}
	}
}
