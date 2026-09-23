package delete

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// The lakehouse delete API is tenant-scoped: a caller acts for the tenant its
// request resolves to (AccountID/ProjectID, or X-Scope-OrgID through the
// tenant middleware), and only the validated global-read credential sees the
// whole instance. See handler_scope.go.

const (
	scopeKey1001   = "1001/0/logs/dt=2026-03-01/hour=07/a.parquet"
	scopeKey2002   = "2002/0/logs/dt=2026-03-01/hour=07/b.parquet"
	scopeKeyLegacy = "logs/dt=2026-03-01/hour=07/legacy.parquet"
	globalHeader   = "X-Lakehouse-Global-Read"
)

// scopedManifest is the manifest as the binaries hand it to the handler:
// ranges, leftovers, and the manifest's own key → tenant parser.
type scopedManifest struct{ leftoverManifest }

func (s *scopedManifest) TenantKeyParser() func(string) (string, string, bool) {
	return s.m.TenantKeyParser()
}

func newScopeHandler(t *testing.T) (*Handler, *lhmanifest.Manifest, *TombstoneStore, http.Handler) {
	t.Helper()
	m := newTestManifest(t, map[string]int64{scopeKey1001: 10, scopeKey2002: 10, scopeKeyLegacy: 10})
	store := NewTombstoneStore()
	auth := tenant.NewGlobalReadAuth(globalHeader, "letmein", "")
	h := NewHandler(store, &scopedManifest{leftoverManifest{m: m}}, NewStorageClassDetector(nil), defaultCfg(), "logs",
		WithGlobalReadAuthorizer(func(r *http.Request) bool { return auth.Enabled() && auth.Authorize(r) }))
	mux := http.NewServeMux()
	h.Register(mux)
	// The binaries put the tenant middleware in front of the whole mux.
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	if err := resolver.AddAlias("acme", tenant.TenantID{AccountID: 2002}); err != nil {
		t.Fatal(err)
	}
	return h, m, store, resolver.Middleware(mux)
}

type scopeReq struct {
	method, path string
	form         url.Values
	headers      map[string]string
}

func (r scopeReq) do(h http.Handler) *httptest.ResponseRecorder {
	var body *strings.Reader
	if r.form != nil {
		body = strings.NewReader(r.form.Encode())
	} else {
		body = strings.NewReader("")
	}
	req := httptest.NewRequest(r.method, r.path, body)
	if r.form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for k, v := range r.headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

var (
	as1001   = map[string]string{"AccountID": "1001"}
	as2002   = map[string]string{"X-Scope-OrgID": "acme"} // alias of 2002:0
	asGlobal = map[string]string{globalHeader: "letmein"}
)

func deleteForm(query string) url.Values {
	return url.Values{"query": {query}, "start": {"0"}, "end": {"4000000000000000000"}, "mode": {"hide"}}
}

func TestDeleteAPI_CreateIsScopedToTheCallersTenant(t *testing.T) {
	_, _, store, srv := newScopeHandler(t)

	rec := scopeReq{method: http.MethodPost, path: "/delete/logsql/delete", form: deleteForm("level:error"), headers: as1001}.do(srv)
	if rec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body)
	if body["tenant"] != "1001:0" || body["affected_files"] != float64(1) {
		t.Fatalf("create response = %v, want tenant 1001:0 over its 1 object", body)
	}
	ts, ok := store.Get(body["tombstone_id"].(string))
	if !ok || !ts.ScopedExactlyTo(1001, 0) || len(ts.AffectedKeys) != 1 || ts.AffectedKeys[0] != scopeKey1001 {
		t.Fatalf("tombstone = %+v, want scoped to 1001:0 over %s only", ts, scopeKey1001)
	}

	// X-Scope-OrgID resolves through the alias, exactly as for a select.
	rec = scopeReq{method: http.MethodPost, path: "/delete/logsql/delete", form: deleteForm("*"), headers: as2002}.do(srv)
	body = decodeJSON(t, rec.Body)
	if body["tenant"] != "2002:0" {
		t.Fatalf("aliased create resolved to %v, want 2002:0", body["tenant"])
	}
	// No headers: the default tenant, whose objects include the legacy ones.
	rec = scopeReq{method: http.MethodPost, path: "/delete/logsql/delete", form: deleteForm("*")}.do(srv)
	body = decodeJSON(t, rec.Body)
	if ts, _ := store.Get(body["tombstone_id"].(string)); !ts.ScopedExactlyTo(0, 0) || len(ts.AffectedKeys) != 1 || ts.AffectedKeys[0] != scopeKeyLegacy {
		t.Fatalf("default-tenant tombstone = %+v, want 0:0 over the legacy object", ts)
	}
	// The operator's delete is still scoped: to the tenant in its headers.
	h := map[string]string{globalHeader: "letmein", "AccountID": "2002"}
	rec = scopeReq{method: http.MethodPost, path: "/delete/logsql/delete", form: deleteForm("*"), headers: h}.do(srv)
	body = decodeJSON(t, rec.Body)
	if ts, _ := store.Get(body["tombstone_id"].(string)); !ts.ScopedExactlyTo(2002, 0) {
		t.Fatalf("operator delete = %+v, want scoped to 2002:0: no request creates an instance-wide tombstone", ts.Tenants)
	}
	if n := store.Count(); n != 4 {
		t.Fatalf("tombstones = %d, want 4", n)
	}
}

func TestDeleteAPI_ListGetUndeleteVerifyAreTenantScoped(t *testing.T) {
	_, _, store, srv := newScopeHandler(t)
	store.Add(Tombstone{ID: "own-1001", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 1001}}})
	store.Add(Tombstone{ID: "own-2002", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 2002}}})
	store.Add(Tombstone{ID: "two-tenants", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 1001}, {AccountID: 2002}}})
	store.Add(Tombstone{ID: "other-3003", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 3003}}})

	list := func(headers map[string]string) map[string]any {
		t.Helper()
		rec := scopeReq{method: http.MethodGet, path: "/delete/logsql/tombstones", headers: headers}.do(srv)
		if rec.Code != http.StatusOK {
			t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
		}
		return decodeJSON(t, rec.Body)
	}
	ids := func(body map[string]any) string {
		var out []string
		for _, ts := range body["tombstones"].([]any) {
			out = append(out, ts.(map[string]any)["ID"].(string))
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	if b := list(as1001); ids(b) != "own-1001" || b["scope"] != "tenant" {
		t.Errorf("1001:0 lists %q (scope %v), want only its own tombstone", ids(b), b["scope"])
	}
	if b := list(as2002); ids(b) != "own-2002" {
		t.Errorf("2002:0 (via alias) lists %q, want only its own tombstone", ids(b))
	}
	if b := list(nil); ids(b) != "" {
		t.Errorf("0:0 lists %q: other tenants' records are not the default tenant's", ids(b))
	}
	if b := list(asGlobal); ids(b) != "other-3003,own-1001,own-2002,two-tenants" || b["scope"] != "instance" {
		t.Errorf("operator lists %q (scope %v), want every tombstone", ids(b), b["scope"])
	}

	// Another tenant's tombstone is not found — to read or to un-delete.
	for _, id := range []string{"own-2002", "two-tenants", "other-3003"} {
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			rec := scopeReq{method: method, path: "/delete/logsql/tombstone/" + id, headers: as1001}.do(srv)
			if rec.Code != http.StatusNotFound {
				t.Errorf("%s %s as 1001:0 = %d, want 404", method, id, rec.Code)
			}
		}
	}
	if store.Count() != 4 {
		t.Fatalf("a tenant removed a tombstone that is not its own: %d left", store.Count())
	}
	if rec := (scopeReq{method: http.MethodGet, path: "/delete/logsql/tombstone/own-1001", headers: as1001}).do(srv); rec.Code != http.StatusOK {
		t.Errorf("GET own tombstone = %d", rec.Code)
	}

	// verify matches only the caller's own tombstones.
	verify := func(headers map[string]string) map[string]any {
		rec := scopeReq{method: http.MethodPost, path: "/delete/logsql/verify",
			form: url.Values{"query": {"level:error"}, "start": {"0"}, "end": {"10"}}, headers: headers}.do(srv)
		return decodeJSON(t, rec.Body)
	}
	if b := verify(as1001); len(b["tombstone_ids"].([]any)) != 1 || b["tombstone_ids"].([]any)[0] != "own-1001" {
		t.Errorf("verify as 1001:0 = %v, want only own-1001", b["tombstone_ids"])
	}
	if b := verify(nil); b["verified"] != false {
		t.Errorf("verify as 0:0 = %v, want nothing verified", b)
	}

	// The operator may un-delete a record the caller does not own.
	if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/two-tenants", headers: asGlobal}).do(srv); rec.Code != http.StatusOK {
		t.Errorf("operator un-delete of a multi-tenant record = %d %s", rec.Code, rec.Body.String())
	}
	if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/own-1001", headers: as1001}).do(srv); rec.Code != http.StatusOK {
		t.Errorf("tenant un-delete of its own tombstone = %d", rec.Code)
	}
	if store.Count() != 2 {
		t.Fatalf("tombstones left = %d, want 2", store.Count())
	}
}

func TestDeleteAPI_EstimateCoversOnlyTheCallersObjects(t *testing.T) {
	_, _, _, srv := newScopeHandler(t)
	for headers, want := range map[*map[string]string]float64{&as1001: 1, &as2002: 1, &asGlobal: 1} {
		rec := scopeReq{method: http.MethodPost, path: "/delete/logsql/estimate", form: deleteForm("*"), headers: *headers}.do(srv)
		if body := decodeJSON(t, rec.Body); body["affected_files"] != want {
			t.Errorf("estimate for %v = %v, want %v (the caller's own objects)", *headers, body["affected_files"], want)
		}
	}
}

func TestDeleteAPI_LeftoversTenantView(t *testing.T) {
	_, m, store, srv := newScopeHandler(t)
	m.Retire(scopeKey1001, "", false)
	m.Retire(scopeKey2002, "", false)
	claimed := "2002/0/logs/dt=2026-03-01/hour=07/claimed.parquet"
	if !m.ClaimPending(claimed) {
		t.Fatal("fixture: claim rejected")
	}
	store.Add(Tombstone{ID: "own-1001", Query: "*", EndNs: 10, Mode: "permanent", Tenants: []TenantRef{{AccountID: 1001}},
		Superseded: map[string]Supersession{scopeKey1001: {State: SupersessionPublished, At: time.Now()}}})
	store.Add(Tombstone{ID: "own-2002", Query: "*", EndNs: 10, Mode: "permanent", Tenants: []TenantRef{{AccountID: 2002}},
		Superseded: map[string]Supersession{scopeKey2002: {State: SupersessionPublished, At: time.Now()}}})

	get := func(headers map[string]string) map[string]any {
		rec := scopeReq{method: http.MethodGet, path: "/delete/logsql/leftovers", headers: headers}.do(srv)
		if rec.Code != http.StatusOK {
			t.Fatalf("leftovers: %d %s", rec.Code, rec.Body.String())
		}
		return decodeJSON(t, rec.Body)
	}
	b := get(as1001)
	counts := b["counts"].(map[string]any)
	if b["scope"] != "tenant" || counts["retired"] != float64(1) || counts["pending"] != float64(0) || counts["unfinished_rewrites"] != float64(1) {
		t.Errorf("1001:0 leftovers = scope %v counts %v, want its own retired key and rewrite only", b["scope"], counts)
	}
	for _, rk := range b["retired_keys"].([]any) {
		if k := rk.(map[string]any)["key"]; k != scopeKey1001 {
			t.Errorf("1001:0 sees another tenant's retired key %v", k)
		}
	}
	b = get(asGlobal)
	counts = b["counts"].(map[string]any)
	if b["scope"] != "instance" || counts["retired"] != float64(2) || counts["pending"] != float64(1) || counts["unfinished_rewrites"] != float64(2) {
		t.Errorf("operator leftovers = scope %v counts %v, want the whole instance", b["scope"], counts)
	}
}

func TestDeleteAPI_UnparseableTenantIsRefused(t *testing.T) {
	_, _, store, srv := newScopeHandler(t)
	for _, path := range []string{"/delete/logsql/delete", "/delete/logsql/estimate", "/delete/logsql/verify"} {
		rec := scopeReq{method: http.MethodPost, path: path, form: deleteForm("*"), headers: map[string]string{"AccountID": "not-a-number"}}.do(srv)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "cannot obtain tenantID") {
			t.Errorf("%s with a bad AccountID = %d %q, want 400", path, rec.Code, rec.Body.String())
		}
	}
	for _, path := range []string{"/delete/logsql/tombstones", "/delete/logsql/leftovers", "/delete/logsql/tombstone/x"} {
		rec := scopeReq{method: http.MethodGet, path: path, headers: map[string]string{"ProjectID": "-1"}}.do(srv)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("GET %s with a bad ProjectID = %d, want 400", path, rec.Code)
		}
	}
	if store.Count() != 0 {
		t.Fatal("a refused request created a tombstone")
	}
}

// Without a configured global-read credential no request is the operator.
func TestDeleteAPI_NoCredentialConfiguredMeansNoOperator(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "other-3003", Query: "*", EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 3003}}})
	h := NewHandler(store, &mockManifest{}, NewStorageClassDetector(nil), defaultCfg(), "logs")
	rec := scopeReq{method: http.MethodGet, path: "/delete/logsql/tombstones", headers: asGlobal}.do(http.HandlerFunc(h.handleListTombstones))
	if body := decodeJSON(t, rec.Body); body["count"] != float64(0) || body["scope"] != "tenant" {
		t.Errorf("listing without a configured credential = %v, want the tenant view", body)
	}
}

func (s *scopedManifest) AccountOnlyTenantKeys() bool { return s.m.AccountOnlyTenantKeys() }
