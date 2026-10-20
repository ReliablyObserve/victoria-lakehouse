package delete

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
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

func newScopeHandler(t *testing.T) (*Handler, *lhmanifest.Manifest, *TombstoneStore, http.Handler) {
	t.Helper()
	return newScopeHandlerOn(t, deletetest.NewBucket(), "pod")
}

// newScopeHandlerOn is newScopeHandler for one pod of a shared bucket.
func newScopeHandlerOn(t *testing.T, b *deletetest.Bucket, pod string) (*Handler, *lhmanifest.Manifest, *TombstoneStore, http.Handler) {
	t.Helper()
	m := newTestManifest(t, map[string]int64{scopeKey1001: 10, scopeKey2002: 10, scopeKeyLegacy: 10})
	store, _ := newSyncedPod(t, b, pod)
	auth := tenant.NewGlobalReadAuth(globalHeader, "letmein", "")
	h := NewHandler(store, &scopedManifest{m: m}, NewStorageClassDetector(nil), defaultCfg(), "logs",
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
	if !ok || !ts.ScopedExactlyTo(1001, 0) {
		t.Fatalf("tombstone = %+v, want scoped to 1001:0", ts)
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
	if ts, _ := store.Get(body["tombstone_id"].(string)); !ts.ScopedExactlyTo(0, 0) || body["affected_files"] != float64(1) {
		t.Fatalf("default-tenant tombstone = %+v (%v), want 0:0 over the legacy object", ts, body)
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
	mustCreate(t, store, Tombstone{ID: "own-1001", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 1001}}})
	mustCreate(t, store, Tombstone{ID: "own-2002", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 2002}}})
	mustCreate(t, store, Tombstone{ID: "two-tenants", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 1001}, {AccountID: 2002}}})
	mustCreate(t, store, Tombstone{ID: "other-3003", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 3003}}})

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

// The leftovers API (retired and pending keys, unfinished rewrites) is gone with
// the rewrite it reported on: nothing is owed any more, so for a tenant and for
// the operator the route answers like any unknown route.
func TestDeleteAPI_LeftoversRouteIsGone(t *testing.T) {
	_, _, _, srv := newScopeHandler(t)
	for _, headers := range []map[string]string{as1001, asGlobal} {
		if rec := (scopeReq{method: http.MethodGet, path: "/delete/logsql/leftovers", headers: headers}).do(srv); rec.Code != http.StatusNotFound {
			t.Errorf("the leftovers route answers %d for %v, want 404 (it is gone)", rec.Code, headers)
		}
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
	for _, path := range []string{"/delete/logsql/tombstones", "/delete/logsql/tombstone/x"} {
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
	store := newTestStore(t)
	mustCreate(t, store, Tombstone{ID: "other-3003", Query: "*", EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 3003}}})
	h := NewHandler(store, &mockManifest{}, NewStorageClassDetector(nil), defaultCfg(), "logs")
	rec := scopeReq{method: http.MethodGet, path: "/delete/logsql/tombstones", headers: asGlobal}.do(http.HandlerFunc(h.handleListTombstones))
	if body := decodeJSON(t, rec.Body); body["count"] != float64(0) || body["scope"] != "tenant" {
		t.Errorf("listing without a configured credential = %v, want the tenant view", body)
	}
}

func (s *scopedManifest) AccountOnlyTenantKeys() bool { return s.m.AccountOnlyTenantKeys() }

// A pod that has not polled yet does not know a tombstone another pod created.
// The un-delete of an id the pod does not know syncs first and then applies the
// same tenant check as for a known id: another tenant gets 404 and nothing is
// written, so the delete stays in force everywhere; the owner and the operator
// can still undo it.
func TestDeleteAPI_UndeleteOfAnUnpolledIDIsTenantScoped(t *testing.T) {
	b := deletetest.NewBucket()
	_, _, storeA, _ := newScopeHandlerOn(t, b, "a")
	// Pods that synced before the delete exists.
	_, _, storeB, srvB := newScopeHandlerOn(t, b, "b")
	_, _, _, srvOwner := newScopeHandlerOn(t, b, "owner")
	_, _, _, srvOperator := newScopeHandlerOn(t, b, "operator")
	mustCreate(t, storeA, Tombstone{ID: "own-1001", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 1001}}})
	if _, ok := storeB.Get("own-1001"); ok {
		t.Fatal("fixture: pod b already knows the tombstone")
	}
	before := len(b.Keys(tombPrefix()))

	rec := scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/own-1001", headers: as2002}.do(srvB)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("tenant 2002 un-deleting tenant 1001's tombstone through a pod that has not polled it: %d %s, want 404", rec.Code, rec.Body.String())
	}
	if n := len(b.Keys(tombPrefix())); n != before {
		t.Fatalf("%d keys after the refused un-delete, want %d: no undo key may be written", n, before)
	}
	mustSync(t, storeA, false)
	if _, ok := storeA.Get("own-1001"); !ok {
		t.Fatal("tenant 1001's delete is no longer in force on pod a")
	}

	if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/own-1001", headers: as1001}).do(srvOwner); rec.Code != http.StatusOK {
		t.Fatalf("the owner's un-delete through an unpolled pod: %d %s", rec.Code, rec.Body.String())
	}
	mustCreate(t, storeA, Tombstone{ID: "own-1001-b", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 1001}}})
	if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/own-1001-b", headers: asGlobal}).do(srvOperator); rec.Code != http.StatusOK {
		t.Fatalf("the operator's un-delete through an unpolled pod: %d %s", rec.Code, rec.Body.String())
	}
}

// The same through an unpolled pod for a permanent or auto delete: another tenant
// still gets 404, not the 409 that would tell it the id exists; the owner gets 409.
func TestDeleteAPI_UndeleteOfAnUnpolledIrreversibleIDDoesNotLeakItsExistence(t *testing.T) {
	for _, mode := range []string{"auto", "permanent"} {
		b := deletetest.NewBucket()
		_, _, storeA, _ := newScopeHandlerOn(t, b, "a")
		_, _, _, srvB := newScopeHandlerOn(t, b, "b")
		mustCreate(t, storeA, Tombstone{ID: "irreversible", Query: "level:error", StartNs: 0, EndNs: 10, Mode: mode, Tenants: []TenantRef{{AccountID: 1001}}})
		if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/irreversible", headers: as2002}).do(srvB); rec.Code != http.StatusNotFound {
			t.Errorf("mode %s: another tenant's un-delete through an unpolled pod = %d %s, want 404 (a 409 would reveal the id)", mode, rec.Code, rec.Body.String())
		}
		if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/irreversible", headers: as1001}).do(srvB); rec.Code != http.StatusConflict {
			t.Errorf("mode %s: the owner's un-delete = %d %s, want 409", mode, rec.Code, rec.Body.String())
		}
	}
}

// An id the pod does not know and cannot look up (its poll fails) is a gateway
// error, not "not found": the delete may well exist.
func TestDeleteAPI_UndeleteWhenTheLookupFailsIs502(t *testing.T) {
	b := deletetest.NewBucket()
	_, _, _, srv := newScopeHandlerOn(t, b, "b")
	// A pod whose listings fail.
	pod := b.Pod("failing")
	store := NewTombstoneStore()
	if err := store.Attach(Options{Backend: pod, Prefix: testPrefix, Interval: time.Minute, Now: pod.Now}); err != nil {
		t.Fatal(err)
	}
	mustSync(t, store, true)
	pod.FailNext("list", 1)
	err := store.Undelete(context.Background(), "unknown")
	if err == nil || errors.Is(err, ErrTombstoneNotFound) {
		t.Fatalf("Undelete of an unknown id whose lookup failed = %v, want the lookup error", err)
	}
	_ = srv
	// And through the API: 502.
	m := newTestManifest(t, map[string]int64{scopeKey1001: 10})
	h := NewHandler(store, &scopedManifest{m: m}, NewStorageClassDetector(nil), defaultCfg(), "logs")
	mux := http.NewServeMux()
	h.Register(mux)
	pod.FailNext("list", 1)
	if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/unknown"}).do(mux); rec.Code != http.StatusBadGateway {
		t.Fatalf("un-delete with a failing lookup = %d %s, want 502", rec.Code, rec.Body.String())
	}
	// A lookup that works and finds nothing is a plain 404.
	if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/unknown"}).do(mux); rec.Code != http.StatusNotFound {
		t.Fatalf("un-delete of an id that does not exist = %d, want 404", rec.Code)
	}
}
