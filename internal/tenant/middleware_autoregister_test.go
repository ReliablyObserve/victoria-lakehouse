package tenant

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

func TestIsWritePath(t *testing.T) {
	cases := []struct {
		method, path string
		want         bool
	}{
		{"POST", "/insert/jsonline", true},
		{"POST", "/insert/loki/api/v1/push", true},
		{"POST", "/insert/opentelemetry/v1/logs", true},
		{"POST", "/insert/opentelemetry/v1/traces", true},
		{"PUT", "/insert/elasticsearch/_bulk", true},
		{"POST", "/internal/insert", true},
		{"POST", "/api/v2/logs", true},
		{"POST", "/services/collector/event", true},
		{"GET", "/insert/jsonline", false},
		{"POST", "/select/logsql/query", false},
		{"GET", "/select/logsql/query", false},
		{"POST", "/select/logsql/stats_query", false},
		{"GET", "/select/jaeger/api/traces", false},
		{"GET", "/api/search", false},
		{"GET", "/lakehouse/api/v1/tenants/aliases", false},
		{"POST", "/lakehouse/api/v1/tenants/aliases", false},
	}
	for _, tc := range cases {
		if got := IsWritePath(tc.method, tc.path); got != tc.want {
			t.Errorf("IsWritePath(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

func recordingHandler(acc, proj *string, called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		*called = true
		*acc = req.Header.Get("AccountID")
		*proj = req.Header.Get("ProjectID")
		w.WriteHeader(http.StatusOK)
	})
}

func TestMiddleware_WriteAutoRegistersInReservedRange(t *testing.T) {
	pool := NewMemConditionalPool()
	r, _ := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	_ = r.AddAliasFrom("acme-corp", TenantID{AccountID: 1001}, SourceConfig)

	var acc, proj string
	var called bool
	h := r.Middleware(recordingHandler(&acc, &proj, &called))
	req := httptest.NewRequest("POST", "/insert/jsonline", nil)
	req.Header.Set("X-Scope-OrgID", "new-team-xyz")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK || !called {
		t.Fatalf("status %d called=%v", rr.Code, called)
	}
	id, err := strconv.ParseUint(acc, 10, 32)
	if err != nil || !DefaultAutoRange().Contains(uint32(id)) || proj != "0" {
		t.Fatalf("AccountID/ProjectID = %s/%s, want reserved range / 0", acc, proj)
	}
	if acc == "1001" {
		t.Fatal("collided with the configured alias")
	}
	if got := r.DisplayName(uint32(id), 0); got != "new-team-xyz" {
		t.Fatalf("display name = %q", got)
	}
	if got := r.DisplayName(1001, 0); got != "acme-corp" {
		t.Fatalf("configured alias lost its name: %q", got)
	}
	if n := len(persisted(t, pool)); n != 1 {
		t.Fatalf("registry holds %d entries, want 1", n)
	}
}

func TestMiddleware_ReadNeverRegistersAndReadsAsNoTenant(t *testing.T) {
	pool := NewMemConditionalPool()
	r, _ := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	reads := metrics.TenantUnknownOrgIDReadsTotal.Get()

	reqs := []struct{ method, path string }{
		{"GET", "/select/logsql/query"},
		{"POST", "/select/logsql/query"},
		{"GET", "/select/logsql/field_names"},
		{"GET", "/select/jaeger/api/services"},
		{"GET", "/api/search"},
		{"GET", "/api/traces/abc"},
		{"GET", "/insert/jsonline"}, // wrong method for ingest: a read
		{"POST", "/lakehouse/api/v1/tenants/aliases"},
	}
	for _, tc := range reqs {
		var acc, proj string
		var called bool
		h := r.Middleware(recordingHandler(&acc, &proj, &called))
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("X-Scope-OrgID", "made-up-name")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK || !called {
			t.Fatalf("%s %s: status %d called=%v (an unknown read must reach the handler as an empty scope)", tc.method, tc.path, rr.Code, called)
		}
		if acc != strconv.FormatUint(uint64(NullAccountID), 10) || proj != "0" {
			t.Fatalf("%s %s: scope %s/%s, want the null tenant, never 0:0", tc.method, tc.path, acc, proj)
		}
	}
	if _, ok := r.Resolve("made-up-name"); ok {
		t.Fatal("a read registered the OrgID")
	}
	if _, found := pool.Get(testKey); found {
		t.Fatal("a read wrote the alias registry")
	}
	if got := metrics.TenantUnknownOrgIDReadsTotal.Get() - reads; got != uint64(len(reqs)) {
		t.Fatalf("unknown reads counted = %d, want %d", got, len(reqs))
	}
}

func TestMiddleware_ReadSeesAliasAnotherPodRegistered(t *testing.T) {
	pool := NewMemConditionalPool()
	_, podA := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	rB, _ := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	tid, err := podA.Allocate(context.Background(), "written-on-a")
	if err != nil {
		t.Fatal(err)
	}
	var acc, proj string
	var called bool
	h := rB.Middleware(recordingHandler(&acc, &proj, &called))
	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-OrgID", "written-on-a")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if acc != strconv.FormatUint(uint64(tid.AccountID), 10) {
		t.Fatalf("pod B read scope %s, want %d", acc, tid.AccountID)
	}
}

func TestMiddleware_WriteWhenRegistryDownIs503(t *testing.T) {
	pool := NewMemConditionalPool()
	pool.BeforeWrite = func(string) error { return ErrPreconditionFailed }
	r := NewResolver(ResolverConfig{AutoRegister: true})
	if _, err := NewRegistry(r, RegistryConfig{Pool: pool, Key: testKey, MaxAttempts: 2, Backoff: fastBackoff}); err != nil {
		t.Fatal(err)
	}
	h := r.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler reached") }))
	req := httptest.NewRequest("POST", "/insert/jsonline", nil)
	req.Header.Set("X-Scope-OrgID", "unlucky")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rr.Code)
	}
	if _, ok := r.Resolve("unlucky"); ok {
		t.Fatal("registered despite the failure")
	}
}

func TestMiddleware_CannotWriteToTheNullAccount(t *testing.T) {
	r := NewResolver(ResolverConfig{AutoRegister: true})
	h := r.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler reached") }))
	for _, hdr := range []string{"AccountID", "X-Scope-AccountID"} {
		req := httptest.NewRequest("POST", "/insert/jsonline", nil)
		req.Header.Set(hdr, strconv.FormatUint(uint64(NullAccountID), 10))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400", hdr, rr.Code)
		}
	}
}

func TestMiddleware_AutoRegisterOffStillRejectsUnknown(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	h := r.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler reached") }))
	for _, method := range []string{"GET", "POST"} {
		path := "/select/logsql/query"
		if method == "POST" {
			path = "/insert/jsonline"
		}
		req := httptest.NewRequest(method, path, nil)
		req.Header.Set("X-Scope-OrgID", "unknown")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400", method, rr.Code)
		}
	}
}

func TestMiddleware_InvalidOrgIDOnReadIs400(t *testing.T) {
	r := NewResolver(ResolverConfig{AutoRegister: true})
	h := r.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler reached") }))
	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-OrgID", "has/slash")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rr.Code)
	}
}
