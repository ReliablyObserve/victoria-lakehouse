package tenant

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware_OrgIDResolved(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("prod-team-eu_staging", TenantID{AccountID: 42, ProjectID: 3})

	var gotAccount, gotProject string
	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotAccount = req.Header.Get("X-Scope-AccountID")
		gotProject = req.Header.Get("X-Scope-ProjectID")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-OrgID", "prod-team-eu_staging")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
	if gotAccount != "42" {
		t.Errorf("account = %q, want %q", gotAccount, "42")
	}
	if gotProject != "3" {
		t.Errorf("project = %q, want %q", gotProject, "3")
	}
}

func TestMiddleware_OrgIDUnknown(t *testing.T) {
	r := NewResolver(ResolverConfig{})

	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Error("handler should not be called for unknown org")
	}))

	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-OrgID", "unknown-org")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestMiddleware_NoOrgIDPassthrough(t *testing.T) {
	r := NewResolver(ResolverConfig{})

	called := false
	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-AccountID", "42")
	req.Header.Set("X-Scope-ProjectID", "3")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if !called {
		t.Error("handler should be called when no X-Scope-OrgID")
	}
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

func TestMiddleware_AutoRegister(t *testing.T) {
	r := NewResolver(ResolverConfig{AutoRegister: true})

	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("POST", "/insert/jsonline", nil)
	req.Header.Set("X-Scope-OrgID", "new-auto-tenant")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for auto-register", rr.Code)
	}

	_, ok := r.Resolve("new-auto-tenant")
	if !ok {
		t.Error("expected auto-registered tenant to be resolvable")
	}
}

func TestMiddleware_CustomHeader(t *testing.T) {
	r := NewResolver(ResolverConfig{OrgIDHeader: "X-Custom-Tenant"})
	r.AddAlias("custom_tenant", TenantID{AccountID: 5, ProjectID: 6})

	var gotAccount string
	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotAccount = req.Header.Get("X-Scope-AccountID")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Custom-Tenant", "custom_tenant")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if gotAccount != "5" {
		t.Errorf("account = %q, want %q", gotAccount, "5")
	}
}
