package buffer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The buffer holds EVERY tenant's unflushed rows, so /internal/buffer/query is
// only safe when it is told which tenant is asking. These tests pin that
// contract: the tenant parameters are required, the answer is filtered to that
// tenant, and the response proves which tenant it was filtered to.

func tenantBufferStore(base time.Time) *mockBufferStore {
	return &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: base.UnixNano(), AccountID: 0, ProjectID: 0, Body: "tenant-0-0"},
			{TimestampUnixNano: base.Add(time.Second).UnixNano(), AccountID: 1001, ProjectID: 0, Body: "tenant-1001-0"},
			{TimestampUnixNano: base.Add(2 * time.Second).UnixNano(), AccountID: 2002, ProjectID: 7, Body: "tenant-2002-7"},
		},
		traceRows: []schema.TraceRow{
			{TimestampUnixNano: base.UnixNano(), AccountID: 0, ProjectID: 0, TraceID: "trace-0-0"},
			{TimestampUnixNano: base.Add(time.Second).UnixNano(), AccountID: 1001, ProjectID: 0, TraceID: "trace-1001-0"},
		},
	}
}

func tenantBufferURL(base time.Time, mode, account, project string) string {
	return fmt.Sprintf("/internal/buffer/query?start=%d&end=%d&mode=%s&account_id=%s&project_id=%s&tenant_scope=%s",
		base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano(), mode, account, project, TenantScopeVersion)
}

func TestBufferQuery_Tenant_FiltersLogRows(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), "")

	cases := []struct {
		account, project string
		wantBodies       []string
	}{
		{"0", "0", []string{"tenant-0-0"}},
		{"1001", "0", []string{"tenant-1001-0"}},
		{"2002", "7", []string{"tenant-2002-7"}},
		{"4242", "0", nil},
	}
	for _, tc := range cases {
		t.Run(tc.account+":"+tc.project, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tenantBufferURL(base, "logs", tc.account, tc.project), nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			if got, want := rec.Header().Get(TenantScopeHeader), tc.account+":"+tc.project; got != want {
				t.Errorf("%s = %q, want %q", TenantScopeHeader, got, want)
			}
			var bodies []string
			dec := json.NewDecoder(rec.Body)
			for dec.More() {
				var row schema.LogRow
				if err := dec.Decode(&row); err != nil {
					t.Fatalf("decode: %v", err)
				}
				bodies = append(bodies, row.Body)
			}
			if len(bodies) != len(tc.wantBodies) {
				t.Fatalf("got rows %v, want %v", bodies, tc.wantBodies)
			}
			for i := range bodies {
				if bodies[i] != tc.wantBodies[i] {
					t.Errorf("row %d = %q, want %q", i, bodies[i], tc.wantBodies[i])
				}
			}
		})
	}
}

func TestBufferQuery_Tenant_FiltersTraceRows(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), "")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tenantBufferURL(base, "traces", "1001", "0"), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	dec := json.NewDecoder(rec.Body)
	n := 0
	for dec.More() {
		var row schema.TraceRow
		if err := dec.Decode(&row); err != nil {
			t.Fatalf("decode: %v", err)
		}
		n++
		if row.TraceID != "trace-1001-0" {
			t.Errorf("tenant 1001:0 got a foreign trace row %q", row.TraceID)
		}
	}
	if n != 1 {
		t.Errorf("got %d trace rows, want 1", n)
	}
}

func TestBufferQuery_Tenant_ParametersAreRequired(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), "")
	window := fmt.Sprintf("start=%d&end=%d&mode=logs", base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())

	cases := []struct {
		name string
		url  string
	}{
		{"no tenant at all (a pre-fix caller)", "/internal/buffer/query?" + window},
		{"no scope version", "/internal/buffer/query?" + window + "&account_id=0&project_id=0"},
		{"wrong scope version", "/internal/buffer/query?" + window + "&account_id=0&project_id=0&tenant_scope=v0"},
		{"account only", "/internal/buffer/query?" + window + "&account_id=0&tenant_scope=" + TenantScopeVersion},
		{"project only", "/internal/buffer/query?" + window + "&project_id=0&tenant_scope=" + TenantScopeVersion},
		{"non-numeric account", "/internal/buffer/query?" + window + "&account_id=x&project_id=0&tenant_scope=" + TenantScopeVersion},
		{"non-numeric project", "/internal/buffer/query?" + window + "&account_id=0&project_id=x&tenant_scope=" + TenantScopeVersion},
		{"account out of uint32 range", "/internal/buffer/query?" + window + "&account_id=4294967296&project_id=0&tenant_scope=" + TenantScopeVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.url, nil))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 — an unscoped buffer query must be refused, not answered", rec.Code)
			}
			if rec.Body.Len() > 0 && rec.Header().Get(TenantScopeHeader) != "" {
				t.Error("a refused request must not carry a tenant-scope header")
			}
		})
	}
}

func TestBufferQuery_Tenant_AllTenantsForAuthorisedCrossTenantRead(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), "")

	u := fmt.Sprintf("/internal/buffer/query?start=%d&end=%d&mode=logs&all_tenants=true&tenant_scope=%s",
		base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano(), TenantScopeVersion)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, u, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get(TenantScopeHeader); got != AllTenantsScope {
		t.Errorf("%s = %q, want %q", TenantScopeHeader, got, AllTenantsScope)
	}
	n := 0
	dec := json.NewDecoder(rec.Body)
	for dec.More() {
		var row schema.LogRow
		if err := dec.Decode(&row); err != nil {
			t.Fatalf("decode: %v", err)
		}
		n++
	}
	if n != 3 {
		t.Errorf("all-tenants answer carried %d rows, want every tenant's 3", n)
	}
}

func TestBufferQuery_Tenant_SelectionFormsAreExclusive(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), "")
	window := fmt.Sprintf("start=%d&end=%d&mode=logs&tenant_scope=%s", base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano(), TenantScopeVersion)

	for _, tc := range []struct{ name, extra string }{
		{"all_tenants combined with account/project", "&all_tenants=true&account_id=0&project_id=0"},
		{"all_tenants combined with account only", "&all_tenants=true&account_id=0"},
		{"all_tenants=false is not a selection", "&all_tenants=false"},
		{"all_tenants=1 is not accepted", "&all_tenants=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/buffer/query?"+window+tc.extra, nil))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
		})
	}
}
