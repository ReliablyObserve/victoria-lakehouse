package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

func TestBreakdown_GroupByTenant_ReturnsRegistrySnapshot(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	_ = resolver.AddAlias("acme-corp", tenant.TenantID{AccountID: 1002, ProjectID: 0})

	registry := NewTenantRegistry("test")
	registry.RecordWrite("0:0", 800, 1600, 80, "STANDARD")
	registry.RecordWrite("1002:0", 200, 400, 20, "STANDARD")

	api := NewAPI(APIConfig{Registry: registry, Resolver: resolver, Mode: "logs", Bucket: "b"})
	mux := http.NewServeMux()
	api.Register(mux)

	t.Run("group_by tenant param", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown?group_by=tenant", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		assertTenantBreakdown(t, rr, registry, resolver)
	})

	t.Run("label tenant param", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown?label=tenant", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		assertTenantBreakdown(t, rr, registry, resolver)
	})
}

func assertTenantBreakdown(t *testing.T, rr *httptest.ResponseRecorder, _ *TenantRegistry, _ *tenant.TenantResolver) {
	t.Helper()
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var resp BreakdownResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Labels) != 1 {
		t.Fatalf("expected exactly 1 label, got %d", len(resp.Labels))
	}
	bl := resp.Labels[0]
	if bl.Name != "tenant" {
		t.Errorf("label name = %q, want %q", bl.Name, "tenant")
	}
	if bl.Cardinality != 2 {
		t.Errorf("cardinality = %d, want 2", bl.Cardinality)
	}

	byValue := make(map[string]BreakdownValue, len(bl.Values))
	for _, v := range bl.Values {
		byValue[v.Value] = v
	}

	if v, ok := byValue["1002:0"]; !ok {
		t.Fatal("missing 1002:0 in breakdown")
	} else {
		if v.OrgID != "acme-corp" {
			t.Errorf("1002:0 org_id = %q, want acme-corp", v.OrgID)
		}
		if v.EstimatedBytes != 200 {
			t.Errorf("1002:0 estimated_bytes = %d, want 200 (exact, not estimated)", v.EstimatedBytes)
		}
	}
	if v, ok := byValue["0:0"]; !ok {
		t.Fatal("missing 0:0 in breakdown")
	} else {
		if v.OrgID != "" {
			t.Errorf("0:0 org_id = %q, want empty (no alias)", v.OrgID)
		}
	}
}

// TestCostEndpoint_DecoratesOrgID guards the cross-endpoint contract that
// per-tenant entries in /api/v1/stats/cost expose org_id alongside the
// integer account:project key, so the UI doesn't fall back to numeric-only
// rendering on the cost screen.
func TestCostEndpoint_DecoratesOrgID(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	_ = resolver.AddAlias("billing-team", tenant.TenantID{AccountID: 99, ProjectID: 0})

	registry := NewTenantRegistry("test")
	registry.RecordWrite("99:0", 1000, 2000, 50, "STANDARD")

	api := NewAPI(APIConfig{
		Registry: registry, Resolver: resolver,
		CostCalc: NewCostCalculator(nil, nil),
		Mode:     "logs", Bucket: "b",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/cost", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	var resp CostResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.PerTenant) != 1 {
		t.Fatalf("expected 1 per-tenant entry, got %d", len(resp.PerTenant))
	}
	e := resp.PerTenant[0]
	if e.OrgID != "billing-team" {
		t.Errorf("per_tenant[0].org_id = %q, want billing-team", e.OrgID)
	}
	if e.Name != "billing-team" {
		t.Errorf("per_tenant[0].name = %q, want billing-team (legacy field kept for UI compat)", e.Name)
	}
}
