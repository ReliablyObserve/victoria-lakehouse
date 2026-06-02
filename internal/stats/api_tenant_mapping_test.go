package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// TestTenantsEndpoint_BidirectionalMapping verifies that /api/v1/tenants
// surfaces both the integer (account_id:project_id) and the string OrgID
// for every tenant the registry knows about, plus alias-only tenants
// known to the resolver but not yet seen by the writer.
//
// Covers the int↔string mapping contract documented in
// docs/multi-tenancy.md "Tenant Name Mapping (X-Scope-OrgID)".
func TestTenantsEndpoint_BidirectionalMapping(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	_ = resolver.AddAlias("acme-corp", tenant.TenantID{AccountID: 1002, ProjectID: 0})
	_ = resolver.AddAlias("staging-team", tenant.TenantID{AccountID: 1001, ProjectID: 0})

	registry := NewTenantRegistry("test-node")
	// Tenant 1:1 wrote rows but has no alias → expect numeric ID with empty org_id.
	registry.RecordWrite("1:1", 1000, 2000, 10, "STANDARD")
	// Tenant 1002:0 has alias acme-corp → expect org_id="acme-corp".
	registry.RecordWrite("1002:0", 2000, 4000, 20, "STANDARD")
	// Default tenant 0:0 (continuous datagen path) → expect numeric ID, no org_id.
	registry.RecordWrite("0:0", 500, 1000, 5, "STANDARD")
	// Note: staging-team (1001:0) has an alias but no writes — should still appear
	// as an alias-only entry with org_id populated.

	api := NewAPI(APIConfig{
		Registry: registry,
		Resolver: resolver,
		Mode:     "logs",
		Bucket:   "test-bucket",
	})

	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var resp TenantsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	byKey := make(map[string]TenantEntry, len(resp.Tenants))
	for _, e := range resp.Tenants {
		byKey[e.AccountID+":"+e.ProjectID] = e
	}

	// Tenant 1002:0 — registry write + alias → both numeric and string visible.
	acme, ok := byKey["1002:0"]
	if !ok {
		t.Fatal("tenant 1002:0 missing from response")
	}
	if acme.OrgID != "acme-corp" {
		t.Errorf("1002:0 org_id = %q, want %q", acme.OrgID, "acme-corp")
	}
	if acme.TotalRows != 20 {
		t.Errorf("1002:0 total_rows = %d, want 20", acme.TotalRows)
	}

	// Tenant 1:1 — registry write, no alias → numeric only.
	t11, ok := byKey["1:1"]
	if !ok {
		t.Fatal("tenant 1:1 missing from response")
	}
	if t11.OrgID != "" {
		t.Errorf("1:1 org_id = %q, want empty (no alias)", t11.OrgID)
	}

	// Tenant 0:0 — default tenant, no alias → numeric only.
	if z, ok := byKey["0:0"]; !ok {
		t.Fatal("tenant 0:0 missing from response")
	} else if z.OrgID != "" {
		t.Errorf("0:0 org_id = %q, want empty", z.OrgID)
	}

	// Tenant 1001:0 — alias only (no writes yet) → still listed via resolver.
	staging, ok := byKey["1001:0"]
	if !ok {
		t.Fatal("alias-only tenant 1001:0 missing from response")
	}
	if staging.OrgID != "staging-team" {
		t.Errorf("1001:0 org_id = %q, want %q", staging.OrgID, "staging-team")
	}
	if staging.Source != "alias" {
		t.Errorf("1001:0 source = %q, want %q", staging.Source, "alias")
	}

	if resp.TotalTenants != 4 {
		t.Errorf("total_tenants = %d, want 4 (three writes + one alias-only)", resp.TotalTenants)
	}
}

// TestTenantsEndpoint_NoResolver verifies the endpoint still works without
// a resolver — every entry should have empty OrgID and Source.
func TestTenantsEndpoint_NoResolver(t *testing.T) {
	registry := NewTenantRegistry("test-node")
	registry.RecordWrite("42:3", 1000, 2000, 10, "STANDARD")

	api := NewAPI(APIConfig{Registry: registry, Mode: "logs", Bucket: "b"})
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	var resp TenantsResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if len(resp.Tenants) != 1 {
		t.Fatalf("got %d tenants, want 1", len(resp.Tenants))
	}
	if resp.Tenants[0].OrgID != "" {
		t.Errorf("OrgID = %q, want empty without resolver", resp.Tenants[0].OrgID)
	}
}
