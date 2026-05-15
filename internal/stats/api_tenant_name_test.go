package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

func TestTenantEntry_HasNameField(t *testing.T) {
	entry := TenantEntry{
		AccountID: "42",
		ProjectID: "3",
		Name:      "prod_staging",
	}
	data, _ := json.Marshal(entry)
	var m map[string]any
	_ = json.Unmarshal(data, &m)

	if m["name"] != "prod_staging" {
		t.Errorf("name field = %v, want %q", m["name"], "prod_staging")
	}
}

func TestTenantCostEntry_HasNameField(t *testing.T) {
	entry := TenantCostEntry{
		AccountID: "42",
		ProjectID: "3",
		Name:      "prod_staging",
	}
	data, _ := json.Marshal(entry)
	var m map[string]any
	_ = json.Unmarshal(data, &m)

	if m["name"] != "prod_staging" {
		t.Errorf("name field = %v, want %q", m["name"], "prod_staging")
	}
}

func TestTenantCompressionEntry_HasNameField(t *testing.T) {
	entry := TenantCompressionEntry{
		AccountID: "42",
		ProjectID: "3",
		Name:      "prod_staging",
	}
	data, _ := json.Marshal(entry)
	var m map[string]any
	_ = json.Unmarshal(data, &m)

	if m["name"] != "prod_staging" {
		t.Errorf("name field = %v, want %q", m["name"], "prod_staging")
	}
}

func TestDecorateCostName(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	resolver.AddAlias("prod_staging", tenant.TenantID{AccountID: 42, ProjectID: 3})

	api := NewAPI(APIConfig{Resolver: resolver})

	entry := TenantCostEntry{AccountID: "42", ProjectID: "3"}
	api.decorateCostName(&entry)
	if entry.Name != "prod_staging" {
		t.Errorf("cost name = %q, want %q", entry.Name, "prod_staging")
	}

	noAlias := TenantCostEntry{AccountID: "99", ProjectID: "1"}
	api.decorateCostName(&noAlias)
	if noAlias.Name != "" {
		t.Errorf("cost name = %q, want empty for unknown tenant", noAlias.Name)
	}
}

func TestDecorateCompressionName(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	resolver.AddAlias("dev_default", tenant.TenantID{AccountID: 1, ProjectID: 1})

	api := NewAPI(APIConfig{Resolver: resolver})

	entry := TenantCompressionEntry{AccountID: "1", ProjectID: "1"}
	api.decorateCompressionName(&entry)
	if entry.Name != "dev_default" {
		t.Errorf("compression name = %q, want %q", entry.Name, "dev_default")
	}
}

func TestTenantDetail_AliasRoute(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	resolver.AddAlias("prod_staging", tenant.TenantID{AccountID: 42, ProjectID: 3})

	registry := NewTenantRegistry("test-node")
	registry.RecordWrite("42:3", 1000, 2000, 10, "STANDARD")

	api := NewAPI(APIConfig{
		Registry: registry,
		Resolver: resolver,
		Mode:     "logs",
		Bucket:   "test-bucket",
	})

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/prod_staging", nil)
	rr := httptest.NewRecorder()

	mux := http.NewServeMux()
	api.Register(mux)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var resp TenantDetailResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Name != "prod_staging" {
		t.Errorf("name = %q, want %q", resp.Name, "prod_staging")
	}
	if resp.AccountID != "42" {
		t.Errorf("account_id = %q, want %q", resp.AccountID, "42")
	}
}
