package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

func TestTenantPolicy_Endpoint_ListsResolvedOverrides(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	_ = resolver.AddAlias("acme-corp", tenant.TenantID{AccountID: 1002, ProjectID: 0})

	policy, err := tenant.NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {
			Retention:   config.TenantRetentionOverride{Keep: "7d"},
			Cardinality: config.TenantCardinalityOverride{MaxFields: 1000},
		},
		"acme-corp": {
			Ingest: config.TenantIngestOverride{MaxBytesPerSec: 5 * 1024 * 1024},
		},
		"pending-team": {
			Retention: config.TenantRetentionOverride{Keep: "90d"},
		},
	}, resolver)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}

	registry := NewTenantRegistry("test")
	registry.RecordWrite("1:1", 100, 200, 10, "STANDARD")
	registry.RecordWrite("1002:0", 200, 400, 20, "STANDARD")

	api := NewAPI(APIConfig{Registry: registry, Resolver: resolver, Policy: policy, Mode: "logs", Bucket: "b"})
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/policy", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rr.Code, rr.Body.String())
	}

	var resp TenantPolicyListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(resp.PendingAliases) != 1 || resp.PendingAliases[0] != "pending-team" {
		t.Errorf("pending_aliases = %v, want [pending-team]", resp.PendingAliases)
	}

	byKey := map[string]TenantPolicyEntry{}
	for _, e := range resp.Entries {
		k := keyOf(e.AccountID, e.ProjectID)
		byKey[k] = e
	}
	if e, ok := byKey["1:1"]; !ok {
		t.Fatal("missing 1:1 in policy listing")
	} else {
		if e.Retention != "168h0m0s" {
			t.Errorf("1:1 retention = %q, want 168h0m0s", e.Retention)
		}
		if e.MaxFields != 1000 {
			t.Errorf("1:1 max_fields = %d, want 1000", e.MaxFields)
		}
	}
	if e, ok := byKey["1002:0"]; !ok {
		t.Fatal("missing 1002:0 in policy listing")
	} else {
		if e.OrgID != "acme-corp" {
			t.Errorf("1002:0 org_id = %q, want acme-corp", e.OrgID)
		}
		if e.MaxBytesPerSec != 5*1024*1024 {
			t.Errorf("1002:0 max_bytes_per_sec = %d, want 5MiB", e.MaxBytesPerSec)
		}
	}
}

func TestTenantDetail_IncludesPolicyWhenConfigured(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	_ = resolver.AddAlias("billing", tenant.TenantID{AccountID: 50, ProjectID: 1})

	policy, _ := tenant.NewPolicyRegistry(map[string]config.TenantOverride{
		"billing": {Retention: config.TenantRetentionOverride{Keep: "30d"}},
	}, resolver)

	registry := NewTenantRegistry("test")
	registry.RecordWrite("50:1", 1, 1, 1, "STANDARD")

	api := NewAPI(APIConfig{Registry: registry, Resolver: resolver, Policy: policy, Mode: "logs", Bucket: "b"})
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/50/1", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rr.Code, rr.Body.String())
	}

	var resp TenantDetailResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Policy == nil {
		t.Fatal("expected policy in detail response, got nil")
	}
	if resp.Policy.Retention != "720h0m0s" {
		t.Errorf("retention = %q, want 720h0m0s", resp.Policy.Retention)
	}
	if resp.Policy.OrgID != "billing" {
		t.Errorf("org_id = %q, want billing", resp.Policy.OrgID)
	}
}

func TestTenantDetail_OmitsPolicyWhenNoOverride(t *testing.T) {
	policy, _ := tenant.NewPolicyRegistry(nil, nil)
	registry := NewTenantRegistry("test")
	registry.RecordWrite("5:5", 1, 1, 1, "STANDARD")

	api := NewAPI(APIConfig{Registry: registry, Policy: policy, Mode: "logs", Bucket: "b"})
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/5/5", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	var resp TenantDetailResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Policy != nil {
		t.Errorf("policy = %+v, want nil for tenant without override", resp.Policy)
	}
}

func keyOf(a, p uint32) string {
	return formatU32(a) + ":" + formatU32(p)
}

func formatU32(x uint32) string {
	// strconv pulled in by the api package already; use the std-lib path
	// via fmt to keep this test self-contained.
	return jsonNumber(x)
}

func jsonNumber(x uint32) string {
	b, _ := json.Marshal(x)
	return string(b)
}
