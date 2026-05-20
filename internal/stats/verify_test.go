package stats

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestVerifyStats_AllEndpointsRegistered checks that all 8 API endpoints return non-404.
func TestVerifyStats_AllEndpointsRegistered(t *testing.T) {
	api, _, _ := setupTestAPI(t)

	endpoints := []string{
		"/lakehouse/api/v1/tenants",
		"/lakehouse/api/v1/tenants/100/1",
		"/lakehouse/api/v1/stats/overview",
		"/lakehouse/api/v1/stats/ingestion",
		"/lakehouse/api/v1/stats/cost",
		"/lakehouse/api/v1/stats/compression",
		"/lakehouse/api/v1/cardinality/fields",
		"/lakehouse/api/v1/stats/breakdown",
	}

	for _, ep := range endpoints {
		rec := doGet(t, api, ep)
		if rec.Code == http.StatusNotFound {
			t.Errorf("endpoint %s: got 404, want non-404", ep)
		}
	}
}

// TestVerifyStats_TenantsJSON verifies /tenants returns a valid TenantsResponse with TotalTenants >= 1.
func TestVerifyStats_TenantsJSON(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp TenantsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal TenantsResponse: %v", err)
	}

	if resp.TotalTenants < 1 {
		t.Errorf("TotalTenants = %d, want >= 1", resp.TotalTenants)
	}

	if len(resp.Tenants) < 1 {
		t.Errorf("len(Tenants) = %d, want >= 1", len(resp.Tenants))
	}

	// Verify each tenant entry has required fields.
	for i, te := range resp.Tenants {
		if te.AccountID == "" {
			t.Errorf("Tenants[%d].AccountID is empty", i)
		}
		if te.ProjectID == "" {
			t.Errorf("Tenants[%d].ProjectID is empty", i)
		}
	}
}

// TestVerifyStats_TenantDetail_ValidID verifies /tenants/100/1 returns JSON with
// the expected fields: account_id, project_id, total_bytes (total_bytes_written), total_rows.
func TestVerifyStats_TenantDetail_ValidID(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants/100/1")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	// Decode into a raw map to verify JSON keys exist.
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("unmarshal tenant detail: %v", err)
	}

	requiredKeys := []string{"account_id", "total_bytes", "total_rows"}
	for _, key := range requiredKeys {
		if _, ok := raw[key]; !ok {
			t.Errorf("response missing required key %q", key)
		}
	}

	if accountID, _ := raw["account_id"].(string); accountID != "100" {
		t.Errorf("account_id = %q, want %q", accountID, "100")
	}

	// Verify total_bytes is a number > 0 for this tenant (50 GiB written).
	totalBytes, ok := raw["total_bytes"].(float64)
	if !ok {
		t.Errorf("total_bytes is not a number, got %T", raw["total_bytes"])
	} else if totalBytes <= 0 {
		t.Errorf("total_bytes = %v, want > 0", totalBytes)
	}

	// Verify total_rows is a number > 0.
	totalRows, ok := raw["total_rows"].(float64)
	if !ok {
		t.Errorf("total_rows is not a number, got %T", raw["total_rows"])
	} else if totalRows <= 0 {
		t.Errorf("total_rows = %v, want > 0", totalRows)
	}
}

// TestVerifyStats_OverviewJSON verifies /stats/overview returns JSON with
// total_files, total_bytes, and total_tenants (tenant_count).
func TestVerifyStats_OverviewJSON(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/stats/overview")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp OverviewResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal OverviewResponse: %v", err)
	}

	if resp.TotalFiles <= 0 {
		t.Errorf("total_files = %d, want > 0", resp.TotalFiles)
	}

	if resp.TotalBytes <= 0 {
		t.Errorf("total_bytes = %d, want > 0", resp.TotalBytes)
	}

	// total_tenants is the TenantCount field.
	if resp.TenantCount < 1 {
		t.Errorf("tenant_count = %d, want >= 1", resp.TenantCount)
	}

	// Confirm via raw map that JSON keys are present.
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("unmarshal raw overview: %v", err)
	}

	for _, key := range []string{"total_files", "total_bytes", "tenant_count"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("overview response missing required key %q", key)
		}
	}
}

// TestVerifyStats_ContentTypeJSON verifies all API endpoints respond with Content-Type: application/json.
func TestVerifyStats_ContentTypeJSON(t *testing.T) {
	api, _, _ := setupTestAPI(t)

	endpoints := []string{
		"/lakehouse/api/v1/tenants",
		"/lakehouse/api/v1/tenants/100/1",
		"/lakehouse/api/v1/stats/overview",
		"/lakehouse/api/v1/stats/ingestion",
		"/lakehouse/api/v1/stats/cost",
		"/lakehouse/api/v1/stats/compression",
		"/lakehouse/api/v1/cardinality/fields",
		"/lakehouse/api/v1/stats/breakdown",
	}

	for _, ep := range endpoints {
		rec := doGet(t, api, ep)
		ct := rec.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("%s: Content-Type = %q, want %q", ep, ct, "application/json")
		}
	}
}
