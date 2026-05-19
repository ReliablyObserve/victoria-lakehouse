package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func setupTestAPI(t *testing.T) (*API, *TenantRegistry, *manifest.Manifest) {
	t.Helper()

	reg := NewTenantRegistry("node-1")
	// Tenant 100:1 — large tenant (50 GiB written, 100 GiB raw, 5M rows, STANDARD)
	reg.RecordWrite("100:1", 50<<30, 100<<30, 5000000, "STANDARD")
	// Tenant 200:5 — smaller tenant (10 GiB written, 20 GiB raw, 1M rows, STANDARD_IA)
	reg.RecordWrite("200:5", 10<<30, 20<<30, 1000000, "STANDARD_IA")
	reg.RecordQuery("100:1")

	m := manifest.New("test-bucket", "data/")
	m.AddFile("dt=2026-05-10/hour=10", manifest.FileInfo{
		Key:      "data/dt=2026-05-10/hour=10/part-001.parquet",
		Size:     1024 * 1024,
		RowCount: 50000,
		RawBytes: 2048 * 1024,
	})
	m.AddFile("dt=2026-05-11/hour=14", manifest.FileInfo{
		Key:      "data/dt=2026-05-11/hour=14/part-002.parquet",
		Size:     512 * 1024,
		RowCount: 25000,
		RawBytes: 1024 * 1024,
	})

	storagePrices := map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
	}
	cc := NewCostCalculator(storagePrices, nil)

	sct := NewStorageClassTracker(nil, nil)

	li := cache.NewLabelIndex()
	li.AddWithTenant("hostname", []string{"host-1", "host-2", "host-3"}, "100:1")
	li.AddWithTenant("service", []string{"api", "web"}, "100:1")
	li.AddWithTenant("level", []string{"info", "warn", "error", "debug"}, "200:5")

	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     m,
		CostCalc:     cc,
		ClassTracker: sct,
		LabelIndex:   li,
		Mode:         "logs",
		Bucket:       "test-bucket",
	})

	return api, reg, m
}

func doGet(t *testing.T, api *API, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	api.Register(mux)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func doMethod(t *testing.T, api *API, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	api.Register(mux)
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestAPITenantsEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp TenantsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.TotalTenants != 2 {
		t.Errorf("TotalTenants = %d, want 2", resp.TotalTenants)
	}
	if len(resp.Tenants) != 2 {
		t.Errorf("len(Tenants) = %d, want 2", len(resp.Tenants))
	}
	if resp.TotalBytes <= 0 {
		t.Errorf("TotalBytes = %d, want > 0", resp.TotalBytes)
	}
	if resp.TotalFiles <= 0 {
		t.Errorf("TotalFiles = %d, want > 0", resp.TotalFiles)
	}

	// Default sort is bytes descending — first tenant should be the larger one.
	if resp.Tenants[0].AccountID != "100" {
		t.Errorf("first tenant AccountID = %q, want %q", resp.Tenants[0].AccountID, "100")
	}
}

func TestAPITenantsSort(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants?sort=bytes")

	var resp TenantsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Tenants) < 2 {
		t.Fatal("expected at least 2 tenants")
	}
	if resp.Tenants[0].TotalBytes < resp.Tenants[1].TotalBytes {
		t.Errorf("sort=bytes: first=%d < second=%d", resp.Tenants[0].TotalBytes, resp.Tenants[1].TotalBytes)
	}
}

func TestAPITenantsSortByFiles(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants?sort=files")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp TenantsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Tenants) < 2 {
		t.Fatal("expected at least 2 tenants")
	}
	// Both have 1 file each, so order may vary, but should be valid.
	if resp.Tenants[0].TotalFiles < resp.Tenants[1].TotalFiles {
		t.Errorf("sort=files: first=%d < second=%d", resp.Tenants[0].TotalFiles, resp.Tenants[1].TotalFiles)
	}
}

func TestAPITenantsSortByCost(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants?sort=cost")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp TenantsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Tenants) < 2 {
		t.Fatal("expected at least 2 tenants")
	}
	if resp.Tenants[0].MonthlyCostUSD < resp.Tenants[1].MonthlyCostUSD {
		t.Errorf("sort=cost: first=%f < second=%f", resp.Tenants[0].MonthlyCostUSD, resp.Tenants[1].MonthlyCostUSD)
	}
}

func TestAPIOverviewEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/stats/overview")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp OverviewResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Bucket != "test-bucket" {
		t.Errorf("Bucket = %q, want %q", resp.Bucket, "test-bucket")
	}
	if resp.Mode != "logs" {
		t.Errorf("Mode = %q, want %q", resp.Mode, "logs")
	}
	if resp.TotalFiles <= 0 {
		t.Errorf("TotalFiles = %d, want > 0", resp.TotalFiles)
	}
	if resp.TenantCount != 2 {
		t.Errorf("TenantCount = %d, want 2", resp.TenantCount)
	}
	if resp.RegistryGeneration == 0 {
		t.Error("RegistryGeneration should not be 0")
	}
}

func TestAPICostEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/stats/cost")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp CostResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.TotalMonthlyUSD <= 0 {
		t.Errorf("TotalMonthlyUSD = %f, want > 0", resp.TotalMonthlyUSD)
	}
}

func TestAPICardinalityEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/cardinality/fields")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp CardinalityResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.TotalFields != 3 {
		t.Errorf("TotalFields = %d, want 3", resp.TotalFields)
	}
	if len(resp.Fields) == 0 {
		t.Error("Fields should not be empty")
	}
}

func TestAPITenantDetailEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants/100/1")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp TenantEntry
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.AccountID != "100" {
		t.Errorf("AccountID = %q, want %q", resp.AccountID, "100")
	}
	if resp.ProjectID != "1" {
		t.Errorf("ProjectID = %q, want %q", resp.ProjectID, "1")
	}
}

func TestAPITenantDetailNotFound(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants/999/99")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (empty tenant)", rec.Code)
	}
	var resp TenantDetailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.AccountID != "999" || resp.ProjectID != "99" {
		t.Errorf("tenant IDs = %s:%s, want 999:99", resp.AccountID, resp.ProjectID)
	}
	if resp.TotalFiles != 0 {
		t.Errorf("total_files = %d, want 0", resp.TotalFiles)
	}
}

func TestAPIIngestionEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/stats/ingestion?period=day&range=7d")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp IngestionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Period != "day" {
		t.Errorf("Period = %q, want %q", resp.Period, "day")
	}
	if resp.Range != "7d" {
		t.Errorf("Range = %q, want %q", resp.Range, "7d")
	}
}

func TestAPICompressionEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/stats/compression")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp CompressionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.AvgRatio <= 0 {
		t.Errorf("AvgRatio = %f, want > 0", resp.AvgRatio)
	}
	if len(resp.PerTenant) != 2 {
		t.Errorf("len(PerTenant) = %d, want 2", len(resp.PerTenant))
	}
}

func TestAPIMethodNotAllowed(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doMethod(t, api, http.MethodPost, "/lakehouse/api/v1/tenants")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestAPITenantsInvalidSort(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/tenants?sort=invalid")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (fallback to default sort)", rec.Code)
	}

	var resp TenantsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.TotalTenants != 2 {
		t.Errorf("TotalTenants = %d, want 2", resp.TotalTenants)
	}
}

func TestAPICardinalityWithTenantFilter(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/cardinality/fields?tenant=100:1")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp CardinalityResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Tenant 100:1 has "hostname" (3 values) and "service" (2 values),
	// but NOT "level" (that's 200:5).
	if resp.TotalFields != 2 {
		t.Errorf("TotalFields = %d, want 2 (only fields for tenant 100:1)", resp.TotalFields)
	}
}

func TestAPIIngestionInvalidPeriod(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/stats/ingestion?period=invalid&range=invalid")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (fallback to defaults)", rec.Code)
	}

	var resp IngestionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Falls back to day/7d
	if resp.Period != "day" {
		t.Errorf("Period = %q, want %q (default)", resp.Period, "day")
	}
	if resp.Range != "7d" {
		t.Errorf("Range = %q, want %q (default)", resp.Range, "7d")
	}
}

func TestAPIResponseContentType(t *testing.T) {
	api, _, _ := setupTestAPI(t)

	endpoints := []string{
		"/lakehouse/api/v1/tenants",
		"/lakehouse/api/v1/tenants/100/1",
		"/lakehouse/api/v1/stats/overview",
		"/lakehouse/api/v1/stats/ingestion",
		"/lakehouse/api/v1/stats/cost",
		"/lakehouse/api/v1/stats/compression",
		"/lakehouse/api/v1/cardinality/fields",
	}

	for _, ep := range endpoints {
		rec := doGet(t, api, ep)
		ct := rec.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("%s: Content-Type = %q, want %q", ep, ct, "application/json")
		}
	}
}

func TestAPICostPerTenant(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/stats/cost")

	var resp CostResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.PerTenant) != 2 {
		t.Fatalf("len(PerTenant) = %d, want 2", len(resp.PerTenant))
	}

	// First entry should have highest cost (STANDARD at 50 GiB > STANDARD_IA at 10 GiB).
	if resp.PerTenant[0].CostUSD <= 0 {
		t.Errorf("PerTenant[0].CostUSD = %f, want > 0", resp.PerTenant[0].CostUSD)
	}
	if resp.PerTenant[0].CostUSD < resp.PerTenant[1].CostUSD {
		t.Errorf("per_tenant not sorted by cost desc: first=%f < second=%f",
			resp.PerTenant[0].CostUSD, resp.PerTenant[1].CostUSD)
	}
}

func TestAPICardinalitySortByName(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/cardinality/fields?sort=name")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp CardinalityResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Fields should be sorted alphabetically: hostname, level, service.
	if len(resp.Fields) < 2 {
		t.Fatal("expected at least 2 fields")
	}
	for i := 1; i < len(resp.Fields); i++ {
		if resp.Fields[i-1].Name > resp.Fields[i].Name {
			t.Errorf("sort=name: fields[%d].Name=%q > fields[%d].Name=%q",
				i-1, resp.Fields[i-1].Name, i, resp.Fields[i].Name)
		}
	}
}

func TestAPICardinalityLimit(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/cardinality/fields?limit=1")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp CardinalityResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Fields) != 1 {
		t.Errorf("len(Fields) = %d, want 1 (limit=1)", len(resp.Fields))
	}
	// TotalFields should still reflect the unfiltered count.
	if resp.TotalFields != 3 {
		t.Errorf("TotalFields = %d, want 3 (total regardless of limit)", resp.TotalFields)
	}
}

func TestAPIOverviewCompressionRatio(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doGet(t, api, "/lakehouse/api/v1/stats/overview")

	var resp OverviewResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Raw = 100 GiB + 20 GiB = 120 GiB, compressed = 50 GiB + 10 GiB = 60 GiB
	// Ratio = 120/60 = 2.0
	if resp.AvgCompressionRatio < 1.0 {
		t.Errorf("AvgCompressionRatio = %f, want >= 1.0 (raw > compressed)", resp.AvgCompressionRatio)
	}
	if resp.AvgCompressionRatio == 0 {
		t.Error("AvgCompressionRatio should not be 0")
	}
}
