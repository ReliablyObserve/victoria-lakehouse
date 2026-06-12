package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

func TestMax64(t *testing.T) {
	tests := []struct {
		a, b int64
		want int64
	}{
		{5, 3, 5},
		{3, 5, 5},
		{7, 7, 7},
		{0, -1, 0},
		{-1, 0, 0},
	}
	for _, tt := range tests {
		if got := max64(tt.a, tt.b); got != tt.want {
			t.Errorf("max64(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestPadHour(t *testing.T) {
	tests := []struct {
		h    int
		want string
	}{
		{0, "00"},
		{9, "09"},
		{10, "10"},
		{23, "23"},
	}
	for _, tt := range tests {
		if got := padHour(tt.h); got != tt.want {
			t.Errorf("padHour(%d) = %q, want %q", tt.h, got, tt.want)
		}
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

// TestAPIInstancesEndpoint verifies /stats/instances emits a self row sourced
// from the registry's gossiped NodeMeta, marks is_self, and carries the
// cluster-wide MetaS3Bytes on the row.
func TestAPIInstancesEndpoint(t *testing.T) {
	reg := NewTenantRegistry("node-self")
	reg.SetNodeMeta(4096, 8192)

	api := NewAPI(APIConfig{
		Registry:    reg,
		Mode:        "logs",
		Bucket:      "test-bucket",
		MetaS3Bytes: func() int64 { return 123456 },
	})

	rec := doGet(t, api, "/lakehouse/api/v1/stats/instances")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp InstancesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Instances) != 1 {
		t.Fatalf("instances = %d, want 1", len(resp.Instances))
	}
	self := resp.Instances[0]
	if self.NodeID != "node-self" {
		t.Errorf("node_id = %q, want node-self", self.NodeID)
	}
	if !self.IsSelf {
		t.Error("is_self = false, want true for the local node")
	}
	if self.MetaResidentBytes != 4096 || self.MetaDiskBytes != 8192 {
		t.Errorf("resident=%d disk=%d, want 4096/8192", self.MetaResidentBytes, self.MetaDiskBytes)
	}
	if self.MetaS3Bytes != 123456 {
		t.Errorf("meta_s3_bytes = %d, want 123456 (cluster-wide)", self.MetaS3Bytes)
	}
}

// TestAPIInstancesEndpointSyntheticSelf verifies that when no SetNodeMeta tick
// has landed, the self row is still emitted, sourced from the local
// MetaResidentBytes/MetaDiskBytes funcs.
func TestAPIInstancesEndpointSyntheticSelf(t *testing.T) {
	reg := NewTenantRegistry("node-solo")

	api := NewAPI(APIConfig{
		Registry:          reg,
		Mode:              "logs",
		Bucket:            "test-bucket",
		MetaResidentBytes: func() int64 { return 555 },
		MetaDiskBytes:     func() int64 { return 666 },
		MetaS3Bytes:       func() int64 { return 777 },
	})

	rec := doGet(t, api, "/lakehouse/api/v1/stats/instances")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp InstancesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Instances) != 1 {
		t.Fatalf("instances = %d, want 1 (synthesised self)", len(resp.Instances))
	}
	self := resp.Instances[0]
	if self.NodeID != "node-solo" || !self.IsSelf {
		t.Errorf("self row = %+v, want node-solo is_self=true", self)
	}
	if self.MetaResidentBytes != 555 || self.MetaDiskBytes != 666 || self.MetaS3Bytes != 777 {
		t.Errorf("synthesised self = %+v, want resident=555 disk=666 s3=777", self)
	}
}

// TestNodeMetaTTLAgesOutStaleNodes (BUG 2) verifies that once a TTL is set,
// NodeMetaAll drops a peer whose last gossip is older than the window — the
// dead-node-from-snapshot case — while keeping a freshly-gossiped peer and
// always keeping self (even if self's own stamp is artificially old).
func TestNodeMetaTTLAgesOutStaleNodes(t *testing.T) {
	reg := NewTenantRegistry("self")
	reg.SetNodeMetaTTL(90 * time.Second)

	now := time.Now()

	// A fresh peer (gossiped just now) and a dead peer (last seen 10m ago, e.g.
	// loaded from a stale shared S3 snapshot). Inject directly with explicit
	// LastUpdated so the test is deterministic.
	reg.mu.Lock()
	reg.nodeMeta["live-peer"] = NodeMeta{ResidentBytes: 100, DiskBytes: 200, Gen: 1, LastUpdated: now}
	reg.nodeMeta["dead-peer"] = NodeMeta{ResidentBytes: 999, DiskBytes: 999, Gen: 1, LastUpdated: now.Add(-10 * time.Minute)}
	// Self with an OLD stamp — must still be returned (self is authoritative).
	reg.nodeMeta["self"] = NodeMeta{ResidentBytes: 50, DiskBytes: 60, Gen: 1, LastUpdated: now.Add(-10 * time.Minute)}
	reg.mu.Unlock()

	all := reg.NodeMetaAll()
	if _, ok := all["dead-peer"]; ok {
		t.Error("dead-peer should have aged out of NodeMetaAll past the TTL")
	}
	if _, ok := all["live-peer"]; !ok {
		t.Error("live-peer (fresh gossip) must remain in NodeMetaAll")
	}
	if _, ok := all["self"]; !ok {
		t.Error("self must ALWAYS be returned regardless of TTL")
	}
	if len(all) != 2 {
		t.Errorf("NodeMetaAll = %d entries, want 2 (self + live-peer)", len(all))
	}

	// With the TTL disabled (0), the dead peer is visible again — proving the
	// filter is what excludes it (and preserving the legacy no-TTL behaviour).
	reg.SetNodeMetaTTL(0)
	if len(reg.NodeMetaAll()) != 3 {
		t.Errorf("TTL=0 NodeMetaAll = %d, want 3 (no filtering)", len(reg.NodeMetaAll()))
	}
}

// TestAPIOverviewClusterWideMetaFootprint (REQUEST 3) verifies /stats/overview
// reports meta_resident_bytes / meta_disk_bytes as the SUM across all live
// instances' gossiped NodeMeta, and falls back to the local funcs when the
// registry has no gossiped entries.
func TestAPIOverviewClusterWideMetaFootprint(t *testing.T) {
	reg := NewTenantRegistry("node-A")
	// Self + two gossiped peers.
	reg.SetNodeMeta(1000, 2000) // self (node-A)
	reg.Merge(&TenantDelta{NodeID: "node-B", NodeMeta: &NodeMeta{ResidentBytes: 3000, DiskBytes: 4000, Gen: 1}})
	reg.Merge(&TenantDelta{NodeID: "node-C", NodeMeta: &NodeMeta{ResidentBytes: 500, DiskBytes: 700, Gen: 1}})

	api := NewAPI(APIConfig{
		Registry: reg,
		Mode:     "logs",
		Bucket:   "test-bucket",
		// Local funcs return THIS node's values; the cluster sum must NOT equal
		// these — it must be the fleet total.
		MetaResidentBytes: func() int64 { return 1000 },
		MetaDiskBytes:     func() int64 { return 2000 },
		MetaS3Bytes:       func() int64 { return 9 },
	})

	var resp OverviewResponse
	rec := doGet(t, api, "/lakehouse/api/v1/stats/overview")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 1000 + 3000 + 500 = 4500 ; 2000 + 4000 + 700 = 6700.
	if resp.MetaResidentBytes != 4500 {
		t.Errorf("meta_resident_bytes = %d, want 4500 (cluster sum, not single node 1000)", resp.MetaResidentBytes)
	}
	if resp.MetaDiskBytes != 6700 {
		t.Errorf("meta_disk_bytes = %d, want 6700 (cluster sum)", resp.MetaDiskBytes)
	}

	// Fallback: a registry with NO node-meta recorded falls back to local funcs.
	regEmpty := NewTenantRegistry("node-solo")
	apiFallback := NewAPI(APIConfig{
		Registry:          regEmpty,
		Mode:              "logs",
		Bucket:            "test-bucket",
		MetaResidentBytes: func() int64 { return 111 },
		MetaDiskBytes:     func() int64 { return 222 },
	})
	var resp2 OverviewResponse
	rec2 := doGet(t, apiFallback, "/lakehouse/api/v1/stats/overview")
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("decode fallback: %v", err)
	}
	if resp2.MetaResidentBytes != 111 || resp2.MetaDiskBytes != 222 {
		t.Errorf("fallback footprint = resident:%d disk:%d, want 111/222 (local funcs)", resp2.MetaResidentBytes, resp2.MetaDiskBytes)
	}
}
