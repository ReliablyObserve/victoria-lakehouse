package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// ---------------------------------------------------------------------------
// Test 1: Full round-trip from registry writes through API endpoints
// ---------------------------------------------------------------------------

func TestIntegration_RegistryToAPI(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("acct1:proj1", 1024, 2048, 100, "STANDARD")
	reg.RecordWrite("acct2:proj2", 2048, 4096, 200, "STANDARD")
	reg.RecordWrite("acct3:proj3", 512, 1024, 50, "GLACIER")
	reg.RecordQuery("acct1:proj1")

	m := manifest.New("test-bucket", "data/")
	li := cache.NewLabelIndex()
	li.Add("hostname", []string{"h1", "h2"})

	costCalc := NewCostCalculator(map[string]float64{
		"STANDARD": 0.023,
		"GLACIER":  0.0036,
	}, nil)
	classTracker := NewStorageClassTracker(nil, nil)

	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     m,
		CostCalc:     costCalc,
		ClassTracker: classTracker,
		LabelIndex:   li,
		Mode:         "logs",
		Bucket:       "test-bucket",
	})

	mux := http.NewServeMux()
	api.Register(mux)

	// --- Tenants list ---
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("tenants list: status %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var tenantsResp TenantsResponse
	if err := json.NewDecoder(rec.Body).Decode(&tenantsResp); err != nil {
		t.Fatalf("decode tenants response: %v", err)
	}
	if len(tenantsResp.Tenants) != 3 {
		t.Fatalf("expected 3 tenants, got %d", len(tenantsResp.Tenants))
	}
	if tenantsResp.TotalTenants != 3 {
		t.Errorf("TotalTenants = %d, want 3", tenantsResp.TotalTenants)
	}

	// --- Single tenant detail ---
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/acct1/proj1", nil)
	mux.ServeHTTP(rec2, req2)

	if rec2.Code != 200 {
		t.Fatalf("tenant detail: status %d, want 200; body: %s", rec2.Code, rec2.Body.String())
	}
	var detail TenantEntry
	if err := json.NewDecoder(rec2.Body).Decode(&detail); err != nil {
		t.Fatalf("decode tenant detail: %v", err)
	}
	if detail.AccountID != "acct1" {
		t.Errorf("detail AccountID = %q, want %q", detail.AccountID, "acct1")
	}
	if detail.ProjectID != "proj1" {
		t.Errorf("detail ProjectID = %q, want %q", detail.ProjectID, "proj1")
	}
	if detail.TotalBytes != 1024 {
		t.Errorf("detail TotalBytes = %d, want 1024", detail.TotalBytes)
	}
	if detail.TotalRows != 100 {
		t.Errorf("detail TotalRows = %d, want 100", detail.TotalRows)
	}
	if detail.LastQueryAt == "" {
		t.Error("detail LastQueryAt should be set after RecordQuery")
	}

	// --- Overview ---
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/overview", nil)
	mux.ServeHTTP(rec3, req3)

	if rec3.Code != 200 {
		t.Fatalf("overview: status %d, want 200; body: %s", rec3.Code, rec3.Body.String())
	}
	var overview OverviewResponse
	if err := json.NewDecoder(rec3.Body).Decode(&overview); err != nil {
		t.Fatalf("decode overview: %v", err)
	}
	if overview.TenantCount != 3 {
		t.Errorf("overview TenantCount = %d, want 3", overview.TenantCount)
	}
	if overview.Mode != "logs" {
		t.Errorf("overview Mode = %q, want %q", overview.Mode, "logs")
	}
	if overview.Bucket != "test-bucket" {
		t.Errorf("overview Bucket = %q, want %q", overview.Bucket, "test-bucket")
	}
	// Registry total: 1024+2048+512 = 3584
	if overview.TotalBytes != 3584 {
		t.Errorf("overview TotalBytes = %d, want 3584", overview.TotalBytes)
	}

	// --- Cost ---
	rec4 := httptest.NewRecorder()
	req4 := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/cost", nil)
	mux.ServeHTTP(rec4, req4)

	if rec4.Code != 200 {
		t.Fatalf("cost: status %d, want 200; body: %s", rec4.Code, rec4.Body.String())
	}
	var costResp CostResponse
	if err := json.NewDecoder(rec4.Body).Decode(&costResp); err != nil {
		t.Fatalf("decode cost: %v", err)
	}
	if len(costResp.PerTenant) != 3 {
		t.Errorf("cost PerTenant count = %d, want 3", len(costResp.PerTenant))
	}

	// --- Compression ---
	rec5 := httptest.NewRecorder()
	req5 := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/compression", nil)
	mux.ServeHTTP(rec5, req5)

	if rec5.Code != 200 {
		t.Fatalf("compression: status %d, want 200; body: %s", rec5.Code, rec5.Body.String())
	}
	var compResp CompressionResponse
	if err := json.NewDecoder(rec5.Body).Decode(&compResp); err != nil {
		t.Fatalf("decode compression: %v", err)
	}
	if len(compResp.PerTenant) != 3 {
		t.Errorf("compression PerTenant count = %d, want 3", len(compResp.PerTenant))
	}

	// --- Ingestion ---
	rec6 := httptest.NewRecorder()
	req6 := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/ingestion", nil)
	mux.ServeHTTP(rec6, req6)

	if rec6.Code != 200 {
		t.Fatalf("ingestion: status %d, want 200; body: %s", rec6.Code, rec6.Body.String())
	}

	// --- Cardinality ---
	rec7 := httptest.NewRecorder()
	req7 := httptest.NewRequest("GET", "/lakehouse/api/v1/cardinality/fields", nil)
	mux.ServeHTTP(rec7, req7)

	if rec7.Code != 200 {
		t.Fatalf("cardinality: status %d, want 200; body: %s", rec7.Code, rec7.Body.String())
	}
	var cardResp CardinalityResponse
	if err := json.NewDecoder(rec7.Body).Decode(&cardResp); err != nil {
		t.Fatalf("decode cardinality: %v", err)
	}
	if cardResp.TotalFields != 1 {
		t.Errorf("cardinality TotalFields = %d, want 1", cardResp.TotalFields)
	}
}

// ---------------------------------------------------------------------------
// Test 2: CRDT convergence across 3 nodes via HTTP sync layer
// ---------------------------------------------------------------------------

func TestIntegration_CRDTConvergence(t *testing.T) {
	// Create 3 registries simulating 3 nodes.
	reg1 := NewTenantRegistry("node-1")
	reg2 := NewTenantRegistry("node-2")
	reg3 := NewTenantRegistry("node-3")

	// Each node records unique data.
	reg1.RecordWrite("t1:p1", 1000, 2000, 10, "STANDARD")
	reg2.RecordWrite("t1:p1", 2000, 4000, 20, "STANDARD")
	reg3.RecordWrite("t2:p2", 500, 1000, 5, "STANDARD")

	// Create sync handlers for each node.
	h1 := NewSyncHandler(reg1, "")
	h2 := NewSyncHandler(reg2, "")
	h3 := NewSyncHandler(reg3, "")

	// Create test servers.
	srv1 := httptest.NewServer(h1)
	srv2 := httptest.NewServer(h2)
	srv3 := httptest.NewServer(h3)
	defer srv1.Close()
	defer srv2.Close()
	defer srv3.Close()

	// Push reg1's full state to reg2 and reg3.
	pusher1 := NewSyncPusher(SyncPusherConfig{
		Registry: reg1,
		GetPeers: func() []string { return []string{srv2.URL, srv3.URL} },
		SelfAddr: srv1.URL,
		Compress: true,
	})
	if err := pusher1.PushFull(context.Background()); err != nil {
		t.Fatalf("push from node-1: %v", err)
	}

	// Push reg2's full state to reg1 and reg3.
	pusher2 := NewSyncPusher(SyncPusherConfig{
		Registry: reg2,
		GetPeers: func() []string { return []string{srv1.URL, srv3.URL} },
		SelfAddr: srv2.URL,
		Compress: true,
	})
	if err := pusher2.PushFull(context.Background()); err != nil {
		t.Fatalf("push from node-2: %v", err)
	}

	// Push reg3's full state to reg1 and reg2.
	pusher3 := NewSyncPusher(SyncPusherConfig{
		Registry: reg3,
		GetPeers: func() []string { return []string{srv1.URL, srv2.URL} },
		SelfAddr: srv3.URL,
		Compress: true,
	})
	if err := pusher3.PushFull(context.Background()); err != nil {
		t.Fatalf("push from node-3: %v", err)
	}

	// Verify convergence: all 3 should have t1:p1 with 3000 bytes and t2:p2 with 500 bytes.
	for name, reg := range map[string]*TenantRegistry{"reg1": reg1, "reg2": reg2, "reg3": reg3} {
		t1 := reg.Get("t1:p1")
		if t1 == nil {
			t.Fatalf("%s missing t1:p1", name)
		}
		if t1.TotalBytes != 3000 {
			t.Errorf("%s t1:p1 TotalBytes = %d, want 3000", name, t1.TotalBytes)
		}
		if t1.TotalRows != 30 {
			t.Errorf("%s t1:p1 TotalRows = %d, want 30", name, t1.TotalRows)
		}

		t2 := reg.Get("t2:p2")
		if t2 == nil {
			t.Fatalf("%s missing t2:p2", name)
		}
		if t2.TotalBytes != 500 {
			t.Errorf("%s t2:p2 TotalBytes = %d, want 500", name, t2.TotalBytes)
		}
	}

	// Verify tenant counts converged.
	for name, reg := range map[string]*TenantRegistry{"reg1": reg1, "reg2": reg2, "reg3": reg3} {
		if reg.TenantCount() != 2 {
			t.Errorf("%s TenantCount = %d, want 2", name, reg.TenantCount())
		}
	}
}

// ---------------------------------------------------------------------------
// Test 3: Snapshot marshal/load round-trip preserves all data
// ---------------------------------------------------------------------------

func TestIntegration_SnapshotRoundTrip(t *testing.T) {
	reg1 := NewTenantRegistry("node-1")
	reg1.RecordWrite("acct:proj", 1024, 2048, 100, "STANDARD")
	reg1.RecordWrite("acct:proj", 512, 1024, 50, "GLACIER")
	reg1.RecordQuery("acct:proj")

	data, err := reg1.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	reg2 := NewTenantRegistry("node-2")
	if err := reg2.LoadSnapshot("node-1", data); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	ts := reg2.Get("acct:proj")
	if ts == nil {
		t.Fatal("missing tenant after snapshot load")
	}

	// TotalBytes is CRDT sum of per-node bytes: node-1 wrote 1024 + 512 = 1536.
	if ts.TotalBytes != 1536 {
		t.Errorf("TotalBytes = %d, want 1536", ts.TotalBytes)
	}
	if ts.TotalRows != 150 {
		t.Errorf("TotalRows = %d, want 150", ts.TotalRows)
	}
	if ts.TotalFiles != 2 {
		t.Errorf("TotalFiles = %d, want 2", ts.TotalFiles)
	}

	// Verify storage class breakdown preserved.
	if ts.BytesByClass["STANDARD"] != 1024 {
		t.Errorf("STANDARD bytes = %d, want 1024", ts.BytesByClass["STANDARD"])
	}
	if ts.BytesByClass["GLACIER"] != 512 {
		t.Errorf("GLACIER bytes = %d, want 512", ts.BytesByClass["GLACIER"])
	}

	// Verify query timestamp was preserved.
	if ts.LastQueryAt.IsZero() {
		t.Error("LastQueryAt should be non-zero after snapshot load")
	}
}

// ---------------------------------------------------------------------------
// Test 4: StorageClassTracker + CostCalculator working together
// ---------------------------------------------------------------------------

func TestIntegration_StorageClassCost(t *testing.T) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
	}
	tracker := NewStorageClassTracker(rules, nil)
	costCalc := NewCostCalculator(
		map[string]float64{
			"STANDARD":    0.023,
			"STANDARD_IA": 0.0125,
			"GLACIER":     0.0036,
		},
		nil,
	)

	now := time.Now()

	// Recent file (10 days old): STANDARD
	recentClass := tracker.PredictClass(now.Add(-10*24*time.Hour), now)
	if recentClass != "STANDARD" {
		t.Errorf("10-day-old file: class = %q, want STANDARD", recentClass)
	}

	// 45 days old: STANDARD_IA
	iaClass := tracker.PredictClass(now.Add(-45*24*time.Hour), now)
	if iaClass != "STANDARD_IA" {
		t.Errorf("45-day-old file: class = %q, want STANDARD_IA", iaClass)
	}

	// 120 days old: GLACIER
	glacierClass := tracker.PredictClass(now.Add(-120*24*time.Hour), now)
	if glacierClass != "GLACIER" {
		t.Errorf("120-day-old file: class = %q, want GLACIER", glacierClass)
	}

	// Compute costs for 1 GiB in each class.
	oneGiB := int64(1 << 30)

	standardCost := costCalc.MonthlyStorageCost("STANDARD", oneGiB)
	if standardCost < 0.022 || standardCost > 0.024 {
		t.Errorf("STANDARD 1GiB cost = %f, want ~0.023", standardCost)
	}

	iaCost := costCalc.MonthlyStorageCost("STANDARD_IA", oneGiB)
	if iaCost < 0.012 || iaCost > 0.013 {
		t.Errorf("STANDARD_IA 1GiB cost = %f, want ~0.0125", iaCost)
	}

	glacierCost := costCalc.MonthlyStorageCost("GLACIER", oneGiB)
	if glacierCost < 0.003 || glacierCost > 0.004 {
		t.Errorf("GLACIER 1GiB cost = %f, want ~0.0036", glacierCost)
	}

	// Verify tiered storage saves money vs all-STANDARD.
	byClass := map[string]int64{
		"STANDARD":    oneGiB,
		"STANDARD_IA": oneGiB,
		"GLACIER":     oneGiB,
	}
	savings := costCalc.LifecycleSavings(byClass)
	if savings <= 0 {
		t.Errorf("lifecycle savings = %f, expected positive", savings)
	}
}

// ---------------------------------------------------------------------------
// Test 5: CardinalityLimiter integration with API
// ---------------------------------------------------------------------------

func TestIntegration_CardinalityLimiterWithAPI(t *testing.T) {
	// Create a limiter that caps at 5 tenants.
	limiter := NewCardinalityLimiter(5)

	// Register 10 tenants through the limiter.
	for i := 0; i < 10; i++ {
		tenant := "acct" + string(rune('A'+i)) + ":proj"
		limiter.Allow(tenant)
	}

	// Only 5 should be tracked.
	if limiter.TrackedCount() != 5 {
		t.Errorf("TrackedCount = %d, want 5", limiter.TrackedCount())
	}
	// 5 should have been rejected.
	if limiter.OverflowCount() != 5 {
		t.Errorf("OverflowCount = %d, want 5", limiter.OverflowCount())
	}

	// Now verify the API still serves all tenants regardless of limiter
	// (the limiter controls metrics cardinality, not API visibility).
	reg := NewTenantRegistry("node-1")
	for i := 0; i < 10; i++ {
		tenant := "acct" + string(rune('A'+i)) + ":proj"
		reg.RecordWrite(tenant, 100, 200, 10, "STANDARD")
	}

	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     manifest.New("test-bucket", ""),
		CostCalc:     NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil),
		ClassTracker: NewStorageClassTracker(nil, nil),
		LabelIndex:   cache.NewLabelIndex(),
		Mode:         "logs",
		Bucket:       "test-bucket",
	})

	mux := http.NewServeMux()
	api.Register(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status %d, want 200", rec.Code)
	}

	var resp TenantsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalTenants != 10 {
		t.Errorf("TotalTenants = %d, want 10 (limiter should not affect API)", resp.TotalTenants)
	}

	// Reset the limiter and verify it accepts new tenants.
	limiter.Reset()
	if limiter.TrackedCount() != 0 {
		t.Errorf("TrackedCount after Reset = %d, want 0", limiter.TrackedCount())
	}
	// Overflow counter is cumulative.
	if limiter.OverflowCount() != 5 {
		t.Errorf("OverflowCount after Reset = %d, want 5 (cumulative)", limiter.OverflowCount())
	}
}

// ---------------------------------------------------------------------------
// Test 6: Sync with auth key — unauthorized rejected, authorized succeeds
// ---------------------------------------------------------------------------

func TestIntegration_SyncWithAuth(t *testing.T) {
	reg := NewTenantRegistry("node-recv")
	authKey := "supersecretkey"
	handler := NewSyncHandler(reg, authKey)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	src := NewTenantRegistry("node-src")
	src.RecordWrite("auth:tenant", 256, 512, 5, "STANDARD")

	// --- Attempt without auth key: should fail ---
	pusherNoAuth := NewSyncPusher(SyncPusherConfig{
		Registry: src,
		GetPeers: func() []string { return []string{srv.URL} },
		SelfAddr: "http://self:9090",
		Compress: false,
	})
	err := pusherNoAuth.PushFull(context.Background())
	if err == nil {
		t.Fatal("expected error pushing without auth, got nil")
	}

	// Verify tenant was NOT merged.
	if reg.Get("auth:tenant") != nil {
		t.Error("tenant should not be present after unauthorized push")
	}

	// --- Attempt with correct auth key: should succeed ---
	pusherAuth := NewSyncPusher(SyncPusherConfig{
		Registry: src,
		GetPeers: func() []string { return []string{srv.URL} },
		SelfAddr: "http://self:9090",
		AuthKey:  authKey,
		Compress: false,
	})
	if err := pusherAuth.PushFull(context.Background()); err != nil {
		t.Fatalf("authorized push failed: %v", err)
	}

	// Verify tenant WAS merged.
	ts := reg.Get("auth:tenant")
	if ts == nil {
		t.Fatal("tenant should be present after authorized push")
	}
	if ts.TotalBytes != 256 {
		t.Errorf("TotalBytes = %d, want 256", ts.TotalBytes)
	}
}

// ---------------------------------------------------------------------------
// Test 7: ZSTD compression round-trip through HTTP sync
// ---------------------------------------------------------------------------

func TestIntegration_SyncWithZSTD(t *testing.T) {
	reg1 := NewTenantRegistry("node-1")
	reg2 := NewTenantRegistry("node-2")

	// Record data on node-1 with multiple tenants.
	reg1.RecordWrite("z:t1", 1000, 2000, 10, "STANDARD")
	reg1.RecordWrite("z:t2", 2000, 4000, 20, "GLACIER")
	reg1.RecordWrite("z:t3", 3000, 6000, 30, "STANDARD_IA")

	handler := NewSyncHandler(reg2, "")
	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Push with compression enabled.
	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg1,
		GetPeers: func() []string { return []string{srv.URL} },
		SelfAddr: "http://self:9090",
		Compress: true,
	})
	if err := pusher.PushFull(context.Background()); err != nil {
		t.Fatalf("PushFull with ZSTD: %v", err)
	}

	// Verify all 3 tenants arrived at reg2.
	if reg2.TenantCount() != 3 {
		t.Fatalf("TenantCount = %d, want 3", reg2.TenantCount())
	}

	for _, tc := range []struct {
		key       string
		wantBytes int64
		wantRows  int64
	}{
		{"z:t1", 1000, 10},
		{"z:t2", 2000, 20},
		{"z:t3", 3000, 30},
	} {
		ts := reg2.Get(tc.key)
		if ts == nil {
			t.Fatalf("missing tenant %s after ZSTD sync", tc.key)
		}
		if ts.TotalBytes != tc.wantBytes {
			t.Errorf("%s TotalBytes = %d, want %d", tc.key, ts.TotalBytes, tc.wantBytes)
		}
		if ts.TotalRows != tc.wantRows {
			t.Errorf("%s TotalRows = %d, want %d", tc.key, ts.TotalRows, tc.wantRows)
		}
	}

	// Verify the data round-trips correctly: compress snapshot, decompress, load.
	snap, err := reg2.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	compressed := compressZSTD(snap)
	decompressed, err := decompressZSTD(compressed)
	if err != nil {
		t.Fatalf("decompressZSTD: %v", err)
	}
	if !bytes.Equal(snap, decompressed) {
		t.Error("ZSTD round-trip mismatch on snapshot data")
	}
}

// ---------------------------------------------------------------------------
// Test 8: All 7 API endpoints return 200 and valid JSON
// ---------------------------------------------------------------------------

func TestIntegration_AllAPIEndpoints(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("acme:web", 50<<20, 100<<20, 500000, "STANDARD")
	reg.RecordWrite("acme:api", 30<<20, 60<<20, 300000, "STANDARD_IA")
	reg.RecordWrite("beta:svc", 10<<20, 20<<20, 100000, "GLACIER")
	reg.RecordQuery("acme:web")

	m := manifest.New("prod-bucket", "data/")
	m.AddFile("dt=2026-05-10/hour=10", manifest.FileInfo{
		Key:      "data/dt=2026-05-10/hour=10/part-001.parquet",
		Size:     1024 * 1024,
		RowCount: 50000,
		RawBytes: 2048 * 1024,
	})

	li := cache.NewLabelIndex()
	li.AddWithTenant("hostname", []string{"h1", "h2", "h3"}, "acme:web")
	li.AddWithTenant("service", []string{"web", "api"}, "acme:api")

	api := NewAPI(APIConfig{
		Registry: reg,
		Manifest: m,
		CostCalc: NewCostCalculator(map[string]float64{
			"STANDARD":    0.023,
			"STANDARD_IA": 0.0125,
			"GLACIER":     0.0036,
		}, nil),
		ClassTracker: NewStorageClassTracker(nil, nil),
		LabelIndex:   li,
		Mode:         "logs",
		Bucket:       "prod-bucket",
	})

	mux := http.NewServeMux()
	api.Register(mux)

	endpoints := []struct {
		path string
		name string
	}{
		{"/lakehouse/api/v1/tenants", "tenants"},
		{"/lakehouse/api/v1/tenants/acme/web", "tenant-detail"},
		{"/lakehouse/api/v1/stats/overview", "overview"},
		{"/lakehouse/api/v1/stats/ingestion", "ingestion"},
		{"/lakehouse/api/v1/stats/cost", "cost"},
		{"/lakehouse/api/v1/stats/compression", "compression"},
		{"/lakehouse/api/v1/cardinality/fields", "cardinality"},
	}

	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", ep.path, nil)
			mux.ServeHTTP(rec, req)

			if rec.Code != 200 {
				t.Fatalf("%s: status %d, want 200; body: %s", ep.name, rec.Code, rec.Body.String())
			}

			// Verify Content-Type is JSON.
			ct := rec.Header().Get("Content-Type")
			if ct != "application/json" {
				t.Errorf("%s: Content-Type = %q, want application/json", ep.name, ct)
			}

			// Verify body is valid JSON.
			body, _ := io.ReadAll(rec.Body)
			var raw json.RawMessage
			if err := json.Unmarshal(body, &raw); err != nil {
				t.Errorf("%s: invalid JSON: %v\nbody: %s", ep.name, err, string(body))
			}
		})
	}

	// Verify specific data in key endpoints.
	t.Run("tenants-data", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil)
		mux.ServeHTTP(rec, req)

		var resp TenantsResponse
		json.NewDecoder(rec.Body).Decode(&resp)
		if resp.TotalTenants != 3 {
			t.Errorf("TotalTenants = %d, want 3", resp.TotalTenants)
		}
		if resp.TotalBytes <= 0 {
			t.Error("TotalBytes should be positive")
		}
	})

	t.Run("overview-data", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/overview", nil)
		mux.ServeHTTP(rec, req)

		var resp OverviewResponse
		json.NewDecoder(rec.Body).Decode(&resp)
		if resp.Bucket != "prod-bucket" {
			t.Errorf("Bucket = %q, want prod-bucket", resp.Bucket)
		}
		if resp.FleetNodes != 1 {
			t.Errorf("FleetNodes = %d, want 1", resp.FleetNodes)
		}
		if resp.RegistryGeneration == 0 {
			t.Error("RegistryGeneration should be non-zero after writes")
		}
	})

	t.Run("cost-data", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/lakehouse/api/v1/stats/cost", nil)
		mux.ServeHTTP(rec, req)

		var resp CostResponse
		json.NewDecoder(rec.Body).Decode(&resp)
		if resp.TotalMonthlyUSD <= 0 {
			t.Error("TotalMonthlyUSD should be positive")
		}
		if len(resp.ByClass) == 0 {
			t.Error("ByClass should not be empty")
		}
		if len(resp.PerTenant) != 3 {
			t.Errorf("PerTenant count = %d, want 3", len(resp.PerTenant))
		}
	})

	t.Run("cardinality-data", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/lakehouse/api/v1/cardinality/fields", nil)
		mux.ServeHTTP(rec, req)

		var resp CardinalityResponse
		json.NewDecoder(rec.Body).Decode(&resp)
		if resp.TotalFields != 2 {
			t.Errorf("TotalFields = %d, want 2", resp.TotalFields)
		}
	})
}
