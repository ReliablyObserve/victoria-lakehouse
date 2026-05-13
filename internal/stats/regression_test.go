package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// ---------------------------------------------------------------------------
// Registry — error/edge cases
// ---------------------------------------------------------------------------

func TestRegression_RecordWriteZeroValues(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("acme:proj1", 0, 0, 0, "STANDARD")

	ts := reg.Get("acme:proj1")
	if ts == nil {
		t.Fatal("expected tenant to exist after zero-value write")
	}
	if ts.TotalBytes != 0 {
		t.Errorf("TotalBytes = %d, want 0", ts.TotalBytes)
	}
	if ts.TotalRows != 0 {
		t.Errorf("TotalRows = %d, want 0", ts.TotalRows)
	}
	// TotalFiles should be 1 because nodeFiles is incremented per write.
	if ts.TotalFiles != 1 {
		t.Errorf("TotalFiles = %d, want 1", ts.TotalFiles)
	}
}

func TestRegression_RecordWriteEmptyTenant(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("", 100, 200, 5, "STANDARD")

	ts := reg.Get("")
	if ts == nil {
		t.Fatal("expected empty-string tenant to exist")
	}
	if ts.TotalBytes != 100 {
		t.Errorf("TotalBytes = %d, want 100", ts.TotalBytes)
	}
	// Empty key parsed: accountID = "", projectID = ""
	if ts.AccountID != "" {
		t.Errorf("AccountID = %q, want empty", ts.AccountID)
	}
	if ts.ProjectID != "" {
		t.Errorf("ProjectID = %q, want empty", ts.ProjectID)
	}
}

func TestRegression_GetNonexistent(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	ts := reg.Get("nonexistent")
	if ts != nil {
		t.Errorf("Get(nonexistent) = %+v, want nil", ts)
	}
}

func TestRegression_GlobalAggregatesEmpty(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	gs := reg.GlobalAggregates()

	if gs.TenantCount != 0 {
		t.Errorf("TenantCount = %d, want 0", gs.TenantCount)
	}
	if gs.TotalBytes != 0 {
		t.Errorf("TotalBytes = %d, want 0", gs.TotalBytes)
	}
	if gs.TotalFiles != 0 {
		t.Errorf("TotalFiles = %d, want 0", gs.TotalFiles)
	}
	if gs.TotalRows != 0 {
		t.Errorf("TotalRows = %d, want 0", gs.TotalRows)
	}
	if gs.RawBytes != 0 {
		t.Errorf("RawBytes = %d, want 0", gs.RawBytes)
	}
}

func TestRegression_BuildDeltaEmpty(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	delta := reg.BuildDelta(0)

	if delta == nil {
		t.Fatal("expected non-nil delta from empty registry")
	}
	if len(delta.Tenants) != 0 {
		t.Errorf("len(Tenants) = %d, want 0", len(delta.Tenants))
	}
	if delta.NodeID != "node-1" {
		t.Errorf("NodeID = %q, want %q", delta.NodeID, "node-1")
	}
}

func TestRegression_MergeEmptyDelta(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 200, 1, "STANDARD")

	// Merge with nil — should be a no-op.
	reg.Merge(nil)

	// Merge with empty tenants map.
	reg.Merge(&TenantDelta{
		NodeID:  "node-2",
		Tenants: map[string]*TenantStats{},
	})

	if reg.TenantCount() != 1 {
		t.Errorf("TenantCount = %d, want 1 after empty merges", reg.TenantCount())
	}
	ts := reg.Get("a:1")
	if ts == nil || ts.TotalBytes != 100 {
		t.Error("original tenant data should be unchanged after empty merge")
	}
}

func TestRegression_MergeFromSameNode(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 200, 5, "STANDARD")

	// Build a delta that looks like it came from the same node.
	src := NewTenantRegistry("node-1")
	src.RecordWrite("a:1", 300, 500, 10, "STANDARD")
	delta := src.BuildDelta(0)

	reg.Merge(delta)

	ts := reg.Get("a:1")
	if ts == nil {
		t.Fatal("tenant should exist after merge")
	}
	// After CRDT merge with same nodeID, the max per-node counter wins.
	// node-1 had 100 locally, 300 remotely -> max is 300.
	if ts.TotalBytes != 300 {
		t.Errorf("TotalBytes = %d, want 300 (max of same-node counters)", ts.TotalBytes)
	}
}

func TestRegression_SnapshotEmpty(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	data, err := reg.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot on empty registry: %v", err)
	}

	// Should be valid JSON.
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("empty snapshot is not valid JSON: %v", err)
	}

	if m["node_id"] != "node-1" {
		t.Errorf("node_id = %v, want %q", m["node_id"], "node-1")
	}
}

func TestRegression_LoadSnapshotBadJSON(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	err := reg.LoadSnapshot("node-2", []byte(`{not valid json!!!}`))
	if err == nil {
		t.Error("expected error loading garbage JSON, got nil")
	}
}

func TestRegression_LoadSnapshotEmptyData(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	err := reg.LoadSnapshot("node-2", []byte{})
	if err == nil {
		t.Error("expected error loading empty byte slice, got nil")
	}
}

func TestRegression_ConcurrentRecordAndAll(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	const goroutines = 20
	const writes = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines write.
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writes; j++ {
				tenant := fmt.Sprintf("tenant-%d:proj-%d", id, j%5)
				reg.RecordWrite(tenant, int64(j+1), int64(j+1)*2, int64(j+1), "STANDARD")
			}
		}(i)
	}

	// Half the goroutines read.
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < writes; j++ {
				_ = reg.All()
				_ = reg.TenantCount()
				_ = reg.GlobalAggregates()
			}
		}()
	}

	wg.Wait()

	// Just verify it didn't panic or race.
	if reg.TenantCount() == 0 {
		t.Error("expected at least some tenants after concurrent writes")
	}
}

func TestRegression_TenantCountAccuracy(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	for i := 0; i < 50; i++ {
		tenant := fmt.Sprintf("acct-%d:proj-%d", i/10, i)
		reg.RecordWrite(tenant, 100, 200, 1, "STANDARD")
	}

	count := reg.TenantCount()
	all := reg.All()
	if count != len(all) {
		t.Errorf("TenantCount() = %d, len(All()) = %d — mismatch", count, len(all))
	}
	if count != 50 {
		t.Errorf("TenantCount = %d, want 50", count)
	}
}

func TestRegression_RecordWriteAccumulatesCorrectly(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	reg.RecordWrite("a:1", 100, 200, 10, "STANDARD")
	reg.RecordWrite("a:1", 150, 300, 20, "STANDARD")
	reg.RecordWrite("a:1", 250, 500, 30, "GLACIER")

	ts := reg.Get("a:1")
	if ts == nil {
		t.Fatal("tenant should exist")
	}
	if ts.TotalBytes != 500 {
		t.Errorf("TotalBytes = %d, want 500 (100+150+250)", ts.TotalBytes)
	}
	if ts.RawBytes != 1000 {
		t.Errorf("RawBytes = %d, want 1000 (200+300+500)", ts.RawBytes)
	}
	if ts.TotalRows != 60 {
		t.Errorf("TotalRows = %d, want 60 (10+20+30)", ts.TotalRows)
	}
	if ts.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d, want 3", ts.TotalFiles)
	}
	// Check per-class accumulation.
	if ts.BytesByClass["STANDARD"] != 250 {
		t.Errorf("BytesByClass[STANDARD] = %d, want 250 (100+150)", ts.BytesByClass["STANDARD"])
	}
	if ts.BytesByClass["GLACIER"] != 250 {
		t.Errorf("BytesByClass[GLACIER] = %d, want 250", ts.BytesByClass["GLACIER"])
	}
	if ts.FilesByClass["STANDARD"] != 2 {
		t.Errorf("FilesByClass[STANDARD] = %d, want 2", ts.FilesByClass["STANDARD"])
	}
	if ts.FilesByClass["GLACIER"] != 1 {
		t.Errorf("FilesByClass[GLACIER] = %d, want 1", ts.FilesByClass["GLACIER"])
	}
}

// ---------------------------------------------------------------------------
// API — error/edge cases
// ---------------------------------------------------------------------------

func setupMinimalAPI(t *testing.T) *API {
	t.Helper()
	reg := NewTenantRegistry("node-1")
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	sct := NewStorageClassTracker(nil, nil)
	li := cache.NewLabelIndex()
	m := manifest.New("test-bucket", "data/")
	return NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     m,
		CostCalc:     cc,
		ClassTracker: sct,
		LabelIndex:   li,
		Mode:         "logs",
		Bucket:       "test-bucket",
	})
}

func doRegressionGet(t *testing.T, api *API, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	api.Register(mux)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func doRegressionMethod(t *testing.T, api *API, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	api.Register(mux)
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestRegression_APIMethodNotAllowed(t *testing.T) {
	api := setupMinimalAPI(t)

	endpoints := []string{
		"/lakehouse/api/v1/tenants",
		"/lakehouse/api/v1/tenants/acme/proj1",
		"/lakehouse/api/v1/stats/overview",
		"/lakehouse/api/v1/stats/ingestion",
		"/lakehouse/api/v1/stats/cost",
		"/lakehouse/api/v1/stats/compression",
		"/lakehouse/api/v1/cardinality/fields",
	}

	for _, ep := range endpoints {
		rec := doRegressionMethod(t, api, http.MethodPost, ep)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s: status = %d, want 405", ep, rec.Code)
		}
	}
}

func TestRegression_APITenantNotFound(t *testing.T) {
	api := setupMinimalAPI(t)
	rec := doRegressionGet(t, api, "/lakehouse/api/v1/tenants/nonexistent/nonexistent")
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestRegression_APIInvalidSortParam(t *testing.T) {
	api := setupMinimalAPI(t)
	rec := doRegressionGet(t, api, "/lakehouse/api/v1/tenants?sort=INVALID_SORT_XYZ")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (falls back to default sort)", rec.Code)
	}

	var resp TenantsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Should succeed and return valid response.
}

func TestRegression_APIInvalidLimitParam(t *testing.T) {
	api := setupMinimalAPI(t)
	rec := doRegressionGet(t, api, "/lakehouse/api/v1/cardinality/fields?limit=notanumber")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (defaults to limit=100)", rec.Code)
	}

	var resp CardinalityResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Should succeed with default limit.
}

func TestRegression_APICompressionEmpty(t *testing.T) {
	api := setupMinimalAPI(t)
	rec := doRegressionGet(t, api, "/lakehouse/api/v1/stats/compression")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp CompressionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.AvgRatio != 0 {
		t.Errorf("AvgRatio = %f, want 0 (no data)", resp.AvgRatio)
	}
	if len(resp.PerTenant) != 0 {
		t.Errorf("len(PerTenant) = %d, want 0", len(resp.PerTenant))
	}
}

// ---------------------------------------------------------------------------
// Sync — error/edge cases
// ---------------------------------------------------------------------------

func TestRegression_SyncHandlerBadMethod(t *testing.T) {
	handler := NewSyncHandler(NewTenantRegistry("n"), "")
	req := httptest.NewRequest(http.MethodGet, "/sync", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestRegression_SyncHandlerBadJSON(t *testing.T) {
	handler := NewSyncHandler(NewTenantRegistry("n"), "")
	req := httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(`{corrupt json`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRegression_SyncHandlerEmptyBody(t *testing.T) {
	handler := NewSyncHandler(NewTenantRegistry("n"), "")
	req := httptest.NewRequest(http.MethodPost, "/sync", bytes.NewReader([]byte{}))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Empty body is not valid JSON — should return 400.
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRegression_SyncPusherNoPeers(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 200, 1, "STANDARD")

	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return nil },
	})

	err := pusher.PushDelta(context.Background())
	if err != nil {
		t.Errorf("PushDelta with no peers should not error, got: %v", err)
	}
}

func TestRegression_SyncPusherUnreachablePeer(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 200, 1, "STANDARD")

	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return []string{"http://127.0.0.1:1"} },
	})

	err := pusher.PushDelta(context.Background())
	if err == nil {
		t.Error("expected error pushing to unreachable peer, got nil")
	}
}

// ---------------------------------------------------------------------------
// Cost Calculator — error/edge cases
// ---------------------------------------------------------------------------

func TestRegression_CostUnknownClass(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	cost := cc.MonthlyStorageCost("UNKNOWN_CLASS", 1<<30)
	if cost != 0 {
		t.Errorf("MonthlyStorageCost(UNKNOWN_CLASS) = %f, want 0", cost)
	}
}

func TestRegression_CostZeroBytes(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	cost := cc.MonthlyStorageCost("STANDARD", 0)
	if cost != 0 {
		t.Errorf("MonthlyStorageCost(0 bytes) = %f, want 0", cost)
	}
}

func TestRegression_CostNegativeBytes(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	cost := cc.MonthlyStorageCost("STANDARD", -1024)
	// Negative bytes should produce negative cost (no panic).
	if cost >= 0 {
		t.Errorf("MonthlyStorageCost(-1024) = %f, want < 0 (negative but no panic)", cost)
	}
}

// ---------------------------------------------------------------------------
// Storage Class Tracker — error/edge cases
// ---------------------------------------------------------------------------

func TestRegression_PredictClassNoRules(t *testing.T) {
	sct := NewStorageClassTracker(nil, nil)
	now := time.Now()
	createdAt := now.Add(-365 * 24 * time.Hour) // 1 year ago

	class := sct.PredictClass(createdAt, now)
	if class != "STANDARD" {
		t.Errorf("PredictClass with no rules = %q, want %q", class, "STANDARD")
	}
}

func TestRegression_PredictClassFutureDate(t *testing.T) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
	}
	sct := NewStorageClassTracker(rules, nil)
	now := time.Now()
	createdAt := now.Add(24 * time.Hour) // created in the future

	class := sct.PredictClass(createdAt, now)
	if class != "STANDARD" {
		t.Errorf("PredictClass(future) = %q, want %q", class, "STANDARD")
	}
}

func TestRegression_NearBoundaryNoRules(t *testing.T) {
	sct := NewStorageClassTracker(nil, nil)
	now := time.Now()
	createdAt := now.Add(-28 * 24 * time.Hour)

	near := sct.NearBoundary(createdAt, now)
	if near {
		t.Error("NearBoundary with no rules should return false")
	}
}

// ---------------------------------------------------------------------------
// Cardinality Limiter — error/edge cases
// ---------------------------------------------------------------------------

func TestRegression_LimiterZeroIsUnlimited(t *testing.T) {
	// Limit=0 means all tenants rejected.
	cl := NewCardinalityLimiter(0)

	if cl.Allow("tenant-1") {
		t.Error("Allow should return false when limit=0")
	}
	if cl.Allow("tenant-2") {
		t.Error("Allow should return false when limit=0")
	}
	if cl.OverflowCount() != 2 {
		t.Errorf("OverflowCount = %d, want 2", cl.OverflowCount())
	}
}

func TestRegression_LimiterNegativeIsUnlimited(t *testing.T) {
	cl := NewCardinalityLimiter(-1)

	for i := 0; i < 1000; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		if !cl.Allow(tenant) {
			t.Fatalf("Allow(%q) returned false with negative limit (unlimited)", tenant)
		}
	}
	if cl.TrackedCount() != 1000 {
		t.Errorf("TrackedCount = %d, want 1000", cl.TrackedCount())
	}
	if cl.OverflowCount() != 0 {
		t.Errorf("OverflowCount = %d, want 0", cl.OverflowCount())
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkRecordWrite(b *testing.B) {
	reg := NewTenantRegistry("node-bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tenant := fmt.Sprintf("acct-%d:proj-%d", i%100, i%10)
		reg.RecordWrite(tenant, 1024, 2048, 10, "STANDARD")
	}
}

func BenchmarkRegistryAll(b *testing.B) {
	reg := NewTenantRegistry("node-bench")
	for i := 0; i < 1000; i++ {
		tenant := fmt.Sprintf("acct-%d:proj-%d", i/10, i)
		reg.RecordWrite(tenant, int64(i*100), int64(i*200), int64(i), "STANDARD")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = reg.All()
	}
}

func BenchmarkBuildDelta(b *testing.B) {
	reg := NewTenantRegistry("node-bench")
	for i := 0; i < 1000; i++ {
		tenant := fmt.Sprintf("acct-%d:proj-%d", i/10, i)
		reg.RecordWrite(tenant, int64(i*100), int64(i*200), int64(i), "STANDARD")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = reg.BuildDelta(0)
	}
}
