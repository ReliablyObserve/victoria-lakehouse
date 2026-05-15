package stats

// Guards tests — protect against unintentional breaking changes to public
// contracts: API paths, JSON field names, config defaults, metric names,
// CRDT invariants, serialisation round-trips, and tenant key format.
//
// If a guard test fails it means a public-facing contract changed. That may
// be intentional — update the test — but it forces explicit acknowledgement.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ---------------------------------------------------------------------------
// Guard: API endpoint paths must not drift
// ---------------------------------------------------------------------------

func TestGuard_APIEndpointPaths(t *testing.T) {
	reg := NewTenantRegistry("guard")
	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     manifest.New("test-bucket", ""),
		CostCalc:     NewCostCalculator(nil, nil),
		ClassTracker: NewStorageClassTracker(nil, nil),
		LabelIndex:   cache.NewLabelIndex(),
		Mode:         "logs",
		Bucket:       "b",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	// Paths that should return 200 with empty data
	okPaths := []string{
		"/lakehouse/api/v1/tenants",
		"/lakehouse/api/v1/stats/overview",
		"/lakehouse/api/v1/stats/ingestion",
		"/lakehouse/api/v1/stats/cost",
		"/lakehouse/api/v1/stats/compression",
		"/lakehouse/api/v1/cardinality/fields",
	}

	for _, p := range okPaths {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", p, nil)
		mux.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("endpoint %s returned 404 — path was removed or renamed", p)
		}
	}

	// Tenant detail path: must return 404 for unknown tenant (not ServeMux 404).
	// The handler responds with {"error":"not found"} in body when the route exists.
	reg.RecordWrite("guard_acct:guard_proj", 1, 1, 1, "STANDARD")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/guard_acct/guard_proj", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code == http.StatusNotFound {
		// If it's a genuine route, it should find the tenant we just added
		t.Errorf("tenant detail endpoint returned 404 — path was removed or renamed")
	}
}

// ---------------------------------------------------------------------------
// Guard: API returns only GET, rejects POST/PUT/DELETE/PATCH
// ---------------------------------------------------------------------------

func TestGuard_APIGetOnlyEndpoints(t *testing.T) {
	reg := NewTenantRegistry("guard")
	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     manifest.New("test-bucket", ""),
		CostCalc:     NewCostCalculator(nil, nil),
		ClassTracker: NewStorageClassTracker(nil, nil),
		LabelIndex:   cache.NewLabelIndex(),
		Mode:         "logs",
		Bucket:       "b",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	paths := []string{
		"/lakehouse/api/v1/tenants",
		"/lakehouse/api/v1/stats/overview",
		"/lakehouse/api/v1/stats/ingestion",
		"/lakehouse/api/v1/stats/cost",
		"/lakehouse/api/v1/stats/compression",
		"/lakehouse/api/v1/cardinality/fields",
	}

	badMethods := []string{"POST", "PUT", "DELETE", "PATCH"}
	for _, p := range paths {
		for _, m := range badMethods {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(m, p, nil)
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: expected 405, got %d", m, p, rec.Code)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Guard: JSON response field names must match spec
// ---------------------------------------------------------------------------

func TestGuard_TenantsResponseJSONFields(t *testing.T) {
	required := []string{"tenants", "total_tenants", "total_bytes", "total_files"}
	var resp TenantsResponse
	data, _ := json.Marshal(resp)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("TenantsResponse missing JSON field %q", f)
		}
	}
}

func TestGuard_TenantEntryJSONFields(t *testing.T) {
	required := []string{
		"account_id", "project_id", "total_files", "total_bytes",
		"raw_bytes", "compression_ratio", "total_rows", "partitions",
		"monthly_cost_usd",
	}
	entry := TenantEntry{AccountID: "a", ProjectID: "p"}
	data, _ := json.Marshal(entry)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("TenantEntry missing JSON field %q", f)
		}
	}
}

func TestGuard_OverviewResponseJSONFields(t *testing.T) {
	required := []string{
		"bucket", "mode", "total_files", "total_bytes", "total_raw_bytes",
		"avg_compression_ratio", "total_rows", "partition_count",
		"tenant_count", "storage_by_class", "fleet_nodes", "registry_generation",
	}
	resp := OverviewResponse{StorageByClass: []ClassBreakdown{}}
	data, _ := json.Marshal(resp)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("OverviewResponse missing JSON field %q", f)
		}
	}
}

func TestGuard_CostResponseJSONFields(t *testing.T) {
	required := []string{"total_monthly_usd", "by_class", "per_tenant"}
	resp := CostResponse{ByClass: []ClassCost{}, PerTenant: []TenantCostEntry{}}
	data, _ := json.Marshal(resp)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("CostResponse missing JSON field %q", f)
		}
	}
}

func TestGuard_CardinalityResponseJSONFields(t *testing.T) {
	required := []string{
		"fields", "total_fields", "total_promoted", "total_map",
		"cardinality_threshold",
	}
	resp := CardinalityResponse{Fields: []FieldEntry{}}
	data, _ := json.Marshal(resp)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("CardinalityResponse missing JSON field %q", f)
		}
	}
}

func TestGuard_FieldEntryJSONFields(t *testing.T) {
	required := []string{"name", "cardinality", "type", "has_bloom"}
	entry := FieldEntry{Name: "x"}
	data, _ := json.Marshal(entry)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("FieldEntry missing JSON field %q", f)
		}
	}
}

func TestGuard_CompressionResponseJSONFields(t *testing.T) {
	required := []string{"avg_compression_ratio", "per_tenant"}
	resp := CompressionResponse{PerTenant: []TenantCompressionEntry{}}
	data, _ := json.Marshal(resp)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("CompressionResponse missing JSON field %q", f)
		}
	}
}

func TestGuard_IngestionResponseJSONFields(t *testing.T) {
	required := []string{"period", "range", "buckets", "total_bytes_ingested", "total_files_written"}
	resp := IngestionResponse{Buckets: []IngestionBucket{}}
	data, _ := json.Marshal(resp)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("IngestionResponse missing JSON field %q", f)
		}
	}
}

// ---------------------------------------------------------------------------
// Guard: TenantStats JSON round-trip preserves all fields
// ---------------------------------------------------------------------------

func TestGuard_TenantStatsJSONRoundTrip(t *testing.T) {
	original := &TenantStats{
		AccountID:    "acc",
		ProjectID:    "prj",
		TotalFiles:   42,
		TotalBytes:   1024,
		RawBytes:     2048,
		TotalRows:    100,
		Partitions:   3,
		MinTimeNs:    1000,
		MaxTimeNs:    9999,
		LastWriteAt:  time.Now().Truncate(time.Millisecond),
		LastQueryAt:  time.Now().Truncate(time.Millisecond),
		Labels:       map[string]int{"svc": 5},
		BytesByClass: map[string]int64{"STANDARD": 1024},
		FilesByClass: map[string]int64{"STANDARD": 42},
		NodeContribs: map[string]int64{"n1": 1024},
		nodeBytes:    map[string]int64{"n1": 1024},
		nodeRows:     map[string]int64{"n1": 100},
		nodeFiles:    map[string]int64{"n1": 42},
	}

	j := original.toJSON()
	data, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}

	var j2 tenantStatsJSON
	if err := json.Unmarshal(data, &j2); err != nil {
		t.Fatal(err)
	}
	restored := tenantStatsFromJSON(j2)

	if restored.AccountID != original.AccountID {
		t.Error("AccountID lost")
	}
	if restored.ProjectID != original.ProjectID {
		t.Error("ProjectID lost")
	}
	if restored.TotalFiles != original.TotalFiles {
		t.Error("TotalFiles lost")
	}
	if restored.TotalBytes != original.TotalBytes {
		t.Error("TotalBytes lost")
	}
	if restored.RawBytes != original.RawBytes {
		t.Error("RawBytes lost")
	}
	if restored.TotalRows != original.TotalRows {
		t.Error("TotalRows lost")
	}
	if restored.Partitions != original.Partitions {
		t.Error("Partitions lost")
	}
	if restored.MinTimeNs != original.MinTimeNs {
		t.Error("MinTimeNs lost")
	}
	if restored.MaxTimeNs != original.MaxTimeNs {
		t.Error("MaxTimeNs lost")
	}
	if !restored.LastWriteAt.Equal(original.LastWriteAt) {
		t.Error("LastWriteAt lost")
	}
	if !restored.LastQueryAt.Equal(original.LastQueryAt) {
		t.Error("LastQueryAt lost")
	}
	if !reflect.DeepEqual(restored.Labels, original.Labels) {
		t.Error("Labels lost")
	}
	if !reflect.DeepEqual(restored.BytesByClass, original.BytesByClass) {
		t.Error("BytesByClass lost")
	}
	if !reflect.DeepEqual(restored.FilesByClass, original.FilesByClass) {
		t.Error("FilesByClass lost")
	}
	if !reflect.DeepEqual(restored.nodeBytes, original.nodeBytes) {
		t.Error("nodeBytes lost in round-trip")
	}
	if !reflect.DeepEqual(restored.nodeRows, original.nodeRows) {
		t.Error("nodeRows lost in round-trip")
	}
	if !reflect.DeepEqual(restored.nodeFiles, original.nodeFiles) {
		t.Error("nodeFiles lost in round-trip")
	}
}

// ---------------------------------------------------------------------------
// Guard: TenantDelta JSON field names must remain stable (sync protocol)
// ---------------------------------------------------------------------------

func TestGuard_TenantDeltaJSONFieldNames(t *testing.T) {
	d := &TenantDelta{
		NodeID:     "n1",
		Generation: 5,
		Tenants:    map[string]*TenantStats{},
		Timestamp:  time.Now(),
	}
	dj := tenantDeltaToJSON(d)
	data, _ := json.Marshal(dj)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}

	required := []string{"node_id", "generation", "tenants", "timestamp"}
	for _, f := range required {
		if _, ok := m[f]; !ok {
			t.Errorf("TenantDelta JSON missing field %q — sync protocol broken", f)
		}
	}
}

// ---------------------------------------------------------------------------
// Guard: Tenant key separator must be colon
// ---------------------------------------------------------------------------

func TestGuard_TenantKeySeparator(t *testing.T) {
	reg := NewTenantRegistry("node")
	reg.RecordWrite("acc:proj", 100, 200, 10, "STANDARD")

	ts := reg.Get("acc:proj")
	if ts == nil {
		t.Fatal("Get with colon separator returned nil — key format changed")
	}
	if ts.AccountID != "acc" || ts.ProjectID != "proj" {
		t.Errorf("AccountID=%q ProjectID=%q — key parsing broken", ts.AccountID, ts.ProjectID)
	}

	// Slash must NOT work as key
	tsSlash := reg.Get("acc/proj")
	if tsSlash != nil {
		t.Error("Get with slash separator returned non-nil — must use colon")
	}
}

// ---------------------------------------------------------------------------
// Guard: CRDT merge invariants
// ---------------------------------------------------------------------------

func TestGuard_CRDTMergeIdempotent(t *testing.T) {
	reg := NewTenantRegistry("n1")
	reg.RecordWrite("t:p", 100, 200, 10, "STANDARD")

	delta := reg.BuildDelta(0)

	reg.Merge(delta)
	reg.Merge(delta)
	reg.Merge(delta)

	ts := reg.Get("t:p")
	if ts.TotalBytes != 100 {
		t.Errorf("idempotency broken: TotalBytes=%d want 100 after triple merge of same delta", ts.TotalBytes)
	}
}

func TestGuard_CRDTMergeCommutative(t *testing.T) {
	// Apply deltas from A then B → must equal B then A
	regA := NewTenantRegistry("A")
	regA.RecordWrite("t:p", 100, 200, 10, "STANDARD")
	deltaA := regA.BuildDelta(0)

	regB := NewTenantRegistry("B")
	regB.RecordWrite("t:p", 200, 400, 20, "STANDARD")
	deltaB := regB.BuildDelta(0)

	// Order 1: A then B
	reg1 := NewTenantRegistry("target1")
	reg1.Merge(deltaA)
	reg1.Merge(deltaB)

	// Order 2: B then A
	reg2 := NewTenantRegistry("target2")
	reg2.Merge(deltaB)
	reg2.Merge(deltaA)

	ts1 := reg1.Get("t:p")
	ts2 := reg2.Get("t:p")
	if ts1.TotalBytes != ts2.TotalBytes {
		t.Errorf("commutativity broken: AB=%d BA=%d", ts1.TotalBytes, ts2.TotalBytes)
	}
	if ts1.TotalRows != ts2.TotalRows {
		t.Errorf("commutativity broken rows: AB=%d BA=%d", ts1.TotalRows, ts2.TotalRows)
	}
}

func TestGuard_CRDTMergeAssociative(t *testing.T) {
	regA := NewTenantRegistry("A")
	regA.RecordWrite("t:p", 100, 200, 10, "STANDARD")
	deltaA := regA.BuildDelta(0)

	regB := NewTenantRegistry("B")
	regB.RecordWrite("t:p", 200, 400, 20, "STANDARD")
	deltaB := regB.BuildDelta(0)

	regC := NewTenantRegistry("C")
	regC.RecordWrite("t:p", 300, 600, 30, "STANDARD")
	deltaC := regC.BuildDelta(0)

	// (A merge B) merge C
	reg1 := NewTenantRegistry("t1")
	reg1.Merge(deltaA)
	reg1.Merge(deltaB)
	reg1.Merge(deltaC)

	// A merge (B merge C)
	reg2 := NewTenantRegistry("t2")
	reg2.Merge(deltaC)
	reg2.Merge(deltaB)
	reg2.Merge(deltaA)

	ts1 := reg1.Get("t:p")
	ts2 := reg2.Get("t:p")
	if ts1.TotalBytes != ts2.TotalBytes {
		t.Errorf("associativity broken: (AB)C=%d A(BC)=%d", ts1.TotalBytes, ts2.TotalBytes)
	}
}

func TestGuard_CRDTMergeMonotonicCounters(t *testing.T) {
	reg := NewTenantRegistry("n1")
	reg.RecordWrite("t:p", 100, 200, 10, "STANDARD")

	bytesBefore := reg.Get("t:p").TotalBytes

	// Merge a delta with a SMALLER value for the same node
	delta := &TenantDelta{
		NodeID:     "n1",
		Generation: 999,
		Tenants: map[string]*TenantStats{
			"t:p": {
				AccountID: "t",
				ProjectID: "p",
				nodeBytes: map[string]int64{"n1": 50}, // smaller than 100
				nodeRows:  map[string]int64{"n1": 5},
				nodeFiles: map[string]int64{"n1": 0},
			},
		},
		Timestamp: time.Now(),
	}
	reg.Merge(delta)

	bytesAfter := reg.Get("t:p").TotalBytes
	if bytesAfter < bytesBefore {
		t.Errorf("counter went backwards: before=%d after=%d", bytesBefore, bytesAfter)
	}
}

func TestGuard_CRDTTimestampExtrema(t *testing.T) {
	reg := NewTenantRegistry("n1")
	reg.RecordWrite("t:p", 100, 200, 10, "STANDARD")

	ts := reg.Get("t:p")
	origWrite := ts.LastWriteAt

	// Merge with an older timestamp — must not regress
	delta := &TenantDelta{
		NodeID:     "n2",
		Generation: 1,
		Tenants: map[string]*TenantStats{
			"t:p": {
				AccountID:   "t",
				ProjectID:   "p",
				LastWriteAt: origWrite.Add(-1 * time.Hour),
				MinTimeNs:   999999,
				MaxTimeNs:   1,
				nodeBytes:   map[string]int64{"n2": 50},
				nodeRows:    map[string]int64{"n2": 5},
				nodeFiles:   map[string]int64{"n2": 1},
			},
		},
		Timestamp: time.Now(),
	}
	reg.Merge(delta)

	ts = reg.Get("t:p")
	if ts.LastWriteAt.Before(origWrite) {
		t.Error("LastWriteAt went backwards after merge")
	}
}

// ---------------------------------------------------------------------------
// Guard: Registry snapshot format backward compat
// ---------------------------------------------------------------------------

func TestGuard_SnapshotContainsExpectedKeys(t *testing.T) {
	reg := NewTenantRegistry("snap-node")
	reg.RecordWrite("a:b", 100, 200, 10, "STANDARD")

	data, err := reg.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}

	required := []string{"node_id", "generation", "tenants"}
	for _, k := range required {
		if _, ok := m[k]; !ok {
			t.Errorf("snapshot missing top-level key %q — format changed", k)
		}
	}
}

func TestGuard_SnapshotCrossVersionLoad(t *testing.T) {
	// Simulate a snapshot from an older version that might have fewer fields.
	// The loader must tolerate missing optional fields.
	oldSnapshot := `{
		"node_id": "old-node",
		"generation": 5,
		"tenants": {
			"x:y": {
				"account_id": "x",
				"project_id": "y",
				"total_files": 10,
				"total_bytes": 500,
				"total_rows": 50,
				"node_bytes": {"old-node": 500},
				"node_rows": {"old-node": 50},
				"node_files": {"old-node": 10}
			}
		}
	}`

	reg := NewTenantRegistry("new-node")
	if err := reg.LoadSnapshot("old-node", []byte(oldSnapshot)); err != nil {
		t.Fatalf("failed to load old-format snapshot: %s", err)
	}

	ts := reg.Get("x:y")
	if ts == nil {
		t.Fatal("tenant not restored from old snapshot")
	}
	if ts.TotalBytes != 500 {
		t.Errorf("TotalBytes=%d want 500", ts.TotalBytes)
	}
}

// ---------------------------------------------------------------------------
// Guard: Config defaults must match spec
// ---------------------------------------------------------------------------

func TestGuard_StatsConfigDefaults(t *testing.T) {
	cfg := config.Default()

	if !cfg.Stats.Enabled {
		t.Error("stats.enabled default must be true")
	}
	if cfg.Stats.PushInterval != 30*time.Second {
		t.Errorf("push_interval default=%v want 30s", cfg.Stats.PushInterval)
	}
	if !cfg.Stats.PushCompression {
		t.Error("push_compression default must be true")
	}
	if cfg.Stats.SnapshotInterval != 5*time.Minute {
		t.Errorf("snapshot_interval default=%v want 5m", cfg.Stats.SnapshotInterval)
	}
	if cfg.Stats.SnapshotPrefix != "_meta/tenant-stats" {
		t.Errorf("snapshot_prefix default=%q want _meta/tenant-stats", cfg.Stats.SnapshotPrefix)
	}
	if cfg.Stats.MaxDeltaCount != 1000 {
		t.Errorf("max_delta_count default=%d want 1000", cfg.Stats.MaxDeltaCount)
	}
	if cfg.Stats.MetricsCardinalityLimit != 100 {
		t.Errorf("metrics_cardinality_limit default=%d want 100", cfg.Stats.MetricsCardinalityLimit)
	}
	if cfg.Stats.CardinalityWarningThreshold != 10000 {
		t.Errorf("cardinality_warning_threshold default=%d want 10000", cfg.Stats.CardinalityWarningThreshold)
	}
}

func TestGuard_StatsConfigS3Pricing(t *testing.T) {
	cfg := config.Default()

	expectedPrices := map[string]float64{
		"STANDARD":     0.023,
		"STANDARD_IA":  0.0125,
		"GLACIER_IR":   0.004,
		"GLACIER":      0.0036,
		"DEEP_ARCHIVE": 0.00099,
	}
	for class, want := range expectedPrices {
		got, ok := cfg.Stats.S3PricePerGB[class]
		if !ok {
			t.Errorf("S3PricePerGB missing class %q", class)
		} else if got != want {
			t.Errorf("S3PricePerGB[%s]=%f want %f", class, got, want)
		}
	}

	expectedRequests := map[string]float64{
		"PUT":  0.005,
		"GET":  0.0004,
		"LIST": 0.005,
	}
	for op, want := range expectedRequests {
		got, ok := cfg.Stats.S3RequestPrices[op]
		if !ok {
			t.Errorf("S3RequestPrices missing op %q", op)
		} else if got != want {
			t.Errorf("S3RequestPrices[%s]=%f want %f", op, got, want)
		}
	}
}

func TestGuard_UIConfigDefaults(t *testing.T) {
	cfg := config.Default()
	if !cfg.UI.Enabled {
		t.Error("ui.enabled default must be true")
	}
	if !cfg.UI.VMUITab {
		t.Error("ui.vmui_tab default must be true")
	}
	if cfg.UI.RefreshDefault != 0 {
		t.Errorf("ui.refresh_default=%d want 0", cfg.UI.RefreshDefault)
	}
	if cfg.UI.Theme != "auto" {
		t.Errorf("ui.theme=%q want auto", cfg.UI.Theme)
	}
}

// ---------------------------------------------------------------------------
// Guard: Storage class names match AWS S3 exactly
// ---------------------------------------------------------------------------

func TestGuard_StorageClassNames(t *testing.T) {
	validClasses := []string{
		"STANDARD", "STANDARD_IA", "GLACIER_IR", "GLACIER", "DEEP_ARCHIVE",
	}

	cfg := config.Default()
	for _, class := range validClasses {
		if _, ok := cfg.Stats.S3PricePerGB[class]; !ok {
			t.Errorf("S3PricePerGB missing AWS storage class %q", class)
		}
	}

	// Verify no typos in lifecycle rule output
	tracker := NewStorageClassTracker([]config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
	}, nil)

	now := time.Now()
	result := tracker.PredictClass(now.Add(-45*24*time.Hour), now)
	if result != "STANDARD_IA" {
		t.Errorf("PredictClass returned %q — expected exact AWS class name STANDARD_IA", result)
	}

	result = tracker.PredictClass(now.Add(-120*24*time.Hour), now)
	if result != "GLACIER" {
		t.Errorf("PredictClass returned %q — expected exact AWS class name GLACIER", result)
	}

	result = tracker.PredictClass(now.Add(-5*24*time.Hour), now)
	if result != "STANDARD" {
		t.Errorf("PredictClass returned %q — expected STANDARD for recent files", result)
	}
}

// ---------------------------------------------------------------------------
// Guard: NewTenantRegistry takes ONLY nodeID
// ---------------------------------------------------------------------------

func TestGuard_NewTenantRegistrySignature(t *testing.T) {
	// This test exists to catch if someone adds parameters to NewTenantRegistry.
	// If the function signature changes, this will fail to compile.
	reg := NewTenantRegistry("test")
	if reg == nil {
		t.Fatal("NewTenantRegistry returned nil")
	}
}

// ---------------------------------------------------------------------------
// Guard: RecordWrite signature and accumulation
// ---------------------------------------------------------------------------

func TestGuard_RecordWriteSignature(t *testing.T) {
	reg := NewTenantRegistry("n")
	// This tests the exact parameter order: tenant, bytes, rawBytes, rows, storageClass
	reg.RecordWrite("a:b", 100, 200, 10, "STANDARD")

	ts := reg.Get("a:b")
	if ts == nil {
		t.Fatal("RecordWrite did not create tenant")
	}
	if ts.TotalBytes != 100 {
		t.Errorf("bytes=%d want 100", ts.TotalBytes)
	}
	if ts.RawBytes != 200 {
		t.Errorf("rawBytes=%d want 200", ts.RawBytes)
	}
	if ts.TotalRows != 10 {
		t.Errorf("rows=%d want 10", ts.TotalRows)
	}
	if ts.BytesByClass["STANDARD"] != 100 {
		t.Errorf("STANDARD bytes=%d want 100", ts.BytesByClass["STANDARD"])
	}
}

// ---------------------------------------------------------------------------
// Guard: All() returns slice (not map)
// ---------------------------------------------------------------------------

func TestGuard_AllReturnsSlice(t *testing.T) {
	reg := NewTenantRegistry("n")
	reg.RecordWrite("a:b", 100, 200, 10, "STANDARD")

	all := reg.All()
	if all == nil {
		t.Fatal("All() returned nil")
	}
	// Type is []*TenantStats — if All() return type changes, element access below breaks.
	_ = all[0].AccountID
	if len(all) != 1 {
		t.Errorf("All() len=%d want 1", len(all))
	}
}

// ---------------------------------------------------------------------------
// Guard: GlobalAggregates returns non-nil with correct sums
// ---------------------------------------------------------------------------

func TestGuard_GlobalAggregatesCorrectness(t *testing.T) {
	reg := NewTenantRegistry("n")
	reg.RecordWrite("a:b", 100, 200, 10, "STANDARD")
	reg.RecordWrite("c:d", 200, 400, 20, "GLACIER")

	ga := reg.GlobalAggregates()
	if ga == nil {
		t.Fatal("GlobalAggregates returned nil")
	}
	if ga.TotalBytes != 300 {
		t.Errorf("global bytes=%d want 300", ga.TotalBytes)
	}
	if ga.RawBytes != 600 {
		t.Errorf("global rawBytes=%d want 600", ga.RawBytes)
	}
	if ga.TotalRows != 30 {
		t.Errorf("global rows=%d want 30", ga.TotalRows)
	}
	if ga.TenantCount != 2 {
		t.Errorf("global tenantCount=%d want 2", ga.TenantCount)
	}
}

// ---------------------------------------------------------------------------
// Guard: Sync handler path must be /internal/stats/sync
// ---------------------------------------------------------------------------

func TestGuard_SyncHandlerPath(t *testing.T) {
	reg := NewTenantRegistry("n")
	handler := NewSyncHandler(reg, "")
	mux := http.NewServeMux()
	mux.Handle("/internal/stats/sync", handler)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/internal/stats/sync", strings.NewReader(`{"node_id":"x","generation":1,"tenants":{},"timestamp":"2026-01-01T00:00:00Z"}`))
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("sync handler at /internal/stats/sync returned %d", rec.Code)
	}
}

// Guard: VMUI inject script path is tested in internal/ui/guards_test.go

// ---------------------------------------------------------------------------
// Guard: Cardinality limiter boundary behavior
// ---------------------------------------------------------------------------

func TestGuard_CardinalityLimiterBoundaries(t *testing.T) {
	// limit=0 → reject all (per spec: "0 = disable per-tenant metrics")
	cl0 := NewCardinalityLimiter(0)
	if cl0.Allow("any") {
		t.Error("limit=0 must reject all tenants")
	}

	// limit<0 → unlimited
	clNeg := NewCardinalityLimiter(-1)
	for i := 0; i < 10000; i++ {
		if !clNeg.Allow("t" + string(rune(i))) {
			t.Fatalf("negative limit must allow unlimited tenants, rejected at %d", i)
		}
	}

	// limit=N → allow exactly N unique, reject N+1
	cl5 := NewCardinalityLimiter(5)
	for i := 0; i < 5; i++ {
		key := "tenant" + string(rune('A'+i))
		if !cl5.Allow(key) {
			t.Fatalf("limit=5 rejected tenant %d", i)
		}
	}
	if cl5.Allow("tenant-overflow") {
		t.Error("limit=5 accepted 6th tenant")
	}
	// Re-allow existing tenant
	if !cl5.Allow("tenantA") {
		t.Error("limit=5 rejected existing tenant")
	}
}

// ---------------------------------------------------------------------------
// Guard: Cost calculator known class prices
// ---------------------------------------------------------------------------

func TestGuard_CostCalculatorAWSPrices(t *testing.T) {
	prices := map[string]float64{
		"STANDARD":     0.023,
		"STANDARD_IA":  0.0125,
		"GLACIER":      0.0036,
		"DEEP_ARCHIVE": 0.00099,
	}
	calc := NewCostCalculator(prices, nil)

	oneGB := int64(1024 * 1024 * 1024)
	for class, pricePerGB := range prices {
		cost := calc.MonthlyStorageCost(class, oneGB)
		if cost < pricePerGB*0.99 || cost > pricePerGB*1.01 {
			t.Errorf("MonthlyStorageCost(%s, 1GB)=%f want ~%f", class, cost, pricePerGB)
		}
	}
}

// ---------------------------------------------------------------------------
// Guard: FileInfo JSON tags stability (manifest persistence format)
// ---------------------------------------------------------------------------

func TestGuard_FileInfoJSONTags(t *testing.T) {
	fi := manifest.FileInfo{
		Key:          "k",
		Size:         1,
		StorageClass: "STANDARD",
		ClassSource:  "write",
		CreatedAt:    time.Now(),
	}
	data, _ := json.Marshal(fi)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}

	requiredFields := []string{
		"key", "size", "storage_class", "class_source", "created_at",
	}
	for _, f := range requiredFields {
		if _, ok := m[f]; !ok {
			t.Errorf("FileInfo JSON missing field %q — manifest format broken", f)
		}
	}
}

// ---------------------------------------------------------------------------
// Guard: Sort parameter values accepted by tenants endpoint
// ---------------------------------------------------------------------------

func TestGuard_TenantsSortParams(t *testing.T) {
	reg := NewTenantRegistry("n")
	reg.RecordWrite("a:b", 100, 200, 10, "STANDARD")
	reg.RecordWrite("c:d", 200, 400, 20, "STANDARD")

	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     manifest.New("test-bucket", ""),
		CostCalc:     NewCostCalculator(nil, nil),
		ClassTracker: NewStorageClassTracker(nil, nil),
		LabelIndex:   cache.NewLabelIndex(),
		Mode:         "logs",
		Bucket:       "b",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	validSorts := []string{"bytes", "files", "cost", "rows", ""}
	for _, s := range validSorts {
		url := "/lakehouse/api/v1/tenants"
		if s != "" {
			url += "?sort=" + s
		}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", url, nil)
		mux.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Errorf("sort=%q returned %d", s, rec.Code)
		}
	}
}

// ---------------------------------------------------------------------------
// Guard: API content type is application/json
// ---------------------------------------------------------------------------

func TestGuard_APIContentType(t *testing.T) {
	reg := NewTenantRegistry("n")
	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     manifest.New("test-bucket", ""),
		CostCalc:     NewCostCalculator(nil, nil),
		ClassTracker: NewStorageClassTracker(nil, nil),
		LabelIndex:   cache.NewLabelIndex(),
		Mode:         "logs",
		Bucket:       "b",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	paths := []string{
		"/lakehouse/api/v1/tenants",
		"/lakehouse/api/v1/stats/overview",
		"/lakehouse/api/v1/stats/cost",
		"/lakehouse/api/v1/stats/compression",
		"/lakehouse/api/v1/cardinality/fields",
	}
	for _, p := range paths {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", p, nil)
		mux.ServeHTTP(rec, req)
		ct := rec.Header().Get("Content-Type")
		if !strings.HasPrefix(ct, "application/json") {
			t.Errorf("%s Content-Type=%q want application/json", p, ct)
		}
	}
}

// ---------------------------------------------------------------------------
// Guard: Lifecycle rule sort order (descending by TransitionDays)
// ---------------------------------------------------------------------------

func TestGuard_LifecycleRuleSortOrder(t *testing.T) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 365, StorageClass: "DEEP_ARCHIVE"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
	}
	tracker := NewStorageClassTracker(rules, nil)
	now := time.Now()

	// 400 days: must hit DEEP_ARCHIVE (365), not stop at GLACIER (90) or IA (30)
	class := tracker.PredictClass(now.Add(-400*24*time.Hour), now)
	if class != "DEEP_ARCHIVE" {
		t.Errorf("400d file class=%q want DEEP_ARCHIVE — rules not sorted descending", class)
	}

	// 100 days: must hit GLACIER (90), not DEEP_ARCHIVE (365) or IA (30)
	class = tracker.PredictClass(now.Add(-100*24*time.Hour), now)
	if class != "GLACIER" {
		t.Errorf("100d file class=%q want GLACIER", class)
	}
}

// ---------------------------------------------------------------------------
// Guard: RecordWrite must not reset existing tenant data
// ---------------------------------------------------------------------------

func TestGuard_RecordWriteAccumulates(t *testing.T) {
	reg := NewTenantRegistry("n")
	reg.RecordWrite("t:p", 100, 200, 10, "STANDARD")
	reg.RecordWrite("t:p", 200, 400, 20, "STANDARD")
	reg.RecordWrite("t:p", 300, 600, 30, "GLACIER")

	ts := reg.Get("t:p")
	if ts.TotalBytes != 600 {
		t.Errorf("bytes=%d want 600 (100+200+300)", ts.TotalBytes)
	}
	if ts.TotalRows != 60 {
		t.Errorf("rows=%d want 60 (10+20+30)", ts.TotalRows)
	}
	if ts.TotalFiles != 3 {
		t.Errorf("files=%d want 3", ts.TotalFiles)
	}
	if ts.BytesByClass["STANDARD"] != 300 {
		t.Errorf("STANDARD bytes=%d want 300", ts.BytesByClass["STANDARD"])
	}
	if ts.BytesByClass["GLACIER"] != 300 {
		t.Errorf("GLACIER bytes=%d want 300", ts.BytesByClass["GLACIER"])
	}
}

// ---------------------------------------------------------------------------
// Guard: Generation must be monotonically increasing
// ---------------------------------------------------------------------------

func TestGuard_GenerationMonotonic(t *testing.T) {
	reg := NewTenantRegistry("n")

	gen0 := reg.BuildDelta(0).Generation
	reg.RecordWrite("a:b", 1, 1, 1, "STANDARD")
	gen1 := reg.BuildDelta(0).Generation
	reg.RecordWrite("a:b", 1, 1, 1, "STANDARD")
	gen2 := reg.BuildDelta(0).Generation

	if gen1 <= gen0 {
		t.Errorf("generation did not increase: %d -> %d", gen0, gen1)
	}
	if gen2 <= gen1 {
		t.Errorf("generation did not increase: %d -> %d", gen1, gen2)
	}
}

// ---------------------------------------------------------------------------
// Guard: API tenant detail path parsing
// ---------------------------------------------------------------------------

func TestGuard_APITenantDetailPathParsing(t *testing.T) {
	reg := NewTenantRegistry("n")
	reg.RecordWrite("myaccount:myproject", 100, 200, 10, "STANDARD")

	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     manifest.New("test-bucket", ""),
		CostCalc:     NewCostCalculator(nil, nil),
		ClassTracker: NewStorageClassTracker(nil, nil),
		LabelIndex:   cache.NewLabelIndex(),
		Mode:         "logs",
		Bucket:       "b",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	// Path format: /lakehouse/api/v1/tenants/{accountID}/{projectID}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/myaccount/myproject", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Errorf("tenant detail returned %d — path parsing broken", rec.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp["account_id"] != "myaccount" {
		t.Errorf("account_id=%v want myaccount", resp["account_id"])
	}
	if resp["project_id"] != "myproject" {
		t.Errorf("project_id=%v want myproject", resp["project_id"])
	}
}

// ---------------------------------------------------------------------------
// Guard: Metric variable names and prefixes
// ---------------------------------------------------------------------------

func TestGuard_TenantMetricVariablesExist(t *testing.T) {
	// These variables are referenced in dashboards, alerting rules, and the
	// Prometheus metrics update loop. If renamed or removed, this test fails
	// at compile time (variable not found) or at run time (nil check).
	vars := map[string]any{
		"TenantFiles":               metrics.TenantFiles,
		"TenantBytes":               metrics.TenantBytes,
		"TenantRawBytes":            metrics.TenantRawBytes,
		"TenantRowsTotal":           metrics.TenantRowsTotal,
		"TenantIngestionBytesTotal": metrics.TenantIngestionBytesTotal,
		"TenantQueriesTotal":        metrics.TenantQueriesTotal,
		"TenantLastWriteTimestamp":  metrics.TenantLastWriteTimestamp,
		"TenantLastQueryTimestamp":  metrics.TenantLastQueryTimestamp,
	}
	for name, v := range vars {
		if v == nil {
			t.Errorf("metrics.%s is nil — metric was removed", name)
		}
	}
}

func TestGuard_GlobalStorageMetricVariablesExist(t *testing.T) {
	vars := map[string]any{
		"StorageFilesTotal":       metrics.StorageFilesTotal,
		"StorageBytesTotal":       metrics.StorageBytesTotal,
		"StorageRawBytesTotal":    metrics.StorageRawBytesTotal,
		"StorageCompressionRatio": metrics.StorageCompressionRatio,
		"StorageRowsTotal":        metrics.StorageRowsTotal,
		"StoragePartitionsTotal":  metrics.StoragePartitionsTotal,
		"StorageTenantsTotal":     metrics.StorageTenantsTotal,
		"StorageBytesByClass":     metrics.StorageBytesByClass,
		"StorageFilesByClass":     metrics.StorageFilesByClass,
		"StorageCostMonthlyUSD":   metrics.StorageCostMonthlyUSD,
		"StorageCostByClassUSD":   metrics.StorageCostByClassUSD,
	}
	for name, v := range vars {
		if v == nil {
			t.Errorf("metrics.%s is nil — metric was removed", name)
		}
	}
}

func TestGuard_CardinalityMetaMetricVariablesExist(t *testing.T) {
	vars := map[string]any{
		"MetricsCardinalityLimit":    metrics.MetricsCardinalityLimit,
		"MetricsCardinalityTracked":  metrics.MetricsCardinalityTracked,
		"MetricsCardinalityOverflow": metrics.MetricsCardinalityOverflow,
	}
	for name, v := range vars {
		if v == nil {
			t.Errorf("metrics.%s is nil — metric was removed", name)
		}
	}
}

func TestGuard_StatsSyncMetricVariablesExist(t *testing.T) {
	vars := map[string]any{
		"StatsPushTotal":      metrics.StatsPushTotal,
		"StatsPushErrors":     metrics.StatsPushErrors,
		"StatsPushBytesTotal": metrics.StatsPushBytesTotal,
		"StatsSnapshotTotal":  metrics.StatsSnapshotTotal,
		"StatsSnapshotErrors": metrics.StatsSnapshotErrors,
		"StatsMergesTotal":    metrics.StatsMergesTotal,
	}
	for name, v := range vars {
		if v == nil {
			t.Errorf("metrics.%s is nil — metric was removed", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Guard: PerTenant field on LabelInfo
// ---------------------------------------------------------------------------

func TestGuard_LabelIndexPerTenantField(t *testing.T) {
	li := cache.NewLabelIndex()
	li.AddWithTenant("service.name", []string{"api", "web"}, "t:p")

	info := li.GetLabelInfo("service.name")
	if info == nil {
		t.Fatal("GetLabelInfo returned nil")
	}
	if info.PerTenant == nil {
		t.Fatal("PerTenant field is nil")
	}
	if info.PerTenant["t:p"] != 2 {
		t.Errorf("PerTenant[t:p]=%d want 2", info.PerTenant["t:p"])
	}
}

// ---------------------------------------------------------------------------
// Guard: BuildDelta respects generation filter
// ---------------------------------------------------------------------------

func TestGuard_BuildDeltaGenerationFilter(t *testing.T) {
	reg := NewTenantRegistry("n")
	reg.RecordWrite("a:b", 100, 200, 10, "STANDARD")

	gen := reg.BuildDelta(0).Generation

	// No new writes — delta should have 0 changed tenants
	delta := reg.BuildDelta(gen)
	if len(delta.Tenants) != 0 {
		t.Errorf("delta after no changes has %d tenants, want 0", len(delta.Tenants))
	}

	// New write — delta should have the changed tenant
	reg.RecordWrite("a:b", 50, 100, 5, "STANDARD")
	delta = reg.BuildDelta(gen)
	if len(delta.Tenants) != 1 {
		t.Errorf("delta after write has %d tenants, want 1", len(delta.Tenants))
	}
}

// ---------------------------------------------------------------------------
// Guard: NearBoundary consistency with PredictClass
// ---------------------------------------------------------------------------

func TestGuard_NearBoundaryConsistency(t *testing.T) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
	}
	tracker := NewStorageClassTracker(rules, nil)
	now := time.Now()

	// File at exactly 28 days (within 2 days of 30-day boundary) should be "near"
	near := tracker.NearBoundary(now.Add(-28*24*time.Hour), now)
	if !near {
		t.Error("28-day file should be near 30-day boundary")
	}

	// File at 15 days should NOT be near
	near = tracker.NearBoundary(now.Add(-15*24*time.Hour), now)
	if near {
		t.Error("15-day file should not be near any boundary")
	}

	// File at 88 days (within 2 days of 90-day boundary) should be "near"
	near = tracker.NearBoundary(now.Add(-88*24*time.Hour), now)
	if !near {
		t.Error("88-day file should be near 90-day boundary")
	}
}

// ---------------------------------------------------------------------------
// Guard: Sync auth header format
// ---------------------------------------------------------------------------

func TestGuard_SyncAuthHeaderFormat(t *testing.T) {
	reg := NewTenantRegistry("n")
	handler := NewSyncHandler(reg, "secret123")

	body := `{"node_id":"x","generation":1,"tenants":{},"timestamp":"2026-01-01T00:00:00Z"}`

	// Correct format: "Bearer <key>"
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/sync", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret123")
	handler.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("correct auth returned %d", rec.Code)
	}

	// Wrong format: just the key (no Bearer prefix)
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("POST", "/sync", strings.NewReader(body))
	req2.Header.Set("Authorization", "secret123")
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != 401 {
		t.Errorf("auth without Bearer prefix returned %d want 401", rec2.Code)
	}
}

// ---------------------------------------------------------------------------
// Guard: API responses are sorted deterministically
// ---------------------------------------------------------------------------

func TestGuard_TenantsResponseSortedByBytes(t *testing.T) {
	reg := NewTenantRegistry("n")
	reg.RecordWrite("small:s", 100, 200, 10, "STANDARD")
	reg.RecordWrite("large:l", 9999, 19998, 999, "STANDARD")
	reg.RecordWrite("med:m", 500, 1000, 50, "STANDARD")

	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     manifest.New("test-bucket", ""),
		CostCalc:     NewCostCalculator(nil, nil),
		ClassTracker: NewStorageClassTracker(nil, nil),
		LabelIndex:   cache.NewLabelIndex(),
		Mode:         "logs",
		Bucket:       "b",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants?sort=bytes", nil)
	mux.ServeHTTP(rec, req)

	var resp TenantsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if len(resp.Tenants) < 3 {
		t.Fatalf("expected 3 tenants, got %d", len(resp.Tenants))
	}

	bytes := make([]int64, len(resp.Tenants))
	for i, te := range resp.Tenants {
		bytes[i] = te.TotalBytes
	}

	if !sort.SliceIsSorted(bytes, func(i, j int) bool { return bytes[i] > bytes[j] }) {
		t.Errorf("tenants not sorted by bytes descending: %v", bytes)
	}
}
