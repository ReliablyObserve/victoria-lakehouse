package stats

import (
	"encoding/json"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func logsSchemaRegistry() *schema.Registry {
	return schema.NewRegistry(schema.LogsProfile)
}

// --- Config option tests ---

func TestConfigBreakdownLabelsAffectsOutput(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	m := manifest.New("test-bucket", "data/")
	m.AddFile("dt=2026-05-10/hour=10", manifest.FileInfo{
		Key:  "0/0/logs/dt=2026-05-10/hour=10/part.parquet",
		Size: 1 << 20,
	})

	li := cache.NewLabelIndex()
	li.AddWithValueCounts("service.name", []string{"api", "web", "worker"}, map[string]int{
		"api": 50, "web": 30, "worker": 20,
	})
	li.AddWithValueCounts("level", []string{"info", "warn", "error"}, map[string]int{
		"info": 70, "warn": 20, "error": 10,
	})

	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)

	api := NewAPI(APIConfig{
		Registry:        reg,
		Manifest:        m,
		CostCalc:        cc,
		LabelIndex:      li,
		Mode:            "logs",
		Bucket:          "test-bucket",
		BreakdownLabels: []string{"service.name"},
	})

	mux := http.NewServeMux()
	api.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown", nil))

	var resp BreakdownResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Labels) != 1 {
		t.Fatalf("expected 1 breakdown label (only service.name), got %d", len(resp.Labels))
	}
	if resp.Labels[0].Name != "service.name" {
		t.Errorf("label name = %q, want %q", resp.Labels[0].Name, "service.name")
	}

	// Now test with both labels
	api2 := NewAPI(APIConfig{
		Registry:        reg,
		Manifest:        m,
		CostCalc:        cc,
		LabelIndex:      li,
		Mode:            "logs",
		Bucket:          "test-bucket",
		BreakdownLabels: []string{"service.name", "level"},
	})
	mux2 := http.NewServeMux()
	api2.Register(mux2)

	rec2 := httptest.NewRecorder()
	mux2.ServeHTTP(rec2, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown", nil))

	var resp2 BreakdownResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp2.Labels) != 2 {
		t.Fatalf("expected 2 breakdown labels, got %d", len(resp2.Labels))
	}
}

func TestConfigEmptyBreakdownLabelsReturnsEmpty(t *testing.T) {
	api := NewAPI(APIConfig{
		Registry:        NewTenantRegistry("node-1"),
		Manifest:        manifest.New("b", "d/"),
		CostCalc:        NewCostCalculator(nil, nil),
		LabelIndex:      cache.NewLabelIndex(),
		BreakdownLabels: nil,
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown", nil))

	var resp BreakdownResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Labels) != 0 {
		t.Errorf("expected 0 labels with empty config, got %d", len(resp.Labels))
	}
}

func TestConfigS3PricePerGBAffectsCost(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("1:1", 10<<30, 20<<30, 100000, "STANDARD")

	// Default AWS pricing
	cc1 := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	api1 := NewAPI(APIConfig{
		Registry: reg,
		CostCalc: cc1,
	})
	mux1 := http.NewServeMux()
	api1.Register(mux1)
	rec1 := httptest.NewRecorder()
	mux1.ServeHTTP(rec1, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/cost", nil))
	var resp1 CostResponse
	json.Unmarshal(rec1.Body.Bytes(), &resp1)

	// Custom pricing (e.g. GCS or discounted)
	cc2 := NewCostCalculator(map[string]float64{"STANDARD": 0.020}, nil)
	api2 := NewAPI(APIConfig{
		Registry: reg,
		CostCalc: cc2,
	})
	mux2 := http.NewServeMux()
	api2.Register(mux2)
	rec2 := httptest.NewRecorder()
	mux2.ServeHTTP(rec2, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/cost", nil))
	var resp2 CostResponse
	json.Unmarshal(rec2.Body.Bytes(), &resp2)

	if resp1.TotalMonthlyUSD <= 0 {
		t.Fatalf("cost should be > 0, got %f", resp1.TotalMonthlyUSD)
	}
	if resp2.TotalMonthlyUSD <= 0 {
		t.Fatalf("cost should be > 0, got %f", resp2.TotalMonthlyUSD)
	}
	if resp1.TotalMonthlyUSD == resp2.TotalMonthlyUSD {
		t.Errorf("different price configs should produce different costs: %f vs %f",
			resp1.TotalMonthlyUSD, resp2.TotalMonthlyUSD)
	}
	if resp1.TotalMonthlyUSD < resp2.TotalMonthlyUSD {
		t.Errorf("higher price ($0.023) should cost more than lower ($0.020): %f vs %f",
			resp1.TotalMonthlyUSD, resp2.TotalMonthlyUSD)
	}
}

func TestConfigMultiClassPricing(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("1:1", 10<<30, 20<<30, 100000, "STANDARD")
	reg.RecordWrite("2:2", 50<<30, 100<<30, 500000, "GLACIER")

	cc := NewCostCalculator(map[string]float64{
		"STANDARD": 0.023,
		"GLACIER":  0.004,
	}, nil)
	api := NewAPI(APIConfig{
		Registry: reg,
		CostCalc: cc,
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/cost", nil))

	var resp CostResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if len(resp.ByClass) != 2 {
		t.Fatalf("expected 2 storage classes, got %d", len(resp.ByClass))
	}

	classMap := make(map[string]float64)
	for _, c := range resp.ByClass {
		classMap[c.Class] = c.CostUSD
	}

	if classMap["STANDARD"] <= 0 {
		t.Error("STANDARD cost should be > 0")
	}
	if classMap["GLACIER"] <= 0 {
		t.Error("GLACIER cost should be > 0")
	}
}

func TestConfigUnknownStorageClassCostsZero(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	cost := cc.MonthlyStorageCost("DEEP_ARCHIVE", 100<<30)
	if cost != 0 {
		t.Errorf("unknown class should cost $0, got %f", cost)
	}
}

func TestConfigBreakdownLabelFilterOverridesConfig(t *testing.T) {
	li := cache.NewLabelIndex()
	li.AddWithValueCounts("service.name", []string{"api"}, map[string]int{"api": 10})
	li.AddWithValueCounts("level", []string{"info"}, map[string]int{"info": 50})

	api := NewAPI(APIConfig{
		Registry:        NewTenantRegistry("node-1"),
		Manifest:        manifest.New("b", "d/"),
		CostCalc:        NewCostCalculator(nil, nil),
		LabelIndex:      li,
		BreakdownLabels: []string{"service.name", "level"},
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown?label=level", nil))

	var resp BreakdownResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if len(resp.Labels) != 1 {
		t.Fatalf("label filter should return 1, got %d", len(resp.Labels))
	}
	if resp.Labels[0].Name != "level" {
		t.Errorf("filtered label = %q, want %q", resp.Labels[0].Name, "level")
	}
}

// --- Regression: ValueCounts weighted breakdown ---

func TestRegressionBreakdownWeightedDistribution(t *testing.T) {
	li := cache.NewLabelIndex()
	li.AddWithValueCounts("service.name", []string{"api", "web", "worker"}, map[string]int{
		"api": 100, "web": 50, "worker": 10,
	})

	m := manifest.New("test-bucket", "data/")
	m.AddFile("dt=2026-05-10/hour=10", manifest.FileInfo{
		Key:  "0/0/logs/dt=2026-05-10/hour=10/part.parquet",
		Size: 1000000,
	})

	api := NewAPI(APIConfig{
		Registry:        NewTenantRegistry("node-1"),
		Manifest:        m,
		CostCalc:        NewCostCalculator(nil, nil),
		LabelIndex:      li,
		BreakdownLabels: []string{"service.name"},
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown", nil))

	var resp BreakdownResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Labels) != 1 {
		t.Fatalf("expected 1 label, got %d", len(resp.Labels))
	}

	values := resp.Labels[0].Values
	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(values))
	}

	byName := make(map[string]BreakdownValue)
	for _, v := range values {
		byName[v.Value] = v
	}

	// "api" has weight 100/160 ≈ 62.5%, "web" 50/160 ≈ 31.25%, "worker" 10/160 ≈ 6.25%
	// They should NOT all be identical (the old bug)
	if byName["api"].SharePct == byName["web"].SharePct && byName["web"].SharePct == byName["worker"].SharePct {
		t.Error("REGRESSION: all values have identical share — weighted distribution is broken")
	}

	if byName["api"].SharePct < byName["web"].SharePct {
		t.Errorf("api (weight=100) should have higher share than web (weight=50): %f vs %f",
			byName["api"].SharePct, byName["web"].SharePct)
	}
	if byName["web"].SharePct < byName["worker"].SharePct {
		t.Errorf("web (weight=50) should have higher share than worker (weight=10): %f vs %f",
			byName["web"].SharePct, byName["worker"].SharePct)
	}

	// Shares should sum to ~100%
	totalShare := byName["api"].SharePct + byName["web"].SharePct + byName["worker"].SharePct
	if math.Abs(totalShare-100.0) > 0.1 {
		t.Errorf("shares should sum to ~100%%, got %f", totalShare)
	}
}

func TestRegressionBreakdownNoNaN(t *testing.T) {
	li := cache.NewLabelIndex()
	li.Add("empty_label", []string{})

	api := NewAPI(APIConfig{
		Registry:        NewTenantRegistry("node-1"),
		Manifest:        manifest.New("b", ""),
		CostCalc:        NewCostCalculator(nil, nil),
		LabelIndex:      li,
		BreakdownLabels: []string{"empty_label"},
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown", nil))

	body := rec.Body.String()
	if strings.Contains(body, "NaN") {
		t.Error("REGRESSION: NaN found in breakdown response")
	}
	if strings.Contains(body, "Inf") {
		t.Error("REGRESSION: Inf found in breakdown response")
	}
}

func TestRegressionBreakdownZeroTotalBytes(t *testing.T) {
	li := cache.NewLabelIndex()
	li.AddWithValueCounts("service.name", []string{"api"}, map[string]int{"api": 10})

	// Empty manifest — 0 total bytes
	api := NewAPI(APIConfig{
		Registry:        NewTenantRegistry("node-1"),
		Manifest:        manifest.New("b", ""),
		CostCalc:        NewCostCalculator(nil, nil),
		LabelIndex:      li,
		BreakdownLabels: []string{"service.name"},
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}

	var resp BreakdownResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	for _, l := range resp.Labels {
		for _, v := range l.Values {
			if v.EstimatedBytes != 0 {
				t.Errorf("with 0 total bytes, estimated should be 0, got %d", v.EstimatedBytes)
			}
			if math.IsNaN(v.SharePct) || math.IsInf(v.SharePct, 0) {
				t.Error("REGRESSION: NaN/Inf in share percentage with zero total bytes")
			}
		}
	}
}

// --- Regression: JSON field collision fix ---

func TestRegressionTenantDetailJSONPartitionList(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("100:1", 10<<20, 20<<20, 50000, "STANDARD")

	m := manifest.New("test-bucket", "data/")
	m.AddFile("dt=2026-05-10/hour=10", manifest.FileInfo{
		Key:  "100/1/logs/dt=2026-05-10/hour=10/part.parquet",
		Size: 1 << 20,
	})

	api := NewAPI(APIConfig{
		Registry: reg,
		Manifest: m,
		CostCalc: NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil),
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/100/1", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}

	// Parse as raw JSON to check field names
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw JSON: %v", err)
	}

	// Must have "partition_list" (not "partitions" conflicting with the int field)
	if _, ok := raw["partition_list"]; !ok {
		t.Error("REGRESSION: 'partition_list' field missing from tenant detail response")
	}

	// "partitions" should be the int count from the embedded TenantEntry
	if pRaw, ok := raw["partitions"]; ok {
		var count int
		if err := json.Unmarshal(pRaw, &count); err != nil {
			t.Errorf("REGRESSION: 'partitions' should be an integer, decode error: %v", err)
		}
	}

	// Full roundtrip decode should work without errors
	var resp TenantDetailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("REGRESSION: TenantDetailResponse decode error: %v", err)
	}
}

// --- Regression: Manifest fallback for read-only mode ---

func TestRegressionEmptyRegistryManifestFallback(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	// Registry is empty — simulates read-only / datagen mode

	m := manifest.New("test-bucket", "data/")
	m.AddFile("dt=2026-05-10/hour=10", manifest.FileInfo{
		Key:  "100/1/logs/dt=2026-05-10/hour=10/part.parquet",
		Size: 5 << 20,
	})
	m.AddFile("dt=2026-05-11/hour=14", manifest.FileInfo{
		Key:  "200/5/logs/dt=2026-05-11/hour=14/part.parquet",
		Size: 3 << 20,
	})

	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	api := NewAPI(APIConfig{
		Registry: reg,
		Manifest: m,
		CostCalc: cc,
		Mode:     "logs",
		Bucket:   "test-bucket",
	})

	mux := http.NewServeMux()
	api.Register(mux)

	t.Run("tenants_fallback", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil))
		var resp TenantsResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)

		if resp.TotalTenants == 0 {
			t.Error("REGRESSION: empty registry should fall back to manifest tenants")
		}
		if resp.TotalBytes == 0 {
			t.Error("total bytes should be > 0 from manifest")
		}
	})

	t.Run("overview_fallback", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/overview", nil))
		var resp OverviewResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)

		if resp.TotalFiles == 0 {
			t.Error("REGRESSION: overview should show manifest file count")
		}
		if resp.TotalBytes == 0 {
			t.Error("REGRESSION: overview should show manifest byte count")
		}
		if resp.TenantCount == 0 {
			t.Error("REGRESSION: overview tenant count should use manifest fallback")
		}
	})

	t.Run("cost_fallback", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/cost", nil))
		var resp CostResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)

		if resp.TotalMonthlyUSD == 0 {
			t.Error("REGRESSION: cost should compute from manifest data when registry empty")
		}
		if len(resp.ByClass) == 0 {
			t.Error("REGRESSION: by_class should have STANDARD fallback")
		}
	})

	t.Run("compression_fallback", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/compression", nil))
		var resp CompressionResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)

		if len(resp.PerTenant) == 0 {
			t.Error("REGRESSION: compression should show tenants from manifest")
		}
	})

	t.Run("tenant_detail_fallback", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/100/1", nil))

		if rec.Code != http.StatusOK {
			t.Errorf("REGRESSION: tenant detail should work via manifest fallback, got %d", rec.Code)
		}
	})
}

// --- Regression: Overview storage_by_class not empty ---

func TestRegressionOverviewStorageByClassFallback(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	m := manifest.New("b", "d/")
	m.AddFile("dt=2026-05-10/hour=10", manifest.FileInfo{
		Key:  "0/0/logs/dt=2026-05-10/hour=10/part.parquet",
		Size: 1 << 20,
	})

	api := NewAPI(APIConfig{
		Registry: reg,
		Manifest: m,
		CostCalc: NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil),
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/overview", nil))

	var resp OverviewResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if len(resp.StorageByClass) == 0 {
		t.Error("REGRESSION: overview storage_by_class should fall back to STANDARD when registry empty")
	}
}

// --- Regression: Cardinality with SchemaRegistry ---

func TestRegressionCardinalityPromotedVsMapType(t *testing.T) {
	li := cache.NewLabelIndex()
	li.Add("service.name", []string{"api", "web"})
	li.Add("custom_field", []string{"val1", "val2"})

	sr := logsSchemaRegistry()

	api := NewAPI(APIConfig{
		Registry:       NewTenantRegistry("node-1"),
		CostCalc:       NewCostCalculator(nil, nil),
		LabelIndex:     li,
		SchemaRegistry: sr,
		BloomColumns:   []string{"service.name", "trace_id"},
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/cardinality/fields", nil))

	var resp CardinalityResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	fieldMap := make(map[string]FieldEntry)
	for _, f := range resp.Fields {
		fieldMap[f.Name] = f
	}

	if f, ok := fieldMap["service.name"]; ok {
		if f.Type != "promoted" {
			t.Errorf("service.name should be promoted, got %q", f.Type)
		}
		if !f.HasBloom {
			t.Error("service.name should have bloom filter")
		}
	}

	if f, ok := fieldMap["custom_field"]; ok {
		if f.Type != "map" {
			t.Errorf("custom_field should be map type, got %q", f.Type)
		}
	}
}

// --- Fuzzy tests ---

func TestFuzzyBreakdownWithRandomWeights(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for iter := 0; iter < 50; iter++ {
		li := cache.NewLabelIndex()

		numValues := rng.Intn(100) + 1
		values := make([]string, numValues)
		counts := make(map[string]int)
		for i := 0; i < numValues; i++ {
			v := "val-" + strconv.Itoa(i)
			values[i] = v
			counts[v] = rng.Intn(1000) + 1
		}
		li.AddWithValueCounts("test_label", values, counts)

		totalBytes := int64(rng.Intn(1<<30)) + 1
		m := manifest.New("b", "d/")
		m.AddFile("dt=2026-05-10/hour=10", manifest.FileInfo{
			Key:  "0/0/logs/dt=2026-05-10/hour=10/part.parquet",
			Size: totalBytes,
		})

		api := NewAPI(APIConfig{
			Registry:        NewTenantRegistry("node-1"),
			Manifest:        m,
			CostCalc:        NewCostCalculator(nil, nil),
			LabelIndex:      li,
			BreakdownLabels: []string{"test_label"},
		})

		mux := http.NewServeMux()
		api.Register(mux)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/breakdown", nil))

		if rec.Code != http.StatusOK {
			t.Fatalf("iter %d: status %d", iter, rec.Code)
		}

		body := rec.Body.String()
		if strings.Contains(body, "NaN") {
			t.Errorf("iter %d: NaN in response", iter)
		}
		if strings.Contains(body, "Inf") {
			t.Errorf("iter %d: Inf in response", iter)
		}

		var resp BreakdownResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("iter %d: decode: %v", iter, err)
		}

		if len(resp.Labels) != 1 {
			t.Fatalf("iter %d: expected 1 label, got %d", iter, len(resp.Labels))
		}

		// All shares must be non-negative and <= 100
		for _, v := range resp.Labels[0].Values {
			if v.SharePct < 0 || v.SharePct > 100.01 {
				t.Errorf("iter %d: share %f out of range for value %q", iter, v.SharePct, v.Value)
			}
			if v.EstimatedBytes < 0 {
				t.Errorf("iter %d: negative estimated bytes for value %q", iter, v.Value)
			}
		}
	}
}

func TestFuzzyIngestionPeriodRange(t *testing.T) {
	m := manifest.New("b", "d/")
	for i := 0; i < 30; i++ {
		dt := time.Now().AddDate(0, 0, -i).Format("2006-01-02")
		partition := "dt=" + dt + "/hour=10"
		m.AddFile(partition, manifest.FileInfo{
			Key:  "0/0/logs/" + partition + "/part.parquet",
			Size: int64(1 << 20),
		})
	}

	api := NewAPI(APIConfig{
		Registry: NewTenantRegistry("node-1"),
		Manifest: m,
		CostCalc: NewCostCalculator(nil, nil),
	})

	mux := http.NewServeMux()
	api.Register(mux)

	combos := []struct{ period, rng string }{
		{"hour", "24h"}, {"hour", "7d"}, {"hour", "30d"},
		{"day", "24h"}, {"day", "7d"}, {"day", "30d"},
		{"month", "24h"}, {"month", "7d"}, {"month", "30d"},
		{"invalid", "invalid"}, {"", ""},
	}

	for _, c := range combos {
		rec := httptest.NewRecorder()
		url := "/lakehouse/api/v1/stats/ingestion?period=" + c.period + "&range=" + c.rng
		mux.ServeHTTP(rec, httptest.NewRequest("GET", url, nil))

		if rec.Code != http.StatusOK {
			t.Errorf("period=%s range=%s: status %d, want 200", c.period, c.rng, rec.Code)
		}

		var resp IngestionResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Errorf("period=%s range=%s: decode error: %v", c.period, c.rng, err)
		}
	}
}

func TestFuzzyTenantDetailBadPaths(t *testing.T) {
	api := NewAPI(APIConfig{
		Registry: NewTenantRegistry("node-1"),
		Manifest: manifest.New("b", "d/"),
		CostCalc: NewCostCalculator(nil, nil),
	})

	mux := http.NewServeMux()
	api.Register(mux)

	badPaths := []string{
		"/lakehouse/api/v1/tenants/",
		"/lakehouse/api/v1/tenants/a/",
		"/lakehouse/api/v1/tenants/a",
	}

	for _, path := range badPaths {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("path %q: expected 404, got %d", path, rec.Code)
		}
	}
}

func TestFuzzyCardinalityEdgeCases(t *testing.T) {
	api := NewAPI(APIConfig{
		Registry:   NewTenantRegistry("node-1"),
		CostCalc:   NewCostCalculator(nil, nil),
		LabelIndex: cache.NewLabelIndex(),
	})

	mux := http.NewServeMux()
	api.Register(mux)

	queries := []string{
		"/lakehouse/api/v1/cardinality/fields?limit=0",
		"/lakehouse/api/v1/cardinality/fields?limit=-1",
		"/lakehouse/api/v1/cardinality/fields?limit=999999",
		"/lakehouse/api/v1/cardinality/fields?limit=abc",
		"/lakehouse/api/v1/cardinality/fields?sort=invalid",
		"/lakehouse/api/v1/cardinality/fields?tenant=nonexistent",
	}

	for _, q := range queries {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", q, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("query %q: expected 200, got %d", q, rec.Code)
		}
		var resp CardinalityResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Errorf("query %q: decode error: %v", q, err)
		}
	}
}

func TestFuzzyAllEndpointsMethodNotAllowed(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

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

	badMethods := []string{"POST", "PUT", "DELETE", "PATCH"}

	for _, ep := range endpoints {
		for _, method := range badMethods {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(method, ep, nil))
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: expected 405, got %d", method, ep, rec.Code)
			}
		}
	}
}

func TestFuzzyAllEndpointsReturnValidJSON(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

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
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", ep, nil))

		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d", ep, rec.Code)
			continue
		}

		var raw json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
			t.Errorf("%s: invalid JSON: %v", ep, err)
		}

		body := rec.Body.String()
		if strings.Contains(body, "NaN") {
			t.Errorf("%s: NaN in response", ep)
		}
		if strings.Contains(body, "Infinity") {
			t.Errorf("%s: Infinity in response", ep)
		}
	}
}

// --- Regression: High cardinality warning ---

func TestRegressionHighCardinalityWarning(t *testing.T) {
	li := cache.NewLabelIndex()
	vals := make([]string, 15000)
	for i := range vals {
		vals[i] = "val-" + strconv.Itoa(i)
	}
	li.Add("high_card_field", vals)
	li.Add("low_card_field", []string{"a", "b"})

	api := NewAPI(APIConfig{
		Registry:   NewTenantRegistry("node-1"),
		CostCalc:   NewCostCalculator(nil, nil),
		LabelIndex: li,
	})

	mux := http.NewServeMux()
	api.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/api/v1/cardinality/fields", nil))

	var resp CardinalityResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	found := false
	for _, w := range resp.HighCardinalityWarning {
		if w == "high_card_field" {
			found = true
		}
	}
	if !found {
		t.Error("high cardinality warning should include 'high_card_field'")
	}

	for _, w := range resp.HighCardinalityWarning {
		if w == "low_card_field" {
			t.Error("low cardinality field should not be in warnings")
		}
	}
}
