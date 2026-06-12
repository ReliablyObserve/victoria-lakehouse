package stats

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// This file raises every stats endpoint to the rigor TestHandleCompaction
// already enforces: HTTP 200 + Content-Type + the Cache-Control contract +
// JSON-decodes-into-its-struct + key-field values for seeded data, plus the
// nil-dependency / empty-data / wrong-method edges. The handlers are listed in
// (a *API).Register; this file walks them one by one.

// ---- Endpoint inventory (single source of truth for the table tests) ----

// statsEndpoint describes one registered route. wrongMethodChecked is true for
// the handlers that explicitly reject non-GET with 405 (all of them today
// except compaction, which has no method guard — that asymmetry is asserted
// below so a future change to either side is caught).
type statsEndpoint struct {
	name               string
	path               string // a concrete request path (detail routes need IDs)
	setsNoStore        bool   // handler sets Cache-Control: no-store
	wrongMethodChecked bool   // handler returns 405 for POST
}

func allEndpoints() []statsEndpoint {
	return []statsEndpoint{
		{"tenants", "/lakehouse/api/v1/tenants", true, true},
		{"tenants_policy", "/lakehouse/api/v1/tenants/policy", true, true},
		{"tenant_detail", "/lakehouse/api/v1/tenants/100/1", true, true},
		{"overview", "/lakehouse/api/v1/stats/overview", true, true},
		{"ingestion", "/lakehouse/api/v1/stats/ingestion", true, true},
		{"cost", "/lakehouse/api/v1/stats/cost", true, true},
		{"compression", "/lakehouse/api/v1/stats/compression", true, true},
		{"cardinality", "/lakehouse/api/v1/cardinality/fields", true, true},
		{"breakdown", "/lakehouse/api/v1/stats/breakdown", true, true},
		{"compaction", "/lakehouse/api/v1/stats/compaction", true, false},
	}
}

// TestEndpoints_CacheControlContract locks which handlers emit a no-store
// Cache-Control. Every stats/tenant response is no-store now: writeJSON sets it
// (so deploys/browsers never surface stale cached stats) and /stats/compaction
// sets it directly. Pinning this means a handler can't silently start caching a
// live stat.
func TestEndpoints_CacheControlContract(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	for _, ep := range allEndpoints() {
		t.Run(ep.name, func(t *testing.T) {
			rec := doGet(t, api, ep.path)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
			}
			cc := rec.Header().Get("Cache-Control")
			hasNoStore := strings.Contains(cc, "no-store")
			if hasNoStore != ep.setsNoStore {
				t.Errorf("Cache-Control = %q (no-store=%v), want no-store=%v",
					cc, hasNoStore, ep.setsNoStore)
			}
		})
	}
}

// TestEndpoints_ContentTypeJSON confirms every endpoint advertises JSON,
// including the compaction endpoint (which sets it on its own, not via
// writeJSON) and the detail/policy routes.
func TestEndpoints_ContentTypeJSON(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	for _, ep := range allEndpoints() {
		t.Run(ep.name, func(t *testing.T) {
			rec := doGet(t, api, ep.path)
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
		})
	}
}

// TestEndpoints_WrongMethod asserts the 405-on-POST contract per endpoint.
// Every GET handler guards the method except /stats/compaction, which has no
// guard and therefore serves its body regardless of method — that exact
// asymmetry is what we lock here.
func TestEndpoints_WrongMethod(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	for _, ep := range allEndpoints() {
		t.Run(ep.name, func(t *testing.T) {
			rec := doMethod(t, api, http.MethodPost, ep.path)
			if ep.wrongMethodChecked {
				if rec.Code != http.StatusMethodNotAllowed {
					t.Errorf("POST %s: status = %d, want 405", ep.path, rec.Code)
				}
			} else {
				// compaction: no method guard, still answers 200.
				if rec.Code != http.StatusOK {
					t.Errorf("POST %s: status = %d, want 200 (no method guard)", ep.path, rec.Code)
				}
			}
		})
	}
}

// TestEndpoints_NilManifest_NoPanic drives every endpoint with a non-nil but
// empty Registry and a nil Manifest. Production always constructs a Registry,
// so "empty Registry" is the real empty-data shape; the nil Manifest exercises
// every `a.cfg.Manifest != nil` guard. Nothing may panic, and each response
// must decode into its struct.
func TestEndpoints_NilManifest_NoPanic(t *testing.T) {
	reg := NewTenantRegistry("node-nilmanifest")
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	api := NewAPI(APIConfig{
		Registry: reg, // empty, non-nil
		Manifest: nil, // exercises the nil-manifest guards
		CostCalc: cc,
		Mode:     "logs",
		Bucket:   "empty-bucket",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	decoders := map[string]func([]byte) error{
		"tenants":        func(b []byte) error { var v TenantsResponse; return json.Unmarshal(b, &v) },
		"tenants_policy": func(b []byte) error { var v TenantPolicyListResponse; return json.Unmarshal(b, &v) },
		"tenant_detail":  func(b []byte) error { var v TenantDetailResponse; return json.Unmarshal(b, &v) },
		"overview":       func(b []byte) error { var v OverviewResponse; return json.Unmarshal(b, &v) },
		"ingestion":      func(b []byte) error { var v IngestionResponse; return json.Unmarshal(b, &v) },
		"cost":           func(b []byte) error { var v CostResponse; return json.Unmarshal(b, &v) },
		"compression":    func(b []byte) error { var v CompressionResponse; return json.Unmarshal(b, &v) },
		"cardinality":    func(b []byte) error { var v CardinalityResponse; return json.Unmarshal(b, &v) },
		"breakdown":      func(b []byte) error { var v BreakdownResponse; return json.Unmarshal(b, &v) },
		"compaction":     func(b []byte) error { var v manifest.CompactionStats; return json.Unmarshal(b, &v) },
	}

	for _, ep := range allEndpoints() {
		t.Run(ep.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("PANIC on %s with nil manifest: %v", ep.path, r)
				}
			}()
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, ep.path, nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
			}
			if dec := decoders[ep.name]; dec != nil {
				if err := dec(rec.Body.Bytes()); err != nil {
					t.Errorf("decode %s: %v; body=%s", ep.name, err, rec.Body.String())
				}
			}
		})
	}
}

// TestOverview_EmptyEverything proves an all-empty deployment (empty registry,
// nil manifest) returns a well-formed zero overview rather than erroring.
func TestOverview_EmptyEverything(t *testing.T) {
	api := NewAPI(APIConfig{
		Registry: NewTenantRegistry("n"),
		CostCalc: NewCostCalculator(nil, nil),
		Mode:     "traces",
		Bucket:   "b",
	})
	rec := doGet(t, api, "/lakehouse/api/v1/stats/overview")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp OverviewResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalFiles != 0 || resp.TotalBytes != 0 || resp.TenantCount != 0 {
		t.Errorf("empty overview not zero: files=%d bytes=%d tenants=%d",
			resp.TotalFiles, resp.TotalBytes, resp.TenantCount)
	}
	if resp.Mode != "traces" || resp.Bucket != "b" {
		t.Errorf("mode/bucket = %q/%q, want traces/b", resp.Mode, resp.Bucket)
	}
	// storage_by_class must serialize as [] not null (UI iterates it).
	if resp.StorageByClass == nil {
		t.Error("storage_by_class is nil; want empty slice")
	}
}

// ---- /stats/breakdown: value-level estimation math ----

// TestBreakdown_ValueLevelEstimation seeds a single label with three values of
// known relative weight and asserts the proportional byte/file split + share
// percentages the handler computes from the manifest totals. This is the
// arithmetic the regression suite samples randomly; here we pin exact numbers.
func TestBreakdown_ValueLevelEstimation(t *testing.T) {
	m := manifest.New("bucket", "logs/")
	// Two files → 1000 total bytes, 10 total files-equivalents is not how the
	// handler counts (it uses TotalFiles()=2), so we keep bytes round.
	m.AddFile("dt=2026-06-01/hour=00", manifest.FileInfo{
		Key:  "logs/dt=2026-06-01/hour=00/a.parquet",
		Size: 600,
	})
	m.AddFile("dt=2026-06-01/hour=01", manifest.FileInfo{
		Key:  "logs/dt=2026-06-01/hour=01/b.parquet",
		Size: 400,
	})

	li := cache.NewLabelIndex()
	// service: prod weight 6, staging weight 3, dev weight 1 (total 10).
	li.AddWithValueCounts("service", []string{"prod", "staging", "dev"},
		map[string]int{"prod": 6, "staging": 3, "dev": 1})

	api := NewAPI(APIConfig{
		Registry:        NewTenantRegistry("n"),
		Manifest:        m,
		LabelIndex:      li,
		BreakdownLabels: []string{"service"},
		Mode:            "logs",
		Bucket:          "bucket",
	})

	rec := doGet(t, api, "/lakehouse/api/v1/stats/breakdown")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp BreakdownResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Labels) != 1 {
		t.Fatalf("labels = %d, want 1", len(resp.Labels))
	}
	bl := resp.Labels[0]
	if bl.Name != "service" {
		t.Errorf("name = %q, want service", bl.Name)
	}
	if bl.Cardinality != 3 {
		t.Errorf("cardinality = %d, want 3", bl.Cardinality)
	}

	byVal := map[string]BreakdownValue{}
	var shareSum float64
	for _, v := range bl.Values {
		byVal[v.Value] = v
		shareSum += v.SharePct
	}
	if len(byVal) != 3 {
		t.Fatalf("values = %d, want 3", len(byVal))
	}
	// total bytes = 1000; prod share = 6/10 → 600 bytes, staging 300, dev 100.
	if got := byVal["prod"].EstimatedBytes; got != 600 {
		t.Errorf("prod estimated_bytes = %d, want 600", got)
	}
	if got := byVal["staging"].EstimatedBytes; got != 300 {
		t.Errorf("staging estimated_bytes = %d, want 300", got)
	}
	if got := byVal["dev"].EstimatedBytes; got != 100 {
		t.Errorf("dev estimated_bytes = %d, want 100", got)
	}
	if got := byVal["prod"].SharePct; got < 59.9 || got > 60.1 {
		t.Errorf("prod share_pct = %f, want ~60", got)
	}
	// Shares must sum to ~100 (no NaN, no drift).
	if shareSum < 99.0 || shareSum > 101.0 {
		t.Errorf("share_pct sum = %f, want ~100", shareSum)
	}
}

// TestBreakdown_PromotedTypeFromSchema confirms the breakdown label inherits
// type=promoted when the schema registry marks the field promoted, and stays
// map otherwise.
func TestBreakdown_PromotedTypeFromSchema(t *testing.T) {
	m := manifest.New("bucket", "logs/")
	m.AddFile("dt=2026-06-01/hour=00", manifest.FileInfo{
		Key:  "logs/dt=2026-06-01/hour=00/a.parquet",
		Size: 100,
	})
	li := cache.NewLabelIndex()
	li.AddWithValueCounts("service.name", []string{"api"}, map[string]int{"api": 1})

	sr := schema.NewRegistry(schema.LogsProfile)

	api := NewAPI(APIConfig{
		Registry:        NewTenantRegistry("n"),
		Manifest:        m,
		LabelIndex:      li,
		SchemaRegistry:  sr,
		BreakdownLabels: []string{"service.name"},
		Mode:            "logs",
		Bucket:          "bucket",
	})
	rec := doGet(t, api, "/lakehouse/api/v1/stats/breakdown?label=service.name")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp BreakdownResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Labels) != 1 {
		t.Fatalf("labels = %d, want 1", len(resp.Labels))
	}
	// ServiceName is a promoted column in the logs profile, so type=promoted.
	if resp.Labels[0].Type != "promoted" {
		t.Errorf("type = %q, want promoted (service.name is a promoted logs column)", resp.Labels[0].Type)
	}
}

// ---- /stats/ingestion: seeded buckets with deterministic dates ----

// TestIngestion_BucketsFromSeededPartitions seeds two files dated today and
// yesterday (inside the default 7d window) and asserts day-bucket totals match
// the seeded bytes/files exactly.
func TestIngestion_BucketsFromSeededPartitions(t *testing.T) {
	now := time.Now().UTC()
	today := now.Format("2006-01-02")
	yesterday := now.Add(-24 * time.Hour).Format("2006-01-02")

	m := manifest.New("bucket", "logs/")
	m.AddFile("dt="+today+"/hour=10", manifest.FileInfo{
		Key:      "logs/dt=" + today + "/hour=10/a.parquet",
		Size:     1000,
		RowCount: 100,
	})
	m.AddFile("dt="+yesterday+"/hour=09", manifest.FileInfo{
		Key:      "logs/dt=" + yesterday + "/hour=09/b.parquet",
		Size:     2000,
		RowCount: 200,
	})

	api := NewAPI(APIConfig{
		Registry: NewTenantRegistry("n"),
		Manifest: m,
		Mode:     "logs",
		Bucket:   "bucket",
	})

	rec := doGet(t, api, "/lakehouse/api/v1/stats/ingestion?period=day&range=7d")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp IngestionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Period != "day" || resp.Range != "7d" {
		t.Errorf("period/range = %q/%q, want day/7d", resp.Period, resp.Range)
	}
	if resp.TotalIn != 3000 {
		t.Errorf("total_bytes_ingested = %d, want 3000", resp.TotalIn)
	}
	if resp.TotalOut != 2 {
		t.Errorf("total_files_written = %d, want 2", resp.TotalOut)
	}
	if len(resp.Buckets) < 2 {
		t.Fatalf("buckets = %d, want >= 2", len(resp.Buckets))
	}
	byDay := map[string]IngestionBucket{}
	for _, b := range resp.Buckets {
		byDay[b.Timestamp] = b
	}
	if b, ok := byDay[today]; !ok {
		t.Errorf("missing today bucket %q", today)
	} else if b.Bytes != 1000 || b.Files != 1 {
		t.Errorf("today bucket = {bytes:%d files:%d}, want {1000,1}", b.Bytes, b.Files)
	}
	if b, ok := byDay[yesterday]; !ok {
		t.Errorf("missing yesterday bucket %q", yesterday)
	} else if b.Bytes != 2000 || b.Files != 1 {
		t.Errorf("yesterday bucket = {bytes:%d files:%d}, want {2000,1}", b.Bytes, b.Files)
	}
}

// TestIngestion_EmptyManifest_EmptyBucketsSlice asserts buckets serializes as
// [] (never null) when there is no data — the UI iterates the array.
func TestIngestion_EmptyManifest_EmptyBucketsSlice(t *testing.T) {
	api := NewAPI(APIConfig{Registry: NewTenantRegistry("n"), Mode: "logs"})
	rec := doGet(t, api, "/lakehouse/api/v1/stats/ingestion")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// Confirm the raw JSON has "buckets":[] not "buckets":null.
	if !strings.Contains(rec.Body.String(), `"buckets":[]`) {
		t.Errorf("buckets not an empty array: %s", rec.Body.String())
	}
}

// ---- /tenants/policy and /tenants/<detail> policy block ----

// TestTenantPolicy_EffectiveValuesAndOrgID seeds an override and asserts the
// resolved EffectiveConfig is rendered with the right retention/limits and the
// resolver-supplied org_id, both in the list endpoint and inside the
// per-tenant detail's policy block.
func TestTenantPolicy_EffectiveValuesAndOrgID(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	if err := resolver.AddAlias("payments", tenant.TenantID{AccountID: 7, ProjectID: 2}); err != nil {
		t.Fatalf("alias: %v", err)
	}
	policy, err := tenant.NewPolicyRegistry(map[string]config.TenantOverride{
		"7:2": {
			Retention:   config.TenantRetentionOverride{Keep: "30d"},
			Cardinality: config.TenantCardinalityOverride{MaxFields: 2500},
			Ingest:      config.TenantIngestOverride{MaxBytesPerSec: 4 * 1024 * 1024},
		},
	}, resolver)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	reg := NewTenantRegistry("n")
	reg.RecordWrite("7:2", 100, 200, 10, "STANDARD")

	api := NewAPI(APIConfig{
		Registry: reg, Resolver: resolver, Policy: policy,
		Manifest: manifest.New("b", "logs/"),
		CostCalc: NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil),
		Mode:     "logs", Bucket: "b",
	})

	t.Run("list", func(t *testing.T) {
		rec := doGet(t, api, "/lakehouse/api/v1/tenants/policy")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
		var resp TenantPolicyListResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		var found *TenantPolicyEntry
		for i := range resp.Entries {
			if resp.Entries[i].AccountID == 7 && resp.Entries[i].ProjectID == 2 {
				found = &resp.Entries[i]
			}
		}
		if found == nil {
			t.Fatalf("7:2 not in policy entries: %+v", resp.Entries)
		}
		// Retention serializes as the parsed time.Duration's String() — "30d"
		// normalizes to "720h0m0s". Pinning the canonical form documents the
		// actual on-the-wire contract.
		if found.Retention != "720h0m0s" {
			t.Errorf("retention = %q, want 720h0m0s (30d normalized)", found.Retention)
		}
		if found.MaxFields != 2500 {
			t.Errorf("max_fields = %d, want 2500", found.MaxFields)
		}
		if found.MaxBytesPerSec != 4*1024*1024 {
			t.Errorf("max_bytes_per_sec = %d, want %d", found.MaxBytesPerSec, 4*1024*1024)
		}
		if found.OrgID != "payments" {
			t.Errorf("org_id = %q, want payments", found.OrgID)
		}
	})

	t.Run("detail_policy_block", func(t *testing.T) {
		rec := doGet(t, api, "/lakehouse/api/v1/tenants/7/2")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
		}
		var resp TenantDetailResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.Policy == nil {
			t.Fatal("detail policy block is nil; want populated override")
		}
		if resp.Policy.Retention != "720h0m0s" || resp.Policy.MaxFields != 2500 {
			t.Errorf("detail policy = {retention:%q max_fields:%d}, want {720h0m0s,2500}",
				resp.Policy.Retention, resp.Policy.MaxFields)
		}
		if resp.Policy.OrgID != "payments" {
			t.Errorf("detail policy org_id = %q, want payments", resp.Policy.OrgID)
		}
	})
}

// TestTenantPolicy_NilPolicy_EmptyList asserts the list endpoint returns a
// well-formed empty response (no panic) when no policy registry is wired.
func TestTenantPolicy_NilPolicy_EmptyList(t *testing.T) {
	api := NewAPI(APIConfig{Registry: NewTenantRegistry("n"), Mode: "logs"})
	rec := doGet(t, api, "/lakehouse/api/v1/tenants/policy")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp TenantPolicyListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("entries = %d, want 0 (no policy registry)", len(resp.Entries))
	}
}

// ---- /tenants/<detail>: histogram + alias-not-found ----

// TestTenantDetail_FileSizeHistogram seeds files across several size buckets
// for one tenant and asserts the histogram counts land in the right buckets.
func TestTenantDetail_FileSizeHistogram(t *testing.T) {
	m := manifest.New("bucket", "logs/")
	// tenant 5/9 — one file per histogram bucket boundary.
	sizes := []int64{
		512 << 10, // <1MB
		5 << 20,   // 1-10MB
		20 << 20,  // 10-50MB
		100 << 20, // 50-128MB
		200 << 20, // >128MB
	}
	for i, sz := range sizes {
		m.AddFile(fmt.Sprintf("dt=2026-06-01/hour=%02d", i), manifest.FileInfo{
			Key:      fmt.Sprintf("5/9/logs/dt=2026-06-01/hour=%02d/f.parquet", i),
			Size:     sz,
			RowCount: 10,
		})
	}

	api := NewAPI(APIConfig{
		Registry: NewTenantRegistry("n"),
		Manifest: m,
		CostCalc: NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil),
		Mode:     "logs", Bucket: "bucket",
	})
	rec := doGet(t, api, "/lakehouse/api/v1/tenants/5/9")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp TenantDetailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalFiles != 5 {
		t.Fatalf("total_files = %d, want 5", resp.TotalFiles)
	}
	if resp.FileSizeHistogram == nil {
		t.Fatal("file_size_histogram is nil")
	}
	if len(resp.FileSizeHistogram.Counts) != 5 {
		t.Fatalf("histogram counts len = %d, want 5", len(resp.FileSizeHistogram.Counts))
	}
	for i, c := range resp.FileSizeHistogram.Counts {
		if c != 1 {
			t.Errorf("histogram bucket %d (%s) = %d, want 1",
				i, resp.FileSizeHistogram.Buckets[i], c)
		}
	}
}

// TestTenantDetail_UnknownAliasReturns404 covers the alias-resolution branch:
// a non-numeric single-segment path with a resolver that doesn't know it must
// 404 (distinct from the numeric-IDs path, which synthesizes an empty tenant).
func TestTenantDetail_UnknownAliasReturns404(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	api := NewAPI(APIConfig{
		Registry: NewTenantRegistry("n"),
		Resolver: resolver,
		Mode:     "logs",
	})
	rec := doGet(t, api, "/lakehouse/api/v1/tenants/no-such-alias")
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for unknown alias", rec.Code)
	}
}

// ---- /cardinality/fields: bloom flag + high-card warning + nil index ----

// TestCardinality_BloomAndWarningAndPromoted seeds a high-cardinality field, a
// bloom-backed field, and a promoted field, then asserts the per-field flags,
// the promoted/map tallies, the threshold, and the high-cardinality warning
// list all reflect the seeded data.
func TestCardinality_BloomAndWarningAndPromoted(t *testing.T) {
	li := cache.NewLabelIndex()
	// trace_id: 20000 distinct → above the 10000 warning threshold.
	hi := make([]string, 0, 20000)
	for i := 0; i < 20000; i++ {
		hi = append(hi, fmt.Sprintf("t%d", i))
	}
	li.Add("trace_id", hi)
	li.Add("service.name", []string{"api", "web"}) // bloom-backed + promoted
	li.Add("region", []string{"us", "eu"})         // plain map field

	sr := schema.NewRegistry(schema.LogsProfile)

	api := NewAPI(APIConfig{
		Registry:       NewTenantRegistry("n"),
		LabelIndex:     li,
		SchemaRegistry: sr,
		BloomColumns:   []string{"service.name"},
		Mode:           "logs",
	})
	rec := doGet(t, api, "/lakehouse/api/v1/cardinality/fields")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp CardinalityResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.CardinalityThreshold != 10000 {
		t.Errorf("threshold = %d, want 10000", resp.CardinalityThreshold)
	}
	if resp.TotalFields != 3 {
		t.Errorf("total_fields = %d, want 3", resp.TotalFields)
	}
	// trace_id must appear in the high-cardinality warning list.
	var warned bool
	for _, w := range resp.HighCardinalityWarning {
		if w == "trace_id" {
			warned = true
		}
	}
	if !warned {
		t.Errorf("trace_id not in high_cardinality_warning: %v", resp.HighCardinalityWarning)
	}

	byName := map[string]FieldEntry{}
	for _, f := range resp.Fields {
		byName[f.Name] = f
	}
	if !byName["service.name"].HasBloom {
		t.Error("service.name has_bloom = false, want true")
	}
	if byName["region"].HasBloom {
		t.Error("region has_bloom = true, want false")
	}
	if byName["service.name"].Type != "promoted" {
		t.Errorf("service.name type = %q, want promoted", byName["service.name"].Type)
	}
	if byName["region"].Type != "map" {
		t.Errorf("region type = %q, want map", byName["region"].Type)
	}
	// promoted + map tallies must cover every field exactly once.
	if resp.TotalPromoted+resp.TotalMap != resp.TotalFields {
		t.Errorf("promoted(%d)+map(%d) != total(%d)", resp.TotalPromoted, resp.TotalMap, resp.TotalFields)
	}
}

// TestCardinality_NilIndex_EmptyResult asserts the cardinality endpoint copes
// with a nil LabelIndex (returns zero fields, no panic).
func TestCardinality_NilIndex_EmptyResult(t *testing.T) {
	api := NewAPI(APIConfig{Registry: NewTenantRegistry("n"), Mode: "logs"})
	rec := doGet(t, api, "/lakehouse/api/v1/cardinality/fields")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp CardinalityResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalFields != 0 {
		t.Errorf("total_fields = %d, want 0 (nil index)", resp.TotalFields)
	}
}

// ---- /stats/breakdown nil-LabelIndex regression (product bug fixed) ----

// TestBreakdown_NilLabelIndex_NoPanic is the regression for the nil-pointer
// crash in handleBreakdown: when BreakdownLabels is configured but the
// LabelIndex is nil, the handler used to call (*cache.LabelIndex)(nil).
// GetLabelInfo and panic the whole request. It must now return 200 with a
// well-formed label carrying empty values.
func TestBreakdown_NilLabelIndex_NoPanic(t *testing.T) {
	api := NewAPI(APIConfig{
		Registry:        NewTenantRegistry("n"),
		LabelIndex:      nil, // configured labels but no index
		BreakdownLabels: []string{"service", "level"},
		Mode:            "logs",
	})
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PANIC: handleBreakdown crashed on nil LabelIndex: %v", r)
		}
	}()
	rec := doGet(t, api, "/lakehouse/api/v1/stats/breakdown")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp BreakdownResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Labels) != 2 {
		t.Fatalf("labels = %d, want 2 (one per configured label)", len(resp.Labels))
	}
	for _, bl := range resp.Labels {
		if len(bl.Values) != 0 {
			t.Errorf("label %q has %d values, want 0 (no index)", bl.Name, len(bl.Values))
		}
	}
}

// TestBreakdown_TenantGroupBy_NilRegistry asserts the tenant breakdown facet is
// safe when the registry is nil (the one handler path that nil-guards Registry).
func TestBreakdown_TenantGroupBy_NilRegistry(t *testing.T) {
	api := NewAPI(APIConfig{Mode: "logs"}) // Registry nil
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PANIC: tenant breakdown crashed on nil registry: %v", r)
		}
	}()
	rec := doGet(t, api, "/lakehouse/api/v1/stats/breakdown?group_by=tenant")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp BreakdownResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Labels) != 1 || resp.Labels[0].Name != "tenant" {
		t.Fatalf("expected single tenant label, got %+v", resp.Labels)
	}
	if resp.Labels[0].Cardinality != 0 {
		t.Errorf("cardinality = %d, want 0 (nil registry)", resp.Labels[0].Cardinality)
	}
}
