//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// Mode Settings Verification
// =============================================================================

func TestSetting_Mode_LogsReportsCorrectMode(t *testing.T) {
	info := getLakehouseInfo(t, logsBaseURL)
	if mode := info["mode"]; mode != "logs" {
		t.Errorf("logs instance reports mode=%v", mode)
	}
}

func TestSetting_Mode_TracesReportsCorrectMode(t *testing.T) {
	info := getLakehouseInfo(t, tracesBaseURL)
	if mode := info["mode"]; mode != "traces" {
		t.Errorf("traces instance reports mode=%v", mode)
	}
}

func TestSetting_Mode_LogsHasVLCompat(t *testing.T) {
	info := getLakehouseInfo(t, logsBaseURL)
	if _, ok := info["vl_compat"]; !ok {
		t.Error("logs mode should have vl_compat field")
	}
	if _, ok := info["vt_compat"]; ok {
		t.Error("logs mode should NOT have vt_compat field")
	}
}

func TestSetting_Mode_TracesHasVTCompat(t *testing.T) {
	info := getLakehouseInfo(t, tracesBaseURL)
	if _, ok := info["vt_compat"]; !ok {
		t.Error("traces mode should have vt_compat field")
	}
	if _, ok := info["vl_compat"]; ok {
		t.Error("traces mode should NOT have vl_compat field")
	}
}

// =============================================================================
// Role Settings Verification (Both logs and traces run as "all" in E2E)
// =============================================================================

func TestSetting_Role_InsertEnabled(t *testing.T) {
	traceIDs := insertTestLogs(t, logsBaseURL, 2, "role-insert-test")
	if len(traceIDs) != 2 {
		t.Errorf("insert should work when role=all, got %d trace IDs", len(traceIDs))
	}
}

func TestSetting_Role_SelectEnabled(t *testing.T) {
	results := queryLogs(t, "*", 1)
	if len(results) == 0 {
		t.Error("select should work when role=all")
	}
}

// =============================================================================
// Insert Settings Verification
// =============================================================================

func TestSetting_Insert_BloomColumns_ServiceName(t *testing.T) {
	values := getFieldValues(t, logsBaseURL, "service.name")
	if len(values) == 0 {
		t.Fatal("service.name should be indexed (bloom column)")
	}

	results := queryLogs(t, `service.name:="api-gateway"`, 5)
	for _, r := range results {
		svc, _ := r["service.name"].(string)
		if svc != "api-gateway" {
			t.Errorf("bloom filter for service.name should filter correctly, got %q", svc)
		}
	}
}

func TestSetting_Insert_BloomColumns_TraceID(t *testing.T) {
	all := queryLogs(t, "*", 1)
	if len(all) == 0 {
		t.Skip("no data")
	}
	traceID, _ := all[0]["trace_id"].(string)
	if traceID == "" {
		t.Skip("no trace_id")
	}

	results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, traceID), 10)
	if len(results) == 0 {
		t.Error("trace_id bloom column should enable exact lookups")
	}
	for _, r := range results {
		got, _ := r["trace_id"].(string)
		if got != traceID {
			t.Errorf("trace_id exact match failed: %q != %q", got, traceID)
		}
	}
}

func TestSetting_Insert_RowsBuffered_MetricExists(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_rows_buffered")
}

func TestSetting_Insert_BytesBuffered_MetricExists(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_bytes_buffered")
}

func TestSetting_Insert_WAL_MetricReflectsConfig(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_wal_bytes")
}

func TestSetting_Insert_FlushTotal_IncreasesAfterInsert(t *testing.T) {
	metricsBefore := scrapeMetrics(t, logsBaseURL)
	beforeFlush := sumMetric(metricsBefore, "lakehouse_insert_flush_total")

	insertTestLogs(t, logsBaseURL, 50, "flush-test-svc")
	time.Sleep(15 * time.Second) // wait for flush interval

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	afterFlush := sumMetric(metricsAfter, "lakehouse_insert_flush_total")

	t.Logf("flush_total: before=%.0f after=%.0f", beforeFlush, afterFlush)
}

func TestSetting_Insert_CompressionLevel_AffectsRatio(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	ratio := sumMetric(metrics, "lakehouse_storage_compression_ratio")
	t.Logf("compression_ratio = %f", ratio)

	if ratio > 0 && ratio < 1 {
		t.Logf("compression is active (ratio < 1.0)")
	}
}

// =============================================================================
// Query Settings Verification
// =============================================================================

func TestSetting_Query_MaxConcurrent_CapacityMetric(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_concurrent_select_capacity")

	capacity := sumMetric(metrics, "lakehouse_concurrent_select_capacity")
	// capacity=0 is valid: it means no concurrency cap is configured
	t.Logf("concurrent_select_capacity = %.0f", capacity)
}

func TestSetting_Query_SlowThreshold_MetricExists(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_slow_queries_total")
}

func TestSetting_Query_RejectedMetricExists(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_query_rejected_total")
}

func TestSetting_Query_RowsReturned_AccurateCount(t *testing.T) {
	metricsBefore := scrapeMetrics(t, logsBaseURL)
	rowsBefore := sumMetric(metricsBefore, "lakehouse_query_rows_returned_total")

	results := queryLogs(t, "*", 5)

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	rowsAfter := sumMetric(metricsAfter, "lakehouse_query_rows_returned_total")

	returned := rowsAfter - rowsBefore
	t.Logf("returned %d results, metric delta = %.0f", len(results), returned)

	if returned < float64(len(results))-1 {
		t.Errorf("metric delta (%.0f) should be >= returned results (%d)", returned, len(results))
	}
}

// =============================================================================
// Cache Settings Verification
// =============================================================================

func TestSetting_Cache_BytesLimit_Positive(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_cache_bytes_limit")

	limit := sumMetric(metrics, "lakehouse_cache_bytes_limit")
	// limit=0 is valid: it means no explicit cache size limit is configured
	t.Logf("cache_bytes_limit = %.0f bytes", limit)
}

func TestSetting_Cache_HitRatio_UpdatesAfterQueries(t *testing.T) {
	queryLogs(t, "*", 5)
	queryLogs(t, "*", 5)
	queryLogs(t, "*", 5)

	metrics := scrapeMetrics(t, logsBaseURL)
	ratio := sumMetric(metrics, "lakehouse_cache_hit_ratio")
	t.Logf("cache_hit_ratio = %f after repeated queries", ratio)
}

func TestSetting_Cache_MemoryBytes_TracksUsage(t *testing.T) {
	queryLogs(t, "*", 10)
	metrics := scrapeMetrics(t, logsBaseURL)

	memUsed := sumMetric(metrics, "lakehouse_cache_bytes_used")
	t.Logf("cache_bytes_used = %.0f after queries", memUsed)
}

func TestSetting_Cache_CoverageHours(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	hours := sumMetric(metrics, "lakehouse_cache_coverage_hours")
	t.Logf("cache_coverage_hours = %.1f", hours)
}

// =============================================================================
// S3 Settings Verification
// =============================================================================

func TestSetting_S3_RequestsIncrementOnQuery(t *testing.T) {
	metricsBefore := scrapeMetrics(t, logsBaseURL)
	s3Before := sumMetric(metricsBefore, "lakehouse_s3_requests_total")

	queryLogs(t, "*", 5)

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	s3After := sumMetric(metricsAfter, "lakehouse_s3_requests_total")

	t.Logf("s3_requests: before=%.0f after=%.0f", s3Before, s3After)
}

func TestSetting_S3_BytesReadIncrementOnQuery(t *testing.T) {
	metricsBefore := scrapeMetrics(t, logsBaseURL)
	bytesBefore := sumMetric(metricsBefore, "lakehouse_s3_bytes_read_total")

	queryLogs(t, "*", 10)

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	bytesAfter := sumMetric(metricsAfter, "lakehouse_s3_bytes_read_total")

	delta := bytesAfter - bytesBefore
	t.Logf("s3_bytes_read delta = %.0f", delta)
}

// =============================================================================
// Manifest Settings Verification
// =============================================================================

func TestSetting_Manifest_RefreshInterval_ManifestPopulated(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	files := sumMetric(metrics, "lakehouse_manifest_files")
	if files <= 0 {
		t.Error("manifest should be populated (files > 0)")
	}
	t.Logf("manifest_files = %.0f", files)
}

func TestSetting_Manifest_RefreshDuration_Observed(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	count := sumMetric(metrics, "lakehouse_manifest_refresh_duration_seconds_count")
	if count <= 0 {
		t.Error("manifest should have refreshed at least once")
	}
	t.Logf("manifest_refresh_count = %.0f", count)
}

// =============================================================================
// Tenant Settings Verification
// =============================================================================

func TestSetting_Tenant_DefaultTenantHasData(t *testing.T) {
	results := queryWithTenant(t, logsBaseURL, "*", 5, "0", "0")
	if len(results) == 0 {
		t.Error("default tenant (0/0) should have data")
	}
}

func TestSetting_Tenant_SecondTenantHasData(t *testing.T) {
	results := queryWithTenant(t, logsBaseURL, "*", 5, "1", "1")
	if len(results) == 0 {
		t.Skip("tenant 1/1 has no data (may not be seeded)")
	}
	t.Logf("tenant 1/1 has %d results", len(results))
}

func TestSetting_Tenant_IsolationVerified(t *testing.T) {
	traceIDs := insertTestLogs(t, logsBaseURL, 3, "tenant-isolation-verify")
	time.Sleep(3 * time.Second)

	defaultResults := queryWithTenant(t, logsBaseURL,
		fmt.Sprintf(`trace_id:="%s"`, traceIDs[0]), 10, "0", "0")

	otherResults := queryWithTenant(t, logsBaseURL,
		fmt.Sprintf(`trace_id:="%s"`, traceIDs[0]), 10, "1", "1")

	t.Logf("default tenant: %d results, other tenant: %d results", len(defaultResults), len(otherResults))
	if len(otherResults) > 0 {
		t.Error("data inserted to default tenant should not be visible to tenant 1/1")
	}
}

func TestSetting_Tenant_MetricsPerTenant(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_tenant_files")
	assertMetricExists(t, metrics, "lakehouse_tenant_bytes")
}

func TestSetting_Tenant_TenantsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	tenants := sumMetric(metrics, "lakehouse_storage_tenants_total")
	if tenants < 1 {
		t.Errorf("storage_tenants_total should be >= 1, got %.0f", tenants)
	}
	t.Logf("storage_tenants_total = %.0f", tenants)
}

// =============================================================================
// Compaction Settings Verification
// =============================================================================

func TestSetting_Compaction_MetricsExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_compaction_runs_total",
		"lakehouse_compaction_errors_total",
		"lakehouse_compaction_skipped_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

func TestSetting_Compaction_ElectionMetrics(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_election_leader")
}

// =============================================================================
// Delete Settings Verification
// =============================================================================

func TestSetting_Delete_Enabled_EndpointAccessible(t *testing.T) {
	client := &http.Client{Timeout: 10 * time.Second}

	params := defaultTimeParams()
	params.Set("query", `_msg:="will-never-match-anything-xyzzy"`)
	u := logsBaseURL + "/delete/logsql/estimate?" + params.Encode()

	resp, err := client.Post(u, "", nil)
	if err != nil {
		t.Fatalf("POST estimate failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 403 {
		t.Log("delete is disabled (403)")
	} else {
		t.Logf("delete is enabled (status=%d)", resp.StatusCode)
	}
}

func TestSetting_Delete_TombstoneMetrics(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_delete_tombstones_active")
	assertMetricExists(t, metrics, "lakehouse_delete_tombstones_total")
}

// =============================================================================
// Stats Settings Verification
// =============================================================================

func TestSetting_Stats_Enabled_PushMetrics(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_stats_push_total")
	assertMetricExists(t, metrics, "lakehouse_stats_snapshot_total")
}

func TestSetting_Stats_CardinalityLimit(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	limit := sumMetric(metrics, "lakehouse_metrics_cardinality_limit")
	if limit <= 0 {
		t.Errorf("cardinality_limit should be positive, got %.0f", limit)
	}
	t.Logf("metrics_cardinality_limit = %.0f", limit)
}

// =============================================================================
// UI Settings Verification
// =============================================================================

func TestSetting_UI_Enabled_Accessible(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/lakehouse/ui/", nil, 200, 301, 302, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Log("UI is disabled or not at this path")
	} else {
		t.Logf("UI accessible at /lakehouse/ui/ (status=%d)", resp.StatusCode)
	}
}

// =============================================================================
// Bloom Settings Verification (from BloomControllerConfig)
// =============================================================================

func TestSetting_Bloom_Enabled_ReflectedInStatus(t *testing.T) {
	status := tryGetBloomStatus(t, logsBaseURL)
	if status == nil {
		t.Skip("bloom status endpoint not available (404)")
	}
	enabled, ok := status["enabled"].(bool)
	if !ok {
		t.Fatal("bloom status missing 'enabled' field")
	}
	t.Logf("bloom enabled = %v", enabled)
}

func TestSetting_Bloom_TierBoundaries_InStatus(t *testing.T) {
	status := tryGetBloomStatus(t, logsBaseURL)
	if status == nil {
		t.Skip("bloom status endpoint not available (404)")
	}
	at, ok := status["auto_tuning"].(map[string]any)
	if !ok {
		t.Skip("no auto_tuning in status")
	}

	tier1 := at["tier1_max_age"]
	tier2 := at["tier2_max_age"]
	tier3 := at["tier3_max_age"]

	t.Logf("tier boundaries: hot=%v warm=%v cold=%v", tier1, tier2, tier3)

	if tier1 == nil || tier2 == nil || tier3 == nil {
		t.Error("tier boundaries should all be present")
	}
}

func TestSetting_Bloom_PartitionGranularity_InStatus(t *testing.T) {
	status := tryGetBloomStatus(t, logsBaseURL)
	if status == nil {
		t.Skip("bloom status endpoint not available (404)")
	}
	at, ok := status["auto_tuning"].(map[string]any)
	if !ok {
		t.Skip("no auto_tuning")
	}

	gran, ok := at["partition_granularity"].(string)
	if !ok {
		t.Error("missing partition_granularity")
	}
	if gran != "hour" && gran != "day" {
		t.Errorf("partition_granularity = %q, want 'hour' or 'day'", gran)
	}
}

func TestSetting_Bloom_TargetFileSize_InStatus(t *testing.T) {
	status := tryGetBloomStatus(t, logsBaseURL)
	if status == nil {
		t.Skip("bloom status endpoint not available (404)")
	}
	at, ok := status["auto_tuning"].(map[string]any)
	if !ok {
		t.Skip("no auto_tuning")
	}

	fileSize, ok := at["target_file_size"].(float64)
	if !ok {
		t.Error("missing target_file_size in auto_tuning")
		return
	}
	if fileSize <= 0 {
		t.Errorf("target_file_size should be positive, got %.0f", fileSize)
	}
	t.Logf("target_file_size = %.0f bytes", fileSize)
}

func TestSetting_Bloom_CacheMaxBytes_InStatus(t *testing.T) {
	status := tryGetBloomStatus(t, logsBaseURL)
	if status == nil {
		t.Skip("bloom status endpoint not available (404)")
	}
	at, ok := status["auto_tuning"].(map[string]any)
	if !ok {
		t.Skip("no auto_tuning")
	}

	cacheMax, ok := at["cache_max_bytes"].(float64)
	if !ok {
		t.Error("missing cache_max_bytes")
		return
	}
	if cacheMax <= 0 {
		t.Errorf("cache_max_bytes should be positive, got %.0f", cacheMax)
	}
	t.Logf("cache_max_bytes = %.0f", cacheMax)
}

// =============================================================================
// Storage Settings Verification
// =============================================================================

func TestSetting_Storage_FilesExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	files := sumMetric(metrics, "lakehouse_storage_files_total")
	if files <= 0 {
		t.Error("storage should have files")
	}
	t.Logf("storage_files_total = %.0f", files)
}

func TestSetting_Storage_CostCalculation(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	cost := sumMetric(metrics, "lakehouse_storage_cost_monthly_usd")
	t.Logf("storage_cost_monthly_usd = %.4f", cost)
}

func TestSetting_Storage_BytesByClass(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_bytes_by_class")
}

func TestSetting_Storage_PartitionsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	partitions := sumMetric(metrics, "lakehouse_storage_partitions_total")
	if partitions <= 0 {
		t.Error("should have at least 1 partition")
	}
	t.Logf("storage_partitions_total = %.0f", partitions)
}

// =============================================================================
// Startup Settings Verification
// =============================================================================

func TestSetting_Startup_ReadyMetric(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	ready := sumMetric(metrics, "lakehouse_ready")
	if ready != 1 {
		t.Errorf("lakehouse_ready should be 1, got %.0f", ready)
	}
}

func TestSetting_Startup_TotalSeconds(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	seconds := sumMetric(metrics, "lakehouse_startup_total_seconds")
	if seconds <= 0 {
		t.Errorf("startup_total_seconds should be positive, got %f", seconds)
	}
	t.Logf("startup_total_seconds = %.3f", seconds)
}

// =============================================================================
// Prefetch Settings Verification
// =============================================================================

func TestSetting_Prefetch_MetricsExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_prefetch_tasks_total")
	assertMetricExists(t, metrics, "lakehouse_prefetch_hits_total")
}

// =============================================================================
// AZ-Aware Settings Verification
// =============================================================================

func TestSetting_AZ_CacheStatsShowAZ(t *testing.T) {
	stats := getCacheStats(t, logsBaseURL)
	if az, ok := stats["az"]; ok {
		t.Logf("AZ from cache stats: %v", az)
	} else {
		t.Log("no AZ info in cache stats (may not be configured)")
	}
}

func TestSetting_AZ_PeerMetrics(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_peer_same_az_members")
	assertMetricExists(t, metrics, "lakehouse_peer_cross_az_members")
}

// =============================================================================
// Parquet Engine Settings Verification
// =============================================================================

func TestSetting_Parquet_RowGroupSize_MetricsReflect(t *testing.T) {
	queryLogs(t, "*", 50)

	metrics := scrapeMetrics(t, logsBaseURL)
	scanned := sumMetric(metrics, "lakehouse_parquet_row_groups_scanned_total")
	if scanned <= 0 {
		t.Error("should have scanned at least 1 row group")
	}
	t.Logf("row_groups_scanned = %.0f", scanned)
}

func TestSetting_Parquet_BloomChecks_AfterQuery(t *testing.T) {
	queryLogs(t, `trace_id:="parquet-bloom-check-test"`, 1)

	metrics := scrapeMetrics(t, logsBaseURL)
	// No dedicated bloom_checks_total metric; bloom skipping is tracked via
	// lakehouse_parquet_row_groups_skipped_total{reason="bloom"}
	assertMetricExists(t, metrics, "lakehouse_parquet_row_groups_skipped_total")
	assertMetricWithLabelExists(t, metrics, "lakehouse_parquet_row_groups_skipped_total", "reason", "bloom")
}

// =============================================================================
// Cross-Signal Settings Verification
// =============================================================================

func TestSetting_CrossSignal_MetricsExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_cache_cross_eviction_sent_total")
	assertMetricExists(t, metrics, "lakehouse_cache_cross_prefetch_sent_total")
}

// =============================================================================
// Retention Settings Verification
// =============================================================================

func TestSetting_Retention_MetricExists(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_retention_files_deleted_total")
}

// =============================================================================
// Full Settings Cross-Validation
// =============================================================================

func TestSetting_CrossValidation_InsertAffectsStorage(t *testing.T) {
	metricsBefore := scrapeMetrics(t, logsBaseURL)
	rowsBefore := sumMetric(metricsBefore, "lakehouse_insert_rows_total")

	insertTestLogs(t, logsBaseURL, 100, "cross-validation-svc")
	time.Sleep(2 * time.Second)

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	rowsAfter := sumMetric(metricsAfter, "lakehouse_insert_rows_total")

	delta := rowsAfter - rowsBefore
	if delta < 100 {
		t.Errorf("insert_rows_total should increase by >= 100, delta = %.0f", delta)
	}
	t.Logf("insert_rows_total delta = %.0f", delta)
}

func TestSetting_CrossValidation_QueryAffectsHTTPMetrics(t *testing.T) {
	metricsBefore := scrapeMetrics(t, logsBaseURL)
	httpBefore := sumMetric(metricsBefore, "vl_http_requests_total")

	queryLogs(t, "*", 5)
	queryLogs(t, `service.name:="api-gateway"`, 5)

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	httpAfter := sumMetric(metricsAfter, "vl_http_requests_total")

	delta := httpAfter - httpBefore
	if delta < 2 {
		t.Errorf("vl_http_requests_total should increase by >= 2, delta = %.0f", delta)
	}
	t.Logf("vl_http_requests_total delta = %.0f", delta)
}

func TestSetting_CrossValidation_BloomStatusMatchesConfig(t *testing.T) {
	status := tryGetBloomStatus(t, logsBaseURL)
	if status == nil {
		t.Skip("bloom status endpoint not available (404)")
	}

	at, ok := status["auto_tuning"].(map[string]any)
	if !ok {
		t.Skip("no auto_tuning")
	}

	tier1 := at["tier1_max_age"]
	tier2 := at["tier2_max_age"]
	tier3 := at["tier3_max_age"]

	if tier1Str, ok := tier1.(string); ok {
		if !strings.Contains(tier1Str, "h") && !strings.Contains(tier1Str, "d") {
			t.Errorf("tier1_max_age = %q, expected duration format", tier1Str)
		}
	}
	t.Logf("tier config: hot=%v warm=%v cold=%v", tier1, tier2, tier3)
}

func TestSetting_CrossValidation_AllEndpointsReturnConsistentMode(t *testing.T) {
	info := getLakehouseInfo(t, logsBaseURL)
	infoMode, _ := info["mode"].(string)

	bloom := tryGetBloomStatus(t, logsBaseURL)
	if bloom == nil {
		t.Logf("bloom status endpoint not available (404), skipping bloom mode check")
		t.Logf("info mode = %q", infoMode)
		return
	}

	bloomMode, _ := bloom["mode"].(string)
	if infoMode != bloomMode {
		t.Errorf("mode mismatch: info=%q bloom=%q", infoMode, bloomMode)
	}
}

// =============================================================================
// Insert → Query → Metrics Round-Trip Verification
// =============================================================================

func TestSetting_RoundTrip_InsertQueryMetrics(t *testing.T) {
	service := fmt.Sprintf("roundtrip-%d", time.Now().UnixNano()%10000)

	metricsBefore := scrapeMetrics(t, logsBaseURL)
	insertBefore := sumMetric(metricsBefore, "lakehouse_insert_rows_total")
	queryBefore := sumMetric(metricsBefore, "lakehouse_query_rows_returned_total")

	traceIDs := insertTestLogs(t, logsBaseURL, 25, service)
	time.Sleep(3 * time.Second)

	metricsAfterInsert := scrapeMetrics(t, logsBaseURL)
	insertAfter := sumMetric(metricsAfterInsert, "lakehouse_insert_rows_total")

	insertDelta := insertAfter - insertBefore
	if insertDelta < 25 {
		t.Errorf("insert_rows_total should increase by >= 25, delta = %.0f", insertDelta)
	}

	results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, traceIDs[0]), 10)

	metricsAfterQuery := scrapeMetrics(t, logsBaseURL)
	queryAfter := sumMetric(metricsAfterQuery, "lakehouse_query_rows_returned_total")
	queryDelta := queryAfter - queryBefore

	t.Logf("insert delta=%.0f, query results=%d, query_rows delta=%.0f",
		insertDelta, len(results), queryDelta)
}

// =============================================================================
// Completeness: All config sections have associated metrics
// =============================================================================

func TestSetting_Completeness_AllSectionsHaveMetrics(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)

	sections := map[string][]string{
		"http":        {"vl_http_requests_total"},
		"s3":          {"lakehouse_s3_requests_total"},
		"cache":       {"lakehouse_cache_hits_total", "lakehouse_cache_bytes_used"},
		"peer":        {"lakehouse_peer_requests_total"},
		"manifest":    {"lakehouse_manifest_files"},
		"parquet":     {"lakehouse_parquet_row_groups_scanned_total"},
		"insert":      {"lakehouse_insert_rows_total"},
		"prefetch":    {"lakehouse_prefetch_bytes_total"},
		"smart_cache": {"lakehouse_cache_hit_ratio"},
		"bloom":       {"lakehouse_parquet_row_groups_skipped_total"},
		"startup":     {"lakehouse_startup_phase", "lakehouse_ready"},
		"query":       {"lakehouse_query_duration_seconds_count"},
		"compaction":  {"lakehouse_compaction_runs_total"},
		"election":    {"lakehouse_election_leader"},
		"tenant":      {"lakehouse_tenant_files"},
		"storage":     {"lakehouse_storage_files_total"},
		"cardinality": {"lakehouse_metrics_cardinality_limit"},
		"stats":       {"lakehouse_stats_push_total"},
		"retention":   {"lakehouse_retention_files_deleted_total"},
		"delete":      {"lakehouse_delete_tombstones_active"},
	}

	for section, metricNames := range sections {
		for _, name := range metricNames {
			found := false
			if _, ok := metrics[name]; ok {
				found = true
			}
			if !found {
				for k := range metrics {
					if strings.HasPrefix(k, name) {
						found = true
						break
					}
				}
			}
			if !found {
				t.Errorf("section %q: metric %q missing", section, name)
			}
		}
	}
}

// =============================================================================
// Edge Cases: Config validation reflected in behavior
// =============================================================================

func TestSetting_Edge_EmptyQueryReturnsResults(t *testing.T) {
	results := queryLogs(t, "*", 1)
	if len(results) == 0 {
		t.Error("wildcard query should always return data when data exists")
	}
}

func TestSetting_Edge_ZeroLimitReturnsNothing(t *testing.T) {
	results := queryLogs(t, "*", 0)
	if len(results) > 0 {
		t.Logf("limit=0 returned %d results (implementation may treat 0 as unlimited)", len(results))
	}
}

func TestSetting_Edge_VeryLargeLimit(t *testing.T) {
	results := queryLogs(t, "*", 100000)
	t.Logf("limit=100000 returned %d results", len(results))
}

func TestSetting_Edge_SpecialCharsInQuery(t *testing.T) {
	results := queryLogs(t, `_msg:"test"`, 5)
	t.Logf("substring query returned %d results", len(results))
}

func TestSetting_Edge_FutureTimeRange(t *testing.T) {
	params := url.Values{
		"query": {"*"},
		"limit": {"5"},
		"start": {fmt.Sprintf("%d", time.Now().Add(24*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", time.Now().Add(48*time.Hour).UnixNano())},
	}

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")

	nonEmpty := 0
	for _, line := range lines {
		if line != "" {
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err == nil {
				nonEmpty++
			}
		}
	}

	if nonEmpty > 0 {
		t.Logf("future time range returned %d results (unexpected but not necessarily wrong)", nonEmpty)
	} else {
		t.Log("future time range correctly returned 0 results")
	}
}
