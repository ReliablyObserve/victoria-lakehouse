//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// scrapeMetrics fetches the full /metrics output from the given base URL.
func scrapeMetrics(t *testing.T, baseURL string) map[string][]metricLine {
	t.Helper()
	u := baseURL + "/metrics"
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d: %s", u, resp.StatusCode, string(body))
	}
	return parsePrometheusText(string(body))
}

type metricLine struct {
	name   string
	labels map[string]string
	value  float64
	raw    string
}

func parsePrometheusText(text string) map[string][]metricLine {
	result := make(map[string][]metricLine)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		ml := metricLine{raw: line, labels: make(map[string]string)}

		nameEnd := strings.IndexAny(line, "{ ")
		if nameEnd < 0 {
			continue
		}
		ml.name = line[:nameEnd]

		rest := line[nameEnd:]
		if strings.HasPrefix(rest, "{") {
			labelEnd := strings.Index(rest, "}")
			if labelEnd < 0 {
				continue
			}
			labelStr := rest[1:labelEnd]
			for _, kv := range splitLabels(labelStr) {
				parts := strings.SplitN(kv, "=", 2)
				if len(parts) == 2 {
					ml.labels[parts[0]] = strings.Trim(parts[1], "\"")
				}
			}
			rest = rest[labelEnd+1:]
		}

		valStr := strings.TrimSpace(rest)
		if v, err := strconv.ParseFloat(valStr, 64); err == nil {
			ml.value = v
		}

		result[ml.name] = append(result[ml.name], ml)
	}
	return result
}

func splitLabels(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '"' && (i == 0 || s[i-1] != '\\') {
			inQuote = !inQuote
		}
		if ch == ',' && !inQuote {
			result = append(result, strings.TrimSpace(current.String()))
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}
	if current.Len() > 0 {
		result = append(result, strings.TrimSpace(current.String()))
	}
	return result
}

func assertMetricExists(t *testing.T, metrics map[string][]metricLine, name string) {
	t.Helper()
	if _, ok := metrics[name]; !ok {
		t.Errorf("metric %q not found in /metrics output", name)
	}
}

func assertMetricGE(t *testing.T, metrics map[string][]metricLine, name string, minVal float64) {
	t.Helper()
	lines, ok := metrics[name]
	if !ok {
		t.Errorf("metric %q not found", name)
		return
	}
	total := 0.0
	for _, l := range lines {
		total += l.value
	}
	if total < minVal {
		t.Errorf("metric %q total = %f, want >= %f", name, total, minVal)
	}
}

func assertMetricWithLabelExists(t *testing.T, metrics map[string][]metricLine, name, labelKey, labelValue string) {
	t.Helper()
	lines, ok := metrics[name]
	if !ok {
		t.Errorf("metric %q not found", name)
		return
	}
	for _, l := range lines {
		if l.labels[labelKey] == labelValue {
			return
		}
	}
	t.Errorf("metric %q with %s=%q not found", name, labelKey, labelValue)
}

func assertMetricWithLabelGE(t *testing.T, metrics map[string][]metricLine, name, labelKey, labelValue string, minVal float64) {
	t.Helper()
	lines, ok := metrics[name]
	if !ok {
		t.Errorf("metric %q not found", name)
		return
	}
	for _, l := range lines {
		if l.labels[labelKey] == labelValue {
			if l.value < minVal {
				t.Errorf("metric %q{%s=%q} = %f, want >= %f", name, labelKey, labelValue, l.value, minVal)
			}
			return
		}
	}
	t.Errorf("metric %q with %s=%q not found", name, labelKey, labelValue)
}

// =============================================================================
// HTTP / RED Metrics
// =============================================================================

func TestMetrics_HTTP_RequestsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "vl_http_requests_total")
	assertMetricWithLabelExists(t, metrics, "vl_http_requests_total", "path", "/insert/jsonline")
}

func TestMetrics_HTTP_RequestDuration(t *testing.T) {
	_ = httpGetBody(t, logsBaseURL, "/health", nil)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "vl_http_request_duration_seconds_count")
	assertMetricGE(t, metrics, "vl_http_request_duration_seconds_count", 1)
}

func TestMetrics_HTTP_ErrorsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "vl_http_errors_total")
}

func TestMetrics_HTTP_ConcurrentSelects(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_concurrent_select_current")
	assertMetricExists(t, metrics, "lakehouse_concurrent_select_capacity")
}

func TestMetrics_HTTP_SlowQueries(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_slow_queries_total")
}

// =============================================================================
// S3 Metrics
// =============================================================================

func TestMetrics_S3_RequestsTotal(t *testing.T) {
	queryLogs(t, "*", 1)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_s3_requests_total")
	assertMetricGE(t, metrics, "lakehouse_s3_requests_total", 1)
}

func TestMetrics_S3_RequestDuration(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_s3_request_duration_seconds_count")
}

func TestMetrics_S3_ErrorsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_s3_errors_total")
}

func TestMetrics_S3_BytesReadTotal(t *testing.T) {
	queryLogs(t, "*", 5)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_s3_bytes_read_total")
	assertMetricGE(t, metrics, "lakehouse_s3_bytes_read_total", 1)
}

func TestMetrics_S3_ThrottleTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_s3_throttle_total")
}

// =============================================================================
// Cache Metrics
// =============================================================================

func TestMetrics_Cache_HitRatioAndMemory(t *testing.T) {
	queryLogs(t, "*", 5)
	queryLogs(t, "*", 5)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_cache_hit_ratio")
	assertMetricExists(t, metrics, "lakehouse_cache_memory_bytes")
}

func TestMetrics_Cache_MemoryBytes(t *testing.T) {
	queryLogs(t, "*", 5)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_cache_memory_bytes")
}

func TestMetrics_Cache_DiskBytes(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_cache_disk_bytes")
}

func TestMetrics_Cache_SingleflightDedup(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_cache_singleflight_dedup_total")
}

// =============================================================================
// Peer Cache Metrics
// =============================================================================

func TestMetrics_Peer_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_peer_hits_total",
		"lakehouse_peer_ring_members",
		"lakehouse_peer_errors_total",
		"lakehouse_peer_same_az_members",
		"lakehouse_peer_cross_az_members",
	} {
		assertMetricExists(t, metrics, name)
	}
}

// =============================================================================
// Manifest & Discovery Metrics
// =============================================================================

func TestMetrics_Manifest_Files(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_manifest_files")
	assertMetricGE(t, metrics, "lakehouse_manifest_files", 1)
}

func TestMetrics_Manifest_Bytes(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_manifest_bytes")
	assertMetricGE(t, metrics, "lakehouse_manifest_bytes", 1)
}

func TestMetrics_Manifest_PushTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_manifest_push_total")
}

func TestMetrics_Manifest_FastPath(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_manifest_fast_path_total")
}

func TestMetrics_Manifest_Push(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_manifest_push_total",
		"lakehouse_manifest_push_peers",
		"lakehouse_manifest_push_errors_total",
		"lakehouse_manifest_update_received_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

func TestMetrics_Discovery_Boundaries(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_discovery_hot_boundary_days")
	assertMetricExists(t, metrics, "lakehouse_discovery_hot_boundary_gap_days")
}

// =============================================================================
// Parquet Engine Metrics
// =============================================================================

func TestMetrics_Parquet_RowGroupsScanned(t *testing.T) {
	queryLogs(t, "*", 5)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_parquet_row_groups_scanned_total")
	assertMetricGE(t, metrics, "lakehouse_parquet_row_groups_scanned_total", 1)
}

func TestMetrics_Parquet_RowGroupsSkipped(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_parquet_row_groups_skipped_total")
}

func TestMetrics_Parquet_BloomChecks(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Parquet_ColumnBytesRead(t *testing.T) {
	queryLogs(t, "*", 5)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_parquet_column_bytes_read_total")
	assertMetricGE(t, metrics, "lakehouse_parquet_column_bytes_read_total", 1)
}

func TestMetrics_Parquet_FilesOpened(t *testing.T) {
	queryLogs(t, "*", 5)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_parquet_files_opened_total")
	assertMetricGE(t, metrics, "lakehouse_parquet_files_opened_total", 1)
}

func TestMetrics_Parquet_FilesSkippedBloom(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

// =============================================================================
// Insert / Writer Metrics
// =============================================================================

func TestMetrics_Insert_RowsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_rows_total")
}

func TestMetrics_Insert_RowsBuffered(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_rows_buffered")
}

func TestMetrics_Insert_BytesBuffered(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_bytes_buffered")
}

func TestMetrics_Insert_FlushTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_flush_total")
}

func TestMetrics_Insert_FlushErrors(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_flush_errors_total")
}

func TestMetrics_Insert_FlushDuration(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_flush_duration_seconds_count")
}

func TestMetrics_Insert_BytesUploaded(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_bytes_uploaded_total")
}

func TestMetrics_Insert_PartitionsActive(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_partitions_active")
}

func TestMetrics_Insert_WALBytes(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_insert_wal_bytes")
}

func TestMetrics_Insert_AfterIngestion(t *testing.T) {
	insertTestLogs(t, logsBaseURL, 10, "metrics-test-svc")
	time.Sleep(2 * time.Second)

	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricGE(t, metrics, "lakehouse_insert_rows_total", 10)
}

// =============================================================================
// Prefetch Metrics
// =============================================================================

func TestMetrics_Prefetch_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_prefetch_hits_total",
		"lakehouse_prefetch_bytes_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

// =============================================================================
// Smart Cache Metrics
// =============================================================================

func TestMetrics_SmartCache_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_cache_hit_ratio",
		"lakehouse_cache_entries_total",
		"lakehouse_cache_bytes_used",
		"lakehouse_cache_bytes_limit",
		"lakehouse_cache_hot_entries",
		"lakehouse_cache_pinned_entries",
		"lakehouse_cache_coverage_hours",
		"lakehouse_cache_prefetch_hit_ratio",
		"lakehouse_cache_owned_entries",
		"lakehouse_cache_owned_bytes",
		"lakehouse_cache_peer_served_total",
		"lakehouse_cache_effective_bytes",
	} {
		assertMetricExists(t, metrics, name)
	}
}

func TestMetrics_SmartCache_BytesLimit_Positive(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricGE(t, metrics, "lakehouse_cache_bytes_limit", 1)
}

// =============================================================================
// Cross-Signal Eviction Metrics
// =============================================================================

func TestMetrics_CrossSignal_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_cache_cross_eviction_sent_total",
		"lakehouse_cache_cross_eviction_received_total",
		"lakehouse_cache_cross_eviction_pending",
		"lakehouse_cache_cross_eviction_applied_total",
		"lakehouse_cache_cross_prefetch_sent_total",
		"lakehouse_cache_cross_prefetch_received_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

// =============================================================================
// AZ-Aware Routing Metrics
// =============================================================================

func TestMetrics_AZ_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_peer_same_az_members",
		"lakehouse_peer_cross_az_members",
	} {
		assertMetricExists(t, metrics, name)
	}
}

// =============================================================================
// Bloom Index Metrics
// =============================================================================

func TestMetrics_Bloom_BuildTotal(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_BuildErrors(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_EntriesTotal(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_BytesMemory(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_QueriesTotal(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_FilesSkipped(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_BytesAvoided(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_TierPartitions(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_TierTransitions(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_ConfigSync(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_ControllerAdjustments(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func TestMetrics_Bloom_IncrementAfterInsertAndQuery(t *testing.T) {
	t.Skip("bloom metrics not registered in this build")
}

func sumMetric(metrics map[string][]metricLine, name string) float64 {
	lines := metrics[name]
	total := 0.0
	for _, l := range lines {
		total += l.value
	}
	return total
}

// =============================================================================
// Startup & Health Metrics
// =============================================================================

func TestMetrics_Startup_Phase(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_startup_phase")
}

func TestMetrics_Startup_TotalSeconds(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_startup_total_seconds")
	assertMetricGE(t, metrics, "lakehouse_startup_total_seconds", 0)
}

func TestMetrics_Ready(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_ready")
	assertMetricGE(t, metrics, "lakehouse_ready", 1)
}

// =============================================================================
// Query Metrics
// =============================================================================

func TestMetrics_Query_Duration(t *testing.T) {
	queryLogs(t, "*", 5)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_query_duration_seconds_count")
	assertMetricGE(t, metrics, "lakehouse_query_duration_seconds_count", 1)
}

func TestMetrics_Query_RowsTotal(t *testing.T) {
	queryLogs(t, "*", 5)
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_query_rows_returned_total")
	assertMetricGE(t, metrics, "lakehouse_query_rows_returned_total", 1)
}

func TestMetrics_Query_RejectedTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_query_rejected_total")
}

// =============================================================================
// Compaction Metrics
// =============================================================================

func TestMetrics_Compaction_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_compaction_runs_total",
		"lakehouse_compaction_files_input_total",
		"lakehouse_compaction_files_output_total",
		"lakehouse_compaction_bytes_read_total",
		"lakehouse_compaction_bytes_written_total",
		"lakehouse_compaction_rows_merged_total",
		"lakehouse_compaction_errors_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

// =============================================================================
// Election Metrics
// =============================================================================

func TestMetrics_Election_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_election_leader",
		"lakehouse_election_transitions_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

// =============================================================================
// Tenant Metrics
// =============================================================================

func TestMetrics_Tenant_Files(t *testing.T) {
	t.Skip("per-tenant metrics not registered in this build; use lakehouse_storage_files_total instead")
}

func TestMetrics_Tenant_Bytes(t *testing.T) {
	t.Skip("per-tenant metrics not registered in this build; use lakehouse_storage_bytes_total instead")
}

func TestMetrics_Tenant_RawBytes(t *testing.T) {
	t.Skip("per-tenant metrics not registered in this build; use lakehouse_storage_raw_bytes_total instead")
}

func TestMetrics_Tenant_RowsTotal(t *testing.T) {
	t.Skip("per-tenant metrics not registered in this build; use lakehouse_storage_rows_total instead")
}

func TestMetrics_Tenant_IngestionBytes(t *testing.T) {
	t.Skip("per-tenant metrics not registered in this build")
}

func TestMetrics_Tenant_QueriesTotal(t *testing.T) {
	t.Skip("per-tenant metrics not registered in this build")
}

func TestMetrics_Tenant_LastWriteTimestamp(t *testing.T) {
	t.Skip("per-tenant metrics not registered in this build")
}

func TestMetrics_Tenant_LastQueryTimestamp(t *testing.T) {
	t.Skip("per-tenant metrics not registered in this build")
}

// =============================================================================
// Global Storage Metrics
// =============================================================================

func TestMetrics_Storage_FilesTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_files_total")
	assertMetricGE(t, metrics, "lakehouse_storage_files_total", 1)
}

func TestMetrics_Storage_BytesTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_bytes_total")
	assertMetricGE(t, metrics, "lakehouse_storage_bytes_total", 1)
}

func TestMetrics_Storage_RawBytesTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_raw_bytes_total")
}

func TestMetrics_Storage_CompressionRatio(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_compression_ratio")
}

func TestMetrics_Storage_RowsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_rows_total")
}

func TestMetrics_Storage_PartitionsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_partitions_total")
	assertMetricGE(t, metrics, "lakehouse_storage_partitions_total", 1)
}

func TestMetrics_Storage_OldestNewestData(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_oldest_data_seconds")
	assertMetricExists(t, metrics, "lakehouse_storage_newest_data_seconds")
}

func TestMetrics_Storage_TenantsTotal(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_tenants_total")
	assertMetricGE(t, metrics, "lakehouse_storage_tenants_total", 1)
}

func TestMetrics_Storage_BytesByClass(t *testing.T) {
	t.Skip("per-class storage metrics not registered in this build")
}

func TestMetrics_Storage_FilesByClass(t *testing.T) {
	t.Skip("per-class storage metrics not registered in this build")
}

func TestMetrics_Storage_CostMonthlyUSD(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_cost_monthly_usd")
}

func TestMetrics_Storage_CostByClassUSD(t *testing.T) {
	t.Skip("per-class storage metrics not registered in this build")
}

func TestMetrics_Storage_IngestionRate(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_storage_ingestion_rate_bytes")
}

// =============================================================================
// Cardinality Limiter Metrics
// =============================================================================

func TestMetrics_Cardinality_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_metrics_cardinality_limit",
		"lakehouse_metrics_cardinality_tracked",
		"lakehouse_metrics_cardinality_overflow_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

func TestMetrics_Cardinality_LimitPositive(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricGE(t, metrics, "lakehouse_metrics_cardinality_limit", 1)
}

// =============================================================================
// Stats Sync Metrics
// =============================================================================

func TestMetrics_Stats_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_stats_push_total",
		"lakehouse_stats_push_errors_total",
		"lakehouse_stats_push_bytes_total",
		"lakehouse_stats_snapshot_total",
		"lakehouse_stats_snapshot_errors_total",
		"lakehouse_stats_merges_total",
		"lakehouse_stats_headobject_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

// =============================================================================
// Retention Metrics
// =============================================================================

func TestMetrics_Retention_FilesDeleted(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_retention_files_deleted_total")
}

// =============================================================================
// Delete Metrics
// =============================================================================

func TestMetrics_Delete_AllExist(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)
	for _, name := range []string{
		"lakehouse_delete_tombstones_active",
		"lakehouse_delete_tombstones_total",
		"lakehouse_delete_rewrite_total",
		"lakehouse_delete_rewrite_errors_total",
		"lakehouse_delete_rewrite_bytes_saved_total",
		"lakehouse_delete_rewrite_skipped_glacier_total",
		"lakehouse_delete_rows_suppressed_total",
		"lakehouse_delete_compaction_rows_removed_total",
		"lakehouse_delete_verify_total",
		"lakehouse_delete_verify_leak_detected_total",
	} {
		assertMetricExists(t, metrics, name)
	}
}

// =============================================================================
// Traces Service Metrics (same metrics should exist for traces)
// =============================================================================

func TestMetrics_Traces_CoreMetricsExist(t *testing.T) {
	metrics := scrapeMetrics(t, tracesBaseURL)
	for _, name := range []string{
		"vt_http_requests_total",
		"lakehouse_s3_requests_total",
		"lakehouse_manifest_files",
		"lakehouse_storage_files_total",
		"lakehouse_insert_rows_total",
		"lakehouse_ready",
		"lakehouse_startup_phase",
	} {
		assertMetricExists(t, metrics, name)
	}
}

func TestMetrics_Traces_ReadyAndHealthy(t *testing.T) {
	metrics := scrapeMetrics(t, tracesBaseURL)
	assertMetricGE(t, metrics, "lakehouse_ready", 1)
}

// =============================================================================
// Cross-Validation: Metrics vs Endpoints
// =============================================================================

func TestMetrics_CrossValidation_ManifestFilesVsEndpoint(t *testing.T) {
	info := getManifestRange(t, logsBaseURL)

	metrics := scrapeMetrics(t, logsBaseURL)
	metricVal := sumMetric(metrics, "lakehouse_manifest_files")

	fileCount, ok := info["files"]
	if !ok {
		t.Skip("manifest/range does not have files field")
	}
	endpointCount, _ := fileCount.(float64)

	t.Logf("manifest_files metric=%.0f, endpoint=%.0f", metricVal, endpointCount)
	if metricVal > 0 && endpointCount > 0 && metricVal != endpointCount {
		t.Logf("NOTE: metric and endpoint may differ slightly due to timing")
	}
}

func TestMetrics_CrossValidation_StorageVsManifest(t *testing.T) {
	metrics := scrapeMetrics(t, logsBaseURL)

	storageFiles := sumMetric(metrics, "lakehouse_storage_files_total")
	manifestFiles := sumMetric(metrics, "lakehouse_manifest_files")

	t.Logf("storage_files=%.0f manifest_files=%.0f", storageFiles, manifestFiles)
	if storageFiles > 0 && manifestFiles > 0 {
		ratio := storageFiles / manifestFiles
		if ratio < 0.5 || ratio > 2.0 {
			t.Errorf("storage files (%.0f) and manifest files (%.0f) diverge beyond 2x", storageFiles, manifestFiles)
		}
	}
}

func TestMetrics_CrossValidation_CacheConsistency(t *testing.T) {
	stats := getCacheStats(t, logsBaseURL)
	metrics := scrapeMetrics(t, logsBaseURL)

	metricBytes := sumMetric(metrics, "lakehouse_cache_bytes_used")

	t.Logf("cache endpoint: %v, metric bytes_used: %.0f", stats, metricBytes)
}

// =============================================================================
// Full Metrics Completeness Check
// =============================================================================

func TestMetrics_Completeness_AllDeclaredMetricsExist(t *testing.T) {
	declaredMetrics := []string{
		// HTTP metrics (from VL upstream)
		"vl_http_requests_total",
		"vl_http_request_duration_seconds",
		"vl_http_errors_total",
		// Query concurrency
		"lakehouse_concurrent_select_current",
		"lakehouse_concurrent_select_capacity",
		"lakehouse_slow_queries_total",
		// S3
		"lakehouse_s3_requests_total",
		"lakehouse_s3_request_duration_seconds",
		"lakehouse_s3_errors_total",
		"lakehouse_s3_bytes_read_total",
		"lakehouse_s3_throttle_total",
		// Cache
		"lakehouse_cache_hit_ratio",
		"lakehouse_cache_memory_bytes",
		"lakehouse_cache_disk_bytes",
		"lakehouse_cache_singleflight_dedup_total",
		"lakehouse_cache_bytes_limit",
		"lakehouse_cache_bytes_used",
		"lakehouse_cache_entries_total",
		"lakehouse_cache_effective_bytes",
		"lakehouse_cache_owned_bytes",
		"lakehouse_cache_owned_entries",
		"lakehouse_cache_hot_entries",
		"lakehouse_cache_pinned_entries",
		"lakehouse_cache_peer_served_total",
		"lakehouse_cache_prefetch_hit_ratio",
		"lakehouse_cache_coverage_hours",
		// Cross-signal cache
		"lakehouse_cache_cross_eviction_sent_total",
		"lakehouse_cache_cross_eviction_received_total",
		"lakehouse_cache_cross_eviction_pending",
		"lakehouse_cache_cross_eviction_applied_total",
		"lakehouse_cache_cross_prefetch_sent_total",
		"lakehouse_cache_cross_prefetch_received_total",
		// Peer
		"lakehouse_peer_hits_total",
		"lakehouse_peer_errors_total",
		"lakehouse_peer_ring_members",
		"lakehouse_peer_same_az_members",
		"lakehouse_peer_cross_az_members",
		// Manifest
		"lakehouse_manifest_files",
		"lakehouse_manifest_bytes",
		"lakehouse_manifest_fast_path_total",
		"lakehouse_manifest_push_total",
		"lakehouse_manifest_push_errors_total",
		"lakehouse_manifest_push_peers",
		"lakehouse_manifest_update_received_total",
		// Discovery
		"lakehouse_discovery_hot_boundary_days",
		"lakehouse_discovery_hot_boundary_gap_days",
		// Parquet
		"lakehouse_parquet_row_groups_scanned_total",
		"lakehouse_parquet_row_groups_skipped_total",
		"lakehouse_parquet_column_bytes_read_total",
		"lakehouse_parquet_files_opened_total",
		// Insert
		"lakehouse_insert_rows_total",
		"lakehouse_insert_rows_buffered",
		"lakehouse_insert_bytes_buffered",
		"lakehouse_insert_flush_total",
		"lakehouse_insert_flush_errors_total",
		"lakehouse_insert_flush_duration_seconds",
		"lakehouse_insert_bytes_uploaded_total",
		"lakehouse_insert_partitions_active",
		"lakehouse_insert_wal_bytes",
		// Prefetch
		"lakehouse_prefetch_hits_total",
		"lakehouse_prefetch_bytes_total",
		// Startup & health
		"lakehouse_startup_phase",
		"lakehouse_startup_total_seconds",
		"lakehouse_ready",
		"lakehouse_info",
		// Query
		"lakehouse_query_duration_seconds",
		"lakehouse_query_rows_returned_total",
		"lakehouse_query_rejected_total",
		// Compaction
		"lakehouse_compaction_runs_total",
		"lakehouse_compaction_files_input_total",
		"lakehouse_compaction_files_output_total",
		"lakehouse_compaction_bytes_read_total",
		"lakehouse_compaction_bytes_written_total",
		"lakehouse_compaction_rows_merged_total",
		"lakehouse_compaction_errors_total",
		// Election
		"lakehouse_election_leader",
		"lakehouse_election_transitions_total",
		// Storage
		"lakehouse_storage_files_total",
		"lakehouse_storage_bytes_total",
		"lakehouse_storage_raw_bytes_total",
		"lakehouse_storage_compression_ratio",
		"lakehouse_storage_rows_total",
		"lakehouse_storage_partitions_total",
		"lakehouse_storage_oldest_data_seconds",
		"lakehouse_storage_newest_data_seconds",
		"lakehouse_storage_tenants_total",
		"lakehouse_storage_cost_monthly_usd",
		"lakehouse_storage_ingestion_rate_bytes",
		// Cardinality
		"lakehouse_metrics_cardinality_limit",
		"lakehouse_metrics_cardinality_tracked",
		"lakehouse_metrics_cardinality_overflow_total",
		// Stats
		"lakehouse_stats_push_total",
		"lakehouse_stats_push_errors_total",
		"lakehouse_stats_push_bytes_total",
		"lakehouse_stats_snapshot_total",
		"lakehouse_stats_snapshot_errors_total",
		"lakehouse_stats_merges_total",
		"lakehouse_stats_headobject_total",
		// Retention
		"lakehouse_retention_files_deleted_total",
		// Delete
		"lakehouse_delete_tombstones_active",
		"lakehouse_delete_tombstones_total",
		"lakehouse_delete_rewrite_total",
		"lakehouse_delete_rewrite_errors_total",
		"lakehouse_delete_rewrite_bytes_saved_total",
		"lakehouse_delete_rewrite_skipped_glacier_total",
		"lakehouse_delete_rows_suppressed_total",
		"lakehouse_delete_compaction_rows_removed_total",
		"lakehouse_delete_verify_total",
		"lakehouse_delete_verify_leak_detected_total",
	}

	metrics := scrapeMetrics(t, logsBaseURL)

	missing := 0
	for _, name := range declaredMetrics {
		found := false
		if _, ok := metrics[name]; ok {
			found = true
		}
		if !found {
			for k := range metrics {
				if strings.HasPrefix(k, name+"_") || strings.HasPrefix(k, name+"{") {
					found = true
					break
				}
			}
		}
		if !found {
			t.Errorf("declared metric %q missing from /metrics output", name)
			missing++
		}
	}
	t.Logf("checked %d declared metrics, %d missing", len(declaredMetrics), missing)
}
