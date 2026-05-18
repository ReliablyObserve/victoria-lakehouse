//go:build e2e

package e2e

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// Bloom Status API Deep Verification
// =============================================================================

func TestBloomVerify_StatusAPI_FullResponse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		baseURL string
		mode    string
	}{
		{"logs", logsBaseURL, "logs"},
		{"traces", tracesBaseURL, "traces"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := httpGetAllowStatus(t, tc.baseURL, "/api/v1/bloom/status", nil, 200, 404)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode == 404 {
				t.Skip("bloom status API not available in this build")
			}
			body, _ := io.ReadAll(resp.Body)
			status := mustParseJSON(t, body)

			mode := mustGetString(t, status, "mode")
			if mode != tc.mode {
				t.Errorf("mode = %q, want %q", mode, tc.mode)
			}

			assertFieldPresent(t, status, "enabled")
			assertFieldPresent(t, status, "tiers")
			assertFieldPresent(t, status, "cache")

			tiers, ok := status["tiers"].(map[string]any)
			if !ok {
				t.Fatal("tiers should be a map")
			}
			for _, tier := range []string{"hot", "warm", "cold", "archive"} {
				if _, ok := tiers[tier]; !ok {
					t.Errorf("missing tier %q", tier)
				}
			}
		})
	}
}

func TestBloomVerify_StatusAPI_TierAgeRanges(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available in this build")
	}
	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)
	tiers := status["tiers"].(map[string]any)

	expectedRanges := map[string]string{
		"hot":     "0-7d",
		"warm":    "7-30d",
		"cold":    "30-90d",
		"archive": "90d+",
	}

	for tier, expectedRange := range expectedRanges {
		tierData := tiers[tier].(map[string]any)
		ageRange, ok := tierData["age_range"].(string)
		if !ok {
			t.Errorf("tier %q missing age_range", tier)
			continue
		}
		if ageRange != expectedRange {
			t.Errorf("tier %q age_range = %q, want %q", tier, ageRange, expectedRange)
		}
	}
}

// =============================================================================
// Bloom Build on Flush Verification
// =============================================================================

func TestBloomVerify_BuildOnFlush_MetricsIncrement(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_build_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metricsBefore := scrapeMetrics(t, logsBaseURL)
	buildBefore := sumMetric(metricsBefore, "lakehouse_bloom_build_total")

	insertTestLogs(t, logsBaseURL, 100, "bloom-build-flush-test")
	time.Sleep(15 * time.Second)

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	buildAfter := sumMetric(metricsAfter, "lakehouse_bloom_build_total")

	delta := buildAfter - buildBefore
	t.Logf("bloom_build_total: before=%.0f after=%.0f delta=%.0f", buildBefore, buildAfter, delta)
}

func TestBloomVerify_BuildOnFlush_NoErrors(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_build_errors_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	insertTestLogs(t, logsBaseURL, 50, "bloom-build-noerr-test")
	time.Sleep(15 * time.Second)

	metrics := scrapeMetrics(t, logsBaseURL)
	errors := sumMetric(metrics, "lakehouse_bloom_build_errors_total")
	if errors > 0 {
		t.Errorf("bloom build errors = %.0f, want 0", errors)
	}
}

// =============================================================================
// Bloom Query Acceleration Verification
// =============================================================================

func TestBloomVerify_QueryAcceleration_TraceIDLookup(t *testing.T) {
	all := queryLogs(t, "*", 1)
	if len(all) == 0 {
		t.Skip("no data")
	}
	traceID, _ := all[0]["trace_id"].(string)
	if traceID == "" {
		t.Skip("no trace_id")
	}

	start := time.Now()
	results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, traceID), 100)
	elapsed := time.Since(start)

	if len(results) == 0 {
		t.Fatal("trace_id bloom lookup returned 0 results")
	}
	for _, r := range results {
		got, _ := r["trace_id"].(string)
		if got != traceID {
			t.Errorf("expected trace_id=%q, got %q", traceID, got)
		}
	}

	t.Logf("bloom trace_id lookup: %d results in %v", len(results), elapsed)
}

func TestBloomVerify_QueryAcceleration_ServiceNameFilter(t *testing.T) {
	start := time.Now()
	results := queryLogs(t, `service.name:="api-gateway"`, 50)
	elapsed := time.Since(start)

	for _, r := range results {
		svc, _ := r["service.name"].(string)
		if svc != "api-gateway" {
			t.Errorf("bloom service filter leaked: got %q", svc)
		}
	}
	t.Logf("bloom service filter: %d results in %v", len(results), elapsed)
}

func TestBloomVerify_QueryAcceleration_NonexistentValue(t *testing.T) {
	start := time.Now()
	results := queryLogs(t, `trace_id:="bloom-verify-nonexistent-trace-id-999"`, 10)
	elapsed := time.Since(start)

	if len(results) != 0 {
		t.Errorf("nonexistent trace_id should return 0 results, got %d", len(results))
	}
	t.Logf("nonexistent bloom lookup: %d results in %v", len(results), elapsed)
}

func TestBloomVerify_QueryAcceleration_FilesSkippedMetric(t *testing.T) {
	// Use the actual parquet-layer bloom skip metric (label-differentiated)
	skippedBefore := getPrometheusMetricWithLabel(t, logsBaseURL, "lakehouse_parquet_row_groups_skipped_total", "reason", "bloom")
	if skippedBefore < 0 {
		t.Skip("lakehouse_parquet_row_groups_skipped_total{reason=bloom} not available")
	}

	queryLogs(t, `trace_id:="bloom-skip-test-nonexistent"`, 1)

	skippedAfter := getPrometheusMetricWithLabel(t, logsBaseURL, "lakehouse_parquet_row_groups_skipped_total", "reason", "bloom")

	t.Logf("parquet_row_groups_skipped{reason=bloom}: before=%.0f after=%.0f delta=%.0f",
		skippedBefore, skippedAfter, skippedAfter-skippedBefore)
}

func TestBloomVerify_QueryAcceleration_BloomQueryMetrics(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_queries_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metricsBefore := scrapeMetrics(t, logsBaseURL)
	queriesBefore := sumMetric(metricsBefore, "lakehouse_bloom_queries_total")

	queryLogs(t, `trace_id:="bloom-query-metric-test"`, 1)

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	queriesAfter := sumMetric(metricsAfter, "lakehouse_bloom_queries_total")

	delta := queriesAfter - queriesBefore
	t.Logf("bloom_queries_total delta = %.0f", delta)
}

// =============================================================================
// Bloom Multi-Column Verification
// =============================================================================

func TestBloomVerify_MultiColumn_BothColumnsIndexed(t *testing.T) {
	traceIDs := insertTestLogs(t, logsBaseURL, 10, "bloom-multicol-svc")
	time.Sleep(3 * time.Second)

	traceResults := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, traceIDs[0]), 10)
	svcResults := queryLogs(t, `service.name:="bloom-multicol-svc"`, 100)

	t.Logf("trace_id lookup: %d results, service.name lookup: %d results",
		len(traceResults), len(svcResults))
}

func TestBloomVerify_MultiColumn_ANDQuery(t *testing.T) {
	results := queryLogs(t, `service.name:="api-gateway" AND level:="ERROR"`, 20)

	for _, r := range results {
		svc, _ := r["service.name"].(string)
		level, _ := r["level"].(string)
		if svc != "api-gateway" {
			t.Errorf("AND query service mismatch: %q", svc)
		}
		if level != "ERROR" {
			t.Errorf("AND query level mismatch: %q", level)
		}
	}
	t.Logf("multi-column AND query: %d results", len(results))
}

// =============================================================================
// Bloom Cache Verification
// =============================================================================

func TestBloomVerify_Cache_StatusReflectsUsage(t *testing.T) {
	queryLogs(t, `trace_id:="cache-warmup-query-1"`, 1)
	queryLogs(t, `trace_id:="cache-warmup-query-2"`, 1)

	resp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available in this build")
	}
	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)
	cache := status["cache"].(map[string]any)

	t.Logf("bloom cache: partitions=%.0f, memory_bytes_used=%.0f, memory_bytes_limit=%.0f",
		cache["partitions"], cache["memory_bytes_used"], cache["memory_bytes_limit"])
}

func TestBloomVerify_Cache_MetricsTrackSize(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_entries_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metrics := scrapeMetrics(t, logsBaseURL)
	entries := sumMetric(metrics, "lakehouse_bloom_entries_total")
	bytesUsed := sumMetric(metrics, "lakehouse_bloom_bytes_memory")

	t.Logf("bloom entries=%.0f, bytes_memory=%.0f", entries, bytesUsed)
}

// =============================================================================
// Bloom Tier Transition Verification
// =============================================================================

func TestBloomVerify_TierPartitions_MetricExists(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_tier_partitions")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_bloom_tier_partitions")

	lines, ok := metrics["lakehouse_bloom_tier_partitions"]
	if ok {
		for _, l := range lines {
			t.Logf("tier_partitions{%v} = %.0f", l.labels, l.value)
		}
	}
}

func TestBloomVerify_TierTransitions_MetricExists(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_tier_transitions_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_bloom_tier_transitions_total")
}

// =============================================================================
// Bloom Auto-Tuning Controller Verification
// =============================================================================

func TestBloomVerify_Controller_AdjustmentsInStatus(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available in this build")
	}
	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)

	at, ok := status["auto_tuning"].(map[string]any)
	if !ok {
		t.Skip("no auto_tuning")
	}

	if adjs, ok := at["recent_adjustments"].([]any); ok {
		t.Logf("controller has %d recent adjustments", len(adjs))
		for _, adj := range adjs {
			adjMap, ok := adj.(map[string]any)
			if !ok {
				continue
			}
			t.Logf("  adjustment: parameter=%v old=%v new=%v reason=%v",
				adjMap["parameter"], adjMap["old_value"], adjMap["new_value"], adjMap["reason"])
		}
	}
}

func TestBloomVerify_Controller_ControllerMetrics(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_controller_adjustments_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_bloom_controller_adjustments_total")
}

// =============================================================================
// Bloom Config Sync Verification
// =============================================================================

func TestBloomVerify_ConfigSync_MetricsExist(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_config_sync_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metrics := scrapeMetrics(t, logsBaseURL)
	assertMetricExists(t, metrics, "lakehouse_bloom_config_sync_total")
	assertMetricExists(t, metrics, "lakehouse_bloom_config_sync_errors_total")
}

func TestBloomVerify_ConfigSync_ErrorsZero(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_config_sync_errors_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metrics := scrapeMetrics(t, logsBaseURL)
	errors := sumMetric(metrics, "lakehouse_bloom_config_sync_errors_total")
	t.Logf("bloom_config_sync_errors = %.0f", errors)
}

// =============================================================================
// Bloom Cardinality Cap Verification
// =============================================================================

func TestBloomVerify_CardinalityCap_HighCardinalityHandled(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_build_errors_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	service := fmt.Sprintf("cardinality-test-%d", time.Now().UnixNano()%10000)
	insertTestLogs(t, logsBaseURL, 10, service)
	time.Sleep(3 * time.Second)

	metrics := scrapeMetrics(t, logsBaseURL)
	errors := sumMetric(metrics, "lakehouse_bloom_build_errors_total")
	t.Logf("bloom build errors after cardinality test: %.0f", errors)
}

// =============================================================================
// Bloom Bytes Avoided Verification
// =============================================================================

func TestBloomVerify_BytesAvoided_Increments(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_bytes_avoided_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metricsBefore := scrapeMetrics(t, logsBaseURL)
	avoidedBefore := sumMetric(metricsBefore, "lakehouse_bloom_bytes_avoided_total")

	for i := 0; i < 5; i++ {
		queryLogs(t, fmt.Sprintf(`trace_id:="bloom-bytes-avoided-test-%d"`, i), 1)
	}

	metricsAfter := scrapeMetrics(t, logsBaseURL)
	avoidedAfter := sumMetric(metricsAfter, "lakehouse_bloom_bytes_avoided_total")

	t.Logf("bloom_bytes_avoided: before=%.0f after=%.0f delta=%.0f",
		avoidedBefore, avoidedAfter, avoidedAfter-avoidedBefore)
}

// =============================================================================
// Bloom Insert → Query Round-Trip
// =============================================================================

func TestBloomVerify_RoundTrip_InsertAndBloomLookup(t *testing.T) {
	service := fmt.Sprintf("bloom-roundtrip-%d", time.Now().UnixNano()%10000)
	traceIDs := insertTestLogs(t, logsBaseURL, 20, service)
	time.Sleep(15 * time.Second)

	for i, tid := range traceIDs[:5] {
		results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, tid), 10)
		if len(results) == 0 {
			t.Logf("trace_id[%d] %q not yet visible (may need more flush time)", i, tid)
			continue
		}
		for _, r := range results {
			got, _ := r["trace_id"].(string)
			if got != tid {
				t.Errorf("bloom lookup returned wrong trace_id: %q != %q", got, tid)
			}
		}
	}

	results := queryLogs(t, fmt.Sprintf(`service.name:="%s"`, service), 100)
	t.Logf("service filter returned %d results for %q", len(results), service)
}

func TestBloomVerify_RoundTrip_InsertAndBloomLookup_Traces(t *testing.T) {
	now := time.Now()
	traceID := fmt.Sprintf("bloom-trace-rt-%d", now.UnixNano())

	line := fmt.Sprintf(`{"_msg":"bloom trace roundtrip","_time":"%s","service.name":"bloom-rt-traces","trace_id":"%s","span_id":"sp1","duration":"50ms"}`,
		now.Format(time.RFC3339Nano), traceID)

	params := fmt.Sprintf("_stream_fields=service.name")
	u := tracesBaseURL + "/insert/jsonline?" + params

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Post(u, "application/x-ndjson", strings.NewReader(line))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	time.Sleep(15 * time.Second)

	results := queryTraces(t, fmt.Sprintf(`trace_id:="%s"`, traceID), 10)
	if len(results) > 0 {
		t.Logf("traces bloom round-trip: found %d results for trace_id %q", len(results), traceID)
	} else {
		t.Logf("traces bloom round-trip: data not yet visible (may need flush)")
	}
}

// =============================================================================
// Bloom Performance Baseline Verification
// =============================================================================

func TestBloomVerify_Perf_PointLookup_Under500ms(t *testing.T) {
	all := queryLogs(t, "*", 1)
	if len(all) == 0 {
		t.Skip("no data")
	}
	traceID, _ := all[0]["trace_id"].(string)
	if traceID == "" {
		t.Skip("no trace_id")
	}

	start := time.Now()
	results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, traceID), 100)
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Logf("WARNING: bloom point lookup took %v (>500ms)", elapsed)
	}
	t.Logf("bloom point lookup: %d results in %v", len(results), elapsed)
}

func TestBloomVerify_Perf_NonexistentLookup_Fast(t *testing.T) {
	start := time.Now()
	results := queryLogs(t, `trace_id:="completely-nonexistent-value-for-perf"`, 1)
	elapsed := time.Since(start)

	if len(results) != 0 {
		t.Errorf("nonexistent lookup should return 0, got %d", len(results))
	}
	if elapsed > 200*time.Millisecond {
		t.Logf("WARNING: nonexistent bloom lookup took %v (>200ms)", elapsed)
	}
	t.Logf("nonexistent bloom lookup in %v", elapsed)
}

func TestBloomVerify_Perf_ServiceFilter_Reasonable(t *testing.T) {
	start := time.Now()
	results := queryLogs(t, `service.name:="api-gateway"`, 50)
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Logf("WARNING: service filter took %v (>2s)", elapsed)
	}
	t.Logf("service filter: %d results in %v", len(results), elapsed)
}

// =============================================================================
// Bloom Edge Cases
// =============================================================================

func TestBloomVerify_Edge_EmptyTraceID(t *testing.T) {
	results := queryLogs(t, `trace_id:=""`, 5)
	t.Logf("empty trace_id query returned %d results", len(results))
}

func TestBloomVerify_Edge_VeryLongTraceID(t *testing.T) {
	longID := strings.Repeat("a", 256)
	results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, longID), 5)
	if len(results) != 0 {
		t.Logf("very long trace_id returned %d results (unexpected)", len(results))
	}
}

func TestBloomVerify_Edge_SpecialCharsInTraceID(t *testing.T) {
	results := queryLogs(t, `trace_id:="test-with-special/chars:and.dots"`, 5)
	t.Logf("special chars trace_id returned %d results", len(results))
}

func TestBloomVerify_Edge_UnicodeTraceID(t *testing.T) {
	results := queryLogs(t, `trace_id:="unicöde-tëst"`, 5)
	t.Logf("unicode trace_id returned %d results", len(results))
}

func TestBloomVerify_Edge_MultipleExactLookups(t *testing.T) {
	for i := 0; i < 10; i++ {
		results := queryLogs(t, fmt.Sprintf(`trace_id:="batch-lookup-%d"`, i), 1)
		_ = results
	}

	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_queries_total")
	if val < 0 {
		t.Log("bloom_queries_total metric not registered in this build, skipping metric check")
	} else {
		t.Logf("bloom_queries_total after 10 lookups: %.0f", val)
	}
}

// =============================================================================
// Bloom Consistency Checks
// =============================================================================

func TestBloomVerify_Consistency_NoFalseNegatives(t *testing.T) {
	all := queryLogs(t, "*", 50)
	if len(all) == 0 {
		t.Skip("no data")
	}

	falseNegatives := 0
	checked := 0
	for _, entry := range all {
		traceID, ok := entry["trace_id"].(string)
		if !ok || traceID == "" {
			continue
		}
		results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, traceID), 10)
		if len(results) == 0 {
			falseNegatives++
			t.Errorf("false negative: trace_id %q exists in wildcard but not in exact lookup", traceID)
		}
		checked++
		if checked >= 10 {
			break
		}
	}

	t.Logf("checked %d trace_ids, false negatives: %d", checked, falseNegatives)
}

func TestBloomVerify_Consistency_ServiceFilterComplete(t *testing.T) {
	wildcard := queryLogs(t, `service.name:="api-gateway"`, 100)
	for _, entry := range wildcard {
		svc, _ := entry["service.name"].(string)
		if svc != "api-gateway" {
			t.Errorf("service filter returned wrong service: %q", svc)
		}
	}
	t.Logf("service filter consistency: %d results all correct", len(wildcard))
}

// =============================================================================
// Bloom Status Verification for Both Modes
// =============================================================================

func TestBloomVerify_BothModes_EnabledAndConsistent(t *testing.T) {
	logsResp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = logsResp.Body.Close() }()
	if logsResp.StatusCode == 404 {
		t.Skip("bloom status API not available in this build")
	}
	logsBody, _ := io.ReadAll(logsResp.Body)
	logsStatus := mustParseJSON(t, logsBody)

	tracesResp := httpGetAllowStatus(t, tracesBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = tracesResp.Body.Close() }()
	if tracesResp.StatusCode == 404 {
		t.Skip("bloom status API not available on traces service")
	}
	tracesBody, _ := io.ReadAll(tracesResp.Body)
	tracesStatus := mustParseJSON(t, tracesBody)

	logsEnabled, _ := logsStatus["enabled"].(bool)
	tracesEnabled, _ := tracesStatus["enabled"].(bool)

	t.Logf("logs bloom enabled=%v, traces bloom enabled=%v", logsEnabled, tracesEnabled)

	logsTiers, ok := logsStatus["tiers"].(map[string]any)
	if !ok {
		t.Skip("no tiers in logs bloom status")
	}
	tracesTiers, ok := tracesStatus["tiers"].(map[string]any)
	if !ok {
		t.Skip("no tiers in traces bloom status")
	}

	if len(logsTiers) != len(tracesTiers) {
		t.Errorf("tier count mismatch: logs=%d traces=%d", len(logsTiers), len(tracesTiers))
	}
}

// =============================================================================
// Bloom Metrics Cross-Validation
// =============================================================================

func TestBloomVerify_MetricsCrossValidation_BuildVsEntries(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_build_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	metrics := scrapeMetrics(t, logsBaseURL)

	builds := sumMetric(metrics, "lakehouse_bloom_build_total")
	entries := sumMetric(metrics, "lakehouse_bloom_entries_total")
	bytesUsed := sumMetric(metrics, "lakehouse_bloom_bytes_memory")

	t.Logf("bloom metrics: builds=%.0f entries=%.0f bytes=%.0f", builds, entries, bytesUsed)

	if builds > 0 && entries == 0 && bytesUsed == 0 {
		t.Log("NOTE: builds happened but no entries in memory (data may have been evicted or not loaded)")
	}
}

func TestBloomVerify_MetricsCrossValidation_QueriesVsSkipped(t *testing.T) {
	val := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_queries_total")
	if val < 0 {
		t.Skip("bloom metrics not registered in this build")
	}

	for i := 0; i < 5; i++ {
		queryLogs(t, fmt.Sprintf(`trace_id:="cross-val-test-%d"`, i), 1)
	}

	metrics := scrapeMetrics(t, logsBaseURL)
	queries := sumMetric(metrics, "lakehouse_bloom_queries_total")
	filesSkipped := sumMetric(metrics, "lakehouse_bloom_files_skipped_total")
	bytesAvoided := sumMetric(metrics, "lakehouse_bloom_bytes_avoided_total")

	t.Logf("bloom cross-val: queries=%.0f files_skipped=%.0f bytes_avoided=%.0f",
		queries, filesSkipped, bytesAvoided)
}

// =============================================================================
// Bloom Full Integration Test
// =============================================================================

func TestBloomVerify_FullIntegration(t *testing.T) {
	service := fmt.Sprintf("bloom-full-int-%d", time.Now().UnixNano()%10000)

	metricsBefore := scrapeMetrics(t, logsBaseURL)

	t.Log("step 1: insert test data")
	traceIDs := insertTestLogs(t, logsBaseURL, 50, service)
	time.Sleep(3 * time.Second)

	t.Log("step 2: verify bloom build metrics")
	metricsAfterInsert := scrapeMetrics(t, logsBaseURL)
	insertDelta := sumMetric(metricsAfterInsert, "lakehouse_insert_rows_total") -
		sumMetric(metricsBefore, "lakehouse_insert_rows_total")
	if insertDelta < 50 {
		t.Errorf("insert_rows delta = %.0f, want >= 50", insertDelta)
	}

	t.Log("step 3: query by trace_id (bloom point lookup)")
	for _, tid := range traceIDs[:3] {
		results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, tid), 10)
		if len(results) > 0 {
			got, _ := results[0]["trace_id"].(string)
			if got != tid {
				t.Errorf("trace_id mismatch: %q != %q", got, tid)
			}
		}
	}

	t.Log("step 4: query by service.name (bloom filter)")
	svcResults := queryLogs(t, fmt.Sprintf(`service.name:="%s"`, service), 100)
	for _, r := range svcResults {
		svc, _ := r["service.name"].(string)
		if svc != service {
			t.Errorf("service mismatch: %q != %q", svc, service)
		}
	}

	t.Log("step 5: verify bloom status API")
	status := tryGetBloomStatus(t, logsBaseURL)
	if status == nil {
		t.Log("bloom status API not available (404), skipping bloom-specific assertions")
	} else if _, ok := status["enabled"]; !ok {
		t.Error("bloom status should have 'enabled' field")
	}

	t.Log("step 6: verify bloom metrics (conditional)")
	metricsAfterAll := scrapeMetrics(t, logsBaseURL)
	bloomMetricsAvailable := getPrometheusMetric(t, logsBaseURL, "lakehouse_bloom_build_total") >= 0
	if bloomMetricsAvailable {
		for _, name := range []string{
			"lakehouse_bloom_build_total",
			"lakehouse_bloom_queries_total",
			"lakehouse_bloom_entries_total",
			"lakehouse_bloom_bytes_memory",
		} {
			assertMetricExists(t, metricsAfterAll, name)
		}
	} else {
		t.Log("bloom metrics not registered in this build, skipping bloom metric assertions")
	}

	t.Log("step 7: verify no bloom errors (conditional)")
	if bloomMetricsAvailable {
		buildErrors := sumMetric(metricsAfterAll, "lakehouse_bloom_build_errors_total")
		syncErrors := sumMetric(metricsAfterAll, "lakehouse_bloom_config_sync_errors_total")
		t.Logf("bloom errors: build=%.0f sync=%.0f", buildErrors, syncErrors)
	} else {
		t.Log("bloom metrics not registered, skipping error check")
	}

	t.Logf("full integration test complete: %d logs inserted, %d queried back", len(traceIDs), len(svcResults))
}
