//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// Health & Readiness Endpoints
// =============================================================================

func TestEndpoint_Logs_Health(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/health", nil)
	if string(body) != "OK" {
		t.Errorf("expected 'OK', got %q", string(body))
	}
}

func TestEndpoint_Traces_Health(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/health", nil)
	if string(body) != "OK" {
		t.Errorf("expected 'OK', got %q", string(body))
	}
}

func TestEndpoint_Logs_Ready(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/ready", nil, 200, 503)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 200 {
		if !strings.Contains(string(body), "READY") {
			t.Errorf("expected READY in body, got %q", string(body))
		}
	}
	t.Logf("/ready status=%d body=%q", resp.StatusCode, string(body))
}

func TestEndpoint_Traces_Ready(t *testing.T) {
	resp := httpGetAllowStatus(t, tracesBaseURL, "/ready", nil, 200, 503)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	t.Logf("/ready status=%d body=%q", resp.StatusCode, string(body))
}

// =============================================================================
// Lakehouse Info Endpoint
// =============================================================================

func TestEndpoint_Logs_LakehouseInfo(t *testing.T) {
	info := getLakehouseInfo(t, logsBaseURL)

	mustFields := []string{"mode", "ready", "phase"}
	for _, f := range mustFields {
		assertFieldPresent(t, info, f)
	}

	if mode := info["mode"]; mode != "logs" {
		t.Errorf("mode = %v, want 'logs'", mode)
	}
	if ready := info["ready"]; ready != true {
		t.Errorf("ready = %v, want true", ready)
	}
	if phase := info["phase"]; phase != "ready" {
		t.Errorf("phase = %v, want 'ready'", phase)
	}
	if _, ok := info["vl_compat"]; !ok {
		t.Error("missing vl_compat field in logs info")
	}
}

func TestEndpoint_Traces_LakehouseInfo(t *testing.T) {
	info := getLakehouseInfo(t, tracesBaseURL)

	if mode := info["mode"]; mode != "traces" {
		t.Errorf("mode = %v, want 'traces'", mode)
	}
	if ready := info["ready"]; ready != true {
		t.Errorf("ready = %v, want true", ready)
	}
	if _, ok := info["vt_compat"]; !ok {
		t.Error("missing vt_compat field in traces info")
	}
}

func TestEndpoint_LakehouseInfo_HasAllFields(t *testing.T) {
	for _, tc := range []struct {
		name    string
		baseURL string
	}{
		{"logs", logsBaseURL},
		{"traces", tracesBaseURL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := getLakehouseInfo(t, tc.baseURL)
			for _, field := range []string{"mode", "ready", "phase"} {
				if _, ok := info[field]; !ok {
					t.Errorf("missing field %q", field)
				}
			}
		})
	}
}

// =============================================================================
// Manifest Range Endpoint
// =============================================================================

func TestEndpoint_Logs_ManifestRange(t *testing.T) {
	m := getManifestRange(t, logsBaseURL)

	for _, f := range []string{"totalFiles", "totalBytes"} {
		assertFieldPresent(t, m, f)
	}

	files := mustGetFloat(t, m, "totalFiles")
	if files <= 0 {
		t.Errorf("totalFiles = %f, want > 0", files)
	}

	totalBytes := mustGetFloat(t, m, "totalBytes")
	if totalBytes <= 0 {
		t.Errorf("totalBytes = %f, want > 0", totalBytes)
	}

	t.Logf("manifest: %.0f files, %.0f bytes", files, totalBytes)
}

func TestEndpoint_Traces_ManifestRange(t *testing.T) {
	m := getManifestRange(t, tracesBaseURL)

	files := mustGetFloat(t, m, "totalFiles")
	if files <= 0 {
		t.Errorf("totalFiles = %f, want > 0", files)
	}
	t.Logf("traces manifest: %.0f files", files)
}

func TestEndpoint_ManifestRange_TimeRanges(t *testing.T) {
	m := getManifestRange(t, logsBaseURL)

	for _, f := range []string{"minDate", "maxDate"} {
		if _, ok := m[f]; ok {
			t.Logf("%s = %v", f, m[f])
		}
	}

	if minTime, ok := m["minTime"]; ok {
		if maxTime, ok2 := m["maxTime"]; ok2 {
			t.Logf("time range: %v to %v", minTime, maxTime)
		}
	}
}

// =============================================================================
// Bloom Status Endpoint
// =============================================================================

func TestEndpoint_Logs_BloomStatus(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available")
	}

	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)

	assertFieldPresent(t, status, "enabled")
	assertFieldPresent(t, status, "mode")
	assertFieldPresent(t, status, "tiers")
	assertFieldPresent(t, status, "cache")

	if mode := mustGetString(t, status, "mode"); mode != "logs" {
		t.Errorf("mode = %q, want 'logs'", mode)
	}
}

func TestEndpoint_Traces_BloomStatus(t *testing.T) {
	resp := httpGetAllowStatus(t, tracesBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available")
	}

	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)

	if mode := mustGetString(t, status, "mode"); mode != "traces" {
		t.Errorf("mode = %q, want 'traces'", mode)
	}
}

func TestEndpoint_BloomStatus_TierStructure(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available")
	}

	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)

	tiersRaw, ok := status["tiers"]
	if !ok {
		t.Fatal("missing tiers field")
	}
	tiers, ok := tiersRaw.(map[string]any)
	if !ok {
		t.Fatalf("tiers is %T, want map", tiersRaw)
	}

	expectedTiers := []string{"hot", "warm", "cold", "archive"}
	for _, tier := range expectedTiers {
		tierData, ok := tiers[tier]
		if !ok {
			t.Errorf("missing tier %q", tier)
			continue
		}
		tierMap, ok := tierData.(map[string]any)
		if !ok {
			t.Errorf("tier %q is %T, want map", tier, tierData)
			continue
		}
		if _, ok := tierMap["age_range"]; !ok {
			t.Errorf("tier %q missing age_range", tier)
		}
		t.Logf("tier %s: %v", tier, tierMap)
	}
}

func TestEndpoint_BloomStatus_AutoTuning(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available")
	}

	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)

	at, ok := status["auto_tuning"]
	if !ok {
		t.Skip("no auto_tuning field (controller may be nil)")
	}

	atMap, ok := at.(map[string]any)
	if !ok {
		t.Fatalf("auto_tuning is %T, want map", at)
	}

	for _, field := range []string{
		"tier1_max_age", "tier2_max_age", "tier3_max_age",
		"target_file_size", "partition_granularity", "cache_max_bytes",
	} {
		if _, ok := atMap[field]; !ok {
			t.Errorf("auto_tuning missing field %q", field)
		}
	}

	if gran, ok := atMap["partition_granularity"].(string); ok {
		if gran != "hour" && gran != "day" {
			t.Errorf("partition_granularity = %q, want 'hour' or 'day'", gran)
		}
	}
}

func TestEndpoint_BloomStatus_CacheStats(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available")
	}

	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)

	cache, ok := status["cache"]
	if !ok {
		t.Fatal("missing cache field")
	}
	cacheMap, ok := cache.(map[string]any)
	if !ok {
		t.Fatalf("cache is %T, want map", cache)
	}

	for _, field := range []string{"memory_bytes_used", "memory_bytes_limit", "partitions"} {
		if _, ok := cacheMap[field]; !ok {
			t.Errorf("cache missing field %q", field)
		}
	}
}

// =============================================================================
// Internal Cache Stats Endpoint
// =============================================================================

func TestEndpoint_Logs_CacheStats(t *testing.T) {
	stats := getCacheStats(t, logsBaseURL)
	t.Logf("cache stats: %v", stats)

	if _, ok := stats["az"]; ok {
		t.Logf("az = %v", stats["az"])
	}
}

func TestEndpoint_Traces_CacheStats(t *testing.T) {
	stats := getCacheStats(t, tracesBaseURL)
	t.Logf("traces cache stats: %v", stats)
}

// =============================================================================
// Metrics Endpoint
// =============================================================================

func TestEndpoint_Logs_Metrics(t *testing.T) {
	u := logsBaseURL + "/metrics"
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		t.Fatalf("/metrics returned %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	lines := strings.Split(string(body), "\n")
	metricCount := 0
	for _, line := range lines {
		if line != "" && !strings.HasPrefix(line, "#") {
			metricCount++
		}
	}

	if metricCount < 50 {
		t.Errorf("expected at least 50 metric lines, got %d", metricCount)
	}
	t.Logf("/metrics has %d metric lines (total lines: %d)", metricCount, len(lines))
}

func TestEndpoint_Traces_Metrics(t *testing.T) {
	u := tracesBaseURL + "/metrics"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		t.Fatalf("/metrics returned %d", resp.StatusCode)
	}
}

// =============================================================================
// Query Endpoints — Logs
// =============================================================================

func TestEndpoint_Logs_Query_Wildcard(t *testing.T) {
	results := queryLogs(t, "*", 10)
	if len(results) == 0 {
		t.Fatal("wildcard query returned no results")
	}

	for _, r := range results {
		if _, ok := r["_msg"]; !ok {
			t.Error("missing _msg in query result")
		}
		if _, ok := r["_time"]; !ok {
			t.Error("missing _time in query result")
		}
	}
	t.Logf("wildcard query returned %d results", len(results))
}

func TestEndpoint_Logs_Query_ServiceFilter(t *testing.T) {
	results := queryLogs(t, `service.name:="api-gateway"`, 20)
	for _, r := range results {
		svc, _ := r["service.name"].(string)
		if svc != "api-gateway" {
			t.Errorf("expected service.name=api-gateway, got %q", svc)
		}
	}
	t.Logf("service filter returned %d results", len(results))
}

func TestEndpoint_Logs_Query_TraceIDExact(t *testing.T) {
	all := queryLogs(t, "*", 1)
	if len(all) == 0 {
		t.Skip("no data")
	}
	traceID, _ := all[0]["trace_id"].(string)
	if traceID == "" {
		t.Skip("no trace_id")
	}

	results := queryLogs(t, fmt.Sprintf(`trace_id:="%s"`, traceID), 100)
	if len(results) == 0 {
		t.Fatal("trace_id exact lookup returned 0 results")
	}
	for _, r := range results {
		got, _ := r["trace_id"].(string)
		if got != traceID {
			t.Errorf("trace_id mismatch: %q != %q", got, traceID)
		}
	}
}

func TestEndpoint_Logs_Query_AND(t *testing.T) {
	results := queryLogs(t, `service.name:="api-gateway" AND level:="ERROR"`, 20)
	for _, r := range results {
		svc, _ := r["service.name"].(string)
		level, _ := r["level"].(string)
		if svc != "api-gateway" {
			t.Errorf("expected service=api-gateway, got %q", svc)
		}
		if level != "ERROR" {
			t.Errorf("expected level=ERROR, got %q", level)
		}
	}
}

func TestEndpoint_Logs_Query_OR(t *testing.T) {
	results := queryLogs(t, `service.name:="api-gateway" OR service.name:="user-service"`, 20)
	for _, r := range results {
		svc, _ := r["service.name"].(string)
		if svc != "api-gateway" && svc != "user-service" {
			t.Errorf("expected api-gateway or user-service, got %q", svc)
		}
	}
}

func TestEndpoint_Logs_Query_NOT(t *testing.T) {
	results := queryLogs(t, `NOT level:="DEBUG"`, 20)
	for _, r := range results {
		level, _ := r["level"].(string)
		if level == "DEBUG" {
			t.Error("NOT filter should exclude DEBUG level")
		}
	}
}

func TestEndpoint_Logs_Query_Limit(t *testing.T) {
	results3 := queryLogs(t, "*", 3)
	results10 := queryLogs(t, "*", 10)
	if len(results3) > 3 {
		t.Errorf("limit=3 returned %d results", len(results3))
	}
	if len(results10) > 10 {
		t.Errorf("limit=10 returned %d results", len(results10))
	}
	t.Logf("limit=3 got %d, limit=10 got %d", len(results3), len(results10))
}

func TestEndpoint_Logs_Query_NoResults(t *testing.T) {
	results := queryLogs(t, `trace_id:="definitely-nonexistent-id-xyz-99999"`, 10)
	if len(results) != 0 {
		t.Errorf("expected 0 results for nonexistent trace_id, got %d", len(results))
	}
}

// =============================================================================
// Query Endpoints — Traces
// =============================================================================

func TestEndpoint_Traces_Query_Wildcard(t *testing.T) {
	results := queryTraces(t, "*", 10)
	if len(results) == 0 {
		t.Fatal("traces wildcard query returned no results")
	}
	t.Logf("traces wildcard returned %d results", len(results))
}

func TestEndpoint_Traces_Query_ServiceFilter(t *testing.T) {
	results := queryTraces(t, `service.name:="api-gateway"`, 10)
	for _, r := range results {
		svc, _ := r["service.name"].(string)
		if svc != "api-gateway" {
			t.Errorf("expected service.name=api-gateway, got %q", svc)
		}
	}
}

// =============================================================================
// Field Names & Values Endpoints
// =============================================================================

func TestEndpoint_Logs_FieldNames(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_names", params)

	// Response is a JSON object with a "values" array: {"values":[{"value":"field","hits":N},...]}
	var resp struct {
		Values []struct {
			Value string  `json:"value"`
			Hits  float64 `json:"hits"`
		} `json:"values"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("failed to parse field_names response: %v\nraw: %s", err, string(body))
	}

	if len(resp.Values) == 0 {
		t.Fatal("field_names returned no results")
	}

	var fieldNames []string
	for _, v := range resp.Values {
		fieldNames = append(fieldNames, v.Value)
	}

	requiredFields := []string{"_msg", "_time", "service.name", "trace_id", "level"}
	for _, req := range requiredFields {
		found := false
		for _, fn := range fieldNames {
			if fn == req {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("required field %q not found in field_names", req)
		}
	}
	t.Logf("field_names returned %d fields: %v", len(fieldNames), fieldNames)
}

func TestEndpoint_Logs_FieldValues_ServiceName(t *testing.T) {
	values := getFieldValues(t, logsBaseURL, "service.name")
	if len(values) == 0 {
		t.Fatal("field_values for service.name returned no results")
	}

	expectedServices := []string{"api-gateway", "user-service", "order-service", "payment-service", "notification-service"}
	for _, expected := range expectedServices {
		found := false
		for _, v := range values {
			if val, ok := v["value"].(string); ok && val == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected service %q not found in field_values", expected)
		}
	}
	t.Logf("service.name has %d distinct values", len(values))
}

func TestEndpoint_Logs_FieldValues_Level(t *testing.T) {
	values := getFieldValues(t, logsBaseURL, "level")
	if len(values) == 0 {
		t.Fatal("field_values for level returned no results")
	}

	expectedLevels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	for _, expected := range expectedLevels {
		found := false
		for _, v := range values {
			if val, ok := v["value"].(string); ok && val == expected {
				found = true
				break
			}
		}
		if !found {
			t.Logf("expected level %q not found (may be missing from datagen)", expected)
		}
	}
}

// =============================================================================
// Hits Endpoint
// =============================================================================

func TestEndpoint_Logs_Hits(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("step", "3600s")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/hits", params)

	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("failed to parse hits response: %v", err)
	}

	hits, ok := resp["hits"]
	if !ok {
		t.Fatal("missing 'hits' in response")
	}

	hitsArr, ok := hits.([]any)
	if !ok {
		t.Fatalf("hits is %T, want array", hits)
	}

	if len(hitsArr) == 0 {
		t.Fatal("hits returned empty array")
	}
	t.Logf("hits returned %d buckets", len(hitsArr))
}

func TestEndpoint_Logs_Hits_WithField(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("step", "3600s")
	params.Set("field", "level")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/hits", params)

	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("failed to parse hits response: %v", err)
	}

	hits, ok := resp["hits"].([]any)
	if !ok || len(hits) == 0 {
		t.Skip("no hits with field grouping")
	}

	for _, h := range hits {
		hMap, ok := h.(map[string]any)
		if !ok {
			continue
		}
		if fields, ok := hMap["fields"].(map[string]any); ok {
			if _, ok := fields["level"]; !ok {
				t.Error("expected 'level' in hit fields")
			}
		}
	}
	t.Logf("hits with field grouping returned %d groups", len(hits))
}

// =============================================================================
// Stats Query Endpoints
// =============================================================================

func TestEndpoint_Logs_StatsQuery(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `* | stats count() as total`)

	resp := httpGetAllowStatus(t, logsBaseURL, "/select/logsql/stats_query", params, 200, 422)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 422 {
		t.Skip("stats_query returned 422 (query format not supported)")
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse stats_query response: %v", err)
	}
	t.Logf("stats_query response: %v", result)
}

func TestEndpoint_Logs_StatsQueryRange(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `* | stats count() as total`)
	params.Set("step", "3600s")

	resp := httpGetAllowStatus(t, logsBaseURL, "/select/logsql/stats_query_range", params, 200, 422)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 422 {
		t.Skip("stats_query_range returned 422 (query format not supported)")
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse stats_query_range response: %v", err)
	}
	t.Logf("stats_query_range response keys: %v", mapKeys(result))
}

// =============================================================================
// Insert Endpoint
// =============================================================================

func TestEndpoint_Logs_Insert_Jsonline(t *testing.T) {
	traceIDs := insertTestLogs(t, logsBaseURL, 5, "endpoint-test-svc")
	if len(traceIDs) != 5 {
		t.Errorf("expected 5 trace IDs, got %d", len(traceIDs))
	}

	time.Sleep(3 * time.Second)

	results := queryLogs(t, fmt.Sprintf(`service.name:="endpoint-test-svc" AND trace_id:="%s"`, traceIDs[0]), 10)
	if len(results) == 0 {
		t.Logf("freshly inserted logs not yet visible (may need flush)")
	} else {
		t.Logf("inserted and queried back %d results", len(results))
	}
}

func TestEndpoint_Traces_Insert_Jsonline(t *testing.T) {
	now := time.Now()
	line := fmt.Sprintf(`{"_msg":"trace test","_time":"%s","service.name":"endpoint-trace-svc","trace_id":"ep-trace-1","span_id":"sp1","duration":"100ms"}`,
		now.Format(time.RFC3339Nano))

	params := url.Values{"_stream_fields": {"service.name"}}
	u := tracesBaseURL + "/insert/jsonline?" + params.Encode()

	resp, err := http.Post(u, "application/x-ndjson", strings.NewReader(line))
	if err != nil {
		t.Fatalf("POST %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("traces service uses OTLP insert (VT), not jsonline (VL)")
	}
	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("traces insert returned %d: %s", resp.StatusCode, string(body))
	}
	t.Log("traces insert succeeded")
}

// =============================================================================
// Multi-Tenant Endpoints
// =============================================================================

func TestEndpoint_MultiTenant_IsolatedQueries(t *testing.T) {
	results0 := queryWithTenant(t, logsBaseURL, "*", 10, "0", "0")
	results1 := queryWithTenant(t, logsBaseURL, "*", 10, "1", "1")

	t.Logf("tenant 0/0: %d results, tenant 1/1: %d results", len(results0), len(results1))

	if len(results0) == 0 && len(results1) == 0 {
		t.Skip("no data for either tenant")
	}
}

func TestEndpoint_MultiTenant_DataIsolation(t *testing.T) {
	traceIDs := insertTestLogs(t, logsBaseURL, 3, "tenant-isolation-svc")
	time.Sleep(3 * time.Second)

	results1 := queryWithTenant(t, logsBaseURL,
		fmt.Sprintf(`trace_id:="%s"`, traceIDs[0]), 10, "1", "1")

	if len(results1) > 0 {
		t.Error("tenant 1/1 should not see data inserted to default tenant")
	}
}

// =============================================================================
// Manifest Partitions Endpoint
// =============================================================================

func TestEndpoint_Logs_ManifestPartitions(t *testing.T) {
	now := time.Now()
	params := url.Values{
		"start": {now.Add(-72 * time.Hour).Format("2006-01-02")},
		"end":   {now.Format("2006-01-02")},
	}

	resp := httpGetAllowStatus(t, logsBaseURL, "/manifest/partitions", params, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("manifest/partitions endpoint not available")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("manifest/partitions response: %s", string(body)[:min(len(body), 500)])
}

// =============================================================================
// Stats API Endpoints
// =============================================================================

func TestEndpoint_Stats_Overview(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/lakehouse/api/v1/stats/overview", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("stats overview not available")
	}

	body, _ := io.ReadAll(resp.Body)
	var overview map[string]any
	if err := json.Unmarshal(body, &overview); err != nil {
		t.Fatalf("failed to parse overview: %v", err)
	}
	t.Logf("stats overview keys: %v", mapKeys(overview))
}

func TestEndpoint_Stats_Tenants(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/lakehouse/api/v1/tenants", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("tenants endpoint not available")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("tenants response (first 500 chars): %s", string(body)[:min(len(body), 500)])
}

func TestEndpoint_Stats_Ingestion(t *testing.T) {
	params := url.Values{"period": {"hour"}, "range": {"24h"}}
	resp := httpGetAllowStatus(t, logsBaseURL, "/lakehouse/api/v1/stats/ingestion", params, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("ingestion stats not available")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("ingestion stats: %s", string(body)[:min(len(body), 500)])
}

func TestEndpoint_Stats_Cost(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/lakehouse/api/v1/stats/cost", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("cost stats not available")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("cost stats: %s", string(body)[:min(len(body), 500)])
}

func TestEndpoint_Stats_Compression(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/lakehouse/api/v1/stats/compression", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("compression stats not available")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("compression stats: %s", string(body)[:min(len(body), 500)])
}

func TestEndpoint_Stats_Cardinality(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/lakehouse/api/v1/cardinality/fields", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("cardinality endpoint not available")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("cardinality fields: %s", string(body)[:min(len(body), 500)])
}

// =============================================================================
// Delete Endpoints
// =============================================================================

func TestEndpoint_Delete_Tombstones_List(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/delete/logsql/tombstones", nil, 200, 403, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 403 {
		t.Skip("delete not enabled")
	}
	if resp.StatusCode == 404 {
		t.Skip("delete endpoint not found")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("tombstones list: %s", string(body)[:min(len(body), 500)])
}

func TestEndpoint_Delete_Estimate(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="nonexistent-svc-for-estimate"`)

	u := logsBaseURL + "/delete/logsql/estimate?" + params.Encode()
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(u, "", nil)
	if err != nil {
		t.Fatalf("POST estimate failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 403 || resp.StatusCode == 404 {
		t.Skip("delete/estimate not available")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("delete estimate (nonexistent svc): status=%d body=%s", resp.StatusCode, string(body)[:min(len(body), 500)])
}

// =============================================================================
// Jaeger Endpoints (Traces only)
// =============================================================================

func TestEndpoint_Traces_Jaeger_Services(t *testing.T) {
	resp := httpGetAllowStatus(t, tracesBaseURL, "/api/services", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("jaeger services endpoint not available")
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse jaeger services: %v", err)
	}

	data, ok := result["data"]
	if !ok {
		t.Fatal("missing 'data' in jaeger services response")
	}

	services, ok := data.([]any)
	if !ok {
		t.Fatalf("data is %T, want array", data)
	}

	if len(services) == 0 {
		t.Error("jaeger returned 0 services")
	}
	t.Logf("jaeger services: %v", services)
}

func TestEndpoint_Traces_Jaeger_SearchTraces(t *testing.T) {
	params := url.Values{
		"service":  {"api-gateway"},
		"lookback": {"2h"},
		"limit":    {"5"},
	}

	resp := httpGetAllowStatus(t, tracesBaseURL, "/api/traces", params, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("jaeger traces endpoint not available")
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse jaeger traces: %v", err)
	}

	data, ok := result["data"].([]any)
	if !ok || len(data) == 0 {
		t.Skip("no traces found for api-gateway")
	}
	t.Logf("jaeger returned %d traces for api-gateway", len(data))
}

// =============================================================================
// Streams Endpoints
// =============================================================================

func TestEndpoint_Logs_Streams(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	resp := httpGetAllowStatus(t, logsBaseURL, "/select/logsql/streams", params, 200, 400, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("streams endpoint not available")
	}
	if resp.StatusCode == 400 {
		t.Skip("streams endpoint returned 400 (may need additional params)")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("streams response: %s", string(body)[:min(len(body), 500)])
}

func TestEndpoint_Logs_StreamFieldNames(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	resp := httpGetAllowStatus(t, logsBaseURL, "/select/logsql/stream_field_names", params, 200, 400, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("stream_field_names endpoint not available")
	}
	if resp.StatusCode == 400 {
		t.Skip("stream_field_names returned 400 (may need additional params)")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("stream_field_names: %s", string(body)[:min(len(body), 500)])
}

// =============================================================================
// Tenant IDs Endpoint
// =============================================================================

func TestEndpoint_Logs_TenantIDs(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/select/tenant_ids", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("tenant_ids endpoint not available")
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("tenant_ids: %s", string(body)[:min(len(body), 500)])
}

// =============================================================================
// UI Endpoint
// =============================================================================

func TestEndpoint_Logs_UI(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/lakehouse/ui/", nil, 200, 404, 301, 302)
	defer func() { _ = resp.Body.Close() }()

	t.Logf("/lakehouse/ui/ status=%d", resp.StatusCode)
}

// =============================================================================
// Tail Endpoint (expected 501)
// =============================================================================

func TestEndpoint_Logs_Tail_NotImplemented(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	resp := httpGetAllowStatus(t, logsBaseURL, "/select/logsql/tail", params, 200, 501, 404)
	defer func() { _ = resp.Body.Close() }()

	t.Logf("/select/logsql/tail status=%d", resp.StatusCode)
}

// =============================================================================
// Method Not Allowed Tests
// =============================================================================

func TestEndpoint_BloomStatus_MethodNotAllowed(t *testing.T) {
	u := logsBaseURL + "/api/v1/bloom/status"
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(u, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available")
	}

	if resp.StatusCode != 405 {
		t.Errorf("POST to bloom/status should return 405, got %d", resp.StatusCode)
	}
}

// =============================================================================
// Cross-Validation: Endpoint data consistency
// =============================================================================

func TestEndpoint_CrossValidation_InfoVsManifest(t *testing.T) {
	info := getLakehouseInfo(t, logsBaseURL)
	manifest := getManifestRange(t, logsBaseURL)

	infoReady := info["ready"]
	manifestFiles := manifest["totalFiles"]

	t.Logf("info.ready=%v manifest.totalFiles=%v", infoReady, manifestFiles)

	if infoReady == true {
		files, ok := manifestFiles.(float64)
		if ok && files <= 0 {
			t.Error("info says ready but manifest has 0 files")
		}
	}
}

func TestEndpoint_CrossValidation_LogsVsTraces_BothHealthy(t *testing.T) {
	logsHealth := httpGetBody(t, logsBaseURL, "/health", nil)
	tracesHealth := httpGetBody(t, tracesBaseURL, "/health", nil)

	if string(logsHealth) != "OK" {
		t.Errorf("logs health = %q", string(logsHealth))
	}
	if string(tracesHealth) != "OK" {
		t.Errorf("traces health = %q", string(tracesHealth))
	}
}

func TestEndpoint_CrossValidation_BloomStatusVsMetrics(t *testing.T) {
	resp := httpGetAllowStatus(t, logsBaseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		t.Skip("bloom status API not available")
	}

	body, _ := io.ReadAll(resp.Body)
	status := mustParseJSON(t, body)
	metrics := scrapeMetrics(t, logsBaseURL)

	enabled, _ := status["enabled"].(bool)
	if enabled {
		assertMetricExists(t, metrics, "lakehouse_bloom_build_total")
		assertMetricExists(t, metrics, "lakehouse_bloom_queries_total")
	}

	if cache, ok := status["cache"].(map[string]any); ok {
		if partitions, ok := cache["partitions"].(float64); ok {
			bloomEntries := sumMetric(metrics, "lakehouse_bloom_entries_total")
			t.Logf("bloom status partitions=%.0f, metric entries=%.0f", partitions, bloomEntries)
		}
	}
}

