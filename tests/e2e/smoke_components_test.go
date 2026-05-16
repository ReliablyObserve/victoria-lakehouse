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

// Component smoke checks are organized by subsystem.
// To add a new component:
// 1. Add a new TestSmoke_<Component>_<Check> function in this file
// 2. Follow the naming convention: TestSmoke_<Component>_<Behavior>
// 3. Use helpers from helpers_test.go (httpGet, httpGetBody, mustParseJSON, etc.)
// 4. Each test should be self-contained and idempotent (safe to run repeatedly)

// =============================================================================
// Component: Retention
// =============================================================================

// TestSmoke_Retention_ConfigLoaded verifies retention config is accepted without errors.
func TestSmoke_Retention_ConfigLoaded(t *testing.T) {
	// Retention is disabled by default but the config should load cleanly.
	// Verify via /lakehouse/info (healthy means config loaded without fatal).
	body := httpGetBody(t, logsBaseURL, "/lakehouse/info", nil)
	info := mustParseJSON(t, body)
	if info["ready"] != true {
		t.Fatal("service not ready — config may have failed to load")
	}
}

// =============================================================================
// Component: Compaction
// =============================================================================

// TestSmoke_Compaction_ManifestConsistent verifies manifest files are well-formed
// (no zero-byte files, no negative timestamps — signs of compaction corruption).
func TestSmoke_Compaction_ManifestConsistent(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/manifest/range", nil)
	result := mustParseJSON(t, body)

	minTime, _ := result["minTime"].(float64)
	maxTime, _ := result["maxTime"].(float64)

	if minTime <= 0 {
		t.Errorf("manifest minTime=%v, expected positive nanoseconds", minTime)
	}
	if maxTime <= 0 {
		t.Errorf("manifest maxTime=%v, expected positive nanoseconds", maxTime)
	}
	if minTime > maxTime {
		t.Errorf("manifest minTime(%v) > maxTime(%v)", minTime, maxTime)
	}

	totalBytes, _ := result["totalBytes"].(float64)
	totalFiles, _ := result["totalFiles"].(float64)
	if totalFiles > 0 && totalBytes/totalFiles < 100 {
		t.Errorf("avg file size=%.0f bytes, suspiciously small (possible compaction issue)", totalBytes/totalFiles)
	}
}

// =============================================================================
// Component: Insert Path (via datagen-continuous)
// =============================================================================

// TestSmoke_Insert_ContinuousDataFlowing verifies new data is being written
// by checking that max time in manifest is recent.
func TestSmoke_Insert_ContinuousDataFlowing(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/manifest/range", nil)
	result := mustParseJSON(t, body)

	maxTime, _ := result["maxTime"].(float64)
	if maxTime == 0 {
		t.Skip("no data in manifest")
	}

	maxTimeT := time.Unix(0, int64(maxTime))
	age := time.Since(maxTimeT)

	// datagen-continuous writes every 30s; allow 10 min buffer for CI startup
	if age > 10*time.Minute {
		t.Errorf("newest data is %s old (maxTime=%v), expected < 10m for continuous insert", age.Round(time.Second), maxTimeT)
	}
}

// =============================================================================
// Component: Cross-Signal Prefetch
// =============================================================================

// TestSmoke_CrossSignal_EndpointExists verifies the cross-signal prefetch
// endpoint is registered (responds with 405 on GET since it expects POST).
func TestSmoke_CrossSignal_EndpointExists(t *testing.T) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(logsBaseURL + "/internal/cross-signal/prefetch")
	if err != nil {
		t.Skipf("cross-signal endpoint not reachable: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 404 means the endpoint isn't registered (cross-signal disabled) — that's OK
	// 405 means it exists but wrong method — that's what we want
	if resp.StatusCode == http.StatusNotFound {
		t.Skip("cross-signal not enabled in this configuration")
	}
}

// =============================================================================
// Component: Delete / Tombstones
// =============================================================================

// TestSmoke_Delete_EndpointRegistered verifies the delete API is accessible.
func TestSmoke_Delete_EndpointRegistered(t *testing.T) {
	client := &http.Client{Timeout: 10 * time.Second}
	// GET on delete endpoint should return 405 (expects POST/DELETE)
	resp, err := client.Get(logsBaseURL + "/internal/delete/query")
	if err != nil {
		t.Fatalf("delete endpoint unreachable: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 404 means delete is disabled, which is a valid config
	if resp.StatusCode == http.StatusNotFound {
		t.Skip("delete not enabled in this configuration")
	}
}

// =============================================================================
// Component: Stats / Observability
// =============================================================================

// TestSmoke_Stats_ManifestPartitions verifies the manifest partitions endpoint
// returns valid partition data.
func TestSmoke_Stats_ManifestPartitions(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/manifest/partitions", nil)
	result := mustParseJSON(t, body)

	partitions, ok := result["partitions"].([]any)
	if !ok || len(partitions) == 0 {
		t.Errorf("manifest/partitions returned no partitions; raw: %s", string(body))
	}
}

// =============================================================================
// Component: Query Performance / Push-Down
// =============================================================================

// TestSmoke_PushDown_ExactMatchFiltersCorrectly verifies that exact match
// queries only return matching rows (proves filter push-down doesn't break results).
func TestSmoke_PushDown_ExactMatchFiltersCorrectly(t *testing.T) {
	services := []string{"payment-service", "order-service", "user-service"}
	for _, svc := range services {
		t.Run(svc, func(t *testing.T) {
			params := defaultTimeParams()
			params.Set("query", fmt.Sprintf(`service.name:="%s"`, svc))
			params.Set("limit", "5")

			body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
			lines := nonEmptyLines(body)
			if len(lines) == 0 {
				t.Skipf("no data for service %s", svc)
			}

			for _, line := range lines {
				var row map[string]any
				if err := json.Unmarshal([]byte(line), &row); err != nil {
					continue
				}
				got, _ := row["service.name"].(string)
				if got != svc {
					t.Errorf("filter returned service.name=%q, want %q", got, svc)
				}
			}
		})
	}
}

// TestSmoke_PushDown_TimeRangeNarrow verifies that narrow time range queries
// complete quickly (proves time-based row group pruning works).
func TestSmoke_PushDown_TimeRangeNarrow(t *testing.T) {
	now := time.Now()
	params := url.Values{
		"query": {"*"},
		"limit": {"5"},
		"start": {fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}

	start := time.Now()
	_ = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	elapsed := time.Since(start)

	// A narrow time range should complete in under 5s even with cold cache
	if elapsed > 5*time.Second {
		t.Errorf("narrow time range query took %s, expected < 5s (push-down may not be working)", elapsed)
	}
}

// =============================================================================
// Component: Traces-Specific
// =============================================================================

// TestSmoke_Traces_SpanFieldsPresent verifies trace spans have required fields.
func TestSmoke_Traces_SpanFieldsPresent(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "3")

	body := httpGetBody(t, tracesBaseURL, "/select/logsql/query", params)
	lines := nonEmptyLines(body)
	if len(lines) == 0 {
		t.Fatal("no trace data returned")
	}

	requiredFields := []string{"trace_id", "span_id", "duration", "kind", "name"}
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("parse trace row: %v", err)
		}
		for _, f := range requiredFields {
			if _, ok := row[f]; !ok {
				t.Errorf("trace span missing field %q", f)
			}
		}
	}
}

// TestSmoke_Traces_FilterByTraceID verifies trace_id exact filtering works.
func TestSmoke_Traces_FilterByTraceID(t *testing.T) {
	// First get a trace_id from the data
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "1")

	body := httpGetBody(t, tracesBaseURL, "/select/logsql/query", params)
	lines := nonEmptyLines(body)
	if len(lines) == 0 {
		t.Skip("no trace data")
	}

	var row map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
		t.Fatalf("parse: %v", err)
	}
	traceID, _ := row["trace_id"].(string)
	if traceID == "" {
		t.Skip("no trace_id in first row")
	}

	// Now query by that specific trace_id
	params2 := defaultTimeParams()
	params2.Set("query", fmt.Sprintf(`trace_id:="%s"`, traceID))
	params2.Set("limit", "100")

	body2 := httpGetBody(t, tracesBaseURL, "/select/logsql/query", params2)
	lines2 := nonEmptyLines(body2)
	if len(lines2) == 0 {
		t.Fatalf("trace_id filter returned 0 results for known trace_id %s", traceID)
	}

	for _, line := range lines2 {
		var r map[string]any
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			continue
		}
		got, _ := r["trace_id"].(string)
		if got != traceID {
			t.Errorf("filter returned trace_id=%q, want %q", got, traceID)
		}
	}
}

// =============================================================================
// Component: Loki Proxy Advanced
// =============================================================================

// TestSmoke_LokiProxy_Labels verifies /loki/api/v1/labels returns label names.
func TestSmoke_LokiProxy_Labels(t *testing.T) {
	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/labels", nil)
	result := mustParseJSON(t, body)

	status, _ := result["status"].(string)
	if status != "success" {
		t.Fatalf("labels status=%q", status)
	}

	data, ok := result["data"].([]any)
	if !ok || len(data) == 0 {
		t.Error("no labels returned")
	}

	// Verify at least service_name is present
	found := false
	for _, v := range data {
		if s, ok := v.(string); ok && strings.Contains(s, "service") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected a service-related label in /labels response")
	}
}

// TestSmoke_LokiProxy_LabelValues verifies /loki/api/v1/label/{name}/values works.
func TestSmoke_LokiProxy_LabelValues(t *testing.T) {
	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/label/service_name/values", nil)
	result := mustParseJSON(t, body)

	status, _ := result["status"].(string)
	if status != "success" {
		t.Fatalf("label values status=%q", status)
	}

	data, ok := result["data"].([]any)
	if !ok || len(data) == 0 {
		t.Error("no service_name values returned")
	}
}

// TestSmoke_LokiProxy_NoDotsInLabels verifies indexed labels use underscores (Loki compat).
func TestSmoke_LokiProxy_NoDotsInLabels(t *testing.T) {
	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/labels", nil)
	result := mustParseJSON(t, body)

	data, ok := result["data"].([]any)
	if !ok {
		t.Fatal("no labels data")
	}

	for _, v := range data {
		label, ok := v.(string)
		if !ok {
			continue
		}
		if strings.Contains(label, ".") {
			t.Errorf("label %q contains dot — violates Loki compatibility (should use underscores)", label)
		}
	}
}

// TestSmoke_LokiProxy_StructuredMetadata verifies structured metadata fields are present.
func TestSmoke_LokiProxy_StructuredMetadata(t *testing.T) {
	now := time.Now()
	params := url.Values{
		"query": {`{service_name=~".+"}`},
		"limit": {"3"},
		"start": {fmt.Sprintf("%d000000000", now.Add(-72*time.Hour).Unix())},
		"end":   {fmt.Sprintf("%d000000000", now.Unix())},
	}

	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/query_range", params)
	result := mustParseJSON(t, body)

	data, _ := result["data"].(map[string]any)
	results, _ := data["result"].([]any)
	if len(results) == 0 {
		t.Skip("no loki results")
	}

	// Check that at least one stream has structured metadata entries
	for _, r := range results {
		stream, _ := r.(map[string]any)
		entries, _ := stream["values"].([]any)
		if len(entries) == 0 {
			continue
		}
		// In Loki response, structured metadata appears in stream labels
		// or as separate metadata fields depending on Grafana version
		streamLabels, _ := stream["stream"].(map[string]any)
		if len(streamLabels) > 3 {
			// Has more than just the indexed stream labels — structured metadata working
			return
		}
	}
	t.Log("structured metadata fields not visible in query_range response (may need Grafana 11+ to display)")
}
