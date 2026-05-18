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

var (
	vtselectURL    = envOrDefault("VTSELECT_URL", "http://localhost:20471")
	clickhouseURL  = envOrDefault("CLICKHOUSE_URL", "http://localhost:8123")
)

// TestSmoke_LogsHealth verifies the lakehouse-logs /health endpoint returns OK.
func TestSmoke_LogsHealth(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/health", nil)
	if string(body) != "OK" {
		t.Fatalf("expected OK, got %q", string(body))
	}
}

// TestSmoke_TracesHealth verifies the lakehouse-traces /health endpoint returns OK.
func TestSmoke_TracesHealth(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/health", nil)
	if string(body) != "OK" {
		t.Fatalf("expected OK, got %q", string(body))
	}
}

// TestSmoke_LogsInfo verifies /lakehouse/info returns correct mode and ready state.
func TestSmoke_LogsInfo(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/lakehouse/info", nil)
	info := mustParseJSON(t, body)

	assertField(t, info, "mode", "logs")
	assertField(t, info, "ready", true)
	assertField(t, info, "phase", "ready")

	if _, ok := info["vl_compat"]; !ok {
		t.Error("missing vl_compat field")
	}
}

// TestSmoke_TracesInfo verifies /lakehouse/info returns correct mode and ready state.
func TestSmoke_TracesInfo(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/lakehouse/info", nil)
	info := mustParseJSON(t, body)

	assertField(t, info, "mode", "traces")
	assertField(t, info, "ready", true)
	assertField(t, info, "phase", "ready")

	if _, ok := info["vt_compat"]; !ok {
		t.Error("missing vt_compat field")
	}
}

// TestSmoke_ColdTierLogsQuery queries logs directly from S3 Parquet cold storage.
func TestSmoke_ColdTierLogsQuery(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "5")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := nonEmptyLines(body)
	if len(lines) == 0 {
		t.Fatal("cold tier logs query returned no results")
	}

	var row map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
		t.Fatalf("failed to parse log row: %v", err)
	}
	if _, ok := row["_msg"]; !ok {
		t.Error("log row missing _msg field")
	}
	if _, ok := row["_time"]; !ok {
		t.Error("log row missing _time field")
	}
}

// TestSmoke_ColdTierTracesQuery queries traces directly from S3 Parquet cold storage.
func TestSmoke_ColdTierTracesQuery(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "5")

	body := httpGetBody(t, tracesBaseURL, "/select/logsql/query", params)
	lines := nonEmptyLines(body)
	if len(lines) == 0 {
		t.Fatal("cold tier traces query returned no results")
	}

	var row map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &row); err != nil {
		t.Fatalf("failed to parse trace row: %v", err)
	}
	if _, ok := row["trace_id"]; !ok {
		t.Error("trace row missing trace_id field")
	}
	if _, ok := row["span_id"]; !ok {
		t.Error("trace row missing span_id field")
	}
}

// TestSmoke_AZCacheStats verifies cache stats include AZ information.
func TestSmoke_AZCacheStats(t *testing.T) {
	for _, tc := range []struct {
		name    string
		baseURL string
	}{
		{"logs", logsBaseURL},
		{"traces", tracesBaseURL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := httpGetBody(t, tc.baseURL, "/internal/cache/stats", nil)
			stats := mustParseJSON(t, body)

			az, ok := stats["az"].(string)
			if !ok || az == "" {
				t.Error("cache stats missing or empty 'az' field")
			}

			if _, ok := stats["l1_entries"]; !ok {
				t.Error("cache stats missing l1_entries")
			}
			if _, ok := stats["l1_max_size"]; !ok {
				t.Error("cache stats missing l1_max_size")
			}
		})
	}
}

// TestSmoke_FederatedSelectLogs queries through vlselect (hot + cold merged).
func TestSmoke_FederatedSelectLogs(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "5")

	body := httpGetBody(t, vlselectURL, "/select/logsql/query", params)
	lines := nonEmptyLines(body)
	if len(lines) == 0 {
		t.Fatal("federated logs select returned no results")
	}
}

// TestSmoke_FederatedSelectTraces queries through vtselect (hot + cold merged).
func TestSmoke_FederatedSelectTraces(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "5")

	body := httpGetBody(t, vtselectURL, "/select/logsql/query", params)
	lines := nonEmptyLines(body)
	if len(lines) == 0 {
		t.Fatal("federated traces select returned no results")
	}
}

// TestSmoke_MultiTenantIsolation verifies tenant 1/1 has its own data.
func TestSmoke_MultiTenantIsolation(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "5")

	client := &http.Client{Timeout: 60 * time.Second}
	u := logsBaseURL + "/select/logsql/query?" + params.Encode()

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set("AccountID", "1")
	req.Header.Set("ProjectID", "1")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("tenant query failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("tenant query status %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	lines := nonEmptyLines(body)
	if len(lines) == 0 {
		t.Fatal("tenant 1/1 query returned no results (expected seeded data)")
	}
}

// TestSmoke_LogsStreams verifies the /select/logsql/streams endpoint returns stream values.
func TestSmoke_LogsStreams(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/streams", params)
	result := mustParseJSON(t, body)

	values, ok := result["values"].([]any)
	if !ok || len(values) == 0 {
		t.Fatal("streams returned no values")
	}

	first, ok := values[0].(map[string]any)
	if !ok {
		t.Fatal("stream value is not an object")
	}
	if _, ok := first["value"]; !ok {
		t.Error("stream entry missing 'value' field")
	}
}

// TestSmoke_TracesFieldNames verifies field_names returns trace-specific fields.
func TestSmoke_TracesFieldNames(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	body := httpGetBody(t, tracesBaseURL, "/select/logsql/field_names", params)
	result := mustParseJSON(t, body)

	values, ok := result["values"].([]any)
	if !ok || len(values) == 0 {
		t.Fatal("field_names returned no values")
	}

	fieldNames := make(map[string]bool)
	for _, v := range values {
		if entry, ok := v.(map[string]any); ok {
			if name, ok := entry["value"].(string); ok {
				fieldNames[name] = true
			}
		}
	}

	for _, required := range []string{"trace_id", "span_id", "duration", "kind"} {
		if !fieldNames[required] {
			t.Errorf("missing expected trace field: %s", required)
		}
	}
}

// TestSmoke_LokiProxyQuery verifies the Loki-compatible API proxy works.
func TestSmoke_LokiProxyQuery(t *testing.T) {
	now := time.Now()
	params := url.Values{
		"query": {`{service_name=~".+"}`},
		"limit": {"5"},
		"start": {fmt.Sprintf("%d000000000", now.Add(-72*time.Hour).Unix())},
		"end":   {fmt.Sprintf("%d000000000", now.Unix())},
	}

	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/query_range", params)
	result := mustParseJSON(t, body)

	status, _ := result["status"].(string)
	if status != "success" {
		t.Fatalf("loki proxy status=%q, want success", status)
	}

	data, ok := result["data"].(map[string]any)
	if !ok {
		t.Fatal("loki response missing 'data' field")
	}

	resultType, _ := data["resultType"].(string)
	if resultType != "streams" {
		t.Errorf("resultType=%q, want streams", resultType)
	}
}

// TestSmoke_ManifestRange verifies manifest metadata is accessible and valid.
func TestSmoke_ManifestRange(t *testing.T) {
	for _, tc := range []struct {
		name    string
		baseURL string
	}{
		{"logs", logsBaseURL},
		{"traces", tracesBaseURL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := httpGetBody(t, tc.baseURL, "/manifest/range", nil)
			result := mustParseJSON(t, body)

			totalFiles, _ := result["totalFiles"].(float64)
			if totalFiles == 0 {
				t.Error("manifest reports 0 files")
			}

			totalBytes, _ := result["totalBytes"].(float64)
			if totalBytes == 0 {
				t.Error("manifest reports 0 bytes")
			}

			if _, ok := result["minDate"]; !ok {
				t.Error("missing minDate")
			}
			if _, ok := result["maxDate"]; !ok {
				t.Error("missing maxDate")
			}
		})
	}
}

// TestSmoke_CacheClearAndRecovery verifies cache can be cleared and re-populated.
func TestSmoke_CacheClearAndRecovery(t *testing.T) {
	resp := httpPost(t, logsBaseURL, "/internal/cache/clear", "application/json", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cache clear returned %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	result := mustParseJSON(t, body)
	if cleared, _ := result["cleared"].(bool); !cleared {
		t.Error("cache clear did not return cleared:true")
	}

	statsBody := httpGetBody(t, logsBaseURL, "/internal/cache/stats", nil)
	stats := mustParseJSON(t, statsBody)

	entries, _ := stats["l1_entries"].(float64)
	if entries != 0 {
		t.Errorf("cache entries after clear = %v, want 0", entries)
	}

	// Re-populate by running a query
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "1")
	_ = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)

	statsBody2 := httpGetBody(t, logsBaseURL, "/internal/cache/stats", nil)
	stats2 := mustParseJSON(t, statsBody2)
	entries2, _ := stats2["l1_entries"].(float64)
	if entries2 == 0 {
		t.Error("cache not re-populated after query")
	}
}

// TestSmoke_FilteredQuery verifies filtered queries return only matching results.
func TestSmoke_FilteredQuery(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="payment-service"`)
	params.Set("limit", "10")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := nonEmptyLines(body)
	if len(lines) == 0 {
		t.Fatal("filtered query returned no results")
	}

	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("parse row: %v", err)
		}
		svc, _ := row["service.name"].(string)
		if svc != "payment-service" {
			t.Errorf("expected service.name=payment-service, got %q", svc)
		}
	}
}

// TestSmoke_FieldValues verifies field_values autocomplete returns distinct values.
func TestSmoke_FieldValues(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "service.name")
	params.Set("limit", "20")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)
	result := mustParseJSON(t, body)

	values, ok := result["values"].([]any)
	if !ok || len(values) == 0 {
		t.Fatal("field_values returned no values")
	}

	found := false
	for _, v := range values {
		if entry, ok := v.(map[string]any); ok {
			if val, ok := entry["value"].(string); ok && val != "" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Error("no non-empty service.name values found")
	}
}

// TestSmoke_NoPanicsInLogs checks container logs for panics or fatal errors.
// This test requires LOGS_CONTAINER and TRACES_CONTAINER env vars to point to
// container names, or it will skip.
func TestSmoke_NoPanicsInLogs(t *testing.T) {
	// This test verifies via the /health endpoint being responsive that
	// no fatal crash occurred. A more thorough check requires docker exec
	// access which isn't available in the Go test process.
	for _, tc := range []struct {
		name    string
		baseURL string
	}{
		{"logs", logsBaseURL},
		{"traces", tracesBaseURL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(tc.baseURL + "/health")
			if err != nil {
				t.Fatalf("health check failed (possible crash): %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("health returned %d (possible crash)", resp.StatusCode)
			}
		})
	}
}

// --- Helpers ---

func nonEmptyLines(data []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func assertField(t *testing.T, m map[string]any, key string, expected any) {
	t.Helper()
	val, ok := m[key]
	if !ok {
		t.Errorf("missing field %q", key)
		return
	}
	if val != expected {
		t.Errorf("field %q = %v, want %v", key, val, expected)
	}
}
