//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func queryLogs(t *testing.T, query string, limit int) []map[string]any {
	t.Helper()
	params := defaultTimeParams()
	params.Set("query", query)
	params.Set("limit", strconv.Itoa(limit))
	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	return assertValidNDJSON(t, body)
}

func queryTraces(t *testing.T, query string, limit int) []map[string]any {
	t.Helper()
	params := defaultTimeParams()
	params.Set("query", query)
	params.Set("limit", strconv.Itoa(limit))
	body := httpGetBody(t, tracesBaseURL, "/select/logsql/query", params)
	return assertValidNDJSON(t, body)
}

func queryWithTenant(t *testing.T, baseURL, query string, limit int, accountID, projectID string) []map[string]any {
	t.Helper()
	params := defaultTimeParams()
	params.Set("query", query)
	params.Set("limit", strconv.Itoa(limit))

	u := baseURL + "/select/logsql/query?" + params.Encode()
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set("AccountID", accountID)
	req.Header.Set("ProjectID", projectID)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d: %s", u, resp.StatusCode, string(body))
	}
	return assertValidNDJSON(t, body)
}

func getManifestRange(t *testing.T, baseURL string) map[string]any {
	t.Helper()
	body := httpGetBody(t, baseURL, "/manifest/range", nil)
	return mustParseJSON(t, body)
}

func getBloomStatus(t *testing.T, baseURL string) map[string]any {
	t.Helper()
	body := httpGetBody(t, baseURL, "/api/v1/bloom/status", nil)
	return mustParseJSON(t, body)
}

// tryGetBloomStatus returns the bloom status or nil if the endpoint returns 404.
func tryGetBloomStatus(t *testing.T, baseURL string) map[string]any {
	t.Helper()
	resp := httpGetAllowStatus(t, baseURL, "/api/v1/bloom/status", nil, 200, 404)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == 404 {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading bloom status response: %v", err)
	}
	return mustParseJSON(t, body)
}

func getLakehouseInfo(t *testing.T, baseURL string) map[string]any {
	t.Helper()
	body := httpGetBody(t, baseURL, "/lakehouse/info", nil)
	return mustParseJSON(t, body)
}

func getCacheStats(t *testing.T, baseURL string) map[string]any {
	t.Helper()
	body := httpGetBody(t, baseURL, "/internal/cache/stats", nil)
	return mustParseJSON(t, body)
}

func getFieldValues(t *testing.T, baseURL, field string) []map[string]any {
	t.Helper()
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", field)
	params.Set("limit", "1000")

	body := httpGetBody(t, baseURL, "/select/logsql/field_values", params)

	// Response is a JSON object with a "values" array: {"values":[{"value":"val","hits":N},...]}
	var wrapper struct {
		Values []map[string]any `json:"values"`
	}
	if err := json.Unmarshal(body, &wrapper); err == nil && wrapper.Values != nil {
		return wrapper.Values
	}

	// Fallback: try NDJSON parsing for backwards compatibility
	var results []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue
		}
		results = append(results, m)
	}
	return results
}

func getPrometheusMetric(t *testing.T, baseURL, metricName string) float64 {
	t.Helper()
	u := baseURL + "/metrics"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, metricName) {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := strconv.ParseFloat(parts[len(parts)-1], 64)
				if err == nil {
					return val
				}
			}
		}
	}
	return -1
}

func getPrometheusMetricWithLabel(t *testing.T, baseURL, metricName, labelKey, labelValue string) float64 {
	t.Helper()
	u := baseURL + "/metrics"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	search := fmt.Sprintf(`%s{%s="%s"`, metricName, labelKey, labelValue)
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Contains(line, search) {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := strconv.ParseFloat(parts[len(parts)-1], 64)
				if err == nil {
					return val
				}
			}
		}
	}
	return -1
}

func timeQuery(t *testing.T, baseURL, query string) time.Duration {
	t.Helper()
	params := defaultTimeParams()
	params.Set("query", query)
	params.Set("limit", "10")

	start := time.Now()
	body := httpGetBody(t, baseURL, "/select/logsql/query", params)
	elapsed := time.Since(start)
	_ = body
	return elapsed
}

func assertFieldPresent(t *testing.T, m map[string]any, key string) {
	t.Helper()
	if _, ok := m[key]; !ok {
		t.Errorf("expected field %q in response, not found. keys: %v", key, mapKeys(m))
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func mustGetFloat(t *testing.T, m map[string]any, key string) float64 {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("key %q not found", key)
	}
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		t.Fatalf("key %q: expected number, got %T", key, v)
		return 0
	}
}

func mustGetString(t *testing.T, m map[string]any, key string) string {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("key %q not found", key)
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("key %q: expected string, got %T", key, v)
	}
	return s
}

// insertTestLogs sends test logs via jsonline endpoint and returns the trace IDs used.
func insertTestLogs(t *testing.T, baseURL string, count int, service string) []string {
	t.Helper()
	traceIDs := make([]string, count)
	var lines []string
	now := time.Now()

	for i := 0; i < count; i++ {
		traceIDs[i] = fmt.Sprintf("bloom-test-%s-%d-%d", service, now.UnixNano(), i)
		line := fmt.Sprintf(`{"_msg":"test log %d","_time":"%s","service.name":"%s","trace_id":"%s","severity_text":"INFO"}`,
			i, now.Add(-time.Duration(i)*time.Second).Format(time.RFC3339Nano), service, traceIDs[i])
		lines = append(lines, line)
	}

	body := strings.Join(lines, "\n")
	params := url.Values{"_stream_fields": {"service.name"}}
	u := baseURL + "/insert/jsonline?" + params.Encode()

	resp, err := http.Post(u, "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("insert returned %d: %s", resp.StatusCode, string(respBody))
	}
	return traceIDs
}
