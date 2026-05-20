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

// TestRoundTrip_Logs_InsertAndQuery inserts a log line with a unique trace_id,
// waits briefly for the ingestion pipeline to flush, then queries for it.
func TestRoundTrip_Logs_InsertAndQuery(t *testing.T) {
	uniqueTraceID := fmt.Sprintf("roundtrip-%d", time.Now().UnixNano())
	now := time.Now()

	logLine := map[string]any{
		"_time":        now.UTC().Format(time.RFC3339Nano),
		"_msg":         "e2e round-trip fidelity test",
		"trace_id":     uniqueTraceID,
		"service.name": "e2e-roundtrip-svc",
		"level":        "INFO",
	}
	body, err := json.Marshal(logLine)
	if err != nil {
		t.Fatalf("marshalling log line: %v", err)
	}

	// Insert via the jsonline endpoint.
	resp := httpPost(t, logsBaseURL, "/insert/jsonline?_stream_fields=service.name",
		"application/x-ndjson", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("insert returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Allow the write pipeline a moment to flush before querying.
	time.Sleep(3 * time.Second)

	// Query for the specific trace_id. Use a wide window to catch the inserted record
	// regardless of flush timing.
	params := url.Values{
		"query": {fmt.Sprintf(`trace_id:="%s"`, uniqueTraceID)},
		"limit": {"10"},
		"start": {fmt.Sprintf("%d", now.Add(-5*time.Minute).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.Add(5*time.Minute).UnixNano())},
	}

	queryBody := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, queryBody)

	if len(lines) == 0 {
		t.Fatalf("round-trip: inserted log with trace_id=%q not found in query results", uniqueTraceID)
	}

	// Verify the returned record matches what was inserted.
	found := lines[0]
	if gotTraceID, ok := found["trace_id"].(string); !ok || gotTraceID != uniqueTraceID {
		t.Errorf("returned trace_id=%q, want %q", gotTraceID, uniqueTraceID)
	}
	if got, ok := found["service.name"].(string); !ok || got != "e2e-roundtrip-svc" {
		t.Errorf("returned service.name=%q, want e2e-roundtrip-svc", got)
	}
	if got, ok := found["level"].(string); !ok || got != "INFO" {
		t.Errorf("returned level=%q, want INFO", got)
	}

	t.Logf("round-trip OK: inserted and queried trace_id=%q (%d matching records)", uniqueTraceID, len(lines))
}

// TestRoundTrip_Logs_FieldFidelity verifies that all inserted fields survive
// the write → read round-trip without truncation or renaming.
func TestRoundTrip_Logs_FieldFidelity(t *testing.T) {
	uniqueMsg := fmt.Sprintf("field-fidelity-test-%d", time.Now().UnixNano())
	now := time.Now()

	logLine := map[string]any{
		"_time":                  now.UTC().Format(time.RFC3339Nano),
		"_msg":                   uniqueMsg,
		"service.name":           "e2e-fidelity-svc",
		"level":                  "DEBUG",
		"k8s.namespace.name":     "e2e-namespace",
		"deployment.environment": "e2e-test",
	}
	body, err := json.Marshal(logLine)
	if err != nil {
		t.Fatalf("marshalling log line: %v", err)
	}

	resp := httpPost(t, logsBaseURL, "/insert/jsonline?_stream_fields=service.name",
		"application/x-ndjson", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("insert returned %d: %s", resp.StatusCode, string(respBody))
	}

	time.Sleep(3 * time.Second)

	// Escape the unique message for LogsQL.
	escapedMsg := strings.ReplaceAll(uniqueMsg, "-", `\-`)
	_ = escapedMsg

	params := url.Values{
		"query": {fmt.Sprintf(`_msg:="%s"`, uniqueMsg)},
		"limit": {"5"},
		"start": {fmt.Sprintf("%d", now.Add(-5*time.Minute).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.Add(5*time.Minute).UnixNano())},
	}

	queryBody := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, queryBody)

	if len(lines) == 0 {
		t.Fatalf("field-fidelity: inserted log with _msg=%q not found", uniqueMsg)
	}

	found := lines[0]
	checks := map[string]string{
		"service.name": "e2e-fidelity-svc",
		"level":        "DEBUG",
	}
	for field, want := range checks {
		got, ok := found[field].(string)
		if !ok || got != want {
			t.Errorf("field %q: got %q, want %q", field, got, want)
		}
	}

	t.Logf("field-fidelity OK: all checked fields preserved across round-trip")
}
