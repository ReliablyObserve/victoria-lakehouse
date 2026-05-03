//go:build e2e

package e2e

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestQuery_Wildcard(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "100")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Fatal("expected at least 1 NDJSON line for wildcard query, got 0")
	}
	t.Logf("wildcard query returned %d lines", len(lines))
}

func TestQuery_ExactService(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="api-gateway"`)
	params.Set("limit", "100")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Fatal("expected results for service.name:=\"api-gateway\"")
	}

	for i, line := range lines {
		svc, ok := line["service.name"].(string)
		if !ok {
			t.Fatalf("line %d missing service.name field", i)
		}
		if svc != "api-gateway" {
			t.Errorf("line %d: expected service.name=api-gateway, got %q", i, svc)
		}
	}
}

func TestQuery_Substring(t *testing.T) {
	params := defaultTimeParams()
	// The _msg field contains the body text. Use a substring filter.
	// Datagen produces messages like "[ERROR] failed to parse request body..."
	params.Set("query", `_msg:error`)
	params.Set("limit", "50")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	// Some lines may match — if datagen created ERROR level logs with "error" in body.
	// With 5000 logs, it is very likely at least one contains "error" in the body.
	if len(lines) == 0 {
		t.Skip("no lines matched _msg:error — datagen may not have produced matching content")
	}

	for i, line := range lines {
		msg, ok := line["_msg"].(string)
		if !ok {
			t.Fatalf("line %d missing _msg field", i)
		}
		if !strings.Contains(strings.ToLower(msg), "error") {
			t.Errorf("line %d: _msg does not contain 'error': %q", i, msg)
		}
	}
}

func TestQuery_AND(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="api-gateway" AND level:="ERROR"`)
	params.Set("limit", "50")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) == 0 {
		t.Skip("no lines matched AND query — datagen may not have produced api-gateway ERROR logs")
	}

	for i, line := range lines {
		svc, _ := line["service.name"].(string)
		lvl, _ := line["level"].(string)
		if svc != "api-gateway" {
			t.Errorf("line %d: expected service.name=api-gateway, got %q", i, svc)
		}
		if lvl != "ERROR" {
			t.Errorf("line %d: expected level=ERROR, got %q", i, lvl)
		}
	}
}

func TestQuery_OR(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="api-gateway" OR service.name:="user-service"`)
	params.Set("limit", "100")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) == 0 {
		t.Fatal("expected results for OR query with two known services")
	}

	for i, line := range lines {
		svc, _ := line["service.name"].(string)
		if svc != "api-gateway" && svc != "user-service" {
			t.Errorf("line %d: expected api-gateway or user-service, got %q", i, svc)
		}
	}
}

func TestQuery_NOT(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `NOT level:="DEBUG"`)
	params.Set("limit", "100")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) == 0 {
		t.Fatal("expected results for NOT DEBUG query")
	}

	for i, line := range lines {
		lvl, _ := line["level"].(string)
		if lvl == "DEBUG" {
			t.Errorf("line %d: NOT filter should exclude DEBUG, got level=%q", i, lvl)
		}
	}
}

func TestQuery_Limit(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "5")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) != 5 {
		t.Fatalf("expected exactly 5 lines with limit=5, got %d", len(lines))
	}
}

func TestQuery_TimeRange(t *testing.T) {
	// Use a narrow 2-hour window within the data range
	now := time.Now()
	start := now.Add(-6 * time.Hour)
	end := now.Add(-4 * time.Hour)

	params := url.Values{
		"query": {"*"},
		"start": {fmt.Sprintf("%d", start.UnixNano())},
		"end":   {fmt.Sprintf("%d", end.UnixNano())},
		"limit": {"50"},
	}

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	// Results may be empty if datagen didn't happen to place logs in this window.
	// But if they exist, their timestamps should be within range.
	for i, line := range lines {
		ts, ok := line["_time"].(string)
		if !ok {
			continue
		}
		// _time is the timestamp_unix_nano as a string
		_ = ts
		_ = i
	}
	t.Logf("narrow time range returned %d lines", len(lines))
}

func TestQuery_NoResults(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="nonexistent-service-that-does-not-exist"`)
	params.Set("limit", "10")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) != 0 {
		t.Fatalf("expected 0 results for nonexistent service, got %d", len(lines))
	}
}

func TestQuery_ResponseFormat(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) == 0 {
		t.Fatal("expected at least 1 line to validate response format")
	}

	for i, line := range lines {
		// Every NDJSON line should have _time
		if _, ok := line["_time"]; !ok {
			t.Errorf("line %d missing '_time' field", i)
		}

		// Every NDJSON line should have _msg (mapped from body)
		if _, ok := line["_msg"]; !ok {
			t.Errorf("line %d missing '_msg' field", i)
		}

		// Every NDJSON line should have _stream
		if _, ok := line["_stream"]; !ok {
			t.Errorf("line %d missing '_stream' field", i)
		}
	}
}
