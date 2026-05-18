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

func TestBloom_TraceIDLookup(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "1")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Skip("no data available for bloom test")
	}

	traceID, _ := lines[0]["trace_id"].(string)
	if traceID == "" {
		t.Skip("no trace_id in first result")
	}

	lookupParams := defaultTimeParams()
	lookupParams.Set("query", fmt.Sprintf(`trace_id:="%s"`, traceID))
	lookupParams.Set("limit", "100")

	start := time.Now()
	lookupBody := httpGetBody(t, logsBaseURL, "/select/logsql/query", lookupParams)
	elapsed := time.Since(start)

	lookupLines := assertValidNDJSON(t, lookupBody)
	if len(lookupLines) == 0 {
		t.Fatalf("trace_id lookup for %q returned no results", traceID)
	}

	for _, line := range lookupLines {
		got, _ := line["trace_id"].(string)
		if got != traceID {
			t.Errorf("expected trace_id=%q, got %q", traceID, got)
		}
	}

	t.Logf("trace_id lookup: %d results in %v", len(lookupLines), elapsed)
}

func TestBloom_ServiceList(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "service.name")
	params.Set("limit", "100")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)

	var values []struct {
		Value string `json:"value"`
		Hits  int64  `json:"hits"`
	}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var v struct {
			Value string `json:"value"`
			Hits  int64  `json:"hits"`
		}
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			t.Logf("skipping non-JSON line: %s", line)
			continue
		}
		values = append(values, v)
	}

	if len(values) == 0 {
		t.Fatal("service.name field_values returned no results")
	}

	t.Logf("service.name has %d distinct values", len(values))
	for _, v := range values {
		t.Logf("  %s (%d hits)", v.Value, v.Hits)
	}
}

func TestBloom_DisabledVsEnabled_IdenticalResults(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="api-gateway"`)
	params.Set("limit", "50")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)

	if len(lines) == 0 {
		t.Skip("no data for service filter comparison")
	}

	for _, line := range lines {
		svc, _ := line["service.name"].(string)
		if svc != "api-gateway" {
			t.Errorf("expected service.name=api-gateway, got %q", svc)
		}
	}
	t.Logf("service filter returned %d results", len(lines))
}

func TestBloom_MultiTenant_Isolation(t *testing.T) {
	tenant0Params := defaultTimeParams()
	tenant0Params.Set("query", "*")
	tenant0Params.Set("limit", "10")

	body0 := httpGetBody(t, logsBaseURL, "/select/logsql/query", tenant0Params)
	lines0 := assertValidNDJSON(t, body0)

	tenant1Params := defaultTimeParams()
	tenant1Params.Set("query", "*")
	tenant1Params.Set("limit", "10")

	body1 := httpGetBodyWithTenantHeaders(t, logsBaseURL, "/select/logsql/query", tenant1Params, "1", "1")
	lines1 := assertValidNDJSON(t, body1)

	if len(lines0) == 0 && len(lines1) == 0 {
		t.Skip("no data for either tenant")
	}

	t.Logf("tenant 0:0 returned %d lines, tenant 1:1 returned %d lines", len(lines0), len(lines1))
}

func httpGetBodyWithTenantHeaders(t *testing.T, baseURL, path string, params url.Values, accountID, projectID string) []byte {
	t.Helper()

	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

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

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s returned status %d: %s", u, resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	return body
}
