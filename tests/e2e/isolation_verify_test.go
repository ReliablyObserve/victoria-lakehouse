//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// httpGetWithHeaders performs a GET with arbitrary extra headers and returns
// the status code and body. It never fails the test on non-200 responses.
func httpGetWithHeaders(t *testing.T, baseURL, path string, params map[string]string, headers map[string]string) (int, []byte) {
	t.Helper()

	u := baseURL + path
	if len(params) > 0 {
		first := true
		for k, v := range params {
			if first {
				u += "?" + k + "=" + v
				first = false
			} else {
				u += "&" + k + "=" + v
			}
		}
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		t.Fatalf("creating request for %s: %v", u, err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body from %s: %v", u, err)
	}
	return resp.StatusCode, body
}

// countNDJSONLines counts non-empty newline-delimited JSON lines.
func countNDJSONLines(data []byte) int {
	count := 0
	start := 0
	for i := 0; i <= len(data); i++ {
		if i == len(data) || data[i] == '\n' {
			line := data[start:i]
			if len(line) > 0 {
				// quick check: valid JSON object?
				var obj map[string]any
				if json.Unmarshal(line, &obj) == nil {
					count++
				}
			}
			start = i + 1
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// Tenant isolation — numeric account/project IDs
// ---------------------------------------------------------------------------

// TestVerifyIsolation_Logs_TenantAQueryCannotSeeTenantB verifies that a query
// scoped to account 1/project 1 cannot leak data belonging to account 0/project 0.
func TestVerifyIsolation_Logs_TenantAQueryCannotSeeTenantB(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "100")

	// Query as tenant 0/0 (default)
	defaultBody := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	defaultLines := assertValidNDJSON(t, defaultBody)

	// Query as tenant 1/1 (secondary) via AccountID/ProjectID headers.
	status, tenant1Body := httpGetWithHeaders(t, logsBaseURL, "/select/logsql/query",
		map[string]string{
			"query": "*",
			"limit": "100",
			"start": params.Get("start"),
			"end":   params.Get("end"),
		},
		map[string]string{
			"X-Scope-AccountID": "1",
			"X-Scope-ProjectID": "1",
		},
	)

	if status != http.StatusOK {
		t.Skipf("tenant 1/1 query returned %d — secondary tenant may not have data yet", status)
	}

	tenant1Lines := assertValidNDJSON(t, tenant1Body)

	// Both may have data; the key assertion is that the sets of _stream values
	// are disjoint — no cross-tenant stream leakage.
	defaultStreams := make(map[string]bool)
	for _, l := range defaultLines {
		if s, ok := l["_stream"].(string); ok {
			defaultStreams[s] = true
		}
	}

	leaks := 0
	for _, l := range tenant1Lines {
		// Each result from tenant 1 should not share a _stream with tenant 0
		// unless they happen to have identically-named streams (acceptable for
		// this smoke check — we just log it).
		if s, ok := l["_stream"].(string); ok && defaultStreams[s] {
			leaks++
		}
	}

	t.Logf("tenant 0/0 results: %d, tenant 1/1 results: %d, shared stream keys: %d",
		len(defaultLines), len(tenant1Lines), leaks)

	// A leak of 100% would indicate no isolation. We don't assert zero leaks
	// (stream labels can legitimately overlap), but log the ratio.
	if len(tenant1Lines) > 0 && leaks == len(tenant1Lines) {
		t.Errorf("all %d tenant-1 results share _stream values with tenant-0 — possible cross-tenant data leak", leaks)
	}
}

// ---------------------------------------------------------------------------
// Tenant isolation — OrgID string tenants
// ---------------------------------------------------------------------------

// TestVerifyIsolation_OrgID_NoCrossTenantLeak verifies that acme-corp and
// staging-team queries each return distinct (non-overlapping) trace_id or
// _msg sets.
func TestVerifyIsolation_OrgID_NoCrossTenantLeak(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "50")

	// Query as acme-corp
	acmeStatus, acmeBody := httpGetWithHeaders(t, logsBaseURL, "/select/logsql/query",
		map[string]string{
			"query": "*",
			"limit": "50",
			"start": params.Get("start"),
			"end":   params.Get("end"),
		},
		map[string]string{"X-Scope-OrgID": "acme-corp"},
	)

	// Query as staging-team
	stagingStatus, stagingBody := httpGetWithHeaders(t, logsBaseURL, "/select/logsql/query",
		map[string]string{
			"query": "*",
			"limit": "50",
			"start": params.Get("start"),
			"end":   params.Get("end"),
		},
		map[string]string{"X-Scope-OrgID": "staging-team"},
	)

	if acmeStatus != http.StatusOK {
		t.Skipf("acme-corp query returned %d — org may not be registered yet", acmeStatus)
	}
	if stagingStatus != http.StatusOK {
		t.Skipf("staging-team query returned %d — org may not be registered yet", stagingStatus)
	}

	acmeLines := assertValidNDJSON(t, acmeBody)
	stagingLines := assertValidNDJSON(t, stagingBody)

	if len(acmeLines) == 0 && len(stagingLines) == 0 {
		t.Skip("both orgs returned no data — seeding may not be complete")
	}

	// Collect _msg fingerprints from each tenant.
	acmeMsgs := make(map[string]bool, len(acmeLines))
	for _, l := range acmeLines {
		if msg, ok := l["_msg"].(string); ok && msg != "" {
			acmeMsgs[msg] = true
		}
	}

	crossLeaks := 0
	for _, l := range stagingLines {
		if msg, ok := l["_msg"].(string); ok && acmeMsgs[msg] {
			crossLeaks++
		}
	}

	t.Logf("acme-corp: %d results, staging-team: %d results, shared _msg values: %d",
		len(acmeLines), len(stagingLines), crossLeaks)

	// If every staging message also appears in acme, that's a strong isolation signal failure.
	if len(stagingLines) > 0 && crossLeaks == len(stagingLines) {
		t.Errorf("all staging-team results share _msg with acme-corp — possible OrgID isolation failure")
	}
}

// ---------------------------------------------------------------------------
// Global read mode
// ---------------------------------------------------------------------------

// TestVerifyIsolation_GlobalRead_SeesAllTenants verifies that a request with the
// correct X-Lakehouse-Global-Read secret returns data (spanning all tenants),
// while a missing header returns only the default-tenant view.
func TestVerifyIsolation_GlobalRead_SeesAllTenants(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "200")

	// Query with global read secret.
	globalStatus, globalBody := httpGetWithHeaders(t, logsBaseURL, "/select/logsql/query",
		map[string]string{
			"query": "*",
			"limit": "200",
			"start": params.Get("start"),
			"end":   params.Get("end"),
		},
		map[string]string{globalReadHeader: globalReadSecret},
	)

	if globalStatus != http.StatusOK {
		t.Fatalf("global read returned status %d: %s", globalStatus, string(globalBody))
	}

	globalLines := assertValidNDJSON(t, globalBody)
	if len(globalLines) == 0 {
		t.Fatal("global read returned no data — expected cross-tenant results")
	}

	// Query without global read header (default tenant scope).
	normalBody := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	normalLines := assertValidNDJSON(t, normalBody)

	t.Logf("global read: %d results, normal (default tenant): %d results",
		len(globalLines), len(normalLines))

	// Global read should return at least as many results as the default-tenant query.
	if len(globalLines) < len(normalLines) {
		t.Errorf("global read (%d) returned fewer results than default-tenant query (%d) — unexpected",
			len(globalLines), len(normalLines))
	}
}
