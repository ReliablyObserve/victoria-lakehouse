//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Alias API Tests
// ---------------------------------------------------------------------------

func TestStringTenant_AliasCreate(t *testing.T) {
	alias := map[string]any{
		"org_id":     "test-alias-create",
		"account_id": 42,
		"project_id": 0,
	}
	body, _ := json.Marshal(alias)

	req, err := http.NewRequest("POST", logsBaseURL+"/lakehouse/api/v1/tenants/aliases", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, string(respBody))
	}
	t.Log("alias 'test-alias-create' created successfully")
}

func TestStringTenant_AliasList(t *testing.T) {
	resp, err := http.Get(logsBaseURL + "/lakehouse/api/v1/tenants/aliases")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Aliases []struct {
			OrgID     string `json:"org_id"`
			AccountID uint32 `json:"account_id"`
			ProjectID uint32 `json:"project_id"`
		} `json:"aliases"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if len(result.Aliases) == 0 {
		t.Fatal("expected at least one alias (auto-registered tenants)")
	}

	t.Logf("found %d aliases:", len(result.Aliases))
	for _, a := range result.Aliases {
		t.Logf("  %s -> %d:%d", a.OrgID, a.AccountID, a.ProjectID)
	}
}

func TestStringTenant_AliasListTraces(t *testing.T) {
	resp, err := http.Get(tracesBaseURL + "/lakehouse/api/v1/tenants/aliases")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Aliases []struct {
			OrgID     string `json:"org_id"`
			AccountID uint32 `json:"account_id"`
			ProjectID uint32 `json:"project_id"`
		} `json:"aliases"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	t.Logf("traces aliases: %d entries", len(result.Aliases))
	for _, a := range result.Aliases {
		t.Logf("  %s -> %d:%d", a.OrgID, a.AccountID, a.ProjectID)
	}
}

// ---------------------------------------------------------------------------
// Auto-Register Verification
// ---------------------------------------------------------------------------

func TestStringTenant_AutoRegisterOnInsert(t *testing.T) {
	orgID := fmt.Sprintf("auto-test-%d", time.Now().UnixNano())

	logLine := map[string]any{
		"_time":        time.Now().Format(time.RFC3339Nano),
		"_msg":         "auto-register test log",
		"service.name": "auto-test-svc",
		"level":        "INFO",
	}
	body, _ := json.Marshal(logLine)

	req, err := http.NewRequest("POST",
		logsBaseURL+"/insert/jsonline?_stream_fields=service.name",
		bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("X-Scope-OrgID", orgID)

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("insert with OrgID failed: status %d: %s", resp.StatusCode, string(respBody))
	}

	// Verify the alias was auto-registered
	aliasResp, err := http.Get(logsBaseURL + "/lakehouse/api/v1/tenants/aliases")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aliasResp.Body.Close() }()

	var result struct {
		Aliases []struct {
			OrgID string `json:"org_id"`
		} `json:"aliases"`
	}
	aliasBody, _ := io.ReadAll(aliasResp.Body)
	if err := json.Unmarshal(aliasBody, &result); err != nil {
		t.Fatal(err)
	}

	found := false
	for _, a := range result.Aliases {
		if a.OrgID == orgID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("auto-registered org_id %q not found in alias list", orgID)
	}
	t.Logf("org_id %q auto-registered successfully", orgID)
}

// ---------------------------------------------------------------------------
// String Tenant Data Isolation
// ---------------------------------------------------------------------------

func TestStringTenant_AcmeCorpDataExists(t *testing.T) {
	// acme-corp is a configured alias (acme-corp:1001:0): it must exist, and
	// data written under it must be readable under it.
	a, ok := stAliasOf(stAliases(t, logsBaseURL), "acme-corp")
	if !ok || a.AccountID != 1001 || a.ProjectID != 0 {
		t.Fatalf("acme-corp alias = %+v (found=%v), want the configured 1001:0", a, ok)
	}
	marker := fmt.Sprintf("acme-exists-%d", time.Now().UnixNano())
	stInsertLog(t, "acme-corp", marker)
	stEventually(t, 90*time.Second, func() string {
		return stExpectSet("acme-corp", stQueryField(t, logsBaseURL, "acme-corp", fmt.Sprintf("%q", marker), "_msg"), marker)
	})
}
func TestStringTenant_StagingTeamDataExists(t *testing.T) {
	a, ok := stAliasOf(stAliases(t, logsBaseURL), "staging-team")
	if !ok || a.AccountID != 1002 || a.ProjectID != 0 {
		t.Fatalf("staging-team alias = %+v (found=%v), want the configured 1002:0", a, ok)
	}
	marker := fmt.Sprintf("staging-exists-%d", time.Now().UnixNano())
	stInsertLog(t, "staging-team", marker)
	stEventually(t, 90*time.Second, func() string {
		return stExpectSet("staging-team", stQueryField(t, logsBaseURL, "staging-team", fmt.Sprintf("%q", marker), "_msg"), marker)
	})
	// acme-corp must not see staging-team's line.
	if got := stQueryField(t, logsBaseURL, "acme-corp", fmt.Sprintf("%q", marker), "_msg"); len(got) != 0 {
		t.Fatalf("acme-corp sees staging-team's line: %v", got)
	}
}

// ---------------------------------------------------------------------------
// Query with OrgID Header
// ---------------------------------------------------------------------------

func TestStringTenant_QueryWithOrgIDHeader(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	status, body := httpGetWithOrgID(t, logsBaseURL, "/select/logsql/query", params, "acme-corp")
	if status != http.StatusOK {
		t.Fatalf("query with the configured acme-corp OrgID returned %d: %s", status, body)
	}
	lines := assertValidNDJSON(t, body)
	t.Logf("acme-corp scoped query returned %d results", len(lines))
}

// ---------------------------------------------------------------------------
// Alias Delete
// ---------------------------------------------------------------------------

func TestStringTenant_AliasDelete(t *testing.T) {
	// Create a temporary alias to delete
	alias := map[string]any{
		"org_id":     "delete-me-test",
		"account_id": 99,
		"project_id": 0,
	}
	body, _ := json.Marshal(alias)
	createReq, _ := http.NewRequest("POST", logsBaseURL+"/lakehouse/api/v1/tenants/aliases", bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := (&http.Client{Timeout: 10 * time.Second}).Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = createResp.Body.Close()

	// Delete it
	deleteReq, _ := http.NewRequest("DELETE", logsBaseURL+"/lakehouse/api/v1/tenants/aliases/delete-me-test", nil)
	deleteResp, err := (&http.Client{Timeout: 10 * time.Second}).Do(deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = deleteResp.Body.Close() }()

	if deleteResp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204 on alias delete, got %d", deleteResp.StatusCode)
	}

	// Verify it's gone
	listResp, _ := http.Get(logsBaseURL + "/lakehouse/api/v1/tenants/aliases")
	defer func() { _ = listResp.Body.Close() }()
	listBody, _ := io.ReadAll(listResp.Body)

	var result struct {
		Aliases []struct {
			OrgID string `json:"org_id"`
		} `json:"aliases"`
	}
	_ = json.Unmarshal(listBody, &result)

	for _, a := range result.Aliases {
		if a.OrgID == "delete-me-test" {
			t.Error("alias 'delete-me-test' still exists after deletion")
		}
	}
	t.Log("alias delete verified")
}

// ---------------------------------------------------------------------------
// Invalid OrgID Validation
// ---------------------------------------------------------------------------

func TestStringTenant_InvalidOrgIDRejected(t *testing.T) {
	invalidIDs := []string{
		"has spaces",
		"has/slash",
		"has|pipe",
		"",
	}

	for _, orgID := range invalidIDs {
		if orgID == "" {
			continue
		}
		logLine := map[string]any{
			"_time": time.Now().Format(time.RFC3339Nano),
			"_msg":  "should be rejected",
		}
		body, _ := json.Marshal(logLine)

		req, _ := http.NewRequest("POST",
			logsBaseURL+"/insert/jsonline?_stream_fields=service.name",
			bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("X-Scope-OrgID", orgID)

		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatalf("orgID %q: request failed: %v", orgID, err)
		}
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
			t.Errorf("orgID %q should have been rejected but got %d", orgID, resp.StatusCode)
		} else {
			t.Logf("orgID %q correctly rejected with status %d", orgID, resp.StatusCode)
		}
	}
}

// ---------------------------------------------------------------------------
// Tenant Summary
// ---------------------------------------------------------------------------

func TestStringTenant_Summary(t *testing.T) {
	resp, err := http.Get(logsBaseURL + "/lakehouse/api/v1/tenants/aliases")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		Aliases []struct {
			OrgID     string `json:"org_id"`
			AccountID uint32 `json:"account_id"`
			ProjectID uint32 `json:"project_id"`
		} `json:"aliases"`
	}
	body, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(body, &result)

	t.Log("=== String Tenant Summary (Logs) ===")
	for _, a := range result.Aliases {
		t.Logf("  %s -> account=%d project=%d", a.OrgID, a.AccountID, a.ProjectID)
	}

	// Check S3 for string-tenant data
	client := newS3Client(t)
	all := listS3Objects(t, client, "")
	tenantPrefixes := make(map[string]int)
	for _, k := range all {
		for _, a := range result.Aliases {
			prefix := fmt.Sprintf("%d/%d/", a.AccountID, a.ProjectID)
			if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
				tenantPrefixes[a.OrgID]++
			}
		}
	}
	for orgID, count := range tenantPrefixes {
		t.Logf("  %s: %d S3 objects", orgID, count)
	}
}

// ---------------------------------------------------------------------------
// Helper — defaultTimeParams is in helpers_test.go
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Traces — String Tenant Tests
// ---------------------------------------------------------------------------

func TestStringTenant_TracesAutoRegisterOnInsert(t *testing.T) {
	orgID := fmt.Sprintf("traces-auto-%d", time.Now().UnixNano())

	payload := map[string]any{
		"resourceSpans": []map[string]any{
			{
				"resource": map[string]any{
					"attributes": []map[string]any{
						{"key": "service.name", "value": map[string]any{"stringValue": "string-tenant-svc"}},
					},
				},
				"scopeSpans": []map[string]any{
					{
						"scope": map[string]any{"name": "test"},
						"spans": []map[string]any{
							{
								"traceId":           "abcdef1234567890abcdef1234567890",
								"spanId":            "1234567890abcdef",
								"name":              "string-tenant-test",
								"kind":              1,
								"startTimeUnixNano": fmt.Sprintf("%d", time.Now().Add(-1*time.Second).UnixNano()),
								"endTimeUnixNano":   fmt.Sprintf("%d", time.Now().UnixNano()),
								"attributes":        []map[string]any{},
							},
						},
					},
				},
			},
		},
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST",
		tracesBaseURL+"/insert/opentelemetry/v1/traces",
		bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", orgID)

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("traces insert with OrgID failed: status %d: %s", resp.StatusCode, string(respBody))
	}

	// Verify alias was auto-registered on traces endpoint
	aliasResp, err := http.Get(tracesBaseURL + "/lakehouse/api/v1/tenants/aliases")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = aliasResp.Body.Close() }()

	var result struct {
		Aliases []struct {
			OrgID string `json:"org_id"`
		} `json:"aliases"`
	}
	aliasBody, _ := io.ReadAll(aliasResp.Body)
	_ = json.Unmarshal(aliasBody, &result)

	found := false
	for _, a := range result.Aliases {
		if a.OrgID == orgID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("traces: auto-registered org_id %q not found in alias list", orgID)
	}
	t.Logf("traces: org_id %q auto-registered successfully", orgID)
}

func TestStringTenant_TracesQueryWithOrgID(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	status, body := httpGetWithOrgID(t, tracesBaseURL, "/select/logsql/query", params, "acme-corp")
	if status != http.StatusOK {
		t.Fatalf("traces query with the configured acme-corp OrgID returned %d: %s", status, body)
	}
	lines := assertValidNDJSON(t, body)
	t.Logf("traces acme-corp scoped query returned %d results", len(lines))
}
func TestStringTenant_TracesAcmeCorpDataExists(t *testing.T) {
	a, ok := stAliasOf(stAliases(t, tracesBaseURL), "acme-corp")
	if !ok || a.AccountID != 1001 || a.ProjectID != 0 {
		t.Fatalf("traces acme-corp alias = %+v (found=%v), want the configured 1001:0", a, ok)
	}
	traceID := stRandHex(t, 16)
	stInsertSpan(t, "acme-corp", traceID, "st-acme-exists")
	stEventually(t, 90*time.Second, func() string {
		if got := stQueryField(t, tracesBaseURL, "acme-corp", fmt.Sprintf("trace_id:%q", traceID), "trace_id"); len(got) == 0 {
			return "acme-corp does not see its own trace " + traceID + " yet"
		}
		return ""
	})
	if got := stQueryField(t, tracesBaseURL, "staging-team", fmt.Sprintf("trace_id:%q", traceID), "trace_id"); len(got) != 0 {
		t.Fatalf("staging-team sees acme-corp's trace: %v", got)
	}
}
func TestStringTenant_BothSignalsStringTenantSummary(t *testing.T) {
	t.Log("=== String Tenant Cross-Signal Summary ===")

	for _, endpoint := range []struct {
		name string
		url  string
	}{
		{"logs", logsBaseURL},
		{"traces", tracesBaseURL},
	} {
		resp, err := http.Get(endpoint.url + "/lakehouse/api/v1/tenants/aliases")
		if err != nil {
			t.Logf("  %s: failed to fetch aliases: %v", endpoint.name, err)
			continue
		}
		var result struct {
			Aliases []struct {
				OrgID     string `json:"org_id"`
				AccountID uint32 `json:"account_id"`
				ProjectID uint32 `json:"project_id"`
			} `json:"aliases"`
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		_ = json.Unmarshal(body, &result)

		t.Logf("  %s: %d aliases registered", endpoint.name, len(result.Aliases))
		for _, a := range result.Aliases {
			t.Logf("    %s -> %d:%d", a.OrgID, a.AccountID, a.ProjectID)
		}
	}
}

func httpGetWithOrgID(t *testing.T, baseURL, path string, params url.Values, orgID string) (int, []byte) {
	t.Helper()
	params = e2eParams(baseURL, path, params)

	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set("X-Scope-OrgID", orgID)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s with OrgID %s failed: %v", u, orgID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}
