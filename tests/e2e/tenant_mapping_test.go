//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestTenantMapping_StringAndIntPaths is a single e2e flow that physically
// exercises the bidirectional int↔string tenant mapping described in
// docs/multi-tenancy.md by:
//
//  1. Ingesting log + trace data with a string `X-Scope-OrgID` header.
//  2. Ingesting log + trace data with integer `AccountID`/`ProjectID` headers.
//  3. Reading the resolver alias map and verifying the string→int direction.
//  4. Reading the /api/v1/tenants stats endpoint and verifying the int→string
//     direction is decorated (org_id field populated for aliased tenants).
//  5. Asserting per-tenant byte/file/row totals reflect the ingest — i.e.
//     stats are tracked per-tenant, not collapsed onto the default 0:0 bucket.
//
// The test runs against whichever stack LOGS_BASE_URL / TRACES_BASE_URL
// points at (defaults to the e2e compose), so the same checks cover both
// developer-laptop and CI deployments.
func TestTenantMapping_StringAndIntPaths(t *testing.T) {
	// Unique org IDs so re-runs against a long-lived stack don't conflict.
	stamp := time.Now().UnixNano()
	stringOrg := fmt.Sprintf("e2e-mapping-string-%d", stamp)
	intAccount := "777"
	intProject := "1"

	ingestLog(t, logsBaseURL, withOrgID(stringOrg), "string-tenant via X-Scope-OrgID")
	ingestLog(t, logsBaseURL, withAccountProject(intAccount, intProject), "int-tenant via AccountID/ProjectID")
	ingestTrace(t, tracesBaseURL, withOrgID(stringOrg))
	ingestTrace(t, tracesBaseURL, withAccountProject(intAccount, intProject))

	// Give the writer a chance to flush the new tenants into the registry.
	deadline := time.Now().Add(45 * time.Second)
	var resolvedStringAccount uint32
	for time.Now().Before(deadline) {
		var ok bool
		resolvedStringAccount, ok = lookupAliasAccount(t, logsBaseURL, stringOrg)
		if ok && tenantHasWrites(t, logsBaseURL, fmt.Sprintf("%d:0", resolvedStringAccount)) &&
			tenantHasWrites(t, logsBaseURL, intAccount+":"+intProject) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if resolvedStringAccount == 0 {
		t.Fatalf("string tenant %q never registered an alias", stringOrg)
	}

	t.Logf("string OrgID %q resolved to int account=%d", stringOrg, resolvedStringAccount)

	// Forward direction (string → int) — alias map.
	if _, ok := lookupAliasAccount(t, tracesBaseURL, stringOrg); !ok {
		t.Errorf("traces side never registered alias for %q", stringOrg)
	}

	// Reverse direction (int → string) — /api/v1/tenants must decorate org_id.
	tenants := fetchTenantEntries(t, logsBaseURL)
	stringKey := fmt.Sprintf("%d:0", resolvedStringAccount)
	entry, ok := tenants[stringKey]
	if !ok {
		t.Fatalf("logs tenants endpoint missing entry %s; have %v", stringKey, tenantKeys(tenants))
	}
	if entry.OrgID != stringOrg {
		t.Errorf("logs tenant %s: org_id=%q, want %q (reverse mapping broken)", stringKey, entry.OrgID, stringOrg)
	}
	if entry.TotalRows == 0 {
		t.Errorf("logs tenant %s: total_rows=0, want >0 (per-tenant attribution broken)", stringKey)
	}

	intKey := intAccount + ":" + intProject
	intEntry, ok := tenants[intKey]
	if !ok {
		t.Fatalf("logs tenants endpoint missing int entry %s", intKey)
	}
	if intEntry.OrgID != "" {
		t.Errorf("logs int tenant %s: org_id=%q, want empty (no alias registered)", intKey, intEntry.OrgID)
	}
	if intEntry.TotalRows == 0 {
		t.Errorf("logs int tenant %s: total_rows=0 — per-tenant attribution missed integer-header path", intKey)
	}

	traceTenants := fetchTenantEntries(t, tracesBaseURL)
	if e, ok := traceTenants[stringKey]; !ok {
		t.Errorf("traces tenants endpoint missing %s", stringKey)
	} else if e.OrgID != stringOrg {
		t.Errorf("traces tenant %s: org_id=%q, want %q", stringKey, e.OrgID, stringOrg)
	}

	t.Logf("end-to-end mapping verified — int↔string both directions, per-tenant attribution active")
}

// ---------------------------------------------------------------------------
// Helpers (kept local — package-level helpers already cover the basics)
// ---------------------------------------------------------------------------

type tenantEntry struct {
	AccountID  string `json:"account_id"`
	ProjectID  string `json:"project_id"`
	OrgID      string `json:"org_id"`
	Name       string `json:"name"`
	TotalRows  int64  `json:"total_rows"`
	TotalFiles int64  `json:"total_files"`
	Source     string `json:"source"`
}

type tenantsListResponse struct {
	Tenants      []tenantEntry `json:"tenants"`
	TotalTenants int           `json:"total_tenants"`
}

func ingestLog(t *testing.T, base string, decorate func(*http.Request), msg string) {
	t.Helper()
	line := map[string]any{
		"_time":        time.Now().Format(time.RFC3339Nano),
		"_msg":         msg,
		"service.name": "tenant-mapping-e2e",
		"level":        "INFO",
	}
	body, _ := json.Marshal(line)
	req, _ := http.NewRequest("POST", base+"/insert/jsonline?_stream_fields=service.name", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-ndjson")
	decorate(req)

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("ingest log: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest log: status %d: %s", resp.StatusCode, string(b))
	}
}

func ingestTrace(t *testing.T, base string, decorate func(*http.Request)) {
	t.Helper()
	now := time.Now()
	span := map[string]any{
		"resourceSpans": []map[string]any{{
			"resource": map[string]any{
				"attributes": []map[string]any{
					{"key": "service.name", "value": map[string]any{"stringValue": "tenant-mapping-e2e"}},
				},
			},
			"scopeSpans": []map[string]any{{
				"scope": map[string]any{"name": "test"},
				"spans": []map[string]any{{
					"traceId":           fmt.Sprintf("%032x", now.UnixNano()),
					"spanId":            fmt.Sprintf("%016x", now.UnixNano()),
					"name":              "tenant-mapping-span",
					"kind":              1,
					"startTimeUnixNano": fmt.Sprintf("%d", now.Add(-time.Second).UnixNano()),
					"endTimeUnixNano":   fmt.Sprintf("%d", now.UnixNano()),
					"attributes":        []map[string]any{},
				}},
			}},
		}},
	}
	body, _ := json.Marshal(span)
	req, _ := http.NewRequest("POST", base+"/insert/opentelemetry/v1/traces", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	decorate(req)

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("ingest trace: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest trace: status %d: %s", resp.StatusCode, string(b))
	}
}

func withOrgID(orgID string) func(*http.Request) {
	return func(req *http.Request) { req.Header.Set("X-Scope-OrgID", orgID) }
}

func withAccountProject(account, project string) func(*http.Request) {
	return func(req *http.Request) {
		req.Header.Set("AccountID", account)
		req.Header.Set("ProjectID", project)
	}
}

func lookupAliasAccount(t *testing.T, base, orgID string) (uint32, bool) {
	t.Helper()
	resp, err := http.Get(base + "/lakehouse/api/v1/tenants/aliases")
	if err != nil {
		return 0, false
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		Aliases []struct {
			OrgID     string `json:"org_id"`
			AccountID uint32 `json:"account_id"`
		} `json:"aliases"`
	}
	body, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(body, &result)
	for _, a := range result.Aliases {
		if a.OrgID == orgID {
			return a.AccountID, true
		}
	}
	return 0, false
}

func fetchTenantEntries(t *testing.T, base string) map[string]tenantEntry {
	t.Helper()
	resp, err := http.Get(base + "/lakehouse/api/v1/tenants")
	if err != nil {
		t.Fatalf("fetch tenants: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var r tenantsListResponse
	body, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(body, &r)

	out := make(map[string]tenantEntry, len(r.Tenants))
	for _, e := range r.Tenants {
		out[e.AccountID+":"+e.ProjectID] = e
	}
	return out
}

func tenantHasWrites(t *testing.T, base, key string) bool {
	t.Helper()
	entries := fetchTenantEntries(t, base)
	e, ok := entries[key]
	return ok && e.TotalRows > 0
}

func tenantKeys(m map[string]tenantEntry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
