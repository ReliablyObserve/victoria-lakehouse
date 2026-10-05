//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestTenantAPI_BreakdownEndpointGroupsByTenant calls
// /api/v1/stats/breakdown?group_by=tenant on the running lakehouse-logs
// service and asserts the response carries per-tenant rows with both the
// integer Victoria key (account:project) and the resolver-decorated org_id
// where an alias exists. This is the contract the UI relies on for the
// tenant facet on the Storage Breakdown screen.
func TestTenantAPI_BreakdownEndpointGroupsByTenant(t *testing.T) {
	orgID := fmt.Sprintf("e2e-breakdown-%d", time.Now().UnixNano())
	ingestLog(t, logsBaseURL, withOrgID(orgID), "breakdown e2e")
	// The breakdown is computed from the manifest, i.e. from Parquet. Rows are
	// durable in the insert buffer first and reach S3 at most
	// insert.buffer_flush_interval (2m on this stack) after their segment opened,
	// so poll until the tenant's object shows up instead of sleeping.
	var body []byte
	deadline := time.Now().Add(5 * time.Minute)
	for {
		resp, err := http.Get(logsBaseURL + "/lakehouse/api/v1/stats/breakdown?group_by=tenant")
		if err != nil {
			t.Fatalf("breakdown request: %v", err)
		}
		body, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("breakdown returned %d: %s", resp.StatusCode, string(body))
		}
		if strings.Contains(string(body), orgID) || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}

	var parsed struct {
		Labels []struct {
			Name        string `json:"name"`
			Cardinality int    `json:"cardinality"`
			Type        string `json:"type"`
			Values      []struct {
				Value          string `json:"value"`
				OrgID          string `json:"org_id"`
				EstimatedBytes int64  `json:"estimated_bytes"`
			} `json:"values"`
		} `json:"labels"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, string(body))
	}
	if len(parsed.Labels) != 1 || parsed.Labels[0].Name != "tenant" {
		t.Fatalf("expected single tenant label, got %+v", parsed.Labels)
	}

	bl := parsed.Labels[0]
	if bl.Cardinality < 2 {
		t.Errorf("tenant cardinality = %d, want >=2 (default + e2e tenant)", bl.Cardinality)
	}

	var sawOrgID bool
	for _, v := range bl.Values {
		if v.OrgID == orgID {
			sawOrgID = true
			if v.EstimatedBytes == 0 {
				t.Errorf("tenant %q reported in breakdown but estimated_bytes=0", orgID)
			}
			break
		}
	}
	if !sawOrgID {
		t.Errorf("breakdown response missing org_id %q; got values=%+v", orgID, bl.Values)
	}
}

// TestTenantUI_RendersBidirectionalMapping hits the lakehouse UI's static
// tenants HTML/JS and asserts the new column headers (Source, S3 Prefix)
// are present in the served assets. Cheap shape check — doesn't actually
// render the SPA — but it guards against accidental UI regressions on the
// tenants tab when the file is edited.
func TestTenantUI_RendersBidirectionalMapping(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		for _, asset := range []string{"/lakehouse/ui/lakehouse-ui.js"} {
			url := base + asset
			resp, err := http.Get(url)
			if err != nil {
				t.Errorf("GET %s: %v", url, err)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			s := string(body)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s status=%d", url, resp.StatusCode)
				continue
			}
			for _, want := range []string{"Victoria ID", "Org / Name", "S3 Prefix", "Source"} {
				if !strings.Contains(s, want) {
					t.Errorf("%s missing UI element %q", url, want)
				}
			}
		}
	}
}
