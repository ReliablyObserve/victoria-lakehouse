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

// TestTenantPolicy_YAMLConfig_LivesOnStack ingests an alias-keyed
// orgID known to the compose-mounted policy file
// (deployment/docker/lakehouse-e2e-config.yml) and verifies the
// /api/v1/tenants/policy endpoint surfaces the full effective config
// — retention duration, cardinality caps, ingest rate caps, and
// lifecycle transitions — once the alias resolves.
//
// Guards: the YAML loader actually merges Tenant.Overrides, the
// PolicyRegistry refreshes pending alias-keyed entries on the sync
// tick, and the API decoration round-trips through to operators.
func TestTenantPolicy_YAMLConfig_LivesOnStack(t *testing.T) {
	registerOrgID := func(base, org string) {
		t.Helper()
		body := fmt.Sprintf(`{"_time":%q,"_msg":"e2e-policy-register","service.name":"e2e"}`,
			time.Now().Format(time.RFC3339Nano))
		req, _ := http.NewRequest("POST", base+"/insert/jsonline?_stream_fields=service.name",
			bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("X-Scope-OrgID", org)
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatalf("ingest for %q: %v", org, err)
		}
		_ = resp.Body.Close()
	}

	// acme-corp and staging-team are both alias-keyed in the YAML.
	// Trigger auto-register so the policy registry can resolve them.
	registerOrgID(logsBaseURL, "acme-corp")
	registerOrgID(logsBaseURL, "staging-team")

	// Wait for the alias-sync tick to re-resolve pending entries.
	deadline := time.Now().Add(60 * time.Second)
	var acme, staging map[string]any
	for time.Now().Before(deadline) {
		acme, staging = fetchAlias(t, logsBaseURL, "acme-corp", "staging-team")
		if acme != nil && staging != nil {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if acme == nil {
		t.Fatal("acme-corp never resolved into policy entries")
	}
	if staging == nil {
		t.Fatal("staging-team never resolved into policy entries")
	}

	if got := acme["retention"]; got != "2160h0m0s" {
		t.Errorf("acme retention = %v, want 2160h0m0s (90d)", got)
	}
	if got := acme["max_bytes_per_sec"]; got != float64(5*1024*1024) {
		t.Errorf("acme max_bytes_per_sec = %v, want 5242880", got)
	}
	if got := acme["max_streams"]; got != float64(5000) {
		t.Errorf("acme max_streams = %v, want 5000", got)
	}
	if lc, ok := acme["lifecycle"].([]any); !ok || len(lc) != 2 {
		t.Errorf("acme lifecycle = %v, want 2 rules", acme["lifecycle"])
	} else {
		first := lc[0].(map[string]any)
		if first["storage_class"] != "ONEZONE_IA" || first["transition_days"] != float64(7) {
			t.Errorf("acme lifecycle[0] = %v, want ONEZONE_IA @7d", first)
		}
	}

	if got := staging["retention"]; got != "720h0m0s" {
		t.Errorf("staging retention = %v, want 720h0m0s (30d)", got)
	}
	if got := staging["max_streams"]; got != float64(50000) {
		t.Errorf("staging max_streams = %v, want 50000", got)
	}

	t.Log("policy override loaded from YAML and resolved through alias map ✓")
}

func fetchAlias(t *testing.T, base string, want ...string) (map[string]any, map[string]any) {
	t.Helper()
	resp, err := http.Get(base + "/lakehouse/api/v1/tenants/policy")
	if err != nil {
		return nil, nil
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	var parsed struct {
		Entries []map[string]any `json:"entries"`
	}
	_ = json.Unmarshal(body, &parsed)
	results := make([]map[string]any, len(want))
	for _, e := range parsed.Entries {
		for i, w := range want {
			if e["org_id"] == w {
				results[i] = e
			}
		}
	}
	if len(want) == 2 {
		return results[0], results[1]
	}
	return results[0], nil
}
