//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// TestTenantPolicy_Endpoint_RespondsOnLiveStack hits the new
// /api/v1/tenants/policy endpoint on both LH services and asserts it
// returns a well-formed response even when no per-tenant overrides are
// configured (entries empty, pending_aliases omitted/empty). Guards
// the route registration so a misnamed handler shows up in CI.
func TestTenantPolicy_Endpoint_RespondsOnLiveStack(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		resp, err := http.Get(base + "/lakehouse/api/v1/tenants/policy")
		if err != nil {
			t.Errorf("GET %s/lakehouse/api/v1/tenants/policy: %v", base, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s status=%d body=%s", base, resp.StatusCode, string(body))
			continue
		}
		var parsed struct {
			Entries        []map[string]any `json:"entries"`
			PendingAliases []string         `json:"pending_aliases"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Errorf("%s unmarshal: %v body=%s", base, err, string(body))
			continue
		}
		t.Logf("%s policy endpoint OK — %d override entries, %d pending aliases",
			base, len(parsed.Entries), len(parsed.PendingAliases))
	}
}
