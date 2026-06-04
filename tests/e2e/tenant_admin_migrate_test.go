//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestTenantAdmin_Migrate_AuthGate guards the admin endpoint's
// auth: an unauthenticated POST must return 403, an authenticated
// POST with an empty body must return 400. Doesn't run an actual
// migration (would mutate live S3 state on every CI run); the unit
// tests cover the move logic.
func TestTenantAdmin_Migrate_AuthGate(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		// Unauthorized: no global-read header.
		resp, err := http.Post(base+"/lakehouse/api/v1/admin/tenant/migrate",
			"application/json", strings.NewReader(`{"tenant_key":"1:1","target_bucket":"x"}`))
		if err != nil {
			t.Errorf("%s POST: %v", base, err)
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s unauthenticated: status=%d, want 403", base, resp.StatusCode)
		}

		// Authorized with header but missing required fields → 400.
		req, _ := http.NewRequest("POST", base+"/lakehouse/api/v1/admin/tenant/migrate",
			strings.NewReader(`{"tenant_key":"1:1"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Lakehouse-Global-Read", "lakehouse-e2e-global-key")
		resp, err = (&http.Client{}).Do(req)
		if err != nil {
			t.Errorf("%s authorized POST: %v", base, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s missing target_bucket: status=%d body=%s, want 400",
				base, resp.StatusCode, string(body))
		}
		t.Logf("%s admin endpoint auth-gated correctly", base)
	}
}
