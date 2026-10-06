//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

// settledParity fetches the parity sample until the endpoint reports it was
// not disturbed by a flush, a compaction commit or ingest between its reads
// (it brackets the buffer and the manifest around the VL read and resamples
// itself). A sample that never settles fails: with ingest running the
// comparison is not defined, and that must not pass as parity.
func settledParity(t *testing.T, base string) map[string]any {
	t.Helper()
	var body map[string]any
	for try := 1; try <= 6; try++ {
		body = fetchParity(t, base, "24h")
		if unstable, _ := body["unstable_sample"].(bool); !unstable {
			return body
		}
		time.Sleep(2 * time.Second)
	}
	t.Errorf("%s parity: the sample never settled in 6 tries (buffer or manifest kept changing); last: %v", base, body)
	return body
}

// TestParity_VLViewVsManifest asserts the operator endpoint's two views agree
// on the SAME scope: the embedded VL `* | stats count()` (every tenant, via the
// caller's global-read credential, including rows only the insert buffer holds)
// and the manifest aggregate over the same hour-aligned window.
//
// The expected gap is only the rows of live UNCOMMITTED buffer segments that the
// manifest does not hold yet (buffer_unflushed_rows). Everything else must be
// exact:
//
//	vl_rows - manifest_rows - buffer_unflushed_rows == 0
//
// and no live segment may disagree with its own objects (segment_mismatches
// empty: a committed segment serves exactly its objects' rows). The comparison
// has no tolerance: the endpoint reads the buffer and the manifest before and
// after the VL read and repeats the sample when either changed, so on a settled
// stack a single lost file of any size is a residual. For BOTH binaries, with no
// per-signal allowance.
func TestParity_VLViewVsManifest(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		body := settledParity(t, base)

		vl, _ := body["vl_rows"].(float64)
		mf, _ := body["manifest_rows"].(float64)
		if vl == 0 && mf == 0 {
			t.Logf("%s parity: both views report 0 rows over 24h (no data)", base)
			continue
		}
		if mf == 0 {
			t.Errorf("%s parity: manifest reports 0 rows but VL reports %.0f", base, vl)
			continue
		}
		if scope, _ := body["scope"].(string); scope != "all_tenants" {
			t.Errorf("%s parity scope=%q, want all_tenants", base, scope)
		}
		if _, ok := body["buffer_unflushed_rows"]; !ok {
			t.Errorf("%s parity response has no buffer_unflushed_rows", base)
		}
		if e, _ := body["buffer_error"].(string); e != "" {
			t.Errorf("%s parity buffer_error=%q", base, e)
		}
		if mm, _ := body["segment_mismatches"].([]any); len(mm) != 0 {
			t.Errorf("%s parity: live segments disagree with their own objects: %v", base, mm)
		}

		buffer, _ := body["buffer_unflushed_rows"].(float64)
		residual := vl - mf - buffer
		if residual != 0 {
			t.Errorf("%s parity residual %.0f rows, want 0 (vl=%.0f manifest=%.0f buffer_unflushed=%.0f attempts=%.0f attribution=%v)",
				base, residual, vl, mf, buffer, safeFloat(body["sample_attempts"]), body["buffer_attribution"])
		} else {
			t.Logf("%s parity OK: residual=0 vl=%.0f manifest=%.0f buffer_unflushed=%.0f attempts=%.0f",
				base, vl, mf, buffer, safeFloat(body["sample_attempts"]))
		}

		if supported, _ := body["per_tenant_supported"].(bool); supported {
			t.Errorf("%s per_tenant_supported=true but per-tenant parity isn't implemented yet — update the test", base)
		}
	}
}

// TestParity_AuthRequired verifies the admin gate is closed by default.
func TestParity_AuthRequired(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		resp, err := http.Get(base + "/lakehouse/api/v1/admin/parity")
		if err != nil {
			t.Errorf("%s: %v", base, err)
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s unauthenticated parity: status=%d, want 403", base, resp.StatusCode)
		}
	}
}

// safeFloat coerces a JSON number-or-nil into a float, returning 0 when
// the field is absent or non-numeric. Used because the parity
// response's optional fields (verified_drift_pct, expected_drift)
// don't appear when their internal counter isn't wired.
func safeFloat(v any) float64 {
	if v == nil {
		return 0
	}
	f, _ := v.(float64)
	return f
}

func fetchParity(t *testing.T, base, window string) map[string]any {
	t.Helper()
	req, _ := http.NewRequest("GET", base+"/lakehouse/api/v1/admin/parity?window="+window, nil)
	req.Header.Set("X-Lakehouse-Global-Read", "lakehouse-e2e-global-key")
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s parity: %v", base, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s parity status=%d body=%s", base, resp.StatusCode, string(body))
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("parse parity: %v body=%s", err, string(body))
	}
	return parsed
}
