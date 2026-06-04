//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// TestTenantStats_OverviewMatchesTenantSum asserts that the global
// /stats/overview totals agree with the sum of per-tenant rows from
// /tenants. Before the manifest-overlay fix the registry's
// cumulative counters drifted higher than the manifest after every
// compaction, causing /tenants to over-report files/bytes vs the
// overview by hundreds of files. Lock that down.
func TestTenantStats_OverviewMatchesTenantSum(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		ov := fetchJSONMap(t, base+"/lakehouse/api/v1/stats/overview")
		tn := fetchJSONMap(t, base+"/lakehouse/api/v1/tenants")

		globalFiles := int64(ov["total_files"].(float64))
		globalBytes := int64(ov["total_bytes"].(float64))
		globalRows := int64(ov["total_rows"].(float64))

		var sumFiles, sumBytes, sumRows int64
		for _, raw := range tn["tenants"].([]any) {
			e := raw.(map[string]any)
			sumFiles += int64(e["total_files"].(float64))
			sumBytes += int64(e["total_bytes"].(float64))
			sumRows += int64(e["total_rows"].(float64))
		}

		if globalFiles != sumFiles {
			t.Errorf("%s files: overview=%d vs tenant-sum=%d (delta=%d) — registry/manifest desync",
				base, globalFiles, sumFiles, globalFiles-sumFiles)
		}
		// Bytes / rows have a small acceptable lag because compaction
		// runs may overlap with the API call; require exact match for
		// files (more stable) and within 1% for bytes/rows.
		if bytesDelta := abs64(globalBytes - sumBytes); bytesDelta > globalBytes/100 {
			t.Errorf("%s bytes desync > 1%%: overview=%d vs tenant-sum=%d (delta=%d)",
				base, globalBytes, sumBytes, bytesDelta)
		}
		if rowsDelta := abs64(globalRows - sumRows); rowsDelta > globalRows/100 {
			t.Errorf("%s rows desync > 1%%: overview=%d vs tenant-sum=%d (delta=%d)",
				base, globalRows, sumRows, rowsDelta)
		}
		t.Logf("%s consistent: files=%d bytes=%d rows=%d", base, globalFiles, globalBytes, globalRows)
	}
}

// TestTenantStats_CompressionRatioNotInverted regression-guards the
// raw-bytes undercount in estimateRaw{Logs,Traces}: with every
// string column counted the ratio (raw/compressed) is >= 1.0 for
// every populated tenant. Allow tiny files (< 16 KiB compressed) to
// slip — Parquet page/footer overhead can legitimately exceed
// content for trivial workloads.
func TestTenantStats_CompressionRatioNotInverted(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		tn := fetchJSONMap(t, base+"/lakehouse/api/v1/tenants")
		for _, raw := range tn["tenants"].([]any) {
			e := raw.(map[string]any)
			compressed := int64(e["total_bytes"].(float64))
			rawB := int64(e["raw_bytes"].(float64))
			if compressed < 16*1024 {
				continue // Parquet overhead vs payload — not a real signal.
			}
			if rawB < compressed {
				t.Errorf("%s tenant %s:%s: raw_bytes=%d < compressed=%d (inverted ratio)",
					base, e["account_id"], e["project_id"], rawB, compressed)
			}
		}
	}
}

func fetchJSONMap(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal %s: %v body=%s", url, err, string(body))
	}
	return out
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
