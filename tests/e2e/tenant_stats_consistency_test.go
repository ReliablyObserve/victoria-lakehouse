//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
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

// TestManifest_CompactedFilesPreserveRawBytes is the manifest-side
// regression guard for the compactor RawBytes carry-forward fix.
// Walks /manifest/range, isolates files with compaction_level > 0
// (i.e. produced by the compactor, not the writer), and asserts
// their raw_bytes field is non-zero. The bug zeroed RawBytes on
// every compacted FileInfo, which then propagated into the
// per-tenant sums and made compression_ratio < 1.0 wherever
// compaction had run.
func TestManifest_CompactedFilesPreserveRawBytes(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		resp, err := http.Get(base + "/manifest/range")
		if err != nil {
			t.Fatalf("GET %s/manifest/range: %v", base, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s/manifest/range status=%d body=%s", base, resp.StatusCode, string(body))
		}

		var parsed struct {
			Partitions map[string][]struct {
				Key             string `json:"key"`
				Size            int64  `json:"size"`
				RawBytes        int64  `json:"raw_bytes"`
				CompactionLevel int    `json:"compaction_level"`
			} `json:"partitions"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("unmarshal %s manifest: %v body=%.200s", base, err, string(body))
		}

		var (
			compactedCount int
			compactedZero  int
			compactedZeros []string
		)
		for _, files := range parsed.Partitions {
			for _, fi := range files {
				if fi.CompactionLevel == 0 {
					continue
				}
				compactedCount++
				if fi.RawBytes == 0 {
					compactedZero++
					if len(compactedZeros) < 5 {
						compactedZeros = append(compactedZeros, fi.Key)
					}
				}
			}
		}

		if compactedCount == 0 {
			t.Logf("%s: no compacted files in manifest yet — test will start enforcing after compaction runs", base)
			continue
		}

		// Allow a small grace for files compacted before the fix
		// shipped — those entries are immutable on disk until they
		// get rolled into a higher compaction level. New compactions
		// from this build must all carry raw_bytes forward.
		grace := compactedCount / 10
		if compactedZero > grace {
			t.Errorf("%s: %d/%d compacted files have raw_bytes=0 (allowed grace=%d); first offenders=%s — compactor regressed",
				base, compactedZero, compactedCount, grace, strings.Join(compactedZeros, ", "))
		}
	}
}

// TestTenantStats_CompressionRatioReasonable is the tighter API-side
// guard. Beyond the existing "not inverted" check, this asserts the
// ratio is in a sane range (1.0 <= r <= 50.0) for tenants with
// at-least-typical-file sizes (64 KiB +). A ratio above 50 usually
// means raw was double-counted; below 1.0 means compressed was
// double-counted OR raw lost (the compactor bug). Both signal
// accounting drift the UI propagates into per-tenant cost/storage
// panels.
func TestTenantStats_CompressionRatioReasonable(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		tn := fetchJSONMap(t, base+"/lakehouse/api/v1/tenants")
		for _, raw := range tn["tenants"].([]any) {
			e := raw.(map[string]any)
			compressed := int64(e["total_bytes"].(float64))
			rawB := int64(e["raw_bytes"].(float64))
			if compressed < 64*1024 {
				continue
			}
			ratio, _ := e["compression_ratio"].(float64)
			if ratio < 1.0 {
				t.Errorf("%s tenant %s:%s: compression_ratio=%.3f (raw=%d compressed=%d) — accounting inversion",
					base, e["account_id"], e["project_id"], ratio, rawB, compressed)
			}
			if ratio > 50.0 {
				t.Errorf("%s tenant %s:%s: compression_ratio=%.3f (raw=%d compressed=%d) — implausibly high, raw likely double-counted",
					base, e["account_id"], e["project_id"], ratio, rawB, compressed)
			}
		}
	}
}

// TestTenantUI_RendersCompressionAndRawBytesFields exercises the
// Lakehouse Explorer UI shell to make sure the tenants tab still
// surfaces the compression_ratio and raw_bytes columns. Compressed-
// > raw is a UX trap; the column being missing is also a UX trap
// because operators then can't tell from the UI alone that the
// numbers are inverted. Cheap shape check on the SPA assets.
func TestTenantUI_RendersCompressionAndRawBytesFields(t *testing.T) {
	for _, base := range []string{logsBaseURL, tracesBaseURL} {
		resp, err := http.Get(base + "/lakehouse/ui/")
		if err != nil {
			t.Errorf("GET %s/lakehouse/ui/: %v", base, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s/lakehouse/ui/ status=%d", base, resp.StatusCode)
			continue
		}
		s := string(body)
		for _, want := range []string{"compression_ratio", "raw_bytes", "total_bytes"} {
			if !strings.Contains(s, want) {
				t.Errorf("%s/lakehouse/ui/ missing stats column %q in served SPA bundle", base, want)
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
