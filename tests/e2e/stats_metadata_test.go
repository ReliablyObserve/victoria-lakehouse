//go:build e2e

package e2e

import (
	"encoding/json"
	"testing"
)

// statsTargets is the (name, baseURL) pair for each binary the stats API runs on.
func statsTargets() []struct{ name, baseURL string } {
	return []struct{ name, baseURL string }{
		{"logs", logsBaseURL},
		{"traces", tracesBaseURL},
	}
}

// TestStatsOverviewMetadata is the e2e guard for Phase B: the Storage Overview
// exposes the metadata footprint (pmeta RAM + disk cache + on-S3 metadata). The
// on-S3 figure is tracked INCREMENTALLY (bundles record their encoded size on
// persist/warm/compaction) — never by an S3 LIST. pmeta is enabled in the e2e
// stack, so RAM + on-S3 metadata are non-zero on a warm node. Both binaries.
func TestStatsOverviewMetadata(t *testing.T) {
	for _, tc := range statsTargets() {
		t.Run(tc.name, func(t *testing.T) {
			body := httpGetBody(t, tc.baseURL, "/lakehouse/api/v1/stats/overview", nil)
			var ov struct {
				MetaResidentBytes int64 `json:"meta_resident_bytes"`
				MetaDiskBytes     int64 `json:"meta_disk_bytes"`
				MetaS3Bytes       int64 `json:"meta_s3_bytes"`
			}
			if err := json.Unmarshal(body, &ov); err != nil {
				t.Fatalf("decode overview: %v (body=%s)", err, body)
			}
			if ov.MetaResidentBytes <= 0 {
				t.Errorf("meta_resident_bytes = %d, want > 0 (pmeta RAM footprint)", ov.MetaResidentBytes)
			}
			if ov.MetaS3Bytes <= 0 {
				t.Errorf("meta_s3_bytes = %d, want > 0 (incremental on-S3 metadata, no scan)", ov.MetaS3Bytes)
			}
			t.Logf("%s metadata footprint: RAM=%d disk=%d s3=%d", tc.name, ov.MetaResidentBytes, ov.MetaDiskBytes, ov.MetaS3Bytes)
		})
	}
}

// TestCardinalityStorageBytes guards the Cardinality Explorer Storage column:
// /cardinality/fields returns per-field storage_bytes scaled to real magnitude
// (MB-scale, not the KB-scale undercount of the original bug). Asserts the top
// indexed field reports at least 1 MB of storage. Both binaries.
func TestCardinalityStorageBytes(t *testing.T) {
	for _, tc := range statsTargets() {
		t.Run(tc.name, func(t *testing.T) {
			body := httpGetBody(t, tc.baseURL, "/lakehouse/api/v1/cardinality/fields", nil)
			var resp struct {
				Fields []struct {
					Name         string `json:"name"`
					StorageBytes int64  `json:"storage_bytes"`
				} `json:"fields"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatalf("decode cardinality: %v (body=%s)", err, body)
			}
			var maxStorage int64
			var topField string
			for _, f := range resp.Fields {
				if f.StorageBytes > maxStorage {
					maxStorage, topField = f.StorageBytes, f.Name
				}
			}
			const oneMB = 1 << 20
			if maxStorage < oneMB {
				t.Errorf("max per-field storage_bytes = %d (%s), want >= 1MB (scaled to the real on-S3 total, not KB)", maxStorage, topField)
			}
			t.Logf("%s top storage field: %s = %d bytes", tc.name, topField, maxStorage)
		})
	}
}
