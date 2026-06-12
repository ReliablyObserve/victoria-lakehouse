package stats

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestHandleCompaction is the "compaction hints + stats stay visible" regression:
// the endpoint must return the full CompactionStats JSON — per-level zstd, stale
// footprint, prioritized candidates, and the per-candidate before/after + next-level
// — so a future change can't silently break what the UI reads.
func TestHandleCompaction(t *testing.T) {
	m := manifest.New("bucket", "logs/")
	// 3 stale (v1) L2 files → one stale+fragmented candidate.
	for i := 0; i < 3; i++ {
		m.AddFile("dt=2026-06-01/hour=00", manifest.FileInfo{
			Key:               fmt.Sprintf("logs/dt=2026-06-01/hour=00/f%d.parquet", i),
			Size:              1_000_000,
			RawBytes:          9_000_000,
			CompactionLevel:   2,
			SchemaFingerprint: "v1",
		})
	}
	cc := config.CompactionConfig{CompressionLevelByOutputLevel: []int{3, 7, 11}}
	a := NewAPI(APIConfig{Manifest: m, CurrentSchemaFingerprint: "v2", CompactionConfig: cc})

	rr := httptest.NewRecorder()
	a.handleCompaction(rr, httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/stats/compaction", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if cch := rr.Header().Get("Cache-Control"); !strings.Contains(cch, "no-store") {
		t.Errorf("Cache-Control = %q, want no-store", cch)
	}

	var got manifest.CompactionStats
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.StaleSchemaFiles != 3 {
		t.Errorf("stale_schema_files = %d, want 3", got.StaleSchemaFiles)
	}
	if got.CompactedBytes != 3_000_000 {
		t.Errorf("compacted_bytes = %d, want 3000000", got.CompactedBytes)
	}
	if len(got.Candidates) != 1 {
		t.Fatalf("candidates = %d, want 1", len(got.Candidates))
	}
	c := got.Candidates[0]
	if c.NextLevel != 3 || c.NextLevelZstd != 11 {
		t.Errorf("candidate next_level=%d next_level_zstd=%d, want 3/11 (level 3 clamps to last)", c.NextLevel, c.NextLevelZstd)
	}
	if c.EstimatedBytesAfter != c.Bytes-c.EstimatedSavingsBytes {
		t.Errorf("estimated_bytes_after=%d, want %d", c.EstimatedBytesAfter, c.Bytes-c.EstimatedSavingsBytes)
	}
	var sawL2zstd bool
	for _, ls := range got.ByLevel {
		if ls.Level == 2 {
			sawL2zstd = ls.ConfiguredZstd == 11
		}
	}
	if !sawL2zstd {
		t.Error("by_level L2 configured_zstd != 11")
	}

	// nil manifest → empty (no panic).
	a2 := NewAPI(APIConfig{})
	rr2 := httptest.NewRecorder()
	a2.handleCompaction(rr2, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rr2.Code != http.StatusOK {
		t.Errorf("nil-manifest status = %d, want 200", rr2.Code)
	}
}
