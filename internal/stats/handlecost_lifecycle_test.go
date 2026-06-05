package stats

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestHandleCost_ManifestFallback_UsesLifecyclePrediction pins the fix
// that wired StorageClassTracker.PredictClass into handleCost's
// manifest-fallback path. Before the fix, every byte was billed as
// STANDARD even when files were old enough to have transitioned to
// IA or GLACIER per the lifecycle rules. After the fix, the response
// reflects the predicted class for each file based on its CreatedAt.
//
// This is the read-only / datagen-only deployment path; the registry-
// backed path uses gs.BytesByClass which already aggregated correctly.
func TestHandleCost_ManifestFallback_UsesLifecyclePrediction(t *testing.T) {
	now := time.Now()
	m := manifest.New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")

	// Files of three ages: 1 day (STANDARD), 45 days (IA), 100 days (GLACIER).
	m.AddFile("dt=2026-06-04/hour=00", manifest.FileInfo{
		Key:       "0/0/traces/dt=2026-06-04/hour=00/recent.parquet",
		Size:      1_000_000_000,
		CreatedAt: now.Add(-1 * 24 * time.Hour),
	})
	m.AddFile("dt=2026-04-20/hour=00", manifest.FileInfo{
		Key:       "0/0/traces/dt=2026-04-20/hour=00/medium.parquet",
		Size:      2_000_000_000,
		CreatedAt: now.Add(-45 * 24 * time.Hour),
	})
	m.AddFile("dt=2026-02-25/hour=00", manifest.FileInfo{
		Key:       "0/0/traces/dt=2026-02-25/hour=00/old.parquet",
		Size:      3_000_000_000,
		CreatedAt: now.Add(-100 * 24 * time.Hour),
	})

	prices := map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
		"GLACIER":     0.004,
	}
	cc := NewCostCalculator(prices, nil)

	// Lifecycle rules: STANDARD → IA at 30d → GLACIER at 90d.
	sct := NewStorageClassTracker(
		[]config.LifecycleRuleConfig{
			{TransitionDays: 30, StorageClass: "STANDARD_IA"},
			{TransitionDays: 90, StorageClass: "GLACIER"},
		},
		nil,
	)

	api := NewAPI(APIConfig{
		Registry:     NewTenantRegistry("test-cluster"), // empty registry forces fallback
		Manifest:     m,
		CostCalc:     cc,
		ClassTracker: sct,
		LabelIndex:   cache.NewLabelIndex(),
		Mode:         "traces",
		Bucket:       "test-bucket",
	})

	rec := doGet(t, api, "/lakehouse/api/v1/stats/cost")
	if rec.Code != http.StatusOK {
		t.Fatalf("cost endpoint returned %d: %s", rec.Code, rec.Body.String())
	}
	var resp CostResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse: %v", err)
	}

	// We should see THREE distinct class entries — not one bucket of
	// "STANDARD" lumping everything together.
	if len(resp.ByClass) < 2 {
		t.Errorf("expected lifecycle-predicted breakdown (≥2 classes), got %d:", len(resp.ByClass))
		for _, c := range resp.ByClass {
			t.Logf("  %s: %d bytes, $%.4f", c.Class, c.Bytes, c.CostUSD)
		}
	}
	classes := map[string]int64{}
	for _, c := range resp.ByClass {
		classes[c.Class] = c.Bytes
	}
	if classes["STANDARD"] != 1_000_000_000 {
		t.Errorf("recent file should be STANDARD: got %d for STANDARD bytes, want 1GB", classes["STANDARD"])
	}
	if classes["STANDARD_IA"] != 2_000_000_000 {
		t.Errorf("45-day file should be STANDARD_IA: got %d for IA bytes, want 2GB", classes["STANDARD_IA"])
	}
	if classes["GLACIER"] != 3_000_000_000 {
		t.Errorf("100-day file should be GLACIER: got %d for GLACIER bytes, want 3GB", classes["GLACIER"])
	}

	// Total cost reflects the cheaper IA/GLACIER tiers rather than
	// inflating everything to STANDARD prices.
	// 1 GB STANDARD ($0.023) + 2 GB IA ($0.025) + 3 GB GLACIER ($0.012) ≈ $0.06
	// All STANDARD would be ($0.023 × 6 GB) ≈ $0.138
	if resp.TotalMonthlyUSD > 0.10 {
		t.Errorf("total cost $%.4f suggests no lifecycle discount applied (all-STANDARD ≈ $0.138)",
			resp.TotalMonthlyUSD)
	}
}
