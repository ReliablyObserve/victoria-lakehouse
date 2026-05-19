package bloomindex

import (
	"math"
	"testing"
)

func TestProjectCost_StandardTiers(t *testing.T) {
	costs := DefaultStorageTierCosts()
	tierGBs := map[Tier]float64{
		TierHot:     700,  // 7d * 100TB/d
		TierWarm:    2300, // 23d * 100TB/d
		TierCold:    6000, // 60d * 100TB/d
		TierArchive: 1000,
	}

	proj := ProjectCost(tierGBs, costs)

	if proj.TotalGB != 10000 {
		t.Errorf("total GB = %f, want 10000", proj.TotalGB)
	}
	if proj.TotalCostUSD <= 0 {
		t.Error("cost should be positive")
	}
	if proj.SavingsVsFlat <= 0 {
		t.Error("tiered should save vs flat STANDARD pricing")
	}
	if len(proj.PerTier) != 4 {
		t.Errorf("per tier count = %d, want 4", len(proj.PerTier))
	}

	t.Logf("total: %.0f GB, cost: $%.2f/mo, savings: %.1f%% vs flat", proj.TotalGB, proj.TotalCostUSD, proj.SavingsVsFlat)
}

func TestProjectCost_EmptyTiers(t *testing.T) {
	costs := DefaultStorageTierCosts()
	proj := ProjectCost(map[Tier]float64{}, costs)

	if proj.TotalGB != 0 {
		t.Errorf("total GB = %f, want 0", proj.TotalGB)
	}
	if proj.TotalCostUSD != 0 {
		t.Errorf("cost = %f, want 0", proj.TotalCostUSD)
	}
}

func TestProjectCost_HotOnly(t *testing.T) {
	costs := DefaultStorageTierCosts()
	tierGBs := map[Tier]float64{TierHot: 1000}

	proj := ProjectCost(tierGBs, costs)

	expected := 1000 * 0.023
	if math.Abs(proj.TotalCostUSD-expected) > 0.01 {
		t.Errorf("cost = %f, want %f", proj.TotalCostUSD, expected)
	}
	if proj.SavingsVsFlat != 0 {
		t.Errorf("hot-only savings should be 0, got %f", proj.SavingsVsFlat)
	}
}

func TestProjectCost_GlacierSavings(t *testing.T) {
	costs := DefaultStorageTierCosts()
	tierGBs := map[Tier]float64{TierArchive: 10000}

	proj := ProjectCost(tierGBs, costs)

	glacierCost := 10000 * 0.004
	flatCost := 10000 * 0.023
	expectedSavings := (1 - glacierCost/flatCost) * 100

	if math.Abs(proj.SavingsVsFlat-expectedSavings) > 1 {
		t.Errorf("savings = %.1f%%, want ~%.1f%%", proj.SavingsVsFlat, expectedSavings)
	}
}

func TestDefaultStorageTierCosts(t *testing.T) {
	costs := DefaultStorageTierCosts()
	if len(costs) != 4 {
		t.Fatalf("want 4 tiers, got %d", len(costs))
	}
	for i, c := range costs {
		if c.CostPerGBMonth <= 0 {
			t.Errorf("tier %d (%s): cost should be positive", i, c.Tier)
		}
		if c.S3StorageClass == "" {
			t.Errorf("tier %d: empty S3 class", i)
		}
	}

	if costs[0].CostPerGBMonth <= costs[3].CostPerGBMonth {
		t.Error("hot should cost more than archive")
	}
}
