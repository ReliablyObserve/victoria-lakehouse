package bloomindex

import (
	"strconv"
	"testing"
	"time"
)

func TestTierForAge_Hot(t *testing.T) {
	cfg := DefaultTierConfig()
	for _, age := range []time.Duration{0, time.Hour, 6 * 24 * time.Hour} {
		tier := TierForAge(age, cfg)
		if tier != TierHot {
			t.Errorf("age %v: want TierHot, got %v", age, tier)
		}
	}
}

func TestTierForAge_Warm(t *testing.T) {
	cfg := DefaultTierConfig()
	for _, age := range []time.Duration{
		7 * 24 * time.Hour,
		14 * 24 * time.Hour,
		29 * 24 * time.Hour,
	} {
		tier := TierForAge(age, cfg)
		if tier != TierWarm {
			t.Errorf("age %v: want TierWarm, got %v", age, tier)
		}
	}
}

func TestTierForAge_Cold(t *testing.T) {
	cfg := DefaultTierConfig()
	for _, age := range []time.Duration{
		30 * 24 * time.Hour,
		60 * 24 * time.Hour,
		89 * 24 * time.Hour,
	} {
		tier := TierForAge(age, cfg)
		if tier != TierCold {
			t.Errorf("age %v: want TierCold, got %v", age, tier)
		}
	}
}

func TestTierForAge_Archive(t *testing.T) {
	cfg := DefaultTierConfig()
	for _, age := range []time.Duration{
		90 * 24 * time.Hour,
		365 * 24 * time.Hour,
	} {
		tier := TierForAge(age, cfg)
		if tier != TierArchive {
			t.Errorf("age %v: want TierArchive, got %v", age, tier)
		}
	}
}

func TestTierForAge_CustomBoundaries(t *testing.T) {
	cfg := TierConfig{
		Tier1MaxAge: 14 * 24 * time.Hour,
		Tier2MaxAge: 60 * 24 * time.Hour,
		Tier3MaxAge: 365 * 24 * time.Hour,
	}

	tests := []struct {
		age  time.Duration
		want Tier
	}{
		{13 * 24 * time.Hour, TierHot},
		{14 * 24 * time.Hour, TierWarm},
		{59 * 24 * time.Hour, TierWarm},
		{60 * 24 * time.Hour, TierCold},
		{364 * 24 * time.Hour, TierCold},
		{365 * 24 * time.Hour, TierArchive},
	}

	for _, tt := range tests {
		got := TierForAge(tt.age, cfg)
		if got != tt.want {
			t.Errorf("custom config, age %v: want %v, got %v", tt.age, tt.want, got)
		}
	}
}

func TestTierForAge_ExactBoundary(t *testing.T) {
	cfg := DefaultTierConfig()
	// At exactly the boundary, the data should transition to the next tier
	if tier := TierForAge(7*24*time.Hour, cfg); tier != TierWarm {
		t.Errorf("exactly 7d: want TierWarm, got %v", tier)
	}
	if tier := TierForAge(30*24*time.Hour, cfg); tier != TierCold {
		t.Errorf("exactly 30d: want TierCold, got %v", tier)
	}
	if tier := TierForAge(90*24*time.Hour, cfg); tier != TierArchive {
		t.Errorf("exactly 90d: want TierArchive, got %v", tier)
	}
}

func TestDowngradeToPerFile(t *testing.T) {
	idx := New()
	numRGs := 10
	numFiles := 360

	for f := 0; f < numFiles; f++ {
		for rg := 0; rg < numRGs; rg++ {
			key := PerRGKey("file"+itoa(f), rg)
			filter := NewFilter(200, 0.01)
			for j := 0; j < 200; j++ {
				filter.Add("trace-" + itoa(f) + "-" + itoa(rg) + "-" + itoa(j))
			}
			idx.Add(key, "trace_id", filter)
		}
	}

	originalEntries := idx.Len()
	if originalEntries != numFiles*numRGs {
		t.Fatalf("want %d entries, got %d", numFiles*numRGs, originalEntries)
	}

	merged := DowngradeToPerFile(idx)
	if merged.Len() != numFiles {
		t.Errorf("after downgrade: want %d per-file entries, got %d", numFiles, merged.Len())
	}

	// Verify all inserted values are still found (no false negatives)
	for f := 0; f < numFiles; f++ {
		fileKey := "file" + itoa(f)
		for rg := 0; rg < numRGs; rg++ {
			for j := 0; j < 200; j++ {
				val := "trace-" + itoa(f) + "-" + itoa(rg) + "-" + itoa(j)
				result := merged.MayContain([]string{fileKey}, "trace_id", val)
				if len(result) == 0 {
					t.Fatalf("downgraded filter missing value %s in %s", val, fileKey)
				}
			}
		}
	}
}

func TestDowngradeToSummary(t *testing.T) {
	idx := New()
	numFiles := 360

	for f := 0; f < numFiles; f++ {
		filter := NewFilter(200, 0.01)
		for j := 0; j < 200; j++ {
			filter.Add("trace-" + itoa(f) + "-" + itoa(j))
		}
		idx.Add("file"+itoa(f), "trace_id", filter)
	}

	summary := DowngradeToSummary(idx)
	if summary.Len() != 1 {
		t.Errorf("summary should have 1 entry, got %d", summary.Len())
	}

	// Summary key must be "summary"
	if !summary.Has(SummaryKey) {
		t.Error("summary entry must use SummaryKey")
	}

	// Verify no false negatives
	for f := 0; f < numFiles; f++ {
		for j := 0; j < 200; j++ {
			val := "trace-" + itoa(f) + "-" + itoa(j)
			result := summary.MayContain([]string{SummaryKey}, "trace_id", val)
			if len(result) == 0 {
				t.Fatalf("summary missing value %s", val)
			}
		}
	}

	// Summary size should be bounded (~9KB for 360 files × 200 items)
	data := summary.Marshal()
	if len(data) > 20*1024 {
		t.Errorf("summary too large: %d bytes (want ≤ 20KB)", len(data))
	}
}

func TestDowngradeIsOneWay(t *testing.T) {
	// Verify tier ordering: Hot < Warm < Cold < Archive
	if TierHot >= TierWarm {
		t.Error("TierHot must be < TierWarm")
	}
	if TierWarm >= TierCold {
		t.Error("TierWarm must be < TierCold")
	}
	if TierCold >= TierArchive {
		t.Error("TierCold must be < TierArchive")
	}

	// Verify TierForAge is monotone: as age increases, tier never decreases
	cfg := DefaultTierConfig()
	prevTier := TierForAge(0, cfg)
	for d := 1; d <= 120; d++ {
		age := time.Duration(d) * 24 * time.Hour
		tier := TierForAge(age, cfg)
		if tier < prevTier {
			t.Errorf("tier decreased from %v to %v at age %v", prevTier, tier, age)
		}
		prevTier = tier
	}
}

func TestPerRGKeyFormat(t *testing.T) {
	key := PerRGKey("partition/file1.parquet", 5)
	want := "partition/file1.parquet#5"
	if key != want {
		t.Errorf("PerRGKey: got %q, want %q", key, want)
	}

	fileKey, rgIdx, ok := ParseRGKey(key)
	if !ok {
		t.Fatal("ParseRGKey failed")
	}
	if fileKey != "partition/file1.parquet" {
		t.Errorf("fileKey: got %q, want %q", fileKey, "partition/file1.parquet")
	}
	if rgIdx != 5 {
		t.Errorf("rgIdx: got %d, want 5", rgIdx)
	}

	// Per-file key has no #
	_, _, ok = ParseRGKey("partition/file1.parquet")
	if ok {
		t.Error("per-file key should not parse as RG key")
	}
}

func TestBloomSizeByTier(t *testing.T) {
	// Tier 1: 3600 per-RG entries (360 files × 10 RGs, 200 items each)
	tier1 := New()
	for f := 0; f < 360; f++ {
		for rg := 0; rg < 10; rg++ {
			filter := NewFilter(200, 0.01)
			for j := 0; j < 200; j++ {
				filter.Add("trace-" + itoa(f) + "-" + itoa(rg) + "-" + itoa(j))
			}
			tier1.Add(PerRGKey("file"+itoa(f), rg), "trace_id", filter)
		}
	}
	tier1Size := len(tier1.Marshal())

	// Tier 2: 360 per-file entries (merged from per-RG)
	tier2 := DowngradeToPerFile(tier1)
	tier2Size := len(tier2.Marshal())

	// Tier 3: 1 summary entry
	tier3 := DowngradeToSummary(tier2)
	tier3Size := len(tier3.Marshal())

	t.Logf("Tier 1 (per-RG, 3600 entries): %d bytes (%.1f KB)", tier1Size, float64(tier1Size)/1024)
	t.Logf("Tier 2 (per-file, 360 entries): %d bytes (%.1f KB)", tier2Size, float64(tier2Size)/1024)
	t.Logf("Tier 3 (summary, 1 entry): %d bytes (%.1f KB)", tier3Size, float64(tier3Size)/1024)

	// Size ordering: tier1 > tier2 > tier3
	if tier2Size >= tier1Size {
		t.Errorf("tier2 (%d) should be smaller than tier1 (%d)", tier2Size, tier1Size)
	}
	if tier3Size >= tier2Size {
		t.Errorf("tier3 (%d) should be smaller than tier2 (%d)", tier3Size, tier2Size)
	}

	// Per spec: tier2 ≈ 10x smaller than tier1
	ratio := float64(tier1Size) / float64(tier2Size)
	if ratio < 2 {
		t.Errorf("tier1/tier2 ratio: %.1fx (want ≥ 2x)", ratio)
	}
}

func TestTierConfig_Validation(t *testing.T) {
	// Invalid: tier1 >= tier2
	cfg := TierConfig{
		Tier1MaxAge: 30 * 24 * time.Hour,
		Tier2MaxAge: 7 * 24 * time.Hour,
		Tier3MaxAge: 90 * 24 * time.Hour,
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for tier1 >= tier2")
	}

	// Invalid: tier2 >= tier3
	cfg = TierConfig{
		Tier1MaxAge: 7 * 24 * time.Hour,
		Tier2MaxAge: 90 * 24 * time.Hour,
		Tier3MaxAge: 30 * 24 * time.Hour,
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for tier2 >= tier3")
	}

	// Valid
	cfg = DefaultTierConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should be valid: %v", err)
	}
}

func TestTier_String(t *testing.T) {
	tests := []struct {
		tier Tier
		want string
	}{
		{TierHot, "hot"},
		{TierWarm, "warm"},
		{TierCold, "cold"},
		{TierArchive, "archive"},
	}
	for _, tt := range tests {
		if got := tt.tier.String(); got != tt.want {
			t.Errorf("Tier(%d).String() = %q, want %q", tt.tier, got, tt.want)
		}
	}
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

// TestTier_String_UnknownValue exercises the default branch in Tier.String() (previously 83.3%).
func TestTier_String_UnknownValue(t *testing.T) {
	unknownTier := Tier(99)
	got := unknownTier.String()
	if got == "" {
		t.Error("expected non-empty string for unknown tier")
	}
	// Should contain "tier" or the number.
	if got != "tier(99)" {
		t.Errorf("unexpected string for unknown tier: %q", got)
	}
}

// TestTierConfig_ApplyOverride exercises TierConfig.ApplyOverride (previously 75%).
func TestTierConfig_ApplyOverride(t *testing.T) {
	cfg := DefaultTierConfig()

	t.Run("no overrides leaves config unchanged", func(t *testing.T) {
		result := cfg.ApplyOverride(TierConfigOverride{})
		if result.Tier1MaxAge != cfg.Tier1MaxAge {
			t.Errorf("Tier1MaxAge changed without override")
		}
	})

	t.Run("override Tier1MaxAge only", func(t *testing.T) {
		d := 3 * 24 * time.Hour
		result := cfg.ApplyOverride(TierConfigOverride{Tier1MaxAge: &d})
		if result.Tier1MaxAge != d {
			t.Errorf("Tier1MaxAge = %v, want %v", result.Tier1MaxAge, d)
		}
		if result.Tier2MaxAge != cfg.Tier2MaxAge {
			t.Errorf("Tier2MaxAge changed unexpectedly")
		}
	})

	t.Run("override all tiers", func(t *testing.T) {
		d1 := 3 * 24 * time.Hour
		d2 := 15 * 24 * time.Hour
		d3 := 45 * 24 * time.Hour
		result := cfg.ApplyOverride(TierConfigOverride{
			Tier1MaxAge: &d1,
			Tier2MaxAge: &d2,
			Tier3MaxAge: &d3,
		})
		if result.Tier1MaxAge != d1 {
			t.Errorf("Tier1MaxAge = %v, want %v", result.Tier1MaxAge, d1)
		}
		if result.Tier2MaxAge != d2 {
			t.Errorf("Tier2MaxAge = %v, want %v", result.Tier2MaxAge, d2)
		}
		if result.Tier3MaxAge != d3 {
			t.Errorf("Tier3MaxAge = %v, want %v", result.Tier3MaxAge, d3)
		}
	})
}
