package delete

import (
	"testing"
)

func TestStorageClass_ParseStorageClass(t *testing.T) {
	tests := []struct {
		input string
		want  StorageClass
	}{
		{"", ClassStandard},
		{"STANDARD", ClassStandard},
		{"standard", ClassStandard},
		{"STANDARD_IA", ClassStandardIA},
		{"GLACIER", ClassGlacier},
		{"DEEP_ARCHIVE", ClassDeepArchive},
		{"INTELLIGENT_TIERING", ClassIntelligentTiering},
		{"ONEZONE_IA", ClassOnezoneIA},
		{"GLACIER_IR", ClassGlacierIR},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseStorageClass(tt.input)
			if got != tt.want {
				t.Errorf("ParseStorageClass(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestStorageClass_CanRewrite(t *testing.T) {
	tests := []struct {
		class StorageClass
		want  bool
	}{
		{ClassStandard, true},
		{ClassIntelligentTiering, true},
		{ClassStandardIA, false},
		{ClassOnezoneIA, false},
		{ClassGlacierIR, false},
		{ClassGlacier, false},
		{ClassDeepArchive, false},
	}
	for _, tt := range tests {
		t.Run(string(tt.class), func(t *testing.T) {
			if got := tt.class.CanRewrite(); got != tt.want {
				t.Errorf("%s.CanRewrite() = %v, want %v", tt.class, got, tt.want)
			}
		})
	}
}

func TestStorageClass_IsArchive(t *testing.T) {
	tests := []struct {
		class StorageClass
		want  bool
	}{
		{ClassStandard, false},
		{ClassIntelligentTiering, false},
		{ClassStandardIA, false},
		{ClassOnezoneIA, false},
		{ClassGlacierIR, true},
		{ClassGlacier, true},
		{ClassDeepArchive, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.class), func(t *testing.T) {
			if got := tt.class.IsArchive(); got != tt.want {
				t.Errorf("%s.IsArchive() = %v, want %v", tt.class, got, tt.want)
			}
		})
	}
}

func TestStorageClass_EstimateRewriteCost(t *testing.T) {
	const oneGB = 1024 * 1024 * 1024

	t.Run("Standard_zero_retrieval", func(t *testing.T) {
		cost := EstimateRewriteCost(ClassStandard, oneGB)
		if cost.RetrievalCostUSD != 0 {
			t.Errorf("expected 0 retrieval cost for STANDARD, got %f", cost.RetrievalCostUSD)
		}
		if cost.TotalCostUSD <= 0 {
			t.Errorf("expected positive total cost (GET+PUT), got %f", cost.TotalCostUSD)
		}
	})

	t.Run("IntelligentTiering_zero_retrieval", func(t *testing.T) {
		cost := EstimateRewriteCost(ClassIntelligentTiering, oneGB)
		if cost.RetrievalCostUSD != 0 {
			t.Errorf("expected 0 retrieval cost for INTELLIGENT_TIERING, got %f", cost.RetrievalCostUSD)
		}
	})

	t.Run("Glacier_nonzero_retrieval", func(t *testing.T) {
		cost := EstimateRewriteCost(ClassGlacier, oneGB)
		if cost.RetrievalCostUSD <= 0 {
			t.Errorf("expected positive retrieval cost for GLACIER, got %f", cost.RetrievalCostUSD)
		}
		if cost.RetrievalCostUSD < 0.02 || cost.RetrievalCostUSD > 0.04 {
			t.Errorf("expected ~$0.03/GB for GLACIER, got %f", cost.RetrievalCostUSD)
		}
		if cost.TotalCostUSD <= cost.RetrievalCostUSD {
			t.Errorf("total should exceed retrieval (includes GET+PUT)")
		}
	})

	t.Run("DeepArchive_highest_retrieval", func(t *testing.T) {
		cost := EstimateRewriteCost(ClassDeepArchive, oneGB)
		glacierCost := EstimateRewriteCost(ClassGlacier, oneGB)
		if cost.RetrievalCostUSD <= glacierCost.RetrievalCostUSD {
			t.Errorf("DEEP_ARCHIVE retrieval (%f) should exceed GLACIER (%f)",
				cost.RetrievalCostUSD, glacierCost.RetrievalCostUSD)
		}
	})

	t.Run("StandardIA_moderate_retrieval", func(t *testing.T) {
		cost := EstimateRewriteCost(ClassStandardIA, oneGB)
		if cost.RetrievalCostUSD <= 0 {
			t.Errorf("expected positive retrieval cost for STANDARD_IA, got %f", cost.RetrievalCostUSD)
		}
		if cost.RetrievalCostUSD < 0.009 || cost.RetrievalCostUSD > 0.011 {
			t.Errorf("expected ~$0.01/GB for STANDARD_IA, got %f", cost.RetrievalCostUSD)
		}
	})
}

func TestPredictClassFromAge(t *testing.T) {
	rules := []LifecycleRule{
		{TransitionDays: 30, Class: ClassStandardIA},
		{TransitionDays: 90, Class: ClassGlacier},
		{TransitionDays: 365, Class: ClassDeepArchive},
	}

	tests := []struct {
		name         string
		fileAgeHours float64
		want         StorageClass
	}{
		{"before_first_threshold", 24 * 10, ClassStandard},         // 10 days
		{"at_first_threshold", 24 * 30, ClassStandardIA},           // 30 days
		{"between_first_and_second", 24 * 60, ClassStandardIA},     // 60 days
		{"at_second_threshold", 24 * 90, ClassGlacier},             // 90 days
		{"between_second_and_third", 24 * 200, ClassGlacier},       // 200 days
		{"at_third_threshold", 24 * 365, ClassDeepArchive},         // 365 days
		{"well_past_all_thresholds", 24 * 1000, ClassDeepArchive},  // 1000 days
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PredictClassFromAge(rules, tt.fileAgeHours)
			if got != tt.want {
				t.Errorf("PredictClassFromAge(age=%.0fh) = %q, want %q",
					tt.fileAgeHours, got, tt.want)
			}
		})
	}

	t.Run("empty_rules", func(t *testing.T) {
		got := PredictClassFromAge(nil, 24*1000)
		if got != ClassStandard {
			t.Errorf("expected STANDARD with no rules, got %q", got)
		}
	})
}

func TestDetector(t *testing.T) {
	rules := []LifecycleRule{
		{TransitionDays: 30, Class: ClassStandardIA},
		{TransitionDays: 90, Class: ClassGlacier},
	}
	det := NewStorageClassDetector(rules)

	t.Run("detect_young_file", func(t *testing.T) {
		got := det.Detect(24 * 10) // 10 days
		if got != ClassStandard {
			t.Errorf("expected STANDARD for 10-day file, got %q", got)
		}
	})

	t.Run("detect_old_file", func(t *testing.T) {
		got := det.Detect(24 * 100) // 100 days
		if got != ClassGlacier {
			t.Errorf("expected GLACIER for 100-day file, got %q", got)
		}
	})

	t.Run("cache_set_get", func(t *testing.T) {
		det.SetCache("bucket/key1.parquet", ClassDeepArchive)
		sc, ok := det.GetCached("bucket/key1.parquet")
		if !ok {
			t.Fatal("expected cache hit")
		}
		if sc != ClassDeepArchive {
			t.Errorf("expected DEEP_ARCHIVE from cache, got %q", sc)
		}
	})

	t.Run("cache_miss", func(t *testing.T) {
		_, ok := det.GetCached("nonexistent/key")
		if ok {
			t.Error("expected cache miss for unknown key")
		}
	})
}
