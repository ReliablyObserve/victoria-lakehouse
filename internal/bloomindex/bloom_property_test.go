package bloomindex

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestProperty_BloomNeverMissesInsertedValue(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for seed := 0; seed < 1000; seed++ {
		n := rng.Intn(500) + 1
		f := NewFilter(n, 0.01)
		values := make([]string, n)
		for i := range values {
			values[i] = fmt.Sprintf("val-%d-%d", seed, i)
			f.Add(values[i])
		}

		for _, v := range values {
			if !f.MayContain(v) {
				t.Fatalf("seed %d: bloom missed inserted value %q", seed, v)
			}
		}
	}
}

func TestProperty_BloomFPRWithinBounds(t *testing.T) {
	tests := []struct {
		n      int
		maxFPR float64
	}{
		{10, 0.05}, // small filters have higher FPR variance
		{100, 0.02},
		{1000, 0.02},
		{10000, 0.02},
		{50000, 0.02},
	}

	for _, tt := range tests {
		f := NewFilter(tt.n, 0.01)
		for i := 0; i < tt.n; i++ {
			f.Add(fmt.Sprintf("inserted-%d", i))
		}

		fp := 0
		checks := 100000
		if tt.n >= 10000 {
			checks = 50000
		}
		for i := 0; i < checks; i++ {
			if f.MayContain(fmt.Sprintf("notinserted-%d", i)) {
				fp++
			}
		}
		rate := float64(fp) / float64(checks)
		if rate > tt.maxFPR {
			t.Errorf("cardinality %d: FPR %.4f exceeds %.0f%% bound", tt.n, rate, tt.maxFPR*100)
		}
		t.Logf("cardinality %5d: FPR=%.4f, filter_size=%d bytes", tt.n, rate, f.Size())
	}
}

func TestProperty_MergePreservesAllPositives(t *testing.T) {
	fa := NewFilter(100, 0.01)
	fb := NewFilter(100, 0.01)

	aVals := make([]string, 100)
	bVals := make([]string, 100)
	for i := 0; i < 100; i++ {
		aVals[i] = fmt.Sprintf("a-%d", i)
		bVals[i] = fmt.Sprintf("b-%d", i)
		fa.Add(aVals[i])
		fb.Add(bVals[i])
	}

	fa.MergeFrom(fb)

	for _, v := range aVals {
		if !fa.MayContain(v) {
			t.Errorf("merged filter lost value from A: %s", v)
		}
	}
	for _, v := range bVals {
		if !fa.MayContain(v) {
			t.Errorf("merged filter lost value from B: %s", v)
		}
	}
}

func TestProperty_MergeNeverReducesFPR(t *testing.T) {
	n := 500
	fa := NewFilter(n, 0.01)
	fb := NewFilter(n, 0.01)
	for i := 0; i < n; i++ {
		fa.Add(fmt.Sprintf("a-%d", i))
		fb.Add(fmt.Sprintf("b-%d", i))
	}

	// Measure FPR before merge
	checkVals := make([]string, 10000)
	for i := range checkVals {
		checkVals[i] = fmt.Sprintf("check-%d", i)
	}

	fpBefore := 0
	for _, v := range checkVals {
		if fa.MayContain(v) {
			fpBefore++
		}
	}

	fa.MergeFrom(fb)

	fpAfter := 0
	for _, v := range checkVals {
		if fa.MayContain(v) {
			fpAfter++
		}
	}

	if fpAfter < fpBefore {
		t.Errorf("FP count decreased after merge: %d → %d (should never decrease)", fpBefore, fpAfter)
	}
}

func TestProperty_QueryWithBloomEqualsQueryWithout(t *testing.T) {
	idx := New()

	// Build index with known data
	files := []struct {
		key    string
		traces []string
	}{
		{"file1", []string{"trace-aaa", "trace-bbb"}},
		{"file2", []string{"trace-ccc", "trace-ddd"}},
		{"file3", []string{"trace-eee"}},
	}

	for _, f := range files {
		filter := NewFilter(len(f.traces), 0.01)
		for _, tr := range f.traces {
			filter.Add(tr)
		}
		idx.Add(f.key, "trace_id", filter)
	}

	keys := []string{"file1", "file2", "file3"}

	// For any query, bloom-filtered results must be a superset of actual results
	queries := []string{
		"trace-aaa", "trace-bbb", "trace-ccc", "trace-ddd", "trace-eee",
		"trace-xxx", "trace-yyy",
	}

	for _, q := range queries {
		bloomResult := idx.MayContain(keys, "trace_id", q)

		// Compute ground truth
		var truthResult []string
		for _, f := range files {
			for _, tr := range f.traces {
				if tr == q {
					truthResult = append(truthResult, f.key)
					break
				}
			}
		}

		// Every true result must appear in bloom result
		bloomSet := make(map[string]bool)
		for _, k := range bloomResult {
			bloomSet[k] = true
		}
		for _, k := range truthResult {
			if !bloomSet[k] {
				t.Errorf("query %q: bloom excluded true positive %q", q, k)
			}
		}
	}
}

func TestProperty_TierTransitionsAreMonotone(t *testing.T) {
	cfg := DefaultTierConfig()

	prevTier := TierForAge(0, cfg)
	for d := 0; d <= 365; d++ {
		age := time.Duration(d) * 24 * time.Hour
		tier := TierForAge(age, cfg)
		if tier < prevTier {
			t.Errorf("tier went backward at day %d: %v → %v", d, prevTier, tier)
		}
		prevTier = tier
	}
}

func TestProperty_LabelRollupIsSupersetOfHourly(t *testing.T) {
	hourlyLabels := []map[string][]string{
		{"service.name": {"api", "web"}, "namespace": {"prod"}},
		{"service.name": {"api", "worker"}, "namespace": {"prod", "staging"}},
		{"service.name": {"db"}, "namespace": {"prod"}},
	}

	daily := UnionLabels(hourlyLabels)

	for _, hourly := range hourlyLabels {
		for col, vals := range hourly {
			dailyVals, ok := daily[col]
			if !ok {
				t.Errorf("daily rollup missing column %q", col)
				continue
			}
			dailySet := make(map[string]bool)
			for _, v := range dailyVals {
				dailySet[v] = true
			}
			for _, v := range vals {
				if !dailySet[v] {
					t.Errorf("daily rollup missing value %q for column %q", v, col)
				}
			}
		}
	}
}

func TestProperty_ConfigMergePreservesUnoverriddenFields(t *testing.T) {
	base := TierConfig{
		Tier1MaxAge: 7 * 24 * time.Hour,
		Tier2MaxAge: 30 * 24 * time.Hour,
		Tier3MaxAge: 90 * 24 * time.Hour,
	}

	override := TierConfigOverride{
		Tier1MaxAge: durationPtr(14 * 24 * time.Hour),
	}

	merged := base.ApplyOverride(override)

	if merged.Tier1MaxAge != 14*24*time.Hour {
		t.Errorf("overridden field wrong: got %v, want 14d", merged.Tier1MaxAge)
	}
	if merged.Tier2MaxAge != base.Tier2MaxAge {
		t.Errorf("non-overridden Tier2MaxAge changed: got %v, want %v", merged.Tier2MaxAge, base.Tier2MaxAge)
	}
	if merged.Tier3MaxAge != base.Tier3MaxAge {
		t.Errorf("non-overridden Tier3MaxAge changed: got %v, want %v", merged.Tier3MaxAge, base.Tier3MaxAge)
	}
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}
