package cache

import (
	"math/rand"
	"strconv"
	"testing"
)

// --- Regression: AddWithValueCounts merges correctly ---

func TestRegressionValueCountsMergeAcrossFiles(t *testing.T) {
	idx := NewLabelIndex()

	// First file sees api=50, web=30
	idx.AddWithValueCounts("service.name", []string{"api", "web"}, map[string]int{
		"api": 50, "web": 30,
	})

	// Second file sees api=20, worker=10
	idx.AddWithValueCounts("service.name", []string{"api", "worker"}, map[string]int{
		"api": 20, "worker": 10,
	})

	li := idx.GetLabelInfo("service.name")
	if li == nil { //nolint:staticcheck // t.Fatal terminates
		t.Fatal("label info should exist")
	}

	// api should have 50+20=70
	if li.ValueCounts["api"] != 70 { //nolint:staticcheck // guarded by t.Fatal above
		t.Errorf("api count = %d, want 70", li.ValueCounts["api"])
	}
	// web should stay 30
	if li.ValueCounts["web"] != 30 {
		t.Errorf("web count = %d, want 30", li.ValueCounts["web"])
	}
	// worker should be 10
	if li.ValueCounts["worker"] != 10 {
		t.Errorf("worker count = %d, want 10", li.ValueCounts["worker"])
	}

	if li.SeenInFiles != 2 {
		t.Errorf("SeenInFiles = %d, want 2", li.SeenInFiles)
	}
}

func TestRegressionAddWithValueCountsNilCounts(t *testing.T) {
	idx := NewLabelIndex()

	// nil counts should increment by 1 per unique value
	idx.AddWithValueCounts("field", []string{"a", "b", "a"}, nil)

	li := idx.GetLabelInfo("field")
	if li == nil { //nolint:staticcheck // t.Fatal terminates
		t.Fatal("label info should exist")
	}

	// "a" appears twice in values, but gets count=1 per add (backward compat)
	if li.ValueCounts["a"] != 2 { //nolint:staticcheck // guarded by t.Fatal above
		t.Errorf("a count = %d, want 2 (once per occurrence in values)", li.ValueCounts["a"])
	}
	if li.ValueCounts["b"] != 1 {
		t.Errorf("b count = %d, want 1", li.ValueCounts["b"])
	}
}

func TestRegressionAddInitializesValueCounts(t *testing.T) {
	idx := NewLabelIndex()

	// plain Add (no counts) should still initialize ValueCounts
	idx.Add("field", []string{"x", "y"})

	li := idx.GetLabelInfo("field")
	if li == nil { //nolint:staticcheck // t.Fatal terminates
		t.Fatal("label info should exist")
	}

	if li.ValueCounts == nil { //nolint:staticcheck // guarded by t.Fatal above
		t.Error("REGRESSION: ValueCounts should be initialized even by plain Add")
	}

	// Then AddWithValueCounts should merge cleanly
	idx.AddWithValueCounts("field", []string{"x", "z"}, map[string]int{"x": 5, "z": 3})

	li = idx.GetLabelInfo("field")
	if li.ValueCounts["x"] < 5 {
		t.Errorf("x count should be >= 5 after merge, got %d", li.ValueCounts["x"])
	}
	if li.ValueCounts["z"] != 3 {
		t.Errorf("z count = %d, want 3", li.ValueCounts["z"])
	}
}

// --- Regression: AddWithTenant per-tenant cardinality ---

func TestRegressionPerTenantCardinality(t *testing.T) {
	idx := NewLabelIndex()

	idx.AddWithTenant("field", []string{"a", "b", "c"}, "tenant1")
	idx.AddWithTenant("field", []string{"x", "y"}, "tenant2")

	li := idx.GetLabelInfo("field")
	if li == nil { //nolint:staticcheck // t.Fatal terminates
		t.Fatal("label info should exist")
	}

	if li.PerTenant == nil { //nolint:staticcheck // guarded by t.Fatal above
		t.Fatal("PerTenant should be initialized")
	}

	if li.PerTenant["tenant1"] != 3 { //nolint:staticcheck // guarded by t.Fatal above
		t.Errorf("tenant1 cardinality = %d, want 3", li.PerTenant["tenant1"])
	}
	if li.PerTenant["tenant2"] != 2 {
		t.Errorf("tenant2 cardinality = %d, want 2", li.PerTenant["tenant2"])
	}
}

func TestRegressionPerTenantCardinalityGrows(t *testing.T) {
	idx := NewLabelIndex()

	idx.AddWithTenant("field", []string{"a", "b"}, "t1")
	idx.AddWithTenant("field", []string{"a", "b", "c", "d"}, "t1")

	li := idx.GetLabelInfo("field")

	// Second call has 4 unique values, which is > 2 from first call
	if li.PerTenant["t1"] != 4 {
		t.Errorf("tenant cardinality should grow to max: got %d, want 4", li.PerTenant["t1"])
	}
}

func TestRegressionPerTenantCardinalityDoesNotShrink(t *testing.T) {
	idx := NewLabelIndex()

	idx.AddWithTenant("field", []string{"a", "b", "c"}, "t1")
	idx.AddWithTenant("field", []string{"a"}, "t1")

	li := idx.GetLabelInfo("field")

	// Second call has only 1 unique value but we should keep the max (3)
	if li.PerTenant["t1"] != 3 {
		t.Errorf("tenant cardinality should not shrink: got %d, want 3", li.PerTenant["t1"])
	}
}

func TestRegressionEmptyTenantIgnored(t *testing.T) {
	idx := NewLabelIndex()
	idx.AddWithTenant("field", []string{"a"}, "")

	li := idx.GetLabelInfo("field")
	if len(li.PerTenant) > 0 {
		t.Error("empty tenant string should not create a PerTenant entry")
	}
}

// --- Regression: Values cap at 10000 ---

func TestRegressionValuesCappedAt10000(t *testing.T) {
	idx := NewLabelIndex()

	vals := make([]string, 15000)
	for i := range vals {
		vals[i] = "v-" + strconv.Itoa(i)
	}

	idx.Add("big_field", vals)

	li := idx.GetLabelInfo("big_field")
	if len(li.Values) > 10000 {
		t.Errorf("values should be capped at 10000, got %d", len(li.Values))
	}
	if li.Cardinality > 10000 {
		t.Errorf("cardinality should be capped at 10000, got %d", li.Cardinality)
	}
}

func TestRegressionValuesCappedAcrossMultipleAdds(t *testing.T) {
	idx := NewLabelIndex()

	for batch := 0; batch < 20; batch++ {
		vals := make([]string, 1000)
		for i := range vals {
			vals[i] = "batch" + strconv.Itoa(batch) + "-v-" + strconv.Itoa(i)
		}
		idx.Add("big_field", vals)
	}

	li := idx.GetLabelInfo("big_field")
	if len(li.Values) > 10000 {
		t.Errorf("values should remain capped at 10000 across adds, got %d", len(li.Values))
	}
}

// --- Fuzzy: Random label operations ---

func TestFuzzyLabelIndexConcurrentAccess(t *testing.T) {
	idx := NewLabelIndex()
	done := make(chan struct{})

	// Writer goroutines
	for i := 0; i < 10; i++ {
		go func(id int) {
			rng := rand.New(rand.NewSource(int64(id)))
			for j := 0; j < 100; j++ {
				name := "field-" + strconv.Itoa(rng.Intn(20))
				nVals := rng.Intn(50) + 1
				vals := make([]string, nVals)
				counts := make(map[string]int)
				for k := 0; k < nVals; k++ {
					v := "val-" + strconv.Itoa(rng.Intn(100))
					vals[k] = v
					counts[v] = rng.Intn(100) + 1
				}
				switch rng.Intn(3) {
				case 0:
					idx.Add(name, vals)
				case 1:
					idx.AddWithValueCounts(name, vals, counts)
				case 2:
					idx.AddWithTenant(name, vals, "tenant-"+strconv.Itoa(rng.Intn(5)))
				}
			}
			done <- struct{}{}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < 5; i++ {
		go func() {
			rng := rand.New(rand.NewSource(int64(i + 100)))
			for j := 0; j < 200; j++ {
				name := "field-" + strconv.Itoa(rng.Intn(20))
				_ = idx.GetFieldNames()
				_ = idx.GetFieldValues(name, 100)
				_ = idx.GetLabelInfo(name)
				_ = idx.GetAllLabelInfo()
				_ = idx.Len()
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 15; i++ {
		<-done
	}
}

func TestFuzzyValueCountsAlwaysNonNegative(t *testing.T) {
	idx := NewLabelIndex()
	rng := rand.New(rand.NewSource(99))

	for i := 0; i < 100; i++ {
		nVals := rng.Intn(20) + 1
		vals := make([]string, nVals)
		counts := make(map[string]int)
		for j := 0; j < nVals; j++ {
			v := "v-" + strconv.Itoa(rng.Intn(50))
			vals[j] = v
			counts[v] = rng.Intn(1000)
		}
		idx.AddWithValueCounts("fuzz_field", vals, counts)
	}

	li := idx.GetLabelInfo("fuzz_field")
	if li == nil { //nolint:staticcheck // t.Fatal terminates
		t.Fatal("label should exist")
	}

	for v, c := range li.ValueCounts {
		if c < 0 {
			t.Errorf("REGRESSION: negative ValueCount for %q: %d", v, c)
		}
	}
}
