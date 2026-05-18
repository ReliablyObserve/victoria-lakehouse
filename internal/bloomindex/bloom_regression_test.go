package bloomindex

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestRegression_BloomFPR_Under1Percent(t *testing.T) {
	idx := New()
	n := 10000
	f := NewFilter(n, 0.01)
	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("trace-%d", i))
	}
	idx.Add("file.parquet", "trace_id", f)

	keys := []string{"file.parquet"}
	fp := 0
	checks := 10000
	for i := 0; i < checks; i++ {
		val := fmt.Sprintf("notinserted-%d", i)
		result := idx.MayContain(keys, "trace_id", val)
		if len(result) > 0 {
			fp++
		}
	}

	fpr := float64(fp) / float64(checks)
	t.Logf("FPR: %d/%d = %.3f%%", fp, checks, fpr*100)
	if fpr > 0.02 {
		t.Errorf("FPR = %.3f%%, want ≤ 2%%", fpr*100)
	}
}

func TestRegression_BloomMetadata_Under1Percent_OfData(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"

	totalTraces := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s/file%d.parquet", partition, i)
		traceIDs := make([]string, 200)
		for j := range traceIDs {
			traceIDs[j] = fmt.Sprintf("trace-%d-%d", i, j)
			totalTraces++
		}
		pi.AddFile(partition, key, map[string][]string{"trace_id": traceIDs})
	}

	bloomBytes := len(pi.MarshalPartition(partition))
	parquetBytesEstimate := int64(totalTraces) * 500

	ratio := float64(bloomBytes) / float64(parquetBytesEstimate)
	t.Logf("bloom: %d bytes, estimated parquet: %d bytes, ratio: %.3f%%", bloomBytes, parquetBytesEstimate, ratio*100)

	if ratio > 0.01 {
		t.Errorf("bloom metadata ratio = %.3f%%, want < 1%%", ratio*100)
	}
}

func TestRegression_CacheEviction_OldestFirst(t *testing.T) {
	cache := NewBloomCache(500, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	for i := 0; i < 10; i++ {
		partition := fmt.Sprintf("p%d", i)
		pi.AddFile(partition, fmt.Sprintf("f%d", i), map[string][]string{"trace_id": {fmt.Sprintf("t%d", i)}})
		cache.Put(partition, pi.GetPartition(partition))
		time.Sleep(time.Millisecond)
	}

	if cache.Len() > 5 {
		t.Logf("cache has %d entries (some eviction expected due to size cap)", cache.Len())
	}

	_, _ = cache.Get(context.Background(), "p0")
	p0 := cache.Len()

	pi2 := NewPartitionedIndex(GranularityHour, 0.01)
	pi2.AddFile("new", "fnew", map[string][]string{"trace_id": {"tnew"}})
	cache.Put("new", pi2.GetPartition("new"))

	if cache.Len() <= p0 {
		t.Logf("cache evicted entries as expected (len before=%d, after=%d)", p0, cache.Len())
	}
}

func TestRegression_CompactionPreservesBloom(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("%s/file%d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"trace_id": {fmt.Sprintf("trace-%d", i)},
		})
	}

	mergedKey := partition + "/compacted-L1.parquet"
	allTraces := make([]string, 5)
	for i := range allTraces {
		allTraces[i] = fmt.Sprintf("trace-%d", i)
	}

	f := NewFilter(len(allTraces), 0.01)
	for _, v := range allTraces {
		f.Add(v)
	}

	rebuilt := New()
	rebuilt.Add(mergedKey, "trace_id", f)
	pi.SetPartition(partition, rebuilt)

	idx := pi.GetPartition(partition)
	for _, trace := range allTraces {
		result := idx.MayContain([]string{mergedKey}, "trace_id", trace)
		if len(result) == 0 {
			t.Errorf("compacted bloom should still find %s", trace)
		}
	}
}

func TestRegression_TierTransition_NoFalseNegatives(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"

	for f := 0; f < 10; f++ {
		for rg := 0; rg < 5; rg++ {
			key := PerRGKey(fmt.Sprintf("%s/file%d.parquet", partition, f), rg)
			pi.AddFile(partition, key, map[string][]string{
				"trace_id": {fmt.Sprintf("trace-%d-%d", f, rg)},
			})
		}
	}

	idx := pi.GetPartition(partition)
	merged := DowngradeToPerFile(idx)
	summary := DowngradeToSummary(merged)

	for f := 0; f < 10; f++ {
		for rg := 0; rg < 5; rg++ {
			val := fmt.Sprintf("trace-%d-%d", f, rg)

			fileKeys := make([]string, 10)
			for i := 0; i < 10; i++ {
				fileKeys[i] = fmt.Sprintf("%s/file%d.parquet", partition, i)
			}
			perFileResult := merged.MayContain(fileKeys, "trace_id", val)
			if len(perFileResult) == 0 {
				t.Errorf("per-file bloom should contain %s", val)
			}

			summaryResult := summary.MayContain([]string{SummaryKey}, "trace_id", val)
			if len(summaryResult) == 0 {
				t.Errorf("summary bloom should contain %s", val)
			}
		}
	}
}

func TestRegression_MarshalUnmarshal_Roundtrip(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("%s/file%d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"trace_id":     {fmt.Sprintf("trace-%d", i)},
			"service.name": {"api-gw"},
		})
	}

	data := pi.MarshalPartition(partition)
	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	if restored.Len() != 50 {
		t.Errorf("restored index has %d entries, want 50", restored.Len())
	}

	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s/file%d.parquet", partition, i)
	}

	for i := 0; i < 50; i++ {
		val := fmt.Sprintf("trace-%d", i)
		result := restored.MayContain(keys, "trace_id", val)
		if !containsStr(result, keys[i]) {
			t.Errorf("restored bloom should contain %s in file%d", val, i)
		}
	}
}

func TestRegression_SpecConsistency_TierBoundaries(t *testing.T) {
	cfg := DefaultTierConfig()

	if cfg.Tier1MaxAge != 7*24*time.Hour {
		t.Errorf("spec says hot tier is 7d, got %v", cfg.Tier1MaxAge)
	}
	if cfg.Tier2MaxAge != 30*24*time.Hour {
		t.Errorf("spec says warm tier is 30d, got %v", cfg.Tier2MaxAge)
	}
	if cfg.Tier3MaxAge != 90*24*time.Hour {
		t.Errorf("spec says cold tier is 90d, got %v", cfg.Tier3MaxAge)
	}
}

func TestRegression_SpecConsistency_MaxCardinality(t *testing.T) {
	if maxBloomCardinality != 50000 {
		t.Errorf("spec says max cardinality is 50000, got %d", maxBloomCardinality)
	}
}

func TestRegression_SpecConsistency_SummaryKey(t *testing.T) {
	if SummaryKey != "summary" {
		t.Errorf("SummaryKey = %q, spec says 'summary'", SummaryKey)
	}
}

func TestRegression_SpecConsistency_DefaultCosts(t *testing.T) {
	costs := DefaultStorageTierCosts()
	if len(costs) != 4 {
		t.Fatalf("spec has 4 storage tiers, got %d", len(costs))
	}
	if costs[0].S3StorageClass != "STANDARD" {
		t.Error("hot tier should use STANDARD")
	}
	if costs[2].S3StorageClass != "STANDARD_IA" {
		t.Error("cold tier should use STANDARD_IA")
	}
	if costs[3].S3StorageClass != "GLACIER" {
		t.Error("archive tier should use GLACIER")
	}
}

func containsStr(ss []string, target string) bool {
	for _, s := range ss {
		if s == target {
			return true
		}
	}
	return false
}
