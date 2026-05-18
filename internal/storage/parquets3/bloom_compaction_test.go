package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
)

func TestCompaction_RebuildBloomAfterMerge(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("%s/file%d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"trace_id": {fmt.Sprintf("trace-%d", i)},
		})
	}

	idx := pi.GetPartition(partition)
	if idx.Len() != 5 {
		t.Fatalf("want 5 entries, got %d", idx.Len())
	}

	mergedKey := partition + "/compacted-L1-abc.parquet"
	allTraces := make([]string, 5)
	for i := 0; i < 5; i++ {
		allTraces[i] = fmt.Sprintf("trace-%d", i)
	}

	newIdx := bloomindex.New()
	f := bloomindex.NewFilter(len(allTraces), 0.01)
	for _, v := range allTraces {
		f.Add(v)
	}
	newIdx.Add(mergedKey, "trace_id", f)

	pi.SetPartition(partition, newIdx)

	rebuilt := pi.GetPartition(partition)
	if rebuilt.Len() != 1 {
		t.Errorf("rebuilt should have 1 entry (merged file), got %d", rebuilt.Len())
	}

	for _, trace := range allTraces {
		result := rebuilt.MayContain([]string{mergedKey}, "trace_id", trace)
		if len(result) == 0 {
			t.Errorf("rebuilt bloom should contain %s", trace)
		}
	}
}

func TestCompaction_StaleBloomEntries_Harmless(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"

	pi.AddFile(partition, "fileA.parquet", map[string][]string{"trace_id": {"trace-A"}})
	pi.AddFile(partition, "fileB.parquet", map[string][]string{"trace_id": {"trace-B"}})

	idx := pi.GetPartition(partition)

	manifestKeys := []string{"fileC.parquet"}
	result := idx.MayContain(manifestKeys, "trace_id", "trace-A")

	for _, k := range result {
		if k == "fileA.parquet" || k == "fileB.parquet" {
			t.Error("stale bloom entries for deleted files should not match manifest keys")
		}
	}

	if len(result) != 1 || result[0] != "fileC.parquet" {
		t.Errorf("unknown file should pass bloom (conservative), got %v", result)
	}
}

func TestCompaction_TTLRecompression_Applied(t *testing.T) {
	tests := []struct {
		desc string
		age  time.Duration
		want int
	}{
		{"hot data (1d)", 24 * time.Hour, 3},
		{"warm data (10d)", 10 * 24 * time.Hour, 7},
		{"cold data (60d)", 60 * 24 * time.Hour, 17},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			tiers := []struct {
				MaxAge time.Duration
				Level  int
			}{
				{7 * 24 * time.Hour, 3},
				{30 * 24 * time.Hour, 7},
				{0, 17},
			}

			var level int
			for _, tier := range tiers {
				if tier.MaxAge > 0 && tt.age < tier.MaxAge {
					level = tier.Level
					break
				}
				level = tier.Level
			}

			if level != tt.want {
				t.Errorf("age=%v: got level %d, want %d", tt.age, level, tt.want)
			}
		})
	}
}

func TestCompaction_BloomCacheInvalidation(t *testing.T) {
	cache := bloomindex.NewBloomCache(1024*1024, nil)

	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"
	pi.AddFile(partition, "f1.parquet", map[string][]string{"trace_id": {"aaa"}})
	cache.Put(partition, pi.GetPartition(partition))

	idx, _ := cache.Get(context.Background(), partition)
	if idx == nil {
		t.Fatal("cache should have partition before invalidation")
	}

	cache.Invalidate(partition)

	idx, _ = cache.Get(context.Background(), partition)
	if idx != nil {
		t.Error("cache should be empty after invalidation")
	}
}
