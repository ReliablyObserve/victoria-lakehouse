package bloomindex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMetadataCompactor_HotNoAction(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	pi.AddFile("p1", PerRGKey("f1.parquet", 0), map[string][]string{"trace_id": {"aaa"}})
	cache.Put("p1", pi.GetPartition("p1"))

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), nil)
	err := mc.CompactPartition(context.Background(), "p1", 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	idx, _ := cache.Get(context.Background(), "p1")
	if idx == nil {
		t.Fatal("hot partition should still exist")
	}
	if !hasPerRGEntries(idx) {
		t.Error("hot partition should still have per-RG entries")
	}
}

func TestMetadataCompactor_WarmDowngrade(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	for rg := 0; rg < 10; rg++ {
		key := PerRGKey("f1.parquet", rg)
		pi.AddFile("p1", key, map[string][]string{"trace_id": {fmt.Sprintf("trace-%d", rg)}})
	}
	cache.Put("p1", pi.GetPartition("p1"))

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), nil)
	err := mc.CompactPartition(context.Background(), "p1", 10*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	idx, _ := cache.Get(context.Background(), "p1")
	if idx == nil {
		t.Fatal("warm partition should still exist")
	}
	if hasPerRGEntries(idx) {
		t.Error("warm partition should not have per-RG entries after downgrade")
	}
	if idx.Len() != 1 {
		t.Errorf("downgraded index should have 1 per-file entry, got %d", idx.Len())
	}
}

func TestMetadataCompactor_ColdDowngrade(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("file%d.parquet", i)
		pi.AddFile("p1", key, map[string][]string{"trace_id": {fmt.Sprintf("trace-%d", i)}})
	}
	cache.Put("p1", pi.GetPartition("p1"))

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), nil)
	err := mc.CompactPartition(context.Background(), "p1", 45*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	idx, _ := cache.Get(context.Background(), "p1")
	if idx == nil {
		t.Fatal("cold partition should still exist (summary)")
	}
	if idx.Len() != 1 {
		t.Errorf("summary should have 1 entry, got %d", idx.Len())
	}
}

func TestMetadataCompactor_ArchiveRemoves(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	pi.AddFile("p1", "f1.parquet", map[string][]string{"trace_id": {"aaa"}})
	cache.Put("p1", pi.GetPartition("p1"))

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), nil)
	err := mc.CompactPartition(context.Background(), "p1", 100*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	idx, _ := cache.Get(context.Background(), "p1")
	if idx != nil {
		t.Error("archive partition should have been removed from cache")
	}
}

func TestMetadataCompactor_PersistOnDowngrade(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	for rg := 0; rg < 5; rg++ {
		key := PerRGKey("f1.parquet", rg)
		pi.AddFile("p1", key, map[string][]string{"trace_id": {fmt.Sprintf("trace-%d", rg)}})
	}
	cache.Put("p1", pi.GetPartition("p1"))

	var mu sync.Mutex
	persisted := map[string][]byte{}
	persistFn := func(ctx context.Context, partition string, data []byte) error {
		mu.Lock()
		persisted[partition] = data
		mu.Unlock()
		return nil
	}

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), persistFn)
	err := mc.CompactPartition(context.Background(), "p1", 10*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	data, ok := persisted["p1"]
	mu.Unlock()
	if !ok {
		t.Fatal("partition should have been persisted after downgrade")
	}
	if len(data) == 0 {
		t.Error("persisted data should not be empty")
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Len() != 1 {
		t.Errorf("restored index should have 1 entry, got %d", restored.Len())
	}
}

func TestMetadataCompactor_NonExistentPartition(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)
	mc := NewMetadataCompactor(cache, DefaultTierConfig(), nil)

	err := mc.CompactPartition(context.Background(), "missing", 10*24*time.Hour)
	if err != nil {
		t.Errorf("non-existent partition should not error: %v", err)
	}
}

func TestMetadataCompactor_AlreadyPerFile_NoDoubleDowngrade(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	pi.AddFile("p1", "f1.parquet", map[string][]string{"trace_id": {"aaa"}})
	pi.AddFile("p1", "f2.parquet", map[string][]string{"trace_id": {"bbb"}})
	cache.Put("p1", pi.GetPartition("p1"))

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), nil)
	err := mc.CompactPartition(context.Background(), "p1", 10*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	idx, _ := cache.Get(context.Background(), "p1")
	if idx == nil {
		t.Fatal("partition should still exist")
	}
	if idx.Len() != 2 {
		t.Errorf("per-file entries should be unchanged, got %d", idx.Len())
	}
}

// TestMetadataCompactor_ColdDowngrade_SingleEntry exercises downgradeToSummary
// early-return when idx.Len() <= 1 (previously 50%).
func TestMetadataCompactor_ColdDowngrade_SingleEntry(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	// Only 1 file entry — downgradeToSummary should return early.
	pi.AddFile("p1", "single_file.parquet", map[string][]string{"trace_id": {"aaa"}})
	cache.Put("p1", pi.GetPartition("p1"))

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), nil)
	err := mc.CompactPartition(context.Background(), "p1", 45*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// Index should still exist with 1 entry (early return, no summary merge).
	idx, _ := cache.Get(context.Background(), "p1")
	if idx == nil {
		t.Fatal("partition should still exist after cold downgrade with single entry")
	}
	if idx.Len() != 1 {
		t.Errorf("single-entry summary should still have 1 entry, got %d", idx.Len())
	}
}

// TestMetadataCompactor_downgradeToSummary_PersistError exercises the persistFn error branch.
func TestMetadataCompactor_downgradeToSummary_PersistError(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	// Add 2+ files so the early-return guard (idx.Len() <= 1) is skipped.
	pi.AddFile("p1", "file1.parquet", map[string][]string{"trace_id": {"aaa"}})
	pi.AddFile("p1", "file2.parquet", map[string][]string{"trace_id": {"bbb"}})
	cache.Put("p1", pi.GetPartition("p1"))

	wantErr := errors.New("s3 write failed")
	persistFn := func(_ context.Context, _ string, _ []byte) error {
		return wantErr
	}

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), persistFn)
	// Cold tier is 30-90 days, so 45 days triggers downgradeToSummary.
	err := mc.CompactPartition(context.Background(), "p1", 45*24*time.Hour)
	if err == nil {
		t.Error("expected persist error to propagate, got nil")
	}
}

func TestMetadataCompactor_QueryCorrectAfterDowngrade(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	for rg := 0; rg < 5; rg++ {
		key := PerRGKey("f1.parquet", rg)
		pi.AddFile("p1", key, map[string][]string{
			"trace_id": {fmt.Sprintf("trace-rg%d", rg)},
		})
	}
	cache.Put("p1", pi.GetPartition("p1"))

	mc := NewMetadataCompactor(cache, DefaultTierConfig(), nil)
	_ = mc.CompactPartition(context.Background(), "p1", 10*24*time.Hour)

	idx, _ := cache.Get(context.Background(), "p1")
	for rg := 0; rg < 5; rg++ {
		val := fmt.Sprintf("trace-rg%d", rg)
		result := idx.MayContain([]string{"f1.parquet"}, "trace_id", val)
		if len(result) == 0 {
			t.Errorf("after warm downgrade, should still find %s in f1.parquet", val)
		}
	}
}
