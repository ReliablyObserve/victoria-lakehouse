package smartcache

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// forceGCSmartcache runs two GC passes to let the runtime reclaim unreachable
// memory before taking heap measurements.
func forceGCSmartcache() {
	runtime.GC()
	runtime.GC()
}

// heapInUseSmartcache returns the current HeapInuse after forcing GC.
func heapInUseSmartcache() uint64 {
	var m runtime.MemStats
	forceGCSmartcache()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// newTestController builds a minimal Controller using in-package mock types
// defined in controller_test.go. All dependencies are no-op or in-memory.
func newTestController() *Controller {
	return NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})
}

// --- Controller goroutine leak tests ---

func TestController_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	ctrl := newTestController()
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, _ = ctrl.LookupOwner(key)
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestController_NoMemoryLeak(t *testing.T) {
	// Warm up: create and discard several controllers.
	for i := 0; i < 10; i++ {
		c := newTestController()
		_, _ = c.LookupOwner("warmup-key")
	}
	forceGCSmartcache()

	before := heapInUseSmartcache()

	for i := 0; i < 100; i++ {
		c := newTestController()
		for j := 0; j < 20; j++ {
			_, _ = c.LookupOwner(fmt.Sprintf("key-%d-%d", i, j))
		}
		// Controller has no Close; let GC reclaim it.
		_ = c
	}
	forceGCSmartcache()

	after := heapInUseSmartcache()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024) // 10 MB
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew %d bytes after 100 controller create/discard cycles (max %d)", growth, maxGrowth)
	}
}

// --- BudgetedL1 goroutine and memory leak tests ---

func TestBudgetedL1_Lifecycle_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for cycle := 0; cycle < 50; cycle++ {
		l1 := NewBudgetedL1(256*1024, nil) // 256 KB budget, no L2 spill
		// Fill past budget to exercise eviction path.
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("k%d", i)
			l1.Put(key, make([]byte, 4096))
		}
		// Read some entries.
		for i := 0; i < 50; i++ {
			_, _ = l1.Get(fmt.Sprintf("k%d", i))
		}
		// Replace keys to trigger more eviction.
		for i := 0; i < 20; i++ {
			l1.Put(fmt.Sprintf("k%d", i), make([]byte, 8192))
		}
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestBudgetedL1_Lifecycle_NoMemoryLeak(t *testing.T) {
	l1 := NewBudgetedL1(512*1024, nil) // 512 KB budget

	// Warm up.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k%d", i%50)
		l1.Put(key, make([]byte, 1024))
		_, _ = l1.Get(key)
	}
	forceGCSmartcache()

	before := heapInUseSmartcache()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("k%d", i%50)
		l1.Put(key, make([]byte, 1024))
		_, _ = l1.Get(key)
		// Trigger eviction every 10 iterations by inserting a large entry.
		if i%10 == 0 {
			l1.Put(fmt.Sprintf("big-%d", i), make([]byte, 128*1024))
		}
	}
	forceGCSmartcache()

	after := heapInUseSmartcache()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024) // 10 MB
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew %d bytes after 1000 put/get/evict cycles (max %d)", growth, maxGrowth)
	}
}

// --- ChunkCacheKey bulk creation goroutine leak test ---

func TestChunkCacheKey_Bulk_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	keys := make([]string, 0, 100_000)
	for i := 0; i < 100_000; i++ {
		k := ChunkCacheKey{
			FileKey:  fmt.Sprintf("s3://bucket/file%d.parquet", i%1000),
			Column:   fmt.Sprintf("col_%d", i%20),
			RowGroup: i % 50,
		}
		keys = append(keys, k.String())
	}
	_ = keys // prevent optimizer elision

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

// --- ColumnPopularity lifecycle goroutine leak test ---

func TestColumnPopularity_Lifecycle_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for cycle := 0; cycle < 10; cycle++ {
		cp := NewColumnPopularity()
		for i := 0; i < 10_000; i++ {
			cp.Record(fmt.Sprintf("col_%d", i%50))
		}
		_ = cp.TopN(10)
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

// --- Controller eviction loop goroutine cleanup test ---

func TestController_EvictionLoop_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	stop := make(chan struct{})
	ctrl := newTestController()
	ctrl.StartEvictionLoop(10*time.Millisecond, stop)

	// Let the loop fire a few times.
	time.Sleep(50 * time.Millisecond)
	close(stop)
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after eviction loop stop: before=%d after=%d", before, after)
	}
}

// --- Controller singleflight no goroutine leak ---

func TestController_Singleflight_NoGoroutineLeak(t *testing.T) {
	s3 := newMockS3()
	s3.data["file1"] = []byte("payload")

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    s3,
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	before := runtime.NumGoroutine()

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_, _ = ctrl.Get(ctx, "file1", 7)
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after singleflight Get cycles: before=%d after=%d", before, after)
	}
}
