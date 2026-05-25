package storage

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestMemLeak_Storage_ContextHintCycles(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 5000
	for c := 0; c < cycles; c++ {
		ctx := context.Background()
		ctx = WithTimestampOnlyHint(ctx)
		_ = IsTimestampOnly(ctx)

		ctx2 := context.Background()
		ctx2 = WithCountOnlyHint(ctx2)
		_ = IsCountOnly(ctx2)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Storage context hints: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// context.WithValue creates small allocations per call: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d context-hint cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Storage_ContextHintComposition(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 2000
	for c := 0; c < cycles; c++ {
		ctx := context.Background()
		ctx = WithTimestampOnlyHint(ctx)
		ctx = WithCountOnlyHint(ctx)

		_ = IsTimestampOnly(ctx)
		_ = IsCountOnly(ctx)

		// Verify false path
		plain := context.Background()
		if IsTimestampOnly(plain) {
			t.Fatal("plain context should not have timestamp hint")
		}
		if IsCountOnly(plain) {
			t.Fatal("plain context should not have count hint")
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Storage context composition: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// context.WithValue chaining: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d composition cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
