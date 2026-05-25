package delete

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestMemLeak_TombstoneStore_AddRemoveCycles(t *testing.T) {
	store := NewTombstoneStore()

	// Warm up
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 100
	const itemsPerCycle = 50
	for c := 0; c < cycles; c++ {
		for i := 0; i < itemsPerCycle; i++ {
			id := fmt.Sprintf("tombstone-%d-%d", c, i)
			store.Add(Tombstone{
				ID:        id,
				Query:     "level:error",
				StartNs:   time.Now().UnixNano() - int64(time.Hour),
				EndNs:     time.Now().UnixNano(),
				CreatedAt: time.Now(),
				Mode:      "hide",
			})
		}
		for i := 0; i < itemsPerCycle; i++ {
			id := fmt.Sprintf("tombstone-%d-%d", c, i)
			store.Remove(id)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TombstoneStore add/remove: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Small in-memory map: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TombstoneStore_ForRangeCycles(t *testing.T) {
	store := NewTombstoneStore()

	// Pre-populate with a fixed set of tombstones
	now := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		store.Add(Tombstone{
			ID:        fmt.Sprintf("ts-%d", i),
			Query:     "*",
			StartNs:   now - int64(time.Hour),
			EndNs:     now,
			CreatedAt: time.Now(),
			Mode:      "hide",
		})
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 200
	for c := 0; c < cycles; c++ {
		start := now - int64(time.Duration(c%60)*time.Minute)
		end := now
		_ = store.ForRange(start, end)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TombstoneStore ForRange: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// ForRange returns slices: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TombstoneStore_ConcurrentAddCheck(t *testing.T) {
	store := NewTombstoneStore()

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const goroutines = 8
	const opsPerGoroutine = 50
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			now := time.Now().UnixNano()
			for i := 0; i < opsPerGoroutine; i++ {
				id := fmt.Sprintf("g%d-ts%d", gID, i)
				store.Add(Tombstone{
					ID:        id,
					Query:     "app:myapp",
					StartNs:   now - int64(time.Hour),
					EndNs:     now,
					CreatedAt: time.Now(),
					Mode:      "auto",
				})
				_, _ = store.Get(id)
				store.Remove(id)
			}
		}(g)
	}
	wg.Wait()

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TombstoneStore concurrent: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Concurrent map ops: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after concurrent ops",
			heapBefore/1024, heapAfter/1024)
	}
}

func TestMemLeak_TombstoneStore_ActiveCycles(t *testing.T) {
	store := NewTombstoneStore()

	// Steady-state: keep 10 tombstones active, call Active() repeatedly
	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		store.Add(Tombstone{
			ID:        fmt.Sprintf("active-%d", i),
			Query:     "*",
			StartNs:   now - int64(time.Hour),
			EndNs:     now,
			CreatedAt: time.Now(),
			Mode:      "permanent",
		})
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 500
	for c := 0; c < cycles; c++ {
		_ = store.Active()
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TombstoneStore Active: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Active() allocates a small slice copy each time: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Active() cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
