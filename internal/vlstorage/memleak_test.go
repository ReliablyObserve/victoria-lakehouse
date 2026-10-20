package vlstorage

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest/memstore"
)

func TestMemLeak_Adapter_TombstoneAddRemoveCycles(t *testing.T) {
	store := memstore.New(t)
	a := &adapter{
		store:      mockStore{},
		tombstones: store,
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 100
	const itemsPerCycle = 20
	for c := 0; c < cycles; c++ {
		for i := 0; i < itemsPerCycle; i++ {
			id := fmt.Sprintf("task-%d-%d", c, i)
			// run_task is refused (tombstones are not tenant-scoped), so the
			// cycle adds through the store and removes through the adapter.
			memstore.Create(t, store, delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: id, Query: "level:error", EndNs: time.Now().UnixNano(), CreatedAt: time.Now(), Mode: "auto"})
		}
		for i := 0; i < itemsPerCycle; i++ {
			id := fmt.Sprintf("task-%d-%d", c, i)
			err := a.DeleteStopTask(context.Background(), id)
			if err != nil {
				t.Fatalf("DeleteStopTask failed: %v", err)
			}
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Adapter tombstone add/remove: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// An un-deleted tombstone stays in the view and the bucket until its
	// retention passes (a late repeat of the task must meet its fate key), so
	// the heap grows with the number of tasks, by about 1KB each: bounded at
	// 2KB per task here (measured ~1KB).
	maxGrowth := uint64(cycles * itemsPerCycle * 2 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Adapter_DeleteActiveTasksCycles(t *testing.T) {
	store := memstore.New(t)
	a := &adapter{
		store:      mockStore{},
		tombstones: store,
	}

	// Pre-populate with stable set
	for i := 0; i < 10; i++ {
		memstore.Create(t, store, delete.Tombstone{
			Tenants:   []delete.TenantRef{{}},
			ID:        fmt.Sprintf("active-%d", i),
			Query:     "*",
			StartNs:   time.Now().Add(-time.Hour).UnixNano(),
			EndNs:     time.Now().UnixNano(),
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
		tasks, err := a.DeleteActiveTasks(context.Background())
		if err != nil {
			t.Fatalf("DeleteActiveTasks failed: %v", err)
		}
		_ = tasks
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Adapter DeleteActiveTasks: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Returns slice of pointers: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Adapter_FilterValuesBySubstring(t *testing.T) {
	values := make([]logstorage.ValueWithHits, 50)
	for i := range values {
		values[i] = logstorage.ValueWithHits{Value: fmt.Sprintf("field-%d", i)}
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 500
	filters := []string{"field-1", "field-2", ""}
	for c := 0; c < cycles; c++ {
		for _, f := range filters {
			result := filterValuesBySubstring(values, f)
			_ = result
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("filterValuesBySubstring: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Filtered slices allocated per call: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d filter cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Adapter_NilTombstoneCycles(t *testing.T) {
	// Test adapter with nil tombstones (no-op paths)
	a := &adapter{
		store:      mockStore{},
		tombstones: nil,
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 200
	for c := 0; c < cycles; c++ {
		q, _ := logstorage.ParseFilter("*")
		_ = a.DeleteRunTask(context.Background(), fmt.Sprintf("t%d", c), 0, nil, q)
		_ = a.DeleteStopTask(context.Background(), fmt.Sprintf("t%d", c))
		tasks, _ := a.DeleteActiveTasks(context.Background())
		_ = tasks
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Adapter nil tombstones: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// No-op paths: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d nil-tombstone cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
