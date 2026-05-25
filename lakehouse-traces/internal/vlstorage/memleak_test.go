package vlstorage

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
)

func TestMemLeak_TracesAdapter_TombstoneAddRemoveCycles(t *testing.T) {
	store := delete.NewTombstoneStore()
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
			q, _ := logstorage.ParseFilter("level:error")
			err := a.DeleteRunTask(context.Background(), id, time.Now().UnixNano(), nil, q)
			if err != nil {
				t.Fatalf("DeleteRunTask failed: %v", err)
			}
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

	t.Logf("Traces adapter tombstone add/remove: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// TombstoneStore map: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TracesAdapter_DeleteActiveTasksCycles(t *testing.T) {
	store := delete.NewTombstoneStore()
	a := &adapter{
		store:      mockStore{},
		tombstones: store,
	}

	// Pre-populate
	for i := 0; i < 10; i++ {
		store.Add(delete.Tombstone{
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

	t.Logf("Traces adapter DeleteActiveTasks: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Slice returned per call: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TracesAdapter_FilterHiddenValuesCycles(t *testing.T) {
	values := make([]logstorage.ValueWithHits, 50)
	for i := range values {
		values[i] = logstorage.ValueWithHits{Value: fmt.Sprintf("field.%d", i)}
	}
	filters := []string{"field.1*", "field.2*", ""}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 500
	for c := 0; c < cycles; c++ {
		for _, f := range filters {
			var fSlice []string
			if f != "" {
				fSlice = []string{f}
			}
			result := filterHiddenValues(values, fSlice)
			_ = result
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Traces filterHiddenValues: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Filtered slices allocated and discarded: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d filter cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TracesAdapter_NilTombstoneCycles(t *testing.T) {
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

	t.Logf("Traces adapter nil tombstones: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// No-op paths: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d nil-tombstone cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
