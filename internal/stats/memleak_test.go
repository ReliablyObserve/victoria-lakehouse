package stats

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestMemLeak_TenantRegistry_RecordWriteCycles(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 100
	const tenantsPerCycle = 10
	for c := 0; c < cycles; c++ {
		for i := 0; i < tenantsPerCycle; i++ {
			tenant := fmt.Sprintf("account%d:project%d", i, i)
			reg.RecordWrite(tenant, 1024, 512, 100, "STANDARD")
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TenantRegistry RecordWrite: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// 10 unique tenants, CRDT maps: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TenantRegistry_GetCycles(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	// Pre-populate
	for i := 0; i < 10; i++ {
		reg.RecordWrite(fmt.Sprintf("acc%d:proj%d", i, i), 1024, 512, 100, "STANDARD")
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 200
	const tenantsPerCycle = 10
	for c := 0; c < cycles; c++ {
		for i := 0; i < tenantsPerCycle; i++ {
			_ = reg.Get(fmt.Sprintf("acc%d:proj%d", i, i))
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TenantRegistry Get: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Get returns deep copies: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TenantRegistry_BuildDeltaCycles(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	for i := 0; i < 10; i++ {
		reg.RecordWrite(fmt.Sprintf("acc%d:proj%d", i, i), 2048, 1024, 200, "STANDARD")
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 100
	var gen uint64
	for c := 0; c < cycles; c++ {
		delta := reg.BuildDelta(gen)
		gen = delta.Generation
		_ = delta
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TenantRegistry BuildDelta: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Delta allocations should be GC'd: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TenantRegistry_MergeCycles(t *testing.T) {
	local := NewTenantRegistry("local-node")
	remote := NewTenantRegistry("remote-node")

	for i := 0; i < 5; i++ {
		remote.RecordWrite(fmt.Sprintf("acc%d:proj%d", i, i), 1024, 512, 100, "STANDARD")
	}
	delta := remote.BuildDelta(0)

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 100
	for c := 0; c < cycles; c++ {
		local.Merge(delta)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TenantRegistry Merge: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Merge is idempotent, no accumulation: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Merge cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TenantRegistry_AllCycles(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	for i := 0; i < 20; i++ {
		reg.RecordWrite(fmt.Sprintf("acc%d:proj%d", i, i), 512, 256, 50, "STANDARD")
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 100
	for c := 0; c < cycles; c++ {
		all := reg.All()
		_ = all
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TenantRegistry All: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Returns deep-copied slice: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d All() cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_TenantRegistry_ConcurrentReadWrite(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	var wg sync.WaitGroup
	const goroutines = 8
	const opsPerGoroutine = 50

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			tenant := fmt.Sprintf("acc%d:proj%d", gID%5, gID%5)
			for i := 0; i < opsPerGoroutine; i++ {
				reg.RecordWrite(tenant, 1024, 512, 100, "STANDARD")
				_ = reg.Get(tenant)
				reg.RecordQuery(tenant)
			}
		}(g)
	}
	wg.Wait()

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TenantRegistry concurrent: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// 5 unique tenants: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after concurrent ops",
			heapBefore/1024, heapAfter/1024)
	}
}

func TestMemLeak_TenantRegistry_SnapshotCycles(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	for i := 0; i < 10; i++ {
		reg.RecordWrite(fmt.Sprintf("acc%d:proj%d", i, i), 2048, 1024, 200, "STANDARD")
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 50
	for c := 0; c < cycles; c++ {
		data, err := reg.MarshalSnapshot()
		if err != nil {
			t.Fatalf("MarshalSnapshot failed: %v", err)
		}

		other := NewTenantRegistry("node-2")
		if err := other.LoadSnapshot("node-1", data); err != nil {
			t.Fatalf("LoadSnapshot failed: %v", err)
		}
		_ = other
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("TenantRegistry snapshot: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// JSON marshal/unmarshal allocations: 5MB max
	maxGrowth := uint64(5 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d snapshot cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
