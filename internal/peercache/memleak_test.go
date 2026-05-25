package peercache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestMemLeak_PeerCache_UpdatePeersCycles(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	peers := make([]string, 5)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer%d:9428", i)
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 500
	for c := 0; c < cycles; c++ {
		pc.UpdatePeers(peers)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("PeerCache UpdatePeers: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Ring rebuild with 5 peers: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d UpdatePeers cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_PeerCache_LookupCycles(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)
	pc.UpdatePeers([]string{"self:9428", "a:9428", "b:9428", "c:9428"})

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 10000
	for c := 0; c < cycles; c++ {
		_, _ = pc.Lookup(fmt.Sprintf("key-%d", c))
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("PeerCache Lookup: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Hash ring lookup only: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Lookup cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_PeerCache_StatsCycles(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)
	pc.UpdatePeers([]string{"a:9428", "b:9428"})

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 5000
	for c := 0; c < cycles; c++ {
		_ = pc.Stats()
		_ = pc.Members()
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("PeerCache Stats/Members: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Structs returned by value, Members() returns small slice: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Stats/Members cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_PeerCache_UpdatePeersWithZonesCycles(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		"peer0:9428": "us-east-1a",
		"peer1:9428": "us-east-1a",
		"peer2:9428": "us-east-1b",
		"peer3:9428": "us-east-1b",
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 200
	for c := 0; c < cycles; c++ {
		pc.UpdatePeersWithZones(peerZones, "us-east-1a")
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("PeerCache UpdatePeersWithZones: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Ring rebuild with zones: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d UpdatePeersWithZones cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_PeerCache_HandlerPutGetCycles(t *testing.T) {
	h := NewHandler("test-key", "us-east-1a")

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 100
	const itemsPerCycle = 20
	for c := 0; c < cycles; c++ {
		for i := 0; i < itemsPerCycle; i++ {
			key := fmt.Sprintf("k-%d", i%10) // bounded key space
			h.Put(key, make([]byte, 256))
			_, _ = h.Get(key)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Handler Put/Get: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Cache map with 10 fixed keys: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Put/Get cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_PeerCache_ConcurrentLookup(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)
	pc.UpdatePeers([]string{"self:9428", "a:9428", "b:9428", "c:9428", "d:9428"})

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	var wg sync.WaitGroup
	const goroutines = 8
	const lookupsPerGoroutine = 200

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < lookupsPerGoroutine; i++ {
				_, _ = pc.Lookup(fmt.Sprintf("g%d-key%d", gID, i))
			}
		}(g)
	}
	wg.Wait()

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("PeerCache concurrent lookup: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Concurrent reads: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after concurrent lookups",
			heapBefore/1024, heapAfter/1024)
	}
}
