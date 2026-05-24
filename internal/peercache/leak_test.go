package peercache

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// forceGCPeercache runs two GC passes to allow the runtime to reclaim
// unreachable memory before heap measurements.
func forceGCPeercache() {
	runtime.GC()
	runtime.GC()
}

// heapInUsePeercache returns the current HeapInuse after forcing GC.
func heapInUsePeercache() uint64 {
	var m runtime.MemStats
	forceGCPeercache()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// testPeers returns a slice of n peer address strings for tests.
func testPeers(n int) []string {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer%d:9428", i)
	}
	return peers
}

// --- Ring goroutine leak tests ---

func TestRing_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	r := NewRing("self:9428", defaultVnodes)
	peers := testPeers(5)
	r.Set(append(peers, "self:9428"))

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, _ = r.Lookup(key)
	}
	_ = r.Members()
	_ = r.MemberCount()

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestRing_NoMemoryLeak(t *testing.T) {
	peers := testPeers(8)
	allPeers := append(peers, "self:9428")

	// Warm up.
	for i := 0; i < 10; i++ {
		r := NewRing("self:9428", defaultVnodes)
		r.Set(allPeers)
		for j := 0; j < 50; j++ {
			_, _ = r.Lookup(fmt.Sprintf("k%d", j))
		}
	}
	forceGCPeercache()

	before := heapInUsePeercache()

	for i := 0; i < 100; i++ {
		r := NewRing("self:9428", defaultVnodes)
		r.Set(allPeers)
		for j := 0; j < 200; j++ {
			_, _ = r.Lookup(fmt.Sprintf("k%d-%d", i, j))
		}
		_ = r.Members()
		_ = r.MemberCount()
		// Ring has no Close; let GC reclaim it.
		_ = r
	}
	forceGCPeercache()

	after := heapInUsePeercache()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024) // 10 MB
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew %d bytes after 100 ring create/set/lookup/discard cycles (max %d)", growth, maxGrowth)
	}
}

// --- Ring with zone info ---

func TestRing_WithZones_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	r := NewRing("self:9428", defaultVnodes)
	peerZones := map[string]string{
		"self:9428": "az-1",
		"peer1:9428": "az-1",
		"peer2:9428": "az-2",
		"peer3:9428": "az-2",
	}
	r.SetWithZones(peerZones, "az-1")

	for i := 0; i < 500; i++ {
		_, _, _ = r.LookupAZ(fmt.Sprintf("key-%d", i))
	}
	_, _ = r.MemberCountByZone()

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after AZ ring usage: before=%d after=%d", before, after)
	}
}

// --- HealthAwareRing goroutine and memory leak tests ---

func TestHealthAwareRing_Lifecycle_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for cycle := 0; cycle < 50; cycle++ {
		r := NewRing("self:9428", defaultVnodes)
		r.Set([]string{"self:9428", "peer1:9428", "peer2:9428"})
		har := NewHealthAwareRing(r, 3, 30*time.Second)

		for i := 0; i < 20; i++ {
			har.RecordSuccess(fmt.Sprintf("peer%d:9428", i%3))
		}
		for i := 0; i < 10; i++ {
			har.RecordFailure(fmt.Sprintf("peer%d:9428", i%3))
		}
		_ = har.IsHealthy("peer1:9428")
		_ = har.HealthyPeerCount()
		_ = har.GetHealth("peer1:9428")
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestHealthAwareRing_Lifecycle_NoMemoryLeak(t *testing.T) {
	r := NewRing("self:9428", defaultVnodes)
	r.Set([]string{"self:9428", "peer1:9428", "peer2:9428", "peer3:9428"})
	har := NewHealthAwareRing(r, 3, 30*time.Second)

	// Warm up.
	for i := 0; i < 100; i++ {
		peer := fmt.Sprintf("peer%d:9428", i%4)
		har.RecordSuccess(peer)
		har.RecordFailure(peer)
		_ = har.IsHealthy(peer)
	}
	forceGCPeercache()

	before := heapInUsePeercache()

	for i := 0; i < 1000; i++ {
		peer := fmt.Sprintf("peer%d:9428", i%4)
		if i%3 == 0 {
			har.RecordFailure(peer)
		} else {
			har.RecordSuccess(peer)
		}
		_ = har.IsHealthy(peer)
		if i%10 == 0 {
			_ = har.HealthyPeerCount()
			_ = har.GetHealth(peer)
		}
	}
	forceGCPeercache()

	after := heapInUsePeercache()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024) // 10 MB
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew %d bytes after 1000 health record cycles (max %d)", growth, maxGrowth)
	}
}

// --- PeerCache goroutine leak test (no network) ---

func TestPeerCache_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for cycle := 0; cycle < 20; cycle++ {
		pc := New("self:9428", "", 100*time.Millisecond, 10)
		pc.UpdatePeers([]string{"self:9428", "peer1:9428", "peer2:9428"})

		for i := 0; i < 100; i++ {
			_, _ = pc.Lookup(fmt.Sprintf("key-%d", i))
		}
		_ = pc.Members()
		_ = pc.Stats()
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}
