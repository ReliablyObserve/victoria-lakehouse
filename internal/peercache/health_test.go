package peercache

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestHealthAwareRing_NewPeerIsHealthy(t *testing.T) {
	ring := NewRing("self:9428", 10)
	ring.Set([]string{"self:9428", "peer1:9428"})

	hr := NewHealthAwareRing(ring, 3, 30*time.Second)

	// Record success for peer1 — should create as healthy
	hr.RecordSuccess("peer1:9428")

	if !hr.IsHealthy("peer1:9428") {
		t.Fatal("newly recorded peer should be healthy")
	}

	ph := hr.GetHealth("peer1:9428")
	if ph == nil {
		t.Fatal("expected PeerHealth, got nil")
	}
	if ph.FailCount != 0 {
		t.Errorf("expected FailCount=0, got %d", ph.FailCount)
	}
	if !ph.IsHealthy {
		t.Error("expected IsHealthy=true")
	}
	if ph.LastSeen.IsZero() {
		t.Error("expected LastSeen to be set")
	}
}

func TestHealthAwareRing_FailureThreshold(t *testing.T) {
	maxFails := 5
	hr := NewHealthAwareRing(nil, maxFails, 30*time.Second)

	peer := "peer1:9428"
	// Record (maxFails - 1) failures — should stay healthy
	for i := 0; i < maxFails-1; i++ {
		hr.RecordFailure(peer)
		if !hr.IsHealthy(peer) {
			t.Fatalf("peer should remain healthy after %d failures (threshold=%d)", i+1, maxFails)
		}
	}

	ph := hr.GetHealth(peer)
	if ph == nil {
		t.Fatal("expected PeerHealth")
	}
	if ph.FailCount != maxFails-1 {
		t.Errorf("expected FailCount=%d, got %d", maxFails-1, ph.FailCount)
	}
}

func TestHealthAwareRing_MarkUnhealthy(t *testing.T) {
	maxFails := 3
	hr := NewHealthAwareRing(nil, maxFails, 30*time.Second)

	peer := "peer1:9428"
	// Record exactly maxFails failures — should become unhealthy
	for i := 0; i < maxFails; i++ {
		hr.RecordFailure(peer)
	}

	if hr.IsHealthy(peer) {
		t.Fatal("peer should be unhealthy after reaching maxFails")
	}

	ph := hr.GetHealth(peer)
	if ph == nil {
		t.Fatal("expected PeerHealth")
	}
	if ph.FailCount != maxFails {
		t.Errorf("expected FailCount=%d, got %d", maxFails, ph.FailCount)
	}
	if ph.IsHealthy {
		t.Error("expected IsHealthy=false")
	}

	// Additional failures keep it unhealthy
	hr.RecordFailure(peer)
	if hr.IsHealthy(peer) {
		t.Fatal("peer should remain unhealthy after additional failures")
	}
}

func TestHealthAwareRing_RecoverAfterSuccess(t *testing.T) {
	maxFails := 2
	hr := NewHealthAwareRing(nil, maxFails, 30*time.Second)

	peer := "peer1:9428"
	// Make unhealthy
	for i := 0; i < maxFails; i++ {
		hr.RecordFailure(peer)
	}
	if hr.IsHealthy(peer) {
		t.Fatal("peer should be unhealthy")
	}

	// Record success — peer recovers
	hr.RecordSuccess(peer)
	if !hr.IsHealthy(peer) {
		t.Fatal("peer should recover after RecordSuccess")
	}

	ph := hr.GetHealth(peer)
	if ph == nil {
		t.Fatal("expected PeerHealth")
	}
	if ph.FailCount != 0 {
		t.Errorf("expected FailCount=0 after recovery, got %d", ph.FailCount)
	}
	if !ph.IsHealthy {
		t.Error("expected IsHealthy=true after recovery")
	}
}

func TestHealthAwareRing_UnknownPeerIsHealthy(t *testing.T) {
	hr := NewHealthAwareRing(nil, 3, 30*time.Second)

	// Never-seen peer should be considered healthy
	if !hr.IsHealthy("unknown:9428") {
		t.Fatal("unknown peer should be considered healthy")
	}

	// GetHealth should return nil for unknown peer
	if ph := hr.GetHealth("unknown:9428"); ph != nil {
		t.Fatalf("expected nil PeerHealth for unknown peer, got %+v", ph)
	}
}

func TestHealthAwareRing_HealthyPeerCount(t *testing.T) {
	maxFails := 2
	hr := NewHealthAwareRing(nil, maxFails, 30*time.Second)

	// No tracked peers
	if c := hr.HealthyPeerCount(); c != 0 {
		t.Fatalf("expected 0 healthy peers initially, got %d", c)
	}

	// Add 3 healthy peers
	hr.RecordSuccess("peer1:9428")
	hr.RecordSuccess("peer2:9428")
	hr.RecordSuccess("peer3:9428")

	if c := hr.HealthyPeerCount(); c != 3 {
		t.Fatalf("expected 3 healthy peers, got %d", c)
	}

	// Make peer2 unhealthy
	for i := 0; i < maxFails; i++ {
		hr.RecordFailure("peer2:9428")
	}

	if c := hr.HealthyPeerCount(); c != 2 {
		t.Fatalf("expected 2 healthy peers after evicting peer2, got %d", c)
	}

	// Recover peer2
	hr.RecordSuccess("peer2:9428")
	if c := hr.HealthyPeerCount(); c != 3 {
		t.Fatalf("expected 3 healthy peers after recovery, got %d", c)
	}

	// Make all unhealthy
	for _, p := range []string{"peer1:9428", "peer2:9428", "peer3:9428"} {
		for i := 0; i < maxFails; i++ {
			hr.RecordFailure(p)
		}
	}
	if c := hr.HealthyPeerCount(); c != 0 {
		t.Fatalf("expected 0 healthy peers, got %d", c)
	}
}

func TestHealthAwareRing_ConcurrentAccess(t *testing.T) {
	hr := NewHealthAwareRing(nil, 5, 30*time.Second)

	const goroutines = 10
	const ops = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				peer := fmt.Sprintf("peer%d:9428", rng.Intn(5))
				switch rng.Intn(4) {
				case 0:
					hr.RecordSuccess(peer)
				case 1:
					hr.RecordFailure(peer)
				case 2:
					_ = hr.IsHealthy(peer)
				case 3:
					_ = hr.HealthyPeerCount()
				}
				if i%200 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestHealthAwareRing_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	hr := NewHealthAwareRing(nil, 3, 30*time.Second)
	for i := 0; i < 100; i++ {
		peer := fmt.Sprintf("peer%d:9428", i)
		hr.RecordSuccess(peer)
		hr.RecordFailure(peer)
		_ = hr.IsHealthy(peer)
		_ = hr.HealthyPeerCount()
	}

	// Discard the ring
	hr = nil       //nolint:ineffassign
	_ = hr         //nolint:staticcheck
	runtime.GC()
	runtime.GC()

	after := runtime.NumGoroutine()
	// Allow ±2 for test infrastructure jitter
	if diff := after - before; diff > 2 {
		t.Errorf("goroutine leak: before=%d after=%d diff=%d", before, after, diff)
	}
}

func TestHealthAwareRing_NoMemoryLeak(t *testing.T) {
	hr := NewHealthAwareRing(nil, 5, 30*time.Second)

	// Warm up: create 1000 peers with some operations
	for i := 0; i < 1000; i++ {
		peer := fmt.Sprintf("peer%d:9428", i)
		hr.RecordSuccess(peer)
	}
	forceGC()
	before := heapInUse()

	// Perform 10k operations on those same peers (no new map growth expected)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10_000; i++ {
		peer := fmt.Sprintf("peer%d:9428", rng.Intn(1000))
		switch rng.Intn(3) {
		case 0:
			hr.RecordSuccess(peer)
		case 1:
			hr.RecordFailure(peer)
		case 2:
			_ = hr.IsHealthy(peer)
		}
	}
	forceGC()
	after := heapInUse()

	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024) // 10 MB
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 10K operations on 1000 peers (max allowed %d)", growth, maxGrowth)
	}
}
