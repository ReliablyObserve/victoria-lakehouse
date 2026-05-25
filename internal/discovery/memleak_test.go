package discovery

import (
	"context"
	"net"
	"runtime"
	"testing"
)

func discoveryGC() {
	runtime.GC()
	runtime.GC()
}

func discoveryHeap() uint64 {
	var m runtime.MemStats
	discoveryGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestMemLeak_Discovery_DiscoverStorageNodes(t *testing.T) {
	d := New(
		"test.svc.cluster.local",
		nil,
		"auth-key",
		"",
		"9428",
		5000000000,
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", []*net.SRV{
				{Target: "node1.svc.cluster.local.", Port: 9428},
				{Target: "node2.svc.cluster.local.", Port: 9428},
				{Target: "node3.svc.cluster.local.", Port: 9428},
			}, nil
		}),
	)

	ctx := context.Background()

	// Warm up
	for i := 0; i < 50; i++ {
		_, _ = d.DiscoverStorageNodes(ctx)
	}
	discoveryGC()

	before := discoveryHeap()

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		_, _ = d.DiscoverStorageNodes(ctx)
	}

	discoveryGC()
	after := discoveryHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d DiscoverStorageNodes cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Discovery_DiscoverPeers(t *testing.T) {
	d := New(
		"",
		nil,
		"",
		"peers.svc.cluster.local",
		"9428",
		5000000000,
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", []*net.SRV{
				{Target: "peer1.", Port: 9428},
				{Target: "peer2.", Port: 9428},
			}, nil
		}),
	)

	ctx := context.Background()

	// Warm up
	for i := 0; i < 50; i++ {
		_, _ = d.DiscoverPeers(ctx)
	}
	discoveryGC()

	before := discoveryHeap()

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		_, _ = d.DiscoverPeers(ctx)
	}

	discoveryGC()
	after := discoveryHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d DiscoverPeers cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Discovery_GetterCycles(t *testing.T) {
	d := New("", []string{"node1:9428", "node2:9428"}, "", "", "9428", 5000000000)
	ctx := context.Background()
	_, _ = d.DiscoverStorageNodes(ctx)
	d.SetHotBoundaryForTest(&HotBoundary{MinDate: "20260401", MaxDate: "20260501"})

	// Warm up
	for i := 0; i < 500; i++ {
		_ = d.GetHotBoundary()
		_ = d.GetStorageNodes()
		_ = d.GetPeers()
	}
	discoveryGC()

	before := discoveryHeap()

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		_ = d.GetHotBoundary()
		_ = d.GetStorageNodes()
		_ = d.GetPeers()
	}

	discoveryGC()
	after := discoveryHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d getter cycles (max %d)", growth, iterations, maxAllowed)
	}
}
