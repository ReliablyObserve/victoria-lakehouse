package discovery

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"testing"
)

func forceGC() {
	runtime.GC()
	runtime.GC()
}

func heapInUse() uint64 {
	var m runtime.MemStats
	forceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestDiscovery_MemLeak_DiscoverStorageNodesCycles(t *testing.T) {
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
	for i := 0; i < 100; i++ {
		_, _ = d.DiscoverStorageNodes(ctx)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 10_000; i++ {
		_, _ = d.DiscoverStorageNodes(ctx)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 10K DiscoverStorageNodes cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestDiscovery_MemLeak_DiscoverPeersCycles(t *testing.T) {
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
	for i := 0; i < 100; i++ {
		_, _ = d.DiscoverPeers(ctx)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 10_000; i++ {
		_, _ = d.DiscoverPeers(ctx)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 10K DiscoverPeers cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestDiscovery_MemLeak_GetterCycles(t *testing.T) {
	d := New("", []string{"node1:9428", "node2:9428"}, "", "", "9428", 5000000000)
	ctx := context.Background()
	_, _ = d.DiscoverStorageNodes(ctx)

	d.SetHotBoundaryForTest(&HotBoundary{MinDate: "20260401", MaxDate: "20260501"})

	for i := 0; i < 1000; i++ {
		_ = d.GetHotBoundary()
		_ = d.GetStorageNodes()
		_ = d.GetPeers()
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		_ = d.GetHotBoundary()
		_ = d.GetStorageNodes()
		_ = d.GetPeers()
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 100K getter cycles (max allowed %d)", growth, maxGrowth)
	}
}

func BenchmarkDiscovery_GetStorageNodes(b *testing.B) {
	d := New("", []string{"node1:9428", "node2:9428", "node3:9428"}, "", "", "9428", 5000000000)
	ctx := context.Background()
	_, _ = d.DiscoverStorageNodes(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.GetStorageNodes()
	}
}

func BenchmarkDiscovery_GetHotBoundary(b *testing.B) {
	d := New("", nil, "", "", "9428", 5000000000)
	d.SetHotBoundaryForTest(&HotBoundary{MinDate: "20260401", MaxDate: "20260501"})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.GetHotBoundary()
	}
}

func BenchmarkDiscovery_DiscoverStorageNodes(b *testing.B) {
	d := New(
		"test.svc.cluster.local",
		nil,
		"auth",
		"",
		"9428",
		5000000000,
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", []*net.SRV{
				{Target: "n1.", Port: 9428},
				{Target: "n2.", Port: 9428},
				{Target: fmt.Sprintf("n%d.", 3), Port: 9428},
			}, nil
		}),
	)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = d.DiscoverStorageNodes(ctx)
	}
}
