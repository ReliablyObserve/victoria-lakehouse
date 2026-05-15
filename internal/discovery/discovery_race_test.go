package discovery

import (
	"context"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestDiscovery_Race_MaxGoroutines(t *testing.T) {
	d := New(
		"test.svc.cluster.local",
		nil,
		"auth-key",
		"peers.svc.cluster.local",
		"9428",
		5000000000,
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", []*net.SRV{
				{Target: "node1.", Port: 9428},
				{Target: "node2.", Port: 9428},
			}, nil
		}),
	)

	d.SetHotBoundaryForTest(&HotBoundary{
		MinDate: "20260401",
		MaxDate: "20260501",
		MinTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
	})

	const goroutines = 300
	const ops = 300
	var wg sync.WaitGroup
	wg.Add(goroutines)
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				switch rng.Intn(6) {
				case 0:
					_, _ = d.DiscoverStorageNodes(ctx)
				case 1:
					_, _ = d.DiscoverPeers(ctx)
				case 2:
					_ = d.GetHotBoundary()
				case 3:
					_ = d.GetStorageNodes()
				case 4:
					_ = d.GetPeers()
				case 5:
					d.SetHotBoundaryForTest(&HotBoundary{
						MinDate: "20260401",
						MaxDate: "20260501",
					})
				}
				if i%50 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestDiscovery_Race_StaticNodes(t *testing.T) {
	d := New(
		"",
		[]string{"node1:9428", "node2:9428", "node3:9428"},
		"auth",
		"",
		"9428",
		5000000000,
	)

	const goroutines = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	ctx := context.Background()

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				_, _ = d.DiscoverStorageNodes(ctx)
				_ = d.GetStorageNodes()
				runtime.Gosched()
			}
		}()
	}
	wg.Wait()
}
