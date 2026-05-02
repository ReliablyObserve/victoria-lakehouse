package discovery

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestDiscovery_ConcurrentGetHotBoundary(t *testing.T) {
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	d := New("", nil, "", "", 5*time.Second, l)

	d.SetHotBoundaryForTest(&HotBoundary{
		MinDate: "20260101",
		MaxDate: "20260501",
		MinTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC),
	})

	const goroutines = 50
	const ops = 500
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				b := d.GetHotBoundary()
				if b == nil {
					t.Error("got nil boundary")
					return
				}
			}
		}()

		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				d.SetHotBoundaryForTest(&HotBoundary{
					MinDate: fmt.Sprintf("2026%02d01", (i%12)+1),
					MaxDate: fmt.Sprintf("2026%02d28", (i%12)+1),
					MinTime: time.Now(),
					MaxTime: time.Now().Add(time.Hour),
				})
			}
		}(g)
	}
	wg.Wait()
}

func TestDiscovery_ConcurrentGetStorageNodes(t *testing.T) {
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	d := New("", []string{"node1:9428", "node2:9428"}, "", "", 5*time.Second, l)

	_, err := d.DiscoverStorageNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 30
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				nodes := d.GetStorageNodes()
				if len(nodes) != 2 {
					t.Errorf("expected 2 nodes, got %d", len(nodes))
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestDiscovery_ConcurrentGetPeers(t *testing.T) {
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	d := New("", nil, "", "", 5*time.Second, l)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				_ = d.GetPeers()
				_ = d.GetStorageNodes()
				_ = d.GetHotBoundary()
			}
		}()
	}
	wg.Wait()
}
