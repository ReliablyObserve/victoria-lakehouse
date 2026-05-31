package cache

// goleak smoke test for the cache package. The LRU + DiskCache use a
// per-entry boundRelease closure that captures a goroutine-free
// resourcebounds.Bound TryAcquire — verifying no leaks here pins the
// contract that bound wiring stays goroutine-neutral on cache hot paths.

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
)

func TestGoleak_LRUPutWithBound(t *testing.T) {
	defer goleak.VerifyNone(t,
		// VL's fasttime package starts a long-running ticker on
		// package init; not our goroutine to manage.
		goleak.IgnoreTopFunction("github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime.init.0.func1"),
	)
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024 * 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	lru := NewLRU(1024 * 1024)
	lru.SetBound(bound)
	for i := 0; i < 100; i++ {
		lru.Put("k", make([]byte, 100))
	}
	lru.Clear()
}

func TestGoleak_DiskCachePutWithBound(t *testing.T) {
	defer goleak.VerifyNone(t,
		// VL's fasttime package starts a long-running ticker on
		// package init; not our goroutine to manage.
		goleak.IgnoreTopFunction("github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime.init.0.func1"),
	)
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024 * 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	dc, err := NewDiskCache(t.TempDir(), 1024*1024, 0.8)
	if err != nil {
		t.Fatalf("disk cache: %v", err)
	}
	dc.SetBound(bound)
	for i := 0; i < 50; i++ {
		if _, err := dc.Put("k", make([]byte, 100)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := dc.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
}
