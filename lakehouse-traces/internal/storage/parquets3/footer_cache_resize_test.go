package parquets3

import (
	"fmt"
	"sync"
	"testing"
)

// TestFooterCache_ResizeShrink pins the eviction behaviour when the
// new cap is smaller than the current cache size. The LRU tail must
// be evicted until we fit; recently-touched entries at the head are
// preserved.
func TestFooterCache_ResizeShrink(t *testing.T) {
	fc := NewFooterCache(100)
	for i := 0; i < 50; i++ {
		fc.Put(fmt.Sprintf("k%d", i), &CachedFooter{FileSize: int64(i)})
	}
	if fc.Len() != 50 {
		t.Fatalf("len before shrink = %d, want 50", fc.Len())
	}
	// Touch the newest 10 so they move to head — must survive shrink.
	for i := 40; i < 50; i++ {
		fc.Get(fmt.Sprintf("k%d", i))
	}

	evicted := fc.Resize(10)
	if evicted != 40 {
		t.Errorf("evicted=%d, want 40", evicted)
	}
	if fc.Len() != 10 {
		t.Errorf("len after shrink = %d, want 10", fc.Len())
	}
	// The 10 most-recently-touched (k40..k49) must still be present.
	for i := 40; i < 50; i++ {
		if _, ok := fc.Get(fmt.Sprintf("k%d", i)); !ok {
			t.Errorf("k%d evicted but it was at the LRU head", i)
		}
	}
	// The cold entries (k0..k39) must be gone.
	for i := 0; i < 40; i++ {
		if fc.Has(fmt.Sprintf("k%d", i)) {
			t.Errorf("k%d survived shrink but it should have been evicted", i)
		}
	}
}

// TestFooterCache_ResizeGrow pins the simple grow path: no entries
// evicted, subsequent puts respect the new ceiling.
func TestFooterCache_ResizeGrow(t *testing.T) {
	fc := NewFooterCache(10)
	for i := 0; i < 10; i++ {
		fc.Put(fmt.Sprintf("k%d", i), &CachedFooter{FileSize: int64(i)})
	}

	evicted := fc.Resize(100)
	if evicted != 0 {
		t.Errorf("grow shouldn't evict anything, got evicted=%d", evicted)
	}
	if fc.MaxItems() != 100 {
		t.Errorf("MaxItems()=%d after grow, want 100", fc.MaxItems())
	}

	// Add up to the new cap; nothing should be evicted.
	for i := 10; i < 100; i++ {
		fc.Put(fmt.Sprintf("k%d", i), &CachedFooter{FileSize: int64(i)})
	}
	if fc.Len() != 100 {
		t.Errorf("len after fill = %d, want 100", fc.Len())
	}
	for i := 0; i < 100; i++ {
		if !fc.Has(fmt.Sprintf("k%d", i)) {
			t.Errorf("k%d missing after grow", i)
		}
	}
}

// TestFooterCache_ResizeIdempotent guards against re-evicting on
// a no-op resize (cap unchanged). The retune-on-every-refresh
// pattern means we call Resize() frequently with the same value
// when the manifest hasn't grown.
func TestFooterCache_ResizeIdempotent(t *testing.T) {
	fc := NewFooterCache(50)
	for i := 0; i < 50; i++ {
		fc.Put(fmt.Sprintf("k%d", i), &CachedFooter{FileSize: int64(i)})
	}
	if evicted := fc.Resize(50); evicted != 0 {
		t.Errorf("same-cap resize shouldn't evict, got evicted=%d", evicted)
	}
	if fc.Len() != 50 {
		t.Errorf("len after no-op resize = %d, want 50", fc.Len())
	}
}

// TestFooterCache_ResizeUnderConcurrentGets is the safety net: while
// queries are issuing Get() on the cache (the only hot path), Resize()
// must not deadlock or skip valid entries. Run under -race to catch
// any lock-ordering bugs.
func TestFooterCache_ResizeUnderConcurrentGets(t *testing.T) {
	if testing.Short() {
		t.Skip("race coverage")
	}
	fc := NewFooterCache(1000)
	for i := 0; i < 500; i++ {
		fc.Put(fmt.Sprintf("k%d", i), &CachedFooter{FileSize: int64(i)})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					fc.Get(fmt.Sprintf("k%d", 0))
				}
			}
		}()
	}

	// Resize a few times while Gets are hammering.
	for _, target := range []int{200, 100, 50, 500, 1000} {
		fc.Resize(target)
		if fc.MaxItems() != target {
			t.Errorf("MaxItems() = %d after Resize(%d)", fc.MaxItems(), target)
		}
	}
	close(stop)
	wg.Wait()
}

// TestFooterCache_ResizeZeroIsNoop pins the safety guard for an
// invalid target. retuneFooterCache could legitimately compute 0 if
// the manifest is empty AND no config override is set; we treat that
// as a no-op rather than collapsing the cache.
func TestFooterCache_ResizeZeroIsNoop(t *testing.T) {
	fc := NewFooterCache(100)
	for i := 0; i < 50; i++ {
		fc.Put(fmt.Sprintf("k%d", i), &CachedFooter{FileSize: int64(i)})
	}
	evicted := fc.Resize(0)
	if evicted != 0 || fc.Len() != 50 {
		t.Errorf("Resize(0) should be no-op, got evicted=%d len=%d", evicted, fc.Len())
	}
}
