package cache

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
)

// TestCacheMemory_BoundEnforced verifies that the LRU's runtime
// SetBound wiring rejects Put calls when the bound is exhausted. With
// Limit=100 bytes and a 50-byte first Put admitted, a 60-byte second
// Put should be rejected — total 110 > Limit 100 and outstanding count
// is already 1 (no outlier path).
//
// NEGATIVE CONTROL: removing the `c.bound.TryAcquire(size)` call from
// putBuffer in cache/lru.go causes BOTH Puts to be admitted (LRU's
// own eviction handles overflow internally, but the bound stays at
// the original value forever, dashboards never tick rejected_total,
// and operators have no signal that they over-provisioned the cache
// memory ceiling relative to other caches sharing the same bound).
func TestCacheMemory_BoundEnforced(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 10, Limit: 100, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)

	lru := NewLRU(1024 * 1024) // generous LRU; bound is the gate
	lru.SetBound(bound)

	// First Put: 50 bytes, admits (outstanding=50 < 100).
	lru.Put("k1", make([]byte, 50))
	if _, ok := lru.Get("k1"); !ok {
		t.Fatal("first Put dropped unexpectedly")
	}

	// Second Put: 60 bytes, would overflow (50+60=110 > 100). The
	// bound's outlier path admits when count==0, but count is 1
	// here, so this MUST reject.
	lru.Put("k2", make([]byte, 60))
	if _, ok := lru.Get("k2"); ok {
		t.Fatal("second Put should have been rejected by bound")
	}

	if got := lru.RejectedByBound(); got != 1 {
		t.Errorf("LRU.RejectedByBound() = %d, want 1", got)
	}
	_, rejected, outBytes, _ := bound.Stats()
	if rejected != 1 {
		t.Errorf("bound.rejected = %d, want 1", rejected)
	}
	if outBytes != 50 {
		t.Errorf("bound.outBytes = %d, want 50 (only the admitted Put)", outBytes)
	}
}

// TestCacheMemory_BoundReleasesOnEviction verifies that the LRU's
// eviction path releases bound slots — without this, churn-heavy
// workloads (write more than the LRU's own maxSize) leak bound slots
// until the bound becomes exhausted and silently rejects all Puts.
//
// NEGATIVE CONTROL: dropping the `e.boundRelease()` call from
// evictOldest causes outstanding bytes to grow unbounded across N
// Puts even though the LRU is at steady state.
func TestCacheMemory_BoundReleasesOnEviction(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 10, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)

	// Tight LRU: maxSize=100 bytes — every Put over capacity evicts.
	lru := NewLRU(100)
	lru.SetBound(bound)

	// Put 10 entries of 50 bytes each — each eviction must release the
	// evicted entry's bound slot.
	for i := 0; i < 10; i++ {
		lru.Put(string([]byte{byte('a' + i)}), make([]byte, 50)) // 1-byte key, 50-byte val
	}

	// LRU.Size returns the LRU's own size tracking, which should be
	// bounded by maxSize=100 (so ~2 entries × 50B in the LRU at any time).
	if got := lru.Size(); got > 100 {
		t.Errorf("LRU.Size=%d exceeds maxSize=100", got)
	}

	// Bound.outBytes must equal LRU's residency, not the 10×50=500B churn.
	_, _, outBytes, outCount := bound.Stats()
	if outBytes > 100 {
		t.Errorf("bound leaked: outBytes=%d > LRU maxSize=100", outBytes)
	}
	if outCount > 2 {
		t.Errorf("bound leaked: outCount=%d > expected (~2 entries fit in 100-byte LRU)", outCount)
	}
}

// TestCacheMemory_BoundReleasesOnDelete verifies the Delete code path
// releases the bound slot. Without this, applications that explicitly
// Delete entries (cache invalidation, tombstones, evictByKey patterns)
// leak bound slots even though the LRU itself drops the entry.
func TestCacheMemory_BoundReleasesOnDelete(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	lru := NewLRU(1024)
	lru.SetBound(bound)

	lru.Put("k1", make([]byte, 50))
	_, _, outBytes, _ := bound.Stats()
	if outBytes != 50 {
		t.Fatalf("pre-delete outBytes=%d, want 50", outBytes)
	}

	lru.Delete("k1")
	_, _, outBytes, _ = bound.Stats()
	if outBytes != 0 {
		t.Errorf("post-delete outBytes=%d, want 0", outBytes)
	}
}

// TestCacheMemory_BoundReleasesOnClear verifies the Clear code path
// releases ALL bound slots. Without this, periodic full-cache flushes
// (operator-triggered or eviction loops) leak the entire residency
// to the bound and quickly exhaust it.
func TestCacheMemory_BoundReleasesOnClear(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	lru := NewLRU(1024)
	lru.SetBound(bound)

	for i := 0; i < 5; i++ {
		lru.Put(string([]byte{byte('a' + i)}), make([]byte, 50))
	}

	lru.Clear()
	_, _, outBytes, outCount := bound.Stats()
	if outBytes != 0 {
		t.Errorf("post-clear outBytes=%d, want 0", outBytes)
	}
	if outCount != 0 {
		t.Errorf("post-clear outCount=%d, want 0", outCount)
	}
}

// TestCacheMemory_BoundRejectedMetricFires proves the operator-visible
// rejected_total Prometheus counter actually increments when Put is
// rejected. This is the dashboard signal that the wiring is load-bearing.
func TestCacheMemory_BoundRejectedMetricFires(t *testing.T) {
	var rejAdds int64
	sink := &resourcebounds.PrometheusSink{
		Rejected: func(n int64) { atomic.AddInt64(&rejAdds, n) },
	}
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 50, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, sink)
	lru := NewLRU(1024)
	lru.SetBound(bound)

	// First Put fills the bound; subsequent puts get rejected.
	lru.Put("k1", make([]byte, 50))
	for i := 0; i < 3; i++ {
		lru.Put(string([]byte{byte('a' + i)}), make([]byte, 50))
	}

	if got := atomic.LoadInt64(&rejAdds); got != 3 {
		t.Errorf("rejected_total metric: got %d adds, want 3", got)
	}
}

// TestCacheMemory_NilBoundPassthrough verifies SetBound(nil) preserves
// pre-bound LRU behaviour exactly — the bound is OPT-IN and never
// breaks the cache for callers that don't wire it.
func TestCacheMemory_NilBoundPassthrough(t *testing.T) {
	lru := NewLRU(100)
	// No SetBound call: lru.bound is nil.

	for i := 0; i < 10; i++ {
		lru.Put(string([]byte{byte('a' + i)}), make([]byte, 50))
	}
	// LRU's own eviction keeps size bounded.
	if got := lru.Size(); got > 100 {
		t.Errorf("nil-bound LRU size=%d > maxSize=100", got)
	}
	if got := lru.RejectedByBound(); got != 0 {
		t.Errorf("nil-bound rejected = %d, want 0", got)
	}
}

// TestCacheMemory_UpdateReleasesOldSlot verifies that re-Putting the
// same key releases the old entry's bound slot before re-acquiring the
// new size. Otherwise, updates would double-charge the bound and
// quickly exhaust it.
func TestCacheMemory_UpdateReleasesOldSlot(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	lru := NewLRU(1024)
	lru.SetBound(bound)

	lru.Put("k", make([]byte, 100))
	lru.Put("k", make([]byte, 200)) // update
	lru.Put("k", make([]byte, 50))  // shrink

	_, _, outBytes, outCount := bound.Stats()
	if outBytes != 50 {
		t.Errorf("update path leaked: outBytes=%d, want 50 (latest size)", outBytes)
	}
	if outCount != 1 {
		t.Errorf("update path leaked count: outCount=%d, want 1", outCount)
	}
}

// TestCacheMemory_ErrBoundFullReachable is a defensive smoke test
// for the error returned by TryAcquire. The bound package exports
// ErrBoundFull and the cache silently swallows it (best-effort); this
// test pins the contract.
func TestCacheMemory_ErrBoundFullReachable(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1, LimitCount: 1, Policy: resourcebounds.Fixed,
	}, nil)
	_, err := bound.TryAcquire(1)
	if err != nil {
		t.Fatalf("first TryAcquire: %v", err)
	}
	_, err = bound.TryAcquire(1)
	if !errors.Is(err, resourcebounds.ErrBoundFull) {
		t.Fatalf("second TryAcquire: got %v, want ErrBoundFull", err)
	}
}
