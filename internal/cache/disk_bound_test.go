package cache

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
)

// TestSmartCacheDisk_BoundEnforced verifies that DiskCache.Put rejects
// when the bound is exhausted. Same shape as
// TestCacheMemory_BoundEnforced but on the disk-cache path —
// SetBound + TryAcquire + per-entry boundRelease.
//
// NEGATIVE CONTROL: removing the `d.bound.TryAcquire(size)` call from
// disk.go Put causes both Puts to succeed, the disk fills past the
// operator-visible limit, and rejected_total never increments.
func TestSmartCacheDisk_BoundEnforced(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 10, Limit: 100, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)

	dc, err := NewDiskCache(t.TempDir(), 1024*1024, 0.8)
	if err != nil {
		t.Fatalf("disk cache: %v", err)
	}
	dc.SetBound(bound)

	if _, err := dc.Put("k1", make([]byte, 50)); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	// Second Put: 60B, would overflow.
	_, err = dc.Put("k2", make([]byte, 60))
	if err == nil {
		t.Fatal("second Put should have been rejected by bound")
	}
	if !errors.Is(err, resourcebounds.ErrBoundFull) {
		t.Errorf("expected ErrBoundFull; got %v", err)
	}
	if got := dc.RejectedByBound(); got != 1 {
		t.Errorf("DiskCache.RejectedByBound() = %d, want 1", got)
	}
	_, rejected, _, _ := bound.Stats()
	if rejected != 1 {
		t.Errorf("bound.rejected = %d, want 1", rejected)
	}
}

// TestSmartCacheDisk_BoundReleasesOnEviction verifies the watermark
// eviction loop in disk.go releases bound slots — otherwise churn-heavy
// disk-cache workloads leak slots into the bound until the operator
// limit silently rejects all future Puts.
//
// NEGATIVE CONTROL: dropping `de.boundRelease()` from evictIfNeeded
// causes outBytes to grow unbounded.
func TestSmartCacheDisk_BoundReleasesOnEviction(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 10, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	// Tight maxSize (100) and tight watermark to force eviction churn.
	dc, err := NewDiskCache(t.TempDir(), 100, 0.5)
	if err != nil {
		t.Fatalf("disk cache: %v", err)
	}
	dc.SetBound(bound)

	for i := 0; i < 10; i++ {
		if _, err := dc.Put(string([]byte{byte('a' + i)}), make([]byte, 40)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Bound's outBytes must match disk-cache's residency — NOT the
	// 10×40=400B churn — proving eviction releases slots.
	_, _, outBytes, _ := bound.Stats()
	if outBytes > 100 {
		t.Errorf("bound leaked: outBytes=%d > disk maxSize=100", outBytes)
	}
}

// TestSmartCacheDisk_BoundReleasesOnDelete proves explicit Delete also
// releases. Symmetric with TestCacheMemory_BoundReleasesOnDelete.
func TestSmartCacheDisk_BoundReleasesOnDelete(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	dc, err := NewDiskCache(t.TempDir(), 1024, 0.8)
	if err != nil {
		t.Fatalf("disk cache: %v", err)
	}
	dc.SetBound(bound)

	if _, err := dc.Put("k1", make([]byte, 50)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, _, outBytes, _ := bound.Stats()
	if outBytes != 50 {
		t.Fatalf("pre-delete outBytes=%d, want 50", outBytes)
	}

	dc.Delete("k1")
	_, _, outBytes, _ = bound.Stats()
	if outBytes != 0 {
		t.Errorf("post-delete outBytes=%d, want 0", outBytes)
	}
}

// TestSmartCacheDisk_BoundReleasesOnClear proves Clear releases all
// bound slots — without this, operator-initiated full flushes leak
// the entire residency.
func TestSmartCacheDisk_BoundReleasesOnClear(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	dc, err := NewDiskCache(t.TempDir(), 1024, 0.8)
	if err != nil {
		t.Fatalf("disk cache: %v", err)
	}
	dc.SetBound(bound)

	for i := 0; i < 5; i++ {
		if _, err := dc.Put(string([]byte{byte('a' + i)}), make([]byte, 50)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if err := dc.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	_, _, outBytes, outCount := bound.Stats()
	if outBytes != 0 {
		t.Errorf("post-clear outBytes=%d, want 0", outBytes)
	}
	if outCount != 0 {
		t.Errorf("post-clear outCount=%d, want 0", outCount)
	}
}

// TestSmartCacheDisk_BoundRejectedMetricFires proves the operator
// signal increments.
func TestSmartCacheDisk_BoundRejectedMetricFires(t *testing.T) {
	var rejAdds int64
	sink := &resourcebounds.PrometheusSink{
		Rejected: func(n int64) { atomic.AddInt64(&rejAdds, n) },
	}
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 50, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, sink)
	dc, err := NewDiskCache(t.TempDir(), 1024, 0.8)
	if err != nil {
		t.Fatalf("disk cache: %v", err)
	}
	dc.SetBound(bound)

	if _, err := dc.Put("k1", make([]byte, 50)); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := dc.Put(string([]byte{byte('a' + i)}), make([]byte, 50)); err == nil {
			t.Fatalf("Put %d should have been rejected", i)
		}
	}
	if got := atomic.LoadInt64(&rejAdds); got != 3 {
		t.Errorf("rejected_total: got %d adds, want 3", got)
	}
}

// TestSmartCacheDisk_NilBoundPassthrough preserves pre-bound behaviour.
func TestSmartCacheDisk_NilBoundPassthrough(t *testing.T) {
	dc, err := NewDiskCache(t.TempDir(), 100, 0.5)
	if err != nil {
		t.Fatalf("disk cache: %v", err)
	}
	// No SetBound.
	for i := 0; i < 10; i++ {
		if _, err := dc.Put(string([]byte{byte('a' + i)}), make([]byte, 50)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if got := dc.RejectedByBound(); got != 0 {
		t.Errorf("nil-bound rejected = %d, want 0", got)
	}
}

// TestSmartCacheDisk_UpdateReleasesOldSlot — symmetric with cache memory.
func TestSmartCacheDisk_UpdateReleasesOldSlot(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	dc, err := NewDiskCache(t.TempDir(), 1024, 0.8)
	if err != nil {
		t.Fatalf("disk cache: %v", err)
	}
	dc.SetBound(bound)

	if _, err := dc.Put("k", make([]byte, 100)); err != nil {
		t.Fatalf("Put 100: %v", err)
	}
	if _, err := dc.Put("k", make([]byte, 200)); err != nil {
		t.Fatalf("Put 200: %v", err)
	}
	if _, err := dc.Put("k", make([]byte, 50)); err != nil {
		t.Fatalf("Put 50: %v", err)
	}
	_, _, outBytes, outCount := bound.Stats()
	if outBytes != 50 {
		t.Errorf("update path leaked: outBytes=%d, want 50", outBytes)
	}
	if outCount != 1 {
		t.Errorf("update count leaked: outCount=%d, want 1", outCount)
	}
}
