package smartcache

import (
	"runtime"
	"sync"
	"testing"
)

const (
	mb256 = 256 * 1024 * 1024 // 256 MB
)

func TestCachePolicy_FirstAccessGoesToL2Only(t *testing.T) {
	p := CachePolicy{HitsThreshold: 2, BypassThreshold: mb256}
	if p.ShouldPromoteToL1(1) {
		t.Error("first access (count=1) should not promote to L1 when threshold=2")
	}
}

func TestCachePolicy_SecondAccessPromotesToL1(t *testing.T) {
	p := CachePolicy{HitsThreshold: 2, BypassThreshold: mb256}
	if !p.ShouldPromoteToL1(2) {
		t.Error("second access (count=2) should promote to L1 when threshold=2")
	}
}

func TestCachePolicy_LargeQueryBypassesCache(t *testing.T) {
	p := CachePolicy{HitsThreshold: 2, BypassThreshold: mb256}
	largeQuery := int64(300 * 1024 * 1024) // 300 MB
	if !p.ShouldBypassL1(largeQuery) {
		t.Error("300MB query should bypass L1 when threshold is 256MB")
	}
}

func TestCachePolicy_SmallQueryDoesNotBypass(t *testing.T) {
	p := CachePolicy{HitsThreshold: 2, BypassThreshold: mb256}
	smallQuery := int64(100 * 1024 * 1024) // 100 MB
	if p.ShouldBypassL1(smallQuery) {
		t.Error("100MB query should not bypass L1 when threshold is 256MB")
	}
}

func TestCachePolicy_ZeroThreshold_AlwaysPromotes(t *testing.T) {
	p := CachePolicy{HitsThreshold: 0, BypassThreshold: mb256}
	for _, count := range []int{0, 1, 100} {
		if !p.ShouldPromoteToL1(count) {
			t.Errorf("zero threshold should always promote, got false for accessCount=%d", count)
		}
	}
}

func TestCachePolicy_ZeroBypass_NeverBypasses(t *testing.T) {
	p := CachePolicy{HitsThreshold: 2, BypassThreshold: 0}
	for _, size := range []int64{0, 1, mb256, int64(1024 * 1024 * 1024)} {
		if p.ShouldBypassL1(size) {
			t.Errorf("zero bypass threshold should never bypass, got true for queryBytes=%d", size)
		}
	}
}

func TestCachePolicy_ExactThreshold_Promotes(t *testing.T) {
	p := CachePolicy{HitsThreshold: 5, BypassThreshold: mb256}
	if !p.ShouldPromoteToL1(5) {
		t.Error("accessCount == threshold should promote (>=)")
	}
}

func TestCachePolicy_ExactBypassThreshold_DoesNotBypass(t *testing.T) {
	p := CachePolicy{HitsThreshold: 2, BypassThreshold: mb256}
	if p.ShouldBypassL1(mb256) {
		t.Error("queryBytes == bypass threshold should NOT bypass (strictly >)")
	}
}

func TestCachePolicy_NegativeThreshold_AlwaysPromotes(t *testing.T) {
	p := CachePolicy{HitsThreshold: -1, BypassThreshold: mb256}
	for _, count := range []int{-10, 0, 1, 100} {
		if !p.ShouldPromoteToL1(count) {
			t.Errorf("negative threshold should always promote, got false for accessCount=%d", count)
		}
	}
}

func TestCachePolicy_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 10_000; i++ {
		p := CachePolicy{HitsThreshold: 2, BypassThreshold: mb256}
		p.ShouldPromoteToL1(i % 3)
		p.ShouldBypassL1(int64(i) * 1024)
	}

	runtime.GC()
	after := runtime.NumGoroutine()

	// Allow a small delta for runtime fluctuations.
	if delta := after - before; delta > 10 {
		t.Errorf("goroutine leak: before=%d after=%d delta=%d", before, after, delta)
	}
}

func TestCachePolicy_NoMemoryLeak(t *testing.T) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < 100_000; i++ {
		p := CachePolicy{HitsThreshold: i%10 + 1, BypassThreshold: int64(i) * 1024}
		p.ShouldPromoteToL1(i)
		p.ShouldBypassL1(int64(i) * 2048)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	growthBytes := int64(after.Alloc) - int64(before.Alloc)
	tenMB := int64(10 * 1024 * 1024)
	if growthBytes > tenMB {
		t.Errorf("memory leak: growth=%dMB exceeds 10MB limit", growthBytes/(1024*1024))
	}
}

func TestCachePolicy_ConcurrentAccess(t *testing.T) {
	p := CachePolicy{HitsThreshold: 3, BypassThreshold: mb256}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				p.ShouldPromoteToL1(n + j)
				p.ShouldBypassL1(int64(n+j) * 1024)
			}
		}(i)
	}
	wg.Wait()
}
