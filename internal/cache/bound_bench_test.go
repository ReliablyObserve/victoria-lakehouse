package cache

// Benchmarks comparing pre-bound and post-bound runtime cost of cache
// Puts. Run with:
//
//   GOWORK=off go test -bench=BenchmarkCachePut -benchmem ./internal/cache/
//
// The bound's TryAcquire adds an atomic+mutex operation on the cache
// admission path. These benchmarks pin the delta so PRs that change
// the bound's hot path can be reviewed against a baseline.

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
)

// BenchmarkCachePut_NoBound is the pre-PR baseline — LRU.Put without
// any bound wired in.
func BenchmarkCachePut_NoBound(b *testing.B) {
	c := NewLRU(100 * 1024 * 1024)
	val := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(fmt.Sprintf("key-%d", i%10000), val)
	}
}

// BenchmarkCachePut_WithBound exercises the bound-wired path. Bound
// limit is large enough that no admit rejection occurs — measures the
// bound's TryAcquire fast path overhead.
func BenchmarkCachePut_WithBound(b *testing.B) {
	c := NewLRU(100 * 1024 * 1024)
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 200 * 1024 * 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	c.SetBound(bound)
	val := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(fmt.Sprintf("key-%d", i%10000), val)
	}
}

// BenchmarkCachePut_WithBoundRejection exercises the bound's rejection
// path — every Put past the limit takes the ErrBoundFull path,
// incrementing the rejected counter atomically.
func BenchmarkCachePut_WithBoundRejection(b *testing.B) {
	c := NewLRU(100 * 1024 * 1024)
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	c.SetBound(bound)
	val := make([]byte, 1024)
	// Fill the bound to capacity first.
	c.Put("seed", val)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(fmt.Sprintf("key-%d", i), val) // rejected
	}
}

// BenchmarkTryAcquire_Hot — direct measurement of TryAcquire's fast
// path (admit + release).
func BenchmarkTryAcquire_Hot(b *testing.B) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1024 * 1024, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rel, err := bound.TryAcquire(1)
		if err != nil {
			b.Fatal(err)
		}
		rel()
	}
}
