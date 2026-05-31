package resourcebounds

// Benchmarks for the bound Acquire/Release hot paths. Run with:
//
//   GOWORK=off go test -bench=Benchmark -benchmem -benchtime=2s ./internal/resourcebounds/
//
// These exist so PRs that touch the bound's hot path can be reviewed
// against a baseline (PR #97 set the foundation; this PR adds the
// runtime wiring that touches Acquire/TryAcquire on every cache.Put
// and every file-worker admission — both surfaces are sensitive to
// ns/op regressions).

import (
	"context"
	"testing"
)

// BenchmarkAcquire_Hot — Acquire + Release on an unbounded gate
// (Limit=0 disables byte accounting). Measures the closure-allocation
// + mutex + cond.Broadcast cost.
func BenchmarkAcquire_Hot(b *testing.B) {
	bound := NewBound(Config{Request: 1, Limit: 0, LimitCount: 0, Policy: Fixed}, nil)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rel, err := bound.Acquire(ctx, 1)
		if err != nil {
			b.Fatal(err)
		}
		rel()
	}
}

// BenchmarkAcquire_Bounded — Acquire + Release on a bounded gate
// where every acquire fits. Measures the count-gate check overhead.
func BenchmarkAcquire_Bounded(b *testing.B) {
	bound := NewBound(Config{Request: 1, Limit: 1024 * 1024, LimitCount: 1024 * 1024, Policy: Fixed}, nil)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rel, err := bound.Acquire(ctx, 1)
		if err != nil {
			b.Fatal(err)
		}
		rel()
	}
}

// BenchmarkTryAcquire_Bounded — TryAcquire fast path on a bounded
// gate. This is the surface the cache.Put and disk.Put hot paths use.
func BenchmarkTryAcquire_Bounded(b *testing.B) {
	bound := NewBound(Config{Request: 1, Limit: 1024 * 1024, LimitCount: 1024 * 1024, Policy: Fixed}, nil)
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

// BenchmarkTryAcquire_Rejected — TryAcquire on a full bound. Measures
// the rejection path overhead (no closure allocation since we return
// the noop release sentinel).
func BenchmarkTryAcquire_Rejected(b *testing.B) {
	bound := NewBound(Config{Request: 1, Limit: 1, LimitCount: 1, Policy: Fixed}, nil)
	// Fill the bound.
	_, _ = bound.TryAcquire(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bound.TryAcquire(1) // rejected with ErrBoundFull
	}
}
