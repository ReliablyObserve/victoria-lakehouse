package vtstorageadapter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestStress_AdapterRunQuery_HighConcurrency fires 256 goroutines each
// running 100 queries — a mix of definitive-miss, hit, and non-index
// plain queries — through the same Adapter. Must complete inside the
// 60s deadline with zero unexpected errors. Surfaces serialization
// bottlenecks or starvation in RunQuery that wouldn't be caught by the
// smaller race tests.
func TestStress_AdapterRunQuery_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress in -short mode")
	}

	// Two stores: one for hits (so emitTraceIndexBlock fires every
	// iteration), one for definitive misses. The plain-query path
	// only touches the hit-store's RunQuery method, exercising the
	// non-fast-path branch.
	hitStore := &raceFastpathStore{startNs: 1_000, endNs: 2_000, found: true}
	missStore := &raceFastpathStore{found: false}
	aHit := &Adapter{store: hitStore}
	aMiss := &Adapter{store: missStore}

	hitQ := makeTraceIndexQuery(t, "hot", 1)
	missQ := makeTraceIndexQuery(t, "cold", 2)
	plainQ, err := logstorage.ParseQueryAtTimestamp(`service.name:="api"`, 1)
	if err != nil {
		t.Fatalf("ParseQueryAtTimestamp: %v", err)
	}

	const goroutines = 256
	const perGoroutine = 100

	var errCount int64
	deadline := time.Now().Add(60 * time.Second)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			wb := func(uint, *logstorage.DataBlock) {}
			for i := 0; i < perGoroutine; i++ {
				if time.Now().After(deadline) {
					atomic.AddInt64(&errCount, 1)
					t.Errorf("goroutine %d hit 60s deadline at iter %d — serialization bottleneck?", g, i)
					return
				}
				kind := (g + i) % 3
				switch kind {
				case 0:
					if err := aHit.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: hitQ}, wb); err != nil {
						atomic.AddInt64(&errCount, 1)
						t.Errorf("hit RunQuery: %v", err)
					}
				case 1:
					err := aMiss.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: missQ}, wb)
					if err != nil {
						atomic.AddInt64(&errCount, 1)
						t.Errorf("miss RunQuery: %v, want nil", err)
					}
				case 2:
					if err := aHit.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: plainQ}, wb); err != nil {
						atomic.AddInt64(&errCount, 1)
						t.Errorf("plain RunQuery: %v", err)
					}
				}
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&errCount); got != 0 {
		t.Fatalf("stress test produced %d errors", got)
	}
}
