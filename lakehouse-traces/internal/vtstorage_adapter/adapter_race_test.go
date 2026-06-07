package vtstorageadapter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// raceFastpathStore is a thread-safe traceIndexLookup mock for race
// tests. The single-test mocks in trace_index_fastpath_test.go race
// on the `got` field under concurrent LookupTraceIndex calls, so we
// use atomics here instead.
type raceFastpathStore struct {
	noopStore
	startNs int64
	endNs   int64
	found   bool

	lookups     int64 // atomic counter — LookupTraceIndex calls
	runQueries  int64 // atomic counter — fall-through RunQuery hits
	lastTraceID atomic.Value
}

func (s *raceFastpathStore) LookupTraceIndex(_ context.Context, traceID string) (int64, int64, bool, error) {
	atomic.AddInt64(&s.lookups, 1)
	s.lastTraceID.Store(traceID)
	return s.startNs, s.endNs, s.found, nil
}

func (s *raceFastpathStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	atomic.AddInt64(&s.runQueries, 1)
	return nil
}

// TestRace_ConcurrentTraceByID_Miss spawns 64 goroutines hitting the
// same trace-by-id query for a missing ID. On miss the adapter falls
// through to VT's natural rewriteTraceIndexQuery span-scan rewrite,
// so the store's RunQuery gets called once per goroutine. The race
// detector must stay clean.
func TestRace_ConcurrentTraceByID_Miss(t *testing.T) {
	store := &raceFastpathStore{found: false}
	a := &Adapter{store: store}
	q := makeTraceIndexQuery(t, "missing-from-footer", 11)

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			wb := func(uint, *logstorage.DataBlock) {}
			if err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, wb); err != nil {
				t.Errorf("RunQuery returned %v, want nil", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&store.lookups); got != goroutines {
		t.Errorf("LookupTraceIndex called %d times, want %d", got, goroutines)
	}
	// Miss falls through to span-scan rewrite, which lands on RunQuery.
	if got := atomic.LoadInt64(&store.runQueries); got != goroutines {
		t.Errorf("fall-through RunQuery called %d times, want %d", got, goroutines)
	}
}

// TestRace_MixedHitMiss_ConcurrentQueries runs hit and miss flows
// against the same adapter concurrently. The store is shared so any
// cross-goroutine state mutation in the fast path is caught by -race.
func TestRace_MixedHitMiss_ConcurrentQueries(t *testing.T) {
	// Two adapters share the same noopStore-like backing but expose
	// different lookup outcomes. Adapter must keep the two flows
	// isolated — there's nothing today that couples them, this test
	// pins that.
	hitStore := &raceFastpathStore{startNs: 1_000_000_000, endNs: 2_500_000_000, found: true}
	missStore := &raceFastpathStore{found: false}
	aHit := &Adapter{store: hitStore}
	aMiss := &Adapter{store: missStore}

	hitQ := makeTraceIndexQuery(t, "hit-trace", 1)
	missQ := makeTraceIndexQuery(t, "miss-trace", 2)

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			wb := func(uint, *logstorage.DataBlock) {}
			if i%2 == 0 {
				if err := aHit.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: hitQ}, wb); err != nil {
					t.Errorf("hit flow err: %v", err)
				}
				return
			}
			err := aMiss.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: missQ}, wb)
			if err != nil {
				t.Errorf("miss flow err: %v, want nil", err)
			}
		}()
	}
	wg.Wait()
}

// TestRace_NonIndexQueriesAlongside races the trace-by-id fast path
// against regular `service.name` queries through the same adapter to
// catch cross-contamination between the two code branches (e.g. a
// shared scratch buffer that the rewrite path reuses).
func TestRace_NonIndexQueriesAlongside(t *testing.T) {
	store := &raceFastpathStore{found: false}
	a := &Adapter{store: store}

	indexQ := makeTraceIndexQuery(t, "missing", 7)
	plainQ, err := logstorage.ParseQueryAtTimestamp(`service.name:="api-gateway"`, 1)
	if err != nil {
		t.Fatalf("ParseQueryAtTimestamp: %v", err)
	}

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			wb := func(uint, *logstorage.DataBlock) {}
			var qq *logstorage.Query
			if i%2 == 0 {
				qq = indexQ
			} else {
				qq = plainQ
			}
			err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: qq}, wb)
			if i%2 == 0 {
				if err != nil {
					t.Errorf("index-query goroutine err=%v, want nil", err)
				}
			} else {
				if err != nil {
					t.Errorf("plain-query goroutine err=%v, want nil", err)
				}
			}
		}()
	}
	wg.Wait()
}

// sanity: silence unused-import warning if a future refactor strips fmt.
var _ = fmt.Sprintf
