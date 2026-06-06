package vtstorageadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	vtstoragecommon "github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage/common"
)

// raceFastpathStore is a thread-safe variant of fastpathStoreDefinitive
// for race tests. The single-test mocks in trace_index_fastpath_test.go
// race on the `got` field under concurrent LookupTraceIndex calls, so
// we use atomics here instead.
type raceFastpathStore struct {
	noopStore
	startNs    int64
	endNs      int64
	found      bool
	definitive bool

	lookups     int64 // atomic counter — definitive lookups
	legacy      int64 // atomic counter — legacy LookupTraceIndex
	runQueries  int64 // atomic counter — fallthrough RunQuery hits
	lastTraceID atomic.Value
}

func (s *raceFastpathStore) LookupTraceIndex(_ context.Context, traceID string) (int64, int64, bool, error) {
	atomic.AddInt64(&s.legacy, 1)
	s.lastTraceID.Store(traceID)
	return s.startNs, s.endNs, s.found, nil
}

func (s *raceFastpathStore) LookupTraceIndexFull(_ context.Context, traceID string) (int64, int64, bool, bool, error) {
	atomic.AddInt64(&s.lookups, 1)
	s.lastTraceID.Store(traceID)
	return s.startNs, s.endNs, s.found, s.definitive, nil
}

func (s *raceFastpathStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	atomic.AddInt64(&s.runQueries, 1)
	return nil
}

// TestRace_ConcurrentTraceByID_DefinitiveMiss spawns 64 goroutines
// hitting the same trace-by-id query for a definitively-missing ID.
// The race detector must stay clean — guards against an accidental
// cache or memoization layer in the short-circuit path mutating shared
// state without synchronization.
func TestRace_ConcurrentTraceByID_DefinitiveMiss(t *testing.T) {
	store := &raceFastpathStore{found: false, definitive: true}
	a := &Adapter{store: store}
	q := makeTraceIndexQuery(t, "definitely-not-here", 11)

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			wb := func(uint, *logstorage.DataBlock) {}
			err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, wb)
			if !errors.Is(err, vtstoragecommon.ErrOutOfRetention) {
				t.Errorf("RunQuery returned %v, want vtstoragecommon.ErrOutOfRetention", err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&store.lookups); got != goroutines {
		t.Errorf("LookupTraceIndexFull called %d times, want %d", got, goroutines)
	}
	if got := atomic.LoadInt64(&store.runQueries); got != 0 {
		t.Errorf("fallback RunQuery called %d times on definitive miss, want 0", got)
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
	hitStore := &raceFastpathStore{startNs: 1_000_000_000, endNs: 2_500_000_000, found: true, definitive: true}
	missStore := &raceFastpathStore{found: false, definitive: true}
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
			if !errors.Is(err, vtstoragecommon.ErrOutOfRetention) {
				t.Errorf("miss flow err: %v, want ErrOutOfRetention", err)
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
	store := &raceFastpathStore{found: false, definitive: true}
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
				if !errors.Is(err, vtstoragecommon.ErrOutOfRetention) {
					t.Errorf("index-query goroutine err=%v, want ErrOutOfRetention", err)
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
