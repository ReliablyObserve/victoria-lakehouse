package vlstorage

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// TestMemLeak_SustainedIngest_NoCachedRowGrowth converts the spans of an insert
// buffer through DataBlockToTraceRows in tight repeated iterations and verifies
// the heap doesn't grow across iterations.
//
// Rationale: catches the class of bug where dropping strings.Clone leaves
// dangling references into the engine's block memory that pin per-batch memory
// until the next allocation, defeating GC.
// Budget: 10 MB across all iterations (matches the budget convention
// in this package's other memleak tests).
func TestMemLeak_SustainedIngest_NoCachedRowGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memleak test in -short mode")
	}

	const iterations = 5
	const rowsPerIter = 20_000 // 5 * 20k = 100k converted rows in total

	st, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	lr := buildSyntheticLogRows(rowsPerIter)
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()
	q, err := logstorage.ParseQueryAtTimestamp("*", math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), 0, math.MaxInt64)
	convert := func() int {
		n := 0
		var mu sync.Mutex
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{{}}, q, false, nil)
		if err := st.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			rows := DataBlockToTraceRows(db, logstorage.TenantID{})
			mu.Lock()
			n += len(rows)
			mu.Unlock()
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Warm-up so any one-shot init (pools, sync.Once, schema bootstrap)
	// is amortized before the first sample.
	if n := convert(); n != rowsPerIter {
		t.Fatalf("warm-up converted %d rows, want %d", n, rowsPerIter)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	for it := 0; it < iterations; it++ {
		if n := convert(); n != rowsPerIter {
			t.Fatalf("iter %d: got %d rows, want %d", it, n, rowsPerIter)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Sustained conversion: heap_before=%dKB, heap_after=%dKB, iterations=%d, rows_per_iter=%d",
		heapBefore/1024, heapAfter/1024, iterations, rowsPerIter)

	const maxGrowth = uint64(10 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d iterations (budget=10MB)",
			heapBefore/1024, heapAfter/1024, iterations)
	}
}

// buildSyntheticLogRows returns a populated *logstorage.LogRows with
// n trace-style rows. Mirrors the field shape used by
// BenchmarkDataBlockToTraceRows so the cost profile matches real ingest.
func buildSyntheticLogRows(n int) *logstorage.LogRows {
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		lr.MustAdd(logstorage.TenantID{}, int64(i+1)*1_000_000_000, []logstorage.Field{
			{Name: "trace_id", Value: fmt.Sprintf("abc123def456-%d", i)},
			{Name: "span_id", Value: fmt.Sprintf("span-%d", i)},
			{Name: "service.name", Value: "benchmark-svc"},
			{Name: "span.name", Value: "GET /api/benchmark"},
			{Name: "duration_ns", Value: "5000000"},
			{Name: "k8s.namespace.name", Value: "prod"},
			{Name: "custom_field_1", Value: "value1"},
			{Name: "custom_field_2", Value: "value2"},
		}, -1)
	}
	return lr
}
