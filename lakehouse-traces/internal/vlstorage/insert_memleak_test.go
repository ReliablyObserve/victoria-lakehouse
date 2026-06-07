package vlstorage

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestMemLeak_SustainedIngest_NoCachedRowGrowth pushes synthetic span
// rows through logRowsToTraceRows in tight repeated iterations and
// verifies the heap doesn't grow unboundedly across iterations.
//
// Rationale: catches the class of bug we hit on the logs side where
// dropping strings.Clone left dangling references into VL's arena that
// pinned per-batch memory until the next allocation, defeating GC.
// Budget: 10 MB across all iterations (matches the budget convention
// in this package's other memleak tests).
func TestMemLeak_SustainedIngest_NoCachedRowGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memleak test in -short mode")
	}

	const iterations = 5
	const rowsPerIter = 20_000 // 5 * 20k = 100k total

	// Warm-up so any one-shot init (pools, sync.Once, schema bootstrap)
	// is amortized before the first sample.
	lr := buildSyntheticLogRows(rowsPerIter)
	_ = logRowsToTraceRows(lr)
	logstorage.PutLogRows(lr)

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	for it := 0; it < iterations; it++ {
		lr := buildSyntheticLogRows(rowsPerIter)
		rows := logRowsToTraceRows(lr)
		if len(rows) != rowsPerIter {
			t.Fatalf("iter %d: got %d rows, want %d", it, len(rows), rowsPerIter)
		}
		// Drop references so GC can reclaim.
		rows = nil
		_ = rows
		logstorage.PutLogRows(lr)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Sustained ingest: heap_before=%dKB, heap_after=%dKB, iterations=%d, rows_per_iter=%d",
		heapBefore/1024, heapAfter/1024, iterations, rowsPerIter)

	const maxGrowth = uint64(10 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d ingest iterations (budget=10MB)",
			heapBefore/1024, heapAfter/1024, iterations)
	}
}

// buildSyntheticLogRows returns a populated *logstorage.LogRows with
// n trace-style rows. Mirrors the field shape used by
// BenchmarkLogRowsToTraceRows so the cost profile matches real ingest.
func buildSyntheticLogRows(n int) *logstorage.LogRows {
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		lr.MustAdd(logstorage.TenantID{}, int64(i)*1_000_000_000, []logstorage.Field{
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
