package vtstorageadapter

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	vtstoragecommon "github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage/common"
)

// TestMemLeak_TraceByID_DefinitiveMissChurn walks the definitive-miss
// short-circuit 100_000 times with distinct trace IDs. A hidden cache
// or per-call accumulator in the fast path would show up as a
// monotonic heap growth past the 10 MB budget.
func TestMemLeak_TraceByID_DefinitiveMissChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memleak in -short mode")
	}
	store := &raceFastpathStore{found: false, definitive: true}
	a := &Adapter{store: store}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	wb := func(uint, *logstorage.DataBlock) {}
	const N = 100_000
	for i := 0; i < N; i++ {
		// Distinct trace IDs to defeat any naive query-string interning.
		q := makeTraceIndexQuery(t, fmt.Sprintf("missing-%d", i), uint64(i&0xff))
		err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, wb)
		if !errors.Is(err, vtstoragecommon.ErrOutOfRetention) {
			t.Fatalf("RunQuery iter=%d returned %v, want ErrOutOfRetention", i, err)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("trace-by-id definitive-miss churn: heap_before=%dKB, heap_after=%dKB, iterations=%d",
		heapBefore/1024, heapAfter/1024, N)

	const maxGrowth = uint64(10 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("possible memory leak in trace-index fast path: heap grew from %dKB to %dKB after %d lookups",
			heapBefore/1024, heapAfter/1024, N)
	}
}

// TestMemLeak_RewriteTraceIndexQuery_ParseChurn exercises the
// query-clone + re-parse path in rewriteTraceIndexQuery. The rewrite
// allocates a new *logstorage.Query per call (ParseQueryAtTimestamp on
// the rewritten string); we want to confirm those temporaries are
// reclaimable by GC, not held by any package-level cache.
func TestMemLeak_RewriteTraceIndexQuery_ParseChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memleak in -short mode")
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const N = 100_000
	for i := 0; i < N; i++ {
		q := makeTraceIndexQuery(t, fmt.Sprintf("id-%d", i), uint64(i&0xff))
		rewritten, ok := rewriteTraceIndexQuery(q)
		if !ok {
			t.Fatalf("rewriteTraceIndexQuery returned ok=false at iter=%d", i)
		}
		if rewritten == nil {
			t.Fatalf("rewriteTraceIndexQuery returned nil at iter=%d", i)
		}
		// Don't keep references — let GC reclaim.
		_ = rewritten.String()
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("rewriteTraceIndexQuery churn: heap_before=%dKB, heap_after=%dKB, iterations=%d",
		heapBefore/1024, heapAfter/1024, N)

	const maxGrowth = uint64(10 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("possible memory leak in rewriteTraceIndexQuery: heap grew from %dKB to %dKB after %d rewrites",
			heapBefore/1024, heapAfter/1024, N)
	}
}
