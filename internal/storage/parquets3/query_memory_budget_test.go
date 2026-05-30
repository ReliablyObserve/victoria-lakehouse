package parquets3

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// TestDataBlockApproxBytes_BasicAccounting confirms the helper walks the
// actual Values slice instead of guessing from row count, which is what
// makes the wildcard wide-schema case account correctly.
func TestDataBlockApproxBytes_BasicAccounting(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_msg", Values: []string{"hello", "world", "foo"}},
		{Name: "service.name", Values: []string{"api", "api", "worker"}},
	})

	got := dataBlockApproxBytes(db)
	// Values: 5+5+3+3+3+6 = 25 bytes of strings
	// Names: 4 + 12 = 16
	// Per-column overhead: 2*16 = 32
	// Total: 73
	const want = int64(73)
	if got != want {
		t.Fatalf("dataBlockApproxBytes = %d, want %d", got, want)
	}

	if dataBlockApproxBytes(nil) != 0 {
		t.Fatal("nil DataBlock should be 0 bytes")
	}
}

// TestRunQuery_WildcardManyFiles_MemoryBudget is the OOM regression lock.
// It builds a many-file (>=20) scenario, fires a wildcard `*` query that
// touches every column of every row group in every file with a slow
// writeBlock consumer (250µs/block — simulates the LogsQL pipe + HTTP
// writer chain), and asserts:
//
//   - the query completes without panic
//   - per-process heap growth stays under a generous ceiling (the bug
//     before the fix grew the heap by gigabytes within a couple of seconds;
//     this test asserts the post-fix bound)
//   - either all rows are returned OR the QueryMemoryBudgetExceeded counter
//     incremented (controlled cancellation, not OOM)
//
// If the fix is reverted (the resultCh:256 channel + unconstrained workers
// with no per-query byte budget) the heap growth assertion fails because
// 20 wide files × 2k rows × wildcard projection × 16 workers freely queue
// 256 multi-MB blocks ahead of the slow consumer, blowing past the 384 MiB
// ceiling we set here.
func TestRunQuery_WildcardManyFiles_MemoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("memory-budget test takes a few seconds and exercises real S3 codepaths")
	}

	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Query.FileWorkers = 16
	s.cfg.Query.MaxLiveBytes = 64 * 1024 * 1024 // 64 MiB live budget — small to make the budget fire

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	const filesCount = 30
	const rowsPerFile = 5000

	for fileIdx := 0; fileIdx < filesCount; fileIdx++ {
		rows := make([]logRow, rowsPerFile)
		for i := 0; i < rowsPerFile; i++ {
			rows[i] = logRow{
				TimestampUnixNano: now.Add(time.Duration(fileIdx*rowsPerFile+i) * time.Microsecond).UnixNano(),
				// Wider body to simulate real log volume — pushes block size up so
				// the 256-deep result-channel buffer pattern (pre-fix) actually
				// pins multi-MB of live DataBlock memory ahead of the slow consumer.
				Body: fmt.Sprintf("file=%d row=%d %s %s %s %s",
					fileIdx, i,
					"log message body that is reasonably long to simulate real log volume",
					"with realistic structured payload-like content woven in",
					"so the parquet-decoded blocks carry the actual byte weight",
					"that triggers the OOM in the live container"),
				SeverityText: "INFO",
				ServiceName:  fmt.Sprintf("svc-%d", fileIdx%4),
			}
		}
		data := writeParquetToBytes(t, rows)
		key := fmt.Sprintf("logs/dt=2026-05-10/hour=14/wildcard_oom_%03d.parquet", fileIdx)
		registerFileInMockS3(t, s, mock, key, data, now)
	}

	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(24 * time.Hour).UnixNano()
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	budgetBefore := getCounterValue(t, metrics.QueryMemoryBudgetExceeded)

	var heapBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&heapBefore)

	var totalRows atomic.Int64
	var peakLive atomic.Int64

	// Sample heap allocations during the query to capture the peak.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&ms)
			cur := int64(ms.HeapAlloc) - int64(heapBefore.HeapAlloc)
			if cur > peakLive.Load() {
				peakLive.Store(cur)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Slow consumer — simulates the LogsQL pipe + HTTP writer chain in
	// production. Without backpressure on the producer side, this lets
	// workers freely queue blocks ahead of consumption, which is exactly
	// how the OOM materialises in the live container.
	var maxBlockBytes atomic.Int64
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		totalRows.Add(int64(db.RowsCount()))
		bz := dataBlockApproxBytes(db)
		if bz > maxBlockBytes.Load() {
			maxBlockBytes.Store(bz)
		}
		// Slow consumer (1ms/block) — emulates LogsQL pipe + HTTP JSON writer.
		// Without backpressure on the producer side, this lets workers freely
		// queue blocks ahead of the consumer.
		time.Sleep(1 * time.Millisecond)
	})

	close(stop)
	<-done

	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	budgetAfter := getCounterValue(t, metrics.QueryMemoryBudgetExceeded)

	// The contract: either we returned all rows, or we triggered the budget
	// (controlled cancellation), but we MUST NOT have ballooned the heap.
	// 256 MiB is comfortably above the 64 MiB live budget plus parquet
	// decode scratch space, but well below what the channel-buffered
	// pattern would queue (256 blocks × multi-MB each = >>1 GiB).
	const maxAllowedHeapGrowthBytes = int64(256 * 1024 * 1024) // 256 MiB
	if peakLive.Load() > maxAllowedHeapGrowthBytes {
		t.Fatalf("heap growth %.1f MiB exceeded the regression ceiling %.1f MiB; "+
			"this is the resultCh:256 OOM regressing — check that filteredWriteBlock "+
			"is synchronous (wbMu.Lock) and that defaultMaxLiveBytes is being honoured",
			float64(peakLive.Load())/(1024*1024),
			float64(maxAllowedHeapGrowthBytes)/(1024*1024))
	}

	// Stronger contract: peak heap growth divided by max block size should
	// be on the order of (file_workers + 1), NOT 256 (the pre-fix channel
	// buffer depth). If this ratio is high it means blocks are being queued
	// faster than they're being consumed.
	if maxBlockBytes.Load() > 0 {
		blockMultiple := float64(peakLive.Load()) / float64(maxBlockBytes.Load())
		// Allow generous slack: workers (16) + decode scratch + parquet-go
		// internal buffers + GC headroom. 64 multiples = ~85 MiB at 1.3 MiB
		// blocks, well above the 16-worker bound but well under 256.
		const maxBlockMultiple = float64(64)
		if blockMultiple > maxBlockMultiple {
			t.Fatalf("peak heap = %.1fx max block size (%.0f MiB peak / %.1f MiB block), "+
				"want at most %.0fx; this suggests blocks are queueing in a deep buffer "+
				"instead of being consumed synchronously",
				blockMultiple,
				float64(peakLive.Load())/(1024*1024),
				float64(maxBlockBytes.Load())/(1024*1024),
				maxBlockMultiple)
		}
	}

	// If the budget fired, totalRows may be less than filesCount*rowsPerFile.
	// If it didn't fire, we should have all rows.
	if budgetAfter == budgetBefore && totalRows.Load() != int64(filesCount*rowsPerFile) {
		t.Fatalf("budget did not fire AND row count is wrong: got %d, want %d",
			totalRows.Load(), filesCount*rowsPerFile)
	}

	t.Logf("wildcard %d files × %d rows: returned=%d, peak_heap=%.1f MiB, max_block=%.2f MiB, budget_fired=%v",
		filesCount, rowsPerFile, totalRows.Load(),
		float64(peakLive.Load())/(1024*1024),
		float64(maxBlockBytes.Load())/(1024*1024),
		budgetAfter > budgetBefore)
}

// getCounterValue exposes a counter's current value for assertions.
func getCounterValue(t *testing.T, c *metrics.Counter) uint64 {
	t.Helper()
	if c == nil {
		return 0
	}
	return c.Get()
}
