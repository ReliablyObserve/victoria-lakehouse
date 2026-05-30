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
// makes the wildcard wide-schema case account correctly. Mirror of the
// logs-module test — both modules now share the same memory-budget
// scaffolding and must move together.
func TestDataBlockApproxBytes_BasicAccounting(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_msg", Values: []string{"hello", "world", "foo"}},
		{Name: "service.name", Values: []string{"api", "api", "worker"}},
	})

	got := dataBlockApproxBytes(db)
	const want = int64(73)
	if got != want {
		t.Fatalf("dataBlockApproxBytes = %d, want %d", got, want)
	}

	if dataBlockApproxBytes(nil) != 0 {
		t.Fatal("nil DataBlock should be 0 bytes")
	}
}

// TestRunQuery_WildcardManyFiles_MemoryBudget is the traces-module mirror of
// the OOM regression lock in internal/storage/parquets3. See that test for
// the contract; this exists so a one-sided fix on logs cannot pass CI while
// the traces module silently regresses (per feedback_logs_traces_module_parity).
func TestRunQuery_WildcardManyFiles_MemoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("memory-budget test takes a few seconds and exercises real S3 codepaths")
	}

	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Query.FileWorkers = 16
	s.cfg.Query.MaxLiveBytes = 64 * 1024 * 1024

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	const filesCount = 30
	const rowsPerFile = 5000

	for fileIdx := 0; fileIdx < filesCount; fileIdx++ {
		rows := make([]logRow, rowsPerFile)
		for i := 0; i < rowsPerFile; i++ {
			rows[i] = logRow{
				TimestampUnixNano: now.Add(time.Duration(fileIdx*rowsPerFile+i) * time.Microsecond).UnixNano(),
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

	var maxBlockBytes atomic.Int64
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		totalRows.Add(int64(db.RowsCount()))
		bz := dataBlockApproxBytes(db)
		if bz > maxBlockBytes.Load() {
			maxBlockBytes.Store(bz)
		}
		time.Sleep(1 * time.Millisecond)
	})

	close(stop)
	<-done

	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	budgetAfter := getCounterValue(t, metrics.QueryMemoryBudgetExceeded)

	const maxAllowedHeapGrowthBytes = int64(256 * 1024 * 1024)
	if peakLive.Load() > maxAllowedHeapGrowthBytes {
		t.Fatalf("heap growth %.1f MiB exceeded the regression ceiling %.1f MiB",
			float64(peakLive.Load())/(1024*1024),
			float64(maxAllowedHeapGrowthBytes)/(1024*1024))
	}

	if maxBlockBytes.Load() > 0 {
		blockMultiple := float64(peakLive.Load()) / float64(maxBlockBytes.Load())
		const maxBlockMultiple = float64(64)
		if blockMultiple > maxBlockMultiple {
			t.Fatalf("peak heap = %.1fx max block size, want at most %.0fx",
				blockMultiple, maxBlockMultiple)
		}
	}

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

func getCounterValue(t *testing.T, c *metrics.Counter) uint64 {
	t.Helper()
	if c == nil {
		return 0
	}
	return c.Get()
}
