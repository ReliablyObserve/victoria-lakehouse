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

// TestSplitAndEmitDataBlock_ChunksLargeBlocks is the traces-module mirror of
// the logs-side unit guard for the chunked emission path. See the logs
// equivalent for the contract; this exists so a one-sided fix on logs cannot
// pass CI while the traces module silently regresses (per
// feedback_logs_traces_module_parity).
func TestSplitAndEmitDataBlock_ChunksLargeBlocks(t *testing.T) {
	const totalRows = 9000
	const chunkRows = 4096
	values := make([]string, totalRows)
	for i := range values {
		values[i] = fmt.Sprintf("row%d", i)
	}
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_msg", Values: values},
		{Name: "service.name", Values: values},
	})

	var emittedRows int
	var chunkCount int
	var maxChunkRows int
	splitAndEmitDataBlock(db, chunkRows, func(chunk *logstorage.DataBlock) {
		chunkCount++
		n := chunk.RowsCount()
		if n > maxChunkRows {
			maxChunkRows = n
		}
		emittedRows += n
		cols := chunk.GetColumns(false)
		if len(cols) != 2 {
			t.Errorf("chunk has %d columns, want 2", len(cols))
		}
		for _, c := range cols {
			if len(c.Values) != n {
				t.Errorf("chunk column %q has %d values for %d rows", c.Name, len(c.Values), n)
			}
		}
	})
	if emittedRows != totalRows {
		t.Errorf("emittedRows=%d, want %d", emittedRows, totalRows)
	}
	if maxChunkRows > chunkRows {
		t.Errorf("chunk size %d exceeded cap %d", maxChunkRows, chunkRows)
	}
	wantChunks := (totalRows + chunkRows - 1) / chunkRows
	if chunkCount != wantChunks {
		t.Errorf("chunkCount=%d, want %d", chunkCount, wantChunks)
	}
}

// TestAcquireRGDecode_BoundsConcurrency is the traces-module mirror of the
// logs-side semaphore guard. The traces module shares the same memory-safety
// invariants for wildcard queries over wide schemas (trace span attributes
// are stored as MAP columns, same shape as log resource attributes).
func TestAcquireRGDecode_BoundsConcurrency(t *testing.T) {
	const goroutines = 64
	var inFlight atomic.Int64
	var peak atomic.Int64
	var done atomic.Int64

	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			release := acquireRGDecode()
			defer release()
			cur := inFlight.Add(1)
			for {
				prev := peak.Load()
				if cur <= prev || peak.CompareAndSwap(prev, cur) {
					break
				}
			}
			<-start
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			done.Add(1)
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(start)
	for done.Load() < int64(goroutines) {
		time.Sleep(5 * time.Millisecond)
	}

	maxAllowed := int64(cap(rgDecodeSem))
	if peak.Load() > maxAllowed {
		t.Fatalf("peak concurrent decoders = %d, semaphore cap = %d", peak.Load(), maxAllowed)
	}
	t.Logf("peak concurrent decoders=%d, cap=%d", peak.Load(), maxAllowed)
}

// TestRunQuery_ProductionShape_WildcardScalesUnderMemoryBudget is the
// traces-module mirror of the PRODUCTION-SHAPE OOM regression lock.
// See the logs-module equivalent for the full rationale. Trace wildcard
// queries hit the same MAP-column heavy decode path as log queries
// (span_attr:* is stored as MAP columns same shape as resource.attributes),
// so the memory-bound failure mode is identical and must be locked
// against in both modules per feedback_logs_traces_module_parity.
func TestRunQuery_ProductionShape_WildcardScalesUnderMemoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("production-shape memory test takes ~10-30s and exercises real S3 codepaths")
	}

	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Query.FileWorkers = 16
	s.cfg.Query.MaxLiveBytes = 64 * 1024 * 1024

	const filesCount = 200
	const rowsPerFile = 5000
	const totalRowsExpected = filesCount * rowsPerFile

	t.Logf("WORKLOAD-SHAPE: filesCount=%d rowsPerFile=%d totalRows=%d fileWorkers=%d liveBudget=%dMiB "+
		"(production-shape: matches the 2-day wildcard cardinality the user reported in Grafana)",
		filesCount, rowsPerFile, totalRowsExpected, s.cfg.Query.FileWorkers, s.cfg.Query.MaxLiveBytes/(1024*1024))

	baseTime := time.Date(2026, 5, 28, 14, 30, 0, 0, time.UTC)
	for fileIdx := 0; fileIdx < filesCount; fileIdx++ {
		rows := make([]logRow, rowsPerFile)
		fileTime := baseTime.Add(time.Duration(fileIdx) * time.Minute)
		for i := 0; i < rowsPerFile; i++ {
			rows[i] = logRow{
				TimestampUnixNano: fileTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
				Body: fmt.Sprintf("file=%d row=%d realistic structured log body with payload-like content "+
					"that simulates production log volume", fileIdx, i),
				SeverityText: "INFO",
				ServiceName:  fmt.Sprintf("svc-%d", fileIdx%8),
			}
		}
		data := writeParquetToBytes(t, rows)
		key := fmt.Sprintf("logs/dt=2026-05-28/hour=%02d/prod_shape_%04d.parquet",
			fileTime.Hour(), fileIdx)
		registerFileInMockS3(t, s, mock, key, data, fileTime)
	}

	startNs := baseTime.Add(-time.Hour).UnixNano()
	endNs := baseTime.Add(48 * time.Hour).UnixNano()
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	budgetBefore := getCounterValue(t, metrics.QueryMemoryBudgetExceeded)

	var heapBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&heapBefore)

	var peakHeap atomic.Int64
	var maxBlockBytes atomic.Int64
	var totalRows atomic.Int64

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
			if cur > peakHeap.Load() {
				peakHeap.Store(cur)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		totalRows.Add(int64(db.RowsCount()))
		bz := dataBlockApproxBytes(db)
		for {
			prev := maxBlockBytes.Load()
			if bz <= prev || maxBlockBytes.CompareAndSwap(prev, bz) {
				break
			}
		}
		time.Sleep(250 * time.Microsecond)
	})

	close(stop)
	<-done

	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	budgetAfter := getCounterValue(t, metrics.QueryMemoryBudgetExceeded)

	const maxPeakHeapBytes = int64(384 * 1024 * 1024)
	if peakHeap.Load() > maxPeakHeapBytes {
		t.Fatalf("TRACES PRODUCTION-SHAPE REGRESSION: peak heap growth = %.1f MiB exceeded the "+
			"%.0f MiB ceiling on a %d-file × %d-row wildcard scan.",
			float64(peakHeap.Load())/(1024*1024),
			float64(maxPeakHeapBytes)/(1024*1024),
			filesCount, rowsPerFile)
	}

	if maxBlockBytes.Load() > 0 {
		queueDepth := float64(peakHeap.Load()) / float64(maxBlockBytes.Load())
		const maxQueueDepth = float64(64)
		if queueDepth > maxQueueDepth {
			t.Fatalf("in-flight queue depth = %.1fx max-block-size (%.0f MiB peak / %.1f MiB block); "+
				"want at most %.0fx",
				queueDepth,
				float64(peakHeap.Load())/(1024*1024),
				float64(maxBlockBytes.Load())/(1024*1024),
				maxQueueDepth)
		}
	}

	if budgetAfter == budgetBefore && totalRows.Load() != int64(totalRowsExpected) {
		t.Fatalf("budget did not fire AND row count is wrong: got %d, want %d",
			totalRows.Load(), totalRowsExpected)
	}

	t.Logf("RESULT: peak_heap=%.1f MiB max_block=%.2f MiB rows_returned=%d/%d budget_fired=%v",
		float64(peakHeap.Load())/(1024*1024),
		float64(maxBlockBytes.Load())/(1024*1024),
		totalRows.Load(), totalRowsExpected,
		budgetAfter > budgetBefore)
}
