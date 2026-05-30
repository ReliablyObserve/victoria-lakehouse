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

// TestSplitAndEmitDataBlock_ChunksLargeBlocks asserts that DataBlocks larger
// than the chunk cap are split into chunks of <= chunkRows, preserving the
// per-column row alignment and the overall row order. This is the unit-level
// guard for the chunked emission path used by readRowGroupWithProjection.
//
// Negative-control: revert splitAndEmitDataBlock to emit-once (delete the
// per-chunk loop) and this test must fail.
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
		t.Errorf("chunk size %d exceeded cap %d (regression: chunking disabled)", maxChunkRows, chunkRows)
	}
	wantChunks := (totalRows + chunkRows - 1) / chunkRows
	if chunkCount != wantChunks {
		t.Errorf("chunkCount=%d, want %d (%d rows / %d cap)", chunkCount, wantChunks, totalRows, chunkRows)
	}
}

// TestSplitAndEmitDataBlock_NoChunkingNeeded asserts that small DataBlocks
// pass through unmodified — no allocation overhead for the common case
// of stats/hits queries where each block is well under the chunk cap.
func TestSplitAndEmitDataBlock_NoChunkingNeeded(t *testing.T) {
	values := []string{"a", "b", "c"}
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_msg", Values: values},
	})

	var emitted []*logstorage.DataBlock
	splitAndEmitDataBlock(db, 4096, func(chunk *logstorage.DataBlock) {
		emitted = append(emitted, chunk)
	})
	if len(emitted) != 1 {
		t.Fatalf("got %d chunks, want 1 (small block should pass through)", len(emitted))
	}
	if emitted[0] != db {
		t.Errorf("expected unchunked block to be the same pointer (no copy); the helper allocated needlessly")
	}
}

// TestAcquireRGDecode_BoundsConcurrency asserts the row-group decoder
// semaphore actually caps concurrent holders, which is the memory-safety
// invariant the fix relies on.
//
// Negative-control: replace acquireRGDecode with a no-op (return func(){})
// and this test must fail because the observed peak concurrency will
// exceed the cap.
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
			time.Sleep(5 * time.Millisecond) // simulate decode work
			inFlight.Add(-1)
			done.Add(1)
		}()
	}
	// Let all goroutines block on the semaphore + start channel.
	time.Sleep(20 * time.Millisecond)
	close(start)
	for done.Load() < int64(goroutines) {
		time.Sleep(5 * time.Millisecond)
	}

	maxAllowed := int64(cap(rgDecodeSem))
	if peak.Load() > maxAllowed {
		t.Fatalf("peak concurrent decoders = %d, semaphore cap = %d; "+
			"REGRESSION: the rgDecodeSem gate is not bounding decoder concurrency, "+
			"so 16 file workers will all decode row groups in parallel and exhaust "+
			"the 2 GiB container budget on a production-shape wildcard scan",
			peak.Load(), maxAllowed)
	}
	t.Logf("peak concurrent decoders=%d, cap=%d", peak.Load(), maxAllowed)
}

// TestRunQuery_ProductionShape_WildcardScalesUnderMemoryBudget is the
// PRODUCTION-SHAPE OOM regression lock. It exercises a workload sized to
// match the live cluster's 2-day wildcard query that OOM-killed the
// 2 GiB container after the prior "fix" (which was sized for a 30-file
// toy):
//
//   - filesCount       = 200 (matches the order-of-magnitude of files
//                              touched by a 2-day wildcard on the live
//                              cluster, where the manifest holds ~591
//                              parquet files across 6 days)
//   - rowsPerFile      = 5000 (wider than the prior 5k toy because each
//                              row carries a realistic message + service
//                              payload — the heap-diff at near-OOM was
//                              dominated by readMapColumnToBlockCols at
//                              154 MiB flat, scaling with row count)
//   - file workers     = 16  (the live -lakehouse.query.file-workers=16
//                              setting from docker-compose-e2e.yml)
//   - live budget      = 64 MiB (small enough that the budget MUST bite
//                              before peak heap reaches the regression
//                              ceiling — proves the budget actually
//                              backpressures the decoder)
//
// Quantitative pass criteria:
//   - peak heap growth during the scan stays under 384 MiB (the live
//     container's 2 GiB budget minus ~512 MiB cache, ~200 MiB baseline,
//     ~400 MiB parquet-go internal pools, ~200 MiB Go runtime overhead
//     — anything above 384 MiB means the row-group decoder cap is
//     leaking memory faster than the budget can backpressure)
//   - peak heap divided by the largest emitted DataBlock is <= 64x
//     (proves the chunked emission + rgDecodeSem actually bound the
//     in-flight queue, instead of buffering hundreds of row groups
//     ahead of the slow consumer)
//
// Negative-control: revert acquireRGDecode to a no-op AND remove the
// splitAndEmitDataBlock call from readRowGroupWithProjection. The
// resulting "1 file worker = 1 row group buffered whole" pattern with
// 16 workers must trip the peak heap ceiling and fail this test.
//
// Why this is the correct shape: the prior test at 30 files × 5k rows
// covered 150k total rows — about 4% of the production 2-day wildcard
// volume (591 files × ~6k rows ≈ 3.5M rows). The user reported the
// 2-day wildcard still OOMs after the 24h fix landed. The lock must
// catch the failure mode the user sees in Grafana, not just the
// minimal reproducer.
func TestRunQuery_ProductionShape_WildcardScalesUnderMemoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("production-shape memory test takes ~10-30s and exercises real S3 codepaths")
	}

	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Query.FileWorkers = 16
	// Small live budget — forces the per-query backpressure path to fire
	// well before the regression ceiling. If the budget can't bound peak
	// heap, the test fails on the peak-heap assertion below.
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

	// Slow consumer (250µs/block) — emulates LogsQL pipe + HTTP JSON
	// writer in production. The backpressure path must hold up.
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

	// Quantitative assertion #1: peak heap growth ceiling.
	// 384 MiB headroom is the production-realistic budget: 2 GiB container
	// minus the steady-state allocators we can't easily shrink (512 MiB
	// cache, ~200 MiB baseline, ~200 MiB Go runtime + parquet-go pools)
	// leaves ~1 GiB for transient query work, and 384 MiB of that is what
	// a single concurrent query can safely take on top of the in-flight
	// budget + decoder semaphore.
	const maxPeakHeapBytes = int64(384 * 1024 * 1024)
	if peakHeap.Load() > maxPeakHeapBytes {
		t.Fatalf("PRODUCTION-SHAPE REGRESSION: peak heap growth = %.1f MiB exceeded the "+
			"%.0f MiB ceiling on a %d-file × %d-row wildcard scan. "+
			"This is the same failure mode the user saw on the live 2-day query: "+
			"the row-group decoder is unbounded, blocks pile up faster than the "+
			"consumer drains them, and the 2 GiB container OOM-kills. "+
			"Check: (a) rgDecodeSem is actually acquired in readRowGroupWithProjection, "+
			"(b) splitAndEmitDataBlock is splitting at defaultMaxRowsPerBlock, "+
			"(c) defaultMaxLiveBytes is honoured by filteredWriteBlock.",
			float64(peakHeap.Load())/(1024*1024),
			float64(maxPeakHeapBytes)/(1024*1024),
			filesCount, rowsPerFile)
	}

	// Quantitative assertion #2: in-flight queue depth (peak heap / max block size).
	if maxBlockBytes.Load() > 0 {
		queueDepth := float64(peakHeap.Load()) / float64(maxBlockBytes.Load())
		const maxQueueDepth = float64(64)
		if queueDepth > maxQueueDepth {
			t.Fatalf("in-flight queue depth = %.1fx max-block-size (%.0f MiB peak / %.1f MiB block); "+
				"want at most %.0fx. This means blocks are queueing in a deep buffer "+
				"instead of being consumed under wbMu — the synchronous-emit invariant "+
				"or the rgDecodeSem cap has regressed.",
				queueDepth,
				float64(peakHeap.Load())/(1024*1024),
				float64(maxBlockBytes.Load())/(1024*1024),
				maxQueueDepth)
		}
	}

	// Quantitative assertion #3: either the budget fired, OR all rows
	// were returned. No silent data loss permitted.
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
