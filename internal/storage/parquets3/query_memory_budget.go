package parquets3

import (
	"context"
	"runtime"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// defaultMaxLiveBytes caps the bytes of in-flight DataBlock rows a single
// query may pin between the parquet decoder and the LogsQL pipe consumer.
// 512 MiB is ~25 % of the canonical 2 GiB container memory limit, leaving
// the remaining ~1.5 GiB for caches, parquet decode buffers, and the Go
// runtime overhead. Overridable per-process via -lakehouse.query.max-live-bytes
// and per-config via QueryConfig.MaxLiveBytes.
const defaultMaxLiveBytes int64 = 512 * 1024 * 1024

// defaultMaxRowsPerBlock caps the number of rows in a single DataBlock
// emitted to the LogsQL pipe consumer. Wildcard queries against wide
// schemas decode whole row groups (typically 100k-256k rows) into a
// single DataBlock; without chunking, a 16-worker fanout of full row
// groups easily exceeds the 2 GiB container limit (per the heap-diff
// at near-OOM: readMapColumnToBlockCols=154 MiB flat,
// parquetValueToInterface=104 MiB, readScalarColumnFormatted=36 MiB —
// the dominant transient decode allocators).
//
// 4096 rows is the same shape VL's writeBlock pipe consumer prefers
// (deps/VictoriaLogs/lib/logstorage/block_search.go: bs.br.rowsLen is
// bounded per partition block) and matches the typed-row reader buffer
// size used by readRowGroupTyped (storage_query.go:614). Chunking the
// post-decode DataBlock here lets liveBytes + wbMu.Lock actually
// backpressure the decoder, which is what stops 16 wide-schema row
// groups from piling up in flight.
const defaultMaxRowsPerBlock int = 4096

// dataBlockApproxBytes returns a best-effort byte cost for a DataBlock that
// is about to be written to the result channel. The accounting is approximate
// (column-name overhead is bounded, raw string bytes dominate); precision is
// less important than the order-of-magnitude ceiling that prevents producer
// fanout from running away while the consumer is slow. We deliberately walk
// the actual Values slice instead of guessing from row count so wide-schema
// wildcard queries (the OOM trigger) are accounted correctly.
func dataBlockApproxBytes(db *logstorage.DataBlock) int64 {
	if db == nil {
		return 0
	}
	var total int64
	cols := db.GetColumns(false)
	for i := range cols {
		col := &cols[i]
		total += int64(len(col.Name))
		// Bound the per-column overhead so a degenerate wide-but-empty block
		// still costs at least the size of its column descriptor.
		total += 16
		for _, v := range col.Values {
			total += int64(len(v))
		}
	}
	return total
}

// splitAndEmitDataBlock emits a DataBlock as one or more chunks of at
// most maxRows rows each. Wildcard wide-schema queries against large
// row groups produce DataBlocks of 100k+ rows that, when held in-flight
// behind a slow writeBlock consumer (LogsQL pipe + HTTP writer), pin
// hundreds of MiB per worker. Splitting before emit bounds the in-flight
// memory to maxRows × column-count × avg-value-bytes per chunk, and
// lets liveBytes + wbMu.Lock backpressure the decoder one chunk at a
// time instead of one whole row group at a time.
//
// maxRows <= 0 means "no chunking" — the DataBlock is emitted as-is.
//
// emit is called for each chunk in order. If emit needs to stop early
// (e.g. budget exceeded), it should rely on the caller's context
// cancellation; this helper does not poll ctx, the per-chunk wbMu.Lock
// loop in storage_query.go's filteredWriteBlock honours the budget.
func splitAndEmitDataBlock(db *logstorage.DataBlock, maxRows int, emit func(*logstorage.DataBlock)) {
	if db == nil {
		return
	}
	rows := db.RowsCount()
	if rows == 0 {
		return
	}
	if maxRows <= 0 || rows <= maxRows {
		emit(db)
		return
	}
	cols := db.GetColumns(false)
	for start := 0; start < rows; start += maxRows {
		end := start + maxRows
		if end > rows {
			end = rows
		}
		chunkCols := make([]logstorage.BlockColumn, len(cols))
		for i := range cols {
			chunkCols[i] = logstorage.BlockColumn{
				Name:   cols[i].Name,
				Values: cols[i].Values[start:end:end],
			}
		}
		chunk := &logstorage.DataBlock{}
		chunk.SetColumns(chunkCols)
		emit(chunk)
	}
}

// rgDecodeSem bounds the number of row-group decoders that may execute
// concurrently across all file workers for all in-flight queries on the
// process. This is the key memory-safety knob for wildcard queries
// over wide schemas: each decoder buffers a full row group's columns
// (~30-50 MiB at production scale per the inuse_space heap-diff at
// near-OOM), so 16 file workers each decoding a row group in parallel
// produces 480-800 MiB of transient memory on top of the 512 MiB cache
// and ~200 MiB baseline — a deterministic OOM on a 2 GiB container.
//
// The cap mirrors VL upstream's partitionSearchConcurrencyLimitCh
// (deps/VictoriaLogs/lib/logstorage/storage_search.go:1424), which
// caps concurrent partition searches to cgroup.AvailableCPUs() for
// the same reason: bound memory under high load.
//
// We size to runtime.GOMAXPROCS / 2 (min 2, max 8): smaller than VL's
// AvailableCPUs because each parquet row-group decoder allocates
// substantially more transient memory than a VL block search (parquet
// is column-oriented with full decompression buffers; VL streams
// compressed chunks from mmap'd files).
var rgDecodeSem = func() chan struct{} {
	n := runtime.GOMAXPROCS(0) / 2
	if n < 2 {
		n = 2
	}
	if n > 8 {
		n = 8
	}
	return make(chan struct{}, n)
}()

// acquireRGDecode blocks until a row-group decoder slot is available.
// Returns a release func that must be called when decoding is complete.
// Callers should defer the release immediately.
//
// If ctx is cancelled while waiting, the returned release is a no-op
// and the caller should treat this as a "decode skipped" signal.
func acquireRGDecode() func() {
	rgDecodeSem <- struct{}{}
	return func() { <-rgDecodeSem }
}

// defaultMaxFileResidentBytes caps the cumulative bytes of parquet-file
// data resident in the file-worker pool across all queries on the
// process. Each file worker, while processing a file, holds the whole
// downloaded body in memory via bytes.NewReader(data) wired into the
// parquet.File — L1 cache eviction cannot reclaim those bytes until the
// worker releases its reference. So peak memory under wildcard fanout
// is dominated NOT by downloads (which complete in <1s) but by the
// FILE-LIFECYCLE: 16 workers × ~30 MiB ≈ 480 MiB pinned for the entire
// open-decode-emit-release window.
//
// Per the 7-day heap-diff at OOM moment, io.ReadAll retained 808 MiB
// at the 99.88% mem peak — that's the L1 cache (512 MiB) plus
// 16 file workers' resident bodies (~10-50 MiB each) plus pending L2
// disk writes (sync). A 256 MiB cap here keeps the pool of "actively
// processing" files bounded — leaving ~512 MiB L1, ~512 MiB live-block
// budget, ~200 MiB baseline, ~400 MiB parquet-go internal pools — fits
// inside the 2 GiB container.
//
// Per [[feedback_k8s_style_resource_bounds]], this is a process-wide
// REQUEST/LIMIT ceiling: cumulative file-worker memory is bound
// regardless of how many concurrent queries land.
const defaultMaxFileResidentBytes int64 = 256 * 1024 * 1024

// defaultMaxConcurrentFiles caps the COUNT of files admitted to the
// file-worker pool concurrently across all queries. The byte budget alone
// is insufficient when files are small (avg 2.5 MiB in production): 16
// workers × 2.5 MiB = 40 MiB << 256 MiB budget, so the budget never
// fires, and the cache/decoder burst still OOM-kills the container as
// the worker pool grinds through hundreds of files quickly. The count
// cap mirrors rgDecodeSem: GOMAXPROCS/2 (min 2, max 8) — small enough
// that the decoder burst per worker (column-decode peak ~60 MiB) times
// active workers stays under ~500 MiB, leaving room for L1 cache +
// parquet-go internal pools inside the 2 GiB container.
//
// This is the K8s-style REQUEST cap (count); defaultMaxFileResidentBytes
// is the LIMIT (bytes). A file admits when BOTH allow — both bound the
// failure mode they target.
func defaultMaxConcurrentFiles() int {
	n := runtime.GOMAXPROCS(0) / 2
	if n < 2 {
		n = 2
	}
	if n > 8 {
		n = 8
	}
	return n
}

// fileBudgetSem is the process-wide byte-budget semaphore that bounds
// cumulative parquet-file bytes resident in the file-worker pool.
// Workers call acquireFileBudget(ctx, size) BEFORE opening a parquet
// file; the budget is released when the worker finishes processing the
// file (including emit). This is what stops the 16-file-worker fanout
// from pinning 16 × ~30 MiB = ~480 MiB of file bodies for the entire
// open-decode-emit window — instead the pool naturally serializes
// when cumulative resident bytes exceed the budget.
//
// Mirrors VL upstream's partitionSearchConcurrencyLimitCh pattern
// (deps/VictoriaLogs/lib/logstorage/storage_search.go:1424) — but on
// bytes instead of count, because parquet files have an order-of-magnitude
// size variance (the count cap doesn't catch the OOM when files are large).
//
// The download itself is also gated by this budget (download is a
// sub-step of file processing) — acquireFileBudget covers
// open-download-decode-emit-release as one lifecycle. This is correct
// because the bytes remain resident the entire time.
var fileBudgetSem = newFileBudget(defaultMaxFileResidentBytes, defaultMaxConcurrentFiles())

type fileBudget struct {
	mu       sync.Mutex
	cond     *sync.Cond
	max      int64
	outBytes int64
	maxCount int
	outCount int
}

func newFileBudget(max int64, maxCount int) *fileBudget {
	if maxCount < 1 {
		maxCount = 1
	}
	b := &fileBudget{max: max, maxCount: maxCount}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// acquire blocks until size bytes AND a count slot can be reserved
// against the budget. Returns a release func that must be called when
// the file's processing completes (success OR failure). If ctx is
// cancelled while waiting, returns ctx.Err() and a no-op release.
//
// A single file larger than max is admitted alone (the budget is soft
// for sizes >= max; we'd rather process one big file slowly than fail
// every query that hits an outlier file). Subsequent acquires wait
// until the giant releases. The count cap (maxCount) is the request-side
// ceiling that bounds decoder fanout when files are small.
func (b *fileBudget) acquire(ctx context.Context, size int64) (func(), error) {
	if size <= 0 {
		size = 1 // still consume a count slot
	}
	cap := b.max
	if size > cap {
		// Outlier file: admit alone but block others while it's in flight.
		size = cap
	}

	// Use a context-aware wait by signalling cond on context cancellation.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		case <-done:
		}
	}()

	b.mu.Lock()
	for {
		// Both gates must allow. An empty pool always admits (the outlier
		// file case): if outCount==0 we let any single request through.
		bytesOK := b.outBytes+size <= b.max || b.outCount == 0
		countOK := b.outCount < b.maxCount
		if bytesOK && countOK {
			break
		}
		if ctx.Err() != nil {
			b.mu.Unlock()
			return func() {}, ctx.Err()
		}
		b.cond.Wait()
	}
	if ctx.Err() != nil {
		b.mu.Unlock()
		return func() {}, ctx.Err()
	}
	b.outBytes += size
	b.outCount++
	b.mu.Unlock()

	released := false
	return func() {
		b.mu.Lock()
		if !released {
			released = true
			b.outBytes -= size
			b.outCount--
			if b.outBytes < 0 {
				b.outBytes = 0
			}
			if b.outCount < 0 {
				b.outCount = 0
			}
			b.cond.Broadcast()
		}
		b.mu.Unlock()
	}, nil
}

// outstanding returns the current resident file bytes and count. Exposed
// for tests and metrics.
//
//nolint:unused // used by tests in query_memory_budget_test.go; reserved for future metrics export
func (b *fileBudget) outstanding() (int64, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.outBytes, b.outCount
}

// acquireFileBudget reserves size bytes + a count slot from the process-wide
// file-resident budget. Callers must defer the returned release. If ctx is
// cancelled while waiting, returns the cancellation error and a no-op release.
func acquireFileBudget(ctx context.Context, size int64) (func(), error) {
	return fileBudgetSem.acquire(ctx, size)
}

// fileBudgetOutstanding returns the current resident file bytes and count.
// Used by tests and metrics to verify the budget is bounding peak memory.
//
//nolint:unused // used by tests in query_memory_budget_test.go; reserved for future metrics export
func fileBudgetOutstanding() (int64, int) {
	return fileBudgetSem.outstanding()
}

// addBytes records n bytes in the budget ledger WITHOUT blocking and
// without consuming a count slot. Used by the plan-then-fetch path to
// charge its fetched span bytes against the SAME ledger the decode path's
// admission control reads (acquire's bytesOK check sees them), so
// concurrent file admissions become strictly more conservative while the
// spans are resident. Non-blocking is deliberate and deadlock-free: the
// caller already holds an fi.Size admission from acquireFileBudget that
// subsumes the plan (a plan is byte ranges WITHIN the file, capped at
// s3.projected_fetch_max_bytes), so blocking here could deadlock the
// worker pool against itself — every worker holding an admission while
// waiting for budget no other worker can release.
func (b *fileBudget) addBytes(n int64) func() {
	if n <= 0 {
		return func() {}
	}
	b.mu.Lock()
	b.outBytes += n
	b.mu.Unlock()
	released := false
	return func() {
		b.mu.Lock()
		if !released {
			released = true
			b.outBytes -= n
			if b.outBytes < 0 {
				b.outBytes = 0
			}
			b.cond.Broadcast()
		}
		b.mu.Unlock()
	}
}

// chargePlannedFetchBytes is the memory-governor hook handed to
// s3reader.NewPlannedFetchReaderAt: the planned spans' bytes count against
// the process-wide file-resident budget for as long as they are held
// (released by the view's Close, or immediately when a Fetch fails).
func chargePlannedFetchBytes(n int64) func() {
	return fileBudgetSem.addBytes(n)
}
