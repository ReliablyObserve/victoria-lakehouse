package parquets3

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// defaultMaxLiveBytes caps the bytes of in-flight DataBlock rows a single
// query may pin between the parquet decoder and the LogsQL pipe consumer.
// Mirror of the logs module — see ../../../../internal/storage/parquets3/
// query_memory_budget.go for the rationale.
const defaultMaxLiveBytes int64 = 512 * 1024 * 1024

// liveBytesGate is the per-query ceiling on the decoded bytes of the objects a
// query is reading at once (query.max_live_bytes). A file worker RESERVES an
// estimate of the object's decoded size before it takes any process-wide slot
// (file budget, row-group decoder) or opens the object, and waits for the
// reservation when the bytes already reserved would pass the ceiling. The wait
// holds nothing, so a query whose consumer is slow or stalled cannot starve the
// other queries of slots; it is ctx-aware; and it never cancels the read or
// drops a block (the query used to be cancelled instead, which answered 200 with
// rows missing).
//
// The reservation is atomic: the bytes reserved never pass the ceiling, except
// by one object larger than the ceiling, which is admitted alone when nothing
// else is reserved (so no object can wait for room that never comes). The
// estimate is the object's uncompressed size from its footer when the footer is
// cached, else its stored size times an assumed expansion
// (assumedDecodeExpansion): a conservative bound, since a worker decodes one row
// group of the object at a time.
type liveBytesGate struct {
	live    atomic.Int64
	peak    atomic.Int64 // the high-water mark of live, for the tests and the ceiling's metric
	waiters atomic.Int32
	logged  atomic.Bool
	mu      sync.Mutex
	cond    *sync.Cond
}

// assumedDecodeExpansion is the assumed ratio of an object's decoded bytes to its
// stored bytes when its footer is not at hand (assumed, not measured: zstd-compressed
// logs and spans decode to several times their size).
const assumedDecodeExpansion = 4

// tryReserve reserves n bytes unless that would pass max while other bytes are reserved.
func (g *liveBytesGate) tryReserve(n, max int64) bool {
	for {
		cur := g.live.Load()
		if cur != 0 && cur+n > max {
			return false
		}
		if g.live.CompareAndSwap(cur, cur+n) {
			for {
				p := g.peak.Load()
				if cur+n <= p || g.peak.CompareAndSwap(p, cur+n) {
					return true
				}
			}
		}
	}
}

// reserve blocks until n bytes can be reserved against max, or ctx ends. The
// returned release must be called once the bytes are no longer held.
func (g *liveBytesGate) reserve(ctx context.Context, n, max int64) (func(), error) {
	if n < 1 {
		n = 1
	}
	release := func() {
		g.live.Add(-n)
		if g.waiters.Load() > 0 {
			g.wake()
		}
	}
	if g.tryReserve(n, max) {
		return release, nil
	}
	metrics.QueryMemoryBudgetExceeded.Inc()
	if g.logged.CompareAndSwap(false, true) {
		logger.Warnf("query memory budget reached: reserved=%d, want=%d, max=%d; waiting for earlier objects to be consumed",
			g.live.Load(), n, max)
	}
	stop := context.AfterFunc(ctx, g.wake)
	defer stop()

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cond == nil {
		g.cond = sync.NewCond(&g.mu)
	}
	g.waiters.Add(1)
	defer g.waiters.Add(-1)
	for !g.tryReserve(n, max) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g.cond.Wait()
	}
	return release, nil
}

func (g *liveBytesGate) wake() {
	g.mu.Lock()
	if g.cond != nil {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

// liveGateHook, when set (tests only), receives each query's gate as it is made.
var liveGateHook func(*liveBytesGate)

type liveGateKey struct{}

type liveGateRef struct {
	g   *liveBytesGate
	max int64
}

// withLiveBytesGate attaches the query's gate to ctx for queryFile.
func withLiveBytesGate(ctx context.Context, g *liveBytesGate, max int64) context.Context {
	return context.WithValue(ctx, liveGateKey{}, liveGateRef{g: g, max: max})
}

// reserveLiveBytes reserves the estimated decoded size of the object against the
// query's ceiling; a query without a gate (QuerySpecificFiles) reserves nothing.
func (s *Storage) reserveLiveBytes(ctx context.Context, fi manifest.FileInfo) (func(), error) {
	ref, ok := ctx.Value(liveGateKey{}).(liveGateRef)
	if !ok {
		return func() {}, nil
	}
	return ref.g.reserve(ctx, s.estimateDecodedBytes(fi), ref.max)
}

// estimateDecodedBytes: the uncompressed size of the object's row groups from its
// cached footer, else the stored size times assumedDecodeExpansion.
func (s *Storage) estimateDecodedBytes(fi manifest.FileInfo) int64 {
	if s.footerCache != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok && cached != nil && cached.File != nil {
			if md := cached.File.Metadata(); md != nil {
				var total int64
				for _, rg := range md.RowGroups {
					total += rg.TotalByteSize
				}
				if total > 0 {
					return total
				}
			}
		}
	}
	return fi.Size * assumedDecodeExpansion
}

// defaultMaxRowsPerBlock caps the number of rows in a single DataBlock
// emitted to the LogsQL pipe consumer. Mirror of the logs module — see
// ../../../../internal/storage/parquets3/query_memory_budget.go for the
// rationale (4096 matches VL's writeBlock pipe consumer shape and the
// typed-row reader buffer size in readRowGroupTyped).
const defaultMaxRowsPerBlock int = 4096

// dataBlockApproxBytes returns a best-effort byte cost for a DataBlock.
// See the logs-module equivalent for context.
func dataBlockApproxBytes(db *logstorage.DataBlock) int64 {
	if db == nil {
		return 0
	}
	var total int64
	cols := db.GetColumns(false)
	for i := range cols {
		col := &cols[i]
		total += int64(len(col.Name))
		total += 16
		for _, v := range col.Values {
			total += int64(len(v))
		}
	}
	return total
}

// splitAndEmitDataBlock emits a DataBlock as one or more chunks of at
// most maxRows rows each. See the logs-module equivalent for full
// context — this bounds the in-flight memory per worker so liveBytes
// + wbMu.Lock can backpressure the decoder one chunk at a time
// instead of one whole row group at a time.
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

// rgDecodeSem bounds concurrent row-group decoders process-wide.
// Mirror of the logs module — see the logs equivalent for full
// rationale. We cap to runtime.GOMAXPROCS(0)/2 (min 2, max 8) to
// match VL upstream's partitionSearchConcurrencyLimitCh memory
// safety pattern, scaled down for parquet's larger decode footprint.
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

// acquireRGDecode blocks until a row-group decoder slot is available, or ctx
// ends (a query its caller gave up on stops waiting at once). Returns a release
// func that must be called when decoding is complete; callers defer it right away.
func acquireRGDecode(ctx context.Context) (func(), error) {
	select {
	case rgDecodeSem <- struct{}{}:
		return func() { <-rgDecodeSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// defaultMaxFileResidentBytes caps the cumulative bytes of parquet-file
// data resident in the file-worker pool across all queries on the
// process. Mirror of the logs module — see
// ../../../../internal/storage/parquets3/query_memory_budget.go for
// the heap-diff sizing rationale.
const defaultMaxFileResidentBytes int64 = 256 * 1024 * 1024

// defaultMaxConcurrentFiles caps the COUNT of concurrent file processing
// slots. Mirror of the logs module — see the equivalent comment there
// for the wildcard-fanout failure mode this bounds.
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

// fileBudgetSem is the process-wide byte+count-budget semaphore that
// bounds cumulative parquet-file bytes resident in the file-worker pool.
// Mirror of the logs module.
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

func (b *fileBudget) acquire(ctx context.Context, size int64) (func(), error) {
	if size <= 0 {
		size = 1
	}
	cap := b.max
	if size > cap {
		size = cap
	}

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

//nolint:unused // reserved for future metrics export; logs module mirror has identical signature exercised by tests
func (b *fileBudget) outstanding() (int64, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.outBytes, b.outCount
}

// acquireFileBudget reserves size bytes + a count slot from the process-wide
// file-resident budget. Callers must defer the returned release.
func acquireFileBudget(ctx context.Context, size int64) (func(), error) {
	return fileBudgetSem.acquire(ctx, size)
}

// fileBudgetOutstanding returns the current resident file bytes and count.
//
//nolint:unused // reserved for future metrics export; logs module mirror has identical signature exercised by tests
func fileBudgetOutstanding() (int64, int) {
	return fileBudgetSem.outstanding()
}

// addBytes records n bytes in the budget ledger WITHOUT blocking and
// without consuming a count slot. Mirror of the logs module — see
// ../../../../internal/storage/parquets3/query_memory_budget.go for the
// deadlock-freedom rationale (the caller already holds an fi.Size
// admission that subsumes the plan bytes).
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
