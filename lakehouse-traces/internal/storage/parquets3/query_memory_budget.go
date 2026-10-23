package parquets3

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// defaultMaxLiveBytes caps the bytes of in-flight DataBlock rows a single
// query may pin between the parquet decoder and the LogsQL pipe consumer.
// Mirror of the logs module — see ../../../../internal/storage/parquets3/
// query_memory_budget.go for the rationale.
const defaultMaxLiveBytes int64 = 512 * 1024 * 1024

// liveBytesGate is the per-query ceiling on the DECODED bytes in flight
// (query.max_live_bytes). Before a worker decodes a row group it RESERVES an
// estimate of the row group's decoded size, taken from the footer it has already
// read (decodedEstimate: no request is added), and waits for the reservation when
// the bytes already reserved would pass the ceiling. The reserved bytes never pass
// the ceiling, except by one row group larger than it, which is admitted alone (so
// nothing waits for room that never comes). The wait is ctx-aware and never
// cancels the read or drops a block (the query used to be cancelled instead,
// which answered 200 with rows missing). A worker that waits has opened its
// object, so it holds that object's file-budget slot: a stalled consumer can
// still hold slots another query needs (see docs/read-path.md).
//
// The unit is the row group (what is decoded at once, 8-16 MiB in production),
// not the object, so a 128 MiB object does not take a 512 MiB ceiling for itself.
type liveBytesGate struct {
	live    atomic.Int64
	peak    atomic.Int64 // the high-water mark of live, for the tests and the ceiling's metric
	waiters atomic.Int32
	logged  atomic.Bool
	mu      sync.Mutex
	cond    *sync.Cond
}

// assumedDecodeExpansion is the assumed ratio of decoded to stored bytes, used only
// when a footer carries no sizes for a row group (assumed, not measured).
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

// inflightBlockHook, when set (tests only), is told the bytes of each block as it
// leaves the decoder for the consumer (positive) and when the consumer is done with it (negative).
var inflightBlockHook func(delta int64)

// liveGateHook, when set (tests only), receives each query's gate as it is made.
var liveGateHook func(*liveBytesGate)

// newQueryLiveGate makes the gate of one query (and shows it to the tests).
func newQueryLiveGate() *liveBytesGate {
	g := &liveBytesGate{}
	if liveGateHook != nil {
		liveGateHook(g)
	}
	return g
}

// trackInflightBlock tells the tests' hook about a block on its way to the
// consumer; the returned func tells it the consumer is done with it.
func trackInflightBlock(db *logstorage.DataBlock) func() {
	if inflightBlockHook == nil {
		return func() {}
	}
	sz := dataBlockApproxBytes(db)
	inflightBlockHook(sz)
	return func() { inflightBlockHook(-sz) }
}

type liveGateKey struct{}

type liveGateRef struct {
	g   *liveBytesGate
	max int64
}

// withLiveBytesGate attaches the query's gate to ctx for queryFile.
func withLiveBytesGate(ctx context.Context, g *liveBytesGate, max int64) context.Context {
	return context.WithValue(ctx, liveGateKey{}, liveGateRef{g: g, max: max})
}

// reserveRowGroup reserves the estimated decoded size of row group idx of f
// against the query's ceiling; a query without a gate (QuerySpecificFiles)
// reserves nothing.
func reserveRowGroup(ctx context.Context, f *parquet.File, idx int, projected map[string]bool, storedSize int64) (func(), error) {
	ref, ok := ctx.Value(liveGateKey{}).(liveGateRef)
	if !ok {
		return func() {}, nil
	}
	return ref.g.reserve(ctx, decodedEstimate(f.Metadata(), idx, projected, storedSize), ref.max)
}

// decodedWidthOfType is the least width in bytes of the text a value of a fixed-width
// physical type decodes to (an int64 timestamp becomes an RFC 3339 string of ~30
// bytes); 0 for the variable-length types, which the statistics describe.
func decodedWidthOfType(t format.Type) int64 {
	switch t {
	case format.Boolean:
		return 5
	case format.Int32:
		return 12
	case format.Int64:
		return 32
	case format.Float:
		return 16
	case format.Double:
		return 24
	}
	return 0
}

// decodedEstimate is the estimate of the decoded bytes of row group idx for the
// projected columns (nil: every column but the storage-bookkeeping ones, as the
// unprojected scan reads): per column the larger of its chunk's uncompressed size
// and its value count times the longest of its min/max statistics and the text width of its type (a dictionary-encoded
// column is small on disk and large decoded), summed. It never reads the
// compressed size: compressible data decodes to several times it. A row group
// without sizes falls back to the object's stored size times assumedDecodeExpansion
// shared by its row groups.
func decodedEstimate(md *format.FileMetaData, idx int, projected map[string]bool, storedSize int64) int64 {
	fallback := func(rowGroups int) int64 {
		if rowGroups < 1 {
			rowGroups = 1
		}
		if e := storedSize * assumedDecodeExpansion / int64(rowGroups); e > 0 {
			return e
		}
		return 1
	}
	if md == nil || idx < 0 || idx >= len(md.RowGroups) {
		return fallback(1)
	}
	var total int64
	for _, c := range md.RowGroups[idx].Columns {
		m := c.MetaData
		if len(m.PathInSchema) == 0 {
			continue
		}
		top := m.PathInSchema[0]
		if projected != nil && !projected[top] {
			continue
		}
		if projected == nil && schema.IsInternalColumn(top) {
			continue
		}
		col := m.TotalUncompressedSize
		longest := len(m.Statistics.MinValue)
		if n := len(m.Statistics.MaxValue); n > longest {
			longest = n
		}
		if w := decodedWidthOfType(m.Type); w > int64(longest) {
			longest = int(w) // a number decodes to its text, which is longer than its binary form
		}
		if v := m.NumValues * int64(longest); v > col {
			col = v
		}
		total += col
	}
	if total <= 0 {
		return fallback(len(md.RowGroups))
	}
	return total
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
