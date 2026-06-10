package parquets3

import (
	"context"
	"runtime"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// defaultMaxLiveBytes caps the bytes of in-flight DataBlock rows a single
// query may pin between the parquet decoder and the LogsQL pipe consumer.
// Mirror of the logs module — see ../../../../internal/storage/parquets3/
// query_memory_budget.go for the rationale.
const defaultMaxLiveBytes int64 = 512 * 1024 * 1024

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

// acquireRGDecode blocks until a row-group decoder slot is available.
// Returns a release func that must be called when decoding is complete.
func acquireRGDecode() func() {
	rgDecodeSem <- struct{}{}
	return func() { <-rgDecodeSem }
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
