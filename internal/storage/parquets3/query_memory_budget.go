package parquets3

import (
	"runtime"

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
