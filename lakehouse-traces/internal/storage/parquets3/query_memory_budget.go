package parquets3

import (
	"runtime"

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
