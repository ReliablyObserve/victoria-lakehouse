package parquets3

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// defaultMaxLiveBytes caps the bytes of in-flight DataBlock rows a single
// query may pin between the parquet decoder and the LogsQL pipe consumer.
// 512 MiB is ~25 % of the canonical 2 GiB container memory limit, leaving
// the remaining ~1.5 GiB for caches, parquet decode buffers, and the Go
// runtime overhead. Overridable per-process via -lakehouse.query.max-live-bytes
// and per-config via QueryConfig.MaxLiveBytes.
const defaultMaxLiveBytes int64 = 512 * 1024 * 1024

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
