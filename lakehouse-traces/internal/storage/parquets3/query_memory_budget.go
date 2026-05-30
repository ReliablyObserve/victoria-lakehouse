package parquets3

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// defaultMaxLiveBytes caps the bytes of in-flight DataBlock rows a single
// query may pin between the parquet decoder and the LogsQL pipe consumer.
// Mirror of the logs module — see ../../../../internal/storage/parquets3/
// query_memory_budget.go for the rationale.
const defaultMaxLiveBytes int64 = 512 * 1024 * 1024

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
