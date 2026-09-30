package vtstorageadapter

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// The hidden_fields_filters request argument (VictoriaLogs' and VictoriaTraces'
// LogsQL handlers both parse it into QueryContext.HiddenFieldsFilters) removes
// matching columns from results. The traces binary serves LogsQL through
// VictoriaTraces' handlers, which reach storage through this adapter, so the
// adapter applies the filters exactly as the logs adapter does
// (internal/vlstorage in the traces module, internal/vlstorage in the root).

// wrapHiddenFields wraps writeBlock to strip columns matching filters.
func wrapHiddenFields(writeBlock logstorage.WriteDataBlockFunc, filters []string) logstorage.WriteDataBlockFunc {
	if len(filters) == 0 {
		return writeBlock
	}
	return func(workerID uint, db *logstorage.DataBlock) {
		columns := db.GetColumns(false)
		filtered := make([]logstorage.BlockColumn, 0, len(columns))
		for _, col := range columns {
			if !prefixfilter.MatchFilters(filters, col.Name) {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) == len(columns) {
			writeBlock(workerID, db)
			return
		}
		if len(filtered) == 0 {
			return
		}
		result := &logstorage.DataBlock{}
		result.SetColumns(filtered)
		writeBlock(workerID, result)
	}
}

// filterHiddenValues removes entries whose Value matches any filter.
func filterHiddenValues(results []logstorage.ValueWithHits, filters []string) []logstorage.ValueWithHits {
	if len(filters) == 0 {
		return results
	}
	filtered := make([]logstorage.ValueWithHits, 0, len(results))
	for _, v := range results {
		if !prefixfilter.MatchFilters(filters, v.Value) {
			filtered = append(filtered, v)
		}
	}
	return filtered
}
