// Package hiddenfields applies a request's hidden_fields_filters to query
// results. It is the one implementation behind the logs adapter, the traces
// module's logs-side adapter and the vtstorage adapter (VictoriaTraces'
// LogsQL handlers), which all receive the filters in
// QueryContext.HiddenFieldsFilters.
//
// Scope: it strips matching columns from result blocks and matching entries
// from field-name/value listings. It does not stop a filter from matching on a
// hidden field, and field_values of a hidden field are not filtered; that gap
// predates this package and is unchanged.
package hiddenfields

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// WrapWriteBlock wraps writeBlock to strip columns matching filters. It uses
// VL's prefixfilter.MatchFilters for exact and wildcard matching. A block left
// with no columns is dropped.
func WrapWriteBlock(writeBlock logstorage.WriteDataBlockFunc, filters []string) logstorage.WriteDataBlockFunc {
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

// FilterValues removes entries whose Value matches any filter.
func FilterValues(results []logstorage.ValueWithHits, filters []string) []logstorage.ValueWithHits {
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
