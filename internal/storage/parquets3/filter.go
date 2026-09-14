package parquets3

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// parseFilterFromQuery returns q's filter for row-level evaluation, or nil when
// nothing is left to evaluate per row (a wildcard or time-only filter; the
// caller treats nil as "match all").
//
// The filter is taken from the parsed query itself (logstorage.QueryFilter).
// Rendering q and parsing the text back failed for queries VL accepts but
// cannot print back (Query.Clone panics on that, and the VictoriaMetrics HTTP
// server exits the process on a handler panic), re-anchored relative ranges
// such as `_time:5m` to the parse time, and fell back to "match all" whenever
// the re-parse failed.
func parseFilterFromQuery(q *logstorage.Query) *logstorage.Filter {
	f := logstorage.QueryFilter(q)
	if f == nil {
		return nil
	}

	// A time-only filter has nothing left to evaluate per row: the cold read
	// paths bound every row by q.GetFilterTimeRange(), and partition and
	// row-group pruning use the same range. The decision walks the parsed
	// filter instead of scanning the query text, so every range form VL accepts
	// ([a, b], [a, b), (a, b], (a, b), >a, 5m offset 1h, ...) is recognized,
	// and a shape the walk does not recognize is evaluated per row.
	if FilterIsTimeOnly(f) {
		return nil
	}
	return f
}

// filterDataBlock removes rows from a DataBlock that don't match the given filter.
// Uses VL's Filter.MatchRow() for full LogsQL evaluation (AND, OR, NOT, regex, etc.).
// Returns nil if all rows are filtered out. Returns the original block if filter is nil.
func filterDataBlock(db *logstorage.DataBlock, f *logstorage.Filter) *logstorage.DataBlock {
	if f == nil || db == nil {
		return db
	}

	rowCount := db.RowsCount()
	if rowCount == 0 {
		return db
	}

	columns := db.GetColumns(false)
	if len(columns) == 0 {
		return db
	}

	keep := make([]bool, rowCount)
	kept := 0

	// Hoist the per-row Field slice outside the loop and reset its length
	// via slicing. Previously buildRowFields allocated a new []Field each
	// iteration; for a 1k-row block that produced 1k+ allocations per
	// filterDataBlock call, dominating CPU at high QPS.
	row := make([]logstorage.Field, len(columns))
	for i := range rowCount {
		fillRowFields(row, columns, i)
		if f.MatchRow(row) {
			keep[i] = true
			kept++
		}
	}
	if kept == 0 {
		return nil
	}
	if kept == rowCount {
		return db
	}

	filtered := make([]logstorage.BlockColumn, len(columns))
	for c, col := range columns {
		vals := make([]string, 0, kept)
		for i, v := range col.Values {
			if keep[i] {
				vals = append(vals, v)
			}
		}
		filtered[c] = logstorage.BlockColumn{Name: col.Name, Values: vals}
	}

	result := &logstorage.DataBlock{}
	result.SetColumns(filtered)
	return result
}

// fillRowFields writes one row's worth of columns into the caller-owned
// dst slice. dst must be sized to len(columns).
func fillRowFields(dst []logstorage.Field, columns []logstorage.BlockColumn, rowIdx int) {
	for i, col := range columns {
		dst[i] = logstorage.Field{
			Name:  col.Name,
			Value: col.Values[rowIdx],
		}
	}
}

// filterMatchesRow checks if a single row (as fields) matches the filter.
func filterMatchesRow(f *logstorage.Filter, fields []logstorage.Field) bool {
	if f == nil {
		return true
	}
	return f.MatchRow(fields)
}
