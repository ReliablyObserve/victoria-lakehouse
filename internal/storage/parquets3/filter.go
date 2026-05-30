package parquets3

import (
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// parseFilterFromQuery extracts the filter from a parsed Query using VL's own
// DropAllPipes() and ParseFilter(). Returns nil for wildcard or time-only queries
// (caller treats nil as "match all").
func parseFilterFromQuery(q *logstorage.Query) *logstorage.Filter {
	if q == nil {
		return nil
	}
	// Clone the query and strip pipes using VL's exported method,
	// then get the filter-only string representation.
	clone := q.Clone(q.GetTimestamp())
	clone.DropAllPipes()
	filterStr := clone.String()

	if filterStr == "" || filterStr == "*" {
		return nil
	}

	// Time-only queries have no field filters to evaluate at row level —
	// time filtering is handled by partition/row-group pruning.
	if isTimeOnlyFilter(filterStr) {
		return nil
	}

	f, err := logstorage.ParseFilter(filterStr)
	if err != nil {
		return nil
	}
	return f
}

// isTimeOnlyFilter returns true if the filter string contains only _time predicates.
func isTimeOnlyFilter(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || s == "*" {
		return true
	}
	cleaned := stripTimePredicates(s)
	cleaned = strings.TrimSpace(cleaned)
	return cleaned == "" || cleaned == "*"
}

// stripTimePredicates removes _time:[...] filter expressions from the string.
func stripTimePredicates(s string) string {
	for {
		idx := strings.Index(s, "_time:")
		if idx < 0 {
			return s
		}
		end := idx + len("_time:")
		if end < len(s) && s[end] == '[' {
			depth := 0
			for i := end; i < len(s); i++ {
				if s[i] == '[' {
					depth++
				} else if s[i] == ']' {
					depth--
					if depth == 0 {
						s = s[:idx] + s[i+1:]
						break
					}
				}
			}
		} else {
			spaceIdx := strings.IndexByte(s[end:], ' ')
			if spaceIdx < 0 {
				s = s[:idx]
			} else {
				s = s[:idx] + s[end+spaceIdx:]
			}
		}
	}
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
