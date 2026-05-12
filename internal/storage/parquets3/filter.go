package parquets3

import (
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// parseFilter extracts the filter portion from a LogsQL query string and parses it
// using VL's own parser. This gives full LogsQL filter support: AND, OR, NOT, regex,
// ranges, case-insensitive matching — everything VL supports natively.
// Returns nil for empty or wildcard queries (caller treats nil as "match all").
func parseFilter(queryStr string) *logstorage.Filter {
	if queryStr == "" || queryStr == "*" {
		return nil
	}

	filterPart := stripPipes(queryStr)
	if filterPart == "" || filterPart == "*" {
		return nil
	}

	// Time-only queries (e.g. just "_time:[start, end]") have no field filters
	// to evaluate — treat them as match-all for field-level filtering.
	if isTimeOnlyFilter(filterPart) {
		return nil
	}

	f, err := logstorage.ParseFilter(filterPart)
	if err != nil {
		return nil
	}
	return f
}

// isTimeOnlyFilter returns true if the filter string contains only _time predicates
// and no field-level filters. This allows fast-path label index usage.
func isTimeOnlyFilter(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || s == "*" {
		return true
	}
	// Remove all _time:[...] predicates and check if anything remains
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
			// Find matching closing bracket
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
			// _time:value without brackets — skip to next space
			spaceIdx := strings.IndexByte(s[end:], ' ')
			if spaceIdx < 0 {
				s = s[:idx]
			} else {
				s = s[:idx] + s[end+spaceIdx:]
			}
		}
	}
}

// stripPipes removes the pipe portion from a LogsQL query string.
func stripPipes(q string) string {
	depth := 0
	inQuote := false
	for i, c := range q {
		switch {
		case c == '"' && (i == 0 || q[i-1] != '\\'):
			inQuote = !inQuote
		case c == '(' && !inQuote:
			depth++
		case c == ')' && !inQuote:
			depth--
		case c == '|' && !inQuote && depth == 0:
			return strings.TrimSpace(q[:i])
		}
	}
	return strings.TrimSpace(q)
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

	for i := range rowCount {
		row := buildRowFields(columns, i)
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

// buildRowFields constructs a []logstorage.Field for a single row from DataBlock columns.
func buildRowFields(columns []logstorage.BlockColumn, rowIdx int) []logstorage.Field {
	fields := make([]logstorage.Field, len(columns))
	for i, col := range columns {
		fields[i] = logstorage.Field{
			Name:  col.Name,
			Value: col.Values[rowIdx],
		}
	}
	return fields
}

// filterMatchesRow checks if a single row (as fields) matches the filter.
func filterMatchesRow(f *logstorage.Filter, fields []logstorage.Field) bool {
	if f == nil {
		return true
	}
	return f.MatchRow(fields)
}
