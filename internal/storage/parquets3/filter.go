package parquets3

import (
	"regexp"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type filterOp int

const (
	filterExact     filterOp = iota // field:="value"
	filterSubstring                 // field:"value" or field:value
	filterRegex                     // field:~"pattern"
	filterNegate                    // NOT wrapping another predicate
)

type filterPredicate struct {
	field   string // column name to match; empty means _msg
	op      filterOp
	value   string
	re      *regexp.Regexp
	negated bool
}

// parseFilterPredicates extracts AND-combined field matchers from a LogsQL query string.
// Supports: field:="exact", field:"substring", field:value, field:~"regex", NOT prefix.
// Returns nil for empty or unparseable queries (caller treats nil as "match all").
func parseFilterPredicates(queryStr string) []filterPredicate {
	if queryStr == "" || queryStr == "*" {
		return nil
	}

	// Strip pipe portion — filters are before the first unquoted |
	filterPart := stripPipes(queryStr)
	if filterPart == "" || filterPart == "*" {
		return nil
	}

	var predicates []filterPredicate
	tokens := tokenizeFilter(filterPart)

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]

		if strings.EqualFold(tok, "AND") || strings.EqualFold(tok, "OR") {
			continue
		}

		negated := false
		if strings.EqualFold(tok, "NOT") && i+1 < len(tokens) {
			negated = true
			i++
			tok = tokens[i]
		}

		if p, ok := parseFieldPredicate(tok, negated); ok {
			predicates = append(predicates, p)
		}
	}

	return predicates
}

func parseFieldPredicate(tok string, negated bool) (filterPredicate, bool) {
	// field:="value"
	if idx := strings.Index(tok, `:="`); idx > 0 {
		field := tok[:idx]
		val := extractQuoted(tok[idx+3:])
		return filterPredicate{field: field, op: filterExact, value: val, negated: negated}, true
	}

	// field:~"pattern"
	if idx := strings.Index(tok, `:~"`); idx > 0 {
		field := tok[:idx]
		pattern := extractQuoted(tok[idx+3:])
		re, err := regexp.Compile(pattern)
		if err != nil {
			return filterPredicate{}, false
		}
		return filterPredicate{field: field, op: filterRegex, value: pattern, re: re, negated: negated}, true
	}

	// field:"value"
	if idx := strings.Index(tok, `:"`); idx > 0 {
		field := tok[:idx]
		val := extractQuoted(tok[idx+2:])
		return filterPredicate{field: field, op: filterSubstring, value: val, negated: negated}, true
	}

	// field:value (no quotes — substring match)
	// Skip _time: filters (handled by time range extraction) and bracketed range expressions
	if idx := strings.Index(tok, ":"); idx > 0 && !strings.HasPrefix(tok, "_time:") && !strings.Contains(tok, "[") {
		field := tok[:idx]
		val := tok[idx+1:]
		if val != "" && val != "*" {
			return filterPredicate{field: field, op: filterSubstring, value: val, negated: negated}, true
		}
	}

	// Bare unquoted word — match against _msg
	// Skip tokens that look like range boundaries (e.g., "2024-01-02)" from _time filters)
	if !strings.Contains(tok, ":") && tok != "*" && tok != "" &&
		!strings.HasSuffix(tok, ")") && !strings.HasSuffix(tok, "]") {
		val := strings.Trim(tok, `"`)
		if val != "" {
			return filterPredicate{field: "_msg", op: filterSubstring, value: val, negated: negated}, true
		}
	}

	return filterPredicate{}, false
}

func extractQuoted(s string) string {
	if idx := strings.Index(s, `"`); idx >= 0 {
		return s[:idx]
	}
	return s
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

// tokenizeFilter splits a LogsQL filter string into tokens, respecting quotes.
func tokenizeFilter(s string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' && (i == 0 || s[i-1] != '\\'):
			inQuote = !inQuote
			current.WriteByte(c)
		case (c == ' ' || c == '\t') && !inQuote:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// filterDataBlock removes rows from a DataBlock that don't match the given predicates.
// Returns nil if all rows are filtered out. Returns the original block if predicates is nil.
func filterDataBlock(db *logstorage.DataBlock, predicates []filterPredicate) *logstorage.DataBlock {
	if len(predicates) == 0 || db == nil {
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

	colMap := make(map[string]int, len(columns))
	for i, col := range columns {
		colMap[col.Name] = i
	}

	// Determine which rows pass all predicates (AND logic)
	keep := make([]bool, rowCount)
	kept := 0
	for i := range rowCount {
		if rowMatchesAll(columns, colMap, i, predicates) {
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

	// Build filtered columns
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

func rowMatchesAll(columns []logstorage.BlockColumn, colMap map[string]int, rowIdx int, predicates []filterPredicate) bool {
	for _, p := range predicates {
		matched := matchPredicate(columns, colMap, rowIdx, p)
		if p.negated {
			matched = !matched
		}
		if !matched {
			return false
		}
	}
	return true
}

func matchPredicate(columns []logstorage.BlockColumn, colMap map[string]int, rowIdx int, p filterPredicate) bool {
	colIdx, ok := colMap[p.field]
	if !ok {
		// Field not present — can't match exact/regex, but substring of empty string is a miss
		return false
	}

	val := columns[colIdx].Values[rowIdx]

	switch p.op {
	case filterExact:
		return val == p.value
	case filterSubstring:
		return strings.Contains(val, p.value)
	case filterRegex:
		if p.re != nil {
			return p.re.MatchString(val)
		}
		return false
	default:
		return true
	}
}
