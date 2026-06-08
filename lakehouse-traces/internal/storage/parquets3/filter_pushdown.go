package parquets3

import (
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// PushDownOp represents a comparison operation for column statistics push-down.
type PushDownOp int

const (
	PushDownExact       PushDownOp = iota // field:="value"
	PushDownGreaterThan                   // field:>"value"
	PushDownLessThan                      // field:<"value"
	PushDownPrefix                        // field:="prefix*"
)

// PushDownCheck is a single predicate that can be evaluated against row-group column stats.
type PushDownCheck struct {
	Column    string
	Op        PushDownOp
	Value     string
	FieldType schema.FieldType
	ColIdx    int // pre-resolved column index; -1 if unresolved
}

// PushDownFilter holds all push-down-able predicates extracted from a query.
type PushDownFilter struct {
	Checks []PushDownCheck
}

// buildPushDownFilter parses the query string for exact, GT, LT, and prefix predicates
// on columns known to the registry. Returns nil if no push-down checks are found.
func buildPushDownFilter(queryStr string, registry *schema.Registry) *PushDownFilter {
	if queryStr == "" || registry == nil {
		return nil
	}
	if containsOrOperator(queryStr) {
		return nil
	}

	var checks []PushDownCheck

	for _, col := range registry.PromotedColumns() {
		// Try both internal name and parquet column name
		names := []string{col.InternalName}
		if col.ParquetColumn != col.InternalName {
			names = append(names, col.ParquetColumn)
		}

		for _, name := range names {
			if isNegatedPredicate(queryStr, name) {
				continue
			}

			// Exact match: field:="value" or field:="prefix*"
			if val := extractQuotedOp(queryStr, name, `:="`); val != "" {
				if strings.HasSuffix(val, "*") && !strings.HasSuffix(val, `\*`) {
					checks = append(checks, PushDownCheck{
						Column:    col.ParquetColumn,
						Op:        PushDownPrefix,
						Value:     val[:len(val)-1],
						FieldType: col.Type,
						ColIdx:    -1,
					})
				} else {
					checks = append(checks, PushDownCheck{
						Column:    col.ParquetColumn,
						Op:        PushDownExact,
						Value:     val,
						FieldType: col.Type,
						ColIdx:    -1,
					})
				}
				break
			}

			// Greater than: field:>"value"
			if val := extractQuotedOp(queryStr, name, `:>"`); val != "" {
				checks = append(checks, PushDownCheck{
					Column:    col.ParquetColumn,
					Op:        PushDownGreaterThan,
					Value:     val,
					FieldType: col.Type,
					ColIdx:    -1,
				})
				break
			}

			// Less than: field:<"value"
			if val := extractQuotedOp(queryStr, name, `:<"`); val != "" {
				checks = append(checks, PushDownCheck{
					Column:    col.ParquetColumn,
					Op:        PushDownLessThan,
					Value:     val,
					FieldType: col.Type,
					ColIdx:    -1,
				})
				break
			}
		}
	}

	if len(checks) == 0 {
		return nil
	}
	return &PushDownFilter{Checks: checks}
}

// extractQuotedOp finds `fieldName + op + value"` in the query string and returns the value.
// op should end with a quote character (e.g., `:="`).
//
// The field name must match at a token boundary. Without this, a
// `fieldName` like `name` would match as a substring inside
// `service.name:="X"`, building a pushdown check against the wrong
// column (`span.name`, whose internal alias is `name`) and causing
// `ColumnStatsContains("span.name", "X")` to drop every file when X
// isn't in span.name's [min,max] range — the regression class that
// silently zeroed `service.name:="api-gateway"` on cold drilldown.
func extractQuotedOp(query, fieldName, op string) string {
	pattern := fieldName + op
	from := 0
	for {
		idx := strings.Index(query[from:], pattern)
		if idx < 0 {
			return ""
		}
		abs := from + idx
		// Field-name boundary check: pattern must start at the query
		// beginning OR be preceded by a non-identifier character. LogsQL
		// field-name characters are [A-Za-z0-9_.:-]; anything else marks
		// a token boundary (whitespace, AND/OR keywords' surrounding
		// space, parentheses, `{`, `"`, the comma in `in(...)`).
		ok := abs == 0
		if !ok {
			c := query[abs-1]
			isIdent := c == '_' || c == '.' || c == ':' || c == '-' ||
				(c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
			ok = !isIdent
		}
		if ok {
			start := abs + len(pattern)
			end := strings.Index(query[start:], `"`)
			if end < 0 {
				return ""
			}
			return query[start : start+end]
		}
		from = abs + 1
		if from >= len(query) {
			return ""
		}
	}
}

// resolvePushDownIndices pre-computes column indices for pushdown checks.
func resolvePushDownIndices(f *parquet.File, pdf *PushDownFilter) *PushDownFilter {
	if pdf == nil {
		return nil
	}
	resolved := &PushDownFilter{Checks: make([]PushDownCheck, 0, len(pdf.Checks))}
	for _, check := range pdf.Checks {
		check.ColIdx = findColumnIndex(f.Root(), check.Column)
		resolved.Checks = append(resolved.Checks, check)
	}
	return resolved
}

// rowGroupMatchesFilter checks whether a row group might contain rows matching
// the push-down filter by examining column statistics (min/max per page).
// Returns false only when stats definitively prove no match is possible.
func rowGroupMatchesFilter(f *parquet.File, rg parquet.RowGroup, pdf *PushDownFilter) bool {
	if pdf == nil || len(pdf.Checks) == 0 {
		return true
	}

	cols := rg.ColumnChunks()

	for _, check := range pdf.Checks {
		colIdx := check.ColIdx
		if colIdx < 0 {
			colIdx = findColumnIndex(f.Root(), check.Column)
		}
		if colIdx < 0 || colIdx >= len(cols) {
			continue
		}

		cidx, err := cols[colIdx].ColumnIndex()
		if err != nil || cidx == nil {
			continue
		}

		numPages := cidx.NumPages()
		if numPages == 0 {
			continue
		}

		isNumeric := check.FieldType == schema.TypeInt32 ||
			check.FieldType == schema.TypeInt64 ||
			check.FieldType == schema.TypeTimestampNano

		if isNumeric {
			minVal := valueToInt64(cidx.MinValue(0))
			maxVal := valueToInt64(cidx.MaxValue(0))
			for p := 1; p < numPages; p++ {
				if v := valueToInt64(cidx.MinValue(p)); v < minVal {
					minVal = v
				}
				if v := valueToInt64(cidx.MaxValue(p)); v > maxVal {
					maxVal = v
				}
			}
			if !checkMatchesStatsNumeric(check, minVal, maxVal) {
				return false
			}
		} else {
			rgMin := valueToString(cidx.MinValue(0))
			rgMax := valueToString(cidx.MaxValue(numPages - 1))
			for p := 1; p < numPages; p++ {
				pageMin := valueToString(cidx.MinValue(p))
				pageMax := valueToString(cidx.MaxValue(p))
				if pageMin < rgMin {
					rgMin = pageMin
				}
				if pageMax > rgMax {
					rgMax = pageMax
				}
			}
			if !checkMatchesStats(check, rgMin, rgMax) {
				return false
			}
		}
	}

	for _, check := range pdf.Checks {
		if check.Op != PushDownExact && check.Op != PushDownPrefix {
			continue
		}
		colIdx := check.ColIdx
		if colIdx < 0 {
			colIdx = findColumnIndex(f.Root(), check.Column)
		}
		if colIdx < 0 || colIdx >= len(cols) {
			continue
		}
		if !dictionaryContainsMatch(cols[colIdx], check) {
			return false
		}
	}

	return true
}

// dictionaryContainsMatch reads the first page of a column chunk and, if it has
// a dictionary, checks whether any dictionary entry matches the predicate.
// Returns true (conservative) if the column is not dictionary-encoded or if any
// dictionary entry matches.
func dictionaryContainsMatch(cc parquet.ColumnChunk, check PushDownCheck) bool {
	pages := cc.Pages()
	defer func() { _ = pages.Close() }()

	page, err := pages.ReadPage()
	if err != nil || page == nil {
		return true
	}

	dict := page.Dictionary()
	if dict == nil {
		return true
	}

	n := dict.Len()
	if n > 10000 {
		return true
	}

	for i := 0; i < n; i++ {
		v := dict.Index(int32(i))
		s := parquetValueToString(v)
		switch check.Op {
		case PushDownExact:
			if s == check.Value {
				return true
			}
		case PushDownPrefix:
			if strings.HasPrefix(s, check.Value) {
				return true
			}
		}
	}
	return false
}

func parquetValueToString(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}
	switch v.Kind() {
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

func checkMatchesStatsNumeric(check PushDownCheck, rgMin, rgMax int64) bool {
	val, err := strconv.ParseInt(check.Value, 10, 64)
	if err != nil {
		return true
	}
	switch check.Op {
	case PushDownExact:
		return val >= rgMin && val <= rgMax
	case PushDownGreaterThan:
		return rgMax > val
	case PushDownLessThan:
		return rgMin < val
	default:
		return true
	}
}

// checkMatchesStats evaluates a single push-down check against column min/max stats.
// Returns false if the check definitively cannot match any value in [rgMin, rgMax].
func checkMatchesStats(check PushDownCheck, rgMin, rgMax string) bool {
	switch check.Op {
	case PushDownExact:
		// Value must be within [min, max] range (lexicographic)
		return check.Value >= rgMin && check.Value <= rgMax

	case PushDownGreaterThan:
		// At least one value must be > threshold, so max must be > threshold
		return rgMax > check.Value

	case PushDownLessThan:
		// At least one value must be < threshold, so min must be < threshold
		return rgMin < check.Value

	case PushDownPrefix:
		// The prefix range [prefix, prefix+) must overlap [min, max]
		// A prefix matches if: prefix <= max AND prefixSuccessor > min
		// where prefixSuccessor is the next string after all strings starting with prefix
		prefix := check.Value
		if prefix > rgMax {
			return false
		}
		// Check if any value starting with prefix could be >= min
		// If prefixSuccessor <= min, then no match possible
		successor := prefixSuccessor(prefix)
		if successor != "" && successor <= rgMin {
			return false
		}
		return true

	default:
		return true
	}
}

// prefixSuccessor returns the lexicographic successor of a prefix.
// For "abc" it returns "abd". For strings ending with 0xFF bytes, it truncates.
// Returns "" if no successor exists (all 0xFF).
func prefixSuccessor(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1])
		}
	}
	return "" // all 0xFF, no successor
}

func containsOrOperator(query string) bool {
	inQuote := false
	for i := 0; i < len(query); i++ {
		if query[i] == '"' {
			inQuote = !inQuote
		}
		if !inQuote && i+4 <= len(query) {
			sub := query[i : i+4]
			if sub == " or " || sub == " OR " {
				return true
			}
		}
	}
	return false
}

func isNegatedPredicate(query, fieldName string) bool {
	idx := strings.Index(query, fieldName)
	if idx < 0 {
		return false
	}
	if idx > 0 && query[idx-1] == '!' {
		return true
	}
	prefix := strings.TrimRight(query[:idx], " ")
	if strings.HasSuffix(prefix, "NOT") {
		return true
	}
	after := query[idx+len(fieldName):]
	if strings.HasPrefix(after, ":!~") || strings.HasPrefix(after, ":!") {
		return true
	}
	return false
}
