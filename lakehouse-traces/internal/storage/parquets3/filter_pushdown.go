package parquets3

import (
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
	Column string
	Op     PushDownOp
	Value  string
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

	var checks []PushDownCheck

	for _, col := range registry.PromotedColumns() {
		// Try both internal name and parquet column name
		names := []string{col.InternalName}
		if col.ParquetColumn != col.InternalName {
			names = append(names, col.ParquetColumn)
		}

		for _, name := range names {
			// Exact match: field:="value" or field:="prefix*"
			if val := extractQuotedOp(queryStr, name, `:="`); val != "" {
				if strings.HasSuffix(val, "*") && !strings.HasSuffix(val, `\*`) {
					checks = append(checks, PushDownCheck{
						Column: col.ParquetColumn,
						Op:     PushDownPrefix,
						Value:  val[:len(val)-1], // strip trailing *
					})
				} else {
					checks = append(checks, PushDownCheck{
						Column: col.ParquetColumn,
						Op:     PushDownExact,
						Value:  val,
					})
				}
				break // found match for this column, skip alternate name
			}

			// Greater than: field:>"value"
			if val := extractQuotedOp(queryStr, name, `:>"`); val != "" {
				checks = append(checks, PushDownCheck{
					Column: col.ParquetColumn,
					Op:     PushDownGreaterThan,
					Value:  val,
				})
				break
			}

			// Less than: field:<"value"
			if val := extractQuotedOp(queryStr, name, `:<"`); val != "" {
				checks = append(checks, PushDownCheck{
					Column: col.ParquetColumn,
					Op:     PushDownLessThan,
					Value:  val,
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
func extractQuotedOp(query, fieldName, op string) string {
	pattern := fieldName + op
	idx := strings.Index(query, pattern)
	if idx < 0 {
		return ""
	}
	start := idx + len(pattern)
	end := strings.Index(query[start:], `"`)
	if end < 0 {
		return ""
	}
	return query[start : start+end]
}

// rowGroupMatchesFilter checks whether a row group might contain rows matching
// the push-down filter by examining column statistics (min/max per page).
// Returns true (conservative) if:
//   - filter is nil
//   - column not found in the file
//   - column index unavailable
//   - stats indicate possible match
//
// Returns false only when stats definitively prove no match is possible.
func rowGroupMatchesFilter(f *parquet.File, rg parquet.RowGroup, pdf *PushDownFilter) bool {
	if pdf == nil || len(pdf.Checks) == 0 {
		return true
	}

	cols := rg.ColumnChunks()

	for _, check := range pdf.Checks {
		colIdx := findColumnIndex(f.Root(), check.Column)
		if colIdx < 0 || colIdx >= len(cols) {
			// Column not found — can't skip, be conservative
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

		// Get overall min and max across all pages in this row group
		rgMin := valueToString(cidx.MinValue(0))
		rgMax := valueToString(cidx.MaxValue(numPages - 1))

		// For multi-page row groups, find the true min/max across all pages
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

	return true
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
