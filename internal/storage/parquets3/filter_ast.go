package parquets3

import (
	"reflect"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Filter-AST helpers.
//
// VL's logstorage package defines its filter AST as a private interface
// (`filter`) with unexported concrete types (filterAnd, filterOr,
// filterNot, filterGeneric, filterExact, filterIn, filterPhrase, ...).
// The wrapper *logstorage.Filter exposes only String() and MatchRow().
//
// To answer structural questions reliably — without re-tokenizing the
// stringified query and tripping on quoted literals — we walk the AST
// via reflection. We recognize concrete nodes by their unqualified
// type name (e.g. "filterAnd") and read the fields we need by name
// (`filters`, `f`, `fieldName`, `value`, `values`).
//
// This is intentionally narrow: we only implement the questions the
// Lakehouse query path actually asks (does the filter contain OR? is a
// field predicate negated? which exact-match values is this field
// constrained to?). Falling back to the existing string scanners when
// reflection fails preserves behaviour for filter shapes we don't yet
// recognize.

const (
	astTypeAnd     = "filterAnd"
	astTypeOr      = "filterOr"
	astTypeNot     = "filterNot"
	astTypeGeneric = "filterGeneric"
	astTypeExact   = "filterExact"
	astTypeIn      = "filterIn"
	astTypePhrase  = "filterPhrase"
	astTypePrefix  = "filterPrefix"
	astTypeNoop    = "filterNoop"
)

// filterInner returns the unwrapped inner filter value from the public
// *logstorage.Filter wrapper. The wrapper's only field is `f filter`,
// reachable only via reflection.
func filterInner(f *logstorage.Filter) reflect.Value {
	if f == nil {
		return reflect.Value{}
	}
	v := reflect.ValueOf(f).Elem()
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	if v.NumField() == 0 {
		return reflect.Value{}
	}
	return v.Field(0)
}

// astTypeName returns the unqualified type name of a reflected filter
// node (e.g. "filterOr"). Returns "" for non-struct/non-pointer values.
func astTypeName(v reflect.Value) string {
	if !v.IsValid() {
		return ""
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	return v.Type().Name()
}

// walkFilterAST recursively visits every filter node in the tree.
// visit may return false to skip recursion into children.
func walkFilterAST(v reflect.Value, visit func(name string, node reflect.Value) bool) {
	v = derefValue(v)
	name := astTypeName(v)
	if name == "" {
		return
	}
	if !visit(name, v) {
		return
	}
	switch name {
	case astTypeAnd, astTypeOr:
		filters := v.FieldByName("filters")
		if filters.IsValid() && filters.Kind() == reflect.Slice {
			for i := 0; i < filters.Len(); i++ {
				walkFilterAST(filters.Index(i), visit)
			}
		}
	case astTypeNot:
		inner := v.FieldByName("f")
		if inner.IsValid() {
			walkFilterAST(inner, visit)
		}
	case astTypeGeneric:
		inner := v.FieldByName("f")
		if inner.IsValid() {
			walkFilterAST(inner, visit)
		}
	}
}

// derefValue unwraps interface/ptr indirection to expose the concrete
// struct underneath.
func derefValue(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr) {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

// FilterContainsOr returns true if the filter tree contains at least
// one OR node. Used to decide whether predicate push-down is safe (we
// only push exact-match constraints; an OR branch could change the
// answer set).
//
// If the AST walk yields no decisive answer (reflection couldn't
// extract a node) we fall back to a quote-aware string scan of the
// stringified filter, which still avoids the dumb " or "-substring
// problem of the previous regex helper.
func FilterContainsOr(f *logstorage.Filter) bool {
	if f == nil {
		return false
	}
	inner := filterInner(f)
	if astTypeName(derefValue(inner)) == "" {
		return containsOrOperatorQuoted(f.String())
	}
	found := false
	walkFilterAST(inner, func(name string, _ reflect.Value) bool {
		if name == astTypeOr {
			found = true
			return false
		}
		return true
	})
	return found
}

// FilterIsNegated reports whether a predicate on fieldName appears
// under a filterNot anywhere in the filter tree. Negated predicates
// must not be pushed down because file-level filtering inverts match
// semantics.
//
// Falls back to the existing string-based isNegatedPredicate when the
// AST walk fails.
func FilterIsNegated(f *logstorage.Filter, fieldName string) bool {
	if f == nil || fieldName == "" {
		return false
	}
	inner := filterInner(f)
	if astTypeName(derefValue(inner)) == "" {
		return isNegatedPredicate(f.String(), fieldName)
	}
	negated := false
	visit := func(under bool) func(string, reflect.Value) bool {
		return nil
	}
	_ = visit
	// Track NOT context via depth stack: walk uses a closure that toggles
	// the bool on entering/leaving a NOT node.
	var helper func(v reflect.Value, underNot bool)
	helper = func(v reflect.Value, underNot bool) {
		v = derefValue(v)
		name := astTypeName(v)
		switch name {
		case astTypeAnd, astTypeOr:
			filters := v.FieldByName("filters")
			if filters.IsValid() && filters.Kind() == reflect.Slice {
				for i := 0; i < filters.Len(); i++ {
					helper(filters.Index(i), underNot)
				}
			}
		case astTypeNot:
			inner := v.FieldByName("f")
			if inner.IsValid() {
				helper(inner, !underNot)
			}
		case astTypeGeneric:
			fn := stringField(v, "fieldName")
			if fn == fieldName && underNot {
				negated = true
			}
			inner := v.FieldByName("f")
			if inner.IsValid() {
				helper(inner, underNot)
			}
		}
	}
	helper(inner, false)
	return negated
}

// FilterExtractFieldValues returns the set of exact-match or in() values
// constraining fieldName at the top level (i.e. directly under AND/root,
// not under OR — values under OR can't be safely pushed down).
//
// Falls back to extractFilterValues for unrecognized shapes.
func FilterExtractFieldValues(f *logstorage.Filter, fieldName string) []string {
	if f == nil || fieldName == "" {
		return nil
	}
	inner := filterInner(f)
	if astTypeName(derefValue(inner)) == "" {
		return extractFilterValues(f.String(), fieldName)
	}
	var values []string
	var helper func(v reflect.Value)
	helper = func(v reflect.Value) {
		v = derefValue(v)
		name := astTypeName(v)
		switch name {
		case astTypeAnd:
			filters := v.FieldByName("filters")
			if filters.IsValid() && filters.Kind() == reflect.Slice {
				for i := 0; i < filters.Len(); i++ {
					helper(filters.Index(i))
				}
			}
		case astTypeGeneric:
			fn := stringField(v, "fieldName")
			if fn != fieldName {
				return
			}
			innerF := v.FieldByName("f")
			innerF = derefValue(innerF)
			switch astTypeName(innerF) {
			case astTypeExact:
				values = append(values, stringField(innerF, "value"))
			case astTypeIn:
				// filterIn has `values inValues` containing `values []string`.
				vs := innerF.FieldByName("values")
				vs = derefValue(vs)
				if vs.IsValid() && vs.Kind() == reflect.Struct {
					arr := vs.FieldByName("values")
					if arr.IsValid() && arr.Kind() == reflect.Slice {
						for i := 0; i < arr.Len(); i++ {
							s := arr.Index(i).String()
							if s != "" {
								values = append(values, s)
							}
						}
					}
				}
			}
		}
	}
	helper(inner)
	if len(values) == 0 {
		return extractFilterValues(f.String(), fieldName)
	}
	return values
}

// FilterReferencedFields returns the set of field names referenced by
// any predicate anywhere in the filter tree (under AND/OR/NOT, at any
// depth). Used to compute the minimal column projection needed to
// evaluate the filter against a Parquet row — projecting only these
// columns plus the target value column lets the S3 range-read path
// fetch a small fraction of the file's column data instead of the
// whole body.
//
// Returns an empty map when the filter is nil or its AST cannot be
// walked via reflection (extremely rare; covers future filter types
// the helper doesn't yet recognize).
func FilterReferencedFields(f *logstorage.Filter) map[string]bool {
	out := map[string]bool{}
	if f == nil {
		return out
	}
	inner := filterInner(f)
	if astTypeName(derefValue(inner)) == "" {
		return out
	}
	walkFilterAST(inner, func(name string, v reflect.Value) bool {
		if name == astTypeGeneric {
			if fn := stringField(v, "fieldName"); fn != "" {
				out[fn] = true
			}
		}
		return true
	})
	return out
}

// stringField returns the string-typed field by name from a struct
// value, or "" if not present.
func stringField(v reflect.Value, name string) string {
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return ""
	}
	f := v.FieldByName(name)
	if !f.IsValid() || f.Kind() != reflect.String {
		return ""
	}
	return f.String()
}

// parseFilterFromQueryStr parses a stringified query (post q.String())
// into a *logstorage.Filter, stripping any trailing pipes the same way
// parseFilterFromQuery does for a *Query. Returns nil for wildcard /
// time-only inputs or on parse errors.
func parseFilterFromQueryStr(queryStr string) *logstorage.Filter {
	if queryStr == "" {
		return nil
	}
	// Drop pipes outside quotes — same heuristic as the existing helper.
	queryStr = stripPipeOutsideQuotes(queryStr)
	queryStr = strings.TrimSpace(queryStr)
	if queryStr == "" || queryStr == "*" {
		return nil
	}
	f, err := logstorage.ParseFilter(queryStr)
	if err != nil {
		return nil
	}
	return f
}

// containsOrOperatorAST prefers the AST view of the query (correct for
// quoted literals and OR nested under AND) and falls back to the
// quote-aware string scan when parsing fails.
func containsOrOperatorAST(queryStr string) bool {
	if f := parseFilterFromQueryStr(queryStr); f != nil {
		return FilterContainsOr(f)
	}
	return containsOrOperatorQuoted(queryStr)
}

// isNegatedPredicateAST is the AST-aware variant of isNegatedPredicate.
func isNegatedPredicateAST(queryStr, fieldName string) bool {
	if f := parseFilterFromQueryStr(queryStr); f != nil {
		return FilterIsNegated(f, fieldName)
	}
	return isNegatedPredicate(queryStr, fieldName)
}

// extractFilterValuesAST is the AST-aware variant of extractFilterValues.
func extractFilterValuesAST(queryStr, fieldName string) []string {
	if f := parseFilterFromQueryStr(queryStr); f != nil {
		return FilterExtractFieldValues(f, fieldName)
	}
	return extractFilterValues(queryStr, fieldName)
}

// containsOrOperatorQuoted is a quote-aware fallback for
// FilterContainsOr — it walks the string ignoring " or "/" OR "
// occurrences inside quoted literals. This is the same idea as the
// previous containsOrOperator helper but rewritten without false-positive
// risk from substring scanning.
func containsOrOperatorQuoted(query string) bool {
	inQuote := byte(0)
	for i := 0; i < len(query); i++ {
		c := query[i]
		if inQuote != 0 {
			if c == '\\' && i+1 < len(query) {
				i++
				continue
			}
			if c == inQuote {
				inQuote = 0
			}
			continue
		}
		if c == '"' || c == '\'' {
			inQuote = c
			continue
		}
		// Look for " or " / " OR " word-boundary occurrences.
		if c == ' ' && i+3 < len(query) {
			next := query[i+1 : i+4]
			if (next == "or " || next == "OR ") && (i+4 < len(query)) {
				return true
			}
		}
	}
	return strings.Contains(query, " or ") || strings.Contains(query, " OR ")
}
