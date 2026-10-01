package parquets3

import (
	"context"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// neededFieldsKey carries the query's needed-field list down to queryFile.
type neededFieldsKey struct{}

// withNeededFields attaches the field names a query needs (see
// logstorage.GetQueryNeededFields) to ctx so every per-file read projects the
// same, AST-derived column set. A context without the value reads every column.
func withNeededFields(ctx context.Context, fields []string) context.Context {
	return context.WithValue(ctx, neededFieldsKey{}, fields)
}

// neededFieldsFrom returns the list attached by withNeededFields, or nil when
// none was attached (callers then read every column).
func neededFieldsFrom(ctx context.Context) []string {
	v, _ := ctx.Value(neededFieldsKey{}).([]string)
	return v
}

// neededColumns maps the field names a query needs onto the Parquet columns to
// read. It returns nil — read every column — when the list is empty, contains
// the "*" wildcard (a pipe or filter that needs fields it cannot name), or ends
// in a prefix wildcard. Otherwise it returns the timestamp column plus, for each
// field, the column that carries it.
//
// The list comes from the PARSED query (logstorage.GetQueryNeededFields), which
// asks upstream's filter and pipe implementations what they read — never from
// the query text. Text heuristics dropped `_msg` whenever a `_time:` term, a
// stream selector or another field term sat next to a default-field filter
// (`_msg:="x"` is printed as `="x"`), so a filtered `| stats count()` ran its
// filter on a block without the body and counted 0 rows from cold.
func neededColumns(reg *schema.Registry, fields []string) map[string]bool {
	if len(fields) == 0 {
		return nil
	}
	cols := map[string]bool{reg.TimestampColumn(): true}
	for _, name := range fields {
		if name == "*" || strings.HasSuffix(name, "*") {
			return nil
		}
		addFieldColumns(reg, name, cols)
	}
	return cols
}

// addFieldColumns adds every Parquet column that can carry the LogsQL field
// name to cols. A promoted field (by its LogsQL or its Parquet spelling) and a
// prefixed attribute (`resource_attr:x`, `span_attr:x`, ...) resolve to one
// column. A bare name that is neither can live in any attribute MAP column or
// in a Tier-2 spare slot (whose binding differs per file), so all of those are
// read: correctness over projection savings.
func addFieldColumns(reg *schema.Registry, name string, cols map[string]bool) {
	if name == "" {
		name = "_msg"
	}
	fm := reg.ResolveToParquet(name)
	if fm == nil {
		return
	}
	cols[fm.ParquetColumn] = true
	if fm.Origin == schema.OriginPromoted || fm.MapKey != name {
		return
	}
	for _, mc := range reg.MapColumns() {
		cols[mc] = true
	}
	for _, slot := range schema.DedicatedSlotColumns {
		cols[slot] = true
	}
}

func hasColumnSelectingPipe(query string) bool {
	idx := strings.Index(query, " | ")
	if idx < 0 {
		return false
	}
	pipes := query[idx:]
	selectingPipes := []string{" | fields ", " | stats ", " | uniq ", " | top "}
	for _, p := range selectingPipes {
		if strings.Contains(pipes, p) {
			return true
		}
	}
	return false
}

// hasContentFilter reports whether the query carries a row filter that must be
// evaluated against row columns at scan time — anything beyond the implicit
// `_time:[...]` range VL prepends to every query and a bare `*` wildcard. Used to
// keep the timestamp-only projection reduction from dropping columns a filter
// needs (notably _msg for a free-text word filter, which has no bloom pushdown).
func hasContentFilter(filterPart string) bool {
	s := stripTimeRange(filterPart)
	return s != "" && s != "*"
}

// stripTimeRange removes the leading `_time:[...]` range term VL prepends to
// every query, returning the remaining filter expression.
func stripTimeRange(filterPart string) string {
	s := strings.TrimSpace(filterPart)
	if strings.HasPrefix(s, "_time:[") {
		if i := strings.IndexByte(s, ']'); i >= 0 {
			s = strings.TrimSpace(s[i+1:])
		}
	}
	return s
}
