package parquets3

import (
	"context"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

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

// rowFilterKey marks a query whose rows must be tested individually (a row
// filter, or a tombstone that overlaps the window).
type rowFilterKey struct{}

// withRowFilter records that rows must be read and tested one by one, so no
// metadata-only shortcut that fabricates row values may stand in for them.
func withRowFilter(ctx context.Context, needed bool) context.Context {
	return context.WithValue(ctx, rowFilterKey{}, needed)
}

// rowFilterFrom reports the flag set by withRowFilter (false when absent).
func rowFilterFrom(ctx context.Context) bool {
	v, _ := ctx.Value(rowFilterKey{}).(bool)
	return v
}

type noFooterBloomKey struct{}

// withNoFooterBloom records that the query has a NOT, an OR, or a per-function
// `if (...)` condition: a bloom term inside one does not have to match, so the
// footer-bloom row-group skip must not run on it.
func withNoFooterBloom(ctx context.Context, off bool) context.Context {
	return context.WithValue(ctx, noFooterBloomKey{}, off)
}

func noFooterBloomFrom(ctx context.Context) bool {
	v, _ := ctx.Value(noFooterBloomKey{}).(bool)
	return v
}

type readAllKey struct{}

// withReadAll records that the query's pipes need every field, so the read is
// deliberately unprojected.
func withReadAll(ctx context.Context, all bool) context.Context {
	return context.WithValue(ctx, readAllKey{}, all)
}

func readAllFrom(ctx context.Context) bool {
	v, _ := ctx.Value(readAllKey{}).(bool)
	return v
}

// containsWildcard reports whether a needed-field list means "everything".
func containsWildcard(fields []string) bool {
	for _, f := range fields {
		if f == "*" || strings.HasSuffix(f, "*") {
			return true
		}
	}
	return false
}

// withTombstoneFields adds every field a tombstone's query references to the
// needed-field list. A tombstone is evaluated against the projected block
// (suppressTombstonedRows), so a projection that lacks its field would see the
// field as absent, match nothing, and show the deleted rows again. A list that
// already means "everything" is returned unchanged.
func withTombstoneFields(fields []string, tss []tombstone) []string {
	if len(tss) == 0 {
		return fields
	}
	seen := make(map[string]bool, len(fields))
	for _, f := range fields {
		if f == "*" {
			return fields
		}
		seen[f] = true
	}
	out := append([]string(nil), fields...)
	for i := range tss {
		for name := range FilterReferencedFields(tss[i].Filter()) {
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	return out
}

// countPushdownSound reports whether the manifest count pushdown for aggField
// may answer q. The pushdown fabricates rows from per-file label counts (one
// field plus made-up timestamps), so the pipe chain must start with a pipe that
// consumes those rows directly (stats, uniq, top, fields) and read nothing but
// aggField from them: a rewriting pipe ahead of it, a per-function `if (...)`
// on another field, or `_time` read by a function (`count() if (_time:...)`)
// would see fabricated values. The filter is vetted separately
// (countPushdownFilterFields).
func countPushdownSound(q *logstorage.Query, aggField string) bool {
	if !logstorage.QueryFirstPipeIsAggregate(q) {
		return false
	}
	for _, f := range logstorage.GetQueryPipeNeededFields(q) {
		if f != aggField {
			return false
		}
	}
	return true
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

// referencesField reports whether the query text spells name as a field term.
// It is NOT used to choose which columns to read (neededColumns does that from
// the parsed query); it only decides which promoted Parquet column spellings
// are also emitted as alias fields (queryParquetNameAliases).
func referencesField(query, name string) bool {
	// VL serializes field names that contain `:` or other special chars
	// (e.g. `span_attr:http.status_code`) with surrounding double quotes:
	// `"span_attr:http.status_code":=200`. Detect both the bare and
	// quoted forms.
	patterns := []string{
		name + `:="`,
		name + `:"`,
		name + `:=`,
		name + `:in(`,
		name + `:`,
		`"` + name + `":=`,
		`"` + name + `":`,
	}
	for _, p := range patterns {
		if strings.Contains(query, p) {
			return true
		}
	}
	return false
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
