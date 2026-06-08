package parquets3

import (
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// queryColumns returns the set of parquet column names needed for a query.
// Returns nil when all columns should be read (the common case).
//
// pipeFields are field names extracted from VL's parsed pipe operators
// (stats by(), uniq by(), top by(), fields) via logstorage.GetQueryPipeFields.
// Using VL's actual parsed representation avoids duplicating query parsing.
func queryColumns(queryStr string, registry *schema.Registry, pipeFields []string) map[string]bool {
	filterPart := queryStr
	if idx := strings.Index(queryStr, " | "); idx >= 0 {
		filterPart = strings.TrimSpace(queryStr[:idx])
	}

	hasPipes := len(pipeFields) > 0 || hasColumnSelectingPipe(queryStr)

	if (filterPart == "" || filterPart == "*") && !hasPipes {
		return nil
	}

	if !hasPipes {
		return nil
	}

	cols := make(map[string]bool)
	cols[registry.TimestampColumn()] = true

	if isFreeTextSearch(filterPart) {
		cols["body"] = true
	}

	for _, fm := range registry.PromotedColumns() {
		if referencesField(filterPart, fm.InternalName) || referencesField(filterPart, fm.ParquetColumn) {
			cols[fm.ParquetColumn] = true
		}
	}

	// Stream selector `{tag="value", ...}` doesn't carry an explicit
	// `_stream:` prefix when VL serializes a parsed query (e.g. `_stream:{x=y}`
	// becomes just `{x=y}` in q.String()). filterStream.matchRow needs the
	// `_stream` field to be present in the projected DataBlock, so detect the
	// `{...}` shape and add `_stream` to the projection. Without this, the
	// projection-reducing path (pipeFields non-empty) would drop `_stream`
	// and the stream filter would silently match zero rows. Mirror of the
	// fix in lakehouse-traces/internal/storage/parquets3/projection.go.
	if referencesStreamSelector(filterPart) {
		cols["_stream"] = true
	}

	for _, name := range pipeFields {
		if fm := registry.ResolveToParquet(name); fm != nil {
			cols[fm.ParquetColumn] = true
		}
	}

	if len(cols) <= 1 && !isFreeTextSearch(filterPart) && !hasPipes {
		return nil
	}

	return cols
}

// referencesStreamSelector returns true if the filter contains an
// unprefixed stream selector `{...}`. VL's q.String() drops the explicit
// `_stream:` prefix from parsed queries (so `_stream:{x=y}` round-trips
// as `{x=y}`), but filterStream.matchRow still requires the `_stream`
// column to be present in the DataBlock.
//
// The `{` character is special in LogsQL only at the top level of a
// filter expression — it cannot appear inside a field value without
// being part of a quoted string. So a bare `{` at filter scope is a
// reliable stream-selector signal.
func referencesStreamSelector(filterPart string) bool {
	s := strings.TrimSpace(filterPart)
	return strings.Contains(s, "{")
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

// hasContentFilter reports whether the query carries a row filter that must be
// evaluated against row columns at scan time — anything beyond the implicit
// `_time:[...]` range VL prepends to every query and a bare `*` wildcard. Used to
// keep the timestamp-only projection reduction from dropping columns a filter
// needs (notably _msg for a free-text word filter, which has no bloom pushdown).
func hasContentFilter(filterPart string) bool {
	s := strings.TrimSpace(filterPart)
	if strings.HasPrefix(s, "_time:[") {
		if i := strings.IndexByte(s, ']'); i >= 0 {
			s = strings.TrimSpace(s[i+1:])
		}
	}
	return s != "" && s != "*"
}

func isFreeTextSearch(query string) bool {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" || trimmed == "*" {
		return false
	}
	if trimmed[0] == '"' {
		return true
	}
	if !strings.Contains(trimmed, ":") {
		return true
	}
	return false
}
