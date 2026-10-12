package parquets3

import (
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The registries that format a row's values the way a Storage of each signal
// does (field_names lists a field only when its formatted value is not empty).
var (
	presenceLogsRegistry   = schema.NewRegistry(schema.LogsProfile)
	presenceTracesRegistry = schema.NewRegistry(schema.TracesProfile)
)

// FieldPresenceComputers are the row-to-facet functions of this binary's read
// path, for the shared compactor and delete rewriter (fieldpresence.SetComputers).
func FieldPresenceComputers() fieldpresence.Computers {
	return fieldpresence.Computers{
		Logs:   presenceOfLogRows,
		Traces: presenceOfTraceRows,
	}
}

// attrMap is one MAP column of a row.
type attrMap struct {
	col string
	m   map[string]string
}

// presenceSpec describes one row type to presenceOfRows.
type presenceSpec[T any] struct {
	reg *schema.Registry
	// toFields is the cold read path's row-to-field emitter, with the file's slot
	// remap; strip clears the MAP columns of the copy it is given, which the
	// emitter would otherwise expand under its own (typed-reader) naming.
	toFields func(*T, []field) []field
	strip    func(*T)
	maps     func(*T) [3]attrMap
	// scalarCols are the scalar Parquet columns a read lists; a MAP key that
	// spells one is not a second field.
	scalarCols map[string]bool
	// pattern is the set of non-empty string columns (fieldpresence.LogPattern).
	// extra, when set, names the fields that depend on a column's content and not
	// only on its being non-empty (the span event and link JSON); strip clears
	// those columns too.
	pattern          func(*T) uint64
	extra            func(*T, func(name string))
	streamID, stream func(*T) string
	timeNs           func(*T) int64
}

// presenceOfLogRows is the field-presence facet of rows being written with the
// given Tier-2 slot binding: per stream-day, the rows and the field names a
// field_names read of the object lists.
func presenceOfLogRows(rows []schema.LogRow, slots schema.SlotMapping) *fieldpresence.Builder {
	return presenceOfRows(rows, presenceSpec[schema.LogRow]{
		reg:      presenceLogsRegistry,
		toFields: withFileSlots(logRowToFields, slots),
		strip:    func(r *schema.LogRow) { r.ResourceAttributes, r.LogAttributes, r.ScopeAttributes = nil, nil, nil },
		maps: func(r *schema.LogRow) [3]attrMap {
			return [3]attrMap{{"resource.attributes", r.ResourceAttributes}, {"log.attributes", r.LogAttributes}, {"scope.attributes", r.ScopeAttributes}}
		},
		scalarCols: presenceScalarColumns[schema.LogRow](),
		pattern:    fieldpresence.LogPattern,
		streamID:   func(r *schema.LogRow) string { return r.StreamID },
		stream:     func(r *schema.LogRow) string { return r.Stream },
		timeNs:     func(r *schema.LogRow) int64 { return r.TimestampUnixNano },
	})
}

// presenceOfTraceRows is presenceOfLogRows for spans.
func presenceOfTraceRows(rows []schema.TraceRow, slots schema.SlotMapping) *fieldpresence.Builder {
	return presenceOfRows(rows, presenceSpec[schema.TraceRow]{
		reg:      presenceTracesRegistry,
		toFields: withFileSlots(traceRowToFields, slots),
		strip: func(r *schema.TraceRow) {
			r.ResourceAttributes, r.SpanAttributes, r.ScopeAttributes = nil, nil, nil
			r.EventsJSON, r.LinksJSON = "", ""
		},
		maps: func(r *schema.TraceRow) [3]attrMap {
			return [3]attrMap{{"resource.attributes", r.ResourceAttributes}, {"span.attributes", r.SpanAttributes}, {"scope.attributes", r.ScopeAttributes}}
		},
		scalarCols: presenceScalarColumns[schema.TraceRow](),
		pattern:    fieldpresence.TracePattern,
		extra: func(r *schema.TraceRow, add func(string)) {
			for _, c := range [2]struct{ col, json string }{{schema.ColSpanEventsJSON, r.EventsJSON}, {schema.ColSpanLinksJSON, r.LinksJSON}} {
				if c.json == "" {
					continue
				}
				_ = schema.ForEachSpanSubField(c.col, c.json, func(name, value string) {
					if value != "" {
						add(name)
					}
				})
			}
		},
		streamID: func(r *schema.TraceRow) string { return r.StreamID },
		stream:   func(r *schema.TraceRow) string { return r.Stream },
		timeNs:   func(r *schema.TraceRow) int64 { return r.TimestampUnixNano },
	})
}

// presenceScalarColumns are the scalar (non-MAP) Parquet columns of a row type
// that a read lists: the names the cold readers pass to the MAP expansion as
// "promoted", so an attribute key that spells one is not a second field.
func presenceScalarColumns[T any]() map[string]bool {
	out := map[string]bool{}
	for _, path := range parquet.SchemaOf(new(T)).Columns() {
		if len(path) == 1 && !schema.IsInternalColumn(path[0]) {
			out[path[0]] = true
		}
	}
	return out
}

// maxPresencePatterns bounds the pattern cache: past it a pattern is computed
// and not kept, so adversarial data cannot grow it with the row count.
const maxPresencePatterns = 4096

// presenceOfRows computes the facet of rows through the cold read path's own
// naming. The names of a row depend on which of its string columns are not empty
// (a field counts when its formatted value is not empty; a number always is) and
// on its attribute keys, so:
//
//   - the scalar columns' names come from the read path's row-to-field emitter
//     (toFields), run once for each pattern of non-empty columns and not for each
//     row (running it per row costs 30 % of a flush: it boxes every value);
//   - each attribute key goes through mapAttrFieldName, the rule the columnar
//     reader applies to every MAP key, once per distinct key.
//
// A name must pass emittableFieldName. The counter's block key is the row's
// _stream_id, else its _stream, plus the UTC day of its _time
// (storage.EmitFieldNamesStreamBlocks).
func presenceOfRows[T any](rows []T, sp presenceSpec[T]) *fieldpresence.Builder {
	b := fieldpresence.NewBuilder()
	patterns := make(map[uint64][]int)
	extraIDs := make(map[string]int)
	var attrIDs [3]map[string]int
	for i := range attrIDs {
		attrIDs[i] = make(map[string]int)
	}
	var scratch T
	var fields []field
	var extra []int
	for i := range rows {
		r := &rows[i]
		pat := sp.pattern(r)
		base, ok := patterns[pat]
		if !ok {
			scratch = *r
			sp.strip(&scratch)
			fields = sp.toFields(&scratch, fields[:0])
			base = presenceScalarIDs(b, sp.reg, fields)
			if len(patterns) < maxPresencePatterns {
				patterns[pat] = base
			}
		}
		extra = extra[:0]
		if sp.extra != nil {
			sp.extra(r, func(name string) {
				id, ok := extraIDs[name]
				if !ok {
					id = -1
					if emittableFieldName(name) {
						id = b.NameID(name)
					}
					extraIDs[strings.Clone(name)] = id
				}
				if id >= 0 {
					extra = append(extra, id)
				}
			})
		}
		for ci, am := range sp.maps(r) {
			for k, v := range am.m {
				if v == "" {
					continue
				}
				id, ok := attrIDs[ci][k]
				if !ok {
					id = -1
					if name, ok := presenceMapName(sp.reg, sp.scalarCols, am.col, k); ok {
						id = b.NameID(name)
					}
					attrIDs[ci][strings.Clone(k)] = id
				}
				if id >= 0 {
					extra = append(extra, id)
				}
			}
		}
		b.AddRowIDs(presenceDay(sp.reg, sp.timeNs(r)), sp.streamID(r), sp.stream(r), base, extra)
	}
	return b
}

// presenceScalarIDs are the builder ids of the names the emitter produced for a
// row whose MAP columns were cleared.
func presenceScalarIDs(b *fieldpresence.Builder, reg *schema.Registry, fields []field) []int {
	var ids []int
	seen := map[int]bool{}
	for _, f := range fields {
		// Formatting a string gives the string, any other value a non-empty text.
		if s, isStr := f.value.(string); isStr && s == "" {
			continue
		}
		if !emittableFieldName(f.name) {
			continue
		}
		name := f.name
		if !reg.IsPromoted(name) {
			// A column the typed emitter names by its Parquet name where the
			// columnar reader (queryFieldName) names it by the registry's.
			if m := reg.ResolveFromParquet(name); m != nil {
				name = m.InternalName
			}
		}
		if id := b.NameID(name); !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

// presenceDay is the UTC day of a row's _time, as the counter derives it from
// the formatted time's first ten characters.
func presenceDay(reg *schema.Registry, ns int64) int32 {
	if ns >= 0 {
		return int32(ns / (86400 * 1e9))
	}
	return fieldpresence.DayOf(reg.FormatField("_time", ns))
}

// presenceMapName is the name the columnar reader gives a MAP column's key, and
// whether it lists one: a key that spells a scalar column is that column's, not a
// second field (a "body" key is listed, as the reader does).
func presenceMapName(reg *schema.Registry, scalarCols map[string]bool, col, key string) (string, bool) {
	if scalarCols[key] && key != "body" {
		return "", false
	}
	return mapAttrFieldName(col, key, reg.TopLevelMapKeys())
}
