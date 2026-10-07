package schema

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// Span events and span links on cold storage.
//
// VictoriaTraces stores every span event and link as extra fields of the span's
// row, one group per index (see app/vtinsert/opentelemetry in VictoriaTraces):
//
//	event:event_time_unix_nano:0   event:event_name:0   event:event_dropped_attributes_count:0
//	event:event_attr:exception.type:0
//	link:link_trace_id:0  link:link_span_id:0  link:link_trace_state:0  link:link_flags:0
//	link:link_dropped_attributes_count:0  link:link_attr:<key>:0
//
// Lakehouse keeps them as two optional string columns, one JSON array per
// span: element i of `span.events_json` holds the fields of event i, named as
// VictoriaTraces names them but without the `event:` prefix and the `:<i>`
// suffix:
//
//	[{"event_name":"exception","event_time_unix_nano":"1700000000000000000",
//	  "event_attr:exception.type":"IOError","event_dropped_attributes_count":"0"}]
//
// All values are strings, exactly the values VictoriaTraces stores (so empty
// attribute values are already "-" and absent fields stay absent). Reading
// the column back regenerates the original VictoriaTraces field names and
// values, so a span read from cold carries the same event and link fields as
// the same span read from hot VictoriaTraces.
//
// Lossless corner cases: a group whose index is not its position (gaps, a
// non-numeric suffix, fields sent through the jsonline path) carries the
// reserved key "$idx" holding the original suffix text, colon included
// (":3", ":x", or "" for a field with no suffix at all). A sub-field name that
// itself starts with "$" is written with one more "$" in front, so it can never
// collide with the reserved key. Elements and keys are written in a
// deterministic order, so the same span produces the same bytes on every write
// path.

// Parquet column names of the span events and links columns.
const (
	ColSpanEventsJSON = "span.events_json"
	ColSpanLinksJSON  = "span.links_json"
)

// Field-name prefixes VictoriaTraces gives span event and link fields
// (otelpb.EventPrefix and otelpb.LinkPrefix; the traces module has a test that
// keeps the two in step).
const (
	EventFieldPrefix = "event:"
	LinkFieldPrefix  = "link:"
)

// idxKey is the reserved element key carrying a non-positional group suffix.
const idxKey = "$idx"

// IsCompositeColumn reports whether a top-level Parquet column is a container
// for several query fields rather than a field itself.
func IsCompositeColumn(parquetColumn string) bool {
	return parquetColumn == ColSpanEventsJSON || parquetColumn == ColSpanLinksJSON
}

// CompositeColumnForField returns the composite column that carries the query
// field name, for the VictoriaTraces event/link field names.
func CompositeColumnForField(field string) (string, bool) {
	switch {
	case strings.HasPrefix(field, EventFieldPrefix):
		return ColSpanEventsJSON, true
	case strings.HasPrefix(field, LinkFieldPrefix):
		return ColSpanLinksJSON, true
	}
	return "", false
}

// compositeFieldPrefix is the VictoriaTraces field prefix of a composite column.
func compositeFieldPrefix(col string) string {
	if col == ColSpanLinksJSON {
		return LinkFieldPrefix
	}
	return EventFieldPrefix
}

// SpanSubFieldCollector gathers the event and link fields of ONE span while
// its fields are mapped onto a TraceRow, then writes the two JSON columns.
// The zero value is ready to use.
type SpanSubFieldCollector struct {
	events, links subGroups
}

// Add takes a field if it is a span event or link field, and reports whether it
// did. name and value are copied.
func (c *SpanSubFieldCollector) Add(name, value string) bool {
	switch {
	case strings.HasPrefix(name, EventFieldPrefix):
		c.events.add(name[len(EventFieldPrefix):], value)
		return true
	case strings.HasPrefix(name, LinkFieldPrefix):
		c.links.add(name[len(LinkFieldPrefix):], value)
		return true
	}
	return false
}

// Apply writes the collected events and links into row.
func (c *SpanSubFieldCollector) Apply(row *TraceRow) {
	row.EventsJSON = c.events.marshal()
	row.LinksJSON = c.links.marshal()
}

type subGroup struct {
	idx      string
	noSuffix bool
	fields   map[string]string
}

type subGroups struct {
	byKey map[string]*subGroup
}

// add files one field (the name after the event:/link: prefix) under its group.
func (g *subGroups) add(rest, value string) {
	sub, idx, noSuffix := rest, "", true
	if i := strings.LastIndexByte(rest, ':'); i >= 0 {
		sub, idx, noSuffix = rest[:i], rest[i+1:], false
	}
	key := idx
	if noSuffix {
		key = "\x00"
	}
	grp, ok := g.byKey[key]
	if !ok {
		if g.byKey == nil {
			g.byKey = make(map[string]*subGroup, 2)
		}
		grp = &subGroup{idx: strings.Clone(idx), noSuffix: noSuffix, fields: make(map[string]string, 6)}
		g.byKey[key] = grp
	}
	if strings.HasPrefix(sub, "$") {
		sub = "$" + sub // escape: keys starting with "$" are reserved
	}
	grp.fields[strings.Clone(sub)] = strings.Clone(value)
}

// marshal returns the JSON array of the groups, or "" when there are none.
func (g *subGroups) marshal() string {
	if len(g.byKey) == 0 {
		return ""
	}
	groups := make([]*subGroup, 0, len(g.byKey))
	for _, grp := range g.byKey {
		groups = append(groups, grp)
	}
	numeric := func(grp *subGroup) (uint64, bool) {
		if grp.noSuffix {
			return 0, false
		}
		v, err := strconv.ParseUint(grp.idx, 10, 64)
		if err != nil || strconv.FormatUint(v, 10) != grp.idx {
			return 0, false
		}
		return v, true
	}
	sort.Slice(groups, func(i, j int) bool {
		vi, ni := numeric(groups[i])
		vj, nj := numeric(groups[j])
		switch {
		case ni && nj:
			return vi < vj
		case ni != nj:
			return ni
		}
		// Neither group has a numeric index: order by suffix text (a group
		// with no suffix first), never by arrival, so the bytes do not depend
		// on the order the fields were sent in.
		if groups[i].noSuffix != groups[j].noSuffix {
			return groups[i].noSuffix
		}
		return groups[i].idx < groups[j].idx
	})
	elems := make([]map[string]string, len(groups))
	for pos, grp := range groups {
		if grp.noSuffix {
			grp.fields[idxKey] = ""
		} else if grp.idx != strconv.Itoa(pos) {
			grp.fields[idxKey] = ":" + grp.idx
		}
		elems[pos] = grp.fields
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(elems); err != nil {
		return ""
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

// ForEachSpanSubField decodes the JSON of a composite column and calls emit
// with every VictoriaTraces field it encodes, in a deterministic order:
// groups in array order, fields by name. It reports an error for content that
// is not what SpanSubFieldCollector.Apply writes; nothing is emitted then.
func ForEachSpanSubField(col, jsonValue string, emit func(name, value string)) error {
	if jsonValue == "" {
		return nil
	}
	var elems []map[string]string
	if err := json.Unmarshal([]byte(jsonValue), &elems); err != nil {
		return err
	}
	prefix := compositeFieldPrefix(col)
	keys := make([]string, 0, 8)
	for pos, el := range elems {
		suffix := ":" + strconv.Itoa(pos)
		if explicit, ok := el[idxKey]; ok {
			suffix = explicit
		}
		keys = keys[:0]
		for k := range el {
			if k != idxKey {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			emit(prefix+strings.TrimPrefix(k, "$")+suffix, el[k])
		}
	}
	return nil
}
