package parquets3

import (
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The field_names truth of the performance matrix, computed from the spans the
// generator wrote and from nothing the storage code produces: no RunQuery, no
// buffer-bridge conversion (traceRowsToDataBlock), no schema registry. It is
// the names VictoriaTraces lists for a span (resource_attr:<k>, span_attr:<k>,
// name, duration, ...), each credited with the matching spans of the stream
// blocks that list it, which is the rule of upstream's field_names pipe.
//
// fmtSpanFields is the table of those names for the fields the generator sets
// (fmtGeneratedSpanFields guards that no other field of a span is set). The
// names are those a hot VictoriaTraces v0.12.0 lists for an OTLP span of this
// shape (checked on a hot instance on 2026-10-12: it lists, for a span with a
// trace id, span id, name, a service.name resource attribute and a start and
// end time, _msg, _stream, _stream_id, _time, dropped_attributes_count,
// dropped_events_count, dropped_links_count, duration, end_time_unix_nano,
// flags, kind, name, resource_attr:service.name, span_id, start_time_unix_nano,
// status_code and trace_id, and no service.name; the generator's rows carry no
// message, no end time, no flags and no dropped counts, so those names are not
// in the table).
type fmtSpanField struct {
	name    string
	present func(*schema.TraceRow) bool
}

func fmtAlways(*schema.TraceRow) bool { return true }

func fmtSpanFields() []fmtSpanField {
	return []fmtSpanField{
		{"_time", fmtAlways},
		{"_stream", func(r *schema.TraceRow) bool { return r.Stream != "" }},
		{"_stream_id", func(r *schema.TraceRow) bool { return r.StreamID != "" }},
		{"trace_id", func(r *schema.TraceRow) bool { return r.TraceID != "" }},
		{"span_id", func(r *schema.TraceRow) bool { return r.SpanID != "" }},
		{"name", func(r *schema.TraceRow) bool { return r.SpanName != "" }},
		{"resource_attr:service.name", func(r *schema.TraceRow) bool { return r.ServiceName != "" }},
		// A span always carries these, with value 0 when nothing else is said.
		{"duration", fmtAlways},
		{"kind", fmtAlways},
		{"status_code", fmtAlways},
		{"start_time_unix_nano", fmtAlways},
	}
}

// fmtGeneratedSpanFields returns the fields of a schema.TraceRow the generator
// (fmtSlotRows) sets. A new one has to be added to fmtSpanFields (with the name
// VictoriaTraces lists it under) before the oracle accepts the generator.
func fmtGeneratedSpanFields() map[string]bool {
	return map[string]bool{
		"TimestampUnixNano": true, "StartTimeUnixNano": true, "TraceID": true, "SpanID": true,
		"SpanName": true, "ServiceName": true, "DurationNs": true, "Stream": true, "StreamID": true,
	}
}

// fmtSpanNamesTruth is the field_names answer for the spans of rows with a
// timestamp in [w.lo, w.hi] that pass the filter ("svc": the resource
// attribute service.name equals fmtFilterSvc, spelled
// "resource_attr:service.name":="svc-a" on a hot instance):
//
//   - the spans are grouped in stream blocks, one per stream and UTC day (a
//     stream's parts are one block on a merged hot storage);
//   - a block lists the names some span of the block in the window has;
//   - a name is credited with the matching spans of the blocks that list it.
func fmtSpanNamesTruth(rows []schema.TraceRow, filter string, w fmtWindow) map[string]uint64 {
	type block struct {
		names    map[string]bool
		matching uint64
	}
	blocks := make(map[string]*block)
	for i := range rows {
		r := &rows[i]
		if r.TimestampUnixNano < w.lo || r.TimestampUnixNano > w.hi {
			continue
		}
		key := r.Stream + "|" + time.Unix(0, r.TimestampUnixNano).UTC().Format("2006-01-02")
		b := blocks[key]
		if b == nil {
			b = &block{names: make(map[string]bool)}
			blocks[key] = b
		}
		for _, f := range fmtSpanFields() {
			if f.present(r) {
				b.names[f.name] = true
			}
		}
		if filter != "svc" || r.ServiceName == fmtFilterSvc {
			b.matching++
		}
	}
	out := make(map[string]uint64)
	for _, b := range blocks {
		if b.matching == 0 {
			continue
		}
		for name := range b.names {
			out[name] += b.matching
		}
	}
	return out
}
