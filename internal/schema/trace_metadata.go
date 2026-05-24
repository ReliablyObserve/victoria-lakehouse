package schema

// VTTopLevelSpanAttrKeys lists OTLP metadata field names that VT stores as
// top-level VL LogRow fields (without any prefix). LH stores them inside
// the span.attributes MAP for Parquet persistence but must emit and index
// them WITHOUT the span_attr: prefix so field_names/query responses match VT.
var VTTopLevelSpanAttrKeys = map[string]bool{
	"end_time_unix_nano":       true,
	"start_time_unix_nano":     true,
	"trace_state":              true,
	"flags":                    true,
	"dropped_attributes_count": true,
	"dropped_events_count":     true,
	"dropped_links_count":      true,
	"scope_version":            true,
	"_msg":                     true,
}
