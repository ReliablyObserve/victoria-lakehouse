package storage

import "strings"

// IsTraceShapedStream reports whether the canonical stream tag string
// was emitted by VT's ingest path rather than VL's log ingest path.
//
// VT spans, service-graph background rows, and the OTLP/protoparser
// path all build stream tags that VL would never produce — they use
// the `resource_attr:` prefix on resource attributes (VL strips
// prefixed keys via `_stream_fields` enforcement), use bare `name=`
// as the partition dimension (VL never partitions logs on operation
// name), or set `trace_service_graph_stream=` (VT-internal marker).
// Any one of those three signals is enough to classify a row as
// "not a log" from the LogsProfile's perspective.
//
// The function is the single source of truth for the classification
// — both the read-side LogsProfile filter (drop trace-shaped rows
// from query results) and the write-side ingest filter (drop
// trace-shaped rows before they hit the writer) consult it. Lives
// in `internal/storage` so neither path imports the other and the
// helper stays out of the parquets3 storage package (which would
// pull a wider dependency tree into the insert path).
//
// Empty stream is treated as log-shaped — synthetic blocks emitted
// from manifest fast-paths or stats short-circuits don't have a
// `_stream` value and shouldn't be dropped on their account.
func IsTraceShapedStream(stream string) bool {
	if stream == "" {
		return false
	}
	if strings.Contains(stream, "resource_attr:") {
		return true
	}
	if strings.HasPrefix(stream, `{name="`) {
		return true
	}
	if strings.Contains(stream, "trace_service_graph_stream=") {
		return true
	}
	return false
}
