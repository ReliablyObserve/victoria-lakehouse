package vtstorageadapter

import (
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

// canonicalTraceIndexQueryStr is the literal shape VT issues against
// the trace-by-ID lookup. Kept as a string (not via makeTraceIndexQuery)
// because fuzz seeds run before the testing.T is in scope.
func canonicalTraceIndexQueryStr(traceID string, bucket uint64) string {
	return `{` + otelpb.TraceIDIndexStreamName + `="` + strconv.FormatUint(bucket, 10) + `"} AND ` +
		otelpb.TraceIDIndexFieldName + `:="` + traceID + `" | stats min(_time) _time, ` +
		`min(` + otelpb.TraceIDIndexStartTimeFieldName + `) ` + otelpb.TraceIDIndexStartTimeFieldName + `, ` +
		`max(` + otelpb.TraceIDIndexEndTimeFieldName + `) ` + otelpb.TraceIDIndexEndTimeFieldName
}

// canonicalSpanScanRewrittenStr is the post-rewriteTraceIndexQuery shape
// — feeding it back through the detector/rewriter must not panic.
func canonicalSpanScanRewrittenStr(traceID string) string {
	return `trace_id:="` + traceID + `" | stats min(_time) _time, min(start_time_unix_nano) start_time, max(end_time_unix_nano) end_time`
}

// FuzzTraceIndexLookupTraceID asserts the index-shape detector never
// panics on any *logstorage.Query that the upstream parser will accept.
// The function inspects q.String() with substring-based heuristics; a
// malformed or adversarial query string (e.g. trace_id_idx:= embedded
// inside a string literal) must not crash the detector.
func FuzzTraceIndexLookupTraceID(f *testing.F) {
	// Canonical VT shape — the happy path.
	f.Add(canonicalTraceIndexQueryStr("abc123", 42))
	f.Add(canonicalTraceIndexQueryStr("", 0))
	f.Add(canonicalTraceIndexQueryStr("deadbeef", 1<<32))

	// Span-scan rewritten output — must be untouched by the detector
	// (no trace_id_idx token, just trace_id).
	f.Add(canonicalSpanScanRewrittenStr("abc"))

	// Non-trace-index queries that share a token prefix or contain the
	// marker token inside string literals — these must be safe.
	f.Add(`service.name:="api-gateway"`)
	f.Add(`trace_id:="abc"`)
	f.Add(`*`)
	f.Add(`"trace_id_idx:=lookalike"`)
	f.Add(`message:"trace_id_idx:=fakeval"`)
	f.Add(`service.name:="trace_id_idx:=embedded"`)

	// Trace-index marker without a value / with weird value boundaries.
	f.Add(`trace_id_idx:=`)
	f.Add(`trace_id_idx:="`)
	f.Add(`trace_id_idx:=unquoted-id`)
	f.Add(`trace_id_idx:=unquoted-id | stats count()`)
	f.Add(`trace_id_idx:=value)`)
	f.Add(`trace_id_idx:="value with spaces and | pipe"`)

	// Empty / pathological.
	f.Add(``)
	f.Add(` `)
	f.Add(`\x00`)

	f.Fuzz(func(t *testing.T, s string) {
		q, err := logstorage.ParseQueryAtTimestamp(s, 1)
		if err != nil {
			// Unparseable input — the detector is never reached in
			// production for these. Skip.
			return
		}
		// Both forms — nil-guarded and the live parsed query — must
		// stay panic-free. The bool/string returns can be anything; we
		// only require liveness.
		_, _ = traceIndexLookupTraceID(nil)
		_, _ = traceIndexLookupTraceID(q)
	})
}

// FuzzRewriteTraceIndexQuery asserts the query-rewrite path is
// panic-free on any parseable LogsQL. rewriteTraceIndexQuery does a
// String() + substring extraction + ParseQueryAtTimestamp(rewritten)
// round-trip; a malformed extracted trace ID must not crash the
// re-parse step.
func FuzzRewriteTraceIndexQuery(f *testing.F) {
	f.Add(canonicalTraceIndexQueryStr("abc123", 42))
	f.Add(canonicalSpanScanRewrittenStr("abc123"))
	f.Add(`trace_id_idx:="hex-only-id"`)
	f.Add(`trace_id_idx:=unquoted`)
	f.Add(`{trace_id_idx_stream="0"} AND trace_id_idx:="x"`)
	f.Add(`service.name:="x"`)
	f.Add(`*`)
	f.Add(``)
	// Trace ID containing characters that would break a naive
	// fmt-style %q-vs-bareword rewrite if the extractor regresses.
	f.Add(`trace_id_idx:="id\"with\"quotes"`)
	f.Add(`trace_id_idx:="id with spaces"`)
	f.Add(`trace_id_idx:="| stats count()"`)

	f.Fuzz(func(t *testing.T, s string) {
		q, err := logstorage.ParseQueryAtTimestamp(s, 1)
		if err != nil {
			return
		}
		rewritten, ok := rewriteTraceIndexQuery(q)
		// Either return shape is acceptable; we only require liveness.
		_ = rewritten
		_ = ok
	})
}
