package storage

import "testing"

// TestIsTraceShapedStream pins the trace/log classifier behavior at
// the shared-helper layer so the read-side LogsProfile filter and
// the write-side ingest gate stay in lockstep. The two paths share
// this single function — a regression here silently re-opens the
// trace-shape data quality issue tracked under task #70.
func TestIsTraceShapedStream(t *testing.T) {
	cases := []struct {
		stream string
		want   bool
		why    string
	}{
		// Legitimate log streams — must NOT be classified as trace.
		{`{service.name="api-gateway",level="INFO"}`, false, "canonical log stream"},
		{`{k8s.node.name="node-pool-a-1",service.name="api"}`, false, "k8s.node.name contains substring name= but is not bare name= at start"},
		{`{host.name="ip-10-0-0-1",service.name="api"}`, false, "host.name false-positive guard"},
		{``, false, "empty stream — synthetic blocks (e.g. handle404Recovery) pass through"},
		{`{}`, false, "explicit empty tag set"},

		// VT-shape streams — MUST be classified as trace.
		{`{resource_attr:service.name="api-gateway"}`, true, "VT prefixed resource attribute"},
		{`{name="HTTP GET",resource_attr:service.name="user-service"}`, true, "VT span with operation partition"},
		{`{name="DB INSERT orders"}`, true, "bare name= at start = VT operation partition"},
		{`{trace_service_graph_stream="-"}`, true, "VT service-graph background task marker"},
		{`{trace_service_graph_stream="other-tenant"}`, true, "service-graph marker per-tenant variant"},
	}
	for _, c := range cases {
		got := IsTraceShapedStream(c.stream)
		if got != c.want {
			t.Errorf("IsTraceShapedStream(%q) = %v, want %v — %s", c.stream, got, c.want, c.why)
		}
	}
}
