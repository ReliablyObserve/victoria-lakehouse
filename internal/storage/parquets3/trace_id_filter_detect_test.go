package parquets3

import "testing"

// TestQueryFiltersTraceID pins detection of EVERY trace_id filter form VT emits,
// especially the phrase form `trace_id:"X"` used by single-trace GetTrace span
// fetch — missing it made the read-merge watermark exclude recent traces' buffer
// spans (cold GetTrace 404).
func TestQueryFiltersTraceID(t *testing.T) {
	yes := []string{
		`trace_id:"abc"`, `trace_id: "abc"`, `trace_id:=abc`, `trace_id:in(a,b)`,
		`trace_id:abc`, `_stream:{service.name="x"} AND trace_id:"abc"`,
		`trace_id:"abc" | stats count() n`,
	}
	no := []string{
		`_stream:{service.name="api-gateway"}`, `*`, `service.name:="x"`,
		`trace_id_idx:"abc"`,  // different field, must NOT match
		`* | fields trace_id`, // projection, not a filter
		`* | stats count() by (trace_id)`,
	}
	for _, q := range yes {
		if !queryFiltersTraceID(q) {
			t.Errorf("queryFiltersTraceID(%q) = false, want true", q)
		}
	}
	for _, q := range no {
		if queryFiltersTraceID(q) {
			t.Errorf("queryFiltersTraceID(%q) = true, want false", q)
		}
	}
}
