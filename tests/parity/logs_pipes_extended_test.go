//go:build parity

package parity

import "testing"

func TestParity_PipesExtended(t *testing.T) {
	cases := []ParityCase{
		{Name: "delete_field", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | delete trace_id | fields _time, _msg, level | sort by(_time) desc", "limit": "10"}, Compare: RowsMatch},
		{Name: "rename_field", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | rename level AS severity | fields _time, _msg, severity | sort by(_time) desc", "limit": "10"}, Compare: RowsMatch},
		{Name: "replace_field", Endpoint: queryEndpoint(), Params: map[string]string{"query": `* | replace ("ERROR", "ERR") at level | fields _time, level | sort by(_time) desc`, "limit": "10"}, Compare: RowsMatch, SkipFields: []string{"_msg"}},
		{Name: "replace_regexp_field", Endpoint: queryEndpoint(), Params: map[string]string{"query": `* | replace_regexp ("api-.*", "api-svc") at service.name | fields _time, service.name | sort by(_time) desc`, "limit": "10"}, Compare: RowsMatch, SkipFields: []string{"_msg"}},
		{Name: "math_duration", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* duration:* | math duration / 1000 as duration_sec | stats count() rows"}, Compare: CountEqual},
		{Name: "filter_pipe", Endpoint: statsEndpoint(), Params: map[string]string{"query": `* | filter level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "filter_pipe_multi", Endpoint: statsEndpoint(), Params: map[string]string{"query": `* | filter level:="ERROR" | filter service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "offset_pipe", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by(_time) desc | offset 5 | limit 5", "limit": "10"}, Compare: RowsMatch},
		{Name: "len_pipe", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | len(_msg) msg_len | stats avg(msg_len) avglen"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "drop_empty_fields", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | drop_empty_fields | fields _time, _msg, level | sort by(_time) desc", "limit": "10"}, Compare: RowsMatch},
		{Name: "format_pipe", Endpoint: queryEndpoint(), Params: map[string]string{"query": `* | format "<level>: <_msg>" as formatted | fields _time, formatted | sort by(_time) desc`, "limit": "5"}, Compare: RowsMatch, SkipFields: []string{"_msg"}},
		{Name: "unpack_json", Endpoint: queryEndpoint(), Params: map[string]string{"query": `format:="otel" | unpack_json _msg | fields _time, severity | sort by(_time) desc`, "limit": "10"}, Compare: RowsMatch, SkipFields: []string{"_msg"}},
		{Name: "extract_pattern", Endpoint: statsEndpoint(), Params: map[string]string{"query": `http.method:* | extract "method=<method>" from http.method | stats count() rows`}, Compare: CountEqual},
		{Name: "pipe_chain_complex", Endpoint: queryEndpoint(), Params: map[string]string{"query": `* | filter level:="ERROR" | fields _time, _msg, service.name | sort by(_time) desc | limit 5`, "limit": "5"}, Compare: RowsMatch},
		{Name: "pipe_chain_rename_stats", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | rename level AS sev | stats by(sev) count() rows"}, Compare: StructureMatch},
		{Name: "pipe_chain_delete_sort", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | delete trace_id, span_id | sort by(_time) desc | limit 10", "limit": "10"}, Compare: RowsMatch},
		{Name: "first_pipe", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by(_time) | first 5", "limit": "5"}, Compare: RowsMatch},
		{Name: "last_pipe", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by(_time) | last 5", "limit": "5"}, Compare: RowsMatch},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
