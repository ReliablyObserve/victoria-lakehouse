//go:build parity

package parity

import "testing"

func TestParity_PipesGapfill(t *testing.T) {
	t.Run("group_by", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "group_by_level", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": "* | stats by(level) count() rows",
			}, Compare: StructureMatch},
			{Name: "group_by_service", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": "* | stats by(service.name) count() rows",
			}, Compare: StructureMatch},
			{Name: "group_by_two_fields", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": "* | stats by(level, service.name) count() rows",
			}, Compare: StructureMatch},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("string_functions", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "upper_level", Endpoint: queryEndpoint(), Params: map[string]string{
				"query": `* | format "<level>" as lvl | fields _time, lvl | sort by(_time) desc`,
				"limit": "10",
			}, Compare: RowsMatch, SkipFields: []string{"_msg"}},
			{Name: "len_msg", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": "* | len(_msg) msg_len | stats avg(msg_len) avglen",
			}, Compare: CountTolerance, Tolerance: 0.05},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("math_functions", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "math_multiply", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": "* duration:* | math duration * 2 as doubled | stats count() rows",
			}, Compare: CountEqual},
			{Name: "math_add", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": "* duration:* | math duration + 100 as shifted | stats count() rows",
			}, Compare: CountEqual},
			{Name: "math_division", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": "* duration:* | math duration / 1000 as dur_sec | stats avg(dur_sec) avg_dur",
			}, Compare: CountTolerance, Tolerance: 0.05},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("dedup", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "dedup_by_level", Endpoint: queryEndpoint(), Params: map[string]string{
				"query": "* | sort by(_time) desc | dedup by(level) | fields _time, _msg, level",
				"limit": "20",
			}, Compare: RowsMatch},
			{Name: "dedup_by_service", Endpoint: queryEndpoint(), Params: map[string]string{
				"query": "* | sort by(_time) desc | dedup by(service.name) | fields _time, _msg, service.name",
				"limit": "20",
			}, Compare: RowsMatch},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("chained_pipes_3plus", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "filter_rename_sort_limit", Endpoint: queryEndpoint(), Params: map[string]string{
				"query": `* | filter level:="ERROR" | rename service.name AS svc | sort by(_time) desc | limit 5`,
				"limit": "5",
			}, Compare: RowsMatch},
			{Name: "delete_filter_fields_sort", Endpoint: queryEndpoint(), Params: map[string]string{
				"query": `* | delete trace_id | filter level:="WARN" | fields _time, _msg, level, service.name | sort by(_time) desc | limit 5`,
				"limit": "5",
			}, Compare: RowsMatch},
			{Name: "len_math_filter_stats", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `* | len(_msg) msg_len | math msg_len * 1 as msg_len2 | filter msg_len2:>0 | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "rename_filter_sort_fields_limit", Endpoint: queryEndpoint(), Params: map[string]string{
				"query": `* | rename level AS severity | filter severity:="ERROR" | sort by(_time) desc | fields _time, _msg, severity | limit 3`,
				"limit": "3",
			}, Compare: RowsMatch},
			{Name: "format_delete_sort_limit", Endpoint: queryEndpoint(), Params: map[string]string{
				"query": `* | format "<level>: <_msg>" as summary | delete trace_id, span_id | sort by(_time) desc | limit 5`,
				"limit": "5",
			}, Compare: RowsMatch, SkipFields: []string{"_msg"}},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})
}
