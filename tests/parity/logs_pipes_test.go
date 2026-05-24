//go:build parity

package parity

import "testing"

func TestParity_Pipes(t *testing.T) {
	cases := []ParityCase{
		{Name: "stats_count", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "stats_count_by_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(level) count() rows"}, Compare: StructureMatch},
		{Name: "stats_count_by_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(service.name) count() rows"}, Compare: StructureMatch},
		{Name: "stats_count_uniq", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count_uniq(service.name) services"}, Compare: CountEqual},
		{Name: "stats_min_max", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats min(_time) earliest, max(_time) latest"}, Compare: StructureMatch},
		{Name: "fields_projection", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | fields _time, _msg, level", "limit": "10"}, Compare: RowsMatch},
		{Name: "fields_single", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | fields _time, _msg | sort by (_time) desc", "limit": "10"}, Compare: RowsMatch},
		{Name: "sort_time_asc", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by(_time)", "limit": "10"}, Compare: RowsMatch},
		{Name: "sort_time_desc", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by(_time) desc", "limit": "10"}, Compare: RowsMatch},
		{Name: "limit_10", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by (_time) desc | limit 10", "limit": "10"}, Compare: RowsMatch},
		{Name: "limit_1", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by (_time) desc | limit 1", "limit": "1"}, Compare: RowsMatch},
		{Name: "uniq_level", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | uniq by(level)"}, Compare: SetEqual},
		{Name: "uniq_service", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | uniq by(service.name)"}, Compare: SetEqual},
		{Name: "top_services", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | top 5 by(service.name)"}, Compare: RowsMatch},
		{Name: "pipe_chain_fields_sort", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | fields _time, level | sort by(_time) | limit 5", "limit": "5"}, Compare: RowsMatch},
		{Name: "pipe_chain_filter_stats", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats by(service.name) count() rows`}, Compare: StructureMatch},
		{Name: "stats_by_two_fields", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(level, service.name) count() rows"}, Compare: StructureMatch},
		{Name: "stats_sum", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats sum(duration) total"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "stats_avg", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats avg(duration) mean"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "copy_pipe", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | copy level AS severity", "limit": "10"}, Compare: RowsMatch},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
