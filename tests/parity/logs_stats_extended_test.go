//go:build parity

package parity

import "testing"

func TestParity_StatsExtended(t *testing.T) {
	cases := []ParityCase{
		{Name: "median_duration", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* duration:* | stats median(duration) med"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "quantile_95_duration", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* duration:* | stats quantile(0.95, duration) p95"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "quantile_50_duration", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* duration:* | stats quantile(0.50, duration) p50"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "count_empty_field", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count_empty(nonexistent_field) empties"}, Compare: CountEqual},
		{Name: "values_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats values(level) all_levels"}, Compare: NonEmpty},
		{Name: "uniq_values_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats uniq_values(level) levels"}, Compare: NonEmpty},
		{Name: "any_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats any(service.name) sample"}, Compare: NonEmpty},
		{Name: "sum_len_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats sum_len(_msg) total_bytes"}, Compare: CountTolerance, Tolerance: 0.01},
		{Name: "count_uniq_hash_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count_uniq_hash(service.name) approx"}, Compare: CountTolerance, Tolerance: 0.1},
		{Name: "stats_multi_agg", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() total, min(duration) lo, max(duration) hi, avg(duration) mean"}, Compare: StructureMatch},
		{Name: "stats_by_three_fields", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(level, service.name, k8s.namespace.name) count() rows"}, Compare: StructureMatch},
		{Name: "stats_by_format", Endpoint: statsEndpoint(), Params: map[string]string{"query": `* format:* | stats by(format) count() rows`}, Compare: StructureMatch},
		{Name: "rate_count", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() as total | math total / 3600 as rate"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "stats_filtered_agg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats count() errors, count_uniq(service.name) affected_services`}, Compare: StructureMatch},
		{Name: "stats_range_by_service", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats by(service.name) count() rows", "step": "3600s"}, Compare: StructureMatch},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
