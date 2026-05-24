//go:build parity

package parity

import (
	"fmt"
	"testing"
	"time"
)

func TestParity_Stats(t *testing.T) {
	now := time.Now()

	cases := []ParityCase{
		{Name: "count_1h", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "count_6h", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "count_24h", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "count_full", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "filtered_count", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "filtered_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "group_by_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(level) count() rows"}, Compare: StructureMatch},
		{Name: "range_rate_1h", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats count() rows", "step": "300s"}, Compare: StructureMatch},
		{Name: "range_rate_6h", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats count() rows", "step": "600s"}, Compare: StructureMatch},
		{Name: "range_rate_24h", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats count() rows", "step": "3600s"}, Compare: StructureMatch},
		{Name: "range_filtered", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats count() rows`, "step": "3600s"}, Compare: StructureMatch},
		{Name: "range_grouped", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats by(level) count() rows", "step": "3600s"}, Compare: StructureMatch},
		{Name: "multi_stat", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() total, count_uniq(level) levels"}, Compare: StructureMatch},
		{Name: "count_over_subrange", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", now.Add(-12*time.Hour).UnixNano()),
			"end":   fmt.Sprintf("%d", now.Add(-6*time.Hour).UnixNano()),
		}, Compare: CountEqual},
		{Name: "empty_range_stats", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", now.Add(24*time.Hour).UnixNano()),
			"end":   fmt.Sprintf("%d", now.Add(48*time.Hour).UnixNano()),
		}, Compare: CountEqual},
	}

	durations := map[string]time.Duration{
		"count_1h":  1 * time.Hour,
		"count_6h":  6 * time.Hour,
		"count_24h": 24 * time.Hour,
	}

	for _, pc := range cases {
		t.Run(pc.Name, func(t *testing.T) {
			var params = fullRangeParams()
			if dur, ok := durations[pc.Name]; ok {
				params = rangeParams(dur)
			}
			for k, v := range pc.Params {
				params.Set(k, v)
			}
			ref := fetch(t, vlBaseURL, pc.Endpoint, params)
			sut := fetch(t, lhBaseURL, pc.Endpoint, params)
			compareParity(t, pc, ref, sut)
		})
	}
}
