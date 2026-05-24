//go:build parity

package parity

import "testing"

func TestParity_Endpoints(t *testing.T) {
	cases := []ParityCase{
		{
			Name:     "query_wildcard",
			Endpoint: "/select/logsql/query",
			Params:   map[string]string{"query": "*", "limit": "10"},
			Compare:  RowsMatch,
		},
		{
			Name:     "query_time_range",
			Endpoint: "/select/logsql/query_time_range",
			Params:   map[string]string{"query": "*"},
			Compare:  StructureMatch,
		},
		{
			Name:     "facets",
			Endpoint: "/select/logsql/facets",
			Params:   map[string]string{"query": "*"},
			Compare:  SetEqual,
		},
		{
			Name:     "field_names",
			Endpoint: "/select/logsql/field_names",
			Params:   map[string]string{"query": "*"},
			Compare:  SetSuperset,
		},
		{
			Name:     "field_values_level",
			Endpoint: "/select/logsql/field_values",
			Params:   map[string]string{"query": "*", "field": "level"},
			Compare:  SetEqual,
		},
		{
			Name:     "stream_field_names",
			Endpoint: "/select/logsql/stream_field_names",
			Params:   map[string]string{"query": "*"},
			Compare:  SetSuperset,
		},
		{
			Name:     "stream_field_values_service",
			Endpoint: "/select/logsql/stream_field_values",
			Params:   map[string]string{"query": "*", "field": "service.name"},
			Compare:  SetEqual,
		},
		{
			Name:     "streams",
			Endpoint: "/select/logsql/streams",
			Params:   map[string]string{"query": "*"},
			Compare:  NonEmpty,
		},
		{
			Name:     "stream_ids",
			Endpoint: "/select/logsql/stream_ids",
			Params:   map[string]string{"query": "*"},
			Compare:  NonEmpty,
		},
		{
			Name:     "hits_1h",
			Endpoint: "/select/logsql/hits",
			Params:   map[string]string{"query": "*", "step": "3600s"},
			Compare:  BucketMatch,
		},
		{
			Name:     "stats_count",
			Endpoint: "/select/logsql/stats_query",
			Params:   map[string]string{"query": "* | stats count() rows"},
			Compare:  CountEqual,
		},
		{
			Name:     "stats_range",
			Endpoint: "/select/logsql/stats_query_range",
			Params:   map[string]string{"query": "* | stats count() rows", "step": "3600s"},
			Compare:  StructureMatch,
		},
		{
			Name:     "tail_not_supported",
			Endpoint: "/select/logsql/tail",
			Params:   map[string]string{"query": "*"},
			Compare:  StatusEqual,
		},
		{
			Name:     "tenant_ids",
			Endpoint: "/select/tenant_ids",
			Params:   map[string]string{},
			Compare:  SetEqual,
		},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
