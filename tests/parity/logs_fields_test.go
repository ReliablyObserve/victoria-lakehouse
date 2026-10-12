//go:build parity

package parity

import "testing"

func TestParity_Fields(t *testing.T) {
	cases := []ParityCase{
		{Name: "field_names_all", Endpoint: "/select/logsql/field_names", Params: map[string]string{"query": "*"}, Compare: SetSuperset},
		{Name: "field_names_filtered", Endpoint: "/select/logsql/field_names", Params: map[string]string{"query": `service.name:="api-gateway"`}, Compare: SetSuperset},
		{Name: "field_values_level", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "level"}, Compare: SetEqual},
		{Name: "field_values_service", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "service.name"}, Compare: SetEqual},
		{Name: "field_values_namespace", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "k8s.namespace.name"}, Compare: SetEqual},
		{Name: "field_values_limit", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "service.name", "limit": "2"}, Compare: NonEmpty},
		{Name: "stream_field_names", Endpoint: "/select/logsql/stream_field_names", Params: map[string]string{"query": "*"}, Compare: SetSuperset},
		{Name: "stream_field_values", Endpoint: "/select/logsql/stream_field_values", Params: map[string]string{"query": "*", "field": "service.name"}, Compare: SetEqual},
		{Name: "streams_list", Endpoint: "/select/logsql/streams", Params: map[string]string{"query": "*"}, Compare: NonEmpty},
		{Name: "stream_ids", Endpoint: "/select/logsql/stream_ids", Params: map[string]string{"query": "*"}, Compare: NonEmpty},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
	// field_names and stream_field_names exactly as hot: names, hits and order.
	t.Run("field_names_exact", func(t *testing.T) {
		for _, q := range []string{"*", `service.name:="api-gateway"`} {
			assertFieldNamesExact(t, vlBaseURL, lhBaseURL, "/select/logsql/field_names", q, fullRangeParams())
			assertFieldNamesExact(t, vlBaseURL, lhBaseURL, "/select/logsql/stream_field_names", q, fullRangeParams())
		}
	})
}
