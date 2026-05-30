//go:build parity

package parity

import "testing"

func TestParity_FiltersGapfill(t *testing.T) {
	t.Run("time_range_filter", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "time_range_1h", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `_time:1h | stats count() rows`,
			}, Compare: CountTolerance, Tolerance: 0.05},
			{Name: "time_range_24h", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `_time:24h | stats count() rows`,
			}, Compare: CountTolerance, Tolerance: 0.05},
			{Name: "time_range_5m", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `_time:5m | stats count() rows`,
			}, Compare: CountTolerance, Tolerance: 0.1},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("nested_boolean_3_levels", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "nested_and_or_not_3_levels", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `((level:="ERROR" OR level:="WARN") AND service.name:="api-gateway") OR (NOT level:="DEBUG" AND service.name:="user-service") | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "deep_nested_or_and", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `(level:="ERROR" AND (service.name:="api-gateway" OR service.name:="user-service")) OR (level:="WARN" AND (service.name:="order-service" OR service.name:="payment-service")) | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "triple_not_nested", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `NOT (NOT (NOT level:="DEBUG")) | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "mixed_and_or_not_parenthesized", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `(level:="ERROR" OR (level:="WARN" AND NOT service.name:="api-gateway")) AND (service.name:="user-service" OR service.name:="order-service") | stats count() rows`,
			}, Compare: CountEqual},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("escaped_special_chars", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "filter_with_backslash_n", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `_msg:~"\\n" | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "filter_with_tab_char", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `_msg:~"\\t" | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "filter_with_double_quote", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `_msg:~"\"" | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "filter_with_curly_braces", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `_msg:~"\\{.*\\}" | stats count() rows`,
			}, Compare: CountEqual},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("range_comparison_filters", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "numeric_greater_than", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `http.status_code:>400 | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "numeric_less_than", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `http.status_code:<300 | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "numeric_gte", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `http.status_code:>=500 | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "numeric_range_combined", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `http.status_code:>=400 http.status_code:<500 | stats count() rows`,
			}, Compare: CountEqual},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("complex_stream_filters", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "stream_with_nested_bool", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `{service.name="api-gateway"} (level:="ERROR" OR level:="WARN") | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "stream_with_not_filter", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `{service.name="api-gateway"} NOT level:="DEBUG" NOT level:="INFO" | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "stream_regexp_and_exact", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `{service.name=~"api-.*"} level:="ERROR" | stats count() rows`,
			}, Compare: CountEqual},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})

	t.Run("in_filter_variants", func(t *testing.T) {
		cases := []ParityCase{
			{Name: "in_filter_levels", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `level:in("ERROR","WARN") | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "in_filter_single_value", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `level:in("ERROR") | stats count() rows`,
			}, Compare: CountEqual},
			{Name: "in_filter_with_not", Endpoint: statsEndpoint(), Params: map[string]string{
				"query": `NOT level:in("DEBUG","INFO") | stats count() rows`,
			}, Compare: CountEqual},
		}
		RunParity(t, vlBaseURL, lhBaseURL, cases)
	})
}
