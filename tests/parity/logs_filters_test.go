//go:build parity

package parity

import "testing"

func TestParity_Filters(t *testing.T) {
	cases := []ParityCase{
		{Name: "wildcard", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "exact_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_namespace", Endpoint: statsEndpoint(), Params: map[string]string{"query": `k8s.namespace.name:="production" | stats count() rows`}, Compare: CountEqual},
		{Name: "substring_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:timeout | stats count() rows`}, Compare: CountEqual},
		{Name: "substring_case", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:Error | stats count() rows`}, Compare: CountEqual},
		{Name: "regexp_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:~"timeout|deadline" | stats count() rows`}, Compare: CountEqual},
		{Name: "regexp_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:~"api-.*" | stats count() rows`}, Compare: CountEqual},
		{Name: "not_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT level:="DEBUG" | stats count() rows`}, Compare: CountEqual},
		{Name: "not_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "and_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" AND level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "or_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" OR level:="WARN" | stats count() rows`}, Compare: CountEqual},
		{Name: "and_or_combined", Endpoint: statsEndpoint(), Params: map[string]string{"query": `(level:="ERROR" OR level:="WARN") AND service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "field_exists", Endpoint: statsEndpoint(), Params: map[string]string{"query": `trace_id:* | stats count() rows`}, Compare: CountEqual},
		{Name: "field_not_exists", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT nonexistent_field:* | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:="specific log message" | stats count() rows`}, Compare: CountEqual},
		{Name: "in_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:in("ERROR", "WARN") | stats count() rows`}, Compare: CountEqual},
		{Name: "range_numeric", Endpoint: statsEndpoint(), Params: map[string]string{"query": `http.status_code:range[400, 599] | stats count() rows`}, Compare: CountEqual},
		{Name: "seq_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:seq("connection", "refused") | stats count() rows`}, Compare: CountEqual},
		{Name: "ipv4_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:ipv4_range("10.0.0.0/8") | stats count() rows`}, Compare: CountEqual},
		{Name: "len_range", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:len_range(100, 500) | stats count() rows`}, Compare: CountEqual},
		{Name: "multi_exact", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" level:="ERROR" k8s.namespace.name:="production" | stats count() rows`}, Compare: CountEqual},
		{Name: "negated_regexp", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:!~"debug|trace" | stats count() rows`}, Compare: CountEqual},
		{Name: "empty_value", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="" | stats count() rows`}, Compare: CountEqual},
		{Name: "stream_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `{service.name="api-gateway"} | stats count() rows`}, Compare: CountEqual},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
