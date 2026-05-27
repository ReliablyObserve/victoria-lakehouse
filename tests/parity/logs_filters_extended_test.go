//go:build parity

package parity

import "testing"

func TestParity_FiltersExtended(t *testing.T) {
	cases := []ParityCase{
		{Name: "prefix_match", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:"java" | stats count() rows`}, Compare: CountEqual},
		{Name: "word_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:~"\btimeout\b" | stats count() rows`}, Compare: CountEqual},
		{Name: "case_insensitive_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:i("error") | stats count() rows`}, Compare: CountEqual},
		{Name: "case_insensitive_exact", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:i("error") | stats count() rows`}, Compare: CountEqual},
		{Name: "prefix_field", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:"api" | stats count() rows`}, Compare: CountEqual},
		{Name: "multiple_not", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT level:="DEBUG" NOT level:="INFO" | stats count() rows`}, Compare: CountEqual},
		{Name: "nested_not_or", Endpoint: statsEndpoint(), Params: map[string]string{"query": `(NOT level:="DEBUG") AND (level:="ERROR" OR level:="WARN") | stats count() rows`}, Compare: CountEqual},
		{Name: "double_negation", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT (NOT level:="ERROR") | stats count() rows`}, Compare: CountEqual},
		{Name: "re2_anchored", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:~"^java\\.lang" | stats count() rows`}, Compare: CountEqual},
		{Name: "re2_multialt", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:~"^(api-gateway|user-service|order-service)$" | stats count() rows`}, Compare: CountEqual},
		{Name: "or_three_values", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" OR level:="WARN" OR level:="INFO" | stats count() rows`}, Compare: CountEqual},
		{Name: "and_not_combined", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" AND NOT level:="DEBUG" AND NOT level:="INFO" | stats count() rows`}, Compare: CountEqual},
		{Name: "in_with_all_services", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:in("api-gateway","user-service","order-service","payment-service","notification-service") | stats count() rows`}, Compare: CountEqual},
		{Name: "stream_filter_namespace", Endpoint: statsEndpoint(), Params: map[string]string{"query": `{k8s.namespace.name="production"} | stats count() rows`}, Compare: CountEqual},
		{Name: "stream_filter_combined", Endpoint: statsEndpoint(), Params: map[string]string{"query": `{service.name="api-gateway"} level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "regexp_dotstar", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:~".*timeout.*" | stats count() rows`}, Compare: CountEqual},
		{Name: "field_exists_multi", Endpoint: statsEndpoint(), Params: map[string]string{"query": `http.method:* http.status_code:* | stats count() rows`}, Compare: CountEqual},
		{Name: "negated_exists_combined", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT http.method:* | stats count() rows`}, Compare: CountEqual},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
