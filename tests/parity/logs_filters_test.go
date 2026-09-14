//go:build parity

package parity

import (
	"strconv"
	"testing"
)

func TestParity_Filters(t *testing.T) {
	// `_msg:=` needs a message that exists verbatim, and cmd/datagen appends
	// random trace and span ids to every body, so no literal survives a
	// re-seed. Take one real message from the reference tier instead.
	exactMsg, _ := referenceRow(t, seedWindowParams(),
		`_msg:"request handled" | sort by (_time) desc | limit 1`)["_msg"].(string)
	if exactMsg == "" {
		t.Fatal("reference row lookup returned a row without _msg — seed defect, not parity")
	}

	cases := []ParityCase{
		{Name: "wildcard", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "exact_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_namespace", Endpoint: statsEndpoint(), Params: map[string]string{"query": `k8s.namespace.name:="production" | stats count() rows`}, Compare: CountEqual},
		{Name: "substring_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:timeout | stats count() rows`}, Compare: CountEqual},
		// Word filters are case-sensitive. The corpus writes "timeout" (logfmt
		// "timeout exceeded") and "Timeout" (the Java "Timeout waiting for
		// task" message) as distinct words, so a tier that folds case counts
		// both here. "Error" never appears as a word — only inside
		// identifiers such as OutOfMemoryError — and matched nothing.
		{Name: "substring_case", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:Timeout | stats count() rows`}, Compare: CountEqual},
		{Name: "regexp_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:~"timeout|deadline" | stats count() rows`}, Compare: CountEqual},
		{Name: "regexp_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:~"api-.*" | stats count() rows`}, Compare: CountEqual},
		{Name: "not_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT level:="DEBUG" | stats count() rows`}, Compare: CountEqual},
		{Name: "not_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "and_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" AND level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "or_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" OR level:="WARN" | stats count() rows`}, Compare: CountEqual},
		{Name: "and_or_combined", Endpoint: statsEndpoint(), Params: map[string]string{"query": `(level:="ERROR" OR level:="WARN") AND service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "field_exists", Endpoint: statsEndpoint(), Params: map[string]string{"query": `trace_id:* | stats count() rows`}, Compare: CountEqual},
		{Name: "field_not_exists", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT nonexistent_field:* | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:=` + strconv.Quote(exactMsg) + ` | stats count() rows`}, Compare: CountEqual},
		{Name: "in_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:in("ERROR", "WARN") | stats count() rows`}, Compare: CountEqual},
		{Name: "range_numeric", Endpoint: statsEndpoint(), Params: map[string]string{"query": `http.status_code:range[400, 599] | stats count() rows`}, Compare: CountEqual},
		// seq() is case-sensitive too: the Java SQLException message is
		// "Connection refused", so the lower-case spelling matched nothing.
		{Name: "seq_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:seq("Connection", "refused") | stats count() rows`}, Compare: CountEqual},
		// ipv4_range() only matches a value that IS an IPv4 address; an
		// address embedded in a longer string such as an nginx access line
		// never matches, so the filter has to read client_ip, the field the
		// nginx pattern stores the bare address in.
		{Name: "ipv4_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `client_ip:ipv4_range("10.0.0.0/8") | stats count() rows`}, Compare: CountEqual},
		{Name: "len_range", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:len_range(100, 500) | stats count() rows`}, Compare: CountEqual},
		{Name: "multi_exact", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" level:="ERROR" k8s.namespace.name:="production" | stats count() rows`}, Compare: CountEqual},
		// Every body ends in " trace_id=... span_id=...", so a pattern that
		// matches "trace" anywhere excluded every row. Anchoring on the
		// logfmt level key keeps the negation selective.
		{Name: "negated_regexp", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:!~"level=(debug|trace)" | stats count() rows`}, Compare: CountEqual},
		{Name: "empty_value", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="" | stats count() rows`}, Compare: CountEqual, ExpectEmpty: true},
		{Name: "stream_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `{service.name="api-gateway"} | stats count() rows`}, Compare: CountEqual},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
