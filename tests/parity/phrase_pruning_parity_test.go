//go:build parity

package parity

import (
	"fmt"
	"testing"
)

// A quoted phrase filter (`field:"v"`) matches any value that CONTAINS v on
// token boundaries (upstream filter_phrase.go). The cold tier used to prune it
// as an exact match, so `k8s.pod.name:"api-gateway"` found nothing where hot
// found every pod name `api-gateway-<hex>` (#319). Every case compares the
// cold answer with hot's, on the data both tiers hold. The seed carries fixed
// rows and OTLP/HTTP JSON spans whose trace_id is not hex (datagen
// --phrase-fixture), the case a full-hex phrase may never be pruned as a value
// on, and the exact-prefix form `field:="v"*`, which is not the exact value v.

// seededFirstValue returns the first value of field on the reference tier,
// read from the seed, so a case can quote a real id or name.
func seededFirstValue(t *testing.T, base, field, filter string) string {
	t.Helper()
	p := seedWindowParams()
	p.Set("query", fmt.Sprintf("%s | limit 1", filter))
	r := fetch(t, base, queryEndpoint(), p)
	if r.StatusCode != 200 {
		t.Fatalf("seed lookup %q on %s: status %d: %s", filter, base, r.StatusCode, string(r.Body))
	}
	for _, row := range parseNDJSON(r.Body) {
		if v, ok := rowValue(row, field); ok && v != "" {
			return v
		}
	}
	t.Fatalf("the hot tier holds no %s for %q: seed defect, not parity", field, filter)
	return ""
}

// hexTraceIDFilter selects rows whose trace_id is a 32-digit hex id, so a
// case built on a seeded id never picks a fixture's hyphenated id.
const hexTraceIDFilter = `trace_id:~"^[0-9a-f]{32}$"`

func TestParity_PhrasePruning_Logs(t *testing.T) {
	traceID := seededFirstValue(t, vlBaseURL, "trace_id", hexTraceIDFilter)
	cases := []ParityCase{
		// Phrase that is a substring of a longer stored value, on a token
		// boundary: pod names are `<service>-<hex>`.
		{Name: "pod_phrase_substring", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `k8s.pod.name:"api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "pod_phrase_substring_2", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `k8s.pod.name:"user-service" | stats count() rows`}, Compare: CountEqual},
		// Phrase at word boundaries of a hyphenated service name.
		{Name: "service_phrase_word", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `service.name:"gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "service_phrase_word_unquoted", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `service.name:api | stats count() rows`}, Compare: CountEqual},
		{Name: "service_phrase_full", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `service.name:"api-gateway" | stats count() rows`}, Compare: CountEqual},
		// Not on a token boundary: neither tier matches.
		{Name: "service_phrase_mid_token", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `service.name:"api-gate" | stats count() rows`}, Compare: CountEqual, ExpectEmpty: true},
		// trace_id: phrase of a full id equals the exact form; a prefix of it is
		// not on a token boundary.
		{Name: "trace_id_phrase_full", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:%q | stats count() rows`, traceID)}, Compare: CountEqual},
		{Name: "trace_id_exact", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:=%q | stats count() rows`, traceID)}, Compare: CountEqual},
		{Name: "trace_id_phrase_prefix", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:%q | stats count() rows`, traceID[:12])}, Compare: CountEqual, ExpectEmpty: true},
		{Name: "trace_id_phrase_rows", Endpoint: queryEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:%q | fields _time, trace_id, span_id`, traceID)}, Compare: CountEqual},
		// Fixed rows (datagen --phrase-fixture): UUID and hyphenated ids. The
		// phrase matches longer values than the exact form does.
		{Name: "trace_id_uuid_phrase_part", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"4bf92f35-77b3" | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_uuid_phrase_middle", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"77b3-4da6" | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_uuid_phrase_full", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"4bf92f35-77b3-4da6-a3ce-929d0e0bf736" | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_hyphen_phrase", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"abc-def" | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_hyphen_exact", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:="abc-def" | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_hyphen_phrase_rows", Endpoint: queryEndpoint(), Params: map[string]string{
			"query": `trace_id:"abc-def" | fields _time, trace_id`}, Compare: CountEqual},
		// phrase AND exact: the phrase must not prune what the exact keeps
		{Name: "phrase_and_exact", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `k8s.pod.name:"api-gateway" service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		// The review's shapes: a hex-looking phrase inside a non-hex id.
		{Name: "trace_id_uuid_hex_part", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"4bf92f35" | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_uuid_hex_part_unquoted", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:4bf92f35 | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_uuid_hex_tail", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"929d0e0bf736" | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_hyphen_word", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"abc" | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_hyphen_word_unquoted", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:abc | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_hyphen_last_word", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"ghi" | stats count() rows`}, Compare: CountEqual},
		// Exact prefix: `field:="v"*` matches every value starting with v.
		{Name: "trace_id_exact_prefix_uuid", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:="4bf92f35"* | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_exact_prefix_hex", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:=%q* | stats count() rows`, traceID[:12])}, Compare: CountEqual},
		{Name: "pod_exact_prefix", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `k8s.pod.name:="api-gateway"* | stats count() rows`}, Compare: CountEqual},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}

func TestParity_PhrasePruning_Traces(t *testing.T) {
	traceID := seededFirstValue(t, vtBaseURL, "trace_id", "span_id:* "+hexTraceIDFilter)
	cases := []ParityCase{
		// VictoriaTraces' own trace-by-ID span fetch form, plus exact.
		{Name: "trace_id_phrase", Endpoint: queryEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:%q | fields _time, trace_id, span_id`, traceID)}, Compare: CountEqual},
		{Name: "trace_id_phrase_stats", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:%q | stats count() rows`, traceID)}, Compare: CountEqual},
		{Name: "trace_id_exact", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:=%q | stats count() rows`, traceID)}, Compare: CountEqual},
		{Name: "trace_id_phrase_prefix", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:%q | stats count() rows`, traceID[:12])}, Compare: CountEqual, ExpectEmpty: true},
		// Span names are free text: a phrase inside a longer name.
		{Name: "span_name_phrase_substring", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `span_id:* name:"GET /api/v1" | stats count() rows`}, Compare: CountEqual},
		{Name: "span_name_phrase_path", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `span_id:* name:"api/v1/users" | stats count() rows`}, Compare: CountEqual},
		{Name: "span_name_phrase_word", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `span_id:* name:users | stats count() rows`}, Compare: CountEqual},
		// Service name, a hyphenated value matched on a word.
		{Name: "service_phrase_word", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "span_id:* `resource_attr:service.name`:\"gateway\" | stats count() rows"}, Compare: CountEqual},
		{Name: "service_phrase_full", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "span_id:* `resource_attr:service.name`:\"api-gateway\" | stats count() rows"}, Compare: CountEqual},
		// OTLP/HTTP JSON spans with a non-hex traceId (datagen --phrase-fixture):
		// VictoriaTraces stores the id as sent, so a phrase finds the span by a
		// part of its id. Before the attestation, cold pruned these as values.
		{Name: "non_hex_trace_id_partial_phrase", Endpoint: queryEndpoint(), Params: map[string]string{
			"query": `trace_id:"abc" | fields _time, trace_id, span_id`}, Compare: CountEqual},
		{Name: "non_hex_trace_id_partial_phrase_stats", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"abc" | stats count() rows`}, Compare: CountEqual},
		{Name: "non_hex_trace_id_partial_unquoted", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:abc | stats count() rows`}, Compare: CountEqual},
		{Name: "non_hex_trace_id_last_word", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"ghi" | stats count() rows`}, Compare: CountEqual},
		{Name: "uuid_trace_id_hex_part", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"4bf92f35" | stats count() rows`}, Compare: CountEqual},
		{Name: "uuid_trace_id_hex_part_unquoted", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:4bf92f35 | stats count() rows`}, Compare: CountEqual},
		{Name: "uuid_trace_id_hex_tail", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:"929d0e0bf736" | stats count() rows`}, Compare: CountEqual},
		{Name: "non_hex_trace_id_exact", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:="abc-def-ghi" | stats count() rows`}, Compare: CountEqual},
		// Exact prefix: never the exact value.
		{Name: "trace_id_exact_prefix_non_hex", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": `trace_id:="abc"* | stats count() rows`}, Compare: CountEqual},
		{Name: "trace_id_exact_prefix_hex", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": fmt.Sprintf(`trace_id:=%q* | stats count() rows`, traceID[:12])}, Compare: CountEqual},
	}
	RunParity(t, vtBaseURL, lhtBaseURL, cases)
}
