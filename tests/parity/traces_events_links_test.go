//go:build parity

package parity

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// Span events, span links and instrumentation-scope attributes (issue #409).
//
// Hot VictoriaTraces stores all three and returns them from trace-by-ID (Jaeger
// v1, Tempo v1/v2), from LogsQL (`event:*`, `link:*`, `scope_attr:*` fields) and
// from TraceQL `instrumentation.*` filters. The cold tier must answer the same,
// for spans that have left the insert buffer (TestMain waits for that), whether
// the file is freshly flushed or compacted. datagen --span-extras (on by default)
// gives about every tenth span an exception event, about 8% of the others a log
// event, about 3% a link, and every span the scope attributes.

// extrasFields keeps the event / link / scope fields of an NDJSON span row.
func extrasFields(row map[string]any) map[string]string {
	out := map[string]string{}
	for k, v := range row {
		if strings.HasPrefix(k, "event:") || strings.HasPrefix(k, "link:") || strings.HasPrefix(k, "scope_attr:") {
			if s, ok := scalarString(v); ok && s != "" {
				out[k] = s
			}
		}
	}
	return out
}

func tracesWindow() url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.Add(time.Hour).UnixNano())},
	}
}

// traceIDsWith returns up to n distinct trace IDs hot VictoriaTraces has a row
// for under the LogsQL filter.
func traceIDsWith(t *testing.T, filter string, n int) []string {
	t.Helper()
	params := tracesWindow()
	params.Set("query", filter+" | fields trace_id")
	params.Set("limit", "400")
	r := fetch(t, vtBaseURL, "/select/logsql/query", params)
	if r.StatusCode != 200 {
		t.Fatalf("hot %q: status %d: %s", filter, r.StatusCode, r.Body)
	}
	seen := map[string]bool{}
	var ids []string
	for _, row := range parseNDJSON(r.Body) {
		id, _ := row["trace_id"].(string)
		if id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
			if len(ids) == n {
				break
			}
		}
	}
	return ids
}

// canon returns v with every array of objects sorted by its canonical JSON, so
// attribute and event lists compare as sets.
func canon(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = canon(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = canon(e)
		}
		sort.SliceStable(out, func(i, j int) bool {
			a, _ := json.Marshal(out[i])
			b, _ := json.Marshal(out[j])
			return string(a) < string(b)
		})
		return out
	}
	return v
}

func canonJSON(v any) string {
	b, _ := json.Marshal(canon(v))
	return string(b)
}

func TestParity_Traces_EventsLinksScopeAttrs(t *testing.T) {
	withEvents := traceIDsWith(t, `"event:event_name:0":*`, 12)
	withLinks := traceIDsWith(t, `"link:link_span_id:0":*`, 8)
	if len(withEvents) == 0 || len(withLinks) == 0 {
		t.Fatalf("the seeded corpus has no spans with events (%d traces) or links (%d traces): "+
			"datagen --span-extras must be on in tests/parity/docker-compose.yml", len(withEvents), len(withLinks))
	}
	ids := append(append([]string{}, withEvents...), withLinks...)
	seen := map[string]bool{}
	uniq := ids[:0]
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	ids = uniq

	t.Run("logsql_rows_by_trace", func(t *testing.T) { parityExtrasLogsQueryRows(t, ids) })
	t.Run("jaeger_trace_by_id", func(t *testing.T) { parityExtrasJaegerByID(t, ids) })
	for _, version := range []string{"v1", "v2"} {
		t.Run("tempo_trace_by_id_"+version, func(t *testing.T) { parityExtrasTempoByID(t, ids, version) })
	}
	// Filters and enumerations over the three field families agree with hot.
	counts := []struct{ name, filter string }{
		{"event_name", `"event:event_name:0":="exception"`},
		{"event_attr", `"event:event_attr:exception.type:0":*`},
		{"link_span_id", `"link:link_span_id:0":*`},
		{"scope_attr", `"scope_attr:otel.scope.version":="0.5.0"`},
	}
	for _, tc := range counts {
		t.Run("count_"+tc.name, func(t *testing.T) {
			params := tracesWindow()
			params.Set("query", "span_id:* "+tc.filter+" | stats count() rows")
			ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
			sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
			compareParity(t, ParityCase{Compare: CountEqual}, ref, sut)
		})
	}

	t.Run("field_values_event_name", parityExtrasFieldValuesEventName)
	t.Run("tempo_search_instrumentation_scope", parityExtrasTempoSearchScope)
}

func parityExtrasLogsQueryRows(t *testing.T, ids []string) {
	var events, links, scope int
	for _, id := range ids {
		params := tracesWindow()
		params.Set("query", fmt.Sprintf(`trace_id:=%q`, id))
		params.Set("limit", "1000")
		ref := fetch(t, vtBaseURL, "/select/logsql/query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/query", params)
		if ref.StatusCode != 200 || sut.StatusCode != 200 {
			t.Fatalf("trace %s: status ref=%d sut=%d", id, ref.StatusCode, sut.StatusCode)
		}
		hot := map[string]map[string]string{}
		for _, row := range parseNDJSON(ref.Body) {
			sid, _ := row["span_id"].(string)
			hot[sid] = extrasFields(row)
		}
		cold := map[string]map[string]string{}
		for _, row := range parseNDJSON(sut.Body) {
			sid, _ := row["span_id"].(string)
			cold[sid] = extrasFields(row)
		}
		if len(hot) != len(cold) {
			t.Errorf("trace %s: %d spans hot, %d cold", id, len(hot), len(cold))
		}
		for sid, h := range hot {
			c := cold[sid]
			for k, v := range h {
				if c[k] != v {
					t.Errorf("trace %s span %s: cold %s = %q, hot has %q", id, sid, k, c[k], v)
				}
				switch {
				case strings.HasPrefix(k, "event:"):
					events++
				case strings.HasPrefix(k, "link:"):
					links++
				default:
					scope++
				}
			}
			for k := range c {
				if _, ok := h[k]; !ok {
					t.Errorf("trace %s span %s: cold has %s, hot does not", id, sid, k)
				}
			}
		}
	}
	if events == 0 || links == 0 || scope == 0 {
		t.Fatalf("vacuous: compared %d event fields, %d link fields, %d scope fields", events, links, scope)
	}
	reportLockCells(t, events+links+scope)
	t.Logf("compared %d traces: %d event, %d link, %d scope fields", len(ids), events, links, scope)
}

func parityExtrasJaegerByID(t *testing.T, ids []string) {
	var logs, refs, scopeTagsSeen int
	for _, id := range ids {
		ref := fetch(t, vtBaseURL, "/select/jaeger/api/traces/"+id, nil)
		sut := fetch(t, lhtBaseURL, "/select/jaeger/api/traces/"+id, nil)
		if ref.StatusCode != 200 || sut.StatusCode != 200 {
			t.Fatalf("trace %s: status ref=%d sut=%d", id, ref.StatusCode, sut.StatusCode)
		}
		hot, cold := jaegerSpansByID(t, ref.Body), jaegerSpansByID(t, sut.Body)
		if len(hot) != len(cold) {
			t.Errorf("trace %s: %d spans hot, %d cold", id, len(hot), len(cold))
		}
		for sid, h := range hot {
			c, ok := cold[sid]
			if !ok {
				t.Errorf("trace %s: span %s missing on cold", id, sid)
				continue
			}
			for _, key := range []string{"logs", "references"} {
				if canonJSON(h[key]) != canonJSON(c[key]) {
					t.Errorf("trace %s span %s: Jaeger %q differ\n hot  %s\n cold %s", id, sid, key, canonJSON(h[key]), canonJSON(c[key]))
				}
			}
			// The instrumentation scope's attributes are span tags in the Jaeger model.
			if hs, cs := scopeTags(h), scopeTags(c); canonJSON(hs) != canonJSON(cs) {
				t.Errorf("trace %s span %s: Jaeger scope tags differ\n hot  %s\n cold %s", id, sid, canonJSON(hs), canonJSON(cs))
			} else if len(hs) > 0 {
				scopeTagsSeen += len(hs)
			}
			if arr, _ := h["logs"].([]any); len(arr) > 0 {
				logs += len(arr)
			}
			for _, r := range asSlice(h["references"]) {
				if m, _ := r.(map[string]any); m != nil && m["refType"] != "CHILD_OF" {
					refs++
				}
			}
		}
	}
	if logs == 0 || scopeTagsSeen == 0 {
		t.Fatalf("vacuous: %d Jaeger span logs (events), %d scope tags on the reference side", logs, scopeTagsSeen)
	}
	t.Logf("compared Jaeger spans: %d logs (events), %d non-parent references (links), %d scope tags", logs, refs, scopeTagsSeen)
}

func parityExtrasTempoByID(t *testing.T, ids []string, version string) {
	var events, links int
	for _, id := range ids {
		path := "/select/tempo/api/" + version + "/traces/" + id
		if version == "v1" {
			path = "/select/tempo/api/traces/" + id // the v1 route has no version segment
		}
		ref := fetch(t, vtBaseURL, path, nil)
		sut := fetch(t, lhtBaseURL, path, nil)
		if ref.StatusCode != 200 || sut.StatusCode != 200 {
			t.Fatalf("trace %s: status ref=%d sut=%d", id, ref.StatusCode, sut.StatusCode)
		}
		hot, hotScopes := tempoSpansByID(t, ref.Body)
		cold, coldScopes := tempoSpansByID(t, sut.Body)
		if len(hot) != len(cold) {
			t.Errorf("trace %s: %d spans hot, %d cold", id, len(hot), len(cold))
		}
		for sid, h := range hot {
			c, ok := cold[sid]
			if !ok {
				t.Errorf("trace %s: span %s missing on cold", id, sid)
				continue
			}
			for _, key := range []string{"events", "links"} {
				if canonJSON(h[key]) != canonJSON(c[key]) {
					t.Errorf("trace %s span %s: Tempo %q differ\n hot  %s\n cold %s", id, sid, key, canonJSON(h[key]), canonJSON(c[key]))
				}
			}
			events += len(asSlice(h["events"]))
			links += len(asSlice(h["links"]))
		}
		if canonJSON(hotScopes) != canonJSON(coldScopes) {
			t.Errorf("trace %s: instrumentation scopes differ\n hot  %s\n cold %s", id, canonJSON(hotScopes), canonJSON(coldScopes))
		}
	}
	if events == 0 || links == 0 {
		t.Fatalf("vacuous: %d events, %d links on the reference side", events, links)
	}
}

func parityExtrasFieldValuesEventName(t *testing.T) {
	params := tracesWindow()
	params.Set("query", "span_id:*")
	params.Set("field", "event:event_name:0")
	ref := fetch(t, vtBaseURL, "/select/logsql/field_values", params)
	sut := fetch(t, lhtBaseURL, "/select/logsql/field_values", params)
	if ref.StatusCode != 200 || sut.StatusCode != 200 {
		t.Fatalf("status ref=%d sut=%d", ref.StatusCode, sut.StatusCode)
	}
	hot, cold := valueHits(ref.Body), valueHits(sut.Body)
	if len(hot) == 0 {
		t.Fatal("vacuous: hot has no values for event:event_name:0")
	}
	if fmt.Sprint(hot) != fmt.Sprint(cold) {
		t.Errorf("field_values event:event_name:0 differ:\n hot  %v\n cold %v", hot, cold)
	}
}

func parityExtrasTempoSearchScope(t *testing.T) {
	now := time.Now().Unix()
	params := url.Values{
		"q":     {`{instrumentation.otel.scope.name="github.com/reliablyobserve/instrumentation"}`},
		"limit": {"1000"}, // more than the corpus holds: the answer is the full set, not a limit's pick
		"start": {fmt.Sprint(now - 24*3600)},
		"end":   {fmt.Sprint(now)},
	}
	ref := fetch(t, vtBaseURL, "/select/tempo/api/search", params)
	sut := fetch(t, lhtBaseURL, "/select/tempo/api/search", params)
	compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
	hot, cold := tempoSearchTraceIDs(t, ref.Body), tempoSearchTraceIDs(t, sut.Body)
	if len(hot) == 0 {
		t.Fatal("vacuous: hot found no trace with the instrumentation scope")
	}
	// The same traces, not just the same number of them.
	sort.Strings(hot)
	sort.Strings(cold)
	if len(hot) >= 1000 {
		t.Fatalf("the search hit its limit (%d traces): the comparison is not of full sets", len(hot))
	}
	if !reflect.DeepEqual(hot, cold) {
		t.Errorf("instrumentation.otel.scope.name search returns different traces\n hot  %v\n cold %v", hot, cold)
	}
}

// scopeTags returns a Jaeger span's tags that come from its instrumentation
// scope (`scope_attr:<key>`, and VictoriaTraces' own copy `otel.scope.*`).
func scopeTags(span map[string]any) []any {
	var out []any
	for _, tg := range asSlice(span["tags"]) {
		m, _ := tg.(map[string]any)
		k, _ := m["key"].(string)
		if strings.HasPrefix(k, "scope_attr:") || strings.HasPrefix(k, "otel.scope.") {
			out = append(out, m)
		}
	}
	return out
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

// jaegerSpansByID returns the spans of a Jaeger trace-by-ID response by span ID.
func jaegerSpansByID(t *testing.T, body []byte) map[string]map[string]any {
	t.Helper()
	var resp struct {
		Data []struct {
			Spans []map[string]any `json:"spans"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("parse Jaeger response: %v\n%s", err, body)
	}
	out := map[string]map[string]any{}
	for _, d := range resp.Data {
		for _, s := range d.Spans {
			sid, _ := s["spanID"].(string)
			out[sid] = s
		}
	}
	return out
}

// tempoSpansByID returns the spans of a Tempo trace-by-ID response by span ID,
// and the instrumentation scopes of the trace (name, version, attributes). It
// reads both shapes: v2 wraps the resource spans in {"trace": {"resourceSpans"}},
// v1 answers {"batches": [...]}.
func tempoSpansByID(t *testing.T, body []byte) (map[string]map[string]any, []any) {
	t.Helper()
	type scopeSpans struct {
		Scope map[string]any   `json:"scope"`
		Spans []map[string]any `json:"spans"`
	}
	type resourceSpans struct {
		ScopeSpans []scopeSpans `json:"scopeSpans"`
	}
	var resp struct {
		Trace struct {
			ResourceSpans []resourceSpans `json:"resourceSpans"`
		} `json:"trace"`
		Batches []resourceSpans `json:"batches"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("parse Tempo response: %v\n%.500s", err, body)
	}
	spans := map[string]map[string]any{}
	var scopes []any
	for _, rs := range append(resp.Trace.ResourceSpans, resp.Batches...) {
		for _, ss := range rs.ScopeSpans {
			scopes = append(scopes, ss.Scope)
			for _, s := range ss.Spans {
				sid, _ := s["spanId"].(string)
				spans[sid] = s
			}
		}
	}
	return spans, scopes
}

func tempoSearchTraceIDs(t *testing.T, body []byte) []string {
	t.Helper()
	var resp struct {
		Traces []map[string]any `json:"traces"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("parse Tempo search: %v", err)
	}
	var ids []string
	for _, tr := range resp.Traces {
		if id, _ := tr["traceID"].(string); id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// valueHits reads a field_values response into "value=hits" strings, sorted.
// The empty value (VictoriaLogs' bucket of rows that lack the field) is left
// out: the cold tier never lists it, for any optional field, and that is a
// separate, older difference than the one this test pins.
func valueHits(body []byte) []string {
	obj, err := parseJSON(body)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range asSlice(obj["values"]) {
		m, _ := e.(map[string]any)
		if m == nil {
			continue
		}
		if m["value"] == "" {
			continue
		}
		out = append(out, fmt.Sprintf("%v=%v", m["value"], m["hits"]))
	}
	sort.Strings(out)
	return out
}
