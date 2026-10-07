//go:build parity

package parity

// Lock for #409: span events, span links and instrumentation-scope attributes
// answer like hot VictoriaTraces on every data layer and tenant form.
//
// TestParity_Traces_EventsLinksScopeAttrs compares the seeded corpus (datagen
// --span-extras) on the freshly flushed layer. This case writes its own spans,
// shaped to break a codec rather than to look like traffic: events and links
// with every OTLP attribute type, empty and unicode values, a 64 KiB value, a
// span with 40 events, two events with the same name, links with trace state,
// flags and attributes. The same spans go to hot VictoriaTraces and to
// Lakehouse, for two numeric tenant forms (AccountID only, AccountID with a
// non-zero ProjectID). They are compared through LogsQL rows, Jaeger
// trace-by-ID and Tempo v1/v2 trace-by-ID on
//
//   - buffer:    rows still in Lakehouse's insert buffer;
//   - parquet:   after the buffer flushed them to Parquet only;
//   - compacted: after two flushed files of the partition were merged
//     (POST /lakehouse/compaction/recompact).
//
// A restart layer is not part of this case: the parity stack has no restart
// step. The Parquet files carry everything the read needs and the manifest is
// rebuilt from them at start, which the traces module's flush end-to-end test
// pins (flush_events_links_e2e_test.go). String tenant aliases are opt-in and
// not configured in the parity stack.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

type extrasTenant struct{ account, project string }

var extrasTenants = []extrasTenant{{"7312", "0"}, {"7313", "5"}}

// isEventsLinksTenant reports whether account is one this case owns, so the
// tests that iterate the seeded tenants leave it out.
func isEventsLinksTenant(account string) bool {
	for _, tn := range extrasTenants {
		if tn.account == account {
			return true
		}
	}
	return false
}

func otlpAttr(key string, value any) map[string]any {
	return map[string]any{"key": key, "value": value}
}

func extrasAttrs() []map[string]any {
	return []map[string]any{
		otlpAttr("str", map[string]any{"stringValue": "plain"}),
		otlpAttr("empty", map[string]any{"stringValue": ""}),
		otlpAttr("unicode", map[string]any{"stringValue": "žółć ☃ \"quoted\" \\ back\nline"}),
		otlpAttr("int", map[string]any{"intValue": "-9007199254740993"}),
		otlpAttr("bool", map[string]any{"boolValue": true}),
		otlpAttr("double", map[string]any{"doubleValue": 0.1}),
		otlpAttr("bytes", map[string]any{"bytesValue": base64.StdEncoding.EncodeToString([]byte{0, 1, 2, 250})}),
		otlpAttr("array", map[string]any{"arrayValue": map[string]any{"values": []any{
			map[string]any{"stringValue": "a"}, map[string]any{"intValue": "2"}}}}),
		otlpAttr("kvlist", map[string]any{"kvlistValue": map[string]any{"values": []any{
			otlpAttr("k", map[string]any{"stringValue": "v"})}}}),
	}
}

// extrasSpans builds the spans of one trace: s1 with events and a link, s2 with
// 40 events and a 64 KiB attribute, s3 with nothing (so a span without extras
// stays without).
func extrasSpans(traceID string, at time.Time) []map[string]any {
	ts := func(d time.Duration) string { return fmt.Sprint(at.Add(d).UnixNano()) }
	span := func(id, name string) map[string]any {
		return map[string]any{
			"traceId": traceID, "spanId": id, "name": name, "kind": 2,
			"startTimeUnixNano": ts(-time.Second), "endTimeUnixNano": ts(0),
		}
	}
	s1 := span("00000000000000a1", "extras-1")
	s1["events"] = []any{
		map[string]any{"timeUnixNano": ts(-900 * time.Millisecond), "name": "exception", "attributes": extrasAttrs(), "droppedAttributesCount": 3},
		map[string]any{"timeUnixNano": ts(-800 * time.Millisecond), "name": "exception", "attributes": []any{otlpAttr("exception.type", map[string]any{"stringValue": "IOError"})}},
		map[string]any{"timeUnixNano": ts(-700 * time.Millisecond), "name": ""},
	}
	s1["links"] = []any{
		map[string]any{"traceId": strings.Repeat("ab", 16), "spanId": "00000000000000b1", "traceState": "k=v", "flags": 257,
			"attributes": extrasAttrs(), "droppedAttributesCount": 1},
		map[string]any{"traceId": strings.Repeat("cd", 16), "spanId": "00000000000000b2"},
	}
	s2 := span("00000000000000a2", "extras-2")
	var many []any
	for i := 0; i < 40; i++ {
		many = append(many, map[string]any{"timeUnixNano": ts(time.Duration(-i) * time.Millisecond), "name": fmt.Sprintf("ev-%d", i),
			"attributes": []any{otlpAttr("i", map[string]any{"intValue": fmt.Sprint(i)})}})
	}
	many = append(many, map[string]any{"timeUnixNano": ts(-time.Millisecond), "name": "big",
		"attributes": []any{otlpAttr("blob", map[string]any{"stringValue": strings.Repeat("xé", 32*1024)})}})
	s2["events"] = many
	return []map[string]any{s1, s2, span("00000000000000a3", "extras-3")}
}

func pushExtrasTrace(t *testing.T, base string, tn extrasTenant, traceID string, at time.Time) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"resourceSpans": []map[string]any{{
		"resource": map[string]any{"attributes": []map[string]any{
			{"key": "service.name", "value": map[string]any{"stringValue": "extras-svc"}},
		}},
		"scopeSpans": []map[string]any{{
			"scope": map[string]any{"name": "extras-scope", "version": "1.2.3", "attributes": extrasAttrs()},
			"spans": extrasSpans(traceID, at),
		}},
	}}})
	req, err := http.NewRequest(http.MethodPost, base+"/insert/opentelemetry/v1/traces", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("AccountID", tn.account)
	req.Header.Set("ProjectID", tn.project)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("push to %s: %v", base, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("push to %s: status %d", base, resp.StatusCode)
	}
}

// compareExtrasTrace requires Lakehouse to answer trace id like hot for tenant
// tn through LogsQL, Jaeger and Tempo, once both tiers return all three spans.
func compareExtrasTrace(t *testing.T, tn extrasTenant, id string) {
	t.Helper()
	get := func(base, path string, params url.Values) fetchResult {
		return tenantFetch(t, base, path, params, tn.account, tn.project)
	}
	params := tracesWindow()
	params.Set("query", fmt.Sprintf(`trace_id:=%q`, id))
	params.Set("limit", "1000")
	deadline := time.Now().Add(45 * time.Second)
	var ref, sut fetchResult
	for {
		ref, sut = get(vtBaseURL, "/select/logsql/query", params), get(lhtBaseURL, "/select/logsql/query", params)
		if len(parseNDJSON(ref.Body)) == 3 && len(parseNDJSON(sut.Body)) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("trace %s never answered with 3 spans: hot=%d cold=%d (status %d/%d)", id,
				len(parseNDJSON(ref.Body)), len(parseNDJSON(sut.Body)), ref.StatusCode, sut.StatusCode)
		}
		time.Sleep(time.Second)
	}
	hot, cold := map[string]map[string]string{}, map[string]map[string]string{}
	for _, row := range parseNDJSON(ref.Body) {
		sid, _ := row["span_id"].(string)
		hot[sid] = extrasFields(row)
	}
	for _, row := range parseNDJSON(sut.Body) {
		sid, _ := row["span_id"].(string)
		cold[sid] = extrasFields(row)
	}
	var compared int
	for sid, h := range hot {
		c := cold[sid]
		for k, v := range h {
			compared++
			if c[k] != v {
				t.Errorf("span %s: cold %s = %.80q, hot has %.80q", sid, k, c[k], v)
			}
		}
		for k := range c {
			if _, ok := h[k]; !ok {
				t.Errorf("span %s: cold has %s, hot does not", sid, k)
			}
		}
	}
	if compared < 60 {
		t.Fatalf("vacuous: only %d event/link/scope fields on the reference side", compared)
	}

	jr, js := get(vtBaseURL, "/select/jaeger/api/traces/"+id, nil), get(lhtBaseURL, "/select/jaeger/api/traces/"+id, nil)
	if jr.StatusCode != 200 || js.StatusCode != 200 {
		t.Fatalf("jaeger status hot=%d cold=%d", jr.StatusCode, js.StatusCode)
	}
	jh, jc := jaegerSpansByID(t, jr.Body), jaegerSpansByID(t, js.Body)
	if len(jh) != 3 || len(jc) != 3 {
		t.Fatalf("jaeger spans hot=%d cold=%d, want 3", len(jh), len(jc))
	}
	for sid, h := range jh {
		for _, key := range []string{"logs", "references"} {
			if canonJSON(h[key]) != canonJSON(jc[sid][key]) {
				t.Errorf("span %s: Jaeger %s differ\n hot  %.300s\n cold %.300s", sid, key, canonJSON(h[key]), canonJSON(jc[sid][key]))
			}
		}
		if canonJSON(scopeTags(h)) != canonJSON(scopeTags(jc[sid])) {
			t.Errorf("span %s: Jaeger scope tags differ", sid)
		}
	}

	for _, path := range []string{"/select/tempo/api/traces/" + id, "/select/tempo/api/v2/traces/" + id} {
		tr, ts := get(vtBaseURL, path, nil), get(lhtBaseURL, path, nil)
		if tr.StatusCode != 200 || ts.StatusCode != 200 {
			t.Fatalf("%s status hot=%d cold=%d", path, tr.StatusCode, ts.StatusCode)
		}
		th, hs := tempoSpansByID(t, tr.Body)
		tc, cs := tempoSpansByID(t, ts.Body)
		if len(th) != 3 || len(tc) != 3 {
			t.Fatalf("%s spans hot=%d cold=%d, want 3", path, len(th), len(tc))
		}
		for sid, h := range th {
			for _, key := range []string{"events", "links"} {
				if canonJSON(h[key]) != canonJSON(tc[sid][key]) {
					t.Errorf("%s span %s: %s differ\n hot  %.300s\n cold %.300s", path, sid, key, canonJSON(h[key]), canonJSON(tc[sid][key]))
				}
			}
		}
		if canonJSON(hs) != canonJSON(cs) {
			t.Errorf("%s: instrumentation scopes differ", path)
		}
	}
}

// recompactPartition merges the tenant files of the partition holding at. The
// answer is 400 until the partition has two compactable files, so it retries.
func recompactPartition(t *testing.T, at time.Time) {
	t.Helper()
	partition := at.UTC().Format("dt=2006-01-02/hour=15")
	body, _ := json.Marshal(map[string]any{"partition": partition})
	deadline := time.Now().Add(60 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodPost, lhtBaseURL+"/lakehouse/compaction/recompact", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err == nil {
			b := readAllOrEmpty(resp)
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
			last = fmt.Sprintf("%d %s", resp.StatusCode, b)
		} else {
			last = err.Error()
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("recompact of %s never succeeded: %s", partition, last)
}

func TestParity_Traces_EventsLinksLayers(t *testing.T) {
	stamp := time.Now().UnixNano()
	at := time.Now().UTC().Truncate(time.Hour).Add(-2*time.Hour + 30*time.Minute)

	type pushed struct {
		tn  extrasTenant
		ids []string
	}
	var all []pushed
	for ti, tn := range extrasTenants {
		p := pushed{tn: tn}
		for b := 0; b < 2; b++ {
			p.ids = append(p.ids, fmt.Sprintf("%016x%016x", stamp, 0xe000+ti*16+b))
		}
		all = append(all, p)
	}
	compareAll := func(t *testing.T) {
		for _, p := range all {
			for _, id := range p.ids {
				t.Run(fmt.Sprintf("tenant_%s_%s/trace_%s", p.tn.account, p.tn.project, id[16:]), func(t *testing.T) {
					compareExtrasTrace(t, p.tn, id)
				})
			}
		}
	}
	flushed := func(t *testing.T) {
		for _, p := range all {
			waitLeftBufferTenant(t, lhtBaseURL, "traces", p.tn.account, p.tn.project, at.Add(-time.Minute), at.Add(time.Minute))
		}
	}

	// Batch 0 first, then batch 1 once batch 0 is in Parquet, so the partition
	// holds two files per tenant to merge.
	for _, p := range all {
		for _, base := range []string{vtBaseURL, lhtBaseURL} {
			pushExtrasTrace(t, base, p.tn, p.ids[0], at)
		}
	}
	t.Run("buffer", compareAll)
	flushed(t)
	for _, p := range all {
		for _, base := range []string{vtBaseURL, lhtBaseURL} {
			pushExtrasTrace(t, base, p.tn, p.ids[1], at.Add(time.Second))
		}
	}
	flushed(t)
	t.Run("parquet", compareAll)
	recompactPartition(t, at)
	t.Run("compacted", compareAll)
}
