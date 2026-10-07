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
	"encoding/hex"
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
	if account == "7314" { // TestParity_Traces_EventsLinksInvalidUTF8
		return true
	}
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
// tn through LogsQL and, with withTraceAPIs, Jaeger and Tempo, once both tiers
// return all three spans. Trace-by-ID reads hot VictoriaTraces' trace index,
// which lags the write by its latency offset, so the buffer layer (read within
// the few seconds before the buffer flushes) compares LogsQL only.
func compareExtrasTrace(t *testing.T, tn extrasTenant, id string, withTraceAPIs bool) {
	t.Helper()
	get := func(base, path string, params url.Values) fetchResult {
		return tenantFetch(t, base, path, params, tn.account, tn.project)
	}
	// Trace-by-ID reads the trace index, which hot VictoriaTraces keeps behind
	// its latency offset: a just-written trace answers 404 for a while, on hot
	// only. Wait for hot to answer before comparing; the cold side is read once
	// hot has answered, so any difference is real.
	byID := func(path string) (fetchResult, fetchResult) {
		t.Helper()
		dl := time.Now().Add(90 * time.Second)
		for {
			h := get(vtBaseURL, path, nil)
			if h.StatusCode != http.StatusNotFound || time.Now().After(dl) {
				return h, get(lhtBaseURL, path, nil)
			}
			time.Sleep(2 * time.Second)
		}
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

	if !withTraceAPIs {
		return
	}

	jr, js := byID("/select/jaeger/api/traces/" + id)
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
		tr, ts := byID(path)
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

// tenantFiles returns how many objects the cold manifest holds for account:project.
func tenantFiles(t *testing.T, account, project string) int64 {
	t.Helper()
	r := fetch(t, lhtBaseURL, "/lakehouse/api/v1/tenants", nil)
	var d struct {
		Tenants []tenantSummary `json:"tenants"`
	}
	if r.StatusCode != http.StatusOK || json.Unmarshal(r.Body, &d) != nil {
		return -1
	}
	for _, te := range d.Tenants {
		if te.AccountID == account && te.ProjectID == project {
			return te.TotalFiles
		}
	}
	return -1
}

// recompactPartition merges the tenant files of the partition holding at with
// POST /lakehouse/compaction/recompact and requires the 200 answer: the
// partition is in the open hour with two files per tenant, below the level
// thresholds and inside min_age, so the compaction schedule never merges it and
// only this call does. It retries while the answer is 400 (the segment guard
// has not released the objects yet) and fails if it never succeeds.
func recompactPartition(t *testing.T, at time.Time) {
	t.Helper()
	partition := at.UTC().Format("dt=2006-01-02/hour=15")
	body, _ := json.Marshal(map[string]any{"partition": partition})
	deadline := time.Now().Add(180 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodPost, lhtBaseURL+"/lakehouse/compaction/recompact", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err == nil {
			b := readAllOrEmpty(resp)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				for _, tn := range extrasTenants {
					if n := tenantFiles(t, tn.account, tn.project); n != 1 {
						t.Fatalf("after the recompact of %s tenant %s:%s has %d objects, want 1 (%s)", partition, tn.account, tn.project, n, b)
					}
				}
				return
			}
			last = fmt.Sprintf("%d %s", resp.StatusCode, b)
		} else {
			last = err.Error()
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("recompact of %s never answered 200: %s", partition, last)
}

// openHourSpanTime returns a time in the current UTC hour, at least 70 seconds
// in the past (past the latency offset). The partition of that hour is younger
// than min_age, so no schedule merges it. Late in the hour it waits for the
// next one: the case runs for minutes and must not cross an hour boundary.
func openHourSpanTime(t *testing.T) time.Time {
	t.Helper()
	for {
		now := time.Now().UTC()
		if m := now.Minute(); m >= 2 && m < 48 {
			return now.Truncate(time.Hour).Add(time.Minute)
		}
		t.Logf("waiting for the open hour to be neither fresh nor nearly over (minute %d)", now.Minute())
		time.Sleep(20 * time.Second)
	}
}

// bufferedRowsOf counts the rows the insert buffer holds for the case's tenant.
func bufferedRowsOf(t *testing.T, tn extrasTenant, at time.Time) int {
	t.Helper()
	n, err := bufferedRows(lhtBaseURL, "traces", url.Values{"account_id": {tn.account}, "project_id": {tn.project}}, at.Add(-time.Minute), at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestParity_Traces_EventsLinksLayers(t *testing.T) {
	stamp := time.Now().UnixNano()
	at := openHourSpanTime(t)

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
	pushedBatches := 1
	compareAll := func(t *testing.T, withTraceAPIs bool) {
		for _, p := range all {
			for _, id := range p.ids[:pushedBatches] {
				t.Run(fmt.Sprintf("tenant_%s_%s/trace_%s", p.tn.account, p.tn.project, id[16:]), func(t *testing.T) {
					compareExtrasTrace(t, p.tn, id, withTraceAPIs)
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
	t.Run("buffer", func(t *testing.T) {
		// The layer is only proven if the rows are in the buffer when read.
		for _, p := range all {
			if n := bufferedRowsOf(t, p.tn, at); n == 0 {
				t.Fatalf("tenant %s:%s: nothing in the insert buffer before the buffer-layer compare; the rows were read from Parquet", p.tn.account, p.tn.project)
			}
		}
		compareAll(t, false)
		for _, p := range all {
			if n := bufferedRowsOf(t, p.tn, at); n == 0 {
				t.Fatalf("tenant %s:%s: the buffer was drained during the buffer-layer compare; part of it read Parquet", p.tn.account, p.tn.project)
			}
		}
	})
	flushed(t)
	for _, p := range all {
		for _, base := range []string{vtBaseURL, lhtBaseURL} {
			pushExtrasTrace(t, base, p.tn, p.ids[1], at.Add(time.Second))
		}
	}
	pushedBatches = 2
	flushed(t)
	t.Run("parquet", func(t *testing.T) {
		// Two objects per tenant, none buffered: the read is Parquet only and
		// not yet compacted.
		for _, p := range all {
			if n := tenantFiles(t, p.tn.account, p.tn.project); n != 2 {
				t.Fatalf("tenant %s:%s has %d objects before the parquet-layer compare, want 2", p.tn.account, p.tn.project, n)
			}
			if n := bufferedRowsOf(t, p.tn, at); n != 0 {
				t.Fatalf("tenant %s:%s still has %d rows in the insert buffer", p.tn.account, p.tn.project, n)
			}
		}
		compareAll(t, true)
		for _, p := range all {
			if n := tenantFiles(t, p.tn.account, p.tn.project); n != 2 {
				t.Fatalf("tenant %s:%s has %d objects after the parquet-layer compare, want 2 (something merged them)", p.tn.account, p.tn.project, n)
			}
		}
	})
	recompactPartition(t, at)
	t.Run("compacted", func(t *testing.T) { compareAll(t, true) })
}

// Minimal protobuf encoding of an OTLP ExportTraceServiceRequest, enough to send
// strings that are not valid UTF-8 (OTLP/JSON cannot carry them; protobuf
// strings are bytes on the wire and VictoriaTraces does not validate them).
func pbVarint(v uint64) []byte {
	var b []byte
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func pbField(num int, wire int, payload []byte) []byte {
	return append(pbVarint(uint64(num<<3|wire)), payload...)
}

func pbBytes(num int, b []byte) []byte {
	return pbField(num, 2, append(pbVarint(uint64(len(b))), b...))
}

func pbFixed64(num int, v uint64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
	return pbField(num, 1, b)
}

func pbCat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func pbKV(key, val string) []byte {
	return pbBytes(1, pbCat(pbBytes(1, []byte(key)), pbBytes(2, pbBytes(1, []byte(val)))))
}

// invalidUTF8Trace is one span with an event and a link whose names and values
// hold bytes that are not valid UTF-8.
func invalidUTF8Trace(traceID string, at time.Time) []byte {
	tid, _ := hex.DecodeString(traceID)
	sid, _ := hex.DecodeString("00000000000000c1")
	ev := pbCat(pbFixed64(1, uint64(at.UnixNano())), pbBytes(2, []byte("\xff\xfe")),
		pbBytes(3, pbCat(pbBytes(1, []byte("bin")), pbBytes(2, pbBytes(1, []byte("a\x80b"))))))
	lk := pbCat(pbBytes(1, bytes.Repeat([]byte{0xab}, 16)), pbBytes(2, []byte{0, 0, 0, 0, 0, 0, 0, 0xb1}),
		pbBytes(4, pbCat(pbBytes(1, []byte("raw")), pbBytes(2, pbBytes(1, []byte("\xc3("))))))
	span := pbCat(pbBytes(1, tid), pbBytes(2, sid), pbBytes(5, []byte("bad-utf8")), pbField(6, 0, pbVarint(2)),
		pbFixed64(7, uint64(at.Add(-time.Second).UnixNano())), pbFixed64(8, uint64(at.UnixNano())),
		pbBytes(11, ev), pbBytes(13, lk))
	rs := pbCat(
		pbBytes(1, pbKV("service.name", "utf8-svc")),
		pbBytes(2, pbCat(pbBytes(1, pbBytes(1, []byte("utf8-scope"))), pbBytes(2, span))))
	return pbBytes(1, rs)
}

// #434: event and link strings that are not valid UTF-8 are answered the same
// by Lakehouse as by hot VictoriaTraces, in the buffer and from Parquet.
func TestParity_Traces_EventsLinksInvalidUTF8(t *testing.T) {
	tn := extrasTenant{"7314", "0"}
	at := openHourSpanTime(t)
	id := fmt.Sprintf("%016x%016x", time.Now().UnixNano(), 0xe0ff)
	for _, base := range []string{vtBaseURL, lhtBaseURL} {
		req, err := http.NewRequest(http.MethodPost, base+"/insert/opentelemetry/v1/traces", bytes.NewReader(invalidUTF8Trace(id, at)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("AccountID", tn.account)
		req.Header.Set("ProjectID", tn.project)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("push to %s: %v", base, err)
		}
		b := readAllOrEmpty(resp)
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("push to %s: status %d: %s", base, resp.StatusCode, b)
		}
	}
	compare := func(t *testing.T) {
		params := tracesWindow()
		params.Set("query", fmt.Sprintf(`trace_id:=%q`, id))
		var hot, cold map[string]string
		deadline := time.Now().Add(60 * time.Second)
		for {
			ref := tenantFetch(t, vtBaseURL, "/select/logsql/query", params, tn.account, tn.project)
			sut := tenantFetch(t, lhtBaseURL, "/select/logsql/query", params, tn.account, tn.project)
			hr, cr := parseNDJSON(ref.Body), parseNDJSON(sut.Body)
			if len(hr) == 1 && len(cr) == 1 {
				hot, cold = extrasFields(hr[0]), extrasFields(cr[0])
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the span never answered on both tiers: hot=%d cold=%d rows", len(hr), len(cr))
			}
			time.Sleep(time.Second)
		}
		if len(hot) < 6 {
			t.Fatalf("vacuous: hot returned only %d event/link/scope fields: %v", len(hot), hot)
		}
		for k, v := range hot {
			if cold[k] != v {
				t.Errorf("%s: cold %q, hot %q", k, cold[k], v)
			}
		}
		for k := range cold {
			if _, ok := hot[k]; !ok {
				t.Errorf("cold has %s, hot does not", k)
			}
		}
	}
	t.Run("buffer", compare)
	waitLeftBufferTenant(t, lhtBaseURL, "traces", tn.account, tn.project, at.Add(-time.Minute), at.Add(time.Minute))
	t.Run("parquet", compare)
}

// The forward fence's counter is 0 on this single-version stack, on both
// binaries: a non-zero value would be a false positive (an object the running
// code wrote itself, refused as unknown). Runs after the cases that flush and
// compact.
func TestParity_ForwardFenceCounterStaysZero(t *testing.T) {
	for name, base := range map[string]string{"logs": lhBaseURL, "traces": lhtBaseURL} {
		body := string(fetch(t, base, "/metrics", nil).Body)
		found := 0
		for _, line := range strings.Split(body, "\n") {
			if !strings.HasPrefix(line, "lakehouse_compaction_skipped_unknown_columns_total{") {
				continue
			}
			found++
			if !strings.HasSuffix(line, " 0") {
				t.Errorf("%s: %s", name, line)
			}
		}
		if found != 4 {
			t.Errorf("%s: %d fence series exported, want 4 (signal x op)", name, found)
		}
	}
}
