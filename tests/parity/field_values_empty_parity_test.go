//go:build parity

package parity

// field_values counts a row without the field (or with an empty value) as one
// hit of the empty value, like upstream's uniq; stream_field_values lists the
// values of the stream tags of the matching streams and never the empty value;
// the substring `filter` applies before `limit`. The cases write the same rows
// to hot and Lakehouse and compare while the rows are still in Lakehouse's
// insert buffer, and again once they are Parquet only. The numeric tenants are
// owned by this file, in the numeric and the alias form, and the layers are the
// ones of TestParity_AllColumnSortTieOrder: buffer, Parquet, compacted and after
// a restart of the Lakehouse service.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

const (
	fvEmptyLogsAccount   = "7321"
	fvEmptyTracesAccount = "7322"
)

type fvEmptyCase struct {
	name       string
	endpoint   string // field_values or stream_field_values
	query      string
	field      string
	extra      map[string]string
	allowEmpty bool
	// pastLimit marks a field_values case whose limit is below the number of
	// distinct values. Upstream runs it as a uniq pipe with a limit, and when the
	// limit is exceeded pipeUniqProcessor.flush deletes entries while ranging over
	// Go maps (deps/VictoriaLogs/lib/logstorage/pipe_uniq.go, flush: the three
	// `for ... := range hm.u64 / hm.negative64 / hm.strings` loops with delete),
	// whose iteration order is randomised by the language, so which values survive
	// is not deterministic for the same data on the same stack. Both tiers must
	// answer as many values, all with zero hits. stream_field_values does not go
	// through that pipe (it sorts and truncates), so its limit cases are exact.
	pastLimit bool
	// ignoreEmpty drops the empty-value bucket from both answers before the
	// comparison: hot VictoriaTraces counts its trace index rows (one per trace,
	// no span attributes) under `*`, which Lakehouse drops at flush (issue 458).
	// Every other value and its hits are compared exactly.
	ignoreEmpty bool
}

// waitRowsVisible returns once base answers query for the tenant form with
// want rows: a row just written is searchable on hot VictoriaLogs/VictoriaTraces
// only after its in-memory part flushes.
func waitRowsVisible(t *testing.T, base string, cold bool, f tenantForm, query string, from, to time.Time, want int) {
	t.Helper()
	params := url.Values{
		"query": {query}, "disable_latency_offset": {"true"}, "limit": {"1000"},
		"start": {fmt.Sprintf("%d", from.UnixNano())},
		"end":   {fmt.Sprintf("%d", to.UnixNano())},
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		r := getWith(t, base, "/select/logsql/query", params, f.header(cold))
		if n := len(parseNDJSON(r.Body)); r.StatusCode == http.StatusOK && n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the %d written rows never became visible for %q (tenant %s)", base, want, query, f.name)
		}
		time.Sleep(time.Second)
	}
}

func runFvEmptyCases(t *testing.T, hot, cold string, f tenantForm, from, to time.Time, filter string, cases []fvEmptyCase) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			params := url.Values{
				"query": {strings.ReplaceAll(c.query, "@F", filter)}, "field": {c.field}, "disable_latency_offset": {"true"},
				"start": {fmt.Sprintf("%d", from.UnixNano())},
				"end":   {fmt.Sprintf("%d", to.UnixNano())},
			}
			for k, v := range c.extra {
				params.Set(k, v)
			}
			path := "/select/logsql/" + c.endpoint
			ref := getWith(t, hot, path, params, f.header(false))
			sut := getWith(t, cold, path, params, f.header(true))
			if c.pastLimit {
				want := fieldValueHits(t, c.name+" reference", ref)
				got := fieldValueHits(t, c.name+" SUT", sut)
				if len(want) == 0 || len(got) == 0 {
					t.Fatalf("%s: a past-limit case needs values on both sides (hot %v, cold %v)", c.name, want, got)
				}
				if len(want) != len(got) {
					t.Errorf("%s: hot lists %d values, cold %d (%v vs %v)", c.name, len(want), len(got), want, got)
				}
				for _, m := range []map[string]int{want, got} {
					for v, h := range m {
						if h != 0 {
							t.Errorf("%s: value %q has hits %d past the limit, want 0", c.name, v, h)
						}
					}
				}
				return
			}
			if c.ignoreEmpty {
				ref, sut = withoutEmptyBucket(t, ref), withoutEmptyBucket(t, sut)
			}
			compareFieldValues(t, c.name, ref, sut, c.allowEmpty)
		})
	}
}

// fvEmptyLayer is one data layer the same cases are compared in, as in
// TestParity_AllColumnSortTieOrder: prepare brings the stack into the layer and
// returns how many rows each tenant then holds, prove checks it for one tenant
// form before and after the compares.
type fvEmptyLayer struct {
	name    string
	prepare func(t *testing.T, c *fvEmptyCase2) int
	prove   func(t *testing.T, c *fvEmptyCase2, f tenantForm)
}

// fvEmptyCase2 is one signal's fixture; every tenant form writes and reads its
// own tenant, so the cells are layer x tenant form x signal.
type fvEmptyCase2 struct {
	hot, cold      string
	mode           string
	forms          []tenantForm
	at             time.Time
	from, to       time.Time
	filter         string
	cases          []fvEmptyCase
	write          func(t *testing.T, c *fvEmptyCase2, batch int)
	restart        []string
	startedAt      time.Time
	rowsPer        int // rows one batch writes per tenant form
	restartBatches int // batches the restart layer wrote (retries add one each)
}

func runFvEmptyLayers(t *testing.T, c *fvEmptyCase2) {
	buffered := func(t *testing.T, rows int) {
		for _, f := range c.forms {
			waitBuffered(t, c.cold, c.mode, f.account, c.from, c.to, rows)
		}
	}
	leftBuffer := func(t *testing.T) {
		for _, f := range c.forms {
			waitLeftBuffer(t, c.cold, c.mode, f.account, c.from, c.to)
		}
	}
	layers := []fvEmptyLayer{
		{"buffer",
			func(t *testing.T, c *fvEmptyCase2) int { c.write(t, c, 0); buffered(t, c.rowsPer); return c.rowsPer },
			func(t *testing.T, c *fvEmptyCase2, f tenantForm) {
				requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, c.rowsPer)
			}},
		{"parquet",
			func(t *testing.T, c *fvEmptyCase2) int { leftBuffer(t); return c.rowsPer },
			func(t *testing.T, c *fvEmptyCase2, f tenantForm) {
				requireFlushedOnly(t, c.cold, c.mode, f.account, c.at, c.from, c.to)
			}},
		{"compacted",
			func(t *testing.T, c *fvEmptyCase2) int {
				c.write(t, c, 1)
				buffered(t, c.rowsPer)
				leftBuffer(t)
				recompactHourPartition(t, c.cold, c.at, 2*len(c.forms))
				return 2 * c.rowsPer
			},
			func(t *testing.T, c *fvEmptyCase2, f tenantForm) {
				requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, 0)
				requireCompactedOnce(t, c.mode, f.account, c.at)
			}},
		// The compacted layer ended with the buffer empty, right after a flush.
		// A third batch is written and the service restarted at once, so the
		// regular 5 s flush should not write the segment before the restart: the
		// new L0 is then the recovered segment's, written after the new
		// StartedAt. The index worker and the service-graph task can open a
		// segment first (traces), and then the 5 s seal can win; the attempt is
		// repeated with another batch, at most fvRestartAttempts times in all.
		// waitFlushIdle / the attempt counting below duplicate what the layer
		// controls of #459 do (waitFlushIdle, waitL0Objects, restartAttempts):
		// remove this copy once #459 is on main.
		{"restart",
			func(t *testing.T, c *fvEmptyCase2) int {
				for attempt := 0; attempt < fvRestartAttempts; attempt++ {
					for _, f := range c.forms {
						waitLeftBuffer(t, c.cold, c.mode, f.account, c.from, c.to)
					}
					c.write(t, c, 2+attempt)
					c.restartBatches = attempt + 1
					c.startedAt = restartComposeServices(t, c.cold, c.mode, c.restart)
					leftBuffer(t)
					recovered := true
					for _, f := range c.forms {
						if fvL0WrittenAfter(t, c.mode, f.account, c.at, c.startedAt) < 1 {
							recovered = false
						}
					}
					if recovered {
						break
					}
					t.Logf("restart attempt %d: the periodic flush wrote the batch before the restart; retrying", attempt+1)
				}
				return (2 + c.restartBatches) * c.rowsPer
			},
			func(t *testing.T, c *fvEmptyCase2, f tenantForm) {
				requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, 0)
				if n := fvL0WrittenAfter(t, c.mode, f.account, c.at, c.startedAt); n < 1 {
					t.Fatalf("layer proof: tenant %s has no L0 object written after the restart started (%s): the batch was flushed before the restart in every attempt, not recovered from its segment", f.account, c.startedAt.UTC().Format(time.RFC3339Nano))
				}
			}},
	}
	for _, l := range layers {
		t.Run(l.name, func(t *testing.T) {
			rows := l.prepare(t, c)
			for _, f := range c.forms {
				t.Run(f.name, func(t *testing.T) {
					l.prove(t, c, f)
					waitRowsVisible(t, c.hot, false, f, c.filter, c.from, c.to, rows)
					waitRowsVisible(t, c.cold, true, f, c.filter, c.from, c.to, rows)
					runFvEmptyCases(t, c.hot, c.cold, f, c.from, c.to, c.filter, c.cases)
					l.prove(t, c, f)
				})
			}
		})
	}
}

func TestParity_FieldValues_EmptyBucket(t *testing.T) {
	stamp := time.Now().UnixNano()
	at := pickQuietHour(t, map[string][]string{
		"logs": {fvEmptyLogsAccount, allColumnSortLogsAliasAccount}, "traces": {fvEmptyTracesAccount, parityTracesAccount}})
	from, to := at.Add(-time.Minute), at.Add(time.Minute)

	t.Run("logs", func(t *testing.T) {
		t.Parallel()
		token := fmt.Sprintf("fve%d", stamp)
		q := "@F"
		c := &fvEmptyCase2{hot: vlBaseURL, cold: lhBaseURL, mode: "logs", at: at, from: from, to: to, filter: "_msg:=" + token, rowsPer: 4,
			forms:   []tenantForm{numericTenant(fvEmptyLogsAccount), aliasTenant(allColumnSortLogsAliasAccount, parityLogsOrgID)},
			restart: []string{"lakehouse-logs"}}
		c.write = func(t *testing.T, c *fvEmptyCase2, batch int) {
			rows := []map[string]string{
				{"app": "ax", "ns": "n1", "lk": "v1"},
				{"app": "ax", "ns": "n1"},
				{"app": "ay"},
				{"app": "ay", "lk": ""},
			}
			var body bytes.Buffer
			for i, r := range rows {
				r["_time"] = at.Add(time.Duration(batch*len(rows)+i) * time.Second).Format(time.RFC3339Nano)
				r["_msg"] = token
				b, _ := json.Marshal(r)
				body.Write(b)
				body.WriteByte('\n')
			}
			for _, f := range c.forms {
				post(t, c.hot+"/insert/jsonline?_stream_fields=app,ns", "application/stream+json", body.Bytes(), f.header(false))
				post(t, c.cold+"/insert/jsonline?_stream_fields=app,ns", "application/stream+json", body.Bytes(), f.header(true))
			}
		}
		c.cases = []fvEmptyCase{
			{name: "field_values_ns", endpoint: "field_values", query: q, field: "ns"},
			{name: "stream_field_values_ns_has_no_empty", endpoint: "stream_field_values", query: q, field: "ns"},
			{name: "field_values_absent_everywhere", endpoint: "field_values", query: q, field: "nosuch"},
			{name: "field_values_lk_filtered_query", endpoint: "field_values", query: q + " app:=ay", field: "lk"},
			{name: "field_values_lk_limit1", endpoint: "field_values", query: q, field: "lk", extra: map[string]string{"limit": "1"}, pastLimit: true},
			{name: "field_values_lk_limit2", endpoint: "field_values", query: q, field: "lk", extra: map[string]string{"limit": "2"}},
			{name: "field_values_lk_substring_filter", endpoint: "field_values", query: q, field: "lk", extra: map[string]string{"filter": "v"}},
			{name: "field_values_lk_filter_then_limit", endpoint: "field_values", query: q, field: "lk", extra: map[string]string{"filter": "v", "limit": "1"}},
			{name: "stream_field_values_nosuch", endpoint: "stream_field_values", query: q, field: "nosuch", allowEmpty: true},
			// lk is a column on one row but no stream tag: hot answers nothing.
			{name: "stream_field_values_non_stream_field", endpoint: "stream_field_values", query: q, field: "lk", allowEmpty: true},
			{name: "stream_field_values_app", endpoint: "stream_field_values", query: q, field: "app"},
			// Upstream sorts by hits then value and truncates (no uniq pipe), so
			// the kept value is deterministic: exact, not shape-only.
			{name: "stream_field_values_limit1_exact", endpoint: "stream_field_values", query: q, field: "app", extra: map[string]string{"limit": "1"}},
			// Filter, then sort by hits, truncate, zero: exact on this path.
			{name: "stream_field_values_filter_limit_exact", endpoint: "stream_field_values", query: q, field: "app", extra: map[string]string{"filter": "a", "limit": "1"}},
		}
		runFvEmptyLayers(t, c)
	})

	t.Run("traces", func(t *testing.T) {
		t.Parallel()
		type span struct {
			name     string
			spanAttr map[string]string
			resAttr  map[string]string
		}
		spans := []span{
			{"op", map[string]string{"sa": "v1"}, map[string]string{"ra": "r1"}},
			{"op", nil, map[string]string{"ra": "r1"}},
			{"op2", nil, nil},
			{"", nil, nil},
		}
		var ids []string
		c := &fvEmptyCase2{hot: vtBaseURL, cold: lhtBaseURL, mode: "traces", at: at, from: from, to: to, rowsPer: len(spans),
			forms:   []tenantForm{numericTenant(fvEmptyTracesAccount), aliasTenant(parityTracesAccount, parityTracesOrgID)},
			restart: []string{"lakehouse-traces"}}
		c.write = func(t *testing.T, c *fvEmptyCase2, batch int) {
			for i, sp := range spans {
				id := fmt.Sprintf("%016x%016x", stamp, 0xfe00+batch*len(spans)+i)
				ids = append(ids, id)
				for _, f := range c.forms {
					pushOTLPSpanAttrsAs(t, c.hot, f.header(false), id, fmt.Sprintf("%016x", batch*len(spans)+i+1), sp.name, "fv-svc", sp.spanAttr, sp.resAttr, at.Add(time.Duration(batch*len(spans)+i)*time.Second))
					pushOTLPSpanAttrsAs(t, c.cold, f.header(true), id, fmt.Sprintf("%016x", batch*len(spans)+i+1), sp.name, "fv-svc", sp.spanAttr, sp.resAttr, at.Add(time.Duration(batch*len(spans)+i)*time.Second))
				}
			}
			c.filter = "trace_id:in(" + strings.Join(ids, ",") + ")"
		}
		// The cases carry the placeholder @F: the filter that selects exactly the
		// rows this case wrote (the trace ids so far).
		q := "@F"
		c.cases = []fvEmptyCase{
			{name: "field_values_span_attr_some_spans", endpoint: "field_values", query: q, field: "span_attr:sa"},
			{name: "field_values_resource_attr_some_spans", endpoint: "field_values", query: q, field: "resource_attr:ra"},
			{name: "field_values_name", endpoint: "field_values", query: q, field: "name"},
			{name: "field_values_absent_everywhere", endpoint: "field_values", query: q, field: "span_attr:nosuch"},
			{name: "field_values_filtered_query", endpoint: "field_values", query: q + " name:=op2", field: "span_attr:sa"},
			{name: "field_values_limit1", endpoint: "field_values", query: q, field: "span_attr:sa", extra: map[string]string{"limit": "1"}, pastLimit: true},
			{name: "field_values_substring_filter_then_limit", endpoint: "field_values", query: q, field: "name", extra: map[string]string{"filter": "op", "limit": "1"}, pastLimit: true},
			{name: "stream_field_values_name_has_no_empty", endpoint: "stream_field_values", query: q, field: "name"},
			{name: "stream_field_values_service", endpoint: "stream_field_values", query: q, field: "resource_attr:service.name"},
			{name: "stream_field_values_non_stream_field", endpoint: "stream_field_values", query: q, field: "span_attr:sa", allowEmpty: true},
			{name: "stream_field_values_limit1_exact", endpoint: "stream_field_values", query: q, field: "name", extra: map[string]string{"limit": "1"}},
			{name: "stream_field_values_filter_limit_exact", endpoint: "stream_field_values", query: q, field: "name", extra: map[string]string{"filter": "op", "limit": "1"}},
			{name: "field_values_star_query", endpoint: "field_values", query: "*", field: "span_attr:sa", ignoreEmpty: true},
		}
		runFvEmptyLayers(t, c)
	})
}

func pushOTLPSpanAttrsAs(t *testing.T, base string, hdr http.Header, traceID, spanID, name, service string, spanAttrs, resAttrs map[string]string, at time.Time) {
	t.Helper()
	kv := func(m map[string]string) []map[string]any {
		out := []map[string]any{}
		for k, v := range m {
			out = append(out, map[string]any{"key": k, "value": map[string]any{"stringValue": v}})
		}
		return out
	}
	res := kv(resAttrs)
	res = append(res, map[string]any{"key": "service.name", "value": map[string]any{"stringValue": service}})
	body, _ := json.Marshal(map[string]any{"resourceSpans": []map[string]any{{
		"resource": map[string]any{"attributes": res},
		"scopeSpans": []map[string]any{{
			"scope": map[string]any{"name": "fv-empty-parity"},
			"spans": []map[string]any{{
				"traceId": traceID, "spanId": spanID, "name": name, "kind": 2,
				"startTimeUnixNano": fmt.Sprintf("%d", at.Add(-time.Second).UnixNano()),
				"endTimeUnixNano":   fmt.Sprintf("%d", at.UnixNano()),
				"attributes":        kv(spanAttrs),
			}},
		}},
	}}})
	post(t, base+"/insert/opentelemetry/v1/traces", "application/json", body, hdr)
}

// withoutEmptyBucket returns a field_values answer without its empty-value entry.
func withoutEmptyBucket(t *testing.T, r fetchResult) fetchResult {
	t.Helper()
	obj, err := parseJSON(r.Body)
	if err != nil || r.StatusCode != http.StatusOK {
		return r
	}
	var kept []any
	for _, e := range asSlice(obj["values"]) {
		if m, _ := e.(map[string]any); m != nil && m["value"] == "" {
			continue
		}
		kept = append(kept, e)
	}
	obj["values"] = kept
	b, _ := json.Marshal(obj)
	return fetchResult{StatusCode: r.StatusCode, Body: b}
}

// fvRestartAttempts bounds how often the restart layer is retried.
const fvRestartAttempts = 3

// fvL0WrittenAfter counts the tenant's L0 objects in the partition written at or
// after startedAt (S3 LastModified has second granularity, so both are
// truncated to the second): the flush of the segment a restarted pod recovered.
func fvL0WrittenAfter(t *testing.T, mode, account string, at, startedAt time.Time) int {
	t.Helper()
	n := 0
	for _, o := range partitionObjectInfo(t, mode, account, at) {
		if strings.HasPrefix(o.name, "compacted-L") {
			continue
		}
		if !o.modified.Truncate(time.Second).Before(startedAt.Truncate(time.Second)) {
			n++
		}
	}
	return n
}
