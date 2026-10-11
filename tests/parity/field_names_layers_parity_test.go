//go:build parity

package parity

// field_names and stream_field_names answer from the rows, as upstream does
// (issues 280, 378, 269, 461, 464): the field names VictoriaLogs/VictoriaTraces
// list for the rows (a map's keys, not its column; VictoriaTraces' own names
// for a span, not the Parquet columns), each credited with the matching rows of
// the blocks that list it, and the tags of the matching streams each credited
// with the rows of every stream that carries it, in upstream's order. The cases
// write the same rows to hot and Lakehouse and compare them in every data layer
// the rows pass through (the insert buffer, Parquet, compacted, after a restart
// of the Lakehouse service), for a numeric tenant and for an alias tenant, with
// no filter and with a filter, for both binaries. Apart from the az / op3
// stream below every batch has the same shape, so the answer does not depend on
// how many objects or parts hold it.
//
// The tenants are the ones of TestParity_FieldValues_EmptyBucket; the layers are
// the ones of TestParity_AllColumnSortTieOrder.
//
// One stream of each fixture (app az, operation op3) is written in batches of
// DIFFERENT shapes: its first batch carries a field (onlya) in one request and
// not in the next, and the later batches never carry it. Hot is asked to merge
// after every layer that wrote data (forceMergeHot), because a stream is one block on hot only
// once its parts are merged, and the cases filter to the rows that lack the
// field: a filter selects rows, not the blocks whose columns the answer lists
// (a block, an object or a row group with no matching row still lists its
// columns for the stream; issue 480, review of PR 487).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fnCase is one request compared between hot and Lakehouse. @F in query is the
// filter that selects exactly the rows the fixture wrote.
type fnCase struct {
	name     string
	endpoint string // field_names or stream_field_names
	query    string
	extra    map[string]string
}

// fieldNamesOrder reads the names of a field_names answer in answer order.
func fieldNamesOrder(t *testing.T, label string, r fetchResult) []string {
	t.Helper()
	obj, err := parseJSON(r.Body)
	if err != nil {
		t.Fatalf("%s: parse: %v: %s", label, err, r.Body)
	}
	var out []string
	for _, e := range asSlice(obj["values"]) {
		if m, _ := e.(map[string]any); m != nil {
			v, _ := m["value"].(string)
			out = append(out, v)
		}
	}
	return out
}

// withoutColdOnlyField removes the one field the cold logs path adds to every
// row that has none (issue 274: severity_number "0", and a level next to it for a
// set one) from the cold answer, when hot does not list it. Nothing else is
// dropped, so any other difference in names, hits or order still fails.
func withoutColdOnlyField(t *testing.T, ref, sut fetchResult, name string) fetchResult {
	t.Helper()
	obj, err := parseJSON(sut.Body)
	if err != nil || sut.StatusCode != 200 {
		return sut
	}
	for _, e := range asSlice(obj["values"]) {
		if m, _ := e.(map[string]any); m != nil && m["value"] == name {
			for _, r := range fieldNamesOrder(t, "reference", ref) {
				if r == name {
					return sut // hot lists it too: compare as is
				}
			}
		}
	}
	var kept []any
	for _, e := range asSlice(obj["values"]) {
		if m, _ := e.(map[string]any); m != nil && m["value"] == name {
			continue
		}
		kept = append(kept, e)
	}
	obj["values"] = kept
	b, _ := json.Marshal(obj)
	return fetchResult{StatusCode: sut.StatusCode, Body: b}
}

func runFnCases(t *testing.T, hot, cold string, f tenantForm, from, to time.Time, filter string, cases []fnCase) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			params := url.Values{
				"query": {strings.ReplaceAll(c.query, "@F", filter)}, "disable_latency_offset": {"true"},
				"start": {fmt.Sprintf("%d", from.UnixNano())},
				"end":   {fmt.Sprintf("%d", to.UnixNano())},
			}
			for k, v := range c.extra {
				params.Set(k, v)
			}
			path := "/select/logsql/" + c.endpoint
			ref := getWith(t, hot, path, params, f.header(false))
			sut := getWith(t, cold, path, params, f.header(true))
			sut = withoutColdOnlyField(t, ref, sut, "severity_number")
			// hits written as JSON numbers by both (compareFieldValues reads either form) ...
			requireNumericHits(t, c.name, ref, sut)
			// the same names with the same hits ...
			compareFieldValues(t, c.name, ref, sut, false)
			// ... in the same order (hits descending, then name)
			if want, got := fieldNamesOrder(t, c.name+" reference", ref), fieldNamesOrder(t, c.name+" SUT", sut); !reflect.DeepEqual(want, got) {
				t.Errorf("%s: order differs:\n hot:  %v\n cold: %v", c.name, want, got)
			}
			reportLockCells(t, 1) // one cell per compared answer's order (floor in lock_cells.txt)
		})
	}
}

// fnLayer is one data layer the same cases are compared in, as in
// TestParity_AllColumnSortTieOrder: prepare brings the stack into the layer and
// returns how many rows each tenant then holds, prove checks it for one tenant
// form before and after the compares.
type fnLayer struct {
	name    string
	prepare func(t *testing.T, c *fnFixture) int
	prove   func(t *testing.T, c *fnFixture, f tenantForm)
}

// fnFixture is one signal's fixture; every tenant form writes and reads its
// own tenant, so the cells are layer x tenant form x signal.
type fnFixture struct {
	hot, cold      string
	mode           string
	forms          []tenantForm
	at             time.Time
	from, to       time.Time
	filter         string
	cases          []fnCase
	write          func(t *testing.T, c *fnFixture, batch int)
	restart        []string
	startedAt      time.Time
	rowsPer        int // rows one batch writes per tenant form
	restartBatches int // batches the restart layer wrote (retries add one each)
	// afterWrite runs once after a layer that wrote data has been brought into
	// its state and before the cases are compared (hot is asked to merge there,
	// not in write: the restart layer restarts the service right after a write,
	// and a delay between the two lets the periodic flush win the race).
	afterWrite func(t *testing.T)
	written    bool
}

func runFnLayers(t *testing.T, c *fnFixture) {
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
	layers := []fnLayer{
		{"buffer",
			func(t *testing.T, c *fnFixture) int { c.write(t, c, 0); buffered(t, c.rowsPer); return c.rowsPer },
			func(t *testing.T, c *fnFixture, f tenantForm) {
				requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, c.rowsPer)
			}},
		{"parquet",
			func(t *testing.T, c *fnFixture) int { leftBuffer(t); return c.rowsPer },
			func(t *testing.T, c *fnFixture, f tenantForm) {
				requireFlushedOnly(t, c.cold, c.mode, f.account, c.at, c.from, c.to)
			}},
		{"compacted",
			func(t *testing.T, c *fnFixture) int {
				c.write(t, c, 1)
				buffered(t, c.rowsPer)
				leftBuffer(t)
				recompactHourPartition(t, c.cold, c.at, 2*len(c.forms))
				return 2 * c.rowsPer
			},
			func(t *testing.T, c *fnFixture, f tenantForm) {
				requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, 0)
				requireCompactedOnce(t, c.mode, f.account, c.at)
			}},
		// A third batch is written into a fresh segment (right after a flush has
		// completed, so the periodic flush is a full interval away) and the
		// service is restarted at once; the restarted pod recovers the segment and
		// flushes it, which the proof sees as the one L0 object written after the
		// new StartedAt. If the periodic flush still won the race the attempt is
		// repeated with another batch, at most restartAttempts times in all (the
		// same helpers and bounds as TestParity_AllColumnSortTieOrder).
		{"restart",
			func(t *testing.T, c *fnFixture) int {
				for attempt := 1; attempt <= restartAttempts; attempt++ {
					for _, f := range c.forms {
						waitFlushIdle(t, c.cold, c.mode, f.account, c.from, c.to)
					}
					c.write(t, c, 1+attempt)
					c.restartBatches = attempt
					c.startedAt = restartComposeServices(t, c.cold, c.mode, c.restart, restartStopTimeout)
					for _, f := range c.forms {
						if n := waitL0Objects(t, c.mode, f.account, c.at, attempt, 90*time.Second); n < attempt {
							t.Fatalf("tenant %s has %d L0 objects 90s after the restart, want %d: the acknowledged batch was not recovered", f.account, n, attempt)
						}
					}
					leftBuffer(t)
					lost := ""
					for _, f := range c.forms {
						if why := recoveredSegmentFlushed(t, c.mode, f.account, c.at, c.startedAt, attempt); why != "" {
							lost = why
						}
					}
					if lost == "" {
						return (2 + attempt) * c.rowsPer
					}
					t.Logf("restart attempt %d of %d: the periodic flush won the race (%s); retrying with a new batch", attempt, restartAttempts, lost)
				}
				t.Fatalf("the periodic flush won the race in all %d restart attempts", restartAttempts)
				return 0
			},
			func(t *testing.T, c *fnFixture, f tenantForm) {
				requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, 0)
				if why := recoveredSegmentFlushed(t, c.mode, f.account, c.at, c.startedAt, c.restartBatches); why != "" {
					t.Fatalf("layer proof: %s", why)
				}
			}},
	}
	for _, l := range layers {
		t.Run(l.name, func(t *testing.T) {
			rows := l.prepare(t, c)
			if c.afterWrite != nil && c.written {
				c.afterWrite(t)
				c.written = false
			}
			for _, f := range c.forms {
				t.Run(f.name, func(t *testing.T) {
					l.prove(t, c, f)
					waitRowsVisible(t, c.hot, false, f, c.filter, c.from, c.to, rows)
					waitRowsVisible(t, c.cold, true, f, c.filter, c.from, c.to, rows)
					runFnCases(t, c.hot, c.cold, f, c.from, c.to, c.filter, c.cases)
					l.prove(t, c, f)
				})
			}
		})
	}
}

func TestParity_FieldNames_Layers(t *testing.T) {
	stamp := time.Now().UnixNano()
	at := pickQuietHour(t, map[string][]string{
		"logs": {fvEmptyLogsAccount, allColumnSortLogsAliasAccount}, "traces": {fvEmptyTracesAccount, parityTracesAccount}})
	from, to := at.Add(-time.Minute), at.Add(time.Minute)

	t.Run("logs", func(t *testing.T) {
		t.Parallel()
		token := fmt.Sprintf("fn%d", stamp)
		q := "@F"
		var base []fnCase
		c := &fnFixture{hot: vlBaseURL, cold: lhBaseURL, mode: "logs", at: at, from: from, to: to, filter: "_msg:=" + token, rowsPer: 10,
			forms:   []tenantForm{numericTenant(fvEmptyLogsAccount), aliasTenant(allColumnSortLogsAliasAccount, parityLogsOrgID)},
			restart: []string{"lakehouse-logs"}}
		c.write = func(t *testing.T, c *fnFixture, batch int) {
			// Two streams (app, ns). The ax stream has a field only some of its rows
			// carry (lk, sparse), a field only it has (only_ax) and a field set to
			// the empty string (empty_v); the ay stream has none of them.
			rows := []map[string]string{
				{"app": "ax", "ns": "n1", "lk": "v1", "only_ax": "o"},
				{"app": "ax", "ns": "n1", "only_ax": "o", "empty_v": ""},
				{"app": "ax", "ns": "n1"},
				{"app": "ay", "ns": "n2", "common": "c"},
				{"app": "ay", "ns": "n2", "common": "c"},
				{"app": "ay", "ns": "n2"},
			}
			send := func(rows []map[string]string, offset int) {
				var body bytes.Buffer
				for i, r := range rows {
					r["_time"] = at.Add(time.Duration(batch*10+offset+i) * time.Second).Format(time.RFC3339Nano)
					r["_msg"] = token
					// a stream of its own per run: upstream credits a column with the rows of the
					// blocks that list it, and the blocks of a stream written by an earlier run
					// (other fields, same day) would otherwise list theirs
					r["run"] = token
					b, _ := json.Marshal(r)
					body.Write(b)
					body.WriteByte('\n')
				}
				for _, f := range c.forms {
					post(t, c.hot+"/insert/jsonline?_stream_fields=app,ns,run", "application/stream+json", body.Bytes(), f.header(false))
					post(t, c.cold+"/insert/jsonline?_stream_fields=app,ns,run", "application/stream+json", body.Bytes(), f.header(true))
				}
			}
			send(rows, 0)
			// The az stream, whose batches differ in shape: the first batch is two
			// requests, the first carrying onlya, the second not; every later batch
			// is one request without it.
			az := func(part string, withField bool) map[string]string {
				r := map[string]string{"app": "az", "ns": "n3", "part": part, "gen": fmt.Sprintf("g%d", batch)}
				if withField {
					r["onlya"] = "a"
				}
				return r
			}
			if batch == 0 {
				send([]map[string]string{az("first", true), az("first", true)}, 6)
				send([]map[string]string{az("second", false), az("second", false)}, 8)
			} else {
				send([]map[string]string{az("later", false), az("later", false), az("later", false), az("later", false)}, 6)
			}
			c.written = true
			c.cases = append(append([]fnCase{}, base...),
				fnCase{name: "field_names_rows_without_the_field", endpoint: "field_names", query: q + " part:=second"},
				fnCase{name: "field_names_rows_with_the_field", endpoint: "field_names", query: q + " part:=first"})
			if batch > 0 {
				// the rows of the newest batch alone: after the restart layer it is an object of
				// its own, and the objects that carry onlya have no matching row
				c.cases = append(c.cases, fnCase{name: "field_names_rows_of_the_newest_batch_only", endpoint: "field_names", query: q + fmt.Sprintf(" gen:=g%d", batch)})
			}
		}
		c.afterWrite = func(t *testing.T) { forceMergeHot(t, c.hot, "logs", at) }
		base = []fnCase{
			{name: "field_names", endpoint: "field_names", query: q},
			{name: "field_names_filtered_stream", endpoint: "field_names", query: q + " app:=ay"},
			{name: "field_names_filtered_by_a_sparse_field", endpoint: "field_names", query: q + " lk:=v1"},
			{name: "field_names_substring_filter", endpoint: "field_names", query: q, extra: map[string]string{"filter": "ly"}},
			{name: "stream_field_names", endpoint: "stream_field_names", query: q},
			{name: "stream_field_names_filtered", endpoint: "stream_field_names", query: q + " app:=ay"},
			{name: "stream_field_names_substring_filter", endpoint: "stream_field_names", query: q, extra: map[string]string{"filter": "n"}},
		}
		runFnLayers(t, c)
	})

	t.Run("traces", func(t *testing.T) {
		t.Parallel()
		type span struct {
			name     string
			spanAttr map[string]string
			resAttr  map[string]string
		}
		// Two streams (the operation name). The op stream has a span attribute only
		// some of its spans carry and a resource attribute they all do; the op2
		// stream has neither.
		spans := []span{
			{"op", map[string]string{"sa": "v1"}, map[string]string{"ra": "r1"}},
			{"op", nil, map[string]string{"ra": "r1"}},
			{"op2", nil, nil},
			{"op2", map[string]string{"other": "x"}, nil},
		}
		var ids []string
		q := "@F"
		var base []fnCase
		c := &fnFixture{hot: vtBaseURL, cold: lhtBaseURL, mode: "traces", at: at, from: from, to: to, rowsPer: len(spans) + 4,
			forms:   []tenantForm{numericTenant(fvEmptyTracesAccount), aliasTenant(parityTracesAccount, parityTracesOrgID)},
			restart: []string{"lakehouse-traces"}}
		c.write = func(t *testing.T, c *fnFixture, batch int) {
			// One request per batch and target for the op and op2 streams: upstream
			// credits a field with the rows of the blocks that list it, and the rows of
			// separate requests are blocks of separate in-memory parts until they merge,
			// a layout that depends on timing (the same spans in two requests a second
			// apart credit "sa" and "other" differently on hot, and differently again
			// after a merge). The rows of one request are one block of their stream, so
			// the answer is defined by the rows. The op3 stream below is written in
			// several requests on purpose, and hot is merged after the write.
			batchSpans := make([]fnOTLPSpan, 0, len(spans))
			for i, sp := range spans {
				id := fmt.Sprintf("%016x%016x", stamp, 0xfa00+batch*len(spans)+i)
				ids = append(ids, id)
				batchSpans = append(batchSpans, fnOTLPSpan{traceID: id, spanID: fmt.Sprintf("%016x", batch*len(spans)+i+1), name: sp.name,
					service: fmt.Sprintf("fn-svc-%d", stamp), spanAttrs: sp.spanAttr, resAttrs: sp.resAttr, at: at.Add(time.Duration(batch*len(spans)+i) * time.Second)})
			}
			send := func(spans []fnOTLPSpan) {
				for _, f := range c.forms {
					pushOTLPSpansOneRequest(t, c.hot, f.header(false), spans)
					pushOTLPSpansOneRequest(t, c.cold, f.header(true), spans)
				}
			}
			send(batchSpans)
			// The op3 stream, whose batches differ in shape: the first batch is two
			// requests, the first carrying the attribute onlya, the second not; every
			// later batch is one request without it.
			op3 := func(i int, part string, withField bool) fnOTLPSpan {
				id := fmt.Sprintf("%016x%016x", stamp, 0xfb00+batch*10+i)
				ids = append(ids, id)
				attrs := map[string]string{"part": part, "gen": fmt.Sprintf("g%d", batch)}
				if withField {
					attrs["onlya"] = "a"
				}
				return fnOTLPSpan{traceID: id, spanID: fmt.Sprintf("%016x", 0x100+batch*10+i), name: "op3",
					service: fmt.Sprintf("fn-svc-%d", stamp), spanAttrs: attrs, at: at.Add(time.Duration(batch*10+6+i) * time.Second)}
			}
			if batch == 0 {
				send([]fnOTLPSpan{op3(0, "first", true), op3(1, "first", true)})
				send([]fnOTLPSpan{op3(2, "second", false), op3(3, "second", false)})
			} else {
				send([]fnOTLPSpan{op3(0, "later", false), op3(1, "later", false), op3(2, "later", false), op3(3, "later", false)})
			}
			c.written = true
			c.filter = "trace_id:in(" + strings.Join(ids, ",") + ")"
			c.cases = append(append([]fnCase{}, base...),
				fnCase{name: "field_names_spans_without_the_attribute", endpoint: "field_names", query: q + " `span_attr:part`:=second"},
				fnCase{name: "field_names_spans_with_the_attribute", endpoint: "field_names", query: q + " `span_attr:part`:=first"})
			if batch > 0 {
				// the spans of the newest batch alone: after the restart layer it is an object of
				// its own, and the objects that carry onlya have no matching span
				c.cases = append(c.cases, fnCase{name: "field_names_spans_of_the_newest_batch_only", endpoint: "field_names", query: q + fmt.Sprintf(" `span_attr:gen`:=g%d", batch)})
			}
		}
		c.afterWrite = func(t *testing.T) { forceMergeHot(t, c.hot, "traces", at) }
		base = []fnCase{
			{name: "field_names", endpoint: "field_names", query: q},
			{name: "field_names_filtered_stream", endpoint: "field_names", query: q + " name:=op2"},
			{name: "field_names_filtered_by_a_sparse_attribute", endpoint: "field_names", query: q + " `span_attr:sa`:=v1"},
			{name: "field_names_substring_filter", endpoint: "field_names", query: q, extra: map[string]string{"filter": "_attr:"}},
			{name: "stream_field_names", endpoint: "stream_field_names", query: q},
			{name: "stream_field_names_filtered", endpoint: "stream_field_names", query: q + " name:=op2"},
			{name: "stream_field_names_substring_filter", endpoint: "stream_field_names", query: q, extra: map[string]string{"filter": "service"}},
		}
		runFnLayers(t, c)
	})
}

// fnOTLPSpan is one span of pushOTLPSpansOneRequest.
type fnOTLPSpan struct {
	traceID, spanID, name, service string
	spanAttrs, resAttrs            map[string]string
	at                             time.Time
}

// pushOTLPSpansOneRequest writes the spans in a single OTLP request (one
// ResourceSpans per span, so each keeps its own resource attributes).
func pushOTLPSpansOneRequest(t *testing.T, base string, hdr http.Header, spans []fnOTLPSpan) {
	t.Helper()
	kv := func(m map[string]string) []map[string]any {
		out := []map[string]any{}
		for k, v := range m {
			out = append(out, map[string]any{"key": k, "value": map[string]any{"stringValue": v}})
		}
		return out
	}
	var resources []map[string]any
	for _, sp := range spans {
		res := append(kv(sp.resAttrs), map[string]any{"key": "service.name", "value": map[string]any{"stringValue": sp.service}})
		resources = append(resources, map[string]any{
			"resource": map[string]any{"attributes": res},
			"scopeSpans": []map[string]any{{
				"scope": map[string]any{"name": "fn-parity"},
				"spans": []map[string]any{{
					"traceId": sp.traceID, "spanId": sp.spanID, "name": sp.name, "kind": 2,
					"startTimeUnixNano": fmt.Sprintf("%d", sp.at.Add(-time.Second).UnixNano()),
					"endTimeUnixNano":   fmt.Sprintf("%d", sp.at.UnixNano()),
					"attributes":        kv(sp.spanAttrs),
				}},
			}},
		})
	}
	body, _ := json.Marshal(map[string]any{"resourceSpans": resources})
	post(t, base+"/insert/opentelemetry/v1/traces", "application/json", body, hdr)
}

// assertFieldNamesExact compares one field_names or stream_field_names answer of
// hot and Lakehouse over the given window the way the lock tests do: the same
// names with the same hits, in the same order (hits descending, then name).
func assertFieldNamesExact(t *testing.T, hot, cold, endpoint, query string, window url.Values) {
	t.Helper()
	params := url.Values{}
	for k, v := range window {
		params[k] = append([]string(nil), v...)
	}
	params.Set("query", query)
	params.Set("disable_latency_offset", "true")
	ref := fetch(t, hot, endpoint, params)
	sut := fetch(t, cold, endpoint, params)
	if ref.StatusCode != 200 || sut.StatusCode != 200 {
		t.Fatalf("%s %s: status hot=%d cold=%d", endpoint, query, ref.StatusCode, sut.StatusCode)
	}
	name := endpoint + " " + query
	requireNumericHits(t, name, ref, sut)
	compareFieldValues(t, name, ref, sut, false)
	if want, got := fieldNamesOrder(t, name+" reference", ref), fieldNamesOrder(t, name+" SUT", sut); !reflect.DeepEqual(want, got) {
		t.Errorf("%s: order differs:\n hot:  %v\n cold: %v", name, want, got)
	}
	reportLockCells(t, 1) // one cell per compared order (floor in lock_cells.txt)
}

// last48h is the window the traces cases of the suite read.
func last48h() url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}
}
