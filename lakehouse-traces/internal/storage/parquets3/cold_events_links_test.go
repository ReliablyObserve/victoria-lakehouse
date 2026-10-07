package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// spanWithExtras builds the TraceRow the flush writes for a span with nEvents
// events, nLinks links and scope attributes, and the VictoriaTraces fields hot
// VictoriaTraces returns for it.
func spanWithExtras(i, nEvents, nLinks int, base time.Time, constKind bool) (schema.TraceRow, map[string]string) {
	want := map[string]string{}
	var c schema.SpanSubFieldCollector
	for e := 0; e < nEvents; e++ {
		s := fmt.Sprintf(":%d", e)
		for k, v := range map[string]string{
			"event:event_time_unix_nano" + s:            fmt.Sprintf("%d", base.UnixNano()+int64(e)),
			"event:event_name" + s:                      []string{"exception", "log", "retry"}[e%3],
			"event:event_dropped_attributes_count" + s:  "0",
			"event:event_attr:exception.type" + s:       "IOError",
			"event:event_attr:exception.stacktrace" + s: "at a.b\nat c.d",
			"event:event_attr:key.with:colons" + s:      "v",
			"event:event_attr:empty.in.source" + s:      "-",
			"event:event_attr:unicode" + s:              "żółć \"quoted\" <tag> & \\ back",
		} {
			c.Add(k, v)
			want[k] = v
		}
	}
	for l := 0; l < nLinks; l++ {
		s := fmt.Sprintf(":%d", l)
		for k, v := range map[string]string{
			"link:link_trace_id" + s:                 fmt.Sprintf("%032x", l+1),
			"link:link_span_id" + s:                  fmt.Sprintf("%016x", l+1),
			"link:link_trace_state" + s:              "vendor=x",
			"link:link_dropped_attributes_count" + s: "0",
			"link:link_flags" + s:                    "257",
			"link:link_attr:link.reason" + s:         "retry",
		} {
			c.Add(k, v)
			want[k] = v
		}
	}
	kind := int32(1 + i%4)
	if constKind {
		kind = 2
	}
	row := schema.TraceRow{
		TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
		StartTimeUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
		TraceID:           fmt.Sprintf("trace-%02d", i/3),
		SpanID:            fmt.Sprintf("%016x", i+1),
		SpanName:          fmt.Sprintf("op-%d", i),
		ServiceName:       "checkout",
		SpanKind:          kind,
		Stream:            `{resource_attr:service.name="checkout"}`,
		StreamID:          fmt.Sprintf("%048x", 7),
	}
	c.Apply(&row)
	if i%2 == 0 {
		row.ScopeAttributes = map[string]string{"scope.key": fmt.Sprintf("sv-%d", i), "other": "o"}
		want["scope_attr:scope.key"] = fmt.Sprintf("sv-%d", i)
		want["scope_attr:other"] = "o"
	}
	return row, want
}

func extrasFixtureStorage(t *testing.T, constKind bool) (run func(string) []map[string]string, want map[string]map[string]string, s *Storage, start, end int64) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s = testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	want = map[string]map[string]string{}
	var rows []schema.TraceRow
	// 0: no extras; 1: events only; 2: links only; 3: both; 4..: 12 events (index order past 10)
	shapes := [][2]int{{0, 0}, {2, 0}, {0, 2}, {3, 2}, {12, 11}, {1, 1}, {0, 0}, {5, 0}}
	for i, sh := range shapes {
		row, w := spanWithExtras(i, sh[0], sh[1], base, constKind)
		rows = append(rows, row)
		want[row.SpanID] = w
	}
	// Several row groups so the scan crosses group boundaries.
	res, err := writeTracesParquet(rows, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/rows.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	start, end = base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	return coldSelectRunner(t, s, start, end), want, s, start, end
}

func extrasFixture(t *testing.T, constKind bool) (run func(string) []map[string]string, want map[string]map[string]string) {
	t.Helper()
	run, want, _, _, _ = extrasFixtureStorage(t, constKind)
	return run, want
}

func isExtraField(name string) bool {
	return strings.HasPrefix(name, "event:") || strings.HasPrefix(name, "link:") || strings.HasPrefix(name, "scope_attr:")
}

// Cold spans carry exactly the event, link and scope fields hot VictoriaTraces
// returns for them: the same names, the same values, nothing extra, and never
// the storage column itself.
func TestCold_EventsLinksScopeAttrs_RowsEqualHot(t *testing.T) {
	for _, constKind := range []bool{false, true} { // true forces the row-oriented path (constant column)
		t.Run(fmt.Sprintf("constantColumn=%v", constKind), func(t *testing.T) {
			run, want := extrasFixture(t, constKind)
			rows := run(`*`)
			if len(rows) != len(want) {
				t.Fatalf("got %d spans, want %d", len(rows), len(want))
			}
			for _, r := range rows {
				w, ok := want[r["span_id"]]
				if !ok {
					t.Fatalf("unexpected span %v", r)
				}
				got := map[string]string{}
				for k, v := range r {
					if isExtraField(k) {
						got[k] = v
					}
					if k == schema.ColSpanEventsJSON || k == schema.ColSpanLinksJSON {
						t.Errorf("span %s leaks storage column %q", r["span_id"], k)
					}
				}
				if len(got) != len(w) {
					t.Errorf("span %s: %d event/link/scope fields, want %d\n got %v\nwant %v", r["span_id"], len(got), len(w), sortedKV(got), sortedKV(w))
				}
				for k, v := range w {
					if got[k] != v {
						t.Errorf("span %s: %s = %q, want %q", r["span_id"], k, got[k], v)
					}
				}
			}
		})
	}
}

func sortedKV(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// Projection: a query naming an event or link field reads it; a query naming
// neither never produces them.
func TestCold_EventsLinks_ProjectedQueries(t *testing.T) {
	run, want := extrasFixture(t, false)

	rows := run(`* | fields span_id, event:event_name:0, link:link_span_id:0`)
	for _, r := range rows {
		w := want[r["span_id"]]
		if r["event:event_name:0"] != w["event:event_name:0"] || r["link:link_span_id:0"] != w["link:link_span_id:0"] {
			t.Errorf("span %s: projected event/link fields %v differ from %v", r["span_id"], r, w)
		}
	}

	for _, q := range []string{`* | fields span_id, name`, `* | stats count() n`, `name:op-1 | fields span_id, trace_id`} {
		for _, r := range run(q) {
			for k := range r {
				if isExtraField(k) {
					t.Errorf("%q must not return %s", q, k)
				}
			}
		}
	}

	// Filters on event / link / scope fields select the same spans hot VT does.
	count := func(filter string) int {
		return statsCountOf(t, run(filter+` | stats count() as n`))
	}
	wantCount := func(field, value string) int {
		n := 0
		for _, w := range want {
			if w[field] == value {
				n++
			}
		}
		return n
	}
	for _, tc := range []struct{ field, value string }{
		{"event:event_name:0", "exception"},
		{"event:event_name:1", "log"},
		{"event:event_name:11", "retry"},
		{"event:event_attr:exception.type:0", "IOError"},
		{"link:link_span_id:0", fmt.Sprintf("%016x", 1)},
		{"link:link_flags:10", "257"},
		{"scope_attr:other", "o"},
		{"scope_attr:scope.key", "sv-4"},
	} {
		got := count(fmt.Sprintf(`%q:=%q`, tc.field, tc.value))
		if exp := wantCount(tc.field, tc.value); got != exp || exp == 0 {
			t.Errorf("filter %s=%q: %d spans, want %d (>0)", tc.field, tc.value, got, exp)
		}
	}
}

func statsCountOf(t *testing.T, rows []map[string]string) int {
	t.Helper()
	if len(rows) != 1 {
		return 0
	}
	var n int
	if _, err := fmt.Sscanf(rows[0]["n"], "%d", &n); err != nil {
		t.Fatalf("count %q: %v", rows[0]["n"], err)
	}
	return n
}

// A file written before the columns existed has no span.events_json: reading it
// is not an error and yields no event fields.
func TestCold_EventsLinks_FileWithoutTheColumns(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	// traceParityRow style: a struct without the new columns.
	type oldRow struct {
		TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
		TraceID           string `parquet:"trace_id"`
		SpanID            string `parquet:"span_id"`
		SpanName          string `parquet:"span.name"`
		ServiceName       string `parquet:"service.name"`
		Stream            string `parquet:"_stream"`
	}
	var rows []oldRow
	for i := 0; i < 5; i++ {
		rows = append(rows, oldRow{base.Add(time.Duration(i) * time.Second).UnixNano(), "t", fmt.Sprintf("%016x", i), "op", "svc", `{resource_attr:service.name="svc"}`})
	}
	data := writeParquetRows(t, rows)
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/old.parquet", base.Format("2006-01-02"), base.Hour()), data, base)
	run := coldSelectRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	for _, q := range []string{`*`, `* | fields span_id, event:event_name:0`, `event:event_name:0:=x | stats count() n`} {
		_ = run(q)
	}
	if got := len(run(`*`)); got != 5 {
		t.Fatalf("old file: %d rows, want 5", got)
	}
	for _, r := range run(`*`) {
		for k := range r {
			if strings.HasPrefix(k, "event:") || strings.HasPrefix(k, "link:") {
				t.Errorf("old file row has %s", k)
			}
		}
	}
}

func writeParquetRows[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// The select-pod buffer bridge turns the insert pods' rows into blocks with
// traceRowsToDataBlock; they must carry the same fields as the cold path.
func TestBridge_EventsLinksScopeAttrs(t *testing.T) {
	s := testStorage()
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	base := time.Now().UTC().Truncate(time.Second)
	var rows []schema.TraceRow
	want := map[string]map[string]string{}
	for i, sh := range [][2]int{{0, 0}, {2, 1}, {12, 11}, {0, 3}} {
		r, w := spanWithExtras(i, sh[0], sh[1], base, false)
		rows = append(rows, r)
		want[r.SpanID] = w
	}
	db := s.traceRowsToDataBlock(tenantScope{all: true}, "test", rows)
	if db == nil || db.RowsCount() != len(rows) {
		t.Fatalf("block = %v", db)
	}
	got := blockRowFields([]*logstorage.DataBlock{db})
	for _, r := range got {
		w := want[r["span_id"]]
		n := 0
		for k, v := range r {
			if !isExtraField(k) {
				continue
			}
			n++
			if w[k] != v {
				t.Errorf("span %s: %s = %q, want %q", r["span_id"], k, v, w[k])
			}
		}
		if n != len(w) {
			t.Errorf("span %s: %d extra fields, want %d", r["span_id"], n, len(w))
		}
	}
}

// The row-at-a-time typed reader emits the decoded fields as well.
func TestTraceRowToFields_EventsAndLinks(t *testing.T) {
	r, w := spanWithExtras(3, 2, 2, time.Now(), false)
	got := map[string]string{}
	for _, f := range traceRowToFields(&r, nil) {
		if strings.HasPrefix(f.name, "event:") || strings.HasPrefix(f.name, "link:") {
			got[f.name], _ = f.value.(string)
		}
	}
	for k, v := range w {
		if strings.HasPrefix(k, "event:") || strings.HasPrefix(k, "link:") {
			if got[k] != v {
				t.Errorf("%s = %q, want %q", k, got[k], v)
			}
			delete(got, k)
		}
	}
	if len(got) != 0 {
		t.Errorf("unexpected fields %v", got)
	}
}

// A query that names no event or link field must not read the JSON columns, and
// one that does reads exactly that column.
func TestNeededColumns_EventsLinks(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	has := func(cols map[string]bool, c string) bool { return cols[c] }
	for _, fields := range [][]string{
		{"trace_id"}, {"name", "resource_attr:service.name", "span_attr:http.method"},
		{"duration", "_time"}, {"scope_attr:x"}, {"events", "linked"},
	} {
		cols := neededColumns(reg, fields)
		if has(cols, schema.ColSpanEventsJSON) || has(cols, schema.ColSpanLinksJSON) {
			t.Errorf("%v must not read the event/link columns: %v", fields, cols)
		}
	}
	cols := neededColumns(reg, []string{"trace_id", "event:event_name:0"})
	if !has(cols, schema.ColSpanEventsJSON) || has(cols, schema.ColSpanLinksJSON) {
		t.Errorf("an event field reads the events column only: %v", cols)
	}
	cols = neededColumns(reg, []string{"link:link_span_id:0"})
	if has(cols, schema.ColSpanEventsJSON) || !has(cols, schema.ColSpanLinksJSON) {
		t.Errorf("a link field reads the links column only: %v", cols)
	}
	// A bare event field name does not fall back to reading every map column.
	if cols := neededColumns(reg, []string{"event:event_name:0"}); has(cols, "resource.attributes") || has(cols, "span.attributes") {
		t.Errorf("event field must not read the attribute maps: %v", cols)
	}
}

// The label index (field_names) learns the event/link field names of a file,
// and never lists the storage column as a field.
func TestLabelIndex_EventsLinksFieldNames(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Second)
	var rows []schema.TraceRow
	for i, sh := range [][2]int{{0, 0}, {2, 1}, {3, 2}} {
		r, _ := spanWithExtras(i, sh[0], sh[1], base, false)
		rows = append(rows, r)
	}
	res, err := writeTracesParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(res.Data), int64(len(res.Data)))
	if err != nil {
		t.Fatal(err)
	}
	s := testStorage()
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	s.updateLabelIndex(f)
	names := map[string]bool{}
	for _, n := range s.labelIndex.GetFieldNames() {
		names[n] = true
	}
	for _, n := range []string{"event:event_name:0", "event:event_name:2", "event:event_attr:exception.type:1", "link:link_span_id:1", "link:link_flags:0", "scope_attr:scope.key"} {
		if !names[n] {
			t.Errorf("field_names lacks %q", n)
		}
	}
	for _, n := range []string{schema.ColSpanEventsJSON, schema.ColSpanLinksJSON, "event:event_name:3", "link:link_span_id:2"} {
		if names[n] {
			t.Errorf("field_names must not list %q", n)
		}
	}
}

// field_values over an event or link field counts, per value, the spans that
// carry it, and the spans that do not under the empty value, as hot
// VictoriaTraces does; a filter narrows the spans counted.
func TestCold_EventsLinks_FieldValues(t *testing.T) {
	_, want, s, start, end := extrasFixtureStorage(t, false)
	expect := func(field string, keep func(map[string]string) bool) map[string]uint64 {
		out := map[string]uint64{}
		for _, w := range want {
			if !keep(w) {
				continue
			}
			// A span without the field is one hit of the empty value, as upstream's
			// uniq counts it.
			out[w[field]]++
		}
		return out
	}
	all := func(map[string]string) bool { return true }
	for _, tc := range []struct {
		name, field, filter string
		keep                func(map[string]string) bool
	}{
		{"event name 0", "event:event_name:0", "*", all},
		{"event name 1", "event:event_name:1", "*", all},
		{"event attr", "event:event_attr:exception.type:0", "*", all},
		{"link span id", "link:link_span_id:0", "*", all},
		// Values of a MAP attribute (span_attr:*, scope_attr:*) are covered by
		// field_values_map_test.go and field_values_map_property_test.go.
		{"event name filtered by event", "event:event_name:0", `"event:event_name:1":="log"`, func(w map[string]string) bool { return w["event:event_name:1"] == "log" }},
		{"event name filtered by a link", "event:event_name:0", `"link:link_span_id:0":*`, func(w map[string]string) bool { _, ok := w["link:link_span_id:0"]; return ok }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exp := expect(tc.field, tc.keep)
			if len(exp) == 0 {
				t.Fatal("fixture has no values for this field")
			}
			got, err := s.GetFieldValues(context.Background(), nil, mustParseQueryWithTime(t, tc.filter, start, end), tc.field, 0)
			if err != nil {
				t.Fatal(err)
			}
			gotM := map[string]uint64{}
			for _, v := range got {
				gotM[v.Value] = v.Hits
			}
			if len(gotM) != len(exp) {
				t.Fatalf("got %v, want %v", gotM, exp)
			}
			for v, n := range exp {
				if gotM[v] != n {
					t.Errorf("value %q: %d hits, want %d (got %v)", v, gotM[v], n, gotM)
				}
			}
		})
	}
}

// #429 interplay: the event, link and scope columns of a cold block take part
// in the upstream column order like every other column. Every read lists the
// columns the same way, in the order orderColumnsLikeUpstream gives them.
func TestCold_EventsLinks_ColumnsAreOrderedLikeUpstream(t *testing.T) {
	for _, constKind := range []bool{false, true} {
		t.Run(fmt.Sprintf("constantColumn=%v", constKind), func(t *testing.T) {
			_, _, s, start, end := extrasFixtureStorage(t, constKind)
			var first []string
			for read := 0; read < 20; read++ {
				q, err := logstorage.ParseQueryAtTimestamp(`*`, end)
				if err != nil {
					t.Fatal(err)
				}
				q.AddTimeFilter(start, end)
				var mu sync.Mutex
				var blocks [][]string
				extras := 0
				wb := func(_ uint, db *logstorage.DataBlock) {
					mu.Lock()
					defer mu.Unlock()
					cols := db.GetColumns(false)
					names := make([]string, len(cols))
					clone := &logstorage.DataBlock{}
					cc := make([]logstorage.BlockColumn, len(cols))
					for i, c := range cols {
						names[i] = c.Name
						cc[i] = logstorage.BlockColumn{Name: c.Name, Values: c.Values}
						if isExtraField(c.Name) {
							extras++
						}
					}
					clone.SetColumns(cc)
					orderColumnsLikeUpstream(clone)
					for i, c := range clone.GetColumns(false) {
						if c.Name != names[i] {
							t.Errorf("block columns are not in upstream order at %d: %v", i, names)
							break
						}
					}
					blocks = append(blocks, names)
				}
				ids := []logstorage.TenantID{{}}
				if err := s.RunQuery(context.Background(), ids, q, wb); err != nil {
					t.Fatal(err)
				}
				if extras == 0 {
					t.Fatal("vacuous: no event, link or scope column in any block")
				}
				var flat []string
				for _, b := range blocks {
					flat = append(flat, strings.Join(b, ","))
				}
				sort.Strings(flat)
				joined := flat
				if read == 0 {
					first = joined
				} else if !reflect.DeepEqual(first, joined) {
					t.Fatalf("read %d lists columns differently:\n first %v\n now   %v", read, first, joined)
				}
			}
		})
	}
}

// #434: event and link strings that are not valid UTF-8 (an event name, an
// event attribute, a link attribute, an attribute name) are read back from
// Parquet byte for byte, as hot VictoriaTraces returns them, not rewritten to
// U+FFFD.
func TestCold_EventsLinks_InvalidUTF8ComesBackExact(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	want := map[string]string{
		"event:event_name:0":                "\xff\xfe",
		"event:event_attr:bin:0":            "a\x80b",
		"event:event_attr:k\xffey:0":        "v",
		"link:link_span_id:0":               "00000000000000aa",
		"link:link_attr:raw:0":              "\xc3(",
		"event:event_time_unix_nano:0":      fmt.Sprint(base.UnixNano()),
		"event:event_attr:valid.text:0":     "żółć <tag> & \"q\"",
		"event:event_attr:looks.like.obj:0": `{"$bytes":"AAAA"}`,
	}
	var c schema.SpanSubFieldCollector
	for k, v := range want {
		c.Add(k, v)
	}
	row := schema.TraceRow{
		TimestampUnixNano: base.UnixNano(), StartTimeUnixNano: base.UnixNano(),
		TraceID: "trace-bad", SpanID: fmt.Sprintf("%016x", 1), SpanName: "op", ServiceName: "checkout",
		Stream: `{resource_attr:service.name="checkout"}`, StreamID: fmt.Sprintf("%048x", 7),
	}
	c.Apply(&row)
	res, err := writeTracesParquet([]schema.TraceRow{row}, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/rows.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	run := coldSelectRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	rows := run(`*`)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	for k, v := range want {
		if rows[0][k] != v {
			t.Errorf("%q = %q, want %q", k, rows[0][k], v)
		}
	}
	for k, v := range rows[0] {
		if isExtraField(k) {
			if _, ok := want[k]; !ok {
				t.Errorf("unexpected field %q = %q", k, v)
			}
		}
	}
}
