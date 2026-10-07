package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// vtFields builds the fields VictoriaTraces writes for n events and m links,
// under the names app/vtinsert/opentelemetry gives them.
func vtFields(nEvents, nLinks int) map[string]string {
	f := map[string]string{}
	for i := 0; i < nEvents; i++ {
		s := fmt.Sprintf(":%d", i)
		f["event:event_time_unix_nano"+s] = fmt.Sprintf("17000000000000000%02d", i)
		f["event:event_name"+s] = fmt.Sprintf("ev%d", i)
		f["event:event_dropped_attributes_count"+s] = "0"
		f["event:event_attr:exception.type"+s] = "IOError"
		f["event:event_attr:a:b:c"+s] = "colons in key"
		f["event:event_attr:empty"+s] = "-"
	}
	for i := 0; i < nLinks; i++ {
		s := fmt.Sprintf(":%d", i)
		f["link:link_trace_id"+s] = fmt.Sprintf("%032x", i+1)
		f["link:link_span_id"+s] = fmt.Sprintf("%016x", i+1)
		f["link:link_trace_state"+s] = "k=v"
		f["link:link_dropped_attributes_count"+s] = "0"
		f["link:link_flags"+s] = "1"
		f["link:link_attr:peer.service"+s] = "svc"
	}
	return f
}

func collect(t *testing.T, fields map[string]string) TraceRow {
	t.Helper()
	// Feed in sorted order, then shuffled order must give the same bytes.
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	sort.Strings(names)
	var c SpanSubFieldCollector
	for _, n := range names {
		if !c.Add(n, fields[n]) {
			t.Fatalf("Add(%q) refused an event/link field", n)
		}
	}
	var row TraceRow
	c.Apply(&row)
	return row
}

func decodeAll(t *testing.T, row TraceRow) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, c := range []struct{ col, js string }{{ColSpanEventsJSON, row.EventsJSON}, {ColSpanLinksJSON, row.LinksJSON}} {
		if err := ForEachSpanSubField(c.col, c.js, func(n, v string) {
			if _, dup := got[n]; dup {
				t.Errorf("field %q emitted twice", n)
			}
			got[n] = v
		}); err != nil {
			t.Fatalf("decode %s: %v", c.col, err)
		}
	}
	return got
}

func TestSpanSubFields_RoundTripsVTFieldNames(t *testing.T) {
	for _, tc := range []struct{ events, links int }{{0, 0}, {1, 0}, {0, 1}, {2, 3}, {12, 11}, {101, 0}} {
		t.Run(fmt.Sprintf("e%d_l%d", tc.events, tc.links), func(t *testing.T) {
			want := vtFields(tc.events, tc.links)
			row := collect(t, want)
			if got := decodeAll(t, row); !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip differs:\n got %v\nwant %v", got, want)
			}
			if (tc.events == 0) != (row.EventsJSON == "") || (tc.links == 0) != (row.LinksJSON == "") {
				t.Fatalf("empty columns must be \"\" (NULL on disk): events=%q links=%q", row.EventsJSON, row.LinksJSON)
			}
		})
	}
}

// With more than ten events a block lists the columns in name order
// (event_name:0, :1, :10, :11, :2 ...). The JSON array must still be in event
// order, or every event after the tenth would land under the wrong index.
func TestSpanSubFields_ElementsAreInIndexOrder(t *testing.T) {
	row := collect(t, vtFields(12, 0))
	if !strings.Contains(row.EventsJSON, `"event_name":"ev0"`) {
		t.Fatalf("unexpected json %s", row.EventsJSON)
	}
	i2 := strings.Index(row.EventsJSON, `"event_name":"ev2"`)
	i10 := strings.Index(row.EventsJSON, `"event_name":"ev10"`)
	if i2 < 0 || i10 < 0 || i2 > i10 {
		t.Fatalf("event 2 must come before event 10 in %s", row.EventsJSON)
	}
	if strings.Contains(row.EventsJSON, idxKey) {
		t.Fatalf("a positional group must not carry %s: %s", idxKey, row.EventsJSON)
	}
}

func TestSpanSubFields_SameBytesWhateverTheFieldOrder(t *testing.T) {
	fields := vtFields(5, 2)
	names := make([]string, 0, len(fields))
	for n := range fields {
		names = append(names, n)
	}
	sort.Strings(names)
	var a SpanSubFieldCollector
	for _, n := range names {
		a.Add(n, fields[n])
	}
	var b SpanSubFieldCollector
	for i := len(names) - 1; i >= 0; i-- {
		b.Add(names[i], fields[names[i]])
	}
	var ra, rb TraceRow
	a.Apply(&ra)
	b.Apply(&rb)
	if ra.EventsJSON != rb.EventsJSON || ra.LinksJSON != rb.LinksJSON {
		t.Fatal("the JSON depends on the order the fields arrived in")
	}
}

func TestSpanSubFields_NonPositionalSuffixesSurvive(t *testing.T) {
	want := map[string]string{
		"event:event_name:3":      "gap",      // gap: first group but index 3
		"event:event_name:7":      "gap2",     // second group, index 7
		"event:event_name:x":      "nonnum",   // not a number
		"event:event_name:007":    "leading0", // not canonical: "007" != 7
		"event:event_name":        "nosuffix", // no suffix at all
		"event:weird::name:0":     "colon",    // empty sub-name segment
		"link:link_span_id:99999": "big",
		"event:$idx:0":            "reserved-looking name",
		"event:$$x:0":             "double dollar",
	}
	row := collect(t, want)
	if got := decodeAll(t, row); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v\njson %s", got, want, row.EventsJSON)
	}
}

func TestSpanSubFields_AddIgnoresOtherFields(t *testing.T) {
	var c SpanSubFieldCollector
	for _, n := range []string{"span_attr:event:x", "trace_id", "events", "linked", "scope_attr:k", ""} {
		if c.Add(n, "v") {
			t.Errorf("Add(%q) must not take a non event/link field", n)
		}
	}
}

func TestSpanSubFields_CorruptJSONEmitsNothing(t *testing.T) {
	for _, js := range []string{"{", "[1,2]", `{"a":"b"}`, `[{"a":1}]`, "null-ish"} {
		n := 0
		if err := ForEachSpanSubField(ColSpanEventsJSON, js, func(string, string) { n++ }); err == nil {
			t.Errorf("%q must be an error", js)
		}
		if n != 0 {
			t.Errorf("%q emitted %d fields", js, n)
		}
	}
	if err := ForEachSpanSubField(ColSpanLinksJSON, "", func(string, string) { t.Fatal("emitted") }); err != nil {
		t.Fatal(err)
	}
}

func TestCompositeColumnForField(t *testing.T) {
	for field, want := range map[string]string{
		"event:event_name:0":   ColSpanEventsJSON,
		"link:link_span_id:2":  ColSpanLinksJSON,
		"event:":               ColSpanEventsJSON,
		"span_attr:event:x":    "",
		"events":               "",
		"link":                 "",
		"scope_attr:link:x":    "",
		"resource_attr:event:": "",
	} {
		got, ok := CompositeColumnForField(field)
		if got != want || ok != (want != "") {
			t.Errorf("CompositeColumnForField(%q) = %q,%v want %q", field, got, ok, want)
		}
	}
	if !IsCompositeColumn(ColSpanEventsJSON) || !IsCompositeColumn(ColSpanLinksJSON) || IsCompositeColumn("span.attributes") {
		t.Error("IsCompositeColumn is wrong")
	}
	if ClassifyColumn(ColSpanEventsJSON) != ColumnComposite {
		t.Error("the composite columns must classify as composite, never as a user-visible field")
	}
}

func TestEstimateRawBytesTraces_CountsEventsAndLinks(t *testing.T) {
	base := []TraceRow{{TraceID: "t"}}
	with := []TraceRow{{TraceID: "t", EventsJSON: strings.Repeat("e", 100), LinksJSON: strings.Repeat("l", 50)}}
	if d := EstimateRawBytesTraces(with) - EstimateRawBytesTraces(base); d != 150 {
		t.Fatalf("events+links must add their bytes to the raw-size estimate, got +%d", d)
	}
}

// FuzzSpanSubFields: any set of field names and values under the two prefixes,
// invalid UTF-8 included, survives collect -> JSON -> decode unchanged, except
// that empty values are absent (VictoriaLogs treats an empty value as no field).
func FuzzSpanSubFields(f *testing.F) {
	f.Add("event:event_name:0", "x", "link:link_span_id:1", "y")
	f.Add("event:event_attr:a:b:3", "-", "event:event_attr:a:b:3x", "z")
	f.Add("event:", "v", "link:", "w")
	f.Add("event:a", "1", "event:a:", "2")
	f.Add("event:$idx:0", "1", "event:$$x:0", "2")
	f.Add("event:event_name:0", "\xff\xfe", "event:event_attr:\xff:0", "\x80")
	f.Add("link:link_attr:$b64:AA:0", "v", "link:x:\xffsuffix", "\xc3")
	f.Add("event:$\xff:0", "{\"$bytes\":\"AA==\"}", "event:$b64:zz:0", "x")
	f.Fuzz(func(t *testing.T, n1, v1, n2, v2 string) {
		in := map[string]string{}
		var c SpanSubFieldCollector
		for _, kv := range [][2]string{{n1, v1}, {n2, v2}} {
			if c.Add(kv[0], kv[1]) {
				in[kv[0]] = kv[1]
			}
		}
		var row TraceRow
		c.Apply(&row)
		got := map[string]string{}
		for _, cc := range []struct{ col, js string }{{ColSpanEventsJSON, row.EventsJSON}, {ColSpanLinksJSON, row.LinksJSON}} {
			if err := ForEachSpanSubField(cc.col, cc.js, func(n, v string) { got[n] = v }); err != nil {
				t.Fatalf("decode failed: %v (%s)", err, cc.js)
			}
		}
		if !reflect.DeepEqual(got, in) {
			t.Fatalf("round trip lost data:\n got %q\nwant %q", got, in)
		}
		// The column is valid JSON for external readers whatever the bytes were.
		for _, js := range []string{row.EventsJSON, row.LinksJSON} {
			if js != "" && !json.Valid([]byte(js)) {
				t.Fatalf("stored column is not valid JSON: %q", js)
			}
		}
	})
}

// The logs signal is unaffected: no log row has an event, link or composite
// column, so a logs Parquet file keeps exactly the columns it had before the
// span events and links columns existed (the logs readback golden pins the
// bytes-level shape of the file).
func TestLogRowHasNoSpanEventLinkColumns(t *testing.T) {
	for _, col := range rowParquetColumnsOf(t, reflect.TypeOf(LogRow{})) {
		if IsCompositeColumn(col) || strings.HasPrefix(col, "span.") {
			t.Errorf("LogRow has the span column %q", col)
		}
	}
	got := rowParquetColumnsOf(t, reflect.TypeOf(TraceRow{}))
	for _, want := range []string{ColSpanEventsJSON, ColSpanLinksJSON} {
		found := false
		for _, c := range got {
			found = found || c == want
		}
		if !found {
			t.Errorf("TraceRow lacks column %q", want)
		}
	}
}

func rowParquetColumnsOf(t *testing.T, typ reflect.Type) []string {
	t.Helper()
	var cols []string
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("parquet")
		if tag == "" || tag == "-" {
			continue
		}
		cols = append(cols, strings.Split(tag, ",")[0])
	}
	return cols
}

// randomSpanSubFields builds up to n random event and link sub-fields the way
// VictoriaTraces names them: indexed groups (gaps allowed), attribute keys with
// colons, dots, a leading "$", unicode, empty-after-trim names, and values from
// empty through 128 KiB.
func randomSpanSubFields(r *rand.Rand, n int) map[string]string {
	keys := []string{"a", "exception.type", "k:with:colons", "$idx", "$x", "žółć", "☃", "dropped", "a.b.c", "", "bad\xff\xfekey", "$b64:AAAA", "$bytes", "\xc3"}
	vals := []func() string{
		func() string { return "" },
		func() string { return "-" },
		func() string { return "v" + strconv.Itoa(r.Intn(1000)) },
		func() string { return "line\nbreak \"q\" \\ \t   \x00ctl" },
		func() string { return strings.Repeat("é", r.Intn(70000)) },
		func() string { return strconv.FormatInt(r.Int63()-r.Int63(), 10) },
		func() string { return "raw\xff\xfe\x00bytes\x80" },
		func() string { return `{"$bytes":"AAAA"}` },
	}
	fixed := []string{"event_name", "event_time_unix_nano", "event_dropped_attributes_count",
		"link_trace_id", "link_span_id", "link_trace_state", "link_flags", "link_dropped_attributes_count"}
	out := map[string]string{}
	for i := 0; i < n; i++ {
		prefix, kind := EventFieldPrefix, "event_attr"
		if r.Intn(2) == 0 {
			prefix, kind = LinkFieldPrefix, "link_attr"
		}
		idx := strconv.Itoa(r.Intn(60))
		if r.Intn(20) == 0 {
			idx = []string{"x", "", "-1", "007"}[r.Intn(4)]
		}
		var name string
		if r.Intn(3) == 0 {
			name = prefix + fixed[r.Intn(len(fixed))]
		} else {
			name = prefix + kind + ":" + keys[r.Intn(len(keys))]
		}
		if idx != "" {
			name += ":" + idx
		}
		out[name] = vals[r.Intn(len(vals))]()
	}
	return out
}

// Property: for any random set of event and link sub-fields, collecting them
// into a row, writing the row to Parquet and reading it back, and decoding the
// two columns, returns exactly the non-empty input fields; the bytes do not
// depend on the order the fields arrive in; and a span with no event or link
// field has both columns NULL.
func TestSpanSubFields_PropertyRoundTripThroughParquet(t *testing.T) {
	r := rand.New(rand.NewSource(409))
	for iter := 0; iter < 200; iter++ {
		in := randomSpanSubFields(r, r.Intn(300))
		names := make([]string, 0, len(in))
		for k := range in {
			names = append(names, k)
		}
		sort.Strings(names)
		collect := func(order []string) TraceRow {
			var c SpanSubFieldCollector
			for _, k := range order {
				c.Add(k, in[k])
			}
			var row TraceRow
			c.Apply(&row)
			return row
		}
		row := collect(names)
		shuffled := append([]string(nil), names...)
		r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		if other := collect(shuffled); other.EventsJSON != row.EventsJSON || other.LinksJSON != row.LinksJSON {
			t.Fatalf("iter %d: the stored bytes depend on field order", iter)
		}

		var buf bytes.Buffer
		w := parquet.NewGenericWriter[TraceRow](&buf)
		if _, err := w.Write([]TraceRow{row}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		rd := parquet.NewGenericReader[TraceRow](bytes.NewReader(buf.Bytes()))
		back := make([]TraceRow, 1)
		if _, err := rd.Read(back); err != nil && !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		_ = rd.Close()

		if back[0].EventsJSON != row.EventsJSON || back[0].LinksJSON != row.LinksJSON {
			t.Fatalf("iter %d: Parquet changed the column bytes (events %d -> %d, links %d -> %d)", iter,
				len(row.EventsJSON), len(back[0].EventsJSON), len(row.LinksJSON), len(back[0].LinksJSON))
		}
		got := map[string]string{}
		for _, cc := range []struct{ col, js string }{{ColSpanEventsJSON, back[0].EventsJSON}, {ColSpanLinksJSON, back[0].LinksJSON}} {
			if err := ForEachSpanSubField(cc.col, cc.js, func(n, v string) { got[n] = v }); err != nil {
				t.Fatalf("iter %d: decode %s: %v", iter, cc.col, err)
			}
		}
		// VictoriaLogs treats an empty value as no field: compare without them.
		want := map[string]string{}
		for k, v := range in {
			if v != "" {
				want[k] = v
			}
		}
		for k, v := range got {
			if v == "" {
				delete(got, k)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iter %d: round trip through Parquet changed the fields (%d in, %d out)", iter, len(want), len(got))
		}
		if len(in) == 0 && (back[0].EventsJSON != "" || back[0].LinksJSON != "") {
			t.Fatalf("iter %d: a span without extras has columns set", iter)
		}
	}
}

// #434: bytes that are not valid UTF-8 in an event name, an event attribute
// value, a link attribute value and an attribute name come back exactly; the
// column stays valid JSON; valid text is stored as it always was.
func TestSpanSubFields_InvalidUTF8IsReversible(t *testing.T) {
	in := map[string]string{
		"event:event_name:0":                "\xff\xfe",
		"event:event_attr:bin:0":            "a\x80b",
		"event:event_attr:k\xffey:0":        "v",
		"link:link_attr:bin:0":              "\xc3(",
		"link:link_span_id:0":               "00000000000000aa",
		"event:event_attr:lookalike:1":      `{"$bytes":"AAAA"}`,
		"event:event_attr:$bytes:1":         "plain",
		"event:event_attr:$b64:AAAA:1":      "legit name that looks like the marker",
		"event:event_attr:html:1":           "<a href=\"x\">&</a>",
		"event:event_attr:name_\xff_suffix": "no group suffix at all",
	}
	var c SpanSubFieldCollector
	for k, v := range in {
		if !c.Add(k, v) {
			t.Fatalf("%q not taken", k)
		}
	}
	var row TraceRow
	c.Apply(&row)
	for _, js := range []string{row.EventsJSON, row.LinksJSON} {
		if !json.Valid([]byte(js)) {
			t.Fatalf("column is not valid JSON: %q", js)
		}
		if strings.Contains(js, "�") {
			t.Fatalf("column holds U+FFFD: %q", js)
		}
	}
	got := map[string]string{}
	for _, cc := range []struct{ col, js string }{{ColSpanEventsJSON, row.EventsJSON}, {ColSpanLinksJSON, row.LinksJSON}} {
		if err := ForEachSpanSubField(cc.col, cc.js, func(n, v string) { got[n] = v }); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("not reversible:\n got %q\nwant %q", got, in)
	}
	// Valid text is stored byte for byte as before: a plain JSON string, HTML
	// characters unescaped.
	if !strings.Contains(row.EventsJSON, `"event_attr:html":"<a href=\"x\">&</a>"`) {
		t.Errorf("valid text changed form: %s", row.EventsJSON)
	}
	if !strings.Contains(row.EventsJSON, `"event_attr:bin":{"$bytes":"YYBi"}`) {
		t.Errorf("invalid value is not stored as a $bytes object: %s", row.EventsJSON)
	}
}

// A $bytes object that is not the one form the encoder writes is an error, not
// silently another value.
func TestForEachSpanSubField_RejectsMalformedBytesObject(t *testing.T) {
	for _, js := range []string{`[{"a":{"$bytes":"!!!"}}]`, `[{"a":{"other":"AA=="}}]`, `[{"a":{"$bytes":"AA==","x":"y"}}]`} {
		if err := ForEachSpanSubField(ColSpanEventsJSON, js, func(string, string) {}); err == nil {
			t.Errorf("%s accepted", js)
		}
	}
}
