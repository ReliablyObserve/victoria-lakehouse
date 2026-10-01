package parquets3

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const tsCol = "timestamp_unix_nano"

// parseForProjection parses a LogsQL query the way a select request does and
// returns the Parquet columns the cold scan will project for it (nil = all).
func parseForProjection(t testing.TB, reg *schema.Registry, query string) map[string]bool {
	t.Helper()
	q, err := logstorage.ParseQueryAtTimestamp(query, time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC).UnixNano())
	if err != nil {
		t.Fatalf("ParseQuery(%q): %v", query, err)
	}
	return neededColumns(reg, logstorage.GetQueryNeededFields(q))
}

func colList(cols map[string]bool) string {
	if cols == nil {
		return "<all columns>"
	}
	names := make([]string, 0, len(cols))
	for c := range cols {
		names = append(names, c)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// attrColumns is the set a bare, unregistered field name expands to: every
// attribute MAP column and every Tier-2 spare slot of the profile.
func attrColumns(reg *schema.Registry) []string {
	out := append([]string(nil), reg.MapColumns()...)
	return append(out, schema.DedicatedSlotColumns...)
}

// assertProjection checks a query's projected column set exactly. want == nil
// means "read every column".
func assertProjection(t *testing.T, reg *schema.Registry, query string, want []string) {
	t.Helper()
	got := parseForProjection(t, reg, query)
	if want == nil {
		if got != nil {
			t.Errorf("%q projected %s, want all columns", query, colList(got))
		}
		return
	}
	wantSet := map[string]bool{}
	for _, c := range want {
		wantSet[c] = true
	}
	if colList(got) != colList(wantSet) {
		t.Errorf("%q projected\n  got  %s\n  want %s", query, colList(got), colList(wantSet))
	}
}

// TestNeededColumns_Derivation_Traces pins the projected column set for the
// span query shapes the traces binary serves (Jaeger, Tempo/TraceQL, drilldown
// and raw LogsQL), derived from the parsed query. Twin of the logs-module test;
// issue #273: a `_time:` term, stream selector or second term next to a filter
// must not make the projection drop a column the filter or a pipe reads.
func TestNeededColumns_Derivation_Traces(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	attrs := attrColumns(reg)
	with := func(base ...string) []string { return append(append([]string(nil), base...), attrs...) }

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		// --- filter shapes under a stats pipe ---
		{"trace id", `trace_id:="abc" | stats count()`, []string{tsCol, "trace_id"}},
		{"span name exact", `name:="GET /x" | stats count()`, []string{tsCol, "span.name"}},
		{"span name word", `name:GET | stats count()`, []string{tsCol, "span.name"}},
		{"span name prefix", `name:GET* | stats count()`, []string{tsCol, "span.name"}},
		{"span name regex", `name:~"GET .*" | stats count()`, []string{tsCol, "span.name"}},
		{"service attr", `"resource_attr:service.name":="api" | stats count()`, []string{tsCol, "service.name"}},
		{"service by parquet spelling", `service.name:="api" | stats count()`, []string{tsCol, "service.name"}},
		{"span attr promoted", `"span_attr:http.method":="GET" | stats count()`, []string{tsCol, "http.method"}},
		{"span attr dedicated", `"span_attr:db.collection.name":="users" | stats count()`, []string{tsCol, "db.collection.name"}},
		{"span attr map", `"span_attr:custom.key":="v" | stats count()`, []string{tsCol, "span.attributes"}},
		{"resource attr map", `"resource_attr:custom.key":="v" | stats count()`, []string{tsCol, "resource.attributes"}},
		{"unregistered bare attr", `http.route:="/x" | stats count()`, with(tsCol)},
		{"duration range", `duration:>1000000 | stats count()`, []string{tsCol, "duration_ns"}},
		{"status code", `status_code:=2 | stats count()`, []string{tsCol, "status.code"}},
		{"AND", `name:=a AND status_code:=2 | stats count()`, []string{tsCol, "span.name", "status.code"}},
		{"OR", `name:=a OR trace_id:=b | stats count()`, []string{tsCol, "span.name", "trace_id"}},
		{"NOT", `NOT name:=a | stats count()`, []string{tsCol, "span.name"}},

		// --- _time terms anywhere ---
		{"time before field", `_time:30m name:=a | stats count()`, []string{tsCol, "span.name"}},
		{"time after field", `name:=a _time:30m | stats count()`, []string{tsCol, "span.name"}},
		{"time range", `_time:[2026-05-10T00:00:00Z, 2026-05-11T00:00:00Z] trace_id:=a | stats count()`, []string{tsCol, "trace_id"}},
		{"time only", `_time:5m | stats count()`, []string{tsCol}},

		// --- stream selectors ---
		{"stream selector", `{resource_attr:service.name="api"} | stats count()`, []string{tsCol, "_stream"}},
		{"stream selector + field", `{resource_attr:service.name="api"} name:=a | stats count()`, []string{tsCol, "_stream", "span.name"}},
		{"stream explicit + time + field", `_stream:{resource_attr:service.name="api"} _time:5m name:=a | stats by (name) count()`, []string{tsCol, "_stream", "span.name"}},

		// --- stats variants (Tempo metrics, drilldown, Jaeger) ---
		{"stats count unfiltered", `* | stats count()`, []string{tsCol}},
		{"stats by name", `* | stats by (name) count()`, []string{tsCol, "span.name"}},
		{"stats by service", `* | stats by ("resource_attr:service.name") count()`, []string{tsCol, "service.name"}},
		{"stats by + filter", `{resource_attr:service.name="api"} | stats by (name) count()`, []string{tsCol, "_stream", "span.name"}},
		{"histogram duration", `* | stats by (_time:1h) histogram(duration)`, []string{tsCol, "duration_ns"}},
		{"quantile duration", `name:=a | stats by (_time:1m) quantile(0.9, duration)`, []string{tsCol, "span.name", "duration_ns"}},
		{"stats by unregistered", `trace_id:=a | stats by (http.route) count()`, with(tsCol, "trace_id")},
		{"stats count_uniq trace_id", `* | stats count_uniq(trace_id)`, []string{tsCol, "trace_id"}},
		{"service graph", `{trace_service_graph_stream="-"} | fields parent, child, callCount | stats by (parent, child) sum(callCount)`, []string{tsCol, "_stream", "parent", "child", "callCount"}},

		// --- other column-needing pipes ---
		{"uniq", `name:=a | uniq by (trace_id)`, []string{tsCol, "span.name", "trace_id"}},
		{"top", `* | top 5 (name)`, []string{tsCol, "span.name"}},
		{"fields", `trace_id:=a | fields trace_id, name`, []string{tsCol, "trace_id", "span.name"}},
		{"filter pipe", `* | filter name:=a | stats count()`, []string{tsCol, "span.name"}},
		{"extract", `* | extract "id=<id>" from status_message | stats by (id) count()`, []string{tsCol, "status.message"}},
		{"math", `* | math duration / 1000 as us | stats avg(us)`, []string{tsCol, "duration_ns"}},
		{"format", `* | format "<name>-<trace_id>" as c | stats by (c) count()`, []string{tsCol, "span.name", "trace_id"}},

		// --- shapes that need every column ---
		{"sort", `trace_id:=a | sort by (_time)`, nil},
		{"limit", `trace_id:=a | limit 10`, nil},
		{"no pipes", `trace_id:=a`, nil},
		{"star", `*`, nil},
		{"field_names", `* | field_names`, nil},
		{"field_values", `* | field_values name`, nil},
		{"facets", `* | facets`, nil},
		{"wildcard field filter", `*:foo | stats count()`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertProjection(t, reg, tt.query, tt.want)
		})
	}
}

// TestNeededColumns_FieldList_Traces exercises the field-list -> column mapping
// directly for the traces registry.
func TestNeededColumns_FieldList_Traces(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)

	if got := neededColumns(reg, nil); got != nil {
		t.Errorf("nil list: %s, want all columns", colList(got))
	}
	if got := neededColumns(reg, []string{"_time", "*"}); got != nil {
		t.Errorf("wildcard entry: %s, want all columns", colList(got))
	}
	if got := neededColumns(reg, []string{"span_attr:*"}); got != nil {
		t.Errorf("prefix wildcard entry: %s, want all columns", colList(got))
	}
	// VT names and Parquet spellings resolve to the same column.
	for _, name := range []string{"name", "span.name"} {
		if got := neededColumns(reg, []string{name}); colList(got) != colList(map[string]bool{tsCol: true, "span.name": true}) {
			t.Errorf("%s: %s, want timestamp,span.name", name, colList(got))
		}
	}
	// A bare unregistered name can live in any of the three attribute maps or a slot.
	want := map[string]bool{tsCol: true}
	for _, c := range attrColumns(reg) {
		want[c] = true
	}
	if got := neededColumns(reg, []string{"http.route"}); colList(got) != colList(want) {
		t.Errorf("bare unregistered name: %s, want %s", colList(got), colList(want))
	}
	for _, mc := range []string{"resource.attributes", "span.attributes", "scope.attributes"} {
		if !want[mc] {
			t.Errorf("traces map column %s missing from the unregistered-name expansion", mc)
		}
	}
}

func TestNeededFieldsContext_Traces(t *testing.T) {
	ctx := t.Context()
	if got := neededFieldsFrom(ctx); got != nil {
		t.Errorf("bare ctx: %v, want nil", got)
	}
	ctx = withNeededFields(ctx, []string{"name", "_time"})
	if got := neededFieldsFrom(ctx); strings.Join(got, ",") != "name,_time" {
		t.Errorf("ctx carries %v", got)
	}
}

func leafColumnCount() int {
	n := 0
	for _, path := range parquet.SchemaOf(schema.TraceRow{}).Columns() {
		if schema.IsInternalColumn(path[0]) {
			continue
		}
		n++
	}
	return n
}

// TestNeededColumns_KeepsProjectionSavings_Traces proves the AST-derived
// projection keeps the savings column projection exists for.
func TestNeededColumns_KeepsProjectionSavings_Traces(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	total := leafColumnCount()

	tests := []struct {
		query   string
		maxCols int
	}{
		{`* | stats count()`, 1},
		{`* | stats by (name) count()`, 2},
		{`* | stats by (_time:1h) histogram(duration)`, 2},
		{`_time:30m name:=a | stats count()`, 2},
		{`{resource_attr:service.name="api"} _time:5m name:=a | stats by (trace_id) count()`, 4},
		{`name:=a | uniq by (trace_id)`, 3},
	}
	for _, tt := range tests {
		cols := parseForProjection(t, reg, tt.query)
		if cols == nil {
			t.Errorf("%q read every column (%d); projection savings lost", tt.query, total)
			continue
		}
		t.Logf("%-75s -> %d of %d columns: %s", tt.query, len(cols), total, colList(cols))
		if len(cols) > tt.maxCols {
			t.Errorf("%q projects %d columns (%s), want at most %d", tt.query, len(cols), colList(cols), tt.maxCols)
		}
		if len(cols)*3 > total {
			t.Errorf("%q projects %d of %d columns, savings below 2/3", tt.query, len(cols), total)
		}
	}
}

func projectedChunkBytes(f *parquet.File, cols map[string]bool) int64 {
	var total int64
	for _, rg := range f.Metadata().RowGroups {
		for _, cc := range rg.Columns {
			path := cc.MetaData.PathInSchema
			if len(path) == 0 {
				continue
			}
			if cols == nil && schema.IsInternalColumn(path[0]) {
				continue
			}
			if cols == nil || cols[path[0]] {
				total += cc.MetaData.TotalCompressedSize
			}
		}
	}
	return total
}

// TestNeededColumns_BytesRead_Traces reports the Parquet bytes each
// representative cold span query fetches with the AST-derived projection
// against a full read of the same file.
func TestNeededColumns_BytesRead_Traces(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	base := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := make([]schema.TraceRow, 0, 20000)
	for i := 0; i < 20000; i++ {
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			StartTimeUnixNano: base.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			TraceID:           fmt.Sprintf("%032x", i/4*2654435761),
			SpanID:            fmt.Sprintf("%016x", i),
			ParentSpanID:      fmt.Sprintf("%016x", i/2),
			SpanName:          fmt.Sprintf("GET /api/v1/items/%d", i%50),
			ServiceName:       fmt.Sprintf("svc-%d", i%8),
			DurationNs:        int64(1000000 + i%977*1000),
			StatusCode:        int32(i % 3),
			HTTPMethod:        []string{"GET", "POST"}[i%2],
			HTTPUrl:           fmt.Sprintf("https://svc-%d.example.com/api/v1/items/%d?q=%x", i%8, i, i*2654435761),
			DBStatement:       fmt.Sprintf("SELECT * FROM items WHERE id = %d AND tag = '%x'", i, i*40503),
			Stream:            fmt.Sprintf(`{resource_attr:service.name="svc-%d"}`, i%8),
			StreamID:          fmt.Sprintf("%048x", i%8),
			SpanAttributes:    map[string]string{"http.route": "/api/v1/items/{id}"},
		})
	}
	res, err := writeTracesParquet(rows, 5000, 3)
	if err != nil {
		t.Fatal(err)
	}
	f := openParquetBytes(t, res.Data)
	full := projectedChunkBytes(f, nil)

	for _, query := range []string{
		`* | stats count()`,
		`_time:30m name:=a | stats count()`,
		`* | stats by (_time:1h) histogram(duration)`,
		`{resource_attr:service.name="svc-1"} _time:5m name:=a | stats by (trace_id) count()`,
		`name:=a | uniq by (trace_id)`,
		`http.route:="/x" | stats count()`,
	} {
		cols := parseForProjection(t, reg, query)
		got := projectedChunkBytes(f, cols)
		t.Logf("%-80s %2d cols  %8d of %8d bytes (%4.1f%%)", query, len(cols), got, full, 100*float64(got)/float64(full))
		if cols == nil {
			t.Errorf("%q read every column", query)
			continue
		}
		if got >= full {
			t.Errorf("%q fetches %d of %d bytes; projection saved nothing", query, got, full)
		}
		if query == `* | stats count()` && got*20 > full {
			t.Errorf("%q fetches %d of %d bytes; a timestamp-only read must stay under 5%%", query, got, full)
		}
	}
}
