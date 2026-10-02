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

// TestNeededColumns_Derivation pins the projected column set for every filter
// shape and every column-needing pipe. The set is derived from the parsed query
// (upstream's updateNeededFields), so each case also doubles as a check that a
// term's position, quoting, operator or default-field serialisation cannot make
// the projection drop a column the filter or a pipe reads. Issue #273: a
// `_time:` term, stream selector or second field term next to a default-field
// `_msg:="x"` filter used to drop `body`, and the filtered stats counted 0.
func TestNeededColumns_Derivation(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	attrs := attrColumns(reg)
	with := func(base ...string) []string { return append(append([]string(nil), base...), attrs...) }

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		// --- filter shapes under a stats pipe ---
		{"word", `error | stats count()`, []string{tsCol, "body"}},
		{"phrase", `"connection refused" | stats count()`, []string{tsCol, "body"}},
		{"default field exact :=", `_msg:="exact line" | stats count()`, []string{tsCol, "body"}},
		{"default field exact bare =", `="exact line" | stats count()`, []string{tsCol, "body"}},
		{"default field :=M", `_msg:=M | stats count()`, []string{tsCol, "body"}},
		{"default field prefix", `err* | stats count()`, []string{tsCol, "body"}},
		{"default field prefix explicit", `_msg:err* | stats count()`, []string{tsCol, "body"}},
		{"default field regex", `~"err.*refused" | stats count()`, []string{tsCol, "body"}},
		{"default field regex explicit", `_msg:~"err.*refused" | stats count()`, []string{tsCol, "body"}},
		{"field word", `level:error | stats count()`, []string{tsCol, "severity_text"}},
		{"field :=", `level:=error | stats count()`, []string{tsCol, "severity_text"}},
		{"field prefix", `service.name:api* | stats count()`, []string{tsCol, "service.name"}},
		{"field regex", `service.name:~"api.*" | stats count()`, []string{tsCol, "service.name"}},
		{"field in()", `level:in(error, warn) | stats count()`, []string{tsCol, "severity_text"}},
		{"field range", `severity_number:>=17 | stats count()`, []string{tsCol, "severity_number"}},
		{"AND", `error AND level:=warn | stats count()`, []string{tsCol, "body", "severity_text"}},
		{"implicit AND", `error level:=warn | stats count()`, []string{tsCol, "body", "severity_text"}},
		{"OR", `error OR service.name:=api | stats count()`, []string{tsCol, "body", "service.name"}},
		{"NOT word", `!error | stats count()`, []string{tsCol, "body"}},
		{"NOT field", `NOT level:=debug | stats count()`, []string{tsCol, "severity_text"}},
		{"nested", `(error OR level:=warn) AND NOT host.name:=h1 | stats count()`, []string{tsCol, "body", "severity_text", "host.name"}},
		{"promoted by parquet spelling", `severity_text:=ERROR | stats count()`, []string{tsCol, "severity_text"}},
		{"unregistered field", `repro_layer:=cold | stats count()`, with(tsCol)},

		// --- _time terms anywhere ---
		{"time before default exact", `_time:30m _msg:=M | stats count()`, []string{tsCol, "body"}},
		{"time after default exact", `_msg:=M _time:30m | stats count()`, []string{tsCol, "body"}},
		{"time range before word", `_time:[2026-05-10T00:00:00Z, 2026-05-11T00:00:00Z] error | stats count()`, []string{tsCol, "body"}},
		{"time between terms", `error _time:5m level:=warn | stats count()`, []string{tsCol, "body", "severity_text"}},
		{"time only", `_time:5m | stats count()`, []string{tsCol}},
		{"time with offset", `_time:1h offset 30m MARKER | stats count()`, []string{tsCol, "body"}},

		// --- stream selectors ---
		{"stream selector + word", `{app="a"} error | stats count()`, []string{tsCol, "_stream", "body"}},
		{"stream selector alone", `{app="a"} | stats count()`, []string{tsCol, "_stream"}},
		{"stream explicit prefix", `_stream:{app="a"} _time:5m _msg:=M | stats count()`, []string{tsCol, "_stream", "body"}},
		{"stream id", `_stream_id:0000000000000000000000000000000000000000000000aa | stats count()`, []string{tsCol, "_stream_id"}},

		// --- stats variants ---
		{"stats count unfiltered", `* | stats count()`, []string{tsCol}},
		{"stats count named", `* | stats count() total`, []string{tsCol}},
		{"stats by one field", `* | stats by (service.name) count()`, []string{tsCol, "service.name"}},
		{"stats by + filter", `error | stats by (level) count()`, []string{tsCol, "body", "severity_text"}},
		{"stats by two fields", `* | stats by (level, service.name) count()`, []string{tsCol, "severity_text", "service.name"}},
		{"stats by time bucket", `* | stats by (_time:1h) count()`, []string{tsCol}},
		{"stats by time bucket + field", `error | stats by (_time:1h, level) count()`, []string{tsCol, "body", "severity_text"}},
		{"stats by unregistered field", `MARKER | stats by (repro_layer) count()`, with(tsCol, "body")},
		{"stats sum arg", `* | stats sum(severity_number)`, []string{tsCol, "severity_number"}},
		{"stats sum unregistered arg", `error | stats sum(bytes)`, with(tsCol, "body")},
		{"stats count_uniq arg", `* | stats count_uniq(trace_id)`, []string{tsCol, "trace_id"}},
		{"stats quantile arg", `error | stats quantile(0.9, severity_number)`, []string{tsCol, "body", "severity_number"}},
		{"stats count if", `* | stats count() if (level:=error) errs`, []string{tsCol, "severity_text"}},
		{"stats then sort", `error | stats count() | sort by (count)`, []string{tsCol, "body"}},

		// --- other column-needing pipes ---
		{"uniq", `error | uniq by (level)`, []string{tsCol, "body", "severity_text"}},
		{"top", `error | top 5 (level)`, []string{tsCol, "body", "severity_text"}},
		{"fields", `error | fields service.name, _msg`, []string{tsCol, "body", "service.name"}},
		{"fields default only", `* | fields _msg`, []string{tsCol, "body"}},
		{"filter pipe", `* | filter level:=error | stats count()`, []string{tsCol, "severity_text"}},
		{"filter pipe default field", `* | filter _msg:=M | stats count()`, []string{tsCol, "body"}},
		{"extract source", `* | extract "ip=<ip>" from _msg | stats by (ip) count()`, []string{tsCol, "body"}},
		{"extract from field", `* | extract "id=<id>" from host.name | stats by (id) count()`, []string{tsCol, "host.name"}},
		// k may come from the unpacked message or already be on the row, so it is
		// needed from the input too; an unregistered name reads the attribute maps.
		{"unpack_json", `* | unpack_json from _msg | stats by (k) count()`, with(tsCol, "body")},
		{"unpack_json promoted key", `* | unpack_json from _msg | stats by (level) count()`, []string{tsCol, "body", "severity_text"}},
		// Nothing downstream reads an unpacked field, so nothing is unpacked.
		{"unpack_logfmt unused", `* | unpack_logfmt from _msg | stats count()`, []string{tsCol}},
		{"math", `* | math severity_number*2 as x | stats sum(x)`, []string{tsCol, "severity_number"}},
		{"format", `* | format "<level>-<service.name>" as c | stats by (c) count()`, []string{tsCol, "severity_text", "service.name"}},
		{"rename then stats", `* | rename level as sev | stats by (sev) count()`, []string{tsCol, "severity_text"}},
		{"copy then stats", `* | copy level as sev | stats by (sev) count()`, []string{tsCol, "severity_text"}},

		// --- shapes that need every column ---
		{"sort", `error | sort by (level)`, nil},
		{"limit", `error | limit 10`, nil},
		{"no pipes word", `error`, nil},
		{"no pipes star", `*`, nil},
		{"empty", ``, nil},
		{"field_names", `error | field_names`, nil},
		{"field_values", `error | field_values level`, nil},
		{"facets", `error | facets`, nil},
		{"wildcard field filter", `*:foo | stats count()`, nil},
		{"fields wildcard", `* | fields k8s.*`, nil},
		// A deleted field is gone before the stats, so it is not needed from storage.
		{"delete then group by it", `* | delete level | stats by (level) count()`, []string{tsCol}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.query
			if q == "" {
				q = "*"
			}
			assertProjection(t, reg, q, tt.want)
		})
	}
}

// TestNeededColumns_FieldList exercises the field-list -> column mapping
// directly, including the cases no parsed query produces.
func TestNeededColumns_FieldList(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	attrs := attrColumns(reg)

	if got := neededColumns(reg, nil); got != nil {
		t.Errorf("nil list: %s, want all columns", colList(got))
	}
	if got := neededColumns(reg, []string{}); got != nil {
		t.Errorf("empty list: %s, want all columns", colList(got))
	}
	if got := neededColumns(reg, []string{"_time", "*"}); got != nil {
		t.Errorf("wildcard entry: %s, want all columns", colList(got))
	}
	if got := neededColumns(reg, []string{"_time", "k8s.*"}); got != nil {
		t.Errorf("prefix wildcard entry: %s, want all columns", colList(got))
	}
	// The empty field name is LogsQL's default field.
	if got := neededColumns(reg, []string{""}); colList(got) != colList(map[string]bool{tsCol: true, "body": true}) {
		t.Errorf("empty field name: %s, want timestamp,body", colList(got))
	}
	// A prefixed attribute resolves to its MAP column alone.
	got := neededColumns(reg, []string{"resource_attr:foo"})
	if colList(got) != colList(map[string]bool{tsCol: true, "resource.attributes": true}) {
		t.Errorf("resource_attr:foo: %s, want timestamp,resource.attributes", colList(got))
	}
	// A bare unregistered name can live in any MAP column or Tier-2 slot.
	want := map[string]bool{tsCol: true}
	for _, c := range attrs {
		want[c] = true
	}
	if got := neededColumns(reg, []string{"repro_layer"}); colList(got) != colList(want) {
		t.Errorf("bare unregistered name: %s, want %s", colList(got), colList(want))
	}
	// The timestamp column is always present.
	if got := neededColumns(reg, []string{"level"}); !got[tsCol] || !got["severity_text"] || len(got) != 2 {
		t.Errorf("level: %s, want timestamp,severity_text", colList(got))
	}
}

// TestNeededFieldsContext covers the ctx carrier queryFile reads: a context
// without the value reads every column.
func TestNeededFieldsContext(t *testing.T) {
	ctx := t.Context()
	if got := neededFieldsFrom(ctx); got != nil {
		t.Errorf("bare ctx: %v, want nil", got)
	}
	ctx = withNeededFields(ctx, []string{"_msg", "_time"})
	if got := neededFieldsFrom(ctx); strings.Join(got, ",") != "_msg,_time" {
		t.Errorf("ctx carries %v", got)
	}
}

// leafColumnCount returns how many Parquet leaf columns an unprojected cold
// scan of a LogRow file reads.
func leafColumnCount() int {
	n := 0
	for _, path := range parquet.SchemaOf(schema.LogRow{}).Columns() {
		if schema.IsInternalColumn(path[0]) {
			continue
		}
		n++
	}
	return n
}

// TestNeededColumns_KeepsProjectionSavings proves the AST-derived projection
// keeps the savings that make column projection worth having: the common cold
// query shapes read a handful of columns, not the whole schema, and the
// unfiltered and single-field queries read no more than they did before the
// derivation moved off the query text.
func TestNeededColumns_KeepsProjectionSavings(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	total := leafColumnCount()

	tests := []struct {
		query   string
		maxCols int
	}{
		{`* | stats count()`, 1},                   // unfiltered: timestamp only (unchanged)
		{`* | stats by (service.name) count()`, 2}, // single field (unchanged)
		{`* | stats by (level) count()`, 2},        // single field (unchanged)
		{`MARKER | stats count()`, 2},              // was 1 column and 0 rows; now correct
		{`_time:30m _msg:=M | stats count()`, 2},   // was 1 column and 0 rows; now correct
		{`error level:=warn | stats by (host.name) count()`, 4},
		{`{app="a"} error | stats count()`, 3},
		{`error | uniq by (level)`, 3},
		{`error | fields service.name, _msg`, 3},
	}
	for _, tt := range tests {
		cols := parseForProjection(t, reg, tt.query)
		if cols == nil {
			t.Errorf("%q read every column (%d); projection savings lost", tt.query, total)
			continue
		}
		t.Logf("%-55s -> %d of %d columns: %s", tt.query, len(cols), total, colList(cols))
		if len(cols) > tt.maxCols {
			t.Errorf("%q projects %d columns (%s), want at most %d", tt.query, len(cols), colList(cols), tt.maxCols)
		}
		if len(cols)*3 > total {
			t.Errorf("%q projects %d of %d columns, savings below 2/3", tt.query, len(cols), total)
		}
	}
}

// projectedChunkBytes sums the compressed size of the column chunks the cols
// projection would fetch from every row group of f — the bytes a planned range
// read pulls from S3 for the projected scan.
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

// TestNeededColumns_BytesRead reports the Parquet bytes each representative cold
// query fetches with the AST-derived projection against a full read of the same
// file, and asserts the projected read is smaller. The body column dominates the
// fixture (long messages), so a body-reading query saves less than a
// timestamp- or label-only one; the logged numbers are the measurement.
func TestNeededColumns_BytesRead(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	base := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := make([]schema.LogRow, 0, 20000)
	for i := 0; i < 20000; i++ {
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("request %d handled path=/api/v1/items/%d status=%d trace=%x", i, i*7, 200+i%5, i*2654435761),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			SeverityNumber:    int32(9 + i%4),
			ServiceName:       fmt.Sprintf("svc-%d", i%8),
			TraceID:           fmt.Sprintf("%032x", i*2654435761),
			SpanID:            fmt.Sprintf("%016x", i),
			HostName:          fmt.Sprintf("host-%d", i%16),
			Stream:            fmt.Sprintf(`{service.name="svc-%d"}`, i%8),
			StreamID:          fmt.Sprintf("%048x", i%8),
			LogAttributes:     map[string]string{"repro_layer": "cold", "region": "eu"},
		})
	}
	res, err := writeLogsParquet(rows, 5000, 3)
	if err != nil {
		t.Fatal(err)
	}
	f := openParquetBytes(t, res.Data)
	full := projectedChunkBytes(f, nil)

	for _, query := range []string{
		`* | stats count()`,
		`MARKER | stats count()`,
		`_time:30m _msg:=M | stats count()`,
		`error | stats by (level) count()`,
		`{service.name="svc-1"} error | stats count()`,
		`error | uniq by (level)`,
		`MARKER | stats by (repro_layer) count()`,
	} {
		cols := parseForProjection(t, reg, query)
		got := projectedChunkBytes(f, cols)
		t.Logf("%-50s %2d cols  %8d of %8d bytes (%4.1f%%)", query, len(cols), got, full, 100*float64(got)/float64(full))
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
