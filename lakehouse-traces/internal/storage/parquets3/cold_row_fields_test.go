package parquets3

import (
	"bytes"
	"context"
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

// Twin of internal/storage/parquets3/cold_row_fields_test.go for spans. The cold
// read path must hand back exactly the fields hot VictoriaTraces returns for the
// same span: VT-prefixed resource/span attributes, no "<null>" placeholders, no
// tenant bookkeeping columns, no raw Tier-2 slots, and no service-graph edge
// columns on rows that are not service-graph edges.

// blockRowFields renders DataBlocks the way the JSON response writer does: one
// map per row, empty values dropped — VictoriaLogs skips empty fields because
// "they equal to non-existing fields".
func blockRowFields(blocks []*logstorage.DataBlock) []map[string]string {
	var rows []map[string]string
	for _, b := range blocks {
		cols := b.GetColumns(false)
		n := b.RowsCount()
		for i := 0; i < n; i++ {
			row := make(map[string]string, len(cols))
			for _, c := range cols {
				if i >= len(c.Values) || c.Values[i] == "" {
					continue
				}
				row[c.Name] = c.Values[i]
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func fieldNamesOf(row map[string]string) []string {
	names := make([]string, 0, len(row))
	for k := range row {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// serviceGraphColumns are the edge fields VictoriaTraces' servicegraph task
// emits. They belong on service-graph rows only; a span row that carries them is
// the leak this file guards against.
var serviceGraphColumns = []string{"parent", "child", "callCount"}

// assertNoJunkFields is the shared "cold span carries no storage internals"
// assertion, reused by every test here.
func assertNoJunkFields(t *testing.T, where string, rows []map[string]string) {
	t.Helper()
	for i, row := range rows {
		for name, value := range row {
			if value == "<null>" {
				t.Errorf("%s: row %d field %q has the literal placeholder %q — a NULL "+
					"Parquet cell reached the value formatter instead of being skipped",
					where, i, name, value)
			}
			if schema.IsInternalColumn(name) {
				t.Errorf("%s: row %d carries storage bookkeeping field %q=%q; hot VT has "+
					"no such field", where, i, name, value)
			}
			if schema.IsDedicatedSlotColumn(name) {
				t.Errorf("%s: row %d carries raw Tier-2 slot column %q=%q; a slot must "+
					"surface under its configured attribute name or not at all",
					where, i, name, value)
			}
		}
		for _, sg := range serviceGraphColumns {
			if v, ok := row[sg]; ok {
				t.Errorf("%s: row %d carries service-graph column %q=%q on a plain span",
					where, i, sg, v)
			}
		}
	}
}

// assertVTFieldNames asserts the row uses VT's field vocabulary: promoted
// resource/span attributes carry their resource_attr: / span_attr: prefix and
// the raw Parquet spelling is absent.
func assertVTFieldNames(t *testing.T, where string, rows []map[string]string) {
	t.Helper()
	bare := []string{
		"service.name", "k8s.namespace.name", "http.method", "http.url",
		"url.full", "container.id", "span.name", "span.kind", "status.code",
		"duration_ns", "timestamp_unix_nano",
	}
	for i, row := range rows {
		for _, name := range bare {
			if v, ok := row[name]; ok {
				t.Errorf("%s: row %d carries Parquet-spelled field %q=%q; hot VT spells "+
					"it with the resource_attr:/span_attr: prefix (or under its VT alias)",
					where, i, name, v)
			}
		}
	}
}

func coldSpanFieldsTestRows(now time.Time, n int) []schema.TraceRow {
	rows := make([]schema.TraceRow, 0, n)
	for i := 0; i < n; i++ {
		r := schema.TraceRow{
			AccountID:          7,
			ProjectID:          42,
			TimestampUnixNano:  now.Add(time.Duration(i) * time.Second).UnixNano(),
			StartTimeUnixNano:  now.Add(time.Duration(i) * time.Second).UnixNano(),
			TraceID:            "trace-abcdef",
			SpanID:             "span-0" + string(rune('0'+i)),
			SpanName:           "GET /api/v1/things",
			ServiceName:        "api-gateway",
			DurationNs:         1_500_000,
			HTTPMethod:         "GET",
			ResourceAttributes: map[string]string{"custom.res": "r1"},
			SpanAttributes:     map[string]string{"custom.span": "s1"},
		}
		// Promoted dedicated columns on even spans only, so the column is NULL
		// for the others — the shape that produced "<null>".
		if i%2 == 0 {
			r.URLFull = "https://x/y"
			r.ClientAddress = "10.0.0.1"
		}
		if i == 0 {
			r.SpanAttributes["only.on.first"] = "yes"
		}
		rows = append(rows, r)
	}
	return rows
}

func openParquetBytes(t *testing.T, data []byte) *parquet.File {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func tracesTestStorage() *Storage {
	s := testStorage()
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	return s
}

// scanRowGroups runs the unprojected scan path — the one a wildcard span list
// uses — over every row group of f.
func scanRowGroups(t *testing.T, s *Storage, f *parquet.File, startNs, endNs int64, aliasCols map[string]bool) []map[string]string {
	t.Helper()
	var blocks []*logstorage.DataBlock
	for _, rg := range f.RowGroups() {
		if err := s.readOneRowGroup(f, rg, startNs, endNs, nil, nil,
			func(_ uint, db *logstorage.DataBlock) { blocks = append(blocks, db) }, nil, aliasCols); err != nil {
			t.Fatalf("readOneRowGroup: %v", err)
		}
	}
	return blockRowFields(blocks)
}

// scanRowGroupsTyped runs the typed reader (parquet -> schema.TraceRow ->
// traceRowToFields), whose field selection emits the ingested fields and nothing
// else.
func scanRowGroupsTyped(t *testing.T, s *Storage, f *parquet.File, startNs, endNs int64) []map[string]string {
	t.Helper()
	var blocks []*logstorage.DataBlock
	for _, rg := range f.RowGroups() {
		if err := s.readRowGroup(f, rg, startNs, endNs,
			func(_ uint, db *logstorage.DataBlock) { blocks = append(blocks, db) }, nil); err != nil {
			t.Fatalf("readRowGroup: %v", err)
		}
	}
	return blockRowFields(blocks)
}

// TestColdSpanFields_ExactFieldSet is the reproduction turned regression test: a
// cold span must carry exactly its ingested fields under VT's names. Before the
// fix it carried every Parquet leaf column of the file — each promoted column
// twice (prefixed and bare), the unset ones as the literal "<null>", plus
// account_id, project_id, ded_s01..ded_s08 and the service-graph columns.
func TestColdSpanFields_ExactFieldSet(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	// rowGroupSize 1 keeps every column constant within its group (the
	// row-oriented slow path); a larger group takes the columnar fast path.
	for _, rowGroupSize := range []int{1, 1000} {
		rows := coldSpanFieldsTestRows(now, 4)
		res, err := writeTracesParquet(rows, rowGroupSize, 3)
		if err != nil {
			t.Fatalf("writeTracesParquet: %v", err)
		}
		f := openParquetBytes(t, res.Data)
		s := tracesTestStorage()

		got := scanRowGroups(t, s, f, startNs, endNs, nil)
		if len(got) != len(rows) {
			t.Fatalf("rowGroupSize=%d: got %d spans, want %d", rowGroupSize, len(got), len(rows))
		}
		assertNoJunkFields(t, "wildcard scan", got)
		assertVTFieldNames(t, "wildcard scan", got)

		base := []string{
			"_time", "duration", "kind", "name", "resource_attr:custom.res",
			"resource_attr:service.name", "span_attr:custom.span",
			"span_attr:http.method", "span_id", "start_time_unix_nano", "status_code",
			"trace_id",
		}
		for i, row := range got {
			want := append([]string(nil), base...)
			if i%2 == 0 {
				want = append(want, "span_attr:url.full", "span_attr:client.address")
			}
			if i == 0 {
				want = append(want, "span_attr:only.on.first")
			}
			sort.Strings(want)
			if g := strings.Join(fieldNamesOf(row), ","); g != strings.Join(want, ",") {
				t.Errorf("rowGroupSize=%d span %d fields = %v, want %v",
					rowGroupSize, i, fieldNamesOf(row), want)
			}
		}
	}
}

// TestColdSpanFields_TypedPathNamesAreExplicit pins the typed reader's own field
// names and cross-checks the junk-hygiene invariants against the scan path.
//
// It deliberately does NOT treat the typed reader as the naming oracle for
// traces: traceRowToFields emits MAP attributes and the Tier-1 dedicated columns
// under their BARE key (`custom.res`, `url.full`), while the scan path emits them
// under VictoriaTraces' names (`resource_attr:custom.res`, `span_attr:url.full`),
// which is what hot VT returns and what a cold response must carry. The two
// spellings are asserted literally here so the divergence is visible and pinned
// rather than hidden behind a field-count comparison. (The logs module has no
// such split — there logRowToFields IS the naming oracle.)
func TestColdSpanFields_TypedPathNamesAreExplicit(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := coldSpanFieldsTestRows(now, 4)
	res, err := writeTracesParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeTracesParquet: %v", err)
	}
	f := openParquetBytes(t, res.Data)
	s := tracesTestStorage()
	startNs, endNs := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()

	got := scanRowGroups(t, s, f, startNs, endNs, nil)
	typed := scanRowGroupsTyped(t, s, f, startNs, endNs)
	assertNoJunkFields(t, "scan path", got)
	assertNoJunkFields(t, "typed path", typed)
	if len(got) != len(rows) || len(typed) != len(rows) {
		t.Fatalf("scan returned %d spans, typed %d, want %d", len(got), len(typed), len(rows))
	}

	// What the SCAN path emits — VT's field names, the response contract.
	scanBase := []string{
		"_time", "duration", "kind", "name", "resource_attr:custom.res",
		"resource_attr:service.name", "span_attr:custom.span",
		"span_attr:http.method", "span_id", "start_time_unix_nano", "status_code",
		"trace_id",
	}
	// What the TYPED path emits — same values, bare spelling for MAP attributes
	// and Tier-1 dedicated columns.
	typedBase := []string{
		"_time", "custom.res", "custom.span", "duration", "kind", "name",
		"resource_attr:service.name", "span_attr:http.method", "span_id",
		"start_time_unix_nano", "status_code", "trace_id",
	}
	for i := range got {
		wantScan := append([]string(nil), scanBase...)
		wantTyped := append([]string(nil), typedBase...)
		if i%2 == 0 {
			wantScan = append(wantScan, "span_attr:url.full", "span_attr:client.address")
			wantTyped = append(wantTyped, "url.full", "client.address")
		}
		if i == 0 {
			wantScan = append(wantScan, "span_attr:only.on.first")
			wantTyped = append(wantTyped, "only.on.first")
		}
		sort.Strings(wantScan)
		sort.Strings(wantTyped)
		if g := strings.Join(fieldNamesOf(got[i]), ","); g != strings.Join(wantScan, ",") {
			t.Errorf("span %d scan-path fields = %v, want %v", i, fieldNamesOf(got[i]), wantScan)
		}
		if g := strings.Join(fieldNamesOf(typed[i]), ","); g != strings.Join(wantTyped, ",") {
			t.Errorf("span %d typed-path fields = %v, want %v", i, fieldNamesOf(typed[i]), wantTyped)
		}
		// Whatever the spelling, both paths must carry the same VALUES.
		for _, pair := range [][2]string{
			{"resource_attr:custom.res", "custom.res"},
			{"span_attr:custom.span", "custom.span"},
			{"span_attr:url.full", "url.full"},
		} {
			if got[i][pair[0]] != typed[i][pair[1]] {
				t.Errorf("span %d value differs: scan %s=%q, typed %s=%q",
					i, pair[0], got[i][pair[0]], pair[1], typed[i][pair[1]])
			}
		}
	}
}

// TestColdSpanFields_MapKeysCannotSmuggleReservedNames pins the shared naming
// rule on the MAP side: mapAttrFieldName runs the name it is about to emit
// through the same classification every top-level column passes, so a key inside
// an attribute map can never introduce a reserved field name.
func TestColdSpanFields_MapKeysCannotSmuggleReservedNames(t *testing.T) {
	// A key VT spells at top level (no prefix) that collides with a reserved
	// name must be dropped — the hypothetical internal MAP column.
	topLevel := map[string]bool{"account_id": true, "ded_s01": true, "flags": true}
	for _, key := range []string{"account_id", "ded_s01"} {
		if name, ok := mapAttrFieldName("span.attributes", key, topLevel); ok {
			t.Errorf("top-level MAP key %q surfaced as %q; a reserved name must never "+
				"be emitted, whichever column produces it", key, name)
		}
	}
	// A genuine OTLP metadata key VT spells at top level still surfaces bare.
	if name, ok := mapAttrFieldName("span.attributes", "flags", topLevel); !ok || name != "flags" {
		t.Errorf("mapAttrFieldName(flags) = (%q, %v), want (\"flags\", true)", name, ok)
	}
	// The SAME keys under VT's prefix are ordinary user attributes and stay:
	// `span_attr:account_id` is not a reserved name.
	for key, want := range map[string]string{
		"account_id": "span_attr:account_id",
		"ded_s01":    "span_attr:ded_s01",
		"custom.x":   "span_attr:custom.x",
	} {
		if name, ok := mapAttrFieldName("span.attributes", key, nil); !ok || name != want {
			t.Errorf("mapAttrFieldName(%q) = (%q, %v), want (%q, true)", key, name, ok, want)
		}
	}
	if _, ok := mapAttrFieldName("span.attributes", "", nil); ok {
		t.Error("empty MAP key must not surface")
	}
}

// TestColdSpanFields_ReservedMapKeysDroppedEndToEnd is the file-level proof of
// the rule above: a span whose attribute maps carry reserved-looking keys never
// yields a reserved field name. `account_id` under VT's prefix is an ordinary
// user attribute and survives as `resource_attr:account_id`; a key spelling a
// Tier-2 slot is dropped outright by the promoted-duplicate rule (the slot
// COLUMN owns that name), so neither the bare nor the prefixed form appears.
func TestColdSpanFields_ReservedMapKeysDroppedEndToEnd(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []schema.TraceRow{{
		TimestampUnixNano:  now.UnixNano(),
		TraceID:            "t1",
		SpanID:             "s1",
		SpanName:           "op",
		ServiceName:        "api-gateway",
		ResourceAttributes: map[string]string{"account_id": "leak", "ded_s01": "leak"},
		SpanAttributes:     map[string]string{"project_id": "leak", "ded_s08": "leak"},
	}}
	res, err := writeTracesParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeTracesParquet: %v", err)
	}
	f := openParquetBytes(t, res.Data)
	got := scanRowGroups(t, tracesTestStorage(), f, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano(), nil)
	if len(got) != 1 {
		t.Fatalf("got %d spans, want 1", len(got))
	}
	assertNoJunkFields(t, "reserved map keys", got)
	for name, want := range map[string]string{
		"resource_attr:account_id": "leak",
		"span_attr:project_id":     "leak",
	} {
		if got[0][name] != want {
			t.Errorf("attribute %q = %q, want %q (the prefix makes it an ordinary user "+
				"attribute, so it must survive): %v", name, got[0][name], want, got[0])
		}
	}
	for _, name := range []string{
		"account_id", "project_id",
		"ded_s01", "ded_s08",
		"resource_attr:ded_s01", "span_attr:ded_s08",
	} {
		if v, ok := got[0][name]; ok {
			t.Errorf("reserved name %q surfaced as a field (=%q) from a MAP key: %v",
				name, v, got[0])
		}
	}
}

// TestColdSpanFields_ParquetNameAliasOnlyWhenQueried pins the alias contract: a
// query that spells the Parquet column name still matches (the a5576bf fix), but
// a query that does not gets the VT field names alone.
func TestColdSpanFields_ParquetNameAliasOnlyWhenQueried(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := coldSpanFieldsTestRows(now, 4)
	res, err := writeTracesParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeTracesParquet: %v", err)
	}
	f := openParquetBytes(t, res.Data)
	s := tracesTestStorage()
	startNs, endNs := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()

	aliases := queryParquetNameAliases(`service.name:="api-gateway"`, s.registry, nil)
	if !aliases["service.name"] {
		t.Fatalf("query spelling the Parquet column name did not request the alias: %v", aliases)
	}
	withAlias := scanRowGroups(t, s, f, startNs, endNs, aliases)
	if len(withAlias) == 0 || withAlias[0]["service.name"] != "api-gateway" {
		t.Errorf("alias column missing for a query that spells it: %v", withAlias[0])
	}
	if withAlias[0]["resource_attr:service.name"] != "api-gateway" {
		t.Errorf("VT field name missing alongside the alias: %v", withAlias[0])
	}

	if a := queryParquetNameAliases("*", s.registry, nil); len(a) != 0 {
		t.Errorf("wildcard query requested aliases %v, want none", a)
	}
	// A column-selecting pipe naming the Parquet spelling must still get it.
	if a := queryParquetNameAliases("* | fields service.name", s.registry, []string{"service.name"}); !a["service.name"] {
		t.Errorf("pipe naming the Parquet column did not request the alias: %v", a)
	}
}

// TestColdSpanFields_AliasTriggersPerPipeShape pins, against VictoriaLogs' own
// parser, exactly which query shapes request the bare Parquet-name alias. It
// derives queryStr and pipeFields the way RunQuery does (q.String() and
// logstorage.GetQueryPipeFields) so the documented contract in
// docs/parity-and-gaps.md "Intentional differences" is verified, not assumed:
// a filter term or a fields/stats/uniq/top pipe spelling the Parquet name
// triggers it; field_values / field_names / facets do not.
func TestColdSpanFields_AliasTriggersPerPipeShape(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cases := []struct {
		query     string
		wantAlias bool
	}{
		{`service.name:="api-gateway"`, true},
		{`* | fields service.name`, true},
		{`* | stats by (service.name) count() hits`, true},
		{`* | uniq by (service.name)`, true},
		{`* | top 5 by (service.name)`, true},
		{`*`, false},
		{`* | limit 10`, false},
		{`* | sort by (_time)`, false},
		{`* | field_values service.name`, false},
		{`* | field_names`, false},
		{`* | facets`, false},
		{`"resource_attr:service.name":="api-gateway"`, false},
	}
	for _, c := range cases {
		q, err := logstorage.ParseQuery(c.query)
		if err != nil {
			t.Fatalf("ParseQuery(%q): %v", c.query, err)
		}
		aliases := queryParquetNameAliases(q.String(), reg, logstorage.GetQueryPipeFields(q))
		if got := aliases["service.name"]; got != c.wantAlias {
			t.Errorf("query %q (serialized %q): service.name alias requested = %v, want %v (aliases=%v)",
				c.query, q.String(), got, c.wantAlias, aliases)
		}
	}
}

// TestColdSpanFields_ServiceGraphRowsKeepTheirColumns is the counterpart of the
// service-graph leak guard: an actual service-graph edge row MUST still surface
// parent / child / callCount, because the Jaeger Dependencies reader queries
// them by name.
func TestColdSpanFields_ServiceGraphRowsKeepTheirColumns(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []schema.TraceRow{{
		TimestampUnixNano:     now.UnixNano(),
		TraceID:               "sg",
		SpanID:                "sg",
		Stream:                `{trace_service_graph_stream="-"}`,
		ServiceGraphParent:    "frontend",
		ServiceGraphChild:     "api-gateway",
		ServiceGraphCallCount: "17",
	}}
	res, err := writeTracesParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeTracesParquet: %v", err)
	}
	f := openParquetBytes(t, res.Data)
	got := scanRowGroups(t, tracesTestStorage(), f, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano(), nil)
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1", len(got))
	}
	for name, want := range map[string]string{"parent": "frontend", "child": "api-gateway", "callCount": "17"} {
		if got[0][name] != want {
			t.Errorf("service-graph row lost %q: got %q, want %q (row = %v)", name, got[0][name], want, got[0])
		}
	}
}

// TestColdSpanFields_NoJunkThroughQueryPipes runs the full query path over a file
// in mock S3 with the query shapes Jaeger/Grafana send.
func TestColdSpanFields_NoJunkThroughQueryPipes(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := coldSpanFieldsTestRows(now, 4)
	res, err := writeTracesParquet(rows, 2, 3)
	if err != nil {
		t.Fatalf("writeTracesParquet: %v", err)
	}
	registerFileInMockS3(t, s, mock, "traces/dt=2026-05-10/hour=14/b1.parquet", res.Data, now)

	for _, queryStr := range []string{
		"*",
		"* | limit 2",
		"* | sort by (_time)",
		`trace_id:="trace-abcdef"`,
	} {
		t.Run(queryStr, func(t *testing.T) {
			q := mustParseQueryWithTime(t, queryStr, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
			var mu sync.Mutex
			var blocks []*logstorage.DataBlock
			if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
				mu.Lock()
				blocks = append(blocks, db)
				mu.Unlock()
			}); err != nil {
				t.Fatalf("RunQuery(%q): %v", queryStr, err)
			}
			got := blockRowFields(blocks)
			if len(got) == 0 {
				t.Fatalf("RunQuery(%q) returned no spans", queryStr)
			}
			assertNoJunkFields(t, queryStr, got)
			assertVTFieldNames(t, queryStr, got)
			for i, row := range got {
				if row["trace_id"] == "" {
					t.Errorf("%s: span %d lost trace_id: %v", queryStr, i, row)
				}
			}
		})
	}
}

// TestQueryFieldName_EveryBranch pins each branch of the shared top-level naming
// rule directly, including the ones the file-level tests cannot reach through a
// real Parquet file (a slot bound to a reserved name, an internal column handed
// in explicitly even though allLeafColumns never projects one).
func TestQueryFieldName_EveryBranch(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	slots := schema.SlotMapping{
		"ded_s01": "tenant_id",
		"ded_s02": "",           // bound to nothing
		"ded_s03": "project_id", // bound to a reserved name
	}
	cases := []struct {
		col      string
		wantName string
		wantOK   bool
	}{
		{"ded_s01", "tenant_id", true},                       // mapped slot -> configured name
		{"ded_s02", "", false},                               // slot bound to the empty name
		{"ded_s03", "", false},                               // slot bound to a reserved name
		{"ded_s08", "", false},                               // unmapped slot
		{"account_id", "", false},                            // internal column
		{"project_id", "", false},                            // internal column
		{"", "", false},                                      // empty column name
		{"timestamp_unix_nano", "_time", true},               // registry-resolved
		{"span.name", "name", true},                          // registry-resolved
		{"service.name", "resource_attr:service.name", true}, // VT resource prefix
		{"http.method", "span_attr:http.method", true},       // VT span prefix
		{"url.full", "span_attr:url.full", true},             // Tier-1 dedicated
		{"container.id", "resource_attr:container.id", true}, // Tier-1 dedicated
		{"parent", "parent", true},                           // service-graph column
		{"some.new.column", "some.new.column", true},         // unknown column passes through
	}
	for _, c := range cases {
		name, ok := queryFieldName(c.col, reg, slots)
		if name != c.wantName || ok != c.wantOK {
			t.Errorf("queryFieldName(%q) = (%q, %v), want (%q, %v)", c.col, name, ok, c.wantName, c.wantOK)
		}
	}
	// A nil slot mapping (no custom promotions configured) drops every slot.
	if name, ok := queryFieldName("ded_s01", reg, nil); ok {
		t.Errorf("queryFieldName(ded_s01, nil slots) = (%q, true), want dropped", name)
	}
}

// TestNonEmptyOrNil covers the all-empty-column rule directly.
func TestNonEmptyOrNil(t *testing.T) {
	if got := nonEmptyOrNil([]string{"", ""}, false); got != nil {
		t.Errorf("all-empty column must be dropped, got %q", got)
	}
	if got := nonEmptyOrNil([]string{"", "x"}, true); len(got) != 2 || got[1] != "x" {
		t.Errorf("column with a value must be kept intact, got %q", got)
	}
}
