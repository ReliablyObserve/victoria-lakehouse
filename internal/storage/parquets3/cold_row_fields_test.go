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

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The cold read path must hand back, for every row, exactly the fields that were
// ingested for it — the same set hot VictoriaLogs returns. It used to hand back
// far more: every Parquet leaf column of the file, with unset cells rendered as
// the literal string "<null>", plus the tenant bookkeeping columns and the
// unmapped Tier-2 spare slots. In Grafana that decorated every cold log line with
// ~30 junk fields that the same line, served hot, never shows.
//
// The tests below pin the contract at the lowest layer that can express it (the
// row-group readers, both the columnar fast path and the row-oriented slow path)
// and at the query layer (RunQuery with the pipes Grafana actually sends).

// blockRowFields renders DataBlocks the way the JSON response writer does:
// one map per row, empty values dropped — VictoriaLogs skips empty fields
// because "they equal to non-existing fields"
// (deps/VictoriaLogs/app/vlselect/logsql/logsql.go).
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

// assertNoJunkFields is the shared "cold row carries no storage internals"
// assertion: no "<null>" placeholder values, no tenant bookkeeping columns, no
// raw Tier-2 slot columns. Reused by every test in this file so a regression in
// any one read path fires here.
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
				t.Errorf("%s: row %d carries storage bookkeeping field %q=%q; hot VL has "+
					"no such field", where, i, name, value)
			}
			if schema.IsDedicatedSlotColumn(name) {
				t.Errorf("%s: row %d carries raw Tier-2 slot column %q=%q; a slot must "+
					"surface under its configured attribute name or not at all",
					where, i, name, value)
			}
		}
	}
}

// coldRowFieldsTestRows is the fixture: attributes present on some rows and
// absent on others, promoted dedicated columns partly unset, tenant columns set,
// and a multi-line body.
func coldRowFieldsTestRows(now time.Time, n int) []schema.LogRow {
	rows := make([]schema.LogRow, 0, n)
	for i := 0; i < n; i++ {
		r := schema.LogRow{
			AccountID:         7,
			ProjectID:         42,
			TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "line one\nline two\ttab",
			SeverityText:      "INFO",
			SeverityNumber:    9,
			ServiceName:       "api-gw",
			K8sNamespaceName:  "prod",
			Stream:            `{service.name="api-gw"}`,
			StreamID:          "stream-1",
			LogAttributes:     map[string]string{"custom.key": "v1"},
		}
		// Promoted dedicated columns: set on even rows only, so the column is
		// NULL for half the rows — the shape that produced "<null>".
		if i%2 == 0 {
			r.ContainerID = "ctr-abc"
			r.ExceptionType = "TimeoutError"
		}
		// An attribute that exists on one row only.
		if i == 0 {
			r.LogAttributes["only.on.first"] = "yes"
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

// scanRowGroups runs the unprojected scan path — the one /select/logsql/query
// uses for a wildcard query — over every row group of f.
func scanRowGroups(t *testing.T, s *Storage, f *parquet.File, startNs, endNs int64, projected map[string]bool) []map[string]string {
	t.Helper()
	var blocks []*logstorage.DataBlock
	for _, rg := range f.RowGroups() {
		if err := s.readOneRowGroup(f, rg, startNs, endNs, projected, nil,
			func(_ uint, db *logstorage.DataBlock) { blocks = append(blocks, db) }, nil); err != nil {
			t.Fatalf("readOneRowGroup: %v", err)
		}
	}
	return blockRowFields(blocks)
}

// scanRowGroupsTyped runs the typed reader (parquet -> schema.LogRow ->
// logRowToFields), the path whose field selection is the reference: it emits the
// ingested fields and nothing else, which is what hot VL returns.
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

// TestColdRowFields_Logs_MatchTypedPath is the reproduction turned regression
// test: the scan path used by a cold wildcard query must produce, row by row,
// the SAME field set as the typed reader — which emits exactly the ingested
// fields. Before the fix the scan path added ~30 "<null>" fields, account_id,
// project_id and ded_s01..ded_s08 to every row.
func TestColdRowFields_Logs_MatchTypedPath(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	// rowGroupSize 1 forces multiple row groups; rowGroupSize >= len(rows)
	// keeps one. Both are exercised, and with >1 row per group the scan takes
	// the columnar fast path while a single-row group falls to the
	// constant-column slow path — the two implementations that used to drift.
	for _, rowGroupSize := range []int{1, 1000} {
		rows := coldRowFieldsTestRows(now, 4)
		res, err := writeLogsParquet(rows, rowGroupSize, 3)
		if err != nil {
			t.Fatalf("writeLogsParquet: %v", err)
		}
		f := openParquetBytes(t, res.Data)
		s := testStorage()

		got := scanRowGroups(t, s, f, startNs, endNs, nil)
		want := scanRowGroupsTyped(t, s, f, startNs, endNs)

		assertNoJunkFields(t, "scan path", got)
		assertNoJunkFields(t, "typed path", want)

		if len(got) != len(rows) || len(want) != len(rows) {
			t.Fatalf("rowGroupSize=%d: scan returned %d rows, typed %d, want %d",
				rowGroupSize, len(got), len(want), len(rows))
		}
		for i := range got {
			g, w := fieldNamesOf(got[i]), fieldNamesOf(want[i])
			if strings.Join(g, ",") != strings.Join(w, ",") {
				t.Errorf("rowGroupSize=%d row %d field set differs\n scan  = %v\n typed = %v",
					rowGroupSize, i, g, w)
				continue
			}
			for _, name := range g {
				if got[i][name] != want[i][name] {
					t.Errorf("rowGroupSize=%d row %d field %q: scan=%q typed=%q",
						rowGroupSize, i, name, got[i][name], want[i][name])
				}
			}
		}
	}
}

// TestColdRowFields_Logs_ExactFieldSet pins the absolute expectation, not just
// agreement between two of our own paths: the exact field names a row carries.
func TestColdRowFields_Logs_ExactFieldSet(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := coldRowFieldsTestRows(now, 4)
	res, err := writeLogsParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeLogsParquet: %v", err)
	}
	f := openParquetBytes(t, res.Data)
	s := testStorage()

	got := scanRowGroups(t, s, f, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano(), nil)
	if len(got) != len(rows) {
		t.Fatalf("got %d rows, want %d", len(got), len(rows))
	}
	assertNoJunkFields(t, "wildcard scan", got)

	base := []string{
		"_msg", "_stream", "_stream_id", "_time",
		"custom.key", "k8s.namespace.name", "level", "service.name", "severity_number",
	}
	for i, row := range got {
		want := append([]string(nil), base...)
		if i%2 == 0 {
			want = append(want, "container.id", "exception.type")
		}
		if i == 0 {
			want = append(want, "only.on.first")
		}
		sort.Strings(want)
		if g := strings.Join(fieldNamesOf(row), ","); g != strings.Join(want, ",") {
			t.Errorf("row %d fields = %v, want %v", i, fieldNamesOf(row), want)
		}
	}
	// The multi-line body must survive intact.
	if got[0]["_msg"] != "line one\nline two\ttab" {
		t.Errorf("_msg = %q, want the ingested multi-line body", got[0]["_msg"])
	}
}

// TestColdRowFields_Logs_SlotColumnsNamedOrDropped pins the Tier-2 spare-slot
// rule on the scan path: a slot bound by the file's footer KV surfaces under the
// configured attribute name, an unbound slot does not surface at all.
func TestColdRowFields_Logs_SlotColumnsNamedOrDropped(t *testing.T) {
	prev := activeSlotResolver
	t.Cleanup(func() { SetSlotResolver(prev) })
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant_id"}}))

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := coldRowFieldsTestRows(now, 2)
	for i := range rows {
		schema.SetLogSlot(&rows[i], "ded_s01", "acme-co")
	}
	res, err := writeLogsParquet(rows, 1000, 3)
	if err != nil {
		t.Fatalf("writeLogsParquet: %v", err)
	}
	f := openParquetBytes(t, res.Data)

	got := scanRowGroups(t, testStorage(), f, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano(), nil)
	assertNoJunkFields(t, "slot scan", got)
	if len(got) == 0 {
		t.Fatal("no rows")
	}
	if got[0]["tenant_id"] != "acme-co" {
		t.Errorf("bound slot did not surface as tenant_id: row = %v", got[0])
	}
}

// TestColdRowFields_Logs_NoJunkThroughQueryPipes runs the full query path
// (RunQuery over a file in mock S3) with the pipe shapes Grafana sends, because
// the pipes route through different projections: a wildcard reads every column,
// `| sort` and `| limit` go through the projection layer.
func TestColdRowFields_Logs_NoJunkThroughQueryPipes(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := coldRowFieldsTestRows(now, 4)
	res, err := writeLogsParquet(rows, 2, 3)
	if err != nil {
		t.Fatalf("writeLogsParquet: %v", err)
	}
	registerFileInMockS3(t, s, mock, "logs/dt=2026-05-10/hour=14/b1.parquet", res.Data, now)

	for _, queryStr := range []string{
		"*",
		"* | limit 2",
		"* | sort by (_time)",
		`service.name:="api-gw"`,
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
				t.Fatalf("RunQuery(%q) returned no rows", queryStr)
			}
			assertNoJunkFields(t, queryStr, got)
			for i, row := range got {
				if row["_msg"] == "" {
					t.Errorf("%s: row %d lost _msg: %v", queryStr, i, row)
				}
			}
		})
	}
}
