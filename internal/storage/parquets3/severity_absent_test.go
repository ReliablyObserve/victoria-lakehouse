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

// Issue #274: severity_number is a nullable column. A row that never carried the
// field (a plain jsonline row) must come back without it, exactly as hot
// VictoriaLogs stores it, while an explicit "0" (what OTLP writes for
// UNSPECIFIED) must come back as "0". Before the fix the column was a required
// INT32, so absent and 0 were the same value and every row was given "0".

// severityAbsentRows is [absent, explicit 0, explicit 9, absent], all on one
// stream, so one file holds every state.
func severityAbsentRows(now time.Time) []schema.LogRow {
	nums := []*int32{nil, schema.Int32Ptr(0), schema.Int32Ptr(9), nil}
	rows := make([]schema.LogRow, len(nums))
	for i, n := range nums {
		rows[i] = schema.LogRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "sevabsent " + string(rune('a'+i)),
			SeverityNumber:    n,
			ServiceName:       "api-gw",
			Stream:            `{service.name="api-gw"}`,
			StreamID:          "stream-1",
		}
	}
	return rows
}

// wantSeverity is the expected severity_number per row ("" = field absent).
var wantSeverity = []string{"", "0", "9", ""}

func severityOf(rows []map[string]string) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r["severity_number"]
	}
	return out
}

// sortedByMsg orders rows by body so the comparison does not depend on the
// order the readers return them in.
func sortedByMsg(rows []map[string]string) []map[string]string {
	out := append([]map[string]string(nil), rows...)
	sort.Slice(out, func(i, j int) bool { return out[i]["_msg"] < out[j]["_msg"] })
	return out
}

func TestSeverityAbsent_ParquetColumnIsNullable(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	res, err := writeLogsParquet(severityAbsentRows(now), 1000, 3)
	if err != nil {
		t.Fatal(err)
	}
	f := openParquetBytes(t, res.Data)
	col := f.Root().Column("severity_number")
	if col == nil {
		t.Fatal("no severity_number column")
	}
	if !col.Optional() {
		t.Fatal("severity_number must be an optional column so an absent value is NULL, not 0")
	}
	if col.Type().Kind() != parquet.Int32 {
		t.Fatalf("severity_number kind = %v, want INT32", col.Type().Kind())
	}
	nulls, zeros := 0, 0
	for _, rg := range f.RowGroups() {
		pages := rg.ColumnChunks()[col.Index()].Pages()
		for {
			p, err := pages.ReadPage()
			if err != nil {
				break
			}
			nulls += int(p.NumNulls())
			vr := p.Values()
			buf := make([]parquet.Value, int(p.NumValues()))
			n, _ := vr.ReadValues(buf)
			for _, v := range buf[:n] {
				if !v.IsNull() && v.Int32() == 0 {
					zeros++
				}
			}
		}
		_ = pages.Close()
	}
	if nulls != 2 || zeros != 1 {
		t.Fatalf("physical cells: %d NULL and %d zero, want 2 NULL (absent rows) and 1 zero (the explicit 0)", nulls, zeros)
	}
}

func TestSeverityAbsent_ScanPathsKeepAbsentAbsent(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	startNs, endNs := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()
	// 1 row per group takes the constant-column path, 1000 the columnar fast path.
	for _, rgSize := range []int{1, 1000} {
		res, err := writeLogsParquet(severityAbsentRows(now), rgSize, 3)
		if err != nil {
			t.Fatal(err)
		}
		f := openParquetBytes(t, res.Data)
		s := testStorage()
		for name, got := range map[string][]map[string]string{
			"scan":  scanRowGroups(t, s, f, startNs, endNs, nil),
			"typed": scanRowGroupsTyped(t, s, f, startNs, endNs),
		} {
			got = sortedByMsg(got)
			if g := severityOf(got); strings.Join(g, "|") != strings.Join(wantSeverity, "|") {
				t.Errorf("rowGroupSize=%d %s path: severity_number per row = %q, want %q", rgSize, name, g, wantSeverity)
			}
		}
	}
}

func TestSeverityAbsent_QuerySurfaces(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	res, err := writeLogsParquet(severityAbsentRows(now), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, "logs/dt=2026-05-10/hour=14/sev.parquet", res.Data, now)
	lo, hi := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()

	run := func(qs string) []map[string]string {
		t.Helper()
		q := mustParseQueryWithTime(t, qs, lo, hi)
		var mu sync.Mutex
		var blocks []*logstorage.DataBlock
		if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			blocks = append(blocks, db)
			mu.Unlock()
		}); err != nil {
			t.Fatalf("RunQuery(%q): %v", qs, err)
		}
		return sortedByMsg(blockRowFields(blocks))
	}

	// query rows
	if g := severityOf(run("*")); strings.Join(g, "|") != strings.Join(wantSeverity, "|") {
		t.Errorf("query rows: severity_number per row = %q, want %q", g, wantSeverity)
	}
	// the projection layer (pipes and filters take different column sets)
	for _, qs := range []string{"* | sort by (_time)", "* | limit 10", "* | fields _msg, severity_number"} {
		if g := severityOf(run(qs)); strings.Join(g, "|") != strings.Join(wantSeverity, "|") {
			t.Errorf("%q: severity_number per row = %q, want %q", qs, g, wantSeverity)
		}
	}
	// filters: the field exists on two rows only
	for qs, want := range map[string]int{
		"severity_number:*":   2,
		"-severity_number:*":  2,
		"severity_number:0":   1,
		"severity_number:=9":  1,
		"severity_number:>=0": 2,
	} {
		if got := len(run(qs)); got != want {
			t.Errorf("%q matched %d rows, want %d", qs, got, want)
		}
	}
	// field_names and field_values
	q := mustParseQueryWithTime(t, "*", lo, hi)
	names, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	listed := false
	for _, n := range names {
		if n.Value == "severity_number" {
			listed = true
		}
	}
	if !listed {
		t.Errorf("field_names does not list severity_number although two rows carry it")
	}
	vals, err := s.GetFieldValues(context.Background(), nil, q, "severity_number", 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]uint64{}
	for _, v := range vals {
		got[v.Value] = v.Hits
	}
	// Upstream counts rows without the field as one hit of the empty value.
	if len(got) != 3 || got[""] != 2 || got["0"] != 1 || got["9"] != 1 {
		t.Errorf("field_values(severity_number) = %v, want exactly {\"\":2, 0:1, 9:1}", got)
	}
}

// A row still in a peer's buffer must read like the same row after the flush:
// the bridge block holds an empty value (an absent field) for a row without
// severity_number and "0" for an explicit 0.
func TestSeverityAbsent_BridgeBlockMatchesFileRows(t *testing.T) {
	s := testStorage()
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := severityAbsentRows(now)
	db := s.logRowsToDataBlock(tenantScope{account: "0", project: "0"}, "test", rows)
	if db == nil {
		t.Fatal("no block")
	}
	var got []string
	for _, c := range db.GetColumns(false) {
		if c.Name == "severity_number" {
			got = c.Values
		}
	}
	if strings.Join(got, "|") != strings.Join(wantSeverity, "|") {
		t.Errorf("bridge severity_number per row = %q, want %q", got, wantSeverity)
	}
	// and the file path (logRowToFields) agrees row by row
	for i := range rows {
		file := ""
		for _, f := range logRowToFields(&rows[i], nil) {
			if f.name == "severity_number" {
				file = s.registry.FormatField(f.name, f.value)
			}
		}
		if file != wantSeverity[i] {
			t.Errorf("row %d: file path severity_number %q, want %q", i, file, wantSeverity[i])
		}
	}
}

// A file written before the fix has a required INT32 severity_number: every
// cell is a value, and a row that never carried the field holds a 0. There are
// no users on such files yet and nothing rewrites them, so the contract is only
// that they stay readable and keep their stored value (the 0 stays "0").
func TestSeverityAbsent_LegacyRequiredColumnStillReads(t *testing.T) {
	type legacyRow struct {
		TimestampUnixNano int64  `parquet:"timestamp_unix_nano,delta"`
		Body              string `parquet:"body"`
		SeverityNumber    int32  `parquet:"severity_number"`
		ServiceName       string `parquet:"service.name,dict"`
		Stream            string `parquet:"_stream,dict"`
		StreamID          string `parquet:"_stream_id,dict"`
	}
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[legacyRow](&buf)
	_, err := w.Write([]legacyRow{
		{now.UnixNano(), "legacy a", 0, "api-gw", `{service.name="api-gw"}`, "stream-1"},
		{now.Add(time.Second).UnixNano(), "legacy b", 9, "api-gw", `{service.name="api-gw"}`, "stream-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f := openParquetBytes(t, buf.Bytes())
	startNs, endNs := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()
	s := testStorage()
	for name, got := range map[string][]map[string]string{
		"scan":  scanRowGroups(t, s, f, startNs, endNs, nil),
		"typed": scanRowGroupsTyped(t, s, f, startNs, endNs),
	} {
		got = sortedByMsg(got)
		if g := severityOf(got); len(g) != 2 || g[0] != "0" || g[1] != "9" {
			t.Errorf("%s path over a legacy file: severity_number = %q, want [0 9]", name, g)
		}
	}
}
