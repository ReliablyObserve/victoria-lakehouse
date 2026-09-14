package main

import (
	"bytes"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// The readback gate (verify.py) holds pyarrow and DuckDB to the truth this
// generator writes into manifest.json. These tests read the generated files
// back with parquet-go and recompute that truth from the rows actually on
// disk, and check the production writer options the gate claims to cover
// (row-group bound, blooms, encoding tags, the trace-index footer). A wrong
// truth would have the gate compare both engines against the wrong numbers;
// a file missing an option would have it certify a layout production does not
// write.

const (
	testRows     = 750
	testRowGroup = 200
)

// readBack opens a generated file and decodes every row.
func readBack[T any](t *testing.T, path string) (*parquet.File, []T) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parquet-go cannot open %s: %v", path, err)
	}
	r := parquet.NewGenericReader[T](bytes.NewReader(data))
	defer func() { _ = r.Close() }()
	rows := make([]T, 0, r.NumRows())
	buf := make([]T, 128)
	for {
		n, err := r.Read(buf)
		rows = append(rows, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
	}
	return f, rows
}

// assertWriterOptions checks the production writer options on the file: every
// row group within the bound, the bloom columns present in every row group,
// and every delta/dict column the truth lists present in the schema.
func assertWriterOptions(t *testing.T, f *parquet.File, truth fileTruth) {
	t.Helper()
	wantGroups := (testRows + testRowGroup - 1) / testRowGroup
	if got := len(f.RowGroups()); got != wantGroups {
		t.Errorf("%s: %d row groups, want %d for %d rows bounded at %d", truth.File, got, wantGroups, testRows, testRowGroup)
	}
	for i, rg := range f.RowGroups() {
		if rg.NumRows() > testRowGroup {
			t.Errorf("%s: row group %d has %d rows, over the %d bound", truth.File, i, rg.NumRows(), testRowGroup)
		}
		for _, col := range []string{"service.name", "trace_id"} {
			leaf, ok := f.Schema().Lookup(col)
			if !ok {
				t.Fatalf("%s: no %s column", truth.File, col)
			}
			if rg.ColumnChunks()[leaf.ColumnIndex].BloomFilter() == nil {
				t.Errorf("%s: row group %d has no bloom filter on %s", truth.File, i, col)
			}
		}
	}
	for kind, cols := range map[string][]string{"delta": truth.DeltaColumns, "dict": truth.DictColumns} {
		if len(cols) == 0 {
			t.Errorf("%s: no %s columns listed; the gate would assert no encodings", truth.File, kind)
		}
		if !sort.StringsAreSorted(cols) {
			t.Errorf("%s: %s columns %v are not sorted", truth.File, kind, cols)
		}
		for _, col := range cols {
			if _, ok := f.Schema().Lookup(col); !ok {
				t.Errorf("%s: %s column %q is not in the written schema", truth.File, kind, col)
			}
		}
	}
}

// assertTruth compares the truth the generator reported with the one
// recomputed from the rows read back.
func assertTruth(t *testing.T, truth fileTruth, rows int, sums map[string]*big.Int, distinct map[string]map[string]bool) {
	t.Helper()
	if truth.Rows != int64(rows) || rows != testRows {
		t.Errorf("%s: truth says %d rows, file has %d, asked for %d", truth.File, truth.Rows, rows, testRows)
	}
	if len(truth.Int64Sums) != len(sums) {
		t.Errorf("%s: truth sums %d columns, recomputed %d", truth.File, len(truth.Int64Sums), len(sums))
	}
	for col, want := range sums {
		if got := truth.Int64Sums[col]; got == nil || got.Cmp(want) != 0 {
			t.Errorf("%s: sum(%s) truth=%v, read back=%v", truth.File, col, got, want)
		}
	}
	if len(truth.DistinctCounts) != len(distinct) {
		t.Errorf("%s: truth counts distinct values of %d columns, recomputed %d", truth.File, len(truth.DistinctCounts), len(distinct))
	}
	for col, set := range distinct {
		if got := truth.DistinctCounts[col]; got != int64(len(set)) {
			t.Errorf("%s: distinct(%s) truth=%d, read back=%d", truth.File, col, got, len(set))
		}
	}
}

func TestGenLogs_TruthMatchesTheFileWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.parquet")
	truth, err := genLogs(path, testRows, testRowGroup)
	if err != nil {
		t.Fatal(err)
	}
	if truth.Signal != "logs" || truth.File != "logs.parquet" {
		t.Errorf("truth names %s/%s", truth.Signal, truth.File)
	}
	f, rows := readBack[schema.LogRow](t, path)

	sums := map[string]*big.Int{}
	distinct := map[string]map[string]bool{}
	add := func(col string, v int64) {
		if sums[col] == nil {
			sums[col] = new(big.Int)
		}
		sums[col].Add(sums[col], big.NewInt(v))
	}
	see := func(col, v string) {
		if distinct[col] == nil {
			distinct[col] = map[string]bool{}
		}
		distinct[col][v] = true
	}
	for _, r := range rows {
		add("timestamp_unix_nano", r.TimestampUnixNano)
		add("severity_number", int64(r.SeverityNumber))
		add("account_id", int64(r.AccountID))
		add("project_id", int64(r.ProjectID))
		see("service.name", r.ServiceName)
		see("severity_text", r.SeverityText)
		see("k8s.namespace.name", r.K8sNamespaceName)
		see("k8s.node.name", r.K8sNodeName)
		see("deployment.environment", r.DeployEnv)
		see("_stream_id", r.StreamID)
	}
	assertTruth(t, truth, len(rows), sums, distinct)
	assertWriterOptions(t, f, truth)
}

func TestGenTraces_TruthMatchesTheFileWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traces.parquet")
	truth, err := genTraces(path, testRows, testRowGroup)
	if err != nil {
		t.Fatal(err)
	}
	if truth.Signal != "traces" || truth.File != "traces.parquet" {
		t.Errorf("truth names %s/%s", truth.Signal, truth.File)
	}
	f, rows := readBack[schema.TraceRow](t, path)

	sums := map[string]*big.Int{}
	distinct := map[string]map[string]bool{}
	add := func(col string, v int64) {
		if sums[col] == nil {
			sums[col] = new(big.Int)
		}
		sums[col].Add(sums[col], big.NewInt(v))
	}
	see := func(col, v string) {
		if distinct[col] == nil {
			distinct[col] = map[string]bool{}
		}
		distinct[col][v] = true
	}
	graphEdges := 0
	for _, r := range rows {
		add("timestamp_unix_nano", r.TimestampUnixNano)
		add("start_time_unix_nano", r.StartTimeUnixNano)
		add("duration_ns", r.DurationNs)
		add("status.code", int64(r.StatusCode))
		add("span.kind", int64(r.SpanKind))
		see("service.name", r.ServiceName)
		see("span.name", r.SpanName)
		see("http.method", r.HTTPMethod)
		see("db.system", r.DBSystem)
		see("_stream_id", r.StreamID)
		if r.ServiceGraphParent != "" {
			graphEdges++
		}
	}
	assertTruth(t, truth, len(rows), sums, distinct)
	assertWriterOptions(t, f, truth)

	if graphEdges == 0 || graphEdges == len(rows) {
		t.Errorf("service-graph columns must carry both values and empties, got %d of %d rows set", graphEdges, len(rows))
	}

	// The production trace writer embeds the trace index in the footer's
	// key-value metadata; the gate file must carry the same footer.
	raw, ok := f.Lookup(traceindex.MetadataKey)
	if !ok {
		t.Fatalf("no %s footer key", traceindex.MetadataKey)
	}
	entries, err := traceindex.Unmarshal([]byte(raw))
	if err != nil {
		t.Fatalf("the %s footer does not decode: %v", traceindex.MetadataKey, err)
	}
	if want := len(traceindex.Compute(rows)); len(entries) != want || want == 0 {
		t.Errorf("the footer indexes %d traces, the rows read back hold %d", len(entries), want)
	}
}

// TestGen_IsDeterministic pins the fixed seeds: the gate's truth is only
// comparable across runs, and a failure only reproducible, if the same
// arguments always write the same data. The logs file is byte-identical. The
// traces file is identical in rows, truth and trace-index content, but not in
// bytes: traceindex.Compute collects entries from a map, so the footer lists
// them in a different order on every run.
func TestGen_IsDeterministic(t *testing.T) {
	dir := t.TempDir()
	path := func(name string) string { return filepath.Join(dir, name) }

	logsA, err := genLogs(path("logs-a.parquet"), testRows, testRowGroup)
	if err != nil {
		t.Fatal(err)
	}
	logsB, err := genLogs(path("logs-b.parquet"), testRows, testRowGroup)
	if err != nil {
		t.Fatal(err)
	}
	da, _ := os.ReadFile(path("logs-a.parquet"))
	db, _ := os.ReadFile(path("logs-b.parquet"))
	if len(da) == 0 || !bytes.Equal(da, db) {
		t.Error("logs: two runs with the same arguments wrote different bytes")
	}
	if !sameTruth(logsA, logsB) {
		t.Error("logs: two runs reported different truth")
	}

	tracesA, err := genTraces(path("traces-a.parquet"), testRows, testRowGroup)
	if err != nil {
		t.Fatal(err)
	}
	tracesB, err := genTraces(path("traces-b.parquet"), testRows, testRowGroup)
	if err != nil {
		t.Fatal(err)
	}
	if !sameTruth(tracesA, tracesB) {
		t.Error("traces: two runs reported different truth")
	}
	fa, rowsA := readBack[schema.TraceRow](t, path("traces-a.parquet"))
	fb, rowsB := readBack[schema.TraceRow](t, path("traces-b.parquet"))
	if !reflect.DeepEqual(rowsA, rowsB) {
		t.Error("traces: two runs wrote different rows")
	}
	if a, b := sortedIndex(t, fa), sortedIndex(t, fb); !reflect.DeepEqual(a, b) {
		t.Error("traces: two runs wrote different trace-index entries")
	}
}

func sameTruth(a, b fileTruth) bool {
	if a.Rows != b.Rows || !reflect.DeepEqual(a.DistinctCounts, b.DistinctCounts) || len(a.Int64Sums) != len(b.Int64Sums) {
		return false
	}
	for col, sum := range a.Int64Sums {
		if b.Int64Sums[col] == nil || sum.Cmp(b.Int64Sums[col]) != 0 {
			return false
		}
	}
	return reflect.DeepEqual(a.DeltaColumns, b.DeltaColumns) && reflect.DeepEqual(a.DictColumns, b.DictColumns)
}

// sortedIndex decodes a file's trace-index footer, ordered by trace id.
func sortedIndex(t *testing.T, f *parquet.File) []traceindex.Entry {
	t.Helper()
	raw, ok := f.Lookup(traceindex.MetadataKey)
	if !ok {
		t.Fatalf("no %s footer key", traceindex.MetadataKey)
	}
	entries, err := traceindex.Unmarshal([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].TraceID < entries[j].TraceID })
	return entries
}

func TestGen_ReportsAnUnwritablePath(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "no", "such", "dir", "x.parquet")
	if _, err := genLogs(missingDir, 10, 5); err == nil {
		t.Error("genLogs must return the write error, not report truth for a file it could not write")
	}
	if _, err := genTraces(missingDir, 10, 5); err == nil {
		t.Error("genTraces must return the write error, not report truth for a file it could not write")
	}
}
