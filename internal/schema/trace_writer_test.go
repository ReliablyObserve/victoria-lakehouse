package schema_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil"
)

const deltaEnc = "DELTA_BINARY_PACKED"

// traceSpans builds n spans whose start time equals the record timestamp (the
// OTLP shape: the span timestamp IS its start time), every 7th span without a
// start time so the column carries NULLs and values.
func traceSpans(n int) []schema.TraceRow {
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		ts := int64(1700000000000000000) + int64(i)*1000
		rows[i] = schema.TraceRow{
			TimestampUnixNano: ts,
			TraceID:           fmt.Sprintf("%032x", i/5),
			SpanID:            fmt.Sprintf("%016x", i),
			SpanName:          "op",
			ServiceName:       "svc",
		}
		if i%7 != 0 {
			rows[i].StartTimeUnixNano = schema.Int64Ptr(ts)
			rows[i].DurationNs = schema.Int64Ptr(int64(i%50) * 1000)
			rows[i].StatusCode = schema.Int32Ptr(int32(i % 3))
		}
		if i%11 == 0 {
			rows[i].StartTimeUnixNano = schema.Int64Ptr(0) // explicit zero is not NULL
			rows[i].SpanKind = schema.Int32Ptr(0)
		}
	}
	return rows
}

func writeTraceObject(t *testing.T, rows []schema.TraceRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := schema.NewTraceWriter(&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// The optional start_time_unix_nano column keeps DELTA_BINARY_PACKED, and no
// other column changes encoding: duration_ns, status.code and span.kind were
// plain before they became nullable and stay plain.
func TestTraceWriter_DeltaEncodingOnOptionalColumns(t *testing.T) {
	data := writeTraceObject(t, traceSpans(3000))
	encs := testutil.ColumnEncodings(t, data)

	for _, col := range []string{"timestamp_unix_nano", "start_time_unix_nano"} {
		if !testutil.HasEncoding(encs, col, deltaEnc) {
			t.Errorf("%s encodings %v lack %s", col, encs[col], deltaEnc)
		}
	}
	for col, e := range encs {
		if col == "timestamp_unix_nano" || col == "start_time_unix_nano" {
			continue
		}
		if slices.Contains(e, deltaEnc) {
			// string columns written DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY are other encodings;
			// DELTA_BINARY_PACKED is only for the two integer time columns.
			t.Errorf("%s unexpectedly %s: %v", col, deltaEnc, e)
		}
	}
	for _, col := range []string{"duration_ns", "status.code", "span.kind"} {
		if testutil.HasEncoding(encs, col, deltaEnc) {
			t.Errorf("%s must stay as before #274 (not delta): %v", col, encs[col])
		}
	}
}

// Without the helper (a plain generic writer on the struct) the column is plain:
// this documents the parquet-go limitation the helper exists for.
func TestTraceWriter_PlainStructWriterLosesDelta(t *testing.T) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf)
	if _, err := w.Write(traceSpans(100)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if testutil.HasEncoding(testutil.ColumnEncodings(t, buf.Bytes()), "start_time_unix_nano", deltaEnc) {
		t.Fatal("the struct tag path now produces delta on an optional column: NewTraceWriter's schema node can be retired")
	}
}

// The restored encoding is the whole point: start_time_unix_nano must cost what
// a required delta column costs (the pre-#274 layout), not the plain 8 bytes/row.
func TestTraceWriter_StartTimeSizeMatchesRequiredDelta(t *testing.T) {
	type required struct {
		Ts    int64 `parquet:"timestamp_unix_nano,delta"`
		Start int64 `parquet:"start_time_unix_nano,delta"`
	}
	const n = 5000
	var rb bytes.Buffer
	rw := parquet.NewGenericWriter[required](&rb)
	rows := make([]required, n)
	trows := make([]schema.TraceRow, n)
	for i := range rows {
		ts := int64(1700000000000000000) + int64(i)*1000
		rows[i] = required{ts, ts}
		trows[i] = schema.TraceRow{TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts)}
	}
	if _, err := rw.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := rw.Close(); err != nil {
		t.Fatal(err)
	}
	size := func(data []byte, col string) int64 {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range f.Metadata().RowGroups[0].Columns {
			if c.MetaData.PathInSchema[0] == col {
				return c.MetaData.TotalCompressedSize
			}
		}
		t.Fatalf("no column %s", col)
		return 0
	}
	want := size(rb.Bytes(), "start_time_unix_nano")
	got := size(writeTraceObject(t, trows), "start_time_unix_nano")
	// optional adds a definition-level run (all 1s: a few bytes); allow 5 % + 64 B.
	if got > want+want/20+64 {
		t.Fatalf("optional delta start_time column is %d B, required delta is %d B", got, want)
	}
	if got > int64(n) { // plain would be 8*n
		t.Fatalf("start_time column %d B for %d rows: not delta packed", got, n)
	}
}

// NULL, zero and non-zero all survive the schema node: the writer's schema
// differs from the struct's only in an encoding, so the #274 absent-vs-zero
// distinction must be untouched.
func TestTraceWriter_RoundTripKeepsNullZeroAndValue(t *testing.T) {
	in := traceSpans(1200)
	// Rows that exercise every pointer combination.
	in = append(in,
		schema.TraceRow{TimestampUnixNano: 5},
		schema.TraceRow{TimestampUnixNano: 6, StartTimeUnixNano: schema.Int64Ptr(0), DurationNs: schema.Int64Ptr(0), StatusCode: schema.Int32Ptr(0), SpanKind: schema.Int32Ptr(0)},
		schema.TraceRow{TimestampUnixNano: 7, StartTimeUnixNano: schema.Int64Ptr(-1), DurationNs: schema.Int64Ptr(-1), StatusCode: schema.Int32Ptr(-1), SpanKind: schema.Int32Ptr(-1)},
		schema.TraceRow{TimestampUnixNano: 8, StartTimeUnixNano: schema.Int64Ptr(1 << 62)},
	)
	data := writeTraceObject(t, in)
	out := make([]schema.TraceRow, len(in))
	r := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
	defer func() { _ = r.Close() }()
	n, _ := r.Read(out)
	if n != len(in) {
		t.Fatalf("read %d rows, want %d", n, len(in))
	}
	eq64 := func(a, b *int64) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }
	eq32 := func(a, b *int32) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }
	for i := range in {
		if !eq64(in[i].StartTimeUnixNano, out[i].StartTimeUnixNano) || !eq64(in[i].DurationNs, out[i].DurationNs) ||
			!eq32(in[i].StatusCode, out[i].StatusCode) || !eq32(in[i].SpanKind, out[i].SpanKind) {
			t.Fatalf("row %d: got start=%v dur=%v status=%v kind=%v, want %v %v %v %v", i,
				out[i].StartTimeUnixNano, out[i].DurationNs, out[i].StatusCode, out[i].SpanKind,
				in[i].StartTimeUnixNano, in[i].DurationNs, in[i].StatusCode, in[i].SpanKind)
		}
	}
}

// The writer schema must stay equal to the struct's under parquet.EqualNodes
// (the encoding is not part of node equality): that is what keeps parquet-go's
// direct Go-value write path instead of the slow per-row deconstruct path, and
// it keeps the physical column order.
func TestTraceWriterSchema_EqualsStructSchemaAndKeepsColumnOrder(t *testing.T) {
	ws := schema.TraceWriterSchema()
	ss := parquet.SchemaOf(new(schema.TraceRow))
	if !parquet.EqualNodes(ws, ss) {
		t.Fatal("writer schema differs from the struct schema beyond encodings")
	}
	wf, sf := ws.Fields(), ss.Fields()
	if len(wf) != len(sf) {
		t.Fatalf("field count %d vs %d", len(wf), len(sf))
	}
	for i := range wf {
		if wf[i].Name() != sf[i].Name() {
			t.Fatalf("column %d: %s vs %s", i, wf[i].Name(), sf[i].Name())
		}
	}
	if !slices.Equal(ws.Columns()[0], ss.Columns()[0]) || len(ws.Columns()) != len(ss.Columns()) {
		t.Fatal("leaf columns differ")
	}
}

// A file written before the column was nullable (REQUIRED INT64, delta) must
// still read into the pointer field, and an object written by the helper is read
// back by the plain struct reader (what the query path uses).
func TestTraceWriter_ReadsPreNullableObjects(t *testing.T) {
	type oldRow struct {
		Ts    int64 `parquet:"timestamp_unix_nano,delta"`
		Start int64 `parquet:"start_time_unix_nano,delta"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[oldRow](&buf)
	if _, err := w.Write([]oldRow{{1, 1}, {2, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out := make([]schema.TraceRow, 2)
	r := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(buf.Bytes()))
	defer func() { _ = r.Close() }()
	if n, _ := r.Read(out); n != 2 {
		t.Fatalf("read %d", n)
	}
	if out[0].StartTimeUnixNano == nil || *out[0].StartTimeUnixNano != 1 || out[1].StartTimeUnixNano == nil || *out[1].StartTimeUnixNano != 0 {
		t.Fatalf("pre-nullable object read back as %v %v", out[0].StartTimeUnixNano, out[1].StartTimeUnixNano)
	}
}

// Every production code path writes a TraceRow object through
// schema.NewTraceWriter. A direct parquet.NewGenericWriter[schema.TraceRow] in
// non-test code would write start_time_unix_nano plain and make that object
// 6-8 % larger than the others (flush, compaction and the delete rewriter are
// all rewrites of the same data: the encoding must not depend on who wrote it).
func TestTraceWriter_NoProductionPathBypassesTheHelper(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root && (strings.HasPrefix(d.Name(), ".") || d.Name() == "deps" || d.Name() == "node_modules" || d.Name() == "vendor") {
				return filepath.SkipDir // .git, .claude, .before-<ref> worktrees, the pinned upstream trees
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		s := string(b)
		// Any other way to build a Parquet file from rows or row groups would
		// also bypass the schema node: none exists in production code today, and
		// none may appear without going through (or extending) NewTraceWriter.
		for _, pat := range []string{
			"NewGenericWriter[schema.TraceRow]", "NewGenericWriter[TraceRow]",
			"parquet.NewWriter(", "parquet.NewSortingWriter", "parquet.NewGenericBuffer", "parquet.NewBuffer(",
			"parquet.WriteFile", "parquet.Write[", "parquet.CopyRows(", "parquet.MergeRowGroups(",
		} {
			if strings.Contains(s, pat) && !strings.HasSuffix(p, filepath.Join("internal", "schema", "trace_writer.go")) {
				rel, _ := filepath.Rel(root, p)
				offenders = append(offenders, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("write TraceRow objects with schema.NewTraceWriter, not a plain generic writer: %v", offenders)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "lakehouse-traces")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root not found")
		}
		dir = parent
	}
}

func BenchmarkTraceWriter_Flush(b *testing.B) {
	rows := traceSpans(20000)
	for _, mode := range []string{"struct", "helper"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				if mode == "struct" {
					w := parquet.NewGenericWriter[schema.TraceRow](&buf)
					_, _ = w.Write(rows)
					_ = w.Close()
				} else {
					w := schema.NewTraceWriter(&buf)
					_, _ = w.Write(rows)
					_ = w.Close()
				}
				b.SetBytes(int64(buf.Len()))
			}
		})
	}
}
