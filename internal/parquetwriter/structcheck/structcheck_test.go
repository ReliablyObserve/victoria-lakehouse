package structcheck_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter/structcheck"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func rows(n int, body bool) []schema.LogRow {
	out := make([]schema.LogRow, n)
	for i := range out {
		out[i] = schema.LogRow{
			TimestampUnixNano: 1_760_000_000_000_000_000 + int64(i)*1_000_000,
			ServiceName:       fmt.Sprintf("svc-%d", i%5),
			TraceID:           fmt.Sprintf("%032x", i),
		}
		if body {
			out[i].Body = fmt.Sprintf("processed request %d", i)
		}
	}
	return out
}

func write(t *testing.T, r []schema.LogRow, o parquetwriter.Options) *structcheck.Structure {
	t.Helper()
	res, err := parquetwriter.WriteLogs(r, o)
	if err != nil {
		t.Fatal(err)
	}
	s, err := structcheck.Describe(res.Data)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var base = parquetwriter.Options{RowGroupSize: 50, CompressionLevel: 3}

func TestIdenticalStructuresHaveNoDiff(t *testing.T) {
	a, b := write(t, rows(120, true), base), write(t, rows(120, true), base)
	if d := structcheck.Diff(a, b); len(d) != 0 {
		t.Fatalf("same rows, same options: %v", d)
	}
}

func TestDescribeReportsWhatTheFooterHolds(t *testing.T) {
	s := write(t, rows(120, true), base)
	if len(s.RowGroups) != 3 || s.NumRows != 120 {
		t.Fatalf("row groups %d, rows %d", len(s.RowGroups), s.NumRows)
	}
	if got := s.BloomColumns(); !contains(got, "trace_id") || !contains(got, "service.name") {
		t.Errorf("bloom columns %v lack trace_id / service.name", got)
	}
	for _, c := range s.RowGroups[0].Columns {
		if !c.ColumnIndex || !c.OffsetIndex {
			t.Errorf("column %s lacks the page index (column=%v offset=%v)", c.Path, c.ColumnIndex, c.OffsetIndex)
		}
	}
	if s.RowGroups[0].Columns[0].MinMax == "" {
		t.Error("no statistics on the first column")
	}
	if names := s.KeyNames(); !contains(names, "_bloom_body_rg_0") || !contains(names, schema.TraceIDHexMetaKey) {
		t.Errorf("footer keys %v", names)
	}
}

func TestDiffNamesEveryKindOfDifference(t *testing.T) {
	full := write(t, rows(120, true), base)

	t.Run("a dropped footer key", func(t *testing.T) {
		d := structcheck.Diff(full, write(t, rows(120, false), base))
		mustMention(t, d, `footer key "_bloom_body_rg_0": present -> MISSING`)
	})
	t.Run("a different row-group count", func(t *testing.T) {
		d := structcheck.Diff(full, write(t, rows(120, true), parquetwriter.Options{RowGroupSize: 60, CompressionLevel: 3}))
		mustMention(t, d, "row groups: 3 -> 2")
	})
	t.Run("a different bloom column set", func(t *testing.T) {
		slotted := rows(120, true)
		for i := range slotted {
			schema.SetLogSlot(&slotted[i], "ded_s01", fmt.Sprintf("tier-%d", i%4))
		}
		res := schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant.tier", Bloom: true}})
		with := write(t, slotted, parquetwriter.Options{RowGroupSize: 50, CompressionLevel: 3, Slots: res})
		without := write(t, slotted, base)
		d := structcheck.Diff(with, without)
		mustMention(t, d, `column "ded_s01": split-block bloom`)
		mustMention(t, d, `footer key "`+schema.DedicatedSlotsMetaKey+`": present -> MISSING`)
	})
	t.Run("different statistics", func(t *testing.T) {
		other := rows(120, true)
		other[3].ServiceName = "zzz-out-of-range"
		d := structcheck.Diff(full, write(t, other, base))
		mustMention(t, d, `column "service.name": statistics`)
	})
	t.Run("a different attestation value", func(t *testing.T) {
		on := parquetwriter.Options{RowGroupSize: 50, CompressionLevel: 3, TraceIDHex: true}
		d := structcheck.Diff(full, write(t, rows(120, true), on))
		mustMention(t, d, `footer key "`+schema.TraceIDHexMetaKey+`": value differs`)
	})
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func mustMention(t *testing.T, diff []string, want string) {
	t.Helper()
	for _, l := range diff {
		if strings.Contains(l, want) {
			return
		}
	}
	t.Errorf("diff %q does not mention %q", diff, want)
}

func TestDescribeRejectsGarbage(t *testing.T) {
	if _, err := structcheck.Describe([]byte("not parquet")); err == nil {
		t.Fatal("Describe accepted garbage")
	}
}

// Diff on hand-built structures covers the differences a writer pair can show that
// the two shared-writer outputs cannot be made to show.
func TestDiffOnBuiltStructures(t *testing.T) {
	col := func(path string) structcheck.Column {
		return structcheck.Column{Path: path, Type: "BYTE_ARRAY", Codec: "ZSTD", Encodings: []string{"PLAIN"}, MinMax: "aa", BloomBytes: 64, ColumnIndex: true, OffsetIndex: true}
	}
	base := func() *structcheck.Structure {
		return &structcheck.Structure{
			CreatedBy: "app 1", NumRows: 10, KeyValues: map[string]string{"k": "d1"},
			RowGroups: []structcheck.RowGroup{{NumRows: 10, Sorting: []string{"ts"}, Columns: []structcheck.Column{col("a"), col("b")}}},
		}
	}
	if d := structcheck.Diff(base(), base()); len(d) != 0 {
		t.Fatalf("equal structures: %v", d)
	}
	mutate := func(f func(s *structcheck.Structure)) []string {
		g := base()
		f(g)
		return structcheck.Diff(base(), g)
	}
	cases := []struct {
		name string
		f    func(s *structcheck.Structure)
		want string
	}{
		{"created_by", func(s *structcheck.Structure) { s.CreatedBy = "app 2" }, "created_by"},
		{"num_rows", func(s *structcheck.Structure) { s.NumRows = 11 }, "num_rows: 10 -> 11"},
		{"new footer key", func(s *structcheck.Structure) { s.KeyValues["extra"] = "d2" }, `footer key "extra": absent -> present`},
		{"changed footer value", func(s *structcheck.Structure) { s.KeyValues["k"] = "d9" }, `footer key "k": value differs`},
		{"row group rows", func(s *structcheck.Structure) { s.RowGroups[0].NumRows = 9 }, "row group 0: rows 10 -> 9"},
		{"sorting", func(s *structcheck.Structure) { s.RowGroups[0].Sorting = []string{"ts desc"} }, "sorting columns"},
		{"column count", func(s *structcheck.Structure) { s.RowGroups[0].Columns = s.RowGroups[0].Columns[:1] }, "columns 2 -> 1"},
		{"column path", func(s *structcheck.Structure) { s.RowGroups[0].Columns[1].Path = "z" }, `path "b" -> "z"`},
		{"type and codec", func(s *structcheck.Structure) { s.RowGroups[0].Columns[0].Codec = "SNAPPY" }, "type/codec"},
		{"encodings", func(s *structcheck.Structure) { s.RowGroups[0].Columns[0].Encodings = []string{"RLE"} }, "encodings"},
		{"statistics", func(s *structcheck.Structure) { s.RowGroups[0].Columns[0].NullCount = 3 }, "statistics"},
		{"bloom size", func(s *structcheck.Structure) { s.RowGroups[0].Columns[0].BloomBytes = 0 }, "split-block bloom 64 bytes -> 0 bytes"},
		{"page index", func(s *structcheck.Structure) { s.RowGroups[0].Columns[0].OffsetIndex = false }, "page index"},
	}
	for _, c := range cases {
		mustMention(t, mutate(c.f), c.want)
	}
}

func TestDescribeReportsTheSortOrder(t *testing.T) {
	r := rows(100, true)
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf, parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("timestamp_unix_nano"), parquet.Descending("service.name"))))
	if _, err := w.Write(r); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := structcheck.Describe(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(s.RowGroups[0].Sorting, ","); got != "timestamp_unix_nano,service.name desc" {
		t.Errorf("sorting = %q", got)
	}
	if got := s.BloomColumns(); len(got) != 0 {
		t.Errorf("a file written without blooms reports bloom columns %v", got)
	}
}

func TestBloomColumnsMarksAPartialColumn(t *testing.T) {
	s := &structcheck.Structure{RowGroups: []structcheck.RowGroup{
		{Columns: []structcheck.Column{{Path: "a", BloomBytes: 8}, {Path: "b", BloomBytes: 8}}},
		{Columns: []structcheck.Column{{Path: "a", BloomBytes: 8}, {Path: "b"}}},
	}}
	if got := strings.Join(s.BloomColumns(), ","); got != "a,b (partial)" {
		t.Errorf("BloomColumns = %q", got)
	}
}
