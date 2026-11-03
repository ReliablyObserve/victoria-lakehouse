package structcheck_test

import (
	"fmt"
	"strings"
	"testing"

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
