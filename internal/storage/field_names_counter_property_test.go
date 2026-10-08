package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// fnModelBlock is a generated block: its group (layer, stream, day), its row
// count and the columns it lists.
type fnModelBlock struct {
	buffer bool
	stream string
	day    string
	rows   int
	cols   []string
}

func (b fnModelBlock) block() *logstorage.DataBlock {
	cols := []logstorage.BlockColumn{
		{Name: "_stream_id", Values: repeat(b.stream, b.rows)},
		{Name: "_time", Values: repeat(b.day+"T10:00:00Z", b.rows)},
	}
	for _, c := range b.cols {
		cols = append(cols, logstorage.BlockColumn{Name: c, Values: repeat("v", b.rows)})
	}
	db := &logstorage.DataBlock{}
	db.SetColumns(cols)
	if b.buffer {
		return MarkFieldNamesBufferBlock(db)
	}
	return db
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}

func genFnBlocks(r *rand.Rand) []fnModelBlock {
	n := 1 + r.Intn(40)
	out := make([]fnModelBlock, n)
	names := []string{"a", "b", "c", "d", "e", "f"}
	for i := range out {
		b := fnModelBlock{buffer: r.Intn(4) == 0, stream: fmt.Sprintf("s%d", r.Intn(5)), day: fmt.Sprintf("2026-10-0%d", 1+r.Intn(3)), rows: 1 + r.Intn(9)}
		for _, c := range names {
			if r.Intn(2) == 0 {
				b.cols = append(b.cols, c)
			}
		}
		out[i] = b
	}
	return out
}

// fnReference is the rule written the plain way: group the blocks by (layer,
// stream, day), take the union of each group's columns and credit every column
// with the group's rows.
func fnReference(blocks []fnModelBlock) map[string]uint64 {
	type g struct {
		rows uint64
		cols map[string]bool
	}
	groups := map[string]*g{}
	for _, b := range blocks {
		k := fmt.Sprintf("%v|%s|%s", b.buffer, b.stream, b.day)
		if groups[k] == nil {
			groups[k] = &g{cols: map[string]bool{"_stream_id": true, "_time": true}}
		}
		groups[k].rows += uint64(b.rows)
		for _, c := range b.cols {
			groups[k].cols[c] = true
		}
	}
	out := map[string]uint64{}
	for _, gr := range groups {
		for c := range gr.cols {
			out[c] += gr.rows
		}
	}
	return out
}

// Property: the counter equals the plain rule for any generated blocks, in any
// order of arrival, and a stream cap only ever moves credit from a union to a
// block, never loses a column or a row.
func TestFieldNamesCounter_Property(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		r := rand.New(rand.NewSource(seed))
		blocks := genFnBlocks(r)
		want := fnReference(blocks)

		c := newFieldNamesCounter(1 << 10)
		for _, b := range blocks {
			c.add(0, b.block())
		}
		if got := fnHits(c.result()); !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d: hits = %v, want %v", seed, got, want)
		}

		r.Shuffle(len(blocks), func(i, j int) { blocks[i], blocks[j] = blocks[j], blocks[i] })
		d := newFieldNamesCounter(1 << 10)
		for _, b := range blocks {
			d.add(0, b.block())
		}
		if got := fnHits(d.result()); !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d reordered: hits = %v, want %v", seed, got, want)
		}

		// With a tiny cap every column of the reference is still listed, and no
		// column is credited with more rows than exist.
		total := uint64(0)
		for _, b := range blocks {
			total += uint64(b.rows)
		}
		e := newFieldNamesCounter(1)
		for _, b := range blocks {
			e.add(0, b.block())
		}
		got := fnHits(e.result())
		for col := range want {
			if got[col] == 0 {
				t.Fatalf("seed %d capped: column %q lost", seed, col)
			}
		}
		for col, h := range got {
			if h > total {
				t.Fatalf("seed %d capped: column %q credited %d of %d rows", seed, col, h, total)
			}
		}
	}
}

// FuzzFieldNamesCounter feeds arbitrary bytes as block shapes: it must never
// panic and must keep the counter equal to the plain rule.
func FuzzFieldNamesCounter(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var blocks []fnModelBlock
		names := []string{"a", "b", "c", "d"}
		for i := 0; i+3 < len(data) && len(blocks) < 64; i += 4 {
			b := fnModelBlock{buffer: data[i]&1 == 1, stream: fmt.Sprintf("s%d", data[i]>>1&3), day: fmt.Sprintf("2026-10-0%d", 1+data[i+1]&1), rows: 1 + int(data[i+2]&7)}
			for j, c := range names {
				if data[i+3]>>j&1 == 1 {
					b.cols = append(b.cols, c)
				}
			}
			blocks = append(blocks, b)
		}
		c := newFieldNamesCounter(1 << 10)
		for _, b := range blocks {
			c.add(0, b.block())
		}
		want := fnReference(blocks)
		if got := fnHits(c.result()); !(len(want) == 0 && len(got) == 0) && !reflect.DeepEqual(got, want) {
			t.Fatalf("hits = %v, want %v", got, want)
		}
	})
}

func BenchmarkFieldNamesCounterAdd(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	blocks := genFnBlocks(r)
	dbs := make([]*logstorage.DataBlock, len(blocks))
	for i, m := range blocks {
		dbs[i] = m.block()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := newFieldNamesCounter(1 << 18)
		for _, db := range dbs {
			c.add(0, db)
		}
		_ = c.result()
	}
}

func BenchmarkEmitFieldNamesStreamBlocks(b *testing.B) {
	const rows = 8192
	ids := make([]string, rows)
	vals := make([]string, rows)
	sparse := make([]string, rows)
	for i := range ids {
		ids[i] = fmt.Sprintf("s%d", i%64)
		vals[i] = "v"
		if i%7 == 0 {
			sparse[i] = "x"
		}
	}
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{{Name: "_stream_id", Values: ids}, {Name: "a", Values: vals}, {Name: "b", Values: sparse}, {Name: "c", Values: vals}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EmitFieldNamesStreamBlocks(db, func(*logstorage.DataBlock) {})
	}
}
