package storage

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func colNames(db *logstorage.DataBlock) string {
	var out []string
	for _, c := range db.GetColumns(false) {
		out = append(out, c.Name)
	}
	return strings.Join(out, ",")
}

func block(cols ...logstorage.BlockColumn) *logstorage.DataBlock {
	db := &logstorage.DataBlock{}
	db.SetColumns(cols)
	return db
}

func col(name string, values ...string) logstorage.BlockColumn {
	return logstorage.BlockColumn{Name: name, Values: values}
}

// The order is upstream's: the four special columns, then the columns with one
// value in every row by name, then the rest by name; values move with their
// column.
func TestOrderColumnsLikeUpstream_Order(t *testing.T) {
	long := strings.Repeat("x", maxConstColumnValueSize+1)
	db := block(
		col("zeta", "a", "b"),
		col("_msg", "m1", "m2"),
		col("svc", "web", "web"),
		col("big", long, long), // too long for a const column upstream
		col("_stream", `{svc="web"}`, `{svc="web"}`),
		col("alpha", "1", "2"),
		col("_time", "t", "t"),
		col("env", "prod", "prod"),
		col("_stream_id", "s1", "s2"),
	)
	OrderColumnsLikeUpstream(db)
	if got, want := colNames(db), "_time,_stream_id,_stream,_msg,env,svc,alpha,big,zeta"; got != want {
		t.Fatalf("order %s, want %s", got, want)
	}
	if c := db.GetColumnByName("zeta"); c.Values[1] != "b" {
		t.Fatalf("values did not move with their column: %v", c.Values)
	}
}

// Whatever order a block arrives in, the result is the same (the map-iteration
// order cold blocks used to have, #427).
func TestOrderColumnsLikeUpstream_DeterministicOverPermutations(t *testing.T) {
	base := []logstorage.BlockColumn{
		col("_time", "t", "t"), col("_stream_id", "a", "b"), col("_stream", "x", "y"),
		col("_msg", "m", "n"), col("svc", "1", "2"), col("level", "info", "info"),
		col("trace_id", "", "z"), col("severity_number", "9", "9"),
	}
	var want string
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		cols := append([]logstorage.BlockColumn(nil), base...)
		r.Shuffle(len(cols), func(a, b int) { cols[a], cols[b] = cols[b], cols[a] })
		db := block(cols...)
		OrderColumnsLikeUpstream(db)
		got := colNames(db)
		if i == 0 {
			want = got
		} else if got != want {
			t.Fatalf("permutation %d gave %s, want %s", i, got, want)
		}
	}
	if want != "_time,_stream_id,_stream,_msg,level,severity_number,svc,trace_id" {
		t.Fatalf("order %s", want)
	}
}

func TestOrderColumnsLikeUpstream_EdgeCases(t *testing.T) {
	OrderColumnsLikeUpstream(nil)
	one := block(col("b", "1"))
	OrderColumnsLikeUpstream(one)
	if colNames(one) != "b" {
		t.Fatal(colNames(one))
	}
	// A single-row block: every column is const upstream.
	db := block(col("z", "1"), col("_msg", "m"), col("a", "2"))
	OrderColumnsLikeUpstream(db)
	if got := colNames(db); got != "_msg,a,z" {
		t.Fatalf("single row: %s", got)
	}
	// A column with no values counts as const, like upstream's.
	db = block(col("b", "1", "2"), col("a"))
	OrderColumnsLikeUpstream(db)
	if got := colNames(db); got != "a,b" {
		t.Fatalf("empty column: %s", got)
	}
	// More columns than the rank buffer holds.
	var cols []logstorage.BlockColumn
	for i := 99; i >= 0; i-- {
		cols = append(cols, col(fmt.Sprintf("f%03d", i), "1", fmt.Sprint(i%2)))
	}
	db = block(cols...)
	OrderColumnsLikeUpstream(db)
	got := db.GetColumns(false)
	for i := 1; i < len(got); i++ {
		gi, gp := isConstColumn(got[i].Values), isConstColumn(got[i-1].Values)
		if gp == gi && got[i-1].Name > got[i].Name || !gp && gi {
			t.Fatalf("100 columns out of order at %d: %s then %s", i, got[i-1].Name, got[i].Name)
		}
	}
}

// Running it twice changes nothing (the already-ordered fast path).
func TestOrderColumnsLikeUpstream_Idempotent(t *testing.T) {
	db := block(col("x", "1", "2"), col("_time", "t", "t"), col("a", "c", "c"))
	OrderColumnsLikeUpstream(db)
	first := colNames(db)
	OrderColumnsLikeUpstream(db)
	if colNames(db) != first {
		t.Fatalf("second pass changed %s to %s", first, colNames(db))
	}
}

func benchBlock(rows, ncols int, shuffled bool) []logstorage.BlockColumn {
	names := []string{"_time", "_stream_id", "_stream", "_msg"}
	for i := 0; len(names) < ncols; i++ {
		names = append(names, fmt.Sprintf("attr.%02d", i))
	}
	cols := make([]logstorage.BlockColumn, len(names))
	for i, n := range names {
		vals := make([]string, rows)
		for r := range vals {
			if i%3 == 0 {
				vals[r] = "const-value" // const: scanned to the end
			} else {
				vals[r] = fmt.Sprintf("v%d", r)
			}
		}
		cols[i] = col(n, vals...)
	}
	if shuffled {
		rand.New(rand.NewSource(2)).Shuffle(len(cols), func(a, b int) { cols[a], cols[b] = cols[b], cols[a] })
	}
	return cols
}

// BenchmarkOrderColumnsLikeUpstream: one 8192-row block (the cold reader's
// block size) with 30 columns, a third of them const, arriving shuffled.
func BenchmarkOrderColumnsLikeUpstream(b *testing.B) {
	src := benchBlock(8192, 30, true)
	cols := make([]logstorage.BlockColumn, len(src))
	db := &logstorage.DataBlock{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(cols, src)
		db.SetColumns(cols)
		OrderColumnsLikeUpstream(db)
	}
}

// BenchmarkOrderColumnsLikeUpstream_Ordered: the same block already in order.
func BenchmarkOrderColumnsLikeUpstream_Ordered(b *testing.B) {
	db := &logstorage.DataBlock{}
	db.SetColumns(benchBlock(8192, 30, true))
	OrderColumnsLikeUpstream(db)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		OrderColumnsLikeUpstream(db)
	}
}
