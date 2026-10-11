package storage

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// A block observed before the row filter contributes its columns to the stream
// and credits no row: the matching block of the stream then carries the credit.
func TestFieldNamesCounter_ObserveAddsColumnsAndNoRows(t *testing.T) {
	c := newFieldNamesCounter(10)
	c.observe(fnBlock("s1", 4, map[string]bool{"a": true, "b": true})) // no row of it matches
	if got := c.result(); got != nil {
		t.Fatalf("observed-only stream answered %v, want nothing", got)
	}
	c.add(0, fnBlock("s1", 2, map[string]bool{"a": true})) // the matching rows
	want := map[string]uint64{"_stream_id": 2, "a": 2, "b": 2}
	if got := fnHits(c.result()); !reflect.DeepEqual(got, want) {
		t.Fatalf("hits = %v, want %v", got, want)
	}
	// A stream with no matching row lists no name, however many columns it has.
	c.observe(fnBlock("s2", 3, map[string]bool{"c": true}))
	if got := fnHits(c.result()); !reflect.DeepEqual(got, want) {
		t.Fatalf("a stream without a matching row changed the answer: %v", got)
	}
	// Observing a nil/empty block, or one past the cap, is harmless.
	c.observe(nil)
	c.observe(&logstorage.DataBlock{})
	full := newFieldNamesCounter(1)
	full.observe(fnBlock("s1", 1, map[string]bool{"a": true}))
	full.observe(fnBlock("s2", 1, map[string]bool{"b": true}))
	if len(full.streams) != 1 {
		t.Fatalf("observe tracked %d streams past the cap 1", len(full.streams))
	}
}

func TestObserveFieldNamesBlock_NeedsACounterInTheContext(t *testing.T) {
	ObserveFieldNamesBlock(context.Background(), fnBlock("s1", 1, map[string]bool{"a": true})) // no counter: no effect, no panic
	if IsFieldNamesCounter(context.Background()) {
		t.Fatal("a plain context is not a counting read")
	}
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatal(err)
	}
	got, err := FieldNamesViaQuery(context.Background(), nil, q, func(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, wb logstorage.WriteDataBlockFunc) error {
		if !IsFieldNamesCounter(ctx) {
			t.Error("the read must be marked as counted")
		}
		ObserveFieldNamesBlock(ctx, fnBlock("s1", 5, map[string]bool{"a": true, "b": true})) // filtered out entirely
		wb(0, fnBlock("s1", 2, map[string]bool{"a": true}))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]uint64{"_stream_id": 2, "a": 2, "b": 2}; !reflect.DeepEqual(fnHits(got), want) {
		t.Fatalf("hits = %v, want %v", fnHits(got), want)
	}
}

// The stream cap tripping is counted, so an operator can see that an answer
// became per-block.
func TestFieldNamesCounter_StreamCapTrippingIsCounted(t *testing.T) {
	before := metrics.FieldNamesStreamCap.Get()
	c := newFieldNamesCounter(1)
	c.add(0, fnBlock("s1", 3, map[string]bool{"a": true}))
	if d := metrics.FieldNamesStreamCap.Get() - before; d != 0 {
		t.Fatalf("counted %d cap trips before the cap was full", d)
	}
	c.add(0, fnBlock("s2", 2, map[string]bool{"b": true}))
	c.add(0, fnBlock("s3", 2, map[string]bool{"b": true}))
	if d := metrics.FieldNamesStreamCap.Get() - before; d != 2 {
		t.Fatalf("counted %d cap trips, want 2 (one per block credited on its own)", d)
	}
	c.add(0, fnBlock("s1", 1, map[string]bool{"a": true})) // a known stream still unions
	if d := metrics.FieldNamesStreamCap.Get() - before; d != 2 {
		t.Fatalf("a tracked stream counted as a cap trip: %d", d)
	}
}

func TestFieldNamesBufferQuery_HasNoRowFilter(t *testing.T) {
	q, err := logstorage.ParseQuery("level:=WARN AND _msg:foo")
	if err != nil {
		t.Fatal(err)
	}
	all := FieldNamesBufferQuery(q, 100, 200)
	if all == nil {
		t.Fatal("no query built")
	}
	if s, e := all.GetFilterTimeRange(); s != 100 || e != 200 {
		t.Errorf("window = [%d, %d], want [100, 200]", s, e)
	}
	if !containsAll(all.String(), "_time") {
		t.Errorf("query = %q: want the window", all.String())
	}
	if containsAll(all.String(), "WARN") || containsAll(all.String(), "foo") {
		t.Errorf("the row filter survived: %q", all.String())
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		found := false
		for i := 0; i+len(p) <= len(s); i++ {
			if s[i:i+len(p)] == p {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// --- an oracle that does not share the counter's rule -----------------------
//
// The oracle works on ROWS, the way hot storage's blocks come to be: every row
// of a stream-day (and layer) belongs to one merged block, which lists a column
// when some row has a value for it; a field is credited with the block's rows
// that pass the filter. The counter is fed the same rows cut into objects
// (chunks) in any order, each chunk observed whole and then added filtered.

type oracleRow struct {
	buffer bool
	stream int
	day    int
	cols   uint8 // bit set over fnOracleNames
	match  bool
}

func fnOracleNames() []string { return []string{"a", "b", "c", "d"} }

func (r oracleRow) group() string { return fmt.Sprintf("%v|%d|%d", r.buffer, r.stream, r.day) }

// oracleHits is hot's merged-block answer over rows.
func oracleHits(rows []oracleRow) map[string]uint64 {
	type g struct {
		cols    uint8
		matched uint64
	}
	groups := map[string]*g{}
	for _, r := range rows {
		k := r.group()
		if groups[k] == nil {
			groups[k] = &g{}
		}
		groups[k].cols |= r.cols
		if r.match {
			groups[k].matched++
		}
	}
	out := map[string]uint64{}
	for _, gr := range groups {
		if gr.matched == 0 {
			continue
		}
		out["_stream_id"] += gr.matched
		out["_time"] += gr.matched
		for i, n := range fnOracleNames() {
			if gr.cols>>i&1 == 1 {
				out[n] += gr.matched
			}
		}
	}
	return out
}

// oracleUnmergedHits is hot's answer while a stream-day's batches are still
// separate blocks: each chunk is its own block.
func oracleUnmergedHits(chunks [][]oracleRow) map[string]uint64 {
	out := map[string]uint64{}
	for _, ch := range chunks {
		for n, h := range oracleHits(ch) {
			out[n] += h
		}
	}
	return out
}

// chunkBlock builds the DataBlock of a chunk of rows of one group: full lists
// every row; matching keeps the matching rows only (and, like a filtered block,
// only the columns some matching row has a value for).
func chunkBlocks(rows []oracleRow) (full, matching *logstorage.DataBlock) {
	var chunkCols uint8
	for _, r := range rows {
		chunkCols |= r.cols
	}
	build := func(rs []oracleRow) *logstorage.DataBlock {
		if len(rs) == 0 {
			return nil
		}
		cols := []logstorage.BlockColumn{
			{Name: "_stream_id", Values: repeat(fmt.Sprintf("s%d", rs[0].stream), len(rs))},
			{Name: "_time", Values: repeat(fmt.Sprintf("2026-10-0%dT10:00:00Z", 1+rs[0].day), len(rs))},
		}
		for i, n := range fnOracleNames() {
			vals := make([]string, len(rs))
			for j, r := range rs {
				if r.cols>>i&1 == 1 {
					vals[j] = "v"
				}
			}
			if chunkCols>>i&1 == 1 {
				cols = append(cols, logstorage.BlockColumn{Name: n, Values: vals})
			}
		}
		db := &logstorage.DataBlock{}
		db.SetColumns(cols)
		if rs[0].buffer {
			return MarkFieldNamesBufferBlock(db)
		}
		return db
	}
	var m []oracleRow
	for _, r := range rows {
		if r.match {
			m = append(m, r)
		}
	}
	return build(rows), build(m)
}

func genOracleChunks(r *rand.Rand) (rows []oracleRow, chunks [][]oracleRow) {
	groups := map[string][]oracleRow{}
	var order []string
	n := 1 + r.Intn(60)
	for i := 0; i < n; i++ {
		row := oracleRow{buffer: r.Intn(4) == 0, stream: r.Intn(3), day: r.Intn(2), cols: uint8(r.Intn(16)), match: r.Intn(3) != 0}
		k := row.group()
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], row)
		rows = append(rows, row)
	}
	for _, k := range order {
		gr := groups[k]
		for len(gr) > 0 { // cut the group into objects of 1..4 rows
			c := 1 + r.Intn(4)
			if c > len(gr) {
				c = len(gr)
			}
			chunks = append(chunks, gr[:c])
			gr = gr[c:]
		}
	}
	r.Shuffle(len(chunks), func(i, j int) { chunks[i], chunks[j] = chunks[j], chunks[i] })
	return rows, chunks
}

func checkOracle(t *testing.T, label string, rows []oracleRow, chunks [][]oracleRow, capacity int) {
	t.Helper()
	c := newFieldNamesCounter(capacity)
	for _, ch := range chunks {
		full, matching := chunkBlocks(ch)
		c.observe(full) // before the row filter, as RunQuery does
		c.add(0, matching)
	}
	got := fnHits(c.result())
	want := oracleHits(rows)
	if capacity >= 1<<10 {
		if (len(got) != 0 || len(want) != 0) && !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: counter = %v, hot (merged) = %v", label, got, want)
		}
		// While the batches are still separate blocks hot credits no more than the
		// merged answer: the counter is an upper bound of it, never below.
		for n, h := range oracleUnmergedHits(chunks) {
			if got[n] < h {
				t.Fatalf("%s: counter credits %q with %d, hot (unmerged) with %d", label, n, got[n], h)
			}
		}
		return
	}
	// A tiny cap moves credit from unions to blocks (upstream's per-block rule):
	// no name hot does not list appears, and nothing is credited beyond the
	// matching rows.
	var matched uint64
	for _, r := range rows {
		if r.match {
			matched++
		}
	}
	for n, h := range got {
		if _, ok := want[n]; !ok {
			t.Fatalf("%s capped: %q listed, hot does not list it", label, n)
		}
		if h > matched {
			t.Fatalf("%s capped: %q credited %d of %d matching rows", label, n, h, matched)
		}
	}
}

// Property: for rows of non-uniform shape, cut into objects at random, read in
// any order and filtered, the counter equals hot's merged-block answer.
func TestFieldNamesCounter_MatchesAnIndependentRowOracle(t *testing.T) {
	for seed := int64(0); seed < 400; seed++ {
		r := rand.New(rand.NewSource(seed))
		rows, chunks := genOracleChunks(r)
		checkOracle(t, fmt.Sprintf("seed %d", seed), rows, chunks, 1<<10)
		checkOracle(t, fmt.Sprintf("seed %d tiny cap", seed), rows, chunks, 1)
	}
}

func FuzzFieldNamesCounterFilteredLayout(f *testing.F) {
	f.Add([]byte{0, 1, 0, 3, 1, 1, 2, 0, 0, 5, 1, 0})
	f.Add([]byte{7, 7, 7, 7, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var rows []oracleRow
		groups := map[string][]oracleRow{}
		var order []string
		for i := 0; i+2 < len(data) && len(rows) < 64; i += 3 {
			row := oracleRow{buffer: data[i]&1 == 1, stream: int(data[i] >> 1 & 3), day: int(data[i+1] & 1), cols: data[i+2] & 15, match: data[i+1]&2 == 0}
			k := row.group()
			if _, ok := groups[k]; !ok {
				order = append(order, k)
			}
			groups[k] = append(groups[k], row)
			rows = append(rows, row)
		}
		var chunks [][]oracleRow
		for _, k := range order {
			gr := groups[k]
			for len(gr) > 0 {
				c := 1 + int(gr[0].cols&3)
				if c > len(gr) {
					c = len(gr)
				}
				chunks = append(chunks, gr[:c])
				gr = gr[c:]
			}
		}
		checkOracle(t, "fuzz", rows, chunks, 1<<10)
		checkOracle(t, "fuzz capped", rows, chunks, 1)
	})
}
