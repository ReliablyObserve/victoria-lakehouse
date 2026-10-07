package parquets3

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// The cold column order is checked against upstream's own engine: the insert
// buffer is an upstream logstorage instance, so what it answers is the hot
// reference (#427). The same queries are then answered from Parquet only, and
// the column order of every block, and the key order of every JSON row
// (upstream's appendJSONRow writes the columns of a block in order and skips
// the empty ones), must be the buffer's.

// colAnswer is what one query returned: the distinct column orders of its
// blocks, and the distinct key orders of its JSON rows.
type colAnswer struct {
	blockCols map[string]bool
	rowKeys   map[string]bool
}

func (a colAnswer) String() string {
	return fmt.Sprintf("blocks %v rows %v", orderKeys(a.blockCols), orderKeys(a.rowKeys))
}

func orderKeys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// knownColdOnlyColumns are the fields the cold path returns and the buffer
// does not. Dropping them is the only normalisation the comparison applies.
//
// severity_number: open issue #274, cold rows carry an extra severity_number
// field that the hot engine does not have. Nothing else is ignored.
var knownColdOnlyColumns = map[string]bool{"severity_number": true}

func runColumnOrderQuery(t *testing.T, e *restartEnv, queryStr string, from, to time.Time, hot bool) colAnswer {
	t.Helper()
	q, err := logstorage.ParseQueryAtTimestamp(queryStr, to.UnixNano())
	if err != nil {
		t.Fatalf("parse %q: %v", queryStr, err)
	}
	q.AddTimeFilter(from.UnixNano(), to.UnixNano())
	ids := []logstorage.TenantID{{}}
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, ids, q, false, nil)
	ans := colAnswer{blockCols: map[string]bool{}, rowKeys: map[string]bool{}}
	var mu sync.Mutex
	wb := func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		cols := db.GetColumns(false)
		var names []string
		for _, c := range cols {
			if knownColdOnlyColumns[c.Name] {
				t.Logf("%q: ignoring cold-only %s (open issue #274)", queryStr, c.Name)
				continue
			}
			if hot && c.Name == "_msg" && allEmpty(c.Values) {
				// A hot trace block carries an empty _msg column that cold
				// trace blocks do not; an all-empty column is skipped by
				// appendJSONRow, so it is invisible to the client.
				continue
			}
			names = append(names, c.Name)
		}
		ans.blockCols[strings.Join(names, ",")] = true
		for r := 0; r < db.RowsCount(); r++ {
			var keys []string
			for _, c := range cols {
				if knownColdOnlyColumns[c.Name] || c.Values[r] == "" {
					continue
				}
				keys = append(keys, c.Name)
			}
			ans.rowKeys[strings.Join(keys, ",")] = true
		}
	}
	searchFn := func(w logstorage.WriteDataBlockFunc) error {
		return e.s.RunQuery(context.Background(), ids, q, w)
	}
	if err := logstorage.RunQueryExternal(qctx, searchFn, wb); err != nil {
		t.Fatalf("RunQueryExternal(%q): %v", queryStr, err)
	}
	return ans
}

// ingestColumnOrderRows adds rows of one or several streams, with const
// columns (level, region, a 300-byte value that is too long to be const), a
// column missing from some rows, and columns whose names sort before and after
// the others.
func ingestColumnOrderRows(e *restartEnv, at time.Time, streams int, from, to int) {
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	long := strings.Repeat("L", 300)
	for i := from; i < to; i++ {
		fields := []logstorage.Field{
			{Name: "resource_attr:service.name", Value: fmt.Sprintf("svc-%d", i%streams)},
			{Name: "trace_id", Value: fmt.Sprintf("%032x", 0xc0de00+i)},
			{Name: "span_id", Value: fmt.Sprintf("%016x", i+1)},
			{Name: "name", Value: "op"},
			{Name: "kind", Value: "2"},
			{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", at.UnixNano()+int64(i))},
			{Name: "duration", Value: fmt.Sprintf("%d", 1000+i)},
			{Name: "status_code", Value: "1"},
			{Name: "span_attr:long_const", Value: long},
			{Name: "span_attr:zeta", Value: fmt.Sprintf("z%d", i)},
			{Name: "span_attr:alpha", Value: fmt.Sprintf("a%d", i)},
		}
		if i%2 == 0 {
			fields = append(fields, logstorage.Field{Name: "span_attr:sometimes", Value: "yes"})
		}
		lr.MustAdd(logstorage.TenantID{}, at.Add(time.Duration(i/4)*time.Millisecond).UnixNano(), fields, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()
}

var columnOrderQueries = []string{
	"*",
	"* | sort | limit 12",
	"* | sort desc | limit 5",
	"* | first 3",
	`"span_attr:zeta":z1* | sort | limit 12`,
	`name:=op "span_attr:alpha":~a`,
}

// columnOrderLayers answers every query on the same rows in each data layer:
// "buffer" (upstream's engine, the reference), "cold" (flushed, Parquet
// only), "restart" (a new pod over the same bucket) and "compacted" (the
// segment objects merged into compacted-L1).
func columnOrderLayers(t *testing.T, streams int) map[string]map[string]colAnswer {
	t.Helper()
	e := newRestartEnv(t)
	at := time.Now().Add(-97 * time.Minute).Truncate(time.Millisecond)
	from, to := at.Add(-time.Minute), at.Add(time.Minute)
	run := func(hot bool) map[string]colAnswer {
		out := map[string]colAnswer{}
		for _, q := range columnOrderQueries {
			out[q] = runColumnOrderQuery(t, e, q, from, to, hot)
			if len(out[q].blockCols) == 0 {
				t.Fatalf("%q answered with no rows", q)
			}
		}
		return out
	}
	layers := map[string]map[string]colAnswer{}

	ingestColumnOrderRows(e, at, streams, 0, 6)
	e.flush() // segment 1, committed
	ingestColumnOrderRows(e, at, streams, 6, 12)
	layers["buffer"] = run(true)
	e.flush() // segment 2, committed
	commit := time.Now()
	markers := e.segmentMarkers(commit)
	e.reap()
	if st := e.segs.Stats(time.Now()); st.Committed+st.Pending != 0 {
		t.Fatalf("fixture: the rows must be read from Parquet only, buffer still holds %+v", st)
	}
	if len(e.objects()) < 2 {
		t.Fatal("fixture: want two flushed objects")
	}
	layers["cold"] = run(false)

	e.restart(true, true, true)
	layers["restart"] = run(false)

	const grace = time.Minute // restartEnv's flusher grace
	if n := e.compactSegmentObjects(markers, 2*grace, commit.Add(3*grace)); n < 2 {
		t.Fatalf("fixture: compaction merged %d objects, want at least 2", n)
	}
	layers["compacted"] = run(false)
	return layers
}

func allEmpty(vs []string) bool {
	for _, v := range vs {
		if v != "" {
			return false
		}
	}
	return true
}

func sameOrderSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// One stream: every column of a hot block is const or not for the whole block,
// exactly as in a cold row group, so cold must list the columns and write the
// JSON keys in the buffer's order.
func TestColdColumnOrder_SingleStreamMatchesBufferEngine(t *testing.T) {
	layers := columnOrderLayers(t, 1)
	for _, layer := range []string{"cold", "restart", "compacted"} {
		t.Run(layer, func(t *testing.T) { checkSingleStreamOrder(t, layers["buffer"], layers[layer]) })
	}
}

func checkSingleStreamOrder(t *testing.T, hot, cold map[string]colAnswer) {
	t.Helper()
	for _, q := range columnOrderQueries {
		if !sameOrderSet(hot[q].blockCols, cold[q].blockCols) {
			t.Errorf("%q: cold column order differs from the buffer's (upstream engine)\n hot:  %s\n cold: %s", q, hot[q], cold[q])
		}
		if !sameOrderSet(hot[q].rowKeys, cold[q].rowKeys) {
			t.Errorf("%q: cold JSON key order differs from the buffer's\n hot:  %v\n cold: %v", q, orderKeys(hot[q].rowKeys), orderKeys(cold[q].rowKeys))
		}
	}
	// The fixture is meaningful: const columns come before the non-const ones
	// that sort ahead of them, and the 300-byte column is not const.
	for blk := range hot["*"].blockCols {
		const prefix = "_time,_stream_id,_stream,kind,name,resource_attr:service.name,status_code,duration,span_attr:alpha,"
		if !strings.HasPrefix(blk, prefix) {
			t.Errorf("buffer block %q does not start with %q: the fixture no longer shows const-first ordering", blk, prefix)
		}
		if !strings.Contains(blk, ",span_attr:alpha,span_attr:long_const,") {
			t.Errorf("buffer block %q: long_const (300 bytes) must be among the non-const columns by name", blk)
		}
	}
}

// Several streams in one cold row group (open issue #452). Upstream decides constness per block
// of one stream, so its stream field (svc) is const and sits in the const
// group; across a mixed cold row group it varies and lands in the rest group.
// This asserts today's documented difference (parity-and-gaps B9, #452: field order
// can differ from hot when a cold block spans several streams; rows and tie
// order are unaffected) so a future change to it is noticed.
func TestColdColumnOrder_MultiStreamDocumentedDifference(t *testing.T) {
	layers := columnOrderLayers(t, 3)
	for _, layer := range []string{"cold", "restart", "compacted"} {
		t.Run(layer, func(t *testing.T) { checkMultiStreamOrder(t, layers["buffer"], layers[layer]) })
	}
}

func checkMultiStreamOrder(t *testing.T, hot, cold map[string]colAnswer) {
	t.Helper()
	const hotConst = "_time,_stream_id,_stream,kind,name,resource_attr:service.name,status_code,duration,span_attr:alpha,"
	for blk := range hot["*"].blockCols {
		if !strings.HasPrefix(blk, hotConst) {
			t.Errorf("buffer block %q: the stream field is const per stream, so it must sit in the const group (%q)", blk, hotConst)
		}
	}
	for blk := range cold["*"].blockCols {
		const coldPrefix = "_time,_stream_id,_stream,kind,name,status_code,duration,resource_attr:service.name,span_attr:alpha,"
		if !strings.HasPrefix(blk, coldPrefix) || !strings.Contains(blk, ",resource_attr:service.name,") {
			t.Errorf("cold block %q: the stream field varies across the mixed row group, so it must be in the rest group after alpha (%q)", blk, coldPrefix)
		}
		if strings.HasPrefix(blk, hotConst) {
			t.Errorf("cold block %q now orders like hot: the documented multi-stream difference is gone, close #452, update docs/parity-and-gaps.md (B9), the expect: differ registry rows and this test", blk)
		}
	}
	// Rows and their values are unaffected: the same JSON keys (as a set) per row.
	for _, q := range columnOrderQueries {
		hs, cs := map[string]bool{}, map[string]bool{}
		for k := range hot[q].rowKeys {
			hs[sortedKeyList(k)] = true
		}
		for k := range cold[q].rowKeys {
			cs[sortedKeyList(k)] = true
		}
		if !sameOrderSet(hs, cs) {
			t.Errorf("%q: the set of fields per row differs between buffer and cold: %v vs %v", q, orderKeys(hs), orderKeys(cs))
		}
	}
}

func sortedKeyList(s string) string {
	parts := strings.Split(s, ",")
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
