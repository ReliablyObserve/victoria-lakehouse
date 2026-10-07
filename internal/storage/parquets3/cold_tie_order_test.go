package parquets3

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// #427 (parity divergence B8): a sort over all columns (`sort`, `first N`,
// `last N` without `by`) keys each row on its columns in block order. Cold
// blocks listed their columns in map-iteration order, different on every
// read, so rows sharing a _time came back in a random order and a limit kept
// random rows. Cold blocks now list their columns in upstream's order: the
// answer is the one the insert buffer (upstream's own engine, the hot
// reference) gives, on every read.
func TestColdTieOrder_MatchesUpstreamOnEveryRead(t *testing.T) {
	e := newRestartEnv(t)
	at := time.Now().Add(-97 * time.Minute).Truncate(time.Millisecond)
	lr := logstorage.GetLogRows([]string{"svc"}, nil, nil, nil, "")
	for i := 1; i <= 6; i++ {
		lr.MustAdd(logstorage.TenantID{}, at.UnixNano(), []logstorage.Field{
			{Name: "svc", Value: fmt.Sprintf("svc-%d", i)},
			{Name: "_msg", Value: fmt.Sprintf("tie zz%d", 7-i)},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()

	from, to := at.Add(-time.Minute), at.Add(time.Minute)
	answer := func(queryStr string) string {
		t.Helper()
		q, err := logstorage.ParseQueryAtTimestamp(queryStr, to.UnixNano())
		if err != nil {
			t.Fatalf("parse %q: %v", queryStr, err)
		}
		q.AddTimeFilter(from.UnixNano(), to.UnixNano())
		ids := []logstorage.TenantID{{}}
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, ids, q, false, nil)
		var mu sync.Mutex
		var keys []string
		wb := func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			if c := db.GetColumnByName("_msg"); c != nil {
				for _, v := range c.Values {
					keys = append(keys, strings.Clone(v))
				}
			}
		}
		searchFn := func(w logstorage.WriteDataBlockFunc) error {
			return e.s.RunQuery(context.Background(), ids, q, w)
		}
		if err := logstorage.RunQueryExternal(qctx, searchFn, wb); err != nil {
			t.Fatalf("RunQueryExternal(%q): %v", queryStr, err)
		}
		return strings.Join(keys, ",")
	}

	queries := []string{"* | first 2", "* | last 2", "* | sort | limit 2", "* | sort | limit 6", "* | sort desc | limit 3"}
	ref := map[string]string{}
	for _, q := range queries {
		ref[q] = answer(q)
		if ref[q] == "" {
			t.Fatalf("buffer answered %q with no rows", q)
		}
	}

	e.flush()
	e.reap()
	if st := e.segs.Stats(time.Now()); st.Committed+st.Pending != 0 {
		t.Fatalf("fixture: the rows must be read from Parquet only, buffer still holds %+v", st)
	}
	if len(e.objects()) == 0 {
		t.Fatal("fixture: nothing was flushed")
	}
	for i := 0; i < 50; i++ {
		for _, q := range queries {
			if got := answer(q); got != ref[q] {
				t.Fatalf("read %d of %q from Parquet: %s, want %s (upstream's order, from the buffer)", i, q, got, ref[q])
			}
		}
	}
	t.Logf("50 reads of %d queries matched upstream: %v", len(queries), ref)
}
