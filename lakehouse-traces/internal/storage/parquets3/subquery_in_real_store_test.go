package parquets3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// TestRealStore_ResolvedInSubqueryWithManyValues runs a LogsQL query whose in()
// filter is a subquery resolving to ~600 values through the REAL parquets3
// Storage (parquet files in a mock S3, footer/bloom/column pushdown and all),
// reached the way the traces binary reaches it: VictoriaTraces'
// vtstorage.RunQuery -> the vtstorage adapter -> initSubqueries -> Storage.
// The fakes used elsewhere evaluate MatchRow in memory; this one proves the
// resolved value list survives the store's own query handling (String() round
// trip, filter extraction, pushdown) and the answer is exact, including that a
// second service's traces are NOT pulled in.
func TestRealStore_ResolvedInSubqueryWithManyValues(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	const apiTraces, otherTraces, spansPer = 600, 100, 2
	buildParityTraceFile(t, s, mock, "api", base, apiTraces, spansPer, true, "api")
	buildParityTraceFile(t, s, mock, "other", base, otherTraces, spansPer, true, "other")

	vtstorageadapter.Init(s)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })

	start, end := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	run := func(query string) (rows int, n string) {
		q := mustParseQueryWithTime(t, query, start, end)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		err := vtstorage.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			rows += db.RowsCount()
			for _, c := range db.GetColumns(false) {
				if c.Name == "n" && len(c.Values) > 0 {
					n = c.Values[0]
				}
			}
		})
		if err != nil {
			t.Fatalf("RunQuery(%s): %v", query, err)
		}
		return rows, n
	}

	sub := `_stream:{resource_attr:service.name="api"} | fields trace_id`
	started := time.Now()
	_, n := run(`trace_id:in(` + sub + `) | stats count() n`)
	elapsed := time.Since(started)
	if want := fmt.Sprint(apiTraces * spansPer); n != want {
		t.Fatalf("in() with %d resolved values: count = %q, want %s", apiTraces, n, want)
	}
	t.Logf("resolved in() with %d values over the real store: %s", apiTraces, elapsed)

	// The same query without pipes (rows, not a count) - the plain in() shape.
	rows, _ := run(`trace_id:in(` + sub + `)`)
	if rows != apiTraces*spansPer {
		t.Fatalf("no-pipes in(): %d rows, want %d", rows, apiTraces*spansPer)
	}

	// Inverse check: a subquery that matches only the small service.
	_, n = run(`trace_id:in(_stream:{resource_attr:service.name="other"} | fields trace_id) | stats count() n`)
	if want := fmt.Sprint(otherTraces * spansPer); n != want {
		t.Fatalf("other service: count = %q, want %s", n, want)
	}
}
