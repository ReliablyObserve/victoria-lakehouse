package vlstorage

import (
	"context"
	"math"
	"sort"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// rowsViaBuffer is what the flusher sees for lr: the rows go into a real insert
// buffer (upstream logstorage), are read back with `*` and converted with
// DataBlockToLogRows — the path every Parquet row of the Lakehouse takes. Rows
// come back ordered by timestamp, then body.
func rowsViaBuffer(t *testing.T, lr *logstorage.LogRows) []schema.LogRow {
	t.Helper()
	st, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer st.Close()
	st.MustAddRows(lr)
	st.DebugFlush()

	tenants, err := st.GetTenantIDs(context.Background(), 0, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var out []schema.LogRow
	for _, tenant := range tenants {
		q, err := logstorage.ParseQueryAtTimestamp("*", math.MaxInt64)
		if err != nil {
			t.Fatal(err)
		}
		q = q.CloneWithTimeFilter(q.GetTimestamp(), 0, math.MaxInt64)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
		if err := st.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			rows := DataBlockToLogRows(db, tenant)
			mu.Lock()
			out = append(out, rows...)
			mu.Unlock()
		}); err != nil {
			t.Fatal(err)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].TimestampUnixNano != out[j].TimestampUnixNano {
			return out[i].TimestampUnixNano < out[j].TimestampUnixNano
		}
		return out[i].Body < out[j].Body
	})
	return out
}
