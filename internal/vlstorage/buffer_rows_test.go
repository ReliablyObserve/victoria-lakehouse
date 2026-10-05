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

// A block of the buffer lists every column any of its rows has, and a row
// without the field reads "" there. Upstream ignores empty-valued fields, so
// the row converted for Parquet must not carry the field at all (the ingest
// matrix found payload.n="" in a drained object for a row hot stores without
// payload.n).
func TestBufferRows_AbsentFieldIsNotWrittenEmpty(t *testing.T) {
	lr := logstorage.GetLogRows([]string{"svc"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	tenant := logstorage.TenantID{AccountID: 4401, ProjectID: 1}
	lr.MustAdd(tenant, 1000, []logstorage.Field{{Name: "_msg", Value: "with"}, {Name: "svc", Value: "a"}, {Name: "payload.n", Value: "7"}}, -1)
	lr.MustAdd(tenant, 2000, []logstorage.Field{{Name: "_msg", Value: "without"}, {Name: "svc", Value: "a"}}, -1)
	rows := rowsViaBuffer(t, lr)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for _, r := range rows {
		v, has := r.LogAttributes["payload.n"]
		switch r.Body {
		case "with":
			if v != "7" {
				t.Errorf("row with payload.n: got %q", v)
			}
		case "without":
			if has {
				t.Errorf("row without payload.n carries it as %q: %+v", v, r.LogAttributes)
			}
		default:
			t.Errorf("unexpected row %+v", r)
		}
	}
}
