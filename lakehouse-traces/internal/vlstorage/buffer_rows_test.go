package vlstorage

import (
	"context"
	"math"
	"sort"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// rowsViaBuffer is what the flusher sees for lr: the spans go into a real
// insert buffer (upstream logstorage), are read back with `*` and converted
// with DataBlockToTraceRows — the path every Parquet row of the Lakehouse takes.
// Rows come back ordered by timestamp, then span id.
func rowsViaBuffer(t *testing.T, lr *logstorage.LogRows) []schema.TraceRow {
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
	var out []schema.TraceRow
	for _, tenant := range tenants {
		q, err := logstorage.ParseQueryAtTimestamp("*", math.MaxInt64)
		if err != nil {
			t.Fatal(err)
		}
		q = q.CloneWithTimeFilter(q.GetTimestamp(), 0, math.MaxInt64)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
		if err := st.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			rows := DataBlockToTraceRows(db, tenant)
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
		return out[i].SpanID < out[j].SpanID
	})
	return out
}

// The traces twin: a span without an attribute that another span of its block
// has is converted without it, not with "" (upstream ignores empty fields).
func TestBufferRows_AbsentSpanAttributeIsNotWrittenEmpty(t *testing.T) {
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	tenant := logstorage.TenantID{AccountID: 4401, ProjectID: 1}
	lr.MustAdd(tenant, 1000, []logstorage.Field{{Name: "resource_attr:service.name", Value: "svc"}, {Name: "trace_id", Value: "t1"}, {Name: "span_id", Value: "s1"}, {Name: "span_attr:payload.n", Value: "7"}}, -1)
	lr.MustAdd(tenant, 2000, []logstorage.Field{{Name: "resource_attr:service.name", Value: "svc"}, {Name: "trace_id", Value: "t2"}, {Name: "span_id", Value: "s2"}}, -1)
	rows := rowsViaBuffer(t, lr)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for _, r := range rows {
		v, has := r.SpanAttributes["payload.n"]
		switch r.SpanID {
		case "s1":
			if v != "7" {
				t.Errorf("span with payload.n: got %q (%+v)", v, r.SpanAttributes)
			}
		case "s2":
			if has {
				t.Errorf("span without payload.n carries it as %q: %+v", v, r.SpanAttributes)
			}
		default:
			t.Errorf("unexpected span %+v", r)
		}
	}
}
