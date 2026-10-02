package parquets3

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// ingestTraceAt adds n spans of tenant to a single-store buffer, spread over
// [startNs, endNs).
func ingestTraceAt(t *testing.T, st *membuffer.Store, tenant logstorage.TenantID, startNs, endNs int64, n int) {
	t.Helper()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	step := (endNs - startNs) / int64(n)
	for i := 0; i < n; i++ {
		ts := startNs + int64(i)*step
		lr.MustAdd(tenant, ts, []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: fmt.Sprintf("t%d-%d", startNs, i)},
			{Name: "span_id", Value: fmt.Sprintf("s%d-%d", startNs, i)},
			{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", ts)},
		}, 1)
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}

// VictoriaTraces' trace_id_idx rows stay in the insert buffer, where hot
// VictoriaTraces also returns them for unflushed data, and are never written
// to Parquet: every span reaches Parquet, no index row does.
func TestSegments_TraceIDIndexRowsStayInTheBufferAndAreNotFlushed(t *testing.T) {
	e := newSegEnv(t)
	e.keep = vlstorage.FlushRowKeeper()
	const nSpans, nIdx = 200, 40
	base := hourAgo.Add(10 * time.Minute)
	e.ingest(segTenantA, base, nSpans)

	lrIdx := logstorage.GetLogRows([]string{"trace_id_idx_stream"}, nil, nil, nil, "")
	for i := 0; i < nIdx; i++ {
		lrIdx.MustAdd(segTenantA, base.Add(time.Duration(i)*time.Second).UnixNano(), []logstorage.Field{
			{Name: "trace_id_idx_stream", Value: fmt.Sprintf("%d", i%256)},
			{Name: "trace_id_idx", Value: fmt.Sprintf("trace-%d", i+1)},
		}, 1)
	}
	e.segs.MustAddRows(lrIdx)
	logstorage.PutLogRows(lrIdx)

	// In the buffer: spans and index rows alike.
	snap := e.segs.Snapshot()
	q, err := logstorage.ParseQueryAtTimestamp("*", time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), 0, time.Now().UnixNano())
	var mu sync.Mutex
	inBuffer := 0
	deadline := time.Now().Add(5 * time.Second)
	for inBuffer < nSpans+nIdx && time.Now().Before(deadline) {
		inBuffer = 0
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{segTenantA}, q, false, nil)
		if err := snap.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			inBuffer += db.RowsCount()
			mu.Unlock()
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	snap.Release()
	if inBuffer != nSpans+nIdx {
		t.Fatalf("the buffer holds %d rows; want %d spans + %d trace_id_idx rows", inBuffer, nSpans, nIdx)
	}

	e.seal()
	e.drainAll(e.flusher(1000))
	e.mustHaveExactly(nSpans, "spans flushed")
	if got := e.storedMsgs()[""]; got != 0 {
		t.Errorf("%d rows without a span id (trace_id_idx rows) were written to Parquet", got)
	}
	for _, k := range e.storedDataKeys() {
		if strings.Contains(k, "trace_id_idx") {
			t.Errorf("an index object: %s", k)
		}
	}
}
