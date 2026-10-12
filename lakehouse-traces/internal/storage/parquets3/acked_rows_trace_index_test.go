package parquets3

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// VictoriaTraces stamps a span with its END time and the trace's trace_id_idx
// row with its START time. An OTLP time above 2^63 ns wraps to a negative int64,
// so a span can have an end time from the epoch on and an index row before it:
// the span is admitted and its index row is refused when it is added. The index
// row is dropped at the flush anyway (the _trace_idx footer replaces it), and
// a trace-by-ID read is a search on trace_id, so the span must be found in the
// buffer and reach Parquet exactly once.

// traceRows runs a trace-by-ID query (trace_id:=id over all time) against the
// segments the buffer serves and returns how many rows it finds.
func (e *segEnv) traceRows(id string) int {
	e.t.Helper()
	e.segs.DebugFlush()
	q, err := logstorage.ParseQueryAtTimestamp(`trace_id:=`+strconv.Quote(id), math.MaxInt64)
	if err != nil {
		e.t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), math.MinInt64, math.MaxInt64)
	n := 0
	var mu sync.Mutex
	for _, g := range append(e.segs.Pending(), e.segs.Active()) {
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{segTenantA}, q, false, nil)
		if err := g.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			n += db.RowsCount()
			mu.Unlock()
		}); err != nil {
			e.t.Fatalf("query segment %d: %v", g.Seq(), err)
		}
	}
	return n
}

func TestPre1970Rows_SpanWithAWrappedStartIsFoundByTraceID(t *testing.T) {
	e := newSegEnv(t)
	const msg = "wrapped-span"
	wrappedStart := int64(-5_000_000_000) // OTLP start of 2^64 - 5e9 ns, as int64
	idxBefore := metrics.VTInternalRowsDropped.Get("trace_id_idx_before_epoch")
	e.sendViaAdapter(segTenantA, []adapterRow{
		{ts: hourAgo.UnixNano(), fields: ackFields("api", msg)},
		{ts: wrappedStart, streamLen: 1, fields: []logstorage.Field{
			{Name: otelpb.TraceIDIndexStreamName, Value: "7"}, {Name: "_msg", Value: "-"},
			{Name: otelpb.TraceIDIndexFieldName, Value: "trace-" + msg},
			{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: strconv.FormatInt(wrappedStart, 10)},
			{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: strconv.FormatInt(hourAgo.UnixNano(), 10)},
		}},
	})
	if d := metrics.VTInternalRowsDropped.Get("trace_id_idx_before_epoch") - idxBefore; d != 1 {
		t.Fatalf("trace_id_idx_before_epoch rose by %d, want 1", d)
	}
	if got := e.traceRows("trace-" + msg); got != 1 {
		t.Fatalf("a trace-by-ID query on the buffer finds %d rows of the trace, want its span", got)
	}
	if got := e.bufferMsgs(); len(got) != 1 || got[msg] != 1 {
		t.Fatalf("the buffer holds spans %v, want only %s", got, msg)
	}
	e.seal()
	e.drainAll(e.flusher(1000))
	if got := e.storedMsgs(); len(got) != 1 || got[msg] != 1 {
		t.Fatalf("Parquet holds spans %v, want %s once", got, msg)
	}
}
