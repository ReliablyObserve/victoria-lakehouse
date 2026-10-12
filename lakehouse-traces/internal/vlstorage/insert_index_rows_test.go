package vlstorage

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// VictoriaTraces sets a span's _time to its END time
// (app/vtinsert/opentelemetry: tsp.AddRow(int64(span.EndTimeUnixNano), ...)) and
// writes, for every trace, a trace_id_idx row whose _time is the trace's START
// time (app/vtinsert/insertutil/index_helper.go: lmp.AddRow(startTimestamp, ...)
// with the first field, trace_id_idx_stream, as the stream). OTLP times are
// unsigned: a value above 2^63 ns wraps to a negative int64.
//
// The index rows are not spans. They have their own counters, so
// lakehouse_insert_rows_dropped_total stays a count of spans.

const wrappedStart = int64(-5_000_000_000) // an OTLP start time of 2^64 - 5e9 ns, as int64

// indexBatch builds the rows VictoriaTraces sends for one trace: a span (its
// _time is the end time) and the trace's index row (its _time is the start
// time; trace_id_idx_stream is its first field and its stream).
func indexBatch(svc, id string, startNs, endNs int64) *logstorage.LogRows {
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	tenant := logstorage.TenantID{AccountID: 1, ProjectID: 2}
	lr.MustAdd(tenant, endNs, []logstorage.Field{
		{Name: "resource_attr:service.name", Value: svc}, {Name: "span_id", Value: id}, {Name: "trace_id", Value: "t-" + id},
	}, 1)
	lr.MustAdd(tenant, startNs, []logstorage.Field{
		{Name: otelpb.TraceIDIndexStreamName, Value: "7"}, {Name: "_msg", Value: "-"},
		{Name: otelpb.TraceIDIndexFieldName, Value: "t-" + id},
		{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: strconv.FormatInt(startNs, 10)},
		{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: strconv.FormatInt(endNs, 10)},
	}, 1)
	return lr
}

func idxDropped(kind string) uint64 { return metrics.VTInternalRowsDropped.Get(kind) }

// A span whose end time is from the epoch on is admitted, and its index row,
// stamped with a start time before the epoch (the wrapped OTLP value), is
// refused: it is counted apart (lakehouse_vt_internal_rows_dropped_total
// {kind="trace_id_idx_before_epoch"}), not as a span, and lakehouse_insert_rows_total
// counts the span only. The span itself is intact.
func TestVTInsertAdapter_SpanWithAWrappedStartIsAdmittedButItsIndexRowIsRefused(t *testing.T) {
	SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}
	now := time.Now().UnixNano()
	lr := indexBatch("api", "wrapped", wrappedStart, now)
	defer logstorage.PutLogRows(lr)

	idx0, span0, total0 := idxDropped("trace_id_idx_before_epoch"), metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp), metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got := strings.Join(buf.got(), ","); got != "wrapped" {
		t.Errorf("the buffer got spans %q, want the span", got)
	}
	if buf.rows != 1 {
		t.Errorf("the buffer got %d rows, want only the span (its index row is refused)", buf.rows)
	}
	if d := idxDropped("trace_id_idx_before_epoch") - idx0; d != 1 {
		t.Errorf("trace_id_idx_before_epoch rose by %d, want 1", d)
	}
	if d := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp) - span0; d != 0 {
		t.Errorf("an index row was counted as a refused span (%d)", d)
	}
	if d := metrics.InsertRowsTotal.Get() - total0; d != 1 {
		t.Errorf("lakehouse_insert_rows_total rose by %d, want 1 (the span)", d)
	}
}

// The reverse: a span whose END is before the epoch is refused and counted as a
// span, while its index row (start from the epoch on) is admitted: an index row
// without its span stays in the buffer, where it is dropped at the flush with
// the other index rows (they are never written to Parquet).
func TestVTInsertAdapter_SpanWithAnEndBeforeTheEpochIsRefusedItsIndexRowIsNot(t *testing.T) {
	SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}
	lr := indexBatch("api", "ends-early", 5, wrappedStart)
	defer logstorage.PutLogRows(lr)

	idx0, span0 := idxDropped("trace_id_idx_before_epoch"), metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp)
	a.MustAddRows(lr)

	if got := buf.got(); len(got) != 0 {
		t.Errorf("the buffer got spans %v, want none", got)
	}
	if buf.rows != 1 {
		t.Errorf("the buffer got %d rows, want only the index row", buf.rows)
	}
	if d := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp) - span0; d != 1 {
		t.Errorf("too_small_timestamp rose by %d, want 1 (the span)", d)
	}
	if d := idxDropped("trace_id_idx_before_epoch") - idx0; d != 0 {
		t.Errorf("trace_id_idx_before_epoch rose by %d, want 0", d)
	}
}

// Index rows are streams of the tenant's cardinality limit like any other
// (trace_id_idx_stream has up to 1024 values per tenant), but they are not
// spans: a refused one is counted in lakehouse_vt_internal_rows_dropped_total
// {kind="trace_id_idx_cardinality_limit"} and a refused span in
// lakehouse_insert_rows_dropped_total{reason="cardinality_limit"}.
func TestVTInsertAdapter_IndexRowsRefusedForTheLimitAreCountedApart(t *testing.T) {
	gate := &countingGate{deny: map[string]bool{
		`{` + otelpb.TraceIDIndexStreamName + `="7"}`: true,
		`{resource_attr:service.name="over"}`:         true,
	}}
	SetCardinalityGate(gate)
	t.Cleanup(func() { SetCardinalityGate(nil) })
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}
	now := time.Now().UnixNano()

	idx0, card0 := idxDropped("trace_id_idx_cardinality_limit"), cardinalityDropped()
	lr := indexBatch("ok", "k", now, now)
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	lr = indexBatch("over", "o", now, now)
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)

	if got := strings.Join(buf.got(), ","); got != "k" {
		t.Errorf("the buffer got spans %q, want only k", got)
	}
	if d := idxDropped("trace_id_idx_cardinality_limit") - idx0; d != 2 {
		t.Errorf("trace_id_idx_cardinality_limit rose by %d, want 2 (one index row per batch)", d)
	}
	if d := cardinalityDropped() - card0; d != 1 {
		t.Errorf("cardinality_limit rose by %d, want 1 (the span of the over-limit service)", d)
	}
}
