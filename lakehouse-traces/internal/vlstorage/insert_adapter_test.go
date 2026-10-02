package vlstorage

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// recordingBuffer is a BufferStore that keeps what it was given (copied: the
// caller reuses the LogRows) and can be read-only or panic.
type recordingBuffer struct {
	mu       sync.Mutex
	spans    []string
	rows     int
	batches  int
	readOnly bool
	panicMsg string
}

func (b *recordingBuffer) MustAddRows(lr *logstorage.LogRows) {
	if b.panicMsg != "" {
		panic(b.panicMsg)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batches++
	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		b.rows++
		for _, f := range r.Fields {
			if f.Name == "span_id" {
				b.spans = append(b.spans, strings.Clone(f.Value))
			}
		}
	})
}

func (b *recordingBuffer) IsReadOnly() bool { return b.readOnly }

func (b *recordingBuffer) got() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.spans...)
}

// fakeGate denies exactly the streams in deny; allows everything else.
type fakeGate struct{ deny map[string]bool }

func (g fakeGate) AllowStream(_, _ uint32, stream string) bool { return !g.deny[stream] }

func spanBatch(svc string, ids ...string) *logstorage.LogRows {
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for _, id := range ids {
		lr.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, time.Now().UnixNano(), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: svc},
			{Name: "trace_id", Value: "t-" + id},
			{Name: "span_id", Value: id},
			{Name: "name", Value: "op"},
		}, 1)
	}
	return lr
}

// Every admitted span reaches the buffer, in the batch it arrived in.
func TestVTInsertAdapter_EverySpanReachesTheBuffer(t *testing.T) {
	SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/data/buffer"}
	lr := spanBatch("api", "s1", "s2", "s3")
	defer logstorage.PutLogRows(lr)

	before := metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got := buf.got(); strings.Join(got, ",") != "s1,s2,s3" || buf.batches != 1 {
		t.Errorf("buffer got %v in %d batches, want s1,s2,s3 in one", got, buf.batches)
	}
	if d := metrics.InsertRowsTotal.Get() - before; d != 3 {
		t.Errorf("lakehouse_insert_rows_total rose by %d, want 3 (the rows admitted into the buffer)", d)
	}
}

// A stream over its tenant's cardinality limit is dropped before the buffer;
// the other streams of the batch are kept, copied with the upstream API, and the
// caller's batch is left as it was. VictoriaTraces' trace_id_idx and
// service_graph rows are admitted like any other.
func TestVTInsertAdapter_DropsOverLimitStreamsKeepsTheRest(t *testing.T) {
	t.Cleanup(func() { SetCardinalityGate(nil) })
	SetCardinalityGate(fakeGate{deny: map[string]bool{`{resource_attr:service.name="noisy"}`: true}})
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	for _, b := range []struct{ svc, id string }{{"api", "keep-1"}, {"noisy", "over-limit"}, {"api", "keep-2"}} {
		one := spanBatch(b.svc, b.id)
		one.ForEachRow(func(_ uint64, r *logstorage.InsertRow) { lr.MustAddInsertRow(r) })
		logstorage.PutLogRows(one)
	}
	idx := logstorage.GetLogRows([]string{"trace_id_idx_stream"}, nil, nil, nil, "")
	idx.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, time.Now().UnixNano(), []logstorage.Field{
		{Name: "trace_id_idx_stream", Value: "3"}, {Name: "span_id", Value: "index-row"},
	}, 1)
	idx.ForEachRow(func(_ uint64, r *logstorage.InsertRow) { lr.MustAddInsertRow(r) })
	logstorage.PutLogRows(idx)
	total := lr.RowsCount()

	rowsBefore := metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got := strings.Join(buf.got(), ","); got != "keep-1,keep-2,index-row" {
		t.Errorf("buffer got %q, want keep-1,keep-2,index-row (the index row stays in the buffer)", got)
	}
	if lr.RowsCount() != total {
		t.Errorf("the caller's batch changed: %d rows, was %d", lr.RowsCount(), total)
	}
	if d := metrics.InsertRowsTotal.Get() - rowsBefore; d != 2 {
		t.Errorf("lakehouse_insert_rows_total rose by %d, want 2 (admitted spans only; the index row is not counted)", d)
	}
}

// A batch the cardinality gate drops entirely never reaches the buffer.
func TestVTInsertAdapter_BatchWithNothingAdmittedIsNotAdded(t *testing.T) {
	t.Cleanup(func() { SetCardinalityGate(nil) })
	SetCardinalityGate(fakeGate{deny: map[string]bool{`{resource_attr:service.name="noisy"}`: true}})
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}
	lr := spanBatch("noisy", "s1", "s2")
	defer logstorage.PutLogRows(lr)
	a.MustAddRows(lr)
	if buf.batches != 0 {
		t.Errorf("the buffer was called %d times for a batch of dropped rows", buf.batches)
	}
}

// 429 with upstream's message while the buffer's volume is below its floor;
// nothing else refuses a write.
func TestVTInsertAdapter_CanWriteData(t *testing.T) {
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/data/lakehouse/buffer"}
	if err := a.CanWriteData(); err != nil {
		t.Fatalf("a healthy buffer refused a write: %v", err)
	}

	buf.readOnly = true
	before := metrics.InsertRejected.Get("read_only")
	err := a.CanWriteData()
	sc, ok := err.(*httpserver.ErrorWithStatusCode)
	if !ok {
		t.Fatalf("error %T (%v), want *httpserver.ErrorWithStatusCode", err, err)
	}
	if sc.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", sc.StatusCode)
	}
	want := "cannot add rows into storage in read-only mode; the storage can be in read-only mode because of lack of free disk space at insert.buffer_dir=/data/lakehouse/buffer"
	if sc.Err.Error() != want {
		t.Errorf("message = %q, want %q", sc.Err.Error(), want)
	}
	if d := metrics.InsertRejected.Get("read_only") - before; d != 1 {
		t.Errorf("lakehouse_insert_rejected_total{reason=read_only} rose by %d, want 1", d)
	}
}

// A real buffer with free disk accepts writes (the buffer's own IsReadOnly
// feeds the 429; a volume below the floor cannot be made in a test).
func TestVTInsertAdapter_RealBufferHealthyIsWritable(t *testing.T) {
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer segs.Close()
	a := &vtInsertAdapter{buf: segs, dir: "/d"}
	if err := a.CanWriteData(); err != nil {
		t.Errorf("a buffer with free disk refused a write: %v", err)
	}
}

// A failure inside the buffer is not recovered: it propagates to the request,
// as in upstream, so the client retries instead of getting an ack for spans that
// were stored nowhere.
func TestVTInsertAdapter_BufferPanicPropagates(t *testing.T) {
	SetCardinalityGate(nil)
	a := &vtInsertAdapter{buf: &recordingBuffer{panicMsg: "simulated buffer failure"}, dir: "/d"}
	lr := spanBatch("api", "s1")
	defer logstorage.PutLogRows(lr)
	defer func() {
		if r := recover(); r != "simulated buffer failure" {
			t.Fatalf("recovered %v, want the buffer's panic to reach the caller", r)
		}
	}()
	a.MustAddRows(lr)
	t.Fatal("the buffer's panic was swallowed")
}

// The spans of an admitted batch are in the real insert buffer, searchable.
func TestVTInsertAdapter_SpansAreInTheSegments(t *testing.T) {
	SetCardinalityGate(nil)
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer segs.Close()
	a := &vtInsertAdapter{buf: segs, dir: "/d"}
	lr := spanBatch("api", "s1", "s2")
	defer logstorage.PutLogRows(lr)
	a.MustAddRows(lr)
	segs.DebugFlush()

	snap := segs.Snapshot()
	defer snap.Release()
	q, err := logstorage.ParseQueryAtTimestamp("*", time.Now().Add(time.Hour).UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), 0, time.Now().Add(time.Hour).UnixNano())
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{{AccountID: 1, ProjectID: 2}}, q, false, nil)
	var mu sync.Mutex
	n := 0
	if err := snap.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		n += db.RowsCount()
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("%d spans searchable in the segments, want 2", n)
	}
}

// VictoriaTraces' trace-ID index rows reach the buffer, as upstream stores
// them, but lakehouse_insert_rows_total counts spans only: the index rows are
// not written to Parquet, and the legacy path did not count them either.
func TestVTInsertAdapter_IndexRowsAreBufferedNotCounted(t *testing.T) {
	SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/data/buffer"}
	spans := spanBatch("api", "s1", "s2")
	defer logstorage.PutLogRows(spans)
	index := logstorage.GetLogRows([]string{"trace_id_idx_stream"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(index)
	for i := 0; i < 3; i++ {
		index.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, time.Now().UnixNano(), []logstorage.Field{
			{Name: "trace_id_idx_stream", Value: "7"},
			{Name: "_msg", Value: "-"},
			{Name: "trace_id_idx", Value: "t-x"},
		}, 1)
	}
	before := metrics.InsertRowsTotal.Get()
	a.MustAddRows(spans)
	a.MustAddRows(index)
	if buf.rows != 5 {
		t.Errorf("the buffer got %d rows; want all 5 (2 spans, 3 index rows)", buf.rows)
	}
	if d := metrics.InsertRowsTotal.Get() - before; d != 2 {
		t.Errorf("lakehouse_insert_rows_total rose by %d; want 2 (the spans)", d)
	}
}
