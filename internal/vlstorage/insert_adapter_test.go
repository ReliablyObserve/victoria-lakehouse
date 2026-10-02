package vlstorage

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// recordingBuffer is a BufferStore that keeps what it was given (copied: the
// caller reuses the LogRows) and can be read-only or panic.
type recordingBuffer struct {
	mu       sync.Mutex
	msgs     []string
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
		for _, f := range r.Fields {
			if f.Name == "_msg" || f.Name == "" {
				b.msgs = append(b.msgs, strings.Clone(f.Value))
			}
		}
	})
}

func (b *recordingBuffer) IsReadOnly() bool { return b.readOnly }

func (b *recordingBuffer) got() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.msgs...)
}

func newBatch(streamField string, rows ...[2]string) *logstorage.LogRows {
	lr := logstorage.GetLogRows([]string{streamField}, nil, nil, nil, "")
	for _, r := range rows {
		lr.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, time.Now().UnixNano(), []logstorage.Field{
			{Name: streamField, Value: r[0]},
			{Name: "_msg", Value: r[1]},
		}, 1)
	}
	return lr
}

// Every admitted row reaches the buffer, in the batch it arrived in.
func TestInsertAdapter_EveryAdmittedRowReachesTheBuffer(t *testing.T) {
	SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &insertAdapter{buf: buf, dir: "/data/buffer"}
	lr := newBatch("service.name", [2]string{"api", "m1"}, [2]string{"api", "m2"}, [2]string{"web", "m3"})
	defer logstorage.PutLogRows(lr)

	before := metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got := buf.got(); strings.Join(got, ",") != "m1,m2,m3" || buf.batches != 1 {
		t.Errorf("buffer got %v in %d batches, want m1,m2,m3 in one", got, buf.batches)
	}
	if d := metrics.InsertRowsTotal.Get() - before; d != 3 {
		t.Errorf("lakehouse_insert_rows_total rose by %d, want 3 (the rows admitted into the buffer)", d)
	}
}

// Trace-shaped streams (span data sent to the logs endpoint) and streams over
// their tenant's cardinality limit are dropped before the buffer; the rows of
// the other streams of the batch are kept, copied with the upstream API, and
// the caller's batch is left as it was.
func TestInsertAdapter_DropsTraceShapedAndOverLimitStreamsKeepsTheRest(t *testing.T) {
	t.Cleanup(func() { SetCardinalityGate(nil) })
	SetCardinalityGate(fakeGate{deny: map[string]bool{`{noisy="x"}`: true}})
	buf := &recordingBuffer{}
	a := &insertAdapter{buf: buf, dir: "/d"}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	add := func(streamTag, streamVal, msg string) {
		lr2 := logstorage.GetLogRows([]string{streamTag}, nil, nil, nil, "")
		defer logstorage.PutLogRows(lr2)
		lr2.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, time.Now().UnixNano(), []logstorage.Field{
			{Name: streamTag, Value: streamVal}, {Name: "_msg", Value: msg},
		}, 1)
		lr2.ForEachRow(func(_ uint64, r *logstorage.InsertRow) { lr.MustAddInsertRow(r) })
	}
	add("service.name", "api", "keep-1")
	add("noisy", "x", "over-limit")
	add("resource_attr:service.name", "svc", "trace-shaped")
	add("service.name", "api", "keep-2")
	add("name", "span", "trace-shaped-2")
	total := lr.RowsCount()

	droppedBefore := metrics.LogsTraceShapedRowsDroppedAtIngest.Get()
	rowsBefore := metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got := strings.Join(buf.got(), ","); got != "keep-1,keep-2" {
		t.Errorf("buffer got %q, want keep-1,keep-2", got)
	}
	if lr.RowsCount() != total {
		t.Errorf("the caller's batch changed: %d rows, was %d", lr.RowsCount(), total)
	}
	if d := metrics.LogsTraceShapedRowsDroppedAtIngest.Get() - droppedBefore; d != 2 {
		t.Errorf("trace-shaped rows counted as dropped: %d, want 2", d)
	}
	if d := metrics.InsertRowsTotal.Get() - rowsBefore; d != 2 {
		t.Errorf("lakehouse_insert_rows_total rose by %d, want 2 (admitted rows only)", d)
	}
}

// A batch the admission rule drops entirely never reaches the buffer.
func TestInsertAdapter_BatchWithNothingAdmittedIsNotAdded(t *testing.T) {
	SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &insertAdapter{buf: buf, dir: "/d"}
	lr := newBatch("resource_attr:service.name", [2]string{"svc", "span"})
	defer logstorage.PutLogRows(lr)
	a.MustAddRows(lr)
	if buf.batches != 0 {
		t.Errorf("the buffer was called %d times for a batch of dropped rows", buf.batches)
	}
}

// Logs keep every field a user sends, however it is named: a log line with
// fields called like VictoriaTraces' trace-ID index fields is a log line, is
// added to the buffer and survives the flush filter. (Only the traces binary
// treats such rows as internal.)
func TestLogsBuffer_FieldsNamedLikeTraceIndexRowsAreKept(t *testing.T) {
	SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &insertAdapter{buf: buf, dir: "/d"}
	lr := logstorage.GetLogRows([]string{"trace_id_idx_stream"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	for i := 0; i < 3; i++ {
		lr.MustAdd(logstorage.TenantID{}, time.Now().UnixNano(), []logstorage.Field{
			{Name: "trace_id_idx_stream", Value: fmt.Sprint(i)},
			{Name: "trace_id_idx", Value: "abc"},
			{Name: "trace_id_idx_start_time", Value: "1"},
			{Name: "trace_id_idx_end_time", Value: "2"},
			{Name: "_msg", Value: fmt.Sprintf("user-line-%d", i)},
		}, 1)
	}
	a.MustAddRows(lr)
	if got := strings.Join(buf.got(), ","); got != "user-line-0,user-line-1,user-line-2" {
		t.Errorf("buffer got %q, want the three user lines", got)
	}
	keep := FlushRowKeeper()
	if !keep(0, 0, `{trace_id_idx_stream="1"}`) {
		t.Error("the flush filter dropped a user stream named like the trace-ID index")
	}
	// And the fields survive a round trip through a real buffer.
	lr2 := logstorage.GetLogRows([]string{"trace_id_idx_stream"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr2)
	lr2.MustAdd(logstorage.TenantID{}, time.Now().UnixNano(), []logstorage.Field{
		{Name: "trace_id_idx_stream", Value: "7"}, {Name: "trace_id_idx", Value: "abc"}, {Name: "_msg", Value: "kept"},
	}, 1)
	rows := rowsViaBuffer(t, lr2)
	if len(rows) != 1 || rows[0].LogAttributes["trace_id_idx"] != "abc" || rows[0].Body != "kept" {
		t.Errorf("round trip lost a user field: %+v", rows)
	}
}

// 429 with upstream's message while the buffer's volume is below its floor;
// nothing else refuses a write.
func TestInsertAdapter_CanWriteData(t *testing.T) {
	buf := &recordingBuffer{}
	a := &insertAdapter{buf: buf, dir: "/data/lakehouse/buffer"}
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
func TestInsertAdapter_RealBufferHealthyIsWritable(t *testing.T) {
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer segs.Close()
	a := &insertAdapter{buf: segs, dir: "/d"}
	if err := a.CanWriteData(); err != nil {
		t.Errorf("a buffer with free disk refused a write: %v", err)
	}
}

// A failure inside the buffer is not recovered: it propagates to the request,
// as in upstream, so the client retries instead of getting an ack for rows that
// were stored nowhere.
func TestInsertAdapter_BufferPanicPropagates(t *testing.T) {
	SetCardinalityGate(nil)
	a := &insertAdapter{buf: &recordingBuffer{panicMsg: "simulated buffer failure"}, dir: "/d"}
	lr := newBatch("service.name", [2]string{"api", "m1"})
	defer logstorage.PutLogRows(lr)
	defer func() {
		if r := recover(); r != "simulated buffer failure" {
			t.Fatalf("recovered %v, want the buffer's panic to reach the caller", r)
		}
	}()
	a.MustAddRows(lr)
	t.Fatal("the buffer's panic was swallowed")
}

// The rows of an admitted batch are in the real insert buffer and searchable.
func TestInsertAdapter_RowsAreInTheSegments(t *testing.T) {
	SetCardinalityGate(nil)
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer segs.Close()
	a := &insertAdapter{buf: segs, dir: "/d"}
	lr := newBatch("service.name", [2]string{"api", "m1"}, [2]string{"api", "m2"})
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
		t.Errorf("%d rows searchable in the segments, want 2", n)
	}
}
