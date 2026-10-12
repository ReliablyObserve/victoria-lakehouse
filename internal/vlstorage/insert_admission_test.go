package vlstorage

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// countingGate denies the streams in deny and records every question it is asked.
type countingGate struct {
	deny  map[string]bool
	asked []string
}

func (g *countingGate) AllowStream(_, _ uint32, stream string) bool {
	g.asked = append(g.asked, stream)
	return !g.deny[stream]
}

func cardinalityDropped() uint64 {
	return metrics.InsertRowsDropped.Get(storage.ReasonCardinalityLimit)
}

// Every row of a stream the tenant's limit refuses is counted under
// lakehouse_insert_rows_dropped_total{reason="cardinality_limit"}, per row and
// not per decision: the request is still answered as a success, so the counter
// is the only trace of rows that are not stored.
func TestInsertAdapter_CountsEveryRowRefusedForTheCardinalityLimit(t *testing.T) {
	gate := &countingGate{deny: map[string]bool{`{app="over"}`: true}}
	SetCardinalityGate(gate)
	t.Cleanup(func() { SetCardinalityGate(nil) })
	buf := &recordingBuffer{}
	a := &insertAdapter{buf: buf, dir: "/d"}

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"app"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	add := func(app, msg string, ts int64) {
		lr.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, ts, []logstorage.Field{{Name: "app", Value: app}, {Name: "_msg", Value: msg}}, 1)
	}
	add("over", "o1", now)
	add("ok", "k1", now)
	add("over", "o2", now+1)
	add("over", "o3", now+2)
	add("ok", "k2", now+3)
	add("over", "before-the-epoch", -1) // refused for its _time, not for the limit

	card0, small0, total0 := cardinalityDropped(), metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp), metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got, want := strings.Join(buf.got(), ","), "k1,k2"; got != want {
		t.Errorf("the buffer got %q, want %q", got, want)
	}
	if d := cardinalityDropped() - card0; d != 3 {
		t.Errorf("lakehouse_insert_rows_dropped_total{reason=cardinality_limit} rose by %d, want 3 (one per row, not per decision)", d)
	}
	if d := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp) - small0; d != 1 {
		t.Errorf("too_small_timestamp rose by %d, want 1", d)
	}
	if d := metrics.InsertRowsTotal.Get() - total0; d != 2 {
		t.Errorf("lakehouse_insert_rows_total rose by %d, want 2", d)
	}
	if len(gate.asked) != 2 {
		t.Errorf("the gate was asked %d times for two streams: %v", len(gate.asked), gate.asked)
	}
}

// The trace-shaped drop at the flush is counted too.
func TestFlushRowKeeper_CountsTheTraceShapedDrop(t *testing.T) {
	SetCardinalityGate(nil)
	keep := FlushRowKeeper()
	before := metrics.BufferFlushRowsDropped.Get(ReasonTraceShapedStream)
	if !keep(1, 2, `{app="api"}`) || !keep(1, 2, "") {
		t.Fatal("an ordinary stream and the empty stream must be kept")
	}
	if d := metrics.BufferFlushRowsDropped.Get(ReasonTraceShapedStream) - before; d != 0 {
		t.Fatalf("kept rows were counted as dropped (%d)", d)
	}
	for i := 0; i < 3; i++ {
		if keep(1, 2, `{resource_attr:service.name="api"}`) {
			t.Fatal("a trace-shaped stream must not be written as logs")
		}
	}
	if d := metrics.BufferFlushRowsDropped.Get(ReasonTraceShapedStream) - before; d != 3 {
		t.Fatalf("lakehouse_buffer_flush_rows_dropped_total{reason=trace_shaped_stream} rose by %d, want 3", d)
	}
}

// checkAdmission builds a batch from data, runs it through the insert adapter
// with a gate that denies some streams, and checks the result against the rule
// written out here independently of the adapter: a row is refused when its _time
// is before the epoch, else when its stream is denied, else it reaches the buffer;
// the input is not changed; the counters move by the number of rows of each kind;
// the gate is asked once per distinct stream with a row from the epoch on and
// never about a row before it. Streams are app0..app3; data[0] says which are
// denied, each further byte makes one row (low 2 bits the stream, bit 2 a _time
// before the epoch).
func checkAdmission(t testing.TB, data []byte) {
	if len(data) == 0 || len(data) > 200 {
		return
	}
	denyMask := data[0]
	rows := data[1:]
	gate := &countingGate{deny: map[string]bool{}}
	for i := 0; i < 4; i++ {
		if denyMask&(1<<i) != 0 {
			gate.deny[fmt.Sprintf(`{app="app%d"}`, i)] = true
		}
	}
	SetCardinalityGate(gate)
	defer SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &insertAdapter{buf: buf, dir: "/d"}

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"app"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	var want []string
	neg, denied := 0, 0
	askedWant := map[string]bool{}
	for i, b := range rows {
		app := fmt.Sprintf("app%d", b&3)
		ts := now + int64(i)
		if b&4 != 0 {
			ts = -int64(i) - 1
			if b&8 != 0 {
				ts = math.MinInt64
			}
		}
		msg := fmt.Sprintf("m%d", i)
		lr.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, ts, []logstorage.Field{{Name: "app", Value: app}, {Name: "_msg", Value: msg}}, 1)
		switch {
		case ts < 0:
			neg++
		case gate.deny[fmt.Sprintf(`{app=%q}`, app)]:
			denied++
			askedWant[app] = true
		default:
			want = append(want, msg)
			askedWant[app] = true
		}
	}
	card0, small0, total0 := cardinalityDropped(), metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp), metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got := strings.Join(buf.got(), ","); got != strings.Join(want, ",") {
		t.Fatalf("the buffer got %q, want %q (denyMask %04b, rows %v)", got, strings.Join(want, ","), denyMask&15, rows)
	}
	if lr.RowsCount() != len(rows) {
		t.Fatalf("the adapter changed its input: %d rows, want %d", lr.RowsCount(), len(rows))
	}
	if d := cardinalityDropped() - card0; d != uint64(denied) {
		t.Fatalf("cardinality_limit rose by %d, want %d", d, denied)
	}
	if d := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp) - small0; d != uint64(neg) {
		t.Fatalf("too_small_timestamp rose by %d, want %d", d, neg)
	}
	if d := metrics.InsertRowsTotal.Get() - total0; d != uint64(len(want)) {
		t.Fatalf("lakehouse_insert_rows_total rose by %d, want %d", d, len(want))
	}
	if len(gate.asked) != len(askedWant) {
		t.Fatalf("the gate was asked %d times (%v), want once for each of the %d streams that have a row from the epoch on", len(gate.asked), gate.asked, len(askedWant))
	}
}

func TestInsertAdapter_AdmissionRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for iter := 0; iter < 500; iter++ {
		data := make([]byte, 1+rng.Intn(60))
		rng.Read(data)
		checkAdmission(t, data)
	}
}

// FuzzAdmittedRows drives the admission and copy loop of the insert adapter
// (admittedRows) with arbitrary batches, gates and times.
func FuzzAdmittedRows(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3})
	f.Add([]byte{0b0101, 0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{0b1111, 0, 1, 2, 3})
	f.Add([]byte{0, 4, 5, 6, 7, 12, 13})
	f.Add([]byte{0b0010, 4, 1, 1, 5, 9, 13, 2})
	f.Fuzz(func(t *testing.T, data []byte) { checkAdmission(t, data) })
}
