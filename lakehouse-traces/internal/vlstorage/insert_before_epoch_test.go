package vlstorage

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// askedGate records the streams it was asked about and admits all of them.
type askedGate struct{ asked []string }

func (g *askedGate) AllowStream(_, _ uint32, stream string) bool {
	g.asked = append(g.asked, stream)
	return true
}

// batchAt builds one batch of spans of the service <app> with the given _time
// values; the span_id of span i is <app>-<i>.
func batchAt(app string, times ...int64) *logstorage.LogRows {
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for i, ts := range times {
		lr.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, ts, []logstorage.Field{
			{Name: "resource_attr:service.name", Value: app}, {Name: "span_id", Value: fmt.Sprintf("%s-%d", app, i)},
			{Name: "trace_id", Value: "t"},
		}, 1)
	}
	return lr
}

// Spans before the Unix epoch are refused (an OTLP end time above 2^63 ns wraps to one: VictoriaTraces stamps a span with its end time) when they are added (#388): they are
// not passed to the buffer, they are counted under
// lakehouse_insert_rows_dropped_total{reason="too_small_timestamp"}, and
// lakehouse_insert_rows_total counts only the spans the buffer was given, so the
// two counters and the buffer agree.
func TestVTInsertAdapter_RefusesRowsBeforeTheEpoch(t *testing.T) {
	SetCardinalityGate(nil)
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}
	now := time.Now().UnixNano()
	lr := batchAt("a", now, -1, 0, math.MinInt64, 1, now-int64(time.Hour))
	defer logstorage.PutLogRows(lr)

	admitted0 := metrics.InsertRowsTotal.Get()
	dropped0 := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp)
	a.MustAddRows(lr)

	if got, want := strings.Join(buf.got(), ","), "a-0,a-2,a-4,a-5"; got != want {
		t.Errorf("the buffer got %q, want %q (the rows from the epoch on)", got, want)
	}
	if d := metrics.InsertRowsTotal.Get() - admitted0; d != 4 {
		t.Errorf("lakehouse_insert_rows_total rose by %d, want 4", d)
	}
	if d := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp) - dropped0; d != 2 {
		t.Errorf("lakehouse_insert_rows_dropped_total{reason=too_small_timestamp} rose by %d, want 2", d)
	}
}

// A batch of only refused rows adds nothing, and a refused row does not take a
// slot of its tenant's cardinality limit: its stream is not even asked about.
func TestVTInsertAdapter_RefusedRowsTakeNoCardinalitySlot(t *testing.T) {
	gate := &askedGate{}
	SetCardinalityGate(gate)
	t.Cleanup(func() { SetCardinalityGate(nil) })
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}

	lr := batchAt("old", -5, -1, math.MinInt64)
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	if buf.batches != 0 {
		t.Errorf("a batch of only refused rows reached the buffer (%d batches)", buf.batches)
	}
	if len(gate.asked) != 0 {
		t.Errorf("the gate was asked about %v for rows that were refused", gate.asked)
	}

	lr = batchAt("mixed", -5, time.Now().UnixNano())
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	if got := strings.Join(buf.got(), ","); got != "mixed-1" {
		t.Errorf("the buffer got %q, want only mixed-1", got)
	}
	if len(gate.asked) != 1 {
		t.Errorf("the gate was asked %d times for one admitted stream: %v", len(gate.asked), gate.asked)
	}
}

// Property: for random batches, the rows that reach the buffer are exactly the
// rows with _time >= 0 in their original order, and the counters move by the
// two counts.
func TestVTInsertAdapter_BeforeEpochProperty(t *testing.T) {
	SetCardinalityGate(nil)
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 300; iter++ {
		times := make([]int64, 1+rng.Intn(40))
		var want []string
		neg := 0
		for i := range times {
			switch rng.Intn(4) {
			case 0:
				times[i] = -rng.Int63n(math.MaxInt64)
			case 1:
				times[i] = rng.Int63n(1000) - 500
			default:
				times[i] = time.Now().UnixNano() - rng.Int63n(int64(24*time.Hour))
			}
			if times[i] < 0 {
				neg++
			} else {
				want = append(want, fmt.Sprintf("p-%d", i))
			}
		}
		buf := &recordingBuffer{}
		a := &vtInsertAdapter{buf: buf, dir: "/d"}
		lr := batchAt("p", times...)
		d0 := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp)
		t0 := metrics.InsertRowsTotal.Get()
		a.MustAddRows(lr)
		logstorage.PutLogRows(lr)
		got := buf.got()
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("iteration %d: the buffer got %v, want %v", iter, got, want)
		}
		if d := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp) - d0; d != uint64(neg) {
			t.Fatalf("iteration %d: dropped counter rose by %d, want %d", iter, d, neg)
		}
		if d := metrics.InsertRowsTotal.Get() - t0; d != uint64(len(want)) {
			t.Fatalf("iteration %d: rows counter rose by %d, want %d", iter, d, len(want))
		}
	}
}
