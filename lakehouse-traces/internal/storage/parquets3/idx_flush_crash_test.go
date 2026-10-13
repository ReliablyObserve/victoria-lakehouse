package parquets3

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// The crash and atomicity matrix of the trace_id_idx objects.
//
// Upstream's index worker emits a trace's index row 20 to 40 seconds after its spans,
// so a span and its index row usually reach the buffer in different segments: the
// guarantee is per row, "an index row that is in the buffer reaches Parquet exactly
// once", and per group, "a group's spans never become visible without the group's
// index rows" (the index object is stored first, the span object settles the group).

func idxHour() time.Time { return time.Date(2026, 10, 1, 7, 0, 0, 0, time.UTC) }

// idxRowFields are the fields of one trace_id_idx row, as the index worker sends them.
func idxRowFields(id string, start, end int64) []logstorage.Field {
	return []logstorage.Field{
		{Name: otelpb.TraceIDIndexStreamName, Value: strconv.Itoa(int(tracesidx.StreamOf(id)))}, {Name: "_msg", Value: "-"},
		{Name: otelpb.TraceIDIndexFieldName, Value: id},
		{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: strconv.FormatInt(start, 10)},
		{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: strconv.FormatInt(end, 10)},
		{Name: otelpb.TraceIDIndexDuration, Value: strconv.FormatInt(end-start, 10)},
	}
}

// sendTrace adds one span (named msg, _time = end) and the trace's index row.
func (e *segEnv) sendTrace(tn logstorage.TenantID, msg string, start, end time.Time) string {
	e.t.Helper()
	id := fmt.Sprintf("%032x", uint64(e.seq.Add(1))*0x9e3779b97f4a7c15)
	e.sendViaAdapter(tn, []adapterRow{
		{ts: end.UnixNano(), fields: ackFields("api", msg)},
		{ts: start.UnixNano(), streamLen: 1, fields: idxRowFields(id, start.UnixNano(), end.UnixNano())},
	})
	return id
}

// storedIdxRows returns, per trace id, how many rows the stored idx objects hold.
func (e *segEnv) storedIdxRows() map[string]int {
	e.t.Helper()
	e.u.mu.Lock()
	objs := map[string][]byte{}
	for k, d := range e.u.data {
		if !e.u.gone[k] && tracesidx.IsKey(k) {
			objs[k] = d
		}
	}
	e.u.mu.Unlock()
	out := map[string]int{}
	for k, d := range objs {
		rows, err := tracesidx.ReadAll(d)
		if err != nil {
			e.t.Fatalf("read idx object %s: %v", k, err)
		}
		for _, r := range rows {
			out[r.TraceID]++
		}
	}
	return out
}

func (e *segEnv) keepIdx() { e.keep = vlstorage.FlushRowKeeper() }

func TestIdxFlush_RowsReachParquetExactlyOnce(t *testing.T) {
	e := newSegEnv(t)
	e.keepIdx()
	ids := map[string]bool{}
	for i := 0; i < 30; i++ {
		start := idxHour().Add(time.Duration(i) * 7 * time.Minute) // spans over several hours
		ids[e.sendTrace(segTenantA, fmt.Sprintf("a%d", i), start, start.Add(time.Second))] = true
		ids[e.sendTrace(segTenantB, fmt.Sprintf("b%d", i), start, start.Add(2*time.Second))] = true
	}
	e.seal()
	e.drainAll(e.flusher(7))
	got := e.storedIdxRows()
	for id := range ids {
		if got[id] != 1 {
			t.Errorf("trace %s: %d index rows stored, want 1", id, got[id])
		}
	}
	if len(got) != len(ids) {
		t.Errorf("%d distinct index rows stored, want %d", len(got), len(ids))
	}
	if n := len(e.storedMsgs()); n != 60 {
		t.Errorf("%d spans stored, want 60", n)
	}
}

// A span object that fails to upload leaves the index object stored (it is stored
// first); the retry sends the span object and nothing else, and the group is
// complete: spans once, index rows once, no key stored twice with other bytes.
func TestIdxFlush_SpanUploadFailsAfterIdxUpload(t *testing.T) {
	e := newSegEnv(t)
	e.keepIdx()
	e.sendTrace(segTenantA, "x", idxHour().Add(time.Minute), idxHour().Add(2*time.Minute))
	e.seal()
	var failSpans atomic.Bool
	failSpans.Store(true)
	e.failFn.Store(func(key string) bool {
		return failSpans.Load() && !tracesidx.IsKey(key) && !strings.Contains(key, "_segments")
	})
	f := e.flusher(1000)
	e.drainAll(f)
	if len(e.segs.Pending()) != 1 {
		t.Fatal("a segment with a failing span upload was committed")
	}
	if len(e.storedIdxRows()) != 1 || len(e.storedMsgs()) != 0 {
		t.Fatalf("after the failed attempt: idx rows %v, spans %v; want the index row stored and no span", e.storedIdxRows(), e.storedMsgs())
	}
	failSpans.Store(false)
	e.drainAll(f)
	if len(e.segs.Pending()) != 0 {
		t.Fatal("the retry did not commit the segment")
	}
	if idx, spans := e.storedIdxRows(), e.storedMsgs(); len(idx) != 1 || spans["x"] != 1 {
		t.Fatalf("after the retry: idx rows %v, spans %v", idx, spans)
	}
}

// An index object that fails to upload stops the group before any span is stored and
// before the stored mark: the retry (also after a restart, which asks the object store
// with HEAD) stores both, once.
func TestIdxFlush_IdxUploadFailureAndRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			e := newSegEnv(t)
			e.keepIdx()
			id := e.sendTrace(segTenantA, "y", idxHour().Add(time.Minute), idxHour().Add(2*time.Minute))
			e.seal()
			var failIdx atomic.Bool
			failIdx.Store(true)
			e.failFn.Store(func(key string) bool { return failIdx.Load() && tracesidx.IsKey(key) })
			f := e.flusher(1000)
			e.drainAll(f)
			if len(e.storedIdxRows()) != 0 || len(e.storedMsgs()) != 0 {
				t.Fatalf("an object was stored although the index upload failed: %v %v", e.storedIdxRows(), e.storedMsgs())
			}
			failIdx.Store(false)
			if restart {
				e.restart()
				f = e.flusher(1000)
			}
			e.drainAll(f)
			if got := e.storedIdxRows()[id]; got != 1 {
				t.Fatalf("index row stored %d times, want 1", got)
			}
			if got := e.storedMsgs()["y"]; got != 1 {
				t.Fatalf("span stored %d times, want 1", got)
			}
		})
	}
}

// A crash between the index object and the span object (the process dies after the
// index PUT; its stored mark was never written) must not lose the spans on restart:
// the recovery asks HEAD for the SPAN object, finds nothing, and re-sends the group.
func TestIdxFlush_CrashBetweenIdxAndSpanObjects(t *testing.T) {
	e := newSegEnv(t)
	e.keepIdx()
	id := e.sendTrace(segTenantA, "z", idxHour().Add(time.Minute), idxHour().Add(2*time.Minute))
	e.seal()
	// The "crash": the span PUT fails, then the process restarts with a new manifest.
	e.failFn.Store(func(key string) bool { return !tracesidx.IsKey(key) && !strings.Contains(key, "_segments") })
	f := e.flusher(1000)
	e.drainAll(f)
	e.restart()
	e.failFn.Store(func(string) bool { return false })
	f = e.flusher(1000)
	e.drainAll(f)
	if got := e.storedIdxRows()[id]; got != 1 {
		t.Fatalf("index row stored %d times, want 1", got)
	}
	if got := e.storedMsgs()["z"]; got != 1 {
		t.Fatalf("span lost or doubled by the crash: stored %d times", got)
	}
}

// A group whose rows are all index rows (the trace's start is in an earlier hour than
// its spans' end) is settled by its index object alone.
func TestIdxFlush_IdxOnlyGroup(t *testing.T) {
	e := newSegEnv(t)
	e.keepIdx()
	start := idxHour().Add(59*time.Minute + 59*time.Second)
	id := e.sendTrace(segTenantA, "w", start, start.Add(3*time.Second)) // span in the next hour
	e.seal()
	f := e.flusher(1000)
	e.drainAll(f)
	if len(e.segs.Pending()) != 0 {
		t.Fatal("the segment was not committed")
	}
	if e.storedIdxRows()[id] != 1 || e.storedMsgs()["w"] != 1 {
		t.Fatalf("idx %v spans %v", e.storedIdxRows(), e.storedMsgs())
	}
	// A restart re-reads the segment's state: nothing is stored twice.
	e.restart()
	f = e.flusher(1000)
	e.drainAll(f)
	if e.storedIdxRows()[id] != 1 {
		t.Fatalf("after a restart: idx %v", e.storedIdxRows())
	}
}
