package parquets3

import (
	"context"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// The fakeLocalBuffer stand-in lives in storage_misc_coverage_test.go (it
// records the query it ran, can emit a sentinel block, and can fail via
// queryErr) — shared so there is a single LocalBuffer fake in this package.

// TestServePureBufferQuery_EngagesAndKeepsPipes is the core regression guard for
// the pure-buffer fast path (the recent-window groupby win). When the query
// window is entirely unflushed (no parquet files) and this is a single node
// (buffer holds ALL unflushed rows), the FULL query — aggregation pipes intact —
// must run natively on the buffer's VL engine. The bridge's alternative
// DropAllPipes()-then-ship-raw-rows path is what we are escaping; if anyone
// reintroduces a pipe strip here, gotQuery loses its "stats" pipe and this fails.
func TestServePureBufferQuery_EngagesAndKeepsPipes(t *testing.T) {
	fake := &fakeLocalBuffer{emit: true}
	s := &Storage{localBuffer: fake} // single node: bufferBridge nil

	q := mustParseQuery(t, "*| stats count()")
	if !strings.Contains(q.String(), "stats") {
		t.Fatalf("precondition: query lost its stats pipe before the call: %q", q.String())
	}

	emitted := 0
	wb := func(_ uint, _ *logstorage.DataBlock) { emitted++ }

	if !s.servePureBufferQuery(context.Background(), q, nil, wb) {
		t.Fatal("fast path did not engage for a single-node unflushed window")
	}
	if !fake.ran {
		t.Fatal("buffer RunQuery was never called")
	}
	if !strings.Contains(fake.gotQuery, "stats") {
		t.Errorf("aggregation pipe was stripped before reaching the buffer (DropAllPipes regression): got %q", fake.gotQuery)
	}
	if emitted == 0 {
		t.Error("buffer output did not reach the caller's writeBlock")
	}
}

// TestServePureBufferQuery_SkipsWhenUnsafe locks the two conditions under which
// the fast path MUST decline (returning false so the caller falls through to the
// bridge): no local buffer at all, and — critically for multi-pod correctness —
// when peers exist, because this node's buffer then holds only its OWN unflushed
// rows and serving locally would silently drop every other pod's recent data.
func TestServePureBufferQuery_SkipsWhenUnsafe(t *testing.T) {
	q := mustParseQuery(t, "*| stats count()")

	t.Run("no local buffer", func(t *testing.T) {
		s := &Storage{} // localBuffer nil
		if s.servePureBufferQuery(context.Background(), q, nil, func(uint, *logstorage.DataBlock) {}) {
			t.Error("fast path engaged with no local buffer; must defer to the bridge")
		}
	})

	t.Run("peers present", func(t *testing.T) {
		fake := &fakeLocalBuffer{}
		s := &Storage{
			localBuffer:  fake,
			bufferBridge: &BufferBridge{endpoints: []string{"http://peer-1:8480"}},
		}
		if s.servePureBufferQuery(context.Background(), q, nil, func(uint, *logstorage.DataBlock) {}) {
			t.Error("fast path engaged while peers exist; would drop peers' unflushed rows")
		}
		if fake.ran {
			t.Error("buffer was queried despite peers present (must fan out via the bridge)")
		}
	})
}

// TestServePureBufferQuery_ErrorFallsBack ensures a buffer failure does not
// swallow the query: servePureBufferQuery returns false so the caller still runs
// the bridge path rather than returning an empty (silently wrong) result.
func TestServePureBufferQuery_ErrorFallsBack(t *testing.T) {
	fake := &fakeLocalBuffer{queryErr: context.DeadlineExceeded}
	s := &Storage{localBuffer: fake}
	q := mustParseQuery(t, "*| stats count()")
	if s.servePureBufferQuery(context.Background(), q, nil, func(uint, *logstorage.DataBlock) {}) {
		t.Error("fast path reported success despite a buffer error; caller would skip the bridge fallback")
	}
	if !fake.ran {
		t.Error("expected the buffer to be attempted before falling back")
	}
}
