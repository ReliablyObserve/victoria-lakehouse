package parquets3

import (
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// failFast runs body in its own goroutine and fails the test after d instead of
// letting a regression (the stream registration blocking or looping on a
// rejected row) hang until the package's test timeout. A panic in body still
// takes the process down at once.
func failFast(t *testing.T, d time.Duration, body func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		body()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("no result after %s: ingest or query of a row with duplicate stream tags is stuck", d)
	}
}

// A span whose resource carries the same attribute twice, where that attribute
// is a stream field, gives the row two stream tags with one name. The
// VictoriaLogs revision this binary embeds (v1.52.0, the one VictoriaTraces
// v0.12.0 pins) rejected that when it registered the stream and panicked
// (VictoriaLogs #1603, #1604; fixed in v1.53.0).
// patches/vl-traces/vl-allow-duplicate-stream-tags.patch carries the same
// two-line change. The insert buffer must take the span and every select
// surface must see it, in the buffer and from the object alone.
func TestDuplicateStreamFields_IngestFlushQuery(t *testing.T) {
	failFast(t, 60*time.Second, func() {
		e := newRestartEnv(t)
		lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
		for i := 0; i < 3; i++ {
			e.spans++
			lr.MustAdd(logstorage.TenantID{}, at(rwHour, time.Duration(10+i)*time.Minute).UnixNano(), []logstorage.Field{
				{Name: "resource_attr:service.name", Value: "a"},
				{Name: "resource_attr:service.name", Value: "b"},
				{Name: "trace_id", Value: fmt.Sprintf("trace-%d", e.spans)},
				{Name: "span_id", Value: fmt.Sprintf("span-%d", e.spans)},
				{Name: "name", Value: "DUP"},
			}, -1)
		}
		e.segs.MustAddRows(lr)
		logstorage.PutLogRows(lr)
		e.segs.DebugFlush()
		e.written["DUP"] += 3
		e.total += 3
		e.ingest("ONE", at(rwHour, 13*time.Minute))

		e.check("in the buffer")
		e.flush()
		e.check("object and committed segment")
		e.reap()
		e.check("object only (cold)")
	})
}
