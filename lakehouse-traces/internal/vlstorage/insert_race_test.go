package vlstorage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// raceTraceWriter is a concurrency-safe TraceWriter used to detect data
// races introduced by future changes to vtInsertAdapter (e.g. an
// accidental package-level row buffer or cache without synchronization).
type raceTraceWriter struct {
	mu       sync.Mutex
	rowCount int64
	canWrite atomic.Value // error or nil
}

func (w *raceTraceWriter) MustAddTraceRows(rows []schema.TraceRow) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rowCount += int64(len(rows))
}

func (w *raceTraceWriter) CanWriteData() error {
	v := w.canWrite.Load()
	if v == nil {
		return nil
	}
	if err, _ := v.(error); err != nil {
		return err
	}
	return nil
}

// TestRace_ConcurrentMustAddRows spawns N goroutines each calling
// MustAddRows with a disjoint *logstorage.LogRows. If any future change
// introduces shared mutable state in the insert path (cached row
// buffer, package-level scratch slice, unsynced stat counter), the
// race detector will catch it here. Run with `go test -race`.
func TestRace_ConcurrentMustAddRows(t *testing.T) {
	w := &raceTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	const goroutines = 32
	const rowsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
			defer logstorage.PutLogRows(lr)

			for i := 0; i < rowsPerGoroutine; i++ {
				lr.MustAdd(logstorage.TenantID{AccountID: uint32(gid)}, int64(i)*1_000_000_000,
					[]logstorage.Field{
						{Name: "trace_id", Value: fmt.Sprintf("t-%d-%d", gid, i)},
						{Name: "span_id", Value: fmt.Sprintf("s-%d-%d", gid, i)},
						{Name: "service.name", Value: fmt.Sprintf("svc-%d", gid)},
						{Name: "duration_ns", Value: "1000000"},
					}, -1)
			}
			a.MustAddRows(lr)
		}(g)
	}
	wg.Wait()

	wantRows := int64(goroutines * rowsPerGoroutine)
	if w.rowCount != wantRows {
		t.Fatalf("rowCount = %d, want %d", w.rowCount, wantRows)
	}
}

// TestRace_ConcurrentReadsDuringIngest exercises concurrent CanWriteData
// (the only reader-style hook on the adapter) interleaved with ingest,
// to catch a future regression where a writer hot-path field gets
// touched from CanWriteData without synchronization.
func TestRace_ConcurrentReadsDuringIngest(t *testing.T) {
	w := &raceTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	const ingestGoroutines = 8
	const readerGoroutines = 8
	const iters = 100

	var wg sync.WaitGroup
	wg.Add(ingestGoroutines + readerGoroutines)

	for g := 0; g < ingestGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
				lr.MustAdd(logstorage.TenantID{}, int64(i)*1_000_000_000,
					[]logstorage.Field{
						{Name: "trace_id", Value: fmt.Sprintf("t-%d-%d", gid, i)},
						{Name: "span.name", Value: "op"},
					}, -1)
				a.MustAddRows(lr)
				logstorage.PutLogRows(lr)
			}
		}(g)
	}
	for r := 0; r < readerGoroutines; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = a.CanWriteData()
				_ = a.IsLocalStorage()
			}
		}()
	}
	wg.Wait()
}

// TestRace_ConcurrentIngestWithCardinalityGate exercises ingest under
// a non-nil cardinality gate (the gate is set once before goroutines
// start — same pattern as production where SetCardinalityGate is
// called at startup). Catches a future regression where the gate's
// AllowStream impl introduces shared mutable state without
// synchronization.
func TestRace_ConcurrentIngestWithCardinalityGate(t *testing.T) {
	prev := globalCardinalityGate
	t.Cleanup(func() { SetCardinalityGate(prev) })
	SetCardinalityGate(allowAllGate{})

	w := &raceTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	const ingestGoroutines = 8
	const iters = 50

	var wg sync.WaitGroup
	wg.Add(ingestGoroutines)
	for g := 0; g < ingestGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
				lr.MustAdd(logstorage.TenantID{}, 1_000_000_000,
					[]logstorage.Field{
						{Name: "trace_id", Value: "t"},
						{Name: "service.name", Value: fmt.Sprintf("svc-%d", gid)},
					}, -1)
				a.MustAddRows(lr)
				logstorage.PutLogRows(lr)
			}
		}(g)
	}
	wg.Wait()
}

type allowAllGate struct{}

func (allowAllGate) AllowStream(_, _ uint32, _ string) bool { return true }
