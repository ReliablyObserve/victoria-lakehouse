package vlstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// panicBufferStore simulates a broken Option B buffer.
type panicBufferStore struct{ calls int }

func (p *panicBufferStore) MustAddRows(_ *logstorage.LogRows) {
	p.calls++
	panic("simulated buffer failure")
}

// TestDualWrite_BufferPanicNeverBreaksIngestion is the core "nothing breaks"
// safeguard: if the logstorage buffer panics, ingestion MUST continue and the
// legacy TraceWriter MUST still receive every row. A regression here would let
// an Option B bug take down the insert path.
func TestDualWrite_BufferPanicNeverBreaksIngestion(t *testing.T) {
	pb := &panicBufferStore{}
	cap := &captureWriter{}
	a := &vtInsertAdapter{writer: cap}
	SetBufferStore(pb)
	defer SetBufferStore(nil)

	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 5; i++ {
		lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: "t"},
		}, 1)
	}

	// Must NOT panic out of MustAddRows.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("buffer panic escaped ingestion: %v", r)
			}
		}()
		a.MustAddRows(lr)
	}()
	logstorage.PutLogRows(lr)

	if pb.calls != 1 {
		t.Fatalf("buffer should have been attempted once, got %d", pb.calls)
	}
	if len(cap.rows) != 5 {
		t.Fatalf("legacy path must still get all 5 rows despite buffer panic, got %d", len(cap.rows))
	}
}

// TestDualWrite_NilBufferIsNoop confirms the default (BufferEngine=buffer, no
// SetBufferStore) leaves ingestion byte-identical to legacy: nothing extra runs.
func TestDualWrite_NilBufferIsNoop(t *testing.T) {
	SetBufferStore(nil)
	cap := &captureWriter{}
	a := &vtInsertAdapter{writer: cap}

	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, []logstorage.Field{
		{Name: "service.name", Value: "api-gateway"},
		{Name: "trace_id", Value: "t"},
	}, 1)
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)

	if len(cap.rows) != 1 {
		t.Fatalf("legacy path: want 1 row, got %d", len(cap.rows))
	}
}

// recordingBufferStore captures the LogRows it was handed (read-only) so the
// test can assert the buffer is fed exactly what the legacy path converted.
type recordingBufferStore struct{ traceIDs []string }

func (r *recordingBufferStore) MustAddRows(lr *logstorage.LogRows) {
	lr.ForEachRow(func(_ uint64, row *logstorage.InsertRow) {
		for _, f := range row.Fields {
			if f.Name == "trace_id" {
				r.traceIDs = append(r.traceIDs, f.Value)
			}
		}
	})
}

// TestDualWrite_BufferSeesSameRows asserts the buffer is handed the SAME logical
// rows the legacy path keeps (field-level), not a divergent set.
func TestDualWrite_BufferSeesSameRows(t *testing.T) {
	rb := &recordingBufferStore{}
	cap := &captureWriter{}
	a := &vtInsertAdapter{writer: cap}
	SetBufferStore(rb)
	defer SetBufferStore(nil)

	want := []string{"alpha", "beta", "gamma"}
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for _, tid := range want {
		lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: tid},
		}, 1)
	}
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)

	// Legacy got 3 spans...
	if len(cap.rows) != len(want) {
		t.Fatalf("legacy: want %d, got %d", len(want), len(cap.rows))
	}
	// ...and the buffer saw the same 3 trace_ids.
	if len(rb.traceIDs) != len(want) {
		t.Fatalf("buffer saw %d trace_ids, want %d", len(rb.traceIDs), len(want))
	}
	legacyIDs := map[string]bool{}
	for _, r := range cap.rows {
		legacyIDs[r.TraceID] = true
	}
	for _, id := range rb.traceIDs {
		if !legacyIDs[id] {
			t.Fatalf("buffer saw trace_id %q the legacy path did not keep", id)
		}
	}
}
