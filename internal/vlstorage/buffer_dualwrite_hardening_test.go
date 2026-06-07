package vlstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type panicBufferStore struct{ calls int }

func (p *panicBufferStore) MustAddRows(_ *logstorage.LogRows) {
	p.calls++
	panic("simulated buffer failure")
}

// TestDualWrite_LogsBufferPanicNeverBreaksIngestion: a panicking logstorage
// buffer MUST NOT break log ingestion — the legacy LogWriter still gets every
// row. The "nothing breaks" safeguard for the logs side.
func TestDualWrite_LogsBufferPanicNeverBreaksIngestion(t *testing.T) {
	pb := &panicBufferStore{}
	cap := &captureLogWriter{}
	a := &insertAdapter{writer: cap}
	SetBufferStore(pb)
	defer SetBufferStore(nil)

	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 5; i++ {
		lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, []logstorage.Field{
			{Name: "service.name", Value: "checkout"},
			{Name: "_msg", Value: "event"},
		}, 1)
	}

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

// TestDualWrite_LogsNilBufferIsNoop confirms the default leaves ingestion
// byte-identical to legacy.
func TestDualWrite_LogsNilBufferIsNoop(t *testing.T) {
	SetBufferStore(nil)
	cap := &captureLogWriter{}
	a := &insertAdapter{writer: cap}

	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, []logstorage.Field{
		{Name: "service.name", Value: "checkout"},
		{Name: "_msg", Value: "event"},
	}, 1)
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)

	if len(cap.rows) != 1 {
		t.Fatalf("legacy path: want 1 row, got %d", len(cap.rows))
	}
}
