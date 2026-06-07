package vlstorage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

type captureWriter struct{ rows []schema.TraceRow }

func (c *captureWriter) MustAddTraceRows(rows []schema.TraceRow) { c.rows = append(c.rows, rows...) }
func (c *captureWriter) CanWriteData() error                     { return nil }

// TestDualWrite_LegacyAndBufferParity is the Phase-1 parity proof: a single
// ingested LogRows batch flowing through vtInsertAdapter.MustAddRows feeds BOTH
// the legacy TraceRow path AND the logstorage-native buffer, and the buffer
// answers the same span count via RunQuery — i.e. the new store is at parity
// with the legacy buffer for the same ingest, with zero struct→DataBlock
// conversion on the query side.
func TestDualWrite_LegacyAndBufferParity(t *testing.T) {
	store, err := membuffer.Open(membuffer.Config{Path: t.TempDir(), Retention: time.Hour})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer store.Close()

	cap := &captureWriter{}
	a := &vtInsertAdapter{writer: cap}
	SetBufferStore(store)
	defer SetBufferStore(nil)

	const n = 8
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		lr.MustAdd(logstorage.TenantID{}, now, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: fmt.Sprintf("trace-%d", i)},
			{Name: "span_id", Value: fmt.Sprintf("span-%d", i)},
		}, -1)
	}
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	store.DebugFlush()

	// Legacy path captured all n spans.
	if len(cap.rows) != n {
		t.Fatalf("legacy TraceWriter: want %d rows, got %d", n, len(cap.rows))
	}

	// Buffer store answers the same span count via its native engine.
	q, err := logstorage.ParseQueryAtTimestamp(`_stream:{service.name="api-gateway"}`, now)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var got atomic.Int64
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{},
		[]logstorage.TenantID{{}}, q, false, nil)
	if err := store.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		got.Add(int64(db.RowsCount()))
	}); err != nil {
		t.Fatalf("runquery: %v", err)
	}
	if got.Load() != n {
		t.Fatalf("buffer store: want %d rows for service.name=api-gateway, got %d", n, got.Load())
	}

	// And the buffer can resolve an individual trace by id — the exact path
	// that 404'd against the struct→DataBlock buffer.
	q2, _ := logstorage.ParseQueryAtTimestamp(`trace_id:"trace-5"`, now)
	var byID atomic.Int64
	qctx2 := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{},
		[]logstorage.TenantID{{}}, q2, false, nil)
	if err := store.RunQuery(qctx2, func(_ uint, db *logstorage.DataBlock) {
		byID.Add(int64(db.RowsCount()))
	}); err != nil {
		t.Fatalf("runquery by id: %v", err)
	}
	if byID.Load() != 1 {
		t.Fatalf("buffer store: want 1 row for trace-5, got %d", byID.Load())
	}
}
