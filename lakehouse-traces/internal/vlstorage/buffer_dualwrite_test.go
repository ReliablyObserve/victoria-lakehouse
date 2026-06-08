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

// TestBufferAuthoritativeFlip pins the cutover flip: when SetBufferAuthoritative
// is true, MustAddRows feeds ONLY the buffer and SKIPS the legacy TraceWriter
// (no double Parquet, no WAL); reverting restores the legacy feed.
func TestBufferAuthoritativeFlip(t *testing.T) {
	store, err := membuffer.Open(membuffer.Config{Path: t.TempDir(), Retention: time.Hour})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer store.Close()
	cap := &captureWriter{}
	a := &vtInsertAdapter{writer: cap}
	SetBufferStore(store)
	defer SetBufferStore(nil)

	mk := func() *logstorage.LogRows {
		lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
		lr.MustAdd(logstorage.TenantID{}, time.Now().UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: "t"},
			{Name: "span_id", Value: "s"},
		}, -1)
		return lr
	}

	// Flipped: legacy writer must NOT receive rows.
	SetBufferAuthoritative(true)
	defer SetBufferAuthoritative(false)
	lr := mk()
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	if len(cap.rows) != 0 {
		t.Fatalf("authoritative buffer: legacy writer must be skipped, got %d rows", len(cap.rows))
	}
	store.DebugFlush()
	if got := countTraceSpans(t, store, "api-gateway"); got == 0 {
		t.Fatal("authoritative buffer: buffer should still receive the rows")
	}

	// Reverted: legacy writer receives rows again.
	SetBufferAuthoritative(false)
	lr2 := mk()
	a.MustAddRows(lr2)
	logstorage.PutLogRows(lr2)
	if len(cap.rows) != 1 {
		t.Fatalf("reverted: legacy writer should receive 1 row, got %d", len(cap.rows))
	}
}

func countTraceSpans(t *testing.T, store *membuffer.Store, svc string) int64 {
	t.Helper()
	q, _ := logstorage.ParseQueryAtTimestamp(`_stream:{service.name="`+svc+`"}`, time.Now().UnixNano())
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{{}}, q, false, nil)
	var n atomic.Int64
	_ = store.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) { n.Add(int64(db.RowsCount())) })
	return n.Load()
}

type denyGate struct{ blocked string }

func (g denyGate) AllowStream(_, _ uint32, stream string) bool {
	return g.blocked == "" || !contains2(stream, g.blocked)
}
func contains2(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestFlushRowKeeper pins the gate-at-flush predicate: it drops VT-internal
// trace_id_idx streams and cardinality-rejected streams, keeps the rest.
func TestFlushRowKeeper(t *testing.T) {
	SetCardinalityGate(denyGate{blocked: "over-limit"})
	defer SetCardinalityGate(nil)
	keep := FlushRowKeeper()

	// trace_id_idx stream → dropped regardless of gate.
	if keep(0, 0, `{trace_id_idx_stream="abc"}`) {
		t.Error("trace_id_idx stream must be dropped")
	}
	// cardinality-rejected stream → dropped.
	if keep(1, 2, `{service.name="over-limit"}`) {
		t.Error("cardinality-rejected stream must be dropped")
	}
	// normal stream → kept.
	if !keep(1, 2, `{service.name="api-gateway"}`) {
		t.Error("normal stream must be kept")
	}
	// service_graph is NOT trace_id_idx → kept (legacy keeps it too).
	if !keep(1, 2, `{service_graph_stream="x"}`) {
		t.Error("service_graph stream must be kept")
	}
}
