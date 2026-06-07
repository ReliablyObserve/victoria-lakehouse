package vlstorage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type captureLogWriter struct{ rows []schema.LogRow }

func (c *captureLogWriter) MustAddLogRows(rows []schema.LogRow) { c.rows = append(c.rows, rows...) }
func (c *captureLogWriter) CanWriteData() error                 { return nil }

// TestDualWrite_LogsLegacyAndBufferParity is the logs-side Phase-1 parity proof:
// one ingested LogRows batch through insertAdapter.MustAddRows feeds BOTH the
// legacy LogRow path and the logstorage-native buffer, and the buffer answers
// the same count via RunQuery — zero struct→DataBlock conversion on the query
// side.
func TestDualWrite_LogsLegacyAndBufferParity(t *testing.T) {
	store, err := membuffer.Open(membuffer.Config{Path: t.TempDir(), Retention: time.Hour})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer store.Close()

	cap := &captureLogWriter{}
	a := &insertAdapter{writer: cap}
	SetBufferStore(store)
	defer SetBufferStore(nil)

	const n = 8
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		lr.MustAdd(logstorage.TenantID{}, now, []logstorage.Field{
			{Name: "service.name", Value: "checkout"},
			{Name: "_msg", Value: fmt.Sprintf("event %d", i)},
		}, -1)
	}
	a.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	store.DebugFlush()

	if len(cap.rows) != n {
		t.Fatalf("legacy LogWriter: want %d rows, got %d", n, len(cap.rows))
	}

	q, err := logstorage.ParseQueryAtTimestamp(`_stream:{service.name="checkout"}`, now)
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
		t.Fatalf("buffer store: want %d rows for service.name=checkout, got %d", n, got.Load())
	}
}
