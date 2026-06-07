package membuffer

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestStore_AddAndQuery is the Phase-1 foundation check: the buffer store opens,
// accepts native logstorage.LogRows via MustAddRows, and answers queries via the
// SAME engine the S3-Parquet path uses — proving the buffer is directly
// queryable (the whole point of Option B) with zero struct→DataBlock conversion.
func TestStore_AddAndQuery(t *testing.T) {
	st, err := Open(Config{Path: t.TempDir(), Retention: time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	now := time.Now().UnixNano()
	tid := logstorage.TenantID{AccountID: 0, ProjectID: 0}

	// service.name is the single stream field (streamFieldsLen=1); _msg and
	// trace_id are regular fields — mirroring how VT's OTLP ingest builds rows.
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	const n = 5
	for i := 0; i < n; i++ {
		lr.MustAdd(tid, now, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "_msg", Value: "hello"},
			{Name: "trace_id", Value: fmt.Sprintf("t%d", i)},
		}, 1)
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()

	count := func(t *testing.T, qStr string) int64 {
		t.Helper()
		q, err := logstorage.ParseQueryAtTimestamp(qStr, now)
		if err != nil {
			t.Fatalf("parse %q: %v", qStr, err)
		}
		var rows atomic.Int64
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{},
			[]logstorage.TenantID{tid}, q, false, nil)
		if err := st.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			rows.Add(int64(db.RowsCount()))
		}); err != nil {
			t.Fatalf("runquery %q: %v", qStr, err)
		}
		return rows.Load()
	}

	if got := count(t, "*"); got != n {
		t.Fatalf("match-all: want %d buffered rows, got %d", n, got)
	}
	// The data is queryable by its stream selector exactly as the file path /
	// hot VT would answer — the parity that the struct→DataBlock buffer kept
	// breaking (missing _stream etc.).
	if got := count(t, `_stream:{service.name="api-gateway"}`); got != n {
		t.Fatalf("stream filter: want %d rows, got %d", n, got)
	}
	if got := count(t, `trace_id:"t3"`); got != 1 {
		t.Fatalf("trace_id filter: want 1 row, got %d", got)
	}
	if got := count(t, `_stream:{service.name="other"}`); got != 0 {
		t.Fatalf("non-matching stream: want 0 rows, got %d", got)
	}
}
