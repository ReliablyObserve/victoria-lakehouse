package parquets3

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// TestQueryBufferBridge_LocalBufferServesRecent is the P3 read-merge proof: with
// a co-located logstorage-native buffer wired via SetLocalBuffer,
// queryBufferBridge serves the recent window from it through the SAME engine
// (RunQuery), with no struct→DataBlock conversion — the path that makes cold
// queries see freshly-ingested spans.
func TestQueryBufferBridge_LocalBufferServesRecent(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer bs.Close()

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	const n = 6
	for i := 0; i < n; i++ {
		lr.MustAdd(logstorage.TenantID{}, now, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: "t"},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	s := &Storage{localBuffer: bs}

	run := func(qStr string) int64 {
		q, err := logstorage.ParseQueryAtTimestamp(qStr, now)
		if err != nil {
			t.Fatalf("parse %q: %v", qStr, err)
		}
		var got atomic.Int64
		wb := func(_ uint, db *logstorage.DataBlock) { got.Add(int64(db.RowsCount())) }
		s.queryBufferBridge(context.Background(), now-int64(time.Hour), now+int64(time.Hour),
			q, []logstorage.TenantID{{}}, wb)
		return got.Load()
	}

	if got := run(`_stream:{service.name="api-gateway"}`); got != n {
		t.Fatalf("stream filter via local buffer: want %d, got %d", n, got)
	}
	if got := run(`_stream:{service.name="other"}`); got != 0 {
		t.Fatalf("non-matching stream: want 0, got %d", got)
	}
	if got := run(`trace_id:"t"`); got != n {
		t.Fatalf("trace_id filter via local buffer: want %d, got %d", n, got)
	}
}
