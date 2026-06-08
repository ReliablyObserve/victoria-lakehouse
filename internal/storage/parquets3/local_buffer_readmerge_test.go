package parquets3

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
)

// TestQueryBufferBridge_LocalBufferServesRecent is the P3 read-merge proof
// (logs): with a co-located logstorage-native buffer wired via SetLocalBuffer,
// queryBufferBridge serves the recent window from it through RunQuery — no
// struct→DataBlock conversion.
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
			{Name: "service.name", Value: "checkout"},
			{Name: "_msg", Value: "event"},
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
		var got, emitted atomic.Int64
		wb := func(_ uint, db *logstorage.DataBlock) { got.Add(int64(db.RowsCount())) }
		s.queryBufferBridge(context.Background(), now-int64(time.Hour), now+int64(time.Hour),
			0, &emitted, 0, q, []logstorage.TenantID{{}}, wb)
		return got.Load()
	}

	if got := run(`_stream:{service.name="checkout"}`); got != n {
		t.Fatalf("stream filter via local buffer: want %d, got %d", n, got)
	}
	if got := run(`_stream:{service.name="other"}`); got != 0 {
		t.Fatalf("non-matching stream: want 0, got %d", got)
	}
}
