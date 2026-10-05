package parquets3

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
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

	s := &Storage{localBuffer: snapshotBuffer{bs.Snapshot()}}

	run := func(qStr string) int64 {
		q, err := logstorage.ParseQueryAtTimestamp(qStr, now)
		if err != nil {
			t.Fatalf("parse %q: %v", qStr, err)
		}
		var got atomic.Int64
		wb := func(_ uint, db *logstorage.DataBlock) { got.Add(int64(db.RowsCount())) }
		s.queryBufferBridge(context.Background(), now-int64(time.Hour), now+int64(time.Hour), nil,
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

// TestQueryBufferBridge_MultiNodeSkipsLocalBuffer pins the B1 fix: when peers are
// present (multi-pod role=all), the read path must NOT serve only the local
// buffer (which holds just this pod's rows) — it must fall through to the
// BufferBridge fan-out so other pods' unflushed rows are gathered. Without peers
// (single-node), the local buffer is used directly.
func TestQueryBufferBridge_MultiNodeSkipsLocalBuffer(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 4; i++ {
		lr.MustAdd(logstorage.TenantID{}, now, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"}, {Name: "trace_id", Value: "t"},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	q, _ := logstorage.ParseQueryAtTimestamp("*", now)
	run := func(setPeers bool) (served int64, local bool) {
		bb := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: false}, config.ModeTraces)
		if setPeers {
			bb.SetEndpoints([]string{"http://peer-a:20428", "http://peer-b:20428"})
		}
		s := &Storage{localBuffer: snapshotBuffer{bs.Snapshot()}, bufferBridge: bb, cfg: &config.Config{Mode: config.ModeTraces}}
		var got atomic.Int64
		s.queryBufferBridge(context.Background(), now-int64(time.Hour), now+int64(time.Hour), nil,
			q, []logstorage.TenantID{{}}, func(_ uint, db *logstorage.DataBlock) { got.Add(int64(db.RowsCount())) })
		return got.Load(), s.useLocalBuffer()
	}
	if n, local := run(false); n != 4 || !local {
		t.Fatalf("no peers: the local buffer should be used directly, served %d (local=%v), want 4", n, local)
	}
	if n, local := run(true); n != 0 || local {
		t.Fatalf("with peers: the local buffer must be SKIPPED (fall through to the fan-out), served %d (local=%v), want 0", n, local)
	}
}
