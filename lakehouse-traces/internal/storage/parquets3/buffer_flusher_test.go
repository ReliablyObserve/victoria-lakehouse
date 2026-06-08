package parquets3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

func ingestTrace(t *testing.T, st *membuffer.Store, tenant logstorage.TenantID, ts int64, svc, traceID string) {
	t.Helper()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	lr.MustAdd(tenant, ts, []logstorage.Field{
		{Name: "service.name", Value: svc},
		{Name: "trace_id", Value: traceID},
		{Name: "span_id", Value: "s-" + traceID},
		{Name: "name", Value: "op"},
		{Name: "start_time_unix_nano", Value: itoa(ts)},
	}, 1)
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// TestBufferFlusher_CollectAppliesGateFilter pins the gate-at-flush: the buffer
// holds raw rows (incl. streams the authoritative path would drop for cardinality
// or as trace_id_idx), but collectTenantRows must return ONLY rows the keep
// filter approves — so the Parquet a flush writes matches the legacy gated path.
func TestBufferFlusher_CollectAppliesGateFilter(t *testing.T) {
	st, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	tenant := logstorage.TenantID{AccountID: 1, ProjectID: 2}
	now := time.Now().UnixNano()
	ingestTrace(t, st, tenant, now, "api-gateway", "t1")
	ingestTrace(t, st, tenant, now, "api-gateway", "t2")
	ingestTrace(t, st, tenant, now, "blocked-svc", "t3") // dropped by the filter below
	st.DebugFlush()

	// keep filter drops any stream containing "blocked-svc" (stands in for the
	// cardinality-gate / trace_id_idx drop the real filter performs).
	var keepCalls atomic.Int64
	f := &BufferFlusher{
		buffer: st,
		keep: func(_, _ uint32, stream string) bool {
			keepCalls.Add(1)
			return !contains(stream, "blocked-svc")
		},
	}
	rows, err := f.collectTenantRows(context.Background(), tenant, now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if keepCalls.Load() != 3 {
		t.Fatalf("keep filter should be consulted for all 3 rows, got %d", keepCalls.Load())
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 kept rows (blocked-svc dropped), got %d", len(rows))
	}
	for _, r := range rows {
		if contains(r.Stream, "blocked-svc") {
			t.Fatalf("blocked-svc row leaked past the gate: %q", r.Stream)
		}
		if r.AccountID != tenant.AccountID || r.ProjectID != tenant.ProjectID {
			t.Fatalf("tenant mismatch on reconstructed row: %+v", r)
		}
	}

	// nil filter keeps everything.
	f.keep = nil
	all, _ := f.collectTenantRows(context.Background(), tenant, now-int64(time.Hour), now+int64(time.Hour))
	if len(all) != 3 {
		t.Fatalf("nil filter: want all 3, got %d", len(all))
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestBufferFlusher_Watermark pins crash-safe idempotency: the watermark
// round-trips; a missing or corrupt file falls back to the caller's default
// (never re-flushing ancient data); and the write is atomic.
func TestBufferFlusher_Watermark(t *testing.T) {
	dir := t.TempDir()
	f := NewBufferFlusher(nil, nil, dir, nil, 0, 0)

	if got := f.loadWatermark(12345); got != 12345 {
		t.Fatalf("missing watermark: want fallback 12345, got %d", got)
	}
	if err := f.saveWatermark(99999); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := f.loadWatermark(12345); got != 99999 {
		t.Fatalf("after save: want 99999, got %d", got)
	}
	// Corrupt file → fallback.
	if err := os.WriteFile(f.watermarkPath, []byte("{not json"), 0o640); err != nil {
		t.Fatal(err)
	}
	if got := f.loadWatermark(7); got != 7 {
		t.Fatalf("corrupt watermark: want fallback 7, got %d", got)
	}
	// No torn temp file left behind after a successful save.
	_ = f.saveWatermark(42)
	if _, err := os.ReadFile(filepath.Join(dir, "buffer_flush_watermark.json.tmp")); err == nil {
		t.Fatal("temp watermark file should not persist after rename")
	}
}

// TestBufferFlusher_CrashRecovery proves the no-loss durability guarantee: rows
// ingested AFTER the last committed watermark but BEFORE a crash survive, because
// (a) the watermark only advances on flush success and (b) the rows live in the
// persisted buffer. After a simulated crash (Close + reopen the buffer from disk,
// reload the watermark), the un-flushed window is recovered and re-collected — no
// LH WAL needed.
func TestBufferFlusher_CrashRecovery(t *testing.T) {
	dir := t.TempDir()   // buffer data dir (persistent volume)
	wmDir := t.TempDir() // watermark dir (persistent)
	tenant := logstorage.TenantID{AccountID: 1, ProjectID: 2}
	base := time.Now().Add(-time.Hour).UnixNano() // well within retention

	bs, err := membuffer.Open(membuffer.Config{Path: dir, Retention: time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Phase A: ingest [base, base+1min] and "commit" the watermark at base+1min.
	ingestTraceAt(t, bs, tenant, base, base+int64(time.Minute), 50)
	bs.DebugFlush()
	f := NewBufferFlusher(nil, bs, wmDir, nil, 0, 0)
	if err := f.saveWatermark(base + int64(time.Minute)); err != nil {
		t.Fatalf("save wm: %v", err)
	}
	// Phase B: ingest the NEXT window [base+1min, base+2min] — NOT yet flushed.
	ingestTraceAt(t, bs, tenant, base+int64(time.Minute), base+int64(2*time.Minute), 70)
	bs.DebugFlush()

	// CRASH: close the buffer (flushes its parts to disk) and drop the flusher.
	bs.Close()

	// RECOVER: reopen the SAME dir (logstorage restores its parts) + reload wm.
	bs2, err := membuffer.Open(membuffer.Config{Path: dir, Retention: time.Hour})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs2.Close()
	f2 := NewBufferFlusher(nil, bs2, wmDir, nil, 0, 0)
	last := f2.loadWatermark(time.Now().UnixNano())
	if last != base+int64(time.Minute) {
		t.Fatalf("recovered watermark = %d, want %d (last committed boundary)", last, base+int64(time.Minute))
	}
	// The un-flushed window (last, base+2min] must be fully recoverable.
	_, n, err := f2.collectWindow(context.Background(), last, base+int64(2*time.Minute))
	if err != nil {
		t.Fatalf("collect after recovery: %v", err)
	}
	if n != 70 {
		t.Fatalf("CRASH-RECOVERY LOSS: want 70 un-flushed rows recovered, got %d", n)
	}
}

func ingestTraceAt(t *testing.T, st *membuffer.Store, tenant logstorage.TenantID, startNs, endNs int64, n int) {
	t.Helper()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	step := (endNs - startNs) / int64(n)
	for i := 0; i < n; i++ {
		ts := startNs + int64(i)*step
		lr.MustAdd(tenant, ts, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: fmt.Sprintf("t%d-%d", startNs, i)},
			{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", ts)},
		}, 1)
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}
