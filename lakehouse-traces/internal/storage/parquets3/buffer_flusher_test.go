package parquets3

import (
	"context"
	"os"
	"path/filepath"
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
	keepCalls := 0
	f := &BufferFlusher{
		buffer: st,
		keep: func(_, _ uint32, stream string) bool {
			keepCalls++
			return !contains(stream, "blocked-svc")
		},
	}
	rows, err := f.collectTenantRows(context.Background(), tenant, now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if keepCalls != 3 {
		t.Fatalf("keep filter should be consulted for all 3 rows, got %d", keepCalls)
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
