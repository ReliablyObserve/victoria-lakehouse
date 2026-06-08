package parquets3

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
)

func ingestLog(t *testing.T, st *membuffer.Store, tenant logstorage.TenantID, ts int64, svc, msg string) {
	t.Helper()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	lr.MustAdd(tenant, ts, []logstorage.Field{
		{Name: "service.name", Value: svc},
		{Name: "_msg", Value: msg},
		{Name: "level", Value: "INFO"},
	}, 1)
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}

// TestBufferFlusher_CollectAppliesGateFilter pins the gate-at-flush (logs): the
// buffer holds raw rows, but collectTenantRows returns ONLY rows the keep filter
// approves — so a buffer flush writes the same Parquet the legacy gated path would.
func TestBufferFlusher_CollectAppliesGateFilter(t *testing.T) {
	st, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	tenant := logstorage.TenantID{AccountID: 1, ProjectID: 2}
	now := time.Now().UnixNano()
	ingestLog(t, st, tenant, now, "api-gateway", "m1")
	ingestLog(t, st, tenant, now, "api-gateway", "m2")
	ingestLog(t, st, tenant, now, "blocked-svc", "m3")
	st.DebugFlush()

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
		t.Fatalf("keep filter should see all 3 rows, got %d", keepCalls)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 kept rows (blocked-svc dropped), got %d", len(rows))
	}
	for _, r := range rows {
		if contains(r.Stream, "blocked-svc") {
			t.Fatalf("blocked-svc row leaked past the gate: %q", r.Stream)
		}
		if r.AccountID != tenant.AccountID || r.ProjectID != tenant.ProjectID {
			t.Fatalf("tenant mismatch: %+v", r)
		}
	}

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

// TestBufferFlusher_Watermark pins crash-safe idempotency (round-trip,
// missing/corrupt fallback, atomic write).
func TestBufferFlusher_Watermark(t *testing.T) {
	dir := t.TempDir()
	f := NewBufferFlusher(nil, nil, dir, nil)

	if got := f.loadWatermark(12345); got != 12345 {
		t.Fatalf("missing watermark: want fallback 12345, got %d", got)
	}
	if err := f.saveWatermark(99999); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := f.loadWatermark(12345); got != 99999 {
		t.Fatalf("after save: want 99999, got %d", got)
	}
	if err := os.WriteFile(f.watermarkPath, []byte("{not json"), 0o640); err != nil {
		t.Fatal(err)
	}
	if got := f.loadWatermark(7); got != 7 {
		t.Fatalf("corrupt watermark: want fallback 7, got %d", got)
	}
	_ = f.saveWatermark(42)
	if _, err := os.ReadFile(filepath.Join(dir, "buffer_flush_watermark.json.tmp")); err == nil {
		t.Fatal("temp watermark file should not persist after rename")
	}
}
