package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

type shadowFakeUploader struct {
	mu   sync.Mutex
	keys []string
	data [][]byte
}

func (f *shadowFakeUploader) Upload(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, key)
	cp := make([]byte, len(data))
	copy(cp, data)
	f.data = append(f.data, cp)
	return nil
}

// TestShadowExporter_ExportTenantOnce proves the P5 shadow exporter: it exports
// the buffer window for a tenant to the SHADOW prefix as a valid, readable
// Parquet, and counts rows — the mechanism an operator watches to confirm
// buffer→Parquet parity before cutover.
func TestShadowExporter_ExportTenantOnce(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()

	tenant := logstorage.TenantID{AccountID: 3, ProjectID: 4}
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	const n = 5
	for i := 0; i < n; i++ {
		lr.MustAdd(tenant, now+int64(i), []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: fmt.Sprintf("t%d", i)},
			{Name: "span_id", Value: fmt.Sprintf("s%d", i)},
			{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", now+int64(i))},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	up := &shadowFakeUploader{}
	se := NewShadowExporter(bs, up, "0/0/traces_shadow/", 1000, 3)
	got, err := se.ExportTenantOnce(context.Background(), tenant, now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if got != n {
		t.Fatalf("exported rows: want %d, got %d", n, got)
	}

	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.keys) != 1 {
		t.Fatalf("want 1 shadow upload, got %d", len(up.keys))
	}
	if !strings.HasPrefix(up.keys[0], "0/0/traces_shadow/3_4/") {
		t.Fatalf("shadow key not under the shadow/tenant prefix: %q", up.keys[0])
	}
	// The uploaded object is a valid Parquet with all n rows.
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(up.data[0]))
	defer reader.Close()
	if reader.NumRows() != n {
		t.Fatalf("shadow parquet has %d rows, want %d", reader.NumRows(), n)
	}

	// Empty window → no upload.
	got2, err := se.ExportTenantOnce(context.Background(), tenant, now+int64(time.Hour), now+int64(2*time.Hour))
	if err != nil {
		t.Fatalf("empty export: %v", err)
	}
	if got2 != 0 || len(up.keys) != 1 {
		t.Fatalf("empty window must not upload (got2=%d keys=%d)", got2, len(up.keys))
	}
}
