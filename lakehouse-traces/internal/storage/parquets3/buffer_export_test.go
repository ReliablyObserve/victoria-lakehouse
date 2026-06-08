package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// TestExportBufferToParquet_RoundTrip is the P5 export proof: ingest into the
// buffer, export the window to Parquet via the SAME writer the legacy flush
// uses, then read the Parquet back and confirm the TraceRows carry the ingested
// values. End-to-end RunQuery → DataBlockToTraceRows → writeTracesParquet →
// readable Parquet, with no production impact.
func TestExportBufferToParquet_RoundTrip(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()

	tenant := logstorage.TenantID{AccountID: 3, ProjectID: 4}
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	const n = 5
	want := map[string]string{} // trace_id -> http.status_code
	for i := 0; i < n; i++ {
		tid := fmt.Sprintf("trace-%d", i)
		status := []string{"200", "500", "200", "404", "503"}[i]
		want[tid] = status
		lr.MustAdd(tenant, now+int64(i), []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: tid},
			{Name: "span_id", Value: fmt.Sprintf("s%d", i)},
			{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", now+int64(i))},
			{Name: "span_attr:http.status_code", Value: status},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	data, rawBytes, rowCount, err := ExportBufferToParquet(
		context.Background(), bs, tenant, now-int64(time.Hour), now+int64(time.Hour), 1000, 3)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if rowCount != n {
		t.Fatalf("export rowCount: want %d, got %d", n, rowCount)
	}
	if len(data) == 0 || rawBytes == 0 {
		t.Fatalf("export produced empty parquet (len=%d rawBytes=%d)", len(data), rawBytes)
	}

	// Read the Parquet back with the same schema the file-scan path uses.
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()
	got := make([]schema.TraceRow, reader.NumRows())
	if _, err := reader.Read(got); err != nil && err.Error() != "EOF" {
		t.Fatalf("read parquet back: %v", err)
	}
	if len(got) != n {
		t.Fatalf("read back %d rows, want %d", len(got), n)
	}

	// Verify sorted by timestamp (legacy flush ordering) + values preserved.
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i].TimestampUnixNano < got[j].TimestampUnixNano }) {
		t.Fatal("exported parquet rows not sorted by timestamp")
	}
	for _, r := range got {
		if r.ServiceName != "api-gateway" {
			t.Fatalf("trace %q: service.name=%q", r.TraceID, r.ServiceName)
		}
		if r.AccountID != tenant.AccountID || r.ProjectID != tenant.ProjectID {
			t.Fatalf("trace %q: tenant=(%d,%d)", r.TraceID, r.AccountID, r.ProjectID)
		}
		ws, ok := want[r.TraceID]
		if !ok {
			t.Fatalf("unexpected trace_id %q in exported parquet", r.TraceID)
		}
		if r.HTTPStatusCode != ws {
			t.Fatalf("trace %q: http.status_code want %q got %q", r.TraceID, ws, r.HTTPStatusCode)
		}
		delete(want, r.TraceID)
	}
	if len(want) != 0 {
		t.Fatalf("missing trace_ids in exported parquet: %v", want)
	}
}
