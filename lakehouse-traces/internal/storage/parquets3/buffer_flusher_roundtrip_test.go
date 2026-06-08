package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// TestInteg_FlusherRoundTrip_MultiFilePartition reproduces the live cutover loss
// in a CONTROLLED write→read round-trip: the flusher writes several flush windows
// into the SAME hour partition (one Parquet file each), then we query the whole
// hour back through the real RunQuery path. Every written row must come back.
func TestInteg_FlusherRoundTrip_MultiFilePartition(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	cfg := testConfig()
	writer := NewBatchWriter(&cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	// Mirror production: keys are <account>/<project>/… so the manifest's
	// tenant aggregate is built and GetFilesForRangeTenant can find them.
	writer.SetTenantPrefix(func(a, p uint32) string { return fmt.Sprintf("%d/%d/", a, p) })
	f := &BufferFlusher{writer: writer}

	tenant := logstorage.TenantID{AccountID: 0, ProjectID: 0}
	base := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC) // all within hour=12
	const windows, perWindow = 3, 100
	total := 0
	for w := 0; w < windows; w++ {
		rows := make([]schema.TraceRow, 0, perWindow)
		for i := 0; i < perWindow; i++ {
			ts := base.Add(time.Duration(w)*time.Minute + time.Duration(i)*time.Second).UnixNano()
			rows = append(rows, schema.TraceRow{
				TimestampUnixNano: ts,
				ServiceName:       "api-gateway",
				TraceID:           fmt.Sprintf("t%d-%d", w, i),
				SpanID:            fmt.Sprintf("s%d-%d", w, i),
				Stream:            `{service.name="api-gateway"}`,
			})
			total++
		}
		if err := f.flushCollected(context.Background(), map[logstorage.TenantID][]schema.TraceRow{tenant: rows}); err != nil {
			t.Fatalf("flush window %d: %v", w, err)
		}
	}

	startNs := base.Add(-time.Minute).UnixNano()
	endNs := base.Add(time.Hour).UnixNano()

	// Tenant-scoped lookup (what RunQuery uses) must find all files — a gap vs
	// the non-tenant lookup would be the tenant-key bug.
	if got, all := len(s.manifest.GetFilesForRangeTenant(startNs, endNs, "0", "0")), len(s.manifest.GetFilesForRange(startNs, endNs)); got != all || got != windows {
		t.Fatalf("manifest tenant lookup found %d files, non-tenant %d, wrote %d", got, all, windows)
	}

	q, err := logstorage.ParseQueryAtTimestamp("*", endNs)
	if err != nil {
		t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), startNs, endNs)
	got := 0
	if err := s.RunQuery(context.Background(), []logstorage.TenantID{tenant}, q, func(_ uint, db *logstorage.DataBlock) { got += db.RowsCount() }); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	t.Logf("wrote %d rows across %d files in ONE hour partition; queried back %d", total, windows, got)
	if got != total {
		t.Fatalf("multi-file partition lost rows — wrote %d, read %d (%.0f%%)", total, got, 100*float64(got)/float64(total))
	}
}

// TestInteg_FlusherRoundTrip_EndToEnd is the FULL flusher data flow: ingest into
// a real membuffer, collectWindow (RunQuery → DataBlockToTraceRows reconstruction),
// flushCollected to S3+manifest, then query back through RunQuery. Every ingested
// span must survive the round trip. This is the path the live cutover exercises.
func TestInteg_FlusherRoundTrip_EndToEnd(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	cfg := testConfig()
	writer := NewBatchWriter(&cfg.Insert, s.pool, s.manifest, "traces/", config.ModeTraces)
	writer.SetTenantPrefix(func(a, p uint32) string { return fmt.Sprintf("%d/%d/", a, p) })

	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir(), Retention: time.Hour})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer bs.Close()
	f := &BufferFlusher{writer: writer, buffer: bs, latencyOffset: 0, targetBytes: 1, maxLinger: 1}

	tenant := logstorage.TenantID{AccountID: 0, ProjectID: 0}
	// Recent base so the 1h-retention buffer keeps the rows (GetTenantIDs +
	// RunQuery on the buffer respect retention).
	base := time.Now().Add(-15 * time.Minute).Truncate(time.Second).UnixNano()
	const n = 300
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		ts := base + int64(i)*int64(time.Second)
		lr.MustAdd(tenant, ts, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: fmt.Sprintf("t%d", i)},
			{Name: "span_id", Value: fmt.Sprintf("s%d", i)},
			{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", ts)},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	startNs := base - int64(time.Minute)
	endNs := base + int64(time.Hour)
	collected, nRows, err := f.collectWindow(context.Background(), startNs, endNs)
	if err != nil {
		t.Fatalf("collectWindow: %v", err)
	}
	t.Logf("collected %d rows from buffer", nRows)
	if err := f.flushCollected(context.Background(), collected); err != nil {
		t.Fatalf("flushCollected: %v", err)
	}

	q, _ := logstorage.ParseQueryAtTimestamp("*", endNs)
	q = q.CloneWithTimeFilter(q.GetTimestamp(), startNs, endNs)
	got := 0
	if err := s.RunQuery(context.Background(), []logstorage.TenantID{tenant}, q, func(_ uint, db *logstorage.DataBlock) { got += db.RowsCount() }); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	t.Logf("END-TO-END: ingested %d → collected %d → queried back %d", n, nRows, got)
	if got != n {
		t.Fatalf("REPRO end-to-end: ingested %d, read back %d (%.0f%%)", n, got, 100*float64(got)/float64(n))
	}
}
