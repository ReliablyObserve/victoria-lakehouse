package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// TestBufferFlusher_Repro_WindowUnderProduction reproduces the live cutover bug:
// the flusher produced only ~4-8% of ingested spans. It ingests N spans with
// timestamps SPREAD across a window (like real traffic) plus VT trace_id_idx
// index rows, then runs collectTenantRows over the window with the REAL
// FlushRowKeeper. Expectation: all N spans survive (index rows dropped). If far
// fewer survive, the loss is in the query/reconstruct/filter stage.
func TestBufferFlusher_Repro_WindowUnderProduction(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir(), Retention: time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()

	tenant := logstorage.TenantID{AccountID: 1, ProjectID: 2}
	now := time.Now().UnixNano()
	const nSpans = 200

	// Spans spread across the last 50s (varied _time), each ingested with the
	// span fields the trace path uses. streamFieldsLen=1 → service.name is the
	// only stream field (the real ingest promotes service.name + resource attrs).
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < nSpans; i++ {
		ts := now - int64(i)*int64(250*time.Millisecond) // 200 spans over 50s
		lr.MustAdd(tenant, ts, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "trace_id", Value: fmt.Sprintf("t%d", i)},
			{Name: "span_id", Value: fmt.Sprintf("s%d", i)},
			{Name: "name", Value: "op"},
			{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", ts)},
		}, 1)
	}
	// A handful of VT-internal trace_id_idx rows (must be DROPPED by the keeper).
	const nIdx = 40
	lrIdx := logstorage.GetLogRows([]string{"trace_id_idx_stream"}, nil, nil, nil, "")
	for i := 0; i < nIdx; i++ {
		ts := now - int64(i)*int64(time.Second)
		lrIdx.MustAdd(tenant, ts, []logstorage.Field{
			{Name: "trace_id_idx_stream", Value: fmt.Sprintf("%d", i%256)},
			{Name: "trace_id_idx", Value: fmt.Sprintf("t%d", i)},
		}, 1)
	}
	bs.MustAddRows(lr)
	bs.MustAddRows(lrIdx)
	logstorage.PutLogRows(lr)
	logstorage.PutLogRows(lrIdx)
	bs.DebugFlush()

	f := &BufferFlusher{buffer: bs, keep: vlstorage.FlushRowKeeper()}

	// Window covering ALL ingested spans (last 60s .. +1s).
	startNs := now - int64(60*time.Second)
	endNs := now + int64(time.Second)
	rows, err := f.collectTenantRows(context.Background(), tenant, startNs, endNs)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	t.Logf("ingested %d spans + %d index rows; collectTenantRows returned %d", nSpans, nIdx, len(rows))
	idxLeaked := 0
	for _, r := range rows {
		if contains(r.Stream, "trace_id_idx") {
			idxLeaked++
		}
	}
	if idxLeaked != 0 {
		t.Errorf("%d trace_id_idx rows leaked past the keeper", idxLeaked)
	}
	if len(rows) != nSpans {
		t.Fatalf("REPRO: expected %d spans, got %d (%.0f%% — the under-production bug)", nSpans, len(rows), 100*float64(len(rows))/float64(nSpans))
	}
}
