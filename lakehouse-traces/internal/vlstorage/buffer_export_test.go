package vlstorage

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// TestDataBlockToTraceRows_ParityWithLegacy is the P5 export-converter proof: the
// TraceRows reconstructed from the buffer's RunQuery DataBlocks must equal what
// the legacy insert path (logRowsToTraceRows) produced for the same ingest —
// field for field. That guarantees the buffer-sourced Parquet matches the legacy
// Parquet before any cutover.
func TestDataBlockToTraceRows_ParityWithLegacy(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 7, ProjectID: 9}
	now := time.Now().UnixNano()
	mk := func() *logstorage.LogRows {
		lr := logstorage.GetLogRows([]string{"service.name", "k8s.namespace.name"}, nil, nil, nil, "")
		for i := 0; i < 5; i++ {
			lr.MustAdd(tenant, now, []logstorage.Field{
				{Name: "service.name", Value: "api-gateway"},
				{Name: "k8s.namespace.name", Value: "prod"},
				{Name: "trace_id", Value: string(rune('a' + i))},
				{Name: "span_id", Value: "s" + string(rune('0'+i))},
				{Name: "parent_span_id", Value: "p"},
				{Name: "name", Value: "GET /x"},
				{Name: "duration_ns", Value: "1000"},
				{Name: "start_time_unix_nano", Value: itoa(now)},
				{Name: "span_attr:http.status_code", Value: "200"},
				{Name: "resource_attr:cloud.region", Value: "us-east-1"},
			}, 2)
		}
		return lr
	}

	// Legacy reference.
	lrLegacy := mk()
	legacy := logRowsToTraceRows(lrLegacy)
	logstorage.PutLogRows(lrLegacy)

	// Buffer → RunQuery → convert.
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	lrBuf := mk()
	bs.MustAddRows(lrBuf)
	logstorage.PutLogRows(lrBuf)
	bs.DebugFlush()

	q, _ := logstorage.ParseQueryAtTimestamp("*", now)
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
	var mu sync.Mutex
	var got []schema.TraceRow
	if err := bs.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		got = append(got, DataBlockToTraceRows(db, tenant)...)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("runquery: %v", err)
	}

	if len(got) != len(legacy) {
		t.Fatalf("row count: legacy=%d export=%d", len(legacy), len(got))
	}

	key := func(r schema.TraceRow) string { return r.TraceID + "|" + r.SpanID }
	sort.Slice(legacy, func(i, j int) bool { return key(legacy[i]) < key(legacy[j]) })
	sort.Slice(got, func(i, j int) bool { return key(got[i]) < key(got[j]) })

	for i := range legacy {
		l, g := legacy[i], got[i]
		switch {
		case l.TraceID != g.TraceID || l.SpanID != g.SpanID:
			t.Fatalf("row %d id mismatch: legacy=(%s,%s) export=(%s,%s)", i, l.TraceID, l.SpanID, g.TraceID, g.SpanID)
		case l.AccountID != g.AccountID || l.ProjectID != g.ProjectID:
			t.Fatalf("row %d tenant mismatch: legacy=(%d,%d) export=(%d,%d)", i, l.AccountID, l.ProjectID, g.AccountID, g.ProjectID)
		case l.ServiceName != g.ServiceName:
			t.Fatalf("row %d service.name: legacy=%q export=%q", i, l.ServiceName, g.ServiceName)
		case l.K8sNamespaceName != g.K8sNamespaceName:
			t.Fatalf("row %d k8s.namespace.name: legacy=%q export=%q", i, l.K8sNamespaceName, g.K8sNamespaceName)
		case l.SpanName != g.SpanName:
			t.Fatalf("row %d name: legacy=%q export=%q", i, l.SpanName, g.SpanName)
		case l.ParentSpanID != g.ParentSpanID:
			t.Fatalf("row %d parent_span_id: legacy=%q export=%q", i, l.ParentSpanID, g.ParentSpanID)
		case l.DurationNs != g.DurationNs:
			t.Fatalf("row %d duration_ns: legacy=%d export=%d", i, l.DurationNs, g.DurationNs)
		case l.StartTimeUnixNano != g.StartTimeUnixNano:
			t.Fatalf("row %d start_time: legacy=%d export=%d", i, l.StartTimeUnixNano, g.StartTimeUnixNano)
		case l.TimestampUnixNano != g.TimestampUnixNano:
			t.Fatalf("row %d timestamp: legacy=%d export=%d", i, l.TimestampUnixNano, g.TimestampUnixNano)
		case l.Stream != g.Stream:
			t.Fatalf("row %d stream: legacy=%q export=%q", i, l.Stream, g.Stream)
		case l.StreamID != g.StreamID:
			t.Fatalf("row %d stream_id: legacy=%q export=%q", i, l.StreamID, g.StreamID)
		case l.HTTPStatusCode != g.HTTPStatusCode:
			t.Fatalf("row %d http.status_code: legacy=%q export=%q", i, l.HTTPStatusCode, g.HTTPStatusCode)
		case l.ResourceAttributes["cloud.region"] != g.ResourceAttributes["cloud.region"]:
			t.Fatalf("row %d resource_attr cloud.region: legacy=%q export=%q", i, l.ResourceAttributes["cloud.region"], g.ResourceAttributes["cloud.region"])
		}
	}
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
	p := len(b)
	for n > 0 {
		p--
		b[p] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
