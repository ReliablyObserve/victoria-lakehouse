package vlstorage

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestDataBlockToLogRows_ParityWithLegacy is the WAL-cutover oracle for logs: the
// LogRows reconstructed from the buffer's RunQuery DataBlocks must equal what the
// legacy insert path (logRowsToSchemaRows) produced for the same ingest — field
// for field. That guarantees a buffer-sourced flush writes the same Parquet the
// legacy []LogRow path would, before the authoritative cutover.
func TestDataBlockToLogRows_ParityWithLegacy(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 7, ProjectID: 9}
	// Microsecond-aligned timestamp so VL's _time round-trips exactly (the
	// converter notes sub-microsecond truncation; this test isolates the
	// field-mapping parity from that separately-tracked precision question).
	now := (time.Now().UnixNano() / 1000) * 1000

	mk := func() *logstorage.LogRows {
		lr := logstorage.GetLogRows([]string{"service.name", "k8s.namespace.name"}, nil, nil, nil, "")
		for i := 0; i < 5; i++ {
			// Stream fields FIRST (streamFieldsLen=2), then regular fields —
			// matching real OTLP ingest where service.name/k8s.* are stream
			// fields and _msg is a regular field (never a stream field).
			lr.MustAdd(tenant, now+int64(i)*1000, []logstorage.Field{
				{Name: "service.name", Value: "checkout"},
				{Name: "k8s.namespace.name", Value: "prod"},
				{Name: "_msg", Value: "log line " + string(rune('a'+i))},
				{Name: "level", Value: "ERROR"},
				{Name: "trace_id", Value: "t" + string(rune('0'+i))},
				{Name: "log_attr:http.status", Value: "500"},
			}, 2)
		}
		return lr
	}

	lrLegacy := mk()
	legacy := logRowsToSchemaRows(lrLegacy)
	logstorage.PutLogRows(lrLegacy)
	if len(legacy) != 5 {
		t.Fatalf("legacy produced %d rows, want 5", len(legacy))
	}

	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	lrBuf := mk()
	bs.MustAddRows(lrBuf)
	logstorage.PutLogRows(lrBuf)
	bs.DebugFlush()

	q, _ := logstorage.ParseQueryAtTimestamp("*", now+int64(time.Hour))
	q = q.CloneWithTimeFilter(q.GetTimestamp(), now-int64(time.Hour), now+int64(time.Hour))
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
	var mu sync.Mutex
	var got []schema.LogRow
	if err := bs.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		got = append(got, DataBlockToLogRows(db, tenant)...)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("runquery: %v", err)
	}
	if len(got) != len(legacy) {
		t.Fatalf("row count: legacy=%d export=%d", len(legacy), len(got))
	}

	key := func(r schema.LogRow) string { return r.Body }
	sort.Slice(legacy, func(i, j int) bool { return key(legacy[i]) < key(legacy[j]) })
	sort.Slice(got, func(i, j int) bool { return key(got[i]) < key(got[j]) })

	for i := range legacy {
		l, g := legacy[i], got[i]
		switch {
		case l.Body != g.Body:
			t.Fatalf("row %d body: legacy=%q export=%q", i, l.Body, g.Body)
		case l.AccountID != g.AccountID || l.ProjectID != g.ProjectID:
			t.Fatalf("row %d tenant: legacy=(%d,%d) export=(%d,%d)", i, l.AccountID, l.ProjectID, g.AccountID, g.ProjectID)
		case l.ServiceName != g.ServiceName:
			t.Fatalf("row %d service.name: legacy=%q export=%q", i, l.ServiceName, g.ServiceName)
		case l.SeverityText != g.SeverityText:
			t.Fatalf("row %d severity_text: legacy=%q export=%q", i, l.SeverityText, g.SeverityText)
		case l.K8sNamespaceName != g.K8sNamespaceName:
			t.Fatalf("row %d k8s.namespace.name: legacy=%q export=%q", i, l.K8sNamespaceName, g.K8sNamespaceName)
		case l.TraceID != g.TraceID:
			t.Fatalf("row %d trace_id: legacy=%q export=%q", i, l.TraceID, g.TraceID)
		case l.TimestampUnixNano != g.TimestampUnixNano:
			t.Fatalf("row %d timestamp: legacy=%d export=%d (delta %dns)", i, l.TimestampUnixNano, g.TimestampUnixNano, l.TimestampUnixNano-g.TimestampUnixNano)
		case l.Stream != g.Stream:
			t.Fatalf("row %d stream: legacy=%q export=%q", i, l.Stream, g.Stream)
		case l.StreamID != g.StreamID:
			t.Fatalf("row %d stream_id: legacy=%q export=%q", i, l.StreamID, g.StreamID)
		case l.LogAttributes["http.status"] != g.LogAttributes["http.status"]:
			t.Fatalf("row %d log_attr http.status: legacy=%q export=%q", i, l.LogAttributes["http.status"], g.LogAttributes["http.status"])
		}
	}
}
