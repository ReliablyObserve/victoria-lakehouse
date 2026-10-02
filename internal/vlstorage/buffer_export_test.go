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

// TestDataBlockToLogRows_FieldMapping pins the conversion every Parquet row of
// the Lakehouse goes through: rows added to the insert buffer come back from its
// RunQuery as the exact LogRow the Parquet file must hold, field for field —
// body, tenant, promoted columns, severity, trace id, timestamp, stream and
// stream id, and attributes.
func TestDataBlockToLogRows_FieldMapping(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 7, ProjectID: 9}
	// Microsecond-aligned timestamp so VL's _time round-trips exactly (the
	// converter notes sub-microsecond truncation; this test isolates the
	// field mapping from that separately-tracked precision question).
	now := (time.Now().UnixNano() / 1000) * 1000

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

	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
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
	if len(got) != 5 {
		t.Fatalf("converted %d rows, want 5", len(got))
	}
	sort.Slice(got, func(i, j int) bool { return got[i].Body < got[j].Body })

	for i, g := range got {
		want := schema.LogRow{
			AccountID: 7, ProjectID: 9,
			Body:              "log line " + string(rune('a'+i)),
			ServiceName:       "checkout",
			K8sNamespaceName:  "prod",
			SeverityText:      "ERROR",
			TraceID:           "t" + string(rune('0'+i)),
			TimestampUnixNano: now + int64(i)*1000,
			Stream:            `{k8s.namespace.name="prod",service.name="checkout"}`,
			StreamID:          computeStreamID(tenant, canonicalStream(t, "service.name", "checkout", "k8s.namespace.name", "prod")),
		}
		switch {
		case g.Body != want.Body:
			t.Fatalf("row %d body: got %q want %q", i, g.Body, want.Body)
		case g.AccountID != want.AccountID || g.ProjectID != want.ProjectID:
			t.Fatalf("row %d tenant: got (%d,%d) want (%d,%d)", i, g.AccountID, g.ProjectID, want.AccountID, want.ProjectID)
		case g.ServiceName != want.ServiceName:
			t.Fatalf("row %d service.name: got %q want %q", i, g.ServiceName, want.ServiceName)
		case g.SeverityText != want.SeverityText:
			t.Fatalf("row %d severity_text: got %q want %q", i, g.SeverityText, want.SeverityText)
		case g.K8sNamespaceName != want.K8sNamespaceName:
			t.Fatalf("row %d k8s.namespace.name: got %q want %q", i, g.K8sNamespaceName, want.K8sNamespaceName)
		case g.TraceID != want.TraceID:
			t.Fatalf("row %d trace_id: got %q want %q", i, g.TraceID, want.TraceID)
		case g.TimestampUnixNano != want.TimestampUnixNano:
			t.Fatalf("row %d timestamp: got %d want %d (delta %dns)", i, g.TimestampUnixNano, want.TimestampUnixNano, g.TimestampUnixNano-want.TimestampUnixNano)
		case g.Stream != want.Stream:
			t.Fatalf("row %d stream: got %q want %q", i, g.Stream, want.Stream)
		case g.StreamID != want.StreamID:
			t.Fatalf("row %d stream_id: got %q want %q", i, g.StreamID, want.StreamID)
		case g.LogAttributes["log_attr:http.status"] != "500":
			t.Fatalf("row %d log_attr:http.status: got %v want 500", i, g.LogAttributes)
		}
	}
}

// canonicalStream returns the canonical encoding of the stream tags (name, value
// pairs) the way VictoriaLogs hashes them into the stream id.
func canonicalStream(t *testing.T, kv ...string) string {
	t.Helper()
	st := logstorage.GetStreamTags()
	defer logstorage.PutStreamTags(st)
	for i := 0; i+1 < len(kv); i += 2 {
		st.Add(kv[i], kv[i+1])
	}
	return string(st.MarshalCanonical(nil))
}
