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

// TestDataBlockToTraceRows_FieldMapping pins the conversion every Parquet row of
// the Lakehouse goes through: spans added to the insert buffer come back from its
// RunQuery as the exact TraceRow the Parquet file must hold, field for field —
// ids, tenant, promoted columns, durations, timestamps, stream and stream id,
// and attributes.
func TestDataBlockToTraceRows_FieldMapping(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 7, ProjectID: 9}
	now := time.Now().UnixNano()

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

	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer bs.Close()
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	q, _ := logstorage.ParseQueryAtTimestamp("*", now)
	q = q.CloneWithTimeFilter(q.GetTimestamp(), now-int64(time.Hour), now+int64(time.Hour))
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
	if len(got) != 5 {
		t.Fatalf("converted %d rows, want 5", len(got))
	}
	sort.Slice(got, func(i, j int) bool { return got[i].TraceID < got[j].TraceID })

	wantStream := `{k8s.namespace.name="prod",service.name="api-gateway"}`
	st := logstorage.GetStreamTags()
	st.Add("service.name", "api-gateway")
	st.Add("k8s.namespace.name", "prod")
	wantStreamID := computeStreamID(tenant, string(st.MarshalCanonical(nil)))
	logstorage.PutStreamTags(st)

	for i, g := range got {
		switch {
		case g.TraceID != string(rune('a'+i)) || g.SpanID != "s"+string(rune('0'+i)):
			t.Fatalf("row %d ids: got (%s,%s)", i, g.TraceID, g.SpanID)
		case g.AccountID != 7 || g.ProjectID != 9:
			t.Fatalf("row %d tenant: got (%d,%d), want (7,9)", i, g.AccountID, g.ProjectID)
		case g.ServiceName != "api-gateway":
			t.Fatalf("row %d service.name: got %q", i, g.ServiceName)
		case g.K8sNamespaceName != "prod":
			t.Fatalf("row %d k8s.namespace.name: got %q", i, g.K8sNamespaceName)
		case g.SpanName != "GET /x":
			t.Fatalf("row %d name: got %q", i, g.SpanName)
		case g.ParentSpanID != "p":
			t.Fatalf("row %d parent_span_id: got %q", i, g.ParentSpanID)
		case g.DurationNs != 1000:
			t.Fatalf("row %d duration_ns: got %d", i, g.DurationNs)
		case g.StartTimeUnixNano != now:
			t.Fatalf("row %d start_time: got %d want %d", i, g.StartTimeUnixNano, now)
		case g.TimestampUnixNano != now:
			t.Fatalf("row %d timestamp: got %d want %d", i, g.TimestampUnixNano, now)
		case g.Stream != wantStream:
			t.Fatalf("row %d stream: got %q want %q", i, g.Stream, wantStream)
		case g.StreamID != wantStreamID:
			t.Fatalf("row %d stream_id: got %q want %q", i, g.StreamID, wantStreamID)
		case g.HTTPStatusCode != "200":
			t.Fatalf("row %d http.status_code: got %q", i, g.HTTPStatusCode)
		case g.ResourceAttributes["cloud.region"] != "us-east-1" && g.CloudRegion != "us-east-1":
			t.Fatalf("row %d cloud.region: attributes %v, column %q", i, g.ResourceAttributes, g.CloudRegion)
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
