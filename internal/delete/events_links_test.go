package delete

import (
	"bytes"
	"context"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func spanRowWithExtras(ts int64, tid, sid, svc, eventName string) schema.TraceRow {
	var c schema.SpanSubFieldCollector
	c.Add("event:event_name:0", eventName)
	c.Add("event:event_attr:exception.type:0", "IOError")
	c.Add("link:link_span_id:0", "00000000000000aa")
	r := schema.TraceRow{TimestampUnixNano: ts, TraceID: tid, SpanID: sid, SpanName: "op", ServiceName: svc,
		ScopeAttributes: map[string]string{"scope.key": "sv"}}
	c.Apply(&r)
	return r
}

// A delete rewrite keeps the events, links and scope attributes of the spans it
// keeps, and a delete filter can select on an event field.
func TestRewriteFile_Traces_KeepsEventsLinksAndMatchesEventFields(t *testing.T) {
	pool := newMockRewriterPool()
	rows := []schema.TraceRow{
		spanRowWithExtras(1000, "t1", "s1", "svc", "exception"),
		spanRowWithExtras(2000, "t2", "s2", "svc", "retry"),
		{TimestampUnixNano: 3000, TraceID: "t3", SpanID: "s3", SpanName: "op", ServiceName: "svc"},
	}
	key := "traces/dt=2026-05-02/hour=10/batch-ev.parquet"
	pool.Put(key, buildTestTraceParquet(t, rows))

	rw := NewRewriter(pool, "traces/", 1000, "traces")
	res, err := rw.RewriteFile(context.Background(), key, []Tombstone{
		{Tenants: []TenantRef{{}}, ID: "ts1", Query: `"event:event_name:0":="retry"`, StartNs: 0, EndNs: 10000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsRemoved != 1 || res.RowsKept != 2 {
		t.Fatalf("removed %d kept %d, want 1 and 2", res.RowsRemoved, res.RowsKept)
	}
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(mustGet(t, pool, res.NewKey)))
	defer func() { _ = reader.Close() }()
	got := make([]schema.TraceRow, 5)
	n, _ := reader.Read(got)
	if n != 2 {
		t.Fatalf("read %d rows", n)
	}
	for _, r := range got[:n] {
		switch r.SpanID {
		case "s1":
			if r.EventsJSON != rows[0].EventsJSON || r.LinksJSON != rows[0].LinksJSON || r.ScopeAttributes["scope.key"] != "sv" {
				t.Errorf("kept span lost its extras: %+v", r)
			}
		case "s3":
			if r.EventsJSON != "" || r.LinksJSON != "" {
				t.Errorf("span without extras gained some: %+v", r)
			}
		default:
			t.Errorf("unexpected span %s", r.SpanID)
		}
	}
}
