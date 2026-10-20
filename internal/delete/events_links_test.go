package delete

import (
	"testing"

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

// A delete filter can select on a span's event field, and hiding one span leaves
// the events, links and scope attributes of the others exactly as they are: the
// tombstone matches the one span and no row is rewritten (compaction's copy of
// events and links is covered in internal/compaction).
func TestDeleteFilter_Traces_MatchesEventFields(t *testing.T) {
	rows := []schema.TraceRow{
		spanRowWithExtras(1000, "t1", "s1", "svc", "exception"),
		spanRowWithExtras(2000, "t2", "s2", "svc", "retry"),
		{TimestampUnixNano: 3000, TraceID: "t3", SpanID: "s3", SpanName: "op", ServiceName: "svc"},
	}
	before := make([]schema.TraceRow, len(rows))
	copy(before, rows)

	ts := Tombstone{Tenants: []TenantRef{{}}, ID: "ts1", Query: `"event:event_name:0":="retry"`, StartNs: 0, EndNs: 10000, Mode: "hide"}
	if err := ts.Validate(); err != nil {
		t.Fatal(err)
	}
	hidden := map[string]bool{}
	for _, r := range rows {
		fields := map[string]string{"trace_id": r.TraceID, "span_id": r.SpanID, "service.name": r.ServiceName}
		for _, col := range []struct{ name, json string }{{schema.ColSpanEventsJSON, r.EventsJSON}, {schema.ColSpanLinksJSON, r.LinksJSON}} {
			if err := schema.ForEachSpanSubField(col.name, col.json, func(name, value string) { fields[name] = value }); err != nil {
				t.Fatal(err)
			}
		}
		if ts.MatchesRow(fields, r.TimestampUnixNano) {
			hidden[r.SpanID] = true
		}
	}
	if len(hidden) != 1 || !hidden["s2"] {
		t.Fatalf("hidden spans = %v, want only s2 (the one whose event is %q)", hidden, "retry")
	}
	for i := range rows {
		if rows[i].EventsJSON != before[i].EventsJSON || rows[i].LinksJSON != before[i].LinksJSON {
			t.Errorf("span %s lost its events or links", rows[i].SpanID)
		}
	}
	if rows[0].EventsJSON == "" || rows[0].LinksJSON == "" || rows[0].ScopeAttributes["scope.key"] != "sv" {
		t.Errorf("fixture: the kept span has no extras: %+v", rows[0])
	}
	if rows[2].EventsJSON != "" || rows[2].LinksJSON != "" {
		t.Errorf("a span without extras gained some: %+v", rows[2])
	}
}
