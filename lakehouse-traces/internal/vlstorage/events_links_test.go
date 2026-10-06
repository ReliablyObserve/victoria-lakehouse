package vlstorage

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The schema package hard-codes VictoriaTraces' event/link field prefixes (it
// cannot import VictoriaTraces); this keeps the two in step.
func TestSchemaPrefixesMatchVictoriaTraces(t *testing.T) {
	if schema.EventFieldPrefix != otelpb.EventPrefix || schema.LinkFieldPrefix != otelpb.LinkPrefix {
		t.Fatalf("schema prefixes %q/%q differ from otelpb %q/%q",
			schema.EventFieldPrefix, schema.LinkFieldPrefix, otelpb.EventPrefix, otelpb.LinkPrefix)
	}
}

// vtSpanFields is a span's fields exactly as VictoriaTraces'
// pushFieldsFromSpan lays them out (app/vtinsert/opentelemetry): n events, m
// links, scope attributes, the metadata fields.
func vtSpanFields(nEvents, nLinks int) []logstorage.Field {
	f := []logstorage.Field{
		{Name: otelpb.ResourceAttrServiceName, Value: "checkout"},
		{Name: otelpb.InstrumentationScopeName, Value: "io.opentelemetry.http"},
		{Name: otelpb.InstrumentationScopeVersion, Value: "1.2.3"},
		{Name: otelpb.InstrumentationScopeAttrPrefix + "scope.key", Value: "scope-value"},
		{Name: otelpb.InstrumentationScopeAttrPrefix + "otel.scope.dotted.key", Value: "-"},
		{Name: otelpb.SpanIDField, Value: "aaaaaaaaaaaaaaaa"},
		{Name: otelpb.NameField, Value: "POST /pay"},
		{Name: otelpb.StartTimeUnixNanoField, Value: "1000000000"},
		{Name: otelpb.EndTimeUnixNanoField, Value: "2000000000"},
		{Name: otelpb.DroppedEventsCountField, Value: "2"},
		{Name: otelpb.DroppedLinksCountField, Value: "1"},
	}
	for i := 0; i < nEvents; i++ {
		s := fmt.Sprintf(":%d", i)
		f = append(f,
			logstorage.Field{Name: otelpb.EventPrefix + otelpb.EventTimeUnixNanoField + s, Value: fmt.Sprintf("15000000%02d", i)},
			logstorage.Field{Name: otelpb.EventPrefix + otelpb.EventNameField + s, Value: "exception"},
			logstorage.Field{Name: otelpb.EventPrefix + otelpb.EventDroppedAttributesCountField + s, Value: "0"},
			logstorage.Field{Name: otelpb.EventPrefix + otelpb.EventAttrPrefix + "exception.type" + s, Value: "IOError"},
			logstorage.Field{Name: otelpb.EventPrefix + otelpb.EventAttrPrefix + "exception.stacktrace" + s, Value: "at a.b\nat c.d"},
		)
	}
	for i := 0; i < nLinks; i++ {
		s := fmt.Sprintf(":%d", i)
		f = append(f,
			logstorage.Field{Name: otelpb.LinkPrefix + otelpb.LinkTraceIDField + s, Value: fmt.Sprintf("%032x", i+1)},
			logstorage.Field{Name: otelpb.LinkPrefix + otelpb.LinkSpanIDField + s, Value: fmt.Sprintf("%016x", i+1)},
			logstorage.Field{Name: otelpb.LinkPrefix + otelpb.LinkTraceStateField + s, Value: "vendor=x"},
			logstorage.Field{Name: otelpb.LinkPrefix + otelpb.LinkDroppedAttributesCountField + s, Value: "0"},
			logstorage.Field{Name: otelpb.LinkPrefix + otelpb.LinkFlagsField + s, Value: "257"},
			logstorage.Field{Name: otelpb.LinkPrefix + otelpb.LinkAttrPrefix + "link.reason" + s, Value: "retry"},
		)
	}
	f = append(f,
		logstorage.Field{Name: "_msg", Value: "-"},
		logstorage.Field{Name: otelpb.TraceIDField, Value: "0123456789abcdef0123456789abcdef"},
	)
	return f
}

func wantSubFields(fields []logstorage.Field) map[string]string {
	want := map[string]string{}
	for _, f := range fields {
		if strings.HasPrefix(f.Name, otelpb.EventPrefix) || strings.HasPrefix(f.Name, otelpb.LinkPrefix) {
			want[f.Name] = f.Value
		}
	}
	return want
}

func decodedSubFields(t *testing.T, row schema.TraceRow) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, c := range []struct{ col, js string }{
		{schema.ColSpanEventsJSON, row.EventsJSON},
		{schema.ColSpanLinksJSON, row.LinksJSON},
	} {
		if err := schema.ForEachSpanSubField(c.col, c.js, func(n, v string) { got[n] = v }); err != nil {
			t.Fatalf("decode %s: %v", c.col, err)
		}
	}
	return got
}

// The fix for the permanent loss of events, links and scope attributes at
// flush: the real insert buffer, then DataBlockToTraceRows (the flush and the
// bridge conversion), must keep every one of them.
func TestDataBlockToTraceRows_KeepsEventsLinksAndScopeAttributes(t *testing.T) {
	for _, tc := range []struct{ events, links int }{{0, 0}, {1, 0}, {0, 2}, {3, 2}, {12, 11}} {
		t.Run(fmt.Sprintf("events%d_links%d", tc.events, tc.links), func(t *testing.T) {
			fields := vtSpanFields(tc.events, tc.links)
			lr := makeLogRows(t, fields...)
			defer logstorage.PutLogRows(lr)
			rows := rowsViaBuffer(t, lr)
			if len(rows) != 1 {
				t.Fatalf("want 1 row, got %d", len(rows))
			}
			row := rows[0]

			if got, want := decodedSubFields(t, row), wantSubFields(fields); !reflect.DeepEqual(got, want) {
				t.Fatalf("event/link fields differ:\n got %v\nwant %v", got, want)
			}
			if (tc.events == 0) != (row.EventsJSON == "") || (tc.links == 0) != (row.LinksJSON == "") {
				t.Fatalf("a span without events/links must leave the columns empty (NULL): %q %q", row.EventsJSON, row.LinksJSON)
			}
			wantScope := map[string]string{"scope.key": "scope-value", "otel.scope.dotted.key": "-"}
			if !reflect.DeepEqual(row.ScopeAttributes, wantScope) {
				t.Fatalf("ScopeAttributes = %v, want %v", row.ScopeAttributes, wantScope)
			}
			if row.ScopeName != "io.opentelemetry.http" || row.SpanAttributes[otelpb.InstrumentationScopeVersion] != "1.2.3" {
				t.Fatalf("scope name/version lost: %q %v", row.ScopeName, row.SpanAttributes)
			}
			// Nothing leaks into the attribute maps: event/link/scope fields are
			// not span attributes.
			for k := range row.SpanAttributes {
				if strings.HasPrefix(k, otelpb.EventPrefix) || strings.HasPrefix(k, otelpb.LinkPrefix) || strings.HasPrefix(k, otelpb.InstrumentationScopeAttrPrefix) {
					t.Errorf("SpanAttributes leaked %q", k)
				}
			}
			if len(row.ResourceAttributes) != 0 {
				t.Errorf("ResourceAttributes = %v", row.ResourceAttributes)
			}
		})
	}
}

// mapFieldToTraceRow alone cannot place an indexed event/link field; it must
// neither store it as an attribute nor panic.
func TestMapFieldToTraceRow_SingleEventLinkFieldIsNotAnAttribute(t *testing.T) {
	var row schema.TraceRow
	mapFieldToTraceRow(&row, otelpb.EventPrefix+otelpb.EventNameField+":0", "exception")
	mapFieldToTraceRow(&row, otelpb.LinkPrefix+otelpb.LinkSpanIDField+":0", "abc")
	if len(row.SpanAttributes) != 0 || len(row.ResourceAttributes) != 0 || row.EventsJSON != "" || row.LinksJSON != "" {
		t.Fatalf("row should be untouched: %+v", row)
	}
	mapFieldToTraceRow(&row, otelpb.InstrumentationScopeAttrPrefix+"lib.version", "2.0")
	mapFieldToTraceRow(&row, otelpb.InstrumentationScopeAttrPrefix, "no-key")
	if !reflect.DeepEqual(row.ScopeAttributes, map[string]string{"lib.version": "2.0"}) {
		t.Fatalf("ScopeAttributes = %v", row.ScopeAttributes)
	}
}

// Two spans in one block: one with events, one without. The block lists the
// event columns for both rows; the span without events must stay without.
func TestDataBlockToTraceRows_RowsDoNotShareEvents(t *testing.T) {
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	withEvents := vtSpanFields(2, 1)
	without := []logstorage.Field{
		{Name: otelpb.ResourceAttrServiceName, Value: "checkout"},
		{Name: otelpb.NameField, Value: "POST /pay"},
		{Name: otelpb.SpanIDField, Value: "bbbbbbbbbbbbbbbb"},
		{Name: "_msg", Value: "-"},
		{Name: otelpb.TraceIDField, Value: "ffffffffffffffffffffffffffffffff"},
	}
	lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, withEvents, -1)
	lr.MustAdd(logstorage.TenantID{}, 1_000_000_001, without, -1)
	rows := rowsViaBuffer(t, lr)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	for _, r := range rows {
		got := decodedSubFields(t, r)
		if r.SpanID == "bbbbbbbbbbbbbbbb" {
			if len(got) != 0 || r.ScopeAttributes != nil {
				t.Errorf("span without events picked up %v / %v", got, r.ScopeAttributes)
			}
		} else if !reflect.DeepEqual(got, wantSubFields(withEvents)) {
			t.Errorf("span with events lost fields: %v", got)
		}
	}
}
