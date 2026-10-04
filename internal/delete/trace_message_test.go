package delete

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestTraceMessageTombstoneProvenance(t *testing.T) {
	row := schema.TraceRow{TimestampUnixNano: 123, Body: "actualnative", SpanName: "operation", SpanAttributes: map[string]string{"_msg": "customer", "body": "spanbody"}, ResourceAttributes: map[string]string{"_msg": "resource", "body": "resourcebody"}, ScopeAttributes: map[string]string{"_msg": "scope", "body": "scopebody"}}
	for _, tc := range []struct {
		q    string
		want bool
	}{
		{`_msg:=actualnative`, true},
		{`_msg:=customer`, false},
		{`"span_attr:_msg":=customer`, true},
		{`"resource_attr:_msg":=resource`, true},
		{`"scope_attr:_msg":=scope`, true},
		{`"span_attr:body":=spanbody`, true},
		{`"resource_attr:body":=resourcebody`, true},
		{`"scope_attr:body":=scopebody`, true},
	} {
		t.Run(tc.q, func(t *testing.T) {
			ts := Tombstone{Query: tc.q, StartNs: 0, EndNs: 1000}
			if got := ts.MatchesFields(TraceRowFields(&row), row.TimestampUnixNano); got != tc.want {
				t.Fatalf("physical trace-delete predicate %q=%v want=%v fields=%v", tc.q, got, tc.want, TraceRowFields(&row))
			}
		})
	}
}

func TestTraceMessageLiteralReservedNamesStayDistinct(t *testing.T) {
	row := schema.TraceRow{TimestampUnixNano: 123, Body: "native", SpanAttributes: map[string]string{}}
	for _, key := range []string{"_msg", "body", "span_attr:_msg", "resource_attr:_msg", "scope_attr:_msg", "span_attr:span_attr:_msg", "span_attr:body"} {
		row.SpanAttributes[key] = key + " value"
	}
	fields := TraceRowFields(&row)
	for key, value := range row.SpanAttributes {
		ts := Tombstone{Query: `"span_attr:` + key + `":="` + value + `"`, StartNs: 0, EndNs: 1000}
		if !ts.MatchesFields(fields, 123) {
			t.Fatalf("literal customer key lost %s: %v", key, fields)
		}
	}
}

func TestReviewTraceMessageReservedMapNamesDoNotOverrideNative(t *testing.T) {
	for _, source := range []string{"resource", "span", "scope"} {
		for _, key := range []string{"_msg", "body"} {
			t.Run(source+"/"+key, func(t *testing.T) {
				row := schema.TraceRow{TimestampUnixNano: 123, Body: "actualnative", SpanName: "operation"}
				attrs := map[string]string{key: "customer"}
				switch source {
				case "resource":
					row.ResourceAttributes = attrs
				case "span":
					row.SpanAttributes = attrs
				case "scope":
					row.ScopeAttributes = attrs
				}
				for _, tc := range []struct {
					q    string
					want bool
				}{
					{`_msg:=actualnative`, true}, {`_msg:=customer`, false},
					{fmt.Sprintf(`"%s_attr:%s":=customer`, source, key), true},
				} {
					ts := Tombstone{Query: tc.q, StartNs: 0, EndNs: 1000}
					if got := ts.MatchesFields(TraceRowFields(&row), 123); got != tc.want {
						t.Errorf("reserved-map trace-delete %q=%v want=%v fields=%v", tc.q, got, tc.want, TraceRowFields(&row))
					}
				}
			})
		}
	}
}
