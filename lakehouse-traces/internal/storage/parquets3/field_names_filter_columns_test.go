package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A filter selects spans; it does not select the blocks whose columns the answer
// lists (see the logs twin).
func TestTraceFieldNames_FilterKeepsColumnsOfNonMatchingObjectsOfTheStream(t *testing.T) {
	first := []schema.TraceRow{fnSpan(1*time.Minute, "GET", "GET"), fnSpan(2*time.Minute, "GET", "GET")}
	second := []schema.TraceRow{fnSpan(3*time.Minute, "GET", ""), fnSpan(4*time.Minute, "GET", "")}
	for i := range second {
		second[i].SpanAttributes = map[string]string{"other": "b"}
	}
	one := fvcStorage(t, append(append([]schema.TraceRow{}, first...), second...))
	two := fvcStorage(t, first, second)
	for name, s := range map[string]*Storage{"one object": one, "two objects": two} {
		got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "`span_attr:other`:=b"))
		if err != nil {
			t.Fatal(err)
		}
		if m := namesToHits(got); m["span_attr:custom.attr"] != 2 {
			t.Errorf("%s: span_attr:custom.attr hits = %d, want 2: %v", name, m["span_attr:custom.attr"], m)
		}
	}
}

// A stream with no matching span lists no name.
func TestTraceFieldNames_StreamWithoutMatchingSpansListsNoName(t *testing.T) {
	s := fvcStorage(t,
		[]schema.TraceRow{fnSpan(1*time.Minute, "GET", "GET")},
		[]schema.TraceRow{fvcSpan(2*time.Minute, "PUT")},
	)
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "name:=PUT"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if _, ok := m["span_attr:custom.attr"]; ok {
		t.Errorf("span_attr:custom.attr listed although no matching span's stream carries it: %v", m)
	}
	for name, h := range m {
		if h == 0 {
			t.Errorf("%s listed with 0 hits: %v", name, m)
		}
	}
}

// An object whose label counts, pushdown statistics or bloom filter say that no
// span matches the filter is still read: it holds columns of the stream the
// matching object does not (see the logs twin).
func TestTraceFieldNames_FilterDoesNotSkipObjectsOrRowGroupsOfTheStream(t *testing.T) {
	// the same stream, two operation names (a quoted value is what the push-down
	// and label pruning read, hence the hyphens)
	a := fnSpan(1*time.Minute, "GET-1", "GET") // span_attr:custom.attr
	b := fvcSpan(2*time.Minute, "PUT-2")
	a.TraceID, a.SpanID = "ta", "sa"
	b.TraceID, b.SpanID = "tb", "sb"
	b.Stream, b.StreamID, b.ServiceName = a.Stream, a.StreamID, a.ServiceName
	s := fvcStorage(t, []schema.TraceRow{a}, []schema.TraceRow{b})
	skipped0 := metrics.ParquetRowGroupsSkipped.Get("bloom") + metrics.ParquetRowGroupsSkipped.Get("pushdown") + metrics.ParquetRowGroupsSkipped.Get("column_stats")
	for _, filter := range []string{`name:="PUT-2"`, "trace_id:=tb", `name:="PUT-2" AND trace_id:=tb`} {
		got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, filter))
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		if m["span_attr:custom.attr"] != 1 || m["trace_id"] != 1 {
			t.Errorf("%s: span_attr:custom.attr/trace_id hits = %d/%d, want 1/1: %v", filter, m["span_attr:custom.attr"], m["trace_id"], m)
		}
	}
	if d := metrics.ParquetRowGroupsSkipped.Get("bloom") + metrics.ParquetRowGroupsSkipped.Get("pushdown") + metrics.ParquetRowGroupsSkipped.Get("column_stats") - skipped0; d != 0 {
		t.Errorf("a field_names count skipped %d row groups for the row filter's sake", d)
	}
}
