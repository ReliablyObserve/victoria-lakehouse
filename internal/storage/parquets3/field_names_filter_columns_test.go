package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A filter selects rows; it does not select the blocks whose columns the answer
// lists. A stream's second object holds none of the matching rows of the first,
// yet the stream is one block on hot storage once merged, so the column the
// first object carries is still listed, credited with the matching rows.
func TestFieldNames_FilterKeepsColumnsOfNonMatchingObjectsOfTheStream(t *testing.T) {
	first := []schema.LogRow{fnRow(1*time.Minute, "INFO", "t1"), fnRow(2*time.Minute, "INFO", "t2")}
	second := []schema.LogRow{fnRow(3*time.Minute, "INFO", ""), fnRow(4*time.Minute, "INFO", "")}
	for i := range second {
		second[i].Body = "y"
	}
	one := fvcStorage(t, append(append([]schema.LogRow{}, first...), second...))
	two := fvcStorage(t, first, second)
	for name, s := range map[string]*Storage{"one object": one, "two objects": two} {
		got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "_msg:=y"))
		if err != nil {
			t.Fatal(err)
		}
		if m := namesToHits(got); m["trace_id"] != 2 {
			t.Errorf("%s: trace_id hits = %d, want 2: %v", name, m["trace_id"], m)
		}
	}
}

// A filter that matches no row of a stream credits that stream's columns with
// nothing: a name with no hit is not listed.
func TestFieldNames_StreamWithoutMatchingRowsListsNoName(t *testing.T) {
	s := fvcStorage(t,
		[]schema.LogRow{fnRow(1*time.Minute, "INFO", "t1")},
		[]schema.LogRow{fvcRow(2*time.Minute, "WARN")},
	)
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "level:=WARN"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if _, ok := m["trace_id"]; ok {
		t.Errorf("trace_id listed although no matching row's stream carries it: %v", m)
	}
	for name, h := range m {
		if h == 0 {
			t.Errorf("%s listed with 0 hits: %v", name, m)
		}
	}
	if m["_msg"] != 1 {
		t.Errorf("_msg hits = %d, want 1: %v", m["_msg"], m)
	}
}

// An object whose label counts, pushdown statistics or bloom filter say that no
// row matches the filter is still read: it holds columns of the stream the
// matching object does not, and a count of the field names lists them. (A
// quoted value is what the push-down and label pruning read, hence the hyphens.)
func TestFieldNames_FilterDoesNotSkipObjectsOrRowGroupsOfTheStream(t *testing.T) {
	sameStream := func(r schema.LogRow, level, traceID string) schema.LogRow {
		r.ServiceName, r.Stream = "svc", `{service.name="svc"}`
		r.SeverityText, r.TraceID = level, traceID
		return r
	}
	a := sameStream(fvcRow(1*time.Minute, "x"), "INFO-1", "ta")
	a.ExceptionType = "E"
	b := sameStream(fvcRow(2*time.Minute, "x"), "WARN-2", "tb")
	s := fvcStorage(t, []schema.LogRow{a}, []schema.LogRow{b})
	bloom0 := metrics.ParquetRowGroupsSkipped.Get("bloom") + metrics.ParquetRowGroupsSkipped.Get("pushdown") + metrics.ParquetRowGroupsSkipped.Get("column_stats")
	for _, filter := range []string{`level:="WARN-2"`, "trace_id:=tb", `service.name:="svc" AND level:="WARN-2"`} {
		got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, filter))
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		if m["exception.type"] != 1 || m["_msg"] != 1 {
			t.Errorf("%s: exception.type/_msg hits = %d/%d, want 1/1: %v", filter, m["exception.type"], m["_msg"], m)
		}
	}
	if d := metrics.ParquetRowGroupsSkipped.Get("bloom") + metrics.ParquetRowGroupsSkipped.Get("pushdown") + metrics.ParquetRowGroupsSkipped.Get("column_stats") - bloom0; d != 0 {
		t.Errorf("a field_names count skipped %d row groups for the row filter's sake", d)
	}
}
