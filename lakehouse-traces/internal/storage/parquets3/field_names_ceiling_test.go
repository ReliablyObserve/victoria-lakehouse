package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// The per-query row ceiling guards queries that return rows. A field_names
// enumeration keeps O(fields) state, and a ceiling that stopped it early would
// hand back a field list missing the fields of the unread rows.
func TestRowCeilingOf_AppliesToRowQueriesOnly(t *testing.T) {
	ctx := context.Background()
	if got := rowCeilingOf(ctx, 7); got != 7 {
		t.Errorf("a row query keeps the ceiling: got %d, want 7", got)
	}
	if got := rowCeilingOf(storage.WithFieldNamesQuery(ctx), 7); got != 0 {
		t.Errorf("a field_names read has no ceiling: got %d, want 0", got)
	}
}

func TestRowFilterOf_FieldNamesKeepsTheTimeRange(t *testing.T) {
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	if f := rowFilterOf(context.Background(), q); f != nil {
		t.Errorf("a time-only row query has nothing left to evaluate per row, got filter %v", f)
	}
	if f := rowFilterOf(storage.WithFieldNamesQuery(context.Background()), q); f == nil {
		t.Error("a field_names read evaluates the whole filter, the time range included, per row: got none")
	}
}

// A ceiling of 2 spans must not cut the field list of five.
func TestFieldNames_IgnoresTheRowCeiling(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{
		fnSpan(1*time.Minute, "GET", "GET"), fnSpan(2*time.Minute, "GET", ""), fnSpan(3*time.Minute, "GET", ""),
		fnSpan(4*time.Minute, "PUT", ""), fnSpan(5*time.Minute, "PUT", ""),
	})
	s.cfg.Query.MaxRows = 2
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["trace_id"] != 5 || m["span_attr:custom.attr"] != 3 {
		t.Errorf("trace_id/span_attr:custom.attr hits = %d/%d, want 5/3 whatever query.max_rows is: %v", m["trace_id"], m["span_attr:custom.attr"], m)
	}
}

// The time range selects the spans AFTER a block's columns are known, so only
// the spans inside it are credited (twin of the windowed half of
// TestFieldNames_FilterSelectsRowsAfterTheColumnsAreKnown in the logs module).
func TestTraceFieldNames_TimeRangeSelectsTheCreditedSpans(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{
		fnSpan(1*time.Minute, "GET", "GET"), fnSpan(2*time.Minute, "GET", ""), fnSpan(3*time.Minute, "GET", ""),
		fnSpan(4*time.Minute, "PUT", ""), fnSpan(5*time.Minute, "PUT", ""),
	})
	win := mustParseQueryWithTime(t, "*", fvcBase.Add(90*time.Second).UnixNano(), fvcBase.Add(3*time.Minute+time.Second).UnixNano())
	got, err := s.GetFieldNames(context.Background(), nil, win)
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["trace_id"] != 2 || m["span_attr:custom.attr"] != 2 {
		t.Errorf("windowed trace_id/span_attr:custom.attr hits = %d/%d, want 2/2 (the two spans inside the window, the GET stream lists the attribute): %v", m["trace_id"], m["span_attr:custom.attr"], m)
	}
}
