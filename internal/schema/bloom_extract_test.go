package schema

import (
	"sort"
	"testing"
)

func TestExtractLogBloomValues(t *testing.T) {
	rows := []LogRow{
		{TraceID: "t1", ServiceName: "svc-a"},
		{TraceID: "t2", ServiceName: "svc-a"}, // duplicate service → deduped
		{TraceID: "t1", ServiceName: "svc-b"}, // duplicate trace_id → deduped
	}
	assertBloom(t, ExtractLogBloomValues(rows), map[string][]string{
		"trace_id":     {"t1", "t2"},
		"service.name": {"svc-a", "svc-b"},
	})
}

func TestExtractTraceBloomValues(t *testing.T) {
	rows := []TraceRow{
		{TraceID: "t1", ServiceName: "svc-a"},
		{TraceID: "t2", ServiceName: "svc-b"},
	}
	assertBloom(t, ExtractTraceBloomValues(rows), map[string][]string{
		"trace_id":     {"t1", "t2"},
		"service.name": {"svc-a", "svc-b"},
	})
}

// TestExtractBloomValues_EmptyAndAbsent locks the all-empty / absent-value paths
// (formerly the bloomSetsToMap test): no rows, or rows with no bloom-column values,
// must yield nil; an absent column must not produce an empty key (so the combined
// bloom on compaction never blooms a column that had no values).
func TestExtractBloomValues_EmptyAndAbsent(t *testing.T) {
	if ExtractLogBloomValues(nil) != nil {
		t.Error("nil log rows → nil")
	}
	if ExtractTraceBloomValues(nil) != nil {
		t.Error("nil trace rows → nil")
	}
	if got := ExtractLogBloomValues([]LogRow{{TimestampUnixNano: 1}}); got != nil {
		t.Errorf("log rows with no bloom values → nil, got %v", got)
	}
	if got := ExtractTraceBloomValues([]TraceRow{{}}); got != nil {
		t.Errorf("trace rows with no bloom values → nil, got %v", got)
	}
	// trace_id present, service.name absent → only the trace_id key.
	got := ExtractLogBloomValues([]LogRow{{TraceID: "t1"}})
	if len(got) != 1 || len(got["trace_id"]) != 1 {
		t.Errorf("expected only trace_id key, got %v", got)
	}
	if _, ok := got["service.name"]; ok {
		t.Error("absent service.name must not produce a bloom key")
	}
}

func assertBloom(t *testing.T, got, want map[string][]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d cols, want %d (%v)", len(got), len(want), got)
	}
	for col, wantVals := range want {
		gotVals := append([]string(nil), got[col]...)
		sort.Strings(gotVals)
		sort.Strings(wantVals)
		if len(gotVals) != len(wantVals) {
			t.Errorf("col %s: got %v, want %v", col, gotVals, wantVals)
			continue
		}
		for i := range wantVals {
			if gotVals[i] != wantVals[i] {
				t.Errorf("col %s: got %v, want %v", col, gotVals, wantVals)
				break
			}
		}
	}
}
