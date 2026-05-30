package parquets3

import (
	"context"
	"testing"
	"time"
)

// TestGetFieldNames_ReportsActualHits verifies that GetFieldNames returns
// per-field hit counts based on actual non-null row counts rather than
// the previous stub value of 1. The hits are summed from the Parquet
// column index (rowGroupNumRows - nullCount).
func TestGetFieldNames_ReportsActualHits(t *testing.T) {
	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	// Mix of populated and partially-null fields. service.name is set in
	// every row; trace_id is set in only 2 of 4 rows. The hit counts
	// should reflect that asymmetry.
	rows := []fullLogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg1", SeverityText: "INFO",
			ServiceName: "api", TraceID: "t1", SpanID: "s1"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "msg2", SeverityText: "INFO",
			ServiceName: "api", TraceID: "t2", SpanID: "s2"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "msg3", SeverityText: "ERROR",
			ServiceName: "web"},
		{TimestampUnixNano: now.Add(3 * time.Second).UnixNano(), Body: "msg4", SeverityText: "ERROR",
			ServiceName: "web"},
	}
	s, _ := testFieldStorage(t, rows)

	q := mustParseQueryWithTime(t, "*",
		time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	)

	fields, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) == 0 {
		t.Fatal("expected non-empty result")
	}

	hitsByName := make(map[string]uint64)
	for _, f := range fields {
		hitsByName[f.Value] = f.Hits
	}

	// service.name is set in all 4 rows.
	if h := hitsByName["service.name"]; h != 4 {
		t.Errorf("service.name hits = %d, want 4 (post-fix: actual non-null count)", h)
	}
	// body is set in all 4 rows; reported as _msg via registry mapping.
	if h := hitsByName["_msg"]; h != 4 {
		t.Errorf("_msg hits = %d, want 4", h)
	}
	// At minimum, hits should not be the stub value 1 for a fully-populated
	// 4-row field.
	for _, name := range []string{"service.name", "_msg", "level", "_time"} {
		if h := hitsByName[name]; h == 1 {
			t.Errorf("field %q has stub hits=1; expected actual non-null row count", name)
		}
	}
}
