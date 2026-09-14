package parquets3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Regressions in parseFilterFromQuery, the first thing every cold query runs:
//
//   - it decided "time-only" by re-scanning the query text for `_time:[` and
//     cutting up to the matching `]`. A half-open range (`_time:[a, b)`) has no
//     `]`, so the scan never advanced and one request kept a CPU core busy until
//     the process restarted. The decision now walks the parsed filter
//     (FilterIsTimeOnly);
//   - it got the filter by Query.Clone, which renders the query and parses it
//     back. For a query VL accepts but cannot print back (`>'-'`) Clone panics,
//     and the VictoriaMetrics HTTP server exits the process on a handler panic,
//     so one request restarted the pod. The filter now comes from the parsed
//     query (logstorage.QueryFilter);
//   - the re-parse anchored relative ranges (`_time:5m`) to the parse time
//     rather than the query's timestamp.

const (
	timeOnlyA = "2026-05-10T14:00:01Z"
	timeOnlyB = "2026-05-10T14:00:03Z"
)

// runWithinDeadline fails the test when fn has not returned within d, so a hang
// names the shape that caused it instead of stalling the package until go test's
// global timeout.
func runWithinDeadline(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v", what, d)
	}
}

func TestParseFilterFromQuery_TimeRangeShapes(t *testing.T) {
	cases := []struct {
		name     string
		query    string
		timeOnly bool
	}{
		{"closed range", `_time:[` + timeOnlyA + `, ` + timeOnlyB + `]`, true},
		{"half-open end (used to hang)", `_time:[` + timeOnlyA + `, ` + timeOnlyB + `)`, true},
		{"half-open start", `_time:(` + timeOnlyA + `, ` + timeOnlyB + `]`, true},
		{"open range", `_time:(` + timeOnlyA + `, ` + timeOnlyB + `)`, true},
		{"greater than", `_time:>` + timeOnlyA, true},
		{"greater or equal", `_time:>=` + timeOnlyA, true},
		{"less than", `_time:<` + timeOnlyB, true},
		{"duration", `_time:5m`, true},
		{"duration with offset", `_time:5m offset 1h`, true},
		{"day", `_time:2026-05-10`, true},
		{"two ranges ANDed", `_time:[` + timeOnlyA + `, ` + timeOnlyB + `) _time:5m`, true},
		{"wildcard and range", `* _time:[` + timeOnlyA + `, ` + timeOnlyB + `)`, true},
		{"day_range is evaluated per row", `_time:day_range[08:00, 18:00)`, false},
		{"week_range is evaluated per row", `_time:week_range[Mon, Fri]`, false},
		{"range and field", `_time:[` + timeOnlyA + `, ` + timeOnlyB + `) service.name:="api"`, false},
		{"range OR field", `_time:[` + timeOnlyA + `, ` + timeOnlyB + `) OR service.name:="api"`, false},
		{"negated range", `NOT _time:[` + timeOnlyA + `, ` + timeOnlyB + `)`, false},
		{"range text inside a phrase", `"_time:[x, y)"`, false},
		{"range text inside a field phrase", `_msg:"_time:[x, y)"`, false},
		{"field only", `service.name:="api"`, false},
	}

	start := time.Date(2026, 5, 10, 13, 0, 0, 0, time.UTC).UnixNano()
	end := time.Date(2026, 5, 10, 15, 0, 0, 0, time.UTC).UnixNano()
	for _, c := range cases {
		for _, form := range []struct {
			name   string
			query  string
			bounds bool // the select handlers AND the request's start/end into the query
		}{
			{"filter", c.query, false},
			{"with pipe", c.query + " | stats count() c", false},
			{"with request bounds", c.query, true},
		} {
			t.Run(c.name+"/"+form.name, func(t *testing.T) {
				q, err := logstorage.ParseQuery(form.query)
				if err != nil {
					t.Fatalf("ParseQuery(%q): %v", form.query, err)
				}
				if form.bounds {
					q.AddTimeFilter(start, end)
				}
				var f *logstorage.Filter
				runWithinDeadline(t, 5*time.Second, "parseFilterFromQuery("+form.query+")", func() {
					f = parseFilterFromQuery(q)
				})
				if got := f == nil; got != c.timeOnly {
					t.Fatalf("parseFilterFromQuery(%q) time-only = %v, want %v (filter %v)", form.query, got, c.timeOnly, f)
				}
			})
		}
	}
}

// TestFilterIsTimeOnly_UpstreamNodeNames pins the VL AST node names the check
// relies on. A rename would not produce wrong results (unrecognized trees are
// evaluated per row) but would silently put every time-only query on the slow
// path, so it has to fail here.
func TestFilterIsTimeOnly_UpstreamNodeNames(t *testing.T) {
	for query, want := range map[string]string{
		`_time:[` + timeOnlyA + `, ` + timeOnlyB + `)`: astTypeTime,
		`_time:5m offset 1h`:                           astTypeTime,
		`_time:day_range[08:00, 18:00)`:                astTypeDayRange,
		`_time:week_range[Mon, Fri]`:                   astTypeWeekRange,
		`_time:5m service.name:="api"`:                 astTypeAnd,
	} {
		f, err := logstorage.ParseFilter(query)
		if err != nil {
			t.Fatalf("ParseFilter(%q): %v", query, err)
		}
		if got := astTypeName(derefValue(filterInner(f))); got != want {
			t.Errorf("ParseFilter(%q) root node = %q, want %q", query, got, want)
		}
	}
	if FilterIsTimeOnly(nil) {
		t.Error("FilterIsTimeOnly(nil) = true, want false")
	}
}

// TestRunQuery_TimeRangeBoundsAreExact runs every range form through the cold
// read path and checks the exact rows, boundaries included. Time-only filters
// reach this path with no row filter at all, so the bounds must come from
// q.GetFilterTimeRange(); day_range must still be applied per row.
func TestRunQuery_TimeRangeBoundsAreExact(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	t0 := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	services := []string{"api", "worker", "api", "worker", "api"}
	rows := make([]logRow, len(services))
	for i, svc := range services {
		rows[i] = logRow{TimestampUnixNano: t0.Add(time.Duration(i) * time.Second).UnixNano(), Body: "row", ServiceName: svc}
	}
	registerFileInMockS3(t, s, mock, "logs/dt=2026-05-10/hour=14/time-bounds.parquet", writeParquetToBytes(t, rows), t0)

	cases := []struct {
		query string
		want  int
	}{
		{`_time:[` + timeOnlyA + `, ` + timeOnlyB + `]`, 3},
		{`_time:[` + timeOnlyA + `, ` + timeOnlyB + `)`, 2},
		{`_time:(` + timeOnlyA + `, ` + timeOnlyB + `]`, 2},
		{`_time:(` + timeOnlyA + `, ` + timeOnlyB + `)`, 1},
		{`_time:>` + timeOnlyA, 3},
		{`_time:<` + timeOnlyB, 3},
		{`_time:[` + timeOnlyA + `, ` + timeOnlyB + `) service.name:="api"`, 1},
		// day_range defaults to the process's local time zone; pin the offset so
		// the expectation holds on any machine.
		{`_time:day_range[14:00, 15:00) offset 0h`, 5},
		{`_time:day_range[13:00, 14:00) offset 0h`, 0},
	}
	for _, c := range cases {
		for _, hint := range []bool{false, true} {
			name := c.query
			ctx := context.Background()
			if hint {
				// The count and hits endpoints set this hint; it narrows the projection.
				name += "/timestamp-only hint"
				ctx = storage.WithTimestampOnlyHint(ctx)
			}
			t.Run(name, func(t *testing.T) {
				q := mustParseQuery(t, c.query)
				var mu sync.Mutex
				got := 0
				var runErr error
				runWithinDeadline(t, 10*time.Second, "RunQuery("+c.query+")", func() {
					runErr = s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
						mu.Lock()
						got += db.RowsCount()
						mu.Unlock()
					})
				})
				if runErr != nil {
					t.Fatalf("RunQuery(%q): %v", c.query, runErr)
				}
				if got != c.want {
					t.Fatalf("RunQuery(%q) returned %d rows, want %d", c.query, got, c.want)
				}
			})
		}
	}
}

// TestRunQuery_RelativeRangeUsesTheQueryTimestamp: the per-row filter of a
// query with a relative range and a field predicate must cover the query's own
// window. Re-parsing at time.Now() moved the window to the present and dropped
// every row of a query issued for an earlier instant.
func TestRunQuery_RelativeRangeUsesTheQueryTimestamp(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	t0 := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	services := []string{"api", "worker", "api", "worker", "api"}
	rows := make([]logRow, len(services))
	for i, svc := range services {
		rows[i] = logRow{TimestampUnixNano: t0.Add(time.Duration(i) * time.Second).UnixNano(), Body: "row", ServiceName: svc}
	}
	registerFileInMockS3(t, s, mock, "logs/dt=2026-05-10/hour=14/relative.parquet", writeParquetToBytes(t, rows), t0)

	query := `_time:5m service.name:="api"`
	// Issued 10s after t0: the 5m window holds all five rows.
	q, err := logstorage.ParseQueryAtTimestamp(query, t0.Add(10*time.Second).UnixNano())
	if err != nil {
		t.Fatalf("ParseQueryAtTimestamp(%q): %v", query, err)
	}
	got := 0
	var mu sync.Mutex
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		got += db.RowsCount()
		mu.Unlock()
	}); err != nil {
		t.Fatalf("RunQuery(%q): %v", query, err)
	}
	if got != 3 {
		t.Fatalf("RunQuery(%q) at t0+10s returned %d rows, want 3 (api rows at t0, t0+2s, t0+4s)", query, got)
	}
}

// FuzzParseFilterFromQuery_TimeOnlyIsSound: for any query VL accepts,
// parseFilterFromQuery returns promptly, and when it returns nil ("match every
// row in the time range") the filter really does accept rows inside that range
// whatever their other fields hold.
func FuzzParseFilterFromQuery_TimeOnlyIsSound(f *testing.F) {
	for _, seed := range []string{
		`_time:[` + timeOnlyA + `, ` + timeOnlyB + `)`,
		`_time:(` + timeOnlyA + `, ` + timeOnlyB + `]`,
		`_time:(` + timeOnlyA + `, ` + timeOnlyB + `)`,
		`_time:[` + timeOnlyA + `, ` + timeOnlyB + `]`,
		`_time:>` + timeOnlyA + ` service.name:="api"`,
		`_time:5m offset 1h`,
		`_time:day_range[08:00, 18:00)`,
		`_time:week_range[Mon, Fri]`,
		`NOT _time:[` + timeOnlyA + `, ` + timeOnlyB + `)`,
		`_time:[` + timeOnlyA + `, ` + timeOnlyB + `) OR foo:bar`,
		`"_time:[x, y)"`,
		`* | stats count() c`,
		`>'-'`,
	} {
		f.Add(seed)
	}
	lo := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	hi := time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	f.Fuzz(func(t *testing.T, input string) {
		q, err := logstorage.ParseQuery(input)
		if err != nil {
			return
		}
		var got *logstorage.Filter
		runWithinDeadline(t, 5*time.Second, "parseFilterFromQuery", func() {
			got = parseFilterFromQuery(q)
		})
		if got != nil {
			return
		}
		pf := logstorage.QueryFilter(q)
		if pf == nil {
			return
		}
		minTs, maxTs := q.GetFilterTimeRange()
		minTs, maxTs = max(minTs, lo), min(maxTs, hi)
		if minTs > maxTs {
			return
		}
		for _, ts := range []int64{minTs, maxTs, minTs + (maxTs-minTs)/2} {
			row := []logstorage.Field{
				{Name: "_time", Value: time.Unix(0, ts).UTC().Format(time.RFC3339Nano)},
				{Name: "_msg", Value: "fuzz message"},
				{Name: "service.name", Value: "fuzz-service"},
			}
			if !pf.MatchRow(row) {
				t.Fatalf("parseFilterFromQuery(%q) = nil (match all), but %q rejects a row at %s inside its time range",
					input, pf, row[0].Value)
			}
		}
	})
}

// TestColdReads_QueryVLCannotPrintBack: `>'-'` parses, but VL renders it as
// `>-`, which does not parse. Every cold entry point must answer it without
// panicking — a panic in a request handler exits the whole process.
func TestColdReads_QueryVLCannotPrintBack(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	t0 := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: t0.UnixNano(), Body: "a", ServiceName: "api"},
		{TimestampUnixNano: t0.Add(time.Second).UnixNano(), Body: "b", ServiceName: "worker"},
	}
	registerFileInMockS3(t, s, mock, "logs/dt=2026-05-10/hour=14/printback.parquet", writeParquetToBytes(t, rows), t0)

	const query = `>'-'`
	q := mustParseQueryWithTime(t, query, t0.Add(-time.Minute).UnixNano(), t0.Add(time.Minute).UnixNano())
	if _, err := logstorage.ParseQuery(q.String()); err == nil {
		t.Fatalf("VL now prints %q back in parseable form (%q); pick another shape it cannot round-trip", query, q.String())
	}

	noPanic := func(t *testing.T, name string, fn func() error) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s(%q) panicked: %v", name, query, r)
			}
		}()
		if err := fn(); err != nil {
			t.Fatalf("%s(%q): %v", name, query, err)
		}
	}
	ctx := context.Background()
	noPanic(t, "parseFilterFromQuery", func() error {
		if parseFilterFromQuery(q) == nil {
			return fmt.Errorf("returned nil (match all) for a query with a field predicate")
		}
		return nil
	})
	noPanic(t, "RunQuery", func() error {
		return s.RunQuery(ctx, nil, q, func(uint, *logstorage.DataBlock) {})
	})
	noPanic(t, "GetFieldNames", func() error {
		_, err := s.GetFieldNames(ctx, nil, q)
		return err
	})
	noPanic(t, "GetFieldValues", func() error {
		_, err := s.GetFieldValues(ctx, nil, q, "service.name", 10)
		return err
	})
	noPanic(t, "GetStreams", func() error {
		_, err := s.GetStreams(ctx, nil, q, 10)
		return err
	})
	noPanic(t, "GetStreamIDs", func() error {
		_, err := s.GetStreamIDs(ctx, nil, q, 10)
		return err
	})
}
