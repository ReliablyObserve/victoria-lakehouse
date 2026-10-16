//go:build parity

package parity

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

// tracesSpanTime returns the _time of the first (or last) span of the seed window on HOT
// VictoriaTraces. The index rows hot VT writes beside the spans (#458) carry no span_id, so every
// query here asks for span_id:*.
func tracesSpanTime(t *testing.T, sortExpr string) int64 {
	t.Helper()
	p := url.Values{}
	for k, v := range seedWindowParams() {
		p[k] = v
	}
	p.Set("query", "span_id:* | sort by (_time"+sortExpr+") | limit 1")
	p.Set("limit", "1")
	r := fetch(t, vtBaseURL, queryEndpoint(), p)
	if r.StatusCode != 200 {
		t.Fatalf("span time lookup returned status %d: %s", r.StatusCode, string(r.Body))
	}
	rows := parseNDJSON(r.Body)
	if len(rows) == 0 {
		t.Fatal("span time lookup returned no spans — seed defect, not parity")
	}
	raw, _ := rows[0]["_time"].(string)
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("span time lookup: cannot parse _time %q: %v", raw, err)
	}
	return ts.UnixNano()
}

// The request window of the traces binary is [start, end) too: start inclusive, end exclusive, to
// the nanosecond and in every unit upstream accepts, on spans in Parquet. Twin of TestParity_TimeRange
// (logs): the same three defects (the select handler cut the arguments to a coarser unit, row-group
// pruning compared the end exclusively, the partition walk stopped at a partition starting on the end)
// are shared with the logs binary, and the twin keeps the traces binary's own wiring covered.
func TestParity_Traces_TimeRange(t *testing.T) {
	count := func(t *testing.T, name string, start, end string, expectEmpty bool) {
		t.Helper()
		pc := ParityCase{Name: name, Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "span_id:* | stats count() rows",
			"start": start,
			"end":   end,
		}, Compare: CountEqual, ExpectEmpty: expectEmpty}
		params := buildParams(pc, nil)
		ref := fetch(t, vtBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhtBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	}
	ns := func(v int64) string { return fmt.Sprintf("%d", v) }

	t.Run("boundary_ns", func(t *testing.T) {
		oldest := tracesSpanTime(t, "")
		count(t, "traces_boundary_ns", ns(oldest-1), ns(oldest), true)
	})
	t.Run("boundary_ns_start_inclusive", func(t *testing.T) {
		oldest := tracesSpanTime(t, "")
		count(t, "traces_boundary_ns_start_inclusive", ns(oldest), ns(oldest+1), false)
	})
	t.Run("boundary_ns_newest_span", func(t *testing.T) {
		newest := tracesSpanTime(t, " desc")
		count(t, "traces_boundary_ns_newest_span", ns(newest), ns(newest+1), false)
	})
	t.Run("unit_window_ms", func(t *testing.T) {
		ms := tracesSpanTime(t, "") / 1e6
		count(t, "traces_unit_window_ms", ns(ms), ns(ms+1), false)
	})
	t.Run("unit_window_us", func(t *testing.T) {
		us := tracesSpanTime(t, "") / 1e3
		count(t, "traces_unit_window_us", ns(us), ns(us+1), false)
	})
	t.Run("ms_epoch_seed_window", func(t *testing.T) {
		count(t, "traces_ms_epoch_seed_window", ns(seedWindowStart().UnixMilli()), ns(seedWindowEnd().UnixMilli()), false)
	})
}
