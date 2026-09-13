//go:build parity

package parity

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestParity_TimeRange(t *testing.T) {
	now := time.Now()
	dataStart := now.Add(-24 * time.Hour)

	t.Run("ns_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "ns_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixNano()),
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("sec_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "sec_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.Unix()),
			"end":   fmt.Sprintf("%d", now.Unix()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("ms_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "ms_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixMilli()),
			"end":   fmt.Sprintf("%d", now.UnixMilli()),
		}, Compare: CountTolerance, Tolerance: 0.01}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("missing_end", func(t *testing.T) {
		pc := ParityCase{Name: "missing_end", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("missing_start", func(t *testing.T) {
		pc := ParityCase{Name: "missing_start", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("future_range", func(t *testing.T) {
		future := now.Add(365 * 24 * time.Hour)
		pc := ParityCase{Name: "future_range", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", future.UnixNano()),
			"end":   fmt.Sprintf("%d", future.Add(time.Hour).UnixNano()),
		}, Compare: CountEqual, ExpectEmpty: true}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("zero_width", func(t *testing.T) {
		ts := fmt.Sprintf("%d", now.Add(-6*time.Hour).UnixNano())
		pc := ParityCase{Name: "zero_width", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": ts,
			"end":   ts,
		}, Compare: CountEqual, ExpectEmpty: true}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("narrow_1min", func(t *testing.T) {
		// A fixed minute of a 10 000-row day holds no row at all in a few
		// percent of seeds, so anchor the minute on a row that exists.
		midHour := url.Values{
			"start": {fmt.Sprintf("%d", now.Add(-12*time.Hour).UnixNano())},
			"end":   {fmt.Sprintf("%d", now.Add(-11*time.Hour).UnixNano())},
		}
		minute := referenceRowTime(t, midHour, "* | sort by (_time) | limit 1").Truncate(time.Minute)
		pc := ParityCase{Name: "narrow_1min", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", minute.UnixNano()),
			"end":   fmt.Sprintf("%d", minute.Add(time.Minute).UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("full_range", func(t *testing.T) {
		pc := ParityCase{Name: "full_range", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": "0",
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	// The request window is [start, end): start inclusive, end exclusive,
	// at nanosecond resolution. Both boundaries are pinned on the oldest
	// seeded row, so neither case depends on where the random seed happened
	// to put rows — a one-second window at the edge of the data used to
	// hold a row in roughly one run in ten. The lookup runs inside each
	// subtest so a failed lookup is attributed to that subtest, never to
	// this parent's body.
	oldestSeededRow := func(t *testing.T) int64 {
		t.Helper()
		return referenceRowTime(t, seedWindowParams(), "* | sort by (_time) | limit 1").UnixNano()
	}

	t.Run("boundary_ns", func(t *testing.T) {
		// Ends exactly on the oldest row: an exclusive end must leave it out.
		oldest := oldestSeededRow(t)
		pc := ParityCase{Name: "boundary_ns", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", oldest-1),
			"end":   fmt.Sprintf("%d", oldest),
		}, Compare: CountEqual, ExpectEmpty: true}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("boundary_ns_start_inclusive", func(t *testing.T) {
		// Starts exactly on the oldest row: an inclusive start must count it.
		oldest := oldestSeededRow(t)
		pc := ParityCase{Name: "boundary_ns_start_inclusive", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", oldest),
			"end":   fmt.Sprintf("%d", oldest+1),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})
}
