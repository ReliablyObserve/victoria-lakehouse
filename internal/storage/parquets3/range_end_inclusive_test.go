package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// The bounds of a query's _time range are inclusive at nanosecond precision, as in VictoriaLogs
// (the HTTP end argument is exclusive and the upstream handler turns it into an inclusive bound
// by subtracting one nanosecond, so the storage always receives [start, end]). A row exactly at
// the end, or one nanosecond inside either bound, matches on every layer and on every query
// surface; a row one nanosecond outside does not.

// rangeEndTimes are the row times: not aligned to a microsecond, so a bound that is rounded to
// microseconds anywhere between the query and the row is seen.
func rangeEndTimes() []time.Time {
	base := rwHour.Add(time.Second + 123*time.Nanosecond)
	return []time.Time{base, base.Add(time.Second), base.Add(2 * time.Second)}
}

type rangeCase struct{ from, to time.Time }

// rangeEndCases are the windows around the first and the last row: ends exactly on a row, one
// nanosecond before and after it, the same for starts, and the windows of one nanosecond.
func rangeEndCases(rows []time.Time) []rangeCase {
	first, last := rows[0], rows[len(rows)-1]
	ns := time.Nanosecond
	var cs []rangeCase
	for _, r := range []time.Time{first, last} {
		for _, d := range []time.Duration{-1000 * ns, -2 * ns, -ns, 0, ns, 2 * ns, 999 * ns, 1000 * ns} {
			cs = append(cs,
				rangeCase{r.Add(-time.Hour), r.Add(d)}, // ends at / around r
				rangeCase{r.Add(d), r.Add(time.Hour)},  // starts at / around r
				rangeCase{r.Add(-ns), r.Add(d)},        // narrow window opening just before r
				rangeCase{r.Add(d - ns), r.Add(d)},     // one-nanosecond-wide window
			)
		}
		cs = append(cs, rangeCase{r, r})
	}
	return cs
}

func (c rangeCase) want(rows []time.Time) int {
	n := 0
	for _, r := range rows {
		if !r.Before(c.from) && !r.After(c.to) {
			n++
		}
	}
	return n
}

func (e *restartEnv) checkRangeEnds(when string, rows []time.Time) {
	e.t.Helper()
	for _, c := range rangeEndCases(rows) {
		if c.to.Before(c.from) {
			continue
		}
		want := c.want(rows)
		label := fmt.Sprintf("%s [%d, %d] (first%+d..%+d)", when, c.from.UnixNano(), c.to.UnixNano(), c.from.Sub(rows[0]), c.to.Sub(rows[0]))
		if got := e.run(context.Background(), "*", c.from, c.to); got != want {
			e.t.Errorf("%s: query=* emitted %d rows, want %d", label, got, want)
		}
		if got := e.run(storage.WithTimestampOnlyHint(context.Background()), "* | stats count() n", c.from, c.to); got != want {
			e.t.Errorf("%s: stats count path (timestamp-only) emitted %d rows, want %d", label, got, want)
		}
		if got := e.run(context.Background(), "* | stats by (level) count() n", c.from, c.to); got != want {
			e.t.Errorf("%s: count pushdown emitted %d rows, want %d", label, got, want)
		}
		if got := e.levelHits(c.from, c.to)["info"]; got != uint64(want) {
			e.t.Errorf("%s: field_values level = %d, want %d", label, got, want)
		}
	}
}

func TestRangeEnds_AreInclusiveOnEveryLayerAndSurface(t *testing.T) {
	rows := rangeEndTimes()
	e := newRestartEnv(t)
	e.ingest("info", rows...)
	e.checkRangeEnds("buffer", rows)
	e.flush()
	e.checkRangeEnds("buffer and parquet", rows)
	e.reap()
	e.checkRangeEnds("parquet", rows)
}

// Row groups and a window: the pruning decision agrees with the brute-force one at every edge.
// The window is [start, end], both inclusive. rowGroupMatchesTimeRange may keep a row group
// that holds no row in the window (it only sees the minimum and maximum), but it must keep
// every row group that holds one, and it must drop one whose [min, max] is outside the window.
// rowGroupFullyInRange may claim containment only when every row is in the window.
func TestRowGroupPruning_EdgesAgreeWithBruteForce(t *testing.T) {
	rnd := rand.New(rand.NewSource(20261016))
	dir := t.TempDir()
	for iteration := 0; iteration < 12; iteration++ {
		n := 1 + rnd.Intn(5)
		times := make([]int64, 0, n)
		base := int64(1_000_000_000_000)
		for i := 0; i < n; i++ {
			times = append(times, base+rnd.Int63n(2000))
		}
		rows := make([]pushdownTestRow, 0, n)
		for _, ts := range times {
			rows = append(rows, pushdownTestRow{TimestampUnixNano: ts, Body: "x", SeverityText: "info", ServiceName: "svc"})
		}
		pf := openTestParquet(t, writePushdownTestParquet(t, dir, rows))
		rg := pf.RowGroups()[0]
		tsIdx := findColumnIndex(pf.Root(), "timestamp_unix_nano")
		min, max := times[0], times[0]
		for _, ts := range times {
			if ts < min {
				min = ts
			}
			if ts > max {
				max = ts
			}
		}
		edges := []int64{min - 2, min - 1, min, min + 1, max - 1, max, max + 1, max + 2}
		for _, ts := range times {
			edges = append(edges, ts-1, ts, ts+1)
		}
		for _, start := range edges {
			for _, end := range edges {
				if end < start {
					continue
				}
				anyRow, allRows := false, true
				for _, ts := range times {
					if ts >= start && ts <= end {
						anyRow = true
					} else {
						allRows = false
					}
				}
				got := rowGroupMatchesTimeRange(rg, tsIdx, start, end)
				if anyRow && !got {
					t.Fatalf("rows %v, window [%d, %d]: the row group holds a row in the window and was pruned", times, start, end)
				}
				if wantOverlap := max >= start && min <= end; got != wantOverlap {
					t.Fatalf("rows %v, window [%d, %d]: rowGroupMatchesTimeRange = %v, want %v ([min, max] = [%d, %d])", times, start, end, got, wantOverlap, min, max)
				}
				if full := rowGroupFullyInRange(rg, tsIdx, start, end); full && !allRows {
					t.Fatalf("rows %v, window [%d, %d]: the row group was declared fully inside the window but holds rows outside it", times, start, end)
				}
			}
		}
	}
}
