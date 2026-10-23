package parquets3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The scheme of the plan is pinned: everything below decides how a segment's
// groups are numbered, so a change to any of it makes a drain that is in
// progress across the upgrade settle groups on the keys of other groups (#748).
// Changing this pin means bumping planScheme in the same commit (a process that
// finds a draining segment of another scheme stops at start).
func TestPlanScheme_PinsTheBucketScheme(t *testing.T) {
	got := fmt.Sprintf("scheme=%d buckets=%d div=%d key=%s pipes=%s delete=%s", planScheme, splitBuckets, splitBucketDiv, splitKeyFormat, splitBucketPipes, splitBucketDelete)
	const want = "scheme=1 buckets=1024 div=8796093022208 key=<_time>|<trace_id>|<span_id> " +
		"pipes=format \"<_time>|<trace_id>|<span_id>\" as _lh_split_k | hash(_lh_split_k) as _lh_split_h | math floor(_lh_split_h / 8796093022208) as _lh_split_b " +
		"delete=delete _lh_split_k, _lh_split_h, _lh_split_b"
	if got != want {
		t.Fatalf("the plan's bucket scheme changed without a planScheme bump:\n got  %s\n want %s", got, want)
	}
}

// The plan itself is pinned, not only the strings it is built from: the groups
// planGroups makes of a fixed segment (an hour over the cap that has a crowded
// second, and a quiet hour) are compared with a list written down. The plan
// depends on what upstream's `format` and `hash` pipes, `_stream_id` and
// `stats by (_time:1h)` / `(_time:1s)` return, and a VictoriaLogs bump that
// changes any of them renumbers the groups of a drain that is in progress across
// the upgrade (#748). This test then fails: bump planScheme and regenerate the
// list (run it with -v and copy the printed plan).
func TestPlanScheme_GoldenPlan(t *testing.T) {
	const maxRows = 10
	base := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC).UnixNano()
	e := newSegEnv(t)
	id := 0
	add := func(ts int64) {
		e.burst(segTenantA, 1, func(int) int64 { return ts }, func(int) string { id++; return rowID(id) })
	}
	for s := 0; s < 7; s++ { // seven quiet seconds with 2 rows: 14 rows
		add(base + sec(int64(s)))
		add(base + sec(int64(s)) + 5)
	}
	for i := 0; i < 25; i++ { // a crowded second
		add(base + sec(30) + int64(i))
	}
	for s := 0; s < 3; s++ { // the next hour
		add(base + int64(time.Hour) + sec(int64(s)))
	}
	g := e.seal()
	f := e.flusher(maxRows)
	slices, err := f.planGroups(context.Background(), g, segTenantA)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, s := range slices {
		fmt.Fprintf(&b, "%s #%d [%d,%d] rows=%d bucketed=%v [%d,%d]\n", s.partition, s.slice, s.start-base, s.end-base, s.rows, s.bucketed, s.bucketLo, s.bucketHi)
	}
	t.Logf("plan:\n%s", b.String())
	if got := b.String(); got != goldenPlan {
		t.Fatalf("the plan of the fixed segment changed (a VictoriaLogs pipe, the key or the summaries): bump planScheme and regenerate the list.\n got:\n%s\nwant:\n%s", got, goldenPlan)
	}
}

const goldenPlan = `dt=2026-01-02/hour=03 #0 [0,4999999999] rows=10 bucketed=false [0,0]
dt=2026-01-02/hour=03 #1 [5000000000,29999999999] rows=4 bucketed=false [0,0]
dt=2026-01-02/hour=03 #2 [30000000000,30999999999] rows=10 bucketed=true [19,486]
dt=2026-01-02/hour=03 #3 [30000000000,30999999999] rows=10 bucketed=true [491,683]
dt=2026-01-02/hour=03 #4 [30000000000,30999999999] rows=5 bucketed=true [762,970]
dt=2026-01-02/hour=04 #0 [3600000000000,7199999999999] rows=3 bucketed=false [0,0]
`
