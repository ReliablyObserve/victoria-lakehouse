package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A second that holds more rows than a group (imports, second-precision sources,
// replays) is cut into groups by a hash of the rows, and the plan is built from
// summaries of bounded size (#386). These tests pin: every group is within the
// cap, every row is written once, the plan of a segment without a crowded second
// is the plan it always was, and a drain interrupted in a crowded second
// resumes without a duplicate.

// crowdedFields is the content of one row of a burst (a log line here, a span in
// lakehouse-traces): id is its identity, which rows may share.
func crowdedFields(id string) []logstorage.Field {
	return []logstorage.Field{
		{Name: "service.name", Value: "api"},
		{Name: "k8s.namespace.name", Value: "prod"},
		{Name: "_msg", Value: id},
	}
}

// crowdedStreamFields names the stream fields of crowdedFields.
var crowdedStreamFields = []string{"service.name"}

// burst adds n rows of tenant: row i has _time tsAt(i) (Unix ns) and identity
// idAt(i).
func (e *segEnv) burst(tenant logstorage.TenantID, n int, tsAt func(i int) int64, idAt func(i int) string) {
	e.t.Helper()
	const batch = 5000
	for off := 0; off < n; off += batch {
		lr := logstorage.GetLogRows(crowdedStreamFields, nil, nil, nil, "")
		for i := off; i < off+batch && i < n; i++ {
			lr.MustAdd(tenant, tsAt(i), crowdedFields(idAt(i)), 1)
		}
		e.segs.MustAddRows(lr)
		logstorage.PutLogRows(lr)
	}
}

func rowID(i int) string { return fmt.Sprintf("row-%d", i+1) }

// objectRows returns the number of rows of every stored object.
func (e *segEnv) objectRows() []int {
	e.t.Helper()
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	var out []int
	for k, d := range e.u.data {
		if e.u.gone[k] {
			continue
		}
		pf, err := parquet.OpenFile(bytes.NewReader(d), int64(len(d)))
		if err != nil {
			e.t.Fatal(err)
		}
		out = append(out, int(pf.NumRows()))
	}
	sort.Ints(out)
	return out
}

func sum(xs []int) (t int) {
	for _, x := range xs {
		t += x
	}
	return t
}

func sec(i int64) int64 { return i * int64(time.Second) }

// The rows of a crowded second, however they differ, are written in groups of at
// most maxRows, each row once.
func TestDrain_CrowdedSecondIsCutIntoBoundedGroups(t *testing.T) {
	const maxRows, n = 1000, 12000
	base := hourAgo.UnixNano()
	for _, c := range []struct {
		name string
		ts   func(i int) int64
		id   func(i int) string
	}{
		{"one nanosecond, distinct rows", func(int) int64 { return base + sec(5) }, rowID},
		{"distinct nanoseconds, distinct rows", func(i int) int64 { return base + sec(5) + int64(i) }, rowID},
		{"distinct nanoseconds, one identity", func(i int) int64 { return base + sec(5) + int64(i) }, func(int) string { return "same" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newSegEnv(t)
			// quiet seconds on both sides of the crowded one
			e.burst(segTenantA, 10, func(i int) int64 { return base + sec(int64(i%3)) }, func(i int) string { return fmt.Sprintf("early-%d", i) })
			e.burst(segTenantA, n, c.ts, c.id)
			e.burst(segTenantA, 10, func(i int) int64 { return base + sec(int64(8+i%3)) }, func(i int) string { return fmt.Sprintf("late-%d", i) })
			g := e.seal()
			f := e.flusher(maxRows)
			if err := f.drain(context.Background(), g); err != nil {
				t.Fatal(err)
			}
			rows := e.objectRows()
			if sum(rows) != n+20 {
				t.Fatalf("stored %d rows, want %d", sum(rows), n+20)
			}
			for _, r := range rows {
				if r > maxRows {
					t.Errorf("a group of %d rows, want at most %d (objects: %v)", r, maxRows, rows)
				}
			}
			if len(rows) < n/maxRows {
				t.Errorf("%d objects for %d rows at %d per group", len(rows), n, maxRows)
			}
			got := e.storedMsgs()
			if c.name != "distinct nanoseconds, one identity" {
				for i := 0; i < n; i++ {
					if got[rowID(i)] != 1 {
						t.Fatalf("%s stored %d times", rowID(i), got[rowID(i)])
					}
				}
			} else if got["same"] != n {
				t.Errorf("stored %d identical rows, want %d", got["same"], n)
			}
			e.storedOnce(c.name)
		})
	}
}

// Rows identical in every field (the time included) cannot be told apart by their
// content: they stay one group over the cap, and are written once. This pins the
// documented limit of the cap.
func TestDrain_IdenticalRowsOfACrowdedSecondStayOneGroup(t *testing.T) {
	e := newSegEnv(t)
	base := hourAgo.UnixNano()
	e.burst(segTenantA, 3000, func(int) int64 { return base + sec(2) }, func(int) string { return "same" })
	e.burst(segTenantA, 500, func(i int) int64 { return base + sec(2) }, rowID)
	g := e.seal()
	f := e.flusher(1000)
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	rows := e.objectRows()
	if sum(rows) != 3500 {
		t.Fatalf("stored %d rows, want 3500", sum(rows))
	}
	var over int
	for _, r := range rows {
		if r > 1000 {
			over++
			if r < 3000 {
				t.Errorf("the identical rows were cut into %d", r)
			}
		}
	}
	if over != 1 {
		t.Errorf("%d groups over the cap, want 1 (the identical rows): %v", over, rows)
	}
	if got := e.storedMsgs()["same"]; got != 3000 {
		t.Errorf("stored %d identical rows, want 3000", got)
	}
}

// The rows a bucketed group collects are the rows of its second that fall in its
// buckets, with the fields they were ingested with: the groups of a second add up
// to the second, and no helper field reaches a row.
func TestCollectSlice_BucketsPartitionASecondAndLeaveNoHelperField(t *testing.T) {
	e := newSegEnv(t)
	base := hourAgo.UnixNano()
	const n = 6000
	e.burst(segTenantA, n, func(i int) int64 { return base + sec(1) + int64(i%7) }, rowID)
	g := e.seal()
	f := e.flusher(500)
	whole := flushSlice{partition: partitionFromNano(base), start: base + sec(1), end: base + sec(2) - 1, rows: n}
	plain, _, err := f.collectSlice(context.Background(), g, segTenantA, whole)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := f.splitSecond(context.Background(), g, segTenantA, whole)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) < n/500 {
		t.Fatalf("%d groups for %d rows at 500 per group", len(groups), n)
	}
	var got []string
	var prevHi int64 = -1
	for _, sl := range groups {
		if sl.rows > 500 || sl.bucketLo <= prevHi || sl.bucketHi < sl.bucketLo {
			t.Errorf("group %+v: over the cap or overlapping the previous bucket %d", sl, prevHi)
		}
		prevHi = sl.bucketHi
		rows, _, err := f.collectSlice(context.Background(), g, segTenantA, sl)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			got = append(got, fmt.Sprintf("%+v", r))
		}
	}
	var want []string
	for _, r := range plain {
		want = append(want, fmt.Sprintf("%+v", r))
	}
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("the buckets hold %d rows, the second %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("row %d differs:\n bucketed %s\n plain    %s", i, got[i], want[i])
		}
	}
}

// A segment without a crowded second is planned as it always was: the same groups,
// in the same order with the same numbers (the numbers name the objects), holding
// the same rows, whatever the spread of its rows over seconds and hours.
func TestPlanGroups_SamePlanAsPlanSlicesWithoutACrowdedSecond(t *testing.T) {
	rng := rand.New(rand.NewSource(386))
	for iter := 0; iter < 12; iter++ {
		e := newSegEnv(t)
		maxRows := int64(5 + rng.Intn(60))
		hours := 1 + rng.Intn(4)
		total := 0
		for h := 0; h < hours; h++ {
			hourStart := hourAgo.UnixNano() + int64(h)*int64(time.Hour)
			used := map[int]bool{}
			for s := 0; s < 1+rng.Intn(80); s++ {
				k := 1 + rng.Intn(int(maxRows)) // never more than a group in one second
				sc := rng.Intn(3600)
				if used[sc] {
					continue
				}
				used[sc] = true
				second := int64(sc)
				id := total
				e.burst(segTenantA, k, func(i int) int64 { return hourStart + sec(second) + int64(i) }, func(i int) string { return rowID(id + i) })
				total += k
			}
		}
		g := e.seal()
		f := e.flusher(maxRows)
		ctx := context.Background()
		counts, err := tenantSecondCounts(ctx, g, segTenantA)
		if err != nil {
			t.Fatal(err)
		}
		old := planSlices(counts, f.maxRows)
		got, err := f.planGroups(ctx, g, segTenantA)
		if err != nil {
			t.Fatal(err)
		}
		// seconds that fit in a group can still make an hour that does not: crowded
		// hours are planned from their seconds, and the others as one group.
		if len(got) != len(old) {
			t.Fatalf("iter %d: %d groups, planSlices made %d\n got %v\n old %v", iter, len(got), len(old), got, old)
		}
		for i := range got {
			if got[i].partition != old[i].partition || got[i].slice != old[i].slice || got[i].rows != old[i].rows || got[i].bucketed {
				t.Fatalf("iter %d group %d: %+v, planSlices made %+v", iter, i, got[i], old[i])
			}
			a, _, err := f.collectSlice(ctx, g, segTenantA, got[i])
			if err != nil {
				t.Fatal(err)
			}
			b, _, err := f.collectSlice(ctx, g, segTenantA, old[i])
			if err != nil {
				t.Fatal(err)
			}
			if len(a) != len(b) {
				t.Fatalf("iter %d group %d: %d rows, planSlices' group %d", iter, i, len(a), len(b))
			}
			ids := map[string]bool{}
			for _, r := range a {
				ids[r.Body] = true
			}
			for _, r := range b {
				if !ids[r.Body] {
					t.Fatalf("iter %d group %d: row %s is not in the group", iter, i, r.Body)
				}
			}
		}
	}
}

// The hours of the plan are the hours of the partitions: a row in the last
// nanosecond of an hour and one in the first of the next are in different groups.
func TestPlanGroups_HourBoundary(t *testing.T) {
	e := newSegEnv(t)
	h := hourAgo.UnixNano()
	e.burst(segTenantA, 3, func(int) int64 { return h + int64(time.Hour) - 1 }, rowID)
	e.burst(segTenantA, 2, func(int) int64 { return h + int64(time.Hour) }, func(i int) string { return rowID(10 + i) })
	g := e.seal()
	f := e.flusher(1000)
	got, err := f.planGroups(context.Background(), g, segTenantA)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].partition != partitionFromNano(h) || got[0].rows != 3 || got[1].partition != partitionFromNano(h+int64(time.Hour)) || got[1].rows != 2 || got[0].slice != 0 || got[1].slice != 0 {
		t.Fatalf("plan = %+v", got)
	}
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if rows := e.objectRows(); len(rows) != 2 || rows[0] != 2 || rows[1] != 3 {
		t.Errorf("objects %v, want one per hour partition", rows)
	}
}

// The plan costs one summary query per tenant while every hour fits in a group,
// however many distinct seconds there are, and one more per crowded hour, and per
// crowded second: it never grows with the number of seconds.
func TestPlanGroups_SummaryQueries(t *testing.T) {
	e := newSegEnv(t)
	h := hourAgo.UnixNano()
	// 3,000 rows in 3,000 distinct seconds of two hours
	e.burst(segTenantA, 3000, func(i int) int64 { return h + sec(int64(i)*2) }, rowID)
	g := e.seal()
	f := e.flusher(5000)
	got, err := f.planGroups(context.Background(), g, segTenantA)
	if err != nil {
		t.Fatal(err)
	}
	if f.planQueries != 1 || len(got) != 2 {
		t.Errorf("%d summary queries and %d groups for 3,000 seconds in 2 hours that fit; want 1 and 2", f.planQueries, len(got))
	}

	e = newSegEnv(t)
	e.burst(segTenantA, 3000, func(i int) int64 { return h + sec(int64(i)*2) }, rowID)             // hours of 1,800 (crowded) and 1,200 rows
	e.burst(segTenantA, 4000, func(i int) int64 { return h + 5*int64(time.Hour) + sec(7) }, rowID) // a crowded hour that is one crowded second
	g = e.seal()
	f = e.flusher(1500)
	if _, err := f.planGroups(context.Background(), g, segTenantA); err != nil {
		t.Fatal(err)
	}
	// the hours, the seconds of each of the 2 crowded hours, the buckets of the crowded second
	if f.planQueries != 1+2+1 {
		t.Errorf("%d summary queries, want 4", f.planQueries)
	}
}

// A drain that fails in the middle of a crowded second writes the rest on the
// next attempt, and, after a restart, from the same groups: every row once.
func TestDrain_CrowdedSecondResumesWithoutDuplicates(t *testing.T) {
	const n = 9000
	base := hourAgo.UnixNano()
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			e := newSegEnv(t)
			e.burst(segTenantA, n, func(i int) int64 { return base + sec(3) + int64(i) }, rowID)
			e.burst(segTenantB, 700, func(i int) int64 { return base + sec(3) }, func(i int) string { return rowID(n + i) })
			g := e.seal()
			f := e.flusher(1000)
			puts := 0
			e.failFn.Store(func(key string) bool {
				puts++
				return puts == 4 // the fourth upload fails
			})
			if err := f.drain(context.Background(), g); err == nil {
				t.Fatal("the drain with a failing upload returned no error")
			}
			if restart {
				e.restart()
				f = e.flusher(1000)
				g = e.segs.Pending()[0]
			}
			e.failFn.Store(func(string) bool { return false })
			e.drainAll(f)
			e.mustHaveExactly(n+700, "after the resumed drain")
			e.storedOnce("crowded second")
			for _, r := range e.objectRows() {
				if r > 1000 {
					t.Errorf("a group of %d rows", r)
				}
			}
		})
	}
}

// The objects written for a segment without a crowded second are the objects the
// per-second plan always wrote: same keys, same bytes (rows without a shared
// _time, so that their order in the file is not left to the order the segment's
// blocks come back in).
func TestDrain_ObjectsAreByteIdenticalToThePerSecondPlansWithoutACrowdedSecond(t *testing.T) {
	e := newSegEnv(t)
	h := hourAgo.UnixNano()
	next := 0
	for hr := 0; hr < 3; hr++ {
		for s := 0; s < 40; s++ {
			k := 1 + (hr*7+s*3)%9
			start := next
			e.burst(segTenantA, k, func(i int) int64 { return h + int64(hr)*int64(time.Hour) + sec(int64(s)*37) + int64(i)*1000 }, func(i int) string { return rowID(start + i) })
			next += k
		}
	}
	g := e.seal()
	f := e.flusher(23)
	ctx := context.Background()
	counts, err := tenantSecondCounts(ctx, g, segTenantA)
	if err != nil {
		t.Fatal(err)
	}
	old := planSlices(counts, f.maxRows)
	if err := f.drain(ctx, g); err != nil {
		t.Fatal(err)
	}
	if len(e.storedDataKeys()) != len(old) || len(old) < 6 {
		t.Fatalf("%d objects, the per-second plan has %d groups", len(e.storedDataKeys()), len(old))
	}
	for _, sl := range old {
		rows, _, err := f.collectSlice(ctx, g, segTenantA, sl)
		if err != nil {
			t.Fatal(err)
		}
		want := encodeGroup(t, e, rows)
		key := groupKey(e, g.Nonce(), sl)
		e.u.mu.Lock()
		got, ok := e.u.data[key]
		e.u.mu.Unlock()
		if !ok {
			t.Fatalf("no object %s", key)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: %d bytes, the per-second plan's group is %d bytes and differs", key, len(got), len(want))
		}
	}
}

// encodeGroup encodes rows as the writer does.
func encodeGroup(t *testing.T, e *segEnv, rows []schema.LogRow) []byte {
	t.Helper()
	res, err := writeLogsParquet(rows, e.bw.cfg.RowGroupSize, e.bw.cfg.CompressionLevel)
	if err != nil {
		t.Fatal(err)
	}
	return res.Data
}

// groupKey is the object key of group sl of the segment with this nonce.
func groupKey(e *segEnv, nonce string, sl flushSlice) string {
	return e.bw.assignLogKey(&logGroupUpload{partition: sl.partition, accountID: segTenantA.AccountID, projectID: segTenantA.ProjectID, batchID: segmentBatchID(nonce, sl.slice)})
}
