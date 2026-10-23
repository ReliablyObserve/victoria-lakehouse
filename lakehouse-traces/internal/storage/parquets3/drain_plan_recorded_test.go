package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// The groups of a sealed segment are numbered by a plan that depends on the rows
// per group (maxRows, from insert.target_file_size), and a group's key is
// <nonce>-<slice>. A drain cut short and resumed by a process with another
// target_file_size must therefore finish with the maxRows the drain started
// with, or a new slice k takes the key of an old, different slice k, is found
// settled, and its rows are never written (#748). The flush state records the
// maxRows of the draining segment; only a segment whose drain has not begun uses
// the configured value.

// mustNoInvariantViolation: the plan-versus-collect row count checks (planTenant,
// collectSlice) never fired since inv0.
func mustNoInvariantViolation(t *testing.T, inv0 uint64) {
	t.Helper()
	if now := metrics.BufferFlushErrors.Get("invariant"); now != inv0 {
		t.Errorf("lakehouse_buffer_flush_errors_total{stage=\"invariant\"} went from %d to %d", inv0, now)
	}
}

// planShape is where the rows of a test segment are in time.
type planShape struct {
	name string
	rows int
	// at gives row i's tenant and _time (Unix ns).
	at func(i int) (logstorage.TenantID, int64)
}

func planShapes() []planShape {
	base := hourAgo.UnixNano()
	return []planShape{
		{"every row in its own second", 12, func(i int) (logstorage.TenantID, int64) { return segTenantA, base + sec(int64(i)) }},
		{"one crowded second", 14, func(i int) (logstorage.TenantID, int64) { return segTenantA, base + sec(5) + int64(i) }},
		{"two tenants, two hours, crowded and quiet seconds", 18, func(i int) (logstorage.TenantID, int64) {
			tn := segTenantA
			if i%3 == 0 {
				tn = segTenantB
			}
			return tn, base + sec(int64(i%7)) + int64(i/9)*int64(time.Hour)
		}},
	}
}

// ingestShape adds the rows of s to e, tenant by tenant in id order, so that the
// ids are row-1..row-n whichever the tenants.
func ingestShape(e *segEnv, s planShape) {
	e.t.Helper()
	for i := 0; i < s.rows; i++ {
		tn, ts := s.at(i)
		e.burst(tn, 1, func(int) int64 { return ts }, func(int) string { return rowID(i) })
	}
}

// drainCut drains the (sealed) segment with maxRows rows per group until `cut`
// data objects were stored: every later upload fails (a process that died or
// lost the object store). It returns how many data objects were uploaded.
func drainCut(e *segEnv, maxRows int64, cut int) int {
	e.t.Helper()
	return drainCutWith(e, e.flusher(maxRows), cut)
}

func drainCutWith(e *segEnv, f *BufferFlusher, cut int) int {
	e.t.Helper()
	var uploads atomic.Int64
	e.failFn.Store(func(key string) bool {
		if strings.Contains(key, "_segments/") {
			return false
		}
		return uploads.Add(1) > int64(cut)
	})
	e.drainAll(f)
	e.failFn.Store(func(string) bool { return false })
	if n := int(uploads.Load()); n < cut {
		return n
	}
	return cut
}

// The reproduction of #748: 12 rows, each in its own second; the first process
// plans groups of 5 and stores one before its second upload fails; the second
// process, after a clean restart, has groups of 1000. All 12 rows must be stored.
func TestDrainPlan_ChangedMaxRowsAcrossARestart_IssueRepro(t *testing.T) {
	e := newSegEnv(t)
	for s := 0; s < 12; s++ {
		e.ingest(segTenantA, hourAgo.Add(time.Duration(s)*time.Second), 1)
	}
	e.seal()
	uploads := 0
	e.failFn.Store(func(key string) bool {
		if !strings.Contains(key, "_segments/") {
			uploads++
		}
		return uploads > 1
	})
	e.drainAll(e.flusher(5))
	e.failFn.Store(func(string) bool { return false })
	e.restart()
	e.drainAll(e.flusher(1000))
	e.mustHaveExactly(12, "maxRows changed across the restart")
	e.storedOnce("maxRows changed across the restart")
	if n := len(e.segs.Pending()); n != 0 {
		t.Errorf("%d segments still pending", n)
	}
}

// How the first process dies, besides losing the object store between two
// uploads (drainCut): the restart then decides which groups are settled by the
// stored marks, by the manifest or by HEAD.
const (
	dieBetweenUploads = iota // the next PUT fails: marks and objects agree
	dieMarksLost             // the process died after a PUT and before its mark was durable: HEAD settles
	dieBeforeCommit          // every object and the marker are stored; the state still says draining
)

// The crash/restart matrix: the first process dies after some uploads, in each of
// the ways above, for a segment whose seconds are normal and for ones with crowded
// seconds, and the next process has a bigger or a smaller maxRows (or, without
// -short, the same one). Every acknowledged row is stored exactly once, and no key
// is stored twice. -short runs the cells of matrixCells; without it the process
// also dies after every number of uploads, for every pair.
func TestDrainPlan_CrashRestartMatrix_EveryRowInItsOwnSecond(t *testing.T) { crashRestartMatrix(t, 0) }
func TestDrainPlan_CrashRestartMatrix_OneCrowdedSecond(t *testing.T)       { crashRestartMatrix(t, 1) }
func TestDrainPlan_CrashRestartMatrix_TwoTenantsTwoHours(t *testing.T)     { crashRestartMatrix(t, 2) }

// (one test for each shape, so that the sharded CI runs spread them)
type matrixCell struct {
	kill, cut int
	m1, m2    int64
}

// matrixCells are the cells of one shape. With -short (the race jobs of CI, whose
// shards have a time budget) a few cells that each cover a different recovery
// path: the first upload stored and the plan changed in both directions, the
// marks lost (HEAD settles), and a kill between the marker and the state commit.
// Without it every upload boundary of the changed pairs also runs.
func matrixCells(t *testing.T, shapeIdx int, shape planShape) []matrixCell {
	const all = 1 << 30 // every upload succeeds
	cells := map[int][]matrixCell{
		0: {{dieBetweenUploads, 1, 5, 1000}},
		1: {{dieBetweenUploads, 1, 5, 1000}, {dieBetweenUploads, 2, 5, 1000}, {dieMarksLost, 1, 5, 1000}, {dieBeforeCommit, all, 5, 1000}, {dieBetweenUploads, 1, 3, 5}},
		2: {{dieBetweenUploads, 1, 5, 1000}, {dieBetweenUploads, 1, 3, 5}, {dieMarksLost, 1, 3, 5}},
	}[shapeIdx]
	if testing.Short() {
		return cells
	}
	for _, p := range [][2]int64{{5, 5}, {5, 1000}, {1000, 5}, {3, 5}} {
		e := newSegEnv(t)
		ingestShape(e, shape)
		e.seal()
		full := drainCut(e, p[0], all)
		for cut := 0; cut <= full; cut++ {
			cells = append(cells, matrixCell{dieBetweenUploads, cut, p[0], p[1]})
		}
	}
	return cells
}

func crashRestartMatrix(t *testing.T, shapeIdx int) {
	shape := planShapes()[shapeIdx]
	seen := map[matrixCell]bool{}
	for _, c := range matrixCells(t, shapeIdx, shape) {
		if seen[c] {
			continue
		}
		seen[c] = true
		c := c
		name := fmt.Sprintf("%s/maxRows %d then %d/kill %d/dies after %d uploads", shape.name, c.m1, c.m2, c.kill, c.cut)
		t.Run(name, func(t *testing.T) {
			inv0 := metrics.BufferFlushErrors.Get("invariant")
			defer func() { mustNoInvariantViolation(t, inv0) }()
			e := newSegEnv(t)
			ingestShape(e, shape)
			g := e.seal()
			f1 := e.flusher(c.m1)
			drainCutWith(e, f1, c.cut)
			switch c.kill {
			case dieMarksLost:
				_ = os.Remove(f1.storedPath())
			case dieBeforeCommit:
				// the segment was committed; rewind the state to what a crash
				// between the marker and the state write leaves
				if err := f1.writeState(flushState{DrainingSeq: g.Seq(), DrainingMaxRows: c.m1, DrainingPlan: planScheme}); err != nil {
					t.Fatal(err)
				}
			}
			e.restart()
			e.drainAll(e.flusher(c.m2))
			e.mustHaveExactly(shape.rows, name)
			e.storedOnce(name)
			if n := len(e.segs.Pending()); n != 0 {
				t.Errorf("%d segments still pending", n)
			}
		})
	}
}

// Every process of a chain of restarts, each with its own maxRows, finishes the
// drain with the first one's.
func TestDrainPlan_ChainOfRestartsKeepsTheFirstPlan(t *testing.T) {
	inv0 := metrics.BufferFlushErrors.Get("invariant")
	defer func() { mustNoInvariantViolation(t, inv0) }()
	e := newSegEnv(t)
	shape := planShapes()[1]
	ingestShape(e, shape)
	e.seal()
	for i, m := range []int64{4, 9, 2, 1000, 6} {
		var fail func(string) bool
		if i < 4 {
			uploads := 0
			fail = func(key string) bool {
				if strings.Contains(key, "_segments/") {
					return false
				}
				uploads++
				return uploads > 1 // one object per process
			}
		} else {
			fail = func(string) bool { return false }
		}
		e.failFn.Store(fail)
		e.drainAll(e.flusher(m))
		e.restart()
	}
	e.drainAll(e.flusher(1000))
	e.mustHaveExactly(shape.rows, "five processes, five maxRows")
	e.storedOnce("five processes, five maxRows")
	// the objects are the first plan's: no group is larger than the first maxRows
	for _, r := range e.objectRows() {
		if r > 4 {
			t.Errorf("an object of %d rows; the drain was planned with groups of at most 4", r)
		}
	}
}

// A drain resumed with its recorded maxRows does not carry it to the next
// segment: that one is planned with the configured value.
func TestDrainPlan_NextSegmentUsesTheConfiguredMaxRows(t *testing.T) {
	e := newSegEnv(t)
	base := hourAgo.UnixNano()
	for i := 0; i < 12; i++ {
		e.burst(segTenantA, 1, func(int) int64 { return base + sec(int64(i)) }, func(int) string { return rowID(i) })
	}
	e.seal()
	drainCut(e, 5, 1)
	e.restart()
	f := e.flusher(1000)
	e.drainAll(f)
	if n := len(e.segs.Pending()); n != 0 {
		t.Fatalf("%d segments pending after the resumed drain", n)
	}
	before := len(e.objectRows())
	for i := 12; i < 24; i++ {
		e.burst(segTenantA, 1, func(int) int64 { return base + sec(int64(i)) }, func(int) string { return rowID(i) })
	}
	e.seal()
	e.drainAll(f)
	rows := e.objectRows()
	if got := len(rows) - before; got != 1 {
		t.Errorf("the next segment was written as %d objects (%v); with 1000 rows per group it is one", got, rows)
	}
	e.mustHaveExactly(24, "second segment")
}

// Property: random rows (a few per second, so seconds are crowded or not, over
// one or two hours, two tenants), a chain of three processes with random maxRows
// and a random cut, and every acknowledged row is stored exactly once.
func TestDrainPlan_PropertyRandomMaxRows(t *testing.T) {
	iters := 30
	if testing.Short() {
		iters = 4
	}
	for seed := int64(1); seed <= int64(iters); seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 1 + rng.Intn(80)
		seconds := 1 + rng.Intn(25)
		hours := 1 + rng.Intn(2)
		tenants := []logstorage.TenantID{segTenantA, segTenantB}
		shape := planShape{rows: n, at: func(i int) (logstorage.TenantID, int64) {
			r := rand.New(rand.NewSource(seed*1000 + int64(i)))
			return tenants[r.Intn(len(tenants))], hourAgo.UnixNano() + sec(int64(r.Intn(seconds))) + int64(r.Intn(hours))*int64(time.Hour) + int64(r.Intn(3))
		}}
		ms := []int64{int64(1 + rng.Intn(60)), int64(1 + rng.Intn(60)), int64(1 + rng.Intn(60))}
		cuts := []int{rng.Intn(6), rng.Intn(6)}
		name := fmt.Sprintf("seed %d: %d rows, %d s, %d h, maxRows %v, cuts %v", seed, n, seconds, hours, ms, cuts)
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			inv0 := metrics.BufferFlushErrors.Get("invariant")
			defer func() { mustNoInvariantViolation(t, inv0) }()
			e := newSegEnv(t)
			ingestShape(e, shape)
			e.seal()
			drainCut(e, ms[0], cuts[0])
			e.restart()
			drainCut(e, ms[1], cuts[1])
			e.restart()
			e.drainAll(e.flusher(ms[2]))
			e.mustHaveExactly(n, name)
			e.storedOnce(name)
		})
	}
}

// Two write instances (each with its own buffer, flush state and nonce) drain
// into one bucket, both are cut short and restarted, each with another
// insert.target_file_size than before: each finishes its own drain with its own
// recorded value, no key is shared, every row is stored once.
func TestDrainPlan_TwoWritersEachRestartedWithAnotherMaxRows(t *testing.T) {
	const n = 12
	e := newSegEnv(t) // writer 1, and the bucket both write to
	base := hourAgo.UnixNano()
	e.burst(segTenantA, n, func(i int) int64 { return base + sec(int64(i)) }, rowID)

	dir2 := filepath.Join(t.TempDir(), "buffer")
	open2 := func() *membuffer.Segments {
		segs, err := membuffer.OpenSegments(membuffer.Config{Path: dir2})
		if err != nil {
			t.Fatal(err)
		}
		return segs
	}
	segs2 := open2()
	lr := logstorage.GetLogRows(crowdedStreamFields, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		lr.MustAdd(segTenantA, base+sec(int64(i)), crowdedFields(rowID(n+i)), 1)
	}
	segs2.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	if _, ok := segs2.Seal(); !ok {
		t.Fatal("nothing sealed")
	}
	e.seal()
	flusher2 := func(maxRows int64) *BufferFlusher {
		f := newBufferFlusher(e.bw, segs2, dir2, nil, BufferFlusherConfig{TargetBytes: maxRows * estBytesPerTraceRow, MaxAge: time.Hour, Grace: time.Minute})
		if err := f.load(time.Now()); err != nil {
			t.Fatal(err)
		}
		return f
	}

	// One object per instance, then the object store fails.
	var mu sync.Mutex
	perNonce := map[string]int{}
	e.failFn.Store(func(key string) bool {
		if strings.Contains(key, "_segments/") {
			return false
		}
		nonce := strings.SplitN(filepath.Base(key), "-", 2)[0]
		mu.Lock()
		defer mu.Unlock()
		perNonce[nonce]++
		return perNonce[nonce] > 1
	})
	e.drainAll(e.flusher(5))
	f2 := flusher2(3)
	for _, g := range segs2.Pending() {
		_ = f2.drain(context.Background(), g)
	}
	e.failFn.Store(func(string) bool { return false })

	// Both restart; writer 1 now has groups of 1000, writer 2 groups of 5.
	segs2.Close()
	e.restart()
	segs2 = open2()
	t.Cleanup(segs2.Close)
	f1, f2 := e.flusher(1000), flusher2(5)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, g := range e.segs.Pending() {
			_ = f1.drain(context.Background(), g)
		}
	}()
	go func() {
		defer wg.Done()
		for _, g := range segs2.Pending() {
			_ = f2.drain(context.Background(), g)
		}
	}()
	wg.Wait()
	e.mustHaveExactly(2*n, "two writers, each restarted with another maxRows")
	e.storedOnce("two writers, each restarted with another maxRows")
	if p1, p2 := len(e.segs.Pending()), len(segs2.Pending()); p1+p2 != 0 {
		t.Errorf("pending segments: %d and %d", p1, p2)
	}
}
