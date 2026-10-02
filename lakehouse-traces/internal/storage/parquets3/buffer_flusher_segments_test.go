package parquets3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// The durable insert path (membuffer.Segments + BufferFlusher) for spans. The
// invariant under test: every acknowledged span reaches Parquet exactly once —
// whatever its _time — and no object key is ever stored with two different byte sequences
// (faultyUploader checks that for every test at cleanup).

var (
	segTenantA = logstorage.TenantID{AccountID: 1, ProjectID: 2}
	segTenantB = logstorage.TenantID{AccountID: 2, ProjectID: 3}
)

// segEnv is a segmented buffer, a writer whose uploads go to a faultyUploader,
// and the flusher's state directory: everything that survives a restart.
type segEnv struct {
	t      *testing.T
	dir    string
	segs   *membuffer.Segments
	u      *faultyUploader
	marks  *faultyUploader // segment markers
	bw     *BatchWriter
	m      *manifest.Manifest
	keep   FlushRowFilter
	failFn atomic.Value // func(key string) bool
	hook   atomic.Value // func(key string): inside every upload, before it is stored
	seq    atomic.Int64 // row id source
}

func newSegEnv(t *testing.T) *segEnv {
	t.Helper()
	e := &segEnv{t: t, dir: t.TempDir()}
	e.failFn.Store(func(string) bool { return false })
	e.hook.Store(func(string) {})
	e.u = &faultyUploader{fail: func(key string) error {
		e.hook.Load().(func(string))(key)
		if e.failFn.Load().(func(string) bool)(key) {
			return errPutFailed
		}
		return nil
	}}
	e.marks = &faultyUploader{fail: func(key string) error {
		if e.failFn.Load().(func(string) bool)(key) {
			return errPutFailed
		}
		return nil
	}}
	e.newWriter()
	e.open()
	t.Cleanup(func() {
		if e.segs != nil {
			e.segs.Close()
		}
		e.marks.checkByteInvariant(t)
	})
	return e
}

func (e *segEnv) newWriter() {
	e.bw, e.m = durabilityWriter(e.t, e.u)
	e.bw.SetTenantPrefix(func(account, project uint32) string { return fmt.Sprintf("t%d-%d/", account, project) })
	e.bw.SetMarkerPool(e.marks)
}

func (e *segEnv) open() {
	e.t.Helper()
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: filepath.Join(e.dir, "buffer")})
	if err != nil {
		e.t.Fatalf("open segments: %v", err)
	}
	e.segs = segs
}

// restart is a process restart: the buffer is closed (upstream writes the
// in-memory rows; a sealed segment is on disk already) and reopened, and the
// writer has a new, empty manifest over the same object store.
func (e *segEnv) restart() {
	e.t.Helper()
	e.u.mu.Lock()
	e.u.allowAttemptDrift = true
	e.u.mu.Unlock()
	e.segs.Close()
	e.newWriter()
	e.open()
}

func (e *segEnv) flusher(maxRows int64) *BufferFlusher {
	e.t.Helper()
	f := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{
		TargetBytes: maxRows * estBytesPerTraceRow, MaxAge: time.Hour, Grace: time.Minute,
	})
	if err := f.load(time.Now()); err != nil {
		e.t.Fatalf("load flush state: %v", err)
	}
	return f
}

// ingest adds n spans of tenant with _time from ts on, 1 ms apart. Each span's
// span_id is unique (row-<id>), so the spans can be counted and told apart.
func (e *segEnv) ingest(tenant logstorage.TenantID, ts time.Time, n int) {
	e.t.Helper()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		id := e.seq.Add(1)
		lr.MustAdd(tenant, ts.Add(time.Duration(i)*time.Millisecond).UnixNano(), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "api"},
			{Name: "trace_id", Value: fmt.Sprintf("trace-%d", id)},
			{Name: "span_id", Value: fmt.Sprintf("row-%d", id)},
			{Name: "name", Value: "op"},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}

// drainAll runs ticks until nothing is pending or a tick makes no progress.
func (e *segEnv) drainAll(f *BufferFlusher) {
	e.t.Helper()
	for i := 0; i < 20 && len(e.segs.Pending()) > 0; i++ {
		f.nextTry = time.Time{}
		before := len(e.segs.Pending())
		for _, g := range e.segs.Pending() {
			if err := f.drain(context.Background(), g); err != nil {
				break
			}
		}
		if len(e.segs.Pending()) == before {
			return
		}
	}
}

func (e *segEnv) seal() *membuffer.Segment {
	e.t.Helper()
	g, ok := e.segs.Seal()
	if !ok {
		e.t.Fatal("nothing to seal")
	}
	return g
}

// storedMsgs returns the span_id of every row in every object stored and not
// deleted, with how many times each appears.
func (e *segEnv) storedMsgs() map[string]int {
	e.t.Helper()
	e.u.mu.Lock()
	objs := map[string][]byte{}
	for k, d := range e.u.data {
		if !e.u.gone[k] {
			objs[k] = d
		}
	}
	e.u.mu.Unlock()
	out := map[string]int{}
	for k, d := range objs {
		r := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(d))
		buf := make([]schema.TraceRow, 256)
		for {
			n, err := r.Read(buf)
			for _, row := range buf[:n] {
				out[row.SpanID]++
			}
			if err != nil {
				break
			}
		}
		if err := r.Close(); err != nil {
			e.t.Fatalf("read %s: %v", k, err)
		}
	}
	return out
}

// mustHaveExactly asserts that the stored objects hold rows row-1..row-n, each
// once.
func (e *segEnv) mustHaveExactly(n int, when string) {
	e.t.Helper()
	got := e.storedMsgs()
	var missing, dup []string
	for i := 1; i <= n; i++ {
		k := fmt.Sprintf("row-%d", i)
		switch c := got[k]; {
		case c == 0:
			missing = append(missing, k)
		case c > 1:
			dup = append(dup, fmt.Sprintf("%s×%d", k, c))
		}
	}
	if len(got) != n || len(missing) > 0 || len(dup) > 0 {
		sort.Strings(missing)
		e.t.Fatalf("%s: stored %d distinct rows, want %d; missing %d %v; duplicated %v", when, len(got), n, len(missing), head(missing, 5), head(dup, 5))
	}
}

// storedOnce fails if any object was stored more than once: a group already
// stored is settled, never sent again.
func (e *segEnv) storedOnce(when string) {
	e.t.Helper()
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	for k, n := range e.u.uploaded {
		if n > 1 {
			e.t.Errorf("%s: %s stored %d times", when, k, n)
		}
	}
}

func head(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func (e *segEnv) storedDataKeys() []string {
	e.u.mu.Lock()
	defer e.u.mu.Unlock()
	var out []string
	for k := range e.u.data {
		if !e.u.gone[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func (e *segEnv) markerStored(nonce string) bool {
	e.marks.mu.Lock()
	defer e.marks.mu.Unlock()
	for k := range e.marks.data {
		if strings.HasSuffix(k, manifest.SegmentMarkerDir+nonce) {
			return true
		}
	}
	return false
}

var hourAgo = time.Now().Add(-time.Hour).Truncate(time.Hour)

// --- the defects of the event-time flusher, as regression tests ---

// B1: a row acknowledged after rows with a later _time were already written
// to Parquet is written too. The event-time flusher never collected it.
func TestSegments_LateRowsReachParquet(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo.Add(10*time.Minute), 40)
	e.seal()
	f := e.flusher(1000)
	e.drainAll(f)
	e.mustHaveExactly(40, "first segment")

	// 7 rows arrive late: _time 50 minutes before the newest row already in Parquet.
	e.ingest(segTenantA, hourAgo.Add(5*time.Minute), 7)
	e.seal()
	e.drainAll(f)
	e.mustHaveExactly(47, "after the late rows")
}

// B2: backfill is kept whatever its age (up to the 2-day future limit
// upstream applies the other way). The 1 h buffer retention dropped rows older
// than about a day.
func TestSegments_BackfillIsKept(t *testing.T) {
	e := newSegEnv(t)
	now := time.Now()
	for _, age := range []time.Duration{10 * time.Minute, 26 * time.Hour, 3 * 24 * time.Hour, 40 * 24 * time.Hour} {
		e.ingest(segTenantA, now.Add(-age), 5)
	}
	e.seal()
	e.drainAll(e.flusher(1000))
	e.mustHaveExactly(20, "backfill")
}

// B3: memory is bounded by the group size, not the segment size: a segment far
// bigger than a group is written as many groups of at most maxRows rows.
func TestSegments_GroupsAreBoundedInRows(t *testing.T) {
	e := newSegEnv(t)
	// 3 rows per second over 40 s, one tenant, one hour.
	base := hourAgo.Add(20 * time.Minute)
	for s := 0; s < 40; s++ {
		e.ingest(segTenantA, base.Add(time.Duration(s)*time.Second), 3)
	}
	e.seal()
	f := e.flusher(10)
	var maxGroup int
	e.hook.Store(func(string) {})
	e.drainAll(f)
	for _, k := range e.storedDataKeys() {
		b := e.u.data[k]
		pf, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
		if err != nil {
			t.Fatal(err)
		}
		if n := int(pf.NumRows()); n > maxGroup {
			maxGroup = n
		}
	}
	if maxGroup > 10 {
		t.Errorf("a group of %d rows; want at most 10", maxGroup)
	}
	if got := len(e.storedDataKeys()); got < 12 {
		t.Errorf("%d objects for 120 rows at 10 per group; want at least 12", got)
	}
	e.mustHaveExactly(120, "bounded groups")
}

// One second holding more rows than a group is one group: the cut is per
// second, never inside one.
func TestPlanSlices(t *testing.T) {
	h := hourAgo.UnixNano()
	sec := int64(time.Second)
	counts := map[int64]int64{h: 4, h + sec: 4, h + 2*sec: 25, h + 3*sec: 1, h + 3600*sec: 2}
	got := planSlices(counts, 10)
	want := []flushSlice{
		{partition: partitionFromNano(h), slice: 0, start: h, end: h + 2*sec - 1, rows: 8},
		{partition: partitionFromNano(h), slice: 1, start: h + 2*sec, end: h + 3*sec - 1, rows: 25},
		{partition: partitionFromNano(h), slice: 2, start: h + 3*sec, end: h + 3600*sec - 1, rows: 1},
		{partition: partitionFromNano(h + 3600*sec), slice: 0, start: h + 3600*sec, end: h + 3601*sec - 1, rows: 2},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("planSlices =\n%v\nwant\n%v", got, want)
	}
	if again := planSlices(counts, 10); fmt.Sprint(again) != fmt.Sprint(got) {
		t.Error("planSlices is not deterministic")
	}
}

// B4: a restart with no flush state (nothing committed yet, or the state was
// lost) drains every segment found. The event-time flusher started at "now"
// and skipped everything buffered.
func TestSegments_RestartBeforeAnyCommitDrainsEverything(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo.Add(5*time.Minute), 30)
	e.seal()
	e.ingest(segTenantB, hourAgo.Add(6*time.Minute), 20) // still in the active segment at the crash
	e.restart()
	if _, err := os.Stat(filepath.Join(e.dir, "buffer", "buffer_flush_state.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no state file yet: %v", err)
	}
	e.drainAll(e.flusher(1000))
	e.mustHaveExactly(50, "after the restart")
	e.storedOnce("after the restart")
	if n := len(e.segs.Pending()); n != 0 {
		t.Errorf("%d segments still pending", n)
	}
}

// --- the crash matrix: a restart at every step of a drain ---

// crashAt runs one drain of a two-tenant, two-hour segment that stops at the
// given step, then restarts and drains again. Every row must be stored once
// and no key re-sent with other bytes.
func TestSegments_CrashMatrix(t *testing.T) {
	steps := []string{
		"before-draining-recorded",
		"after-draining-recorded",
		"after-first-put-before-mark",
		"after-first-mark",
		"after-all-puts",
		"marker-put-fails",
		"after-marker-before-commit",
		"after-commit",
	}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			e := newSegEnv(t)
			for _, tn := range []logstorage.TenantID{segTenantA, segTenantB} {
				e.ingest(tn, hourAgo.Add(-50*time.Minute), 10)
				e.ingest(tn, hourAgo.Add(10*time.Minute), 10)
			}
			g := e.seal()
			f := e.flusher(1000)
			stateDir := filepath.Join(e.dir, "buffer")

			crash := errors.New("crash")
			switch step {
			case "before-draining-recorded":
				blockDir(t, stateDir)
				if err := f.drain(context.Background(), g); err == nil {
					t.Fatal("the drain recorded its state through a read-only directory")
				}
				unblockDir(t, stateDir)
			case "after-draining-recorded":
				e.failFn.Store(func(string) bool { return true })
				_ = f.drain(context.Background(), g)
				e.failFn.Store(func(string) bool { return false })
			case "after-first-put-before-mark":
				// The first PUT is stored; the process dies before its mark.
				var n atomic.Int32
				e.hook.Store(func(key string) {
					if n.Add(1) == 2 {
						panic(crash)
					}
				})
				drainUntilPanic(t, f, g)
				e.hook.Store(func(string) {})
				// The first object is stored but has no mark (the PUT is stored
				// before the hook of the next one runs; drop its mark).
				_ = os.Remove(f.storedPath())
			case "after-first-mark":
				var n atomic.Int32
				e.hook.Store(func(string) {
					if n.Add(1) == 2 {
						panic(crash)
					}
				})
				drainUntilPanic(t, f, g)
				e.hook.Store(func(string) {})
			case "after-all-puts":
				e.failFn.Store(func(key string) bool { return strings.Contains(key, manifest.SegmentMarkerDir) })
				_ = f.drain(context.Background(), g)
				e.failFn.Store(func(string) bool { return false })
			case "marker-put-fails":
				e.failFn.Store(func(key string) bool { return strings.Contains(key, manifest.SegmentMarkerDir) })
				if err := f.drain(context.Background(), g); err == nil {
					t.Fatal("the segment committed without its marker")
				}
				if len(e.segs.Pending()) != 1 {
					t.Fatal("the segment left pending without its marker")
				}
				e.failFn.Store(func(string) bool { return false })
			case "after-marker-before-commit":
				blockAfterMarker(t, e, stateDir)
				_ = f.drain(context.Background(), g)
				unblockDir(t, stateDir)
			case "after-commit":
				if err := f.drain(context.Background(), g); err != nil {
					t.Fatal(err)
				}
			}

			var marked []string
			if step == "after-first-mark" {
				for ref := range f.loadMarks(g.Nonce()) {
					up := &traceGroupUpload{partition: ref.Partition, accountID: ref.Account, projectID: ref.Project, batchID: segmentBatchID(g.Nonce(), ref.Slice)}
					marked = append(marked, e.bw.assignTraceKey(up))
				}
				if len(marked) == 0 {
					t.Fatal("no durable mark before the crash")
				}
			}

			e.restart()
			f2 := e.flusher(1000)
			e.drainAll(f2)
			e.mustHaveExactly(40, "after the restart")
			// A group with a durable mark is settled without asking the store.
			for _, k := range marked {
				if n := e.u.headCount(k); n != 0 {
					t.Errorf("%s has a stored mark but was checked by HEAD %d times", k, n)
				}
			}
			e.storedOnce("after the restart")
			if !e.markerStored(g.Nonce()) {
				t.Error("no marker for the committed segment")
			}
			if len(e.segs.Pending()) != 0 {
				t.Error("a segment is still pending")
			}
		})
	}
}

// drainUntilPanic runs one drain that a hook stops by panicking (the process
// dying mid-upload).
func drainUntilPanic(t *testing.T, f *BufferFlusher, g *membuffer.Segment) {
	t.Helper()
	defer func() { _ = recover() }()
	_ = f.drain(context.Background(), g)
	t.Fatal("the drain was not interrupted")
}

func blockDir(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permissions are not enforced for root")
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
}

func unblockDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}
}

// blockAfterMarker makes the state directory read-only as soon as the
// segment's marker is stored, so the commit write fails.
func blockAfterMarker(t *testing.T, e *segEnv, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permissions are not enforced for root")
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })
	e.marks.mu.Lock()
	prev := e.marks.fail
	var once sync.Once
	e.marks.fail = func(key string) error {
		if err := prev(key); err != nil {
			return err
		}
		var err error
		once.Do(func() { err = os.Chmod(dir, 0o500) })
		return err
	}
	e.marks.mu.Unlock()
}

// A group whose PUT fails is retried with the very same bytes (faultyUploader
// fails the test otherwise) and the segment commits once it is stored.
func TestSegments_FailedPutIsRetriedWithTheSameBytes(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 15)
	e.ingest(segTenantB, hourAgo, 15)
	g := e.seal()
	f := e.flusher(1000)
	e.failFn.Store(func(key string) bool { return strings.HasPrefix(key, "t2-3/") })
	if err := f.drain(context.Background(), g); err == nil {
		t.Fatal("the drain succeeded with a failing tenant")
	}
	e.failFn.Store(func(string) bool { return false })
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	e.mustHaveExactly(30, "after the retry")
	for _, k := range e.storedDataKeys() {
		if e.u.attempts(k) > 2 {
			t.Errorf("%s attempted %d times", k, e.u.attempts(k))
		}
	}
}

// A PUT that fails is kept, encoded, and sent again as it is: when the store
// kept the object of a timed-out PUT, the retry must not store other bytes
// under its key.
func TestSegments_FailedUploadIsKeptForTheRetry(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 12)
	g := e.seal()
	f := e.flusher(1000)
	e.failFn.Store(func(key string) bool { return !strings.Contains(key, manifest.SegmentMarkerDir) })
	if err := f.drain(context.Background(), g); err == nil {
		t.Fatal("the drain succeeded with every PUT failing")
	}
	if len(f.retry) != 1 {
		t.Fatalf("%d failed groups kept for the retry; want 1", len(f.retry))
	}
	var kept *traceGroupUpload
	for _, up := range f.retry {
		kept = up
	}
	if kept.result == nil {
		t.Fatal("the failed group was not kept encoded")
	}
	e.failFn.Store(func(string) bool { return false })
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.u.data[kept.key], kept.result.Data) {
		t.Error("the retry stored other bytes than the failed attempt")
	}
	if len(f.retry) != 0 {
		t.Errorf("%d groups still kept after the retry", len(f.retry))
	}
}

// A HEAD error during recovery uploads nothing and retries; it never guesses.
func TestSegments_HeadErrorWaits(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 10)
	g := e.seal()
	f := e.flusher(1000)
	e.failFn.Store(func(string) bool { return true })
	_ = f.drain(context.Background(), g)
	e.failFn.Store(func(string) bool { return false })
	e.restart()
	e.u.mu.Lock()
	e.u.headErr = func(string) error { return errors.New("head unavailable") }
	e.u.mu.Unlock()
	f2 := e.flusher(1000)
	if err := f2.drain(context.Background(), e.segs.Pending()[0]); err == nil {
		t.Fatal("the drain went on without knowing whether the object exists")
	}
	if n := len(e.storedDataKeys()); n != 0 {
		t.Fatalf("%d objects uploaded while HEAD failed", n)
	}
	e.u.mu.Lock()
	e.u.headErr = nil
	e.u.mu.Unlock()
	e.drainAll(f2)
	e.mustHaveExactly(10, "after HEAD recovers")
}

// A group whose key a compaction retired is settled: its rows live in the
// compacted object; nothing is uploaded again and the rows are counted as
// superseded.
func TestSegments_RetiredGroupIsSettled(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 10)
	g := e.seal()
	f := e.flusher(1000)
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	keys := e.storedDataKeys()
	// Simulate a restart with the segment uncommitted and its object retired.
	if err := f.writeState(flushState{DrainingSeq: g.Seq()}); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(f.storedPath())
	e.restart()
	e.m.Retire(keys[0], "", false)
	before := metrics.InsertRowsSuperseded.Get()
	e.drainAll(e.flusher(1000))
	if got := e.u.attempts(keys[0]); got != 1 {
		t.Errorf("the retired key was attempted %d times", got)
	}
	if d := metrics.InsertRowsSuperseded.Get() - before; d != 10 {
		t.Errorf("superseded rows rose by %d; want 10", d)
	}
}

// The keep filter (cardinality, trace-shaped streams) drops rows from the
// objects; a group left empty uploads nothing.
func TestSegments_KeepFilter(t *testing.T) {
	e := newSegEnv(t)
	e.keep = func(account, _ uint32, _ string) bool { return account != segTenantB.AccountID }
	e.ingest(segTenantA, hourAgo, 10)
	e.ingest(segTenantB, hourAgo, 10)
	e.seal()
	e.drainAll(e.flusher(1000))
	for _, k := range e.storedDataKeys() {
		if strings.HasPrefix(k, "t2-3/") {
			t.Errorf("an object of a dropped tenant: %s", k)
		}
	}
	if got := len(e.storedMsgs()); got != 10 {
		t.Errorf("stored %d rows; want 10", got)
	}
}

// A state file and its backup that both exist and cannot be read stop the
// process instead of guessing.
func TestSegments_UnreadableStateStopsTheFlusher(t *testing.T) {
	e := newSegEnv(t)
	dir := filepath.Join(e.dir, "buffer")
	for _, name := range []string{"buffer_flush_state.json", "buffer_flush_state.json.prev"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{torn"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var msg string
	old := fatalf
	fatalf = func(format string, args ...any) { msg = fmt.Sprintf(format, args...) }
	defer func() { fatalf = old }()
	NewBufferFlusher(e.bw, e.segs, dir, nil, BufferFlusherConfig{})
	if !strings.Contains(msg, "delete both") {
		t.Errorf("no fatal stop: %q", msg)
	}
	// A torn main file falls back to the backup.
	if err := os.WriteFile(filepath.Join(dir, "buffer_flush_state.json.prev"), []byte(`{"version":5,"committed_through_seq":3}`), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newBufferFlusher(e.bw, e.segs, dir, nil, BufferFlusherConfig{})
	if err := f.load(time.Now()); err != nil || f.state.CommittedThroughSeq != 3 {
		t.Errorf("load from the backup: %v, state %+v", err, f.state)
	}
}

// Two buffers never derive the same key: the nonce is per segment.
func TestSegments_TwoNodesNeverShareKeys(t *testing.T) {
	a, b := newSegEnv(t), newSegEnv(t)
	b.u = a.u // one bucket
	b.newWriter()
	for _, e := range []*segEnv{a, b} {
		e.ingest(segTenantA, hourAgo, 5)
		e.seal()
		e.drainAll(e.flusher(1000))
	}
	if n := len(a.storedDataKeys()); n != 2 {
		t.Fatalf("%d objects for two nodes' segments; want 2", n)
	}
}

// Sealing: by age, by size while few segments wait, by age only beyond that,
// never an empty segment.
func TestSegments_SealPolicy(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusher(10) // 10 rows ≈ the size target
	f.maxAge = time.Hour
	f.maybeSeal(time.Now())
	if len(e.segs.Pending()) != 0 {
		t.Fatal("an empty segment was sealed")
	}
	e.ingest(segTenantA, hourAgo, 5)
	f.maybeSeal(time.Now())
	if len(e.segs.Pending()) != 0 {
		t.Fatal("sealed below the size target and age")
	}
	e.ingest(segTenantA, hourAgo, 5)
	f.maybeSeal(time.Now())
	if len(e.segs.Pending()) != 1 {
		t.Fatal("not sealed at the size target")
	}
	e.ingest(segTenantA, hourAgo, 1)
	f.maybeSeal(time.Now().Add(2 * time.Hour))
	if len(e.segs.Pending()) != 2 {
		t.Fatal("not sealed at the age limit")
	}
}

// Committed segments stay readable through the grace period and while a
// snapshot holds them, and are removed after.
func TestSegments_ReapRespectsGraceAndSnapshots(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 5)
	g := e.seal()
	f := e.flusher(1000)
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	dir := ""
	entries, _ := os.ReadDir(filepath.Join(e.dir, "buffer"))
	for _, en := range entries {
		if strings.Contains(en.Name(), g.Nonce()) {
			dir = filepath.Join(e.dir, "buffer", en.Name())
		}
	}
	snap := e.segs.Snapshot()
	if n := e.segs.Reap(time.Now().Add(time.Hour), time.Minute); n != 0 {
		t.Fatalf("reaped %d segments a snapshot holds", n)
	}
	if _, ok := snap.Nonces()[g.Nonce()]; !ok {
		t.Fatal("the snapshot lost the segment")
	}
	snap.Release()
	if n := e.segs.Reap(time.Now(), time.Hour); n != 0 {
		t.Fatal("reaped inside the grace period")
	}
	if n := e.segs.Reap(time.Now().Add(2*time.Hour), time.Hour); n != 1 {
		t.Fatalf("reaped %d; want the committed segment", n)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the segment directory is still there: %v", err)
	}
}

// A buffer directory of an earlier, single-store release is moved aside, not
// read or flushed.
func TestSegments_SingleStoreBufferIsMovedAside(t *testing.T) {
	dir := t.TempDir()
	st, err := membuffer.Open(membuffer.Config{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	ingestTraceAt(t, st, segTenantA, hourAgo.UnixNano(), hourAgo.Add(time.Minute).UnixNano(), 5)
	st.Close()
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer segs.Close()
	if n := len(segs.Pending()); n != 0 {
		t.Errorf("%d segments from the old layout", n)
	}
	var moved bool
	entries, _ := os.ReadDir(dir)
	for _, en := range entries {
		moved = moved || strings.HasPrefix(en.Name(), "legacy-")
	}
	if !moved {
		t.Error("the old layout was not moved aside")
	}
}

// Ingest keeps flowing while a drain runs: rows added during the drain land in
// the next segment and are written by the next drain, all exactly once.
func TestSegments_IngestDuringDrainLosesNothing(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusher(50)
	const batches = 400
	var wg sync.WaitGroup
	var done atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < batches; i++ {
			e.ingest(segTenantA, hourAgo.Add(time.Duration(i%3000)*time.Second), 7)
			done.Add(1)
			time.Sleep(200 * time.Microsecond)
		}
	}()
	for i := 0; done.Load() < batches; i++ {
		f.tick(context.Background(), time.Now().Add(time.Duration(i)*2*time.Hour))
	}
	wg.Wait()
	e.segs.Seal() // whatever arrived after the last tick
	e.drainAll(f)
	if n := len(e.segs.Pending()); n != 0 {
		t.Fatalf("%d segments still pending", n)
	}
	e.mustHaveExactly(batches*7, "ingest during drains")
}

// An upload "fails" on the client side but the store kept the object, and a
// manifest refresh adopts it: it is live. The drain's retry must not upload it
// again (that would rewrite a stored object) nor add it a second time — and it
// is not "superseded": nothing replaced it, its rows are simply already in the
// manifest.
func TestSegments_AdoptedObjectIsNotUploadedAgain(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 10)
	g := e.seal()
	f := e.flusher(1000)
	e.failFn.Store(func(key string) bool { return !strings.Contains(key, manifest.SegmentMarkerDir) })
	if err := f.drain(context.Background(), g); err == nil {
		t.Fatal("expected the first drain to fail")
	}
	e.failFn.Store(func(string) bool { return false })
	if len(f.retry) != 1 {
		t.Fatalf("%d groups kept for the retry, want 1", len(f.retry))
	}
	var key string
	for k := range f.retry {
		key = k
	}
	attempts := e.u.attempts(key)
	// The refresh lists the bucket and adopts the object the store kept.
	e.m.AddFile(partitionFromNano(hourAgo.UnixNano()), manifest.FileInfo{Key: key, Size: 1000, RowCount: 10, MinTimeNs: hourAgo.UnixNano(), MaxTimeNs: hourAgo.Add(time.Second).UnixNano()})
	superseded0 := metrics.InsertRowsSuperseded.Get()

	if err := f.drain(context.Background(), g); err != nil {
		t.Fatalf("the retry of an adopted group must complete, not fail: %v", err)
	}
	if got := e.u.attempts(key); got != attempts {
		t.Errorf("%s was uploaded again (%d attempts, was %d)", key, got, attempts)
	}
	if rows, files := committedRows(e.m); rows != 10 || files != 1 {
		t.Fatalf("committed %d rows in %d files, want 10 in 1 (the adopted object)", rows, files)
	}
	if d := metrics.InsertRowsSuperseded.Get() - superseded0; d != 0 {
		t.Errorf("superseded counter rose by %d: an adopted live object is not superseded", d)
	}
	if len(e.segs.Pending()) != 0 {
		t.Error("the segment is still pending")
	}
}

// Random PUT failures while rows keep arriving: every row is stored exactly
// once when the failures stop, and no key is ever stored with other bytes
// (faultyUploader checks that at cleanup).
func TestSegments_RandomFailuresUnderConcurrentIngestLoseNothing(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusher(40)
	rng := rand.New(rand.NewSource(1))
	var rngMu sync.Mutex
	e.failFn.Store(func(string) bool {
		rngMu.Lock()
		defer rngMu.Unlock()
		return rng.Intn(100) < 35
	})

	const workers, batches, perBatch = 3, 40, 5
	var wg sync.WaitGroup
	var done atomic.Int32
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			tenant := segTenantA
			if w%2 == 1 {
				tenant = segTenantB
			}
			for i := 0; i < batches; i++ {
				e.ingest(tenant, hourAgo.Add(time.Duration(w*batches+i)*time.Second), perBatch)
				time.Sleep(100 * time.Microsecond)
			}
			done.Add(1)
		}(w)
	}
	for i := 0; done.Load() < workers; i++ {
		f.nextTry = time.Time{}
		f.tick(context.Background(), time.Now().Add(time.Duration(i)*2*time.Hour))
	}
	wg.Wait()

	// The failures stop; whatever is left drains.
	e.failFn.Store(func(string) bool { return false })
	e.segs.Seal()
	for i := 0; i < 50 && len(e.segs.Pending()) > 0; i++ {
		f.nextTry = time.Time{}
		f.tick(context.Background(), time.Now().Add(time.Duration(i)*2*time.Hour))
	}
	if n := len(e.segs.Pending()); n != 0 {
		t.Fatalf("%d segments still pending", n)
	}
	e.mustHaveExactly(workers*batches*perBatch, "random failures under concurrent ingest")
}

// One tenant whose uploads fail holds back the segment's commit, not the other
// tenants' objects: they are written in the same pass. The failing tenant's
// later groups wait for the retry.
func TestSegments_OneFailingTenantDoesNotHoldBackOthers(t *testing.T) {
	e := newSegEnv(t)
	// Tenant A sorts first and has two groups (two hours); tenant B one.
	e.ingest(segTenantA, hourAgo.Add(-50*time.Minute), 10)
	e.ingest(segTenantA, hourAgo.Add(10*time.Minute), 10)
	e.ingest(segTenantB, hourAgo, 10)
	g := e.seal()
	f := e.flusher(1000)
	e.failFn.Store(func(key string) bool { return strings.HasPrefix(key, "t1-2/") })
	if err := f.drain(context.Background(), g); err == nil {
		t.Fatal("the segment committed with a failing tenant")
	}
	var storedA, storedB int
	for _, k := range e.storedDataKeys() {
		switch {
		case strings.HasPrefix(k, "t1-2/"):
			storedA++
		case strings.HasPrefix(k, "t2-3/"):
			storedB++
		}
	}
	if storedB != 1 || storedA != 0 {
		t.Errorf("stored: tenant A %d, tenant B %d; want 0 and 1 (B is written although A fails)", storedA, storedB)
	}
	e.u.mu.Lock()
	attemptedA := 0
	for k := range e.u.attemptHashes {
		if strings.HasPrefix(k, "t1-2/") {
			attemptedA++
		}
	}
	e.u.mu.Unlock()
	if attemptedA != 1 {
		t.Errorf("tenant A had %d groups attempted; want only the first (its later groups wait for the retry)", attemptedA)
	}
	if len(e.segs.Pending()) != 1 || e.markerStored(g.Nonce()) {
		t.Fatal("the segment must stay pending, without a marker, until every group is written")
	}
	e.failFn.Store(func(string) bool { return false })
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	e.mustHaveExactly(30, "after the retry")
	e.storedOnce("after the retry")
	if !e.markerStored(g.Nonce()) {
		t.Error("no marker after the commit")
	}
}
