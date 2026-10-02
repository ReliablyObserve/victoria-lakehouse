package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// ---------------------------------------------------------------------------
// One Lakehouse pod across restarts. The bucket and the buffer directory
// survive; the process state (manifest, caches, writer, flusher) does not. After
// every step each written row must be visible exactly once on every select
// surface, whether it is only in the insert buffer, in both the buffer and an
// object, or only in an object — and however the restarted pod learned about
// the objects: from a manifest snapshot, from the bucket listing alone (inferred
// time bounds), or from a snapshot an older release wrote.
// ---------------------------------------------------------------------------

var rwHour = time.Date(2026, 10, 1, 7, 0, 0, 0, time.UTC)

type restartEnv struct {
	t        *testing.T
	mock     *mockS3Server
	dir      string
	snapshot string
	s        *Storage
	bw       *BatchWriter
	segs     *membuffer.Segments
	f        *BufferFlusher
	written  map[string]uint64 // spans written per name
	total    int
	spans    int
}

func newRestartEnv(t *testing.T) *restartEnv {
	t.Helper()
	e := &restartEnv{t: t, mock: newMockS3Server(), dir: t.TempDir(), written: map[string]uint64{}}
	t.Cleanup(e.mock.close)
	e.snapshot = filepath.Join(e.dir, "manifest.snapshot")
	e.boot(manifest.New("test-bucket", "traces/"))
	t.Cleanup(func() { e.segs.Close() })
	return e
}

// boot starts a pod over the surviving directory and bucket with manifest m.
func (e *restartEnv) boot(m *manifest.Manifest) {
	e.t.Helper()
	s := testStorageWithS3(e.t, e.mock.url())
	s.manifest = m
	e.bw = NewBatchWriter(testInsertConfig(), s.pool, s.manifest, "traces/", config.ModeTraces)
	s.writer = e.bw
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: filepath.Join(e.dir, "buffer")})
	if err != nil {
		e.t.Fatal(err)
	}
	s.cfg.Mode = config.ModeTraces // as the traces binary runs
	s.localBuffer = segs
	e.s, e.segs = s, segs
	e.f = newBufferFlusher(e.bw, segs, filepath.Join(e.dir, "buffer"), nil, BufferFlusherConfig{
		TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: time.Hour, Grace: time.Minute})
	if err := e.f.load(time.Now()); err != nil {
		e.t.Fatal(err)
	}
}

// ingest adds one span per time to the insert buffer, named level.
func (e *restartEnv) ingest(level string, at ...time.Time) {
	e.t.Helper()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for _, ts := range at {
		e.spans++
		lr.MustAdd(logstorage.TenantID{}, ts.UnixNano(), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "svc-" + level},
			{Name: "trace_id", Value: fmt.Sprintf("trace-%d", e.spans)},
			{Name: "span_id", Value: fmt.Sprintf("span-%d", e.spans)},
			{Name: "name", Value: level},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()
	e.written[level] += uint64(len(at))
	e.total += len(at)
}

// flush seals the active segment and drains everything pending to Parquet.
func (e *restartEnv) flush() {
	e.t.Helper()
	e.segs.Seal()
	for _, g := range e.segs.Pending() {
		if err := e.f.drain(context.Background(), g); err != nil {
			e.t.Fatalf("drain: %v", err)
		}
	}
}

// reap removes the committed segments whose grace has passed.
func (e *restartEnv) reap() { e.segs.Reap(time.Now().Add(time.Hour), time.Minute) }

// restart stops the pod and starts a new one. snapshotBefore persists the
// manifest before the segments are drained, snapshotAfter after (runShutdown
// persists it too); drain says whether the pod got to write its buffer first
// (false is a crash with the rows only in the buffer). The new pod loads the
// snapshot it was given, then learns whatever else the bucket holds from the
// listing.
func (e *restartEnv) restart(snapshotBefore, snapshotAfter, drain bool) {
	e.t.Helper()
	if snapshotBefore {
		if err := e.s.manifest.SaveTo(e.snapshot); err != nil {
			e.t.Fatal(err)
		}
	}
	if drain {
		e.flush()
	}
	if snapshotAfter {
		if err := e.s.manifest.SaveTo(e.snapshot); err != nil {
			e.t.Fatal(err)
		}
	}
	e.segs.Close()
	m := manifest.New("test-bucket", "traces/")
	if snapshotBefore || snapshotAfter {
		if err := m.LoadFrom(e.snapshot); err != nil {
			e.t.Fatal(err)
		}
	}
	e.boot(m)
	if err := m.RefreshFromS3(context.Background(), e.s.pool.S3Client()); err != nil {
		e.t.Fatal(err)
	}
}

func (e *restartEnv) window(queryStr string, from, to time.Time) *logstorage.Query {
	e.t.Helper()
	return mustParseQueryWithTime(e.t, queryStr, from.UnixNano(), to.UnixNano())
}

// run executes queryStr over [from, to] and returns the number of rows emitted.
func (e *restartEnv) run(ctx context.Context, queryStr string, from, to time.Time) int {
	e.t.Helper()
	total := 0
	var mu sync.Mutex
	err := e.s.RunQuery(ctx, nil, e.window(queryStr, from, to), func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		total += db.RowsCount()
		mu.Unlock()
	})
	if err != nil {
		e.t.Fatalf("RunQuery(%s): %v", queryStr, err)
	}
	return total
}

func (e *restartEnv) levelHits(from, to time.Time) map[string]uint64 {
	e.t.Helper()
	got, err := e.s.GetFieldValues(context.Background(), nil, e.window("*", from, to), "name", 0)
	if err != nil {
		e.t.Fatalf("GetFieldValues: %v", err)
	}
	out := map[string]uint64{}
	for _, v := range got {
		out[v.Value] = v.Hits
	}
	return out
}

func at(base time.Time, d time.Duration) time.Time { return base.Add(d) }

func rwWindow() (time.Time, time.Time) { return rwHour.Add(-time.Hour), rwHour.Add(2 * time.Hour) }

// checkRows asserts that every select surface sees want rows in [from, to]:
// query=*, the timestamp-only count path (answered from metadata where it can
// be), the count pushdown, and the field_values of level.
func (e *restartEnv) checkRows(when string, from, to time.Time, want int) {
	e.t.Helper()
	if got := e.run(context.Background(), "*", from, to); got != want {
		e.t.Errorf("%s: query=* emitted %d rows, want %d", when, got, want)
	}
	if got := e.run(storage.WithTimestampOnlyHint(context.Background()), "* | stats count() n", from, to); got != want {
		e.t.Errorf("%s: stats count path emitted %d rows, want %d", when, got, want)
	}
	if got := e.run(context.Background(), "* | stats by (name) count() n", from, to); got != want {
		e.t.Errorf("%s: count pushdown emitted %d rows, want %d", when, got, want)
	}
}

// check asserts every written row is visible exactly once, on every surface.
func (e *restartEnv) check(when string) {
	e.t.Helper()
	from, to := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	e.checkRows(when, from, to, e.total)
	hits := e.levelHits(from, to)
	for lvl, n := range e.written {
		if hits[lvl] != n {
			e.t.Errorf("%s: field_values name %s = %d, want %d (all: %v)", when, lvl, hits[lvl], n, hits)
		}
	}
}

// checkAllStages checks the written rows while they are only in the buffer, then
// once drained (objects and the committed segments), then with the segments
// removed.
func (e *restartEnv) checkAllStages(when string) {
	e.t.Helper()
	e.check(when + ", in the buffer")
	e.flush()
	e.check(when + ", segments committed in grace")
	e.reap()
	e.check(when + ", segments removed")
}

// The reported bug of the earlier design: cold rows in hour H, a restart, rows
// buffered in hour H afterwards. They must be visible exactly once.
func TestBufferRestart_SameHourBufferedRowsVisibleExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		snapshotBefore, snapshotAfter bool
		drain                         bool
	}{
		{"snapshot before the drain only (the object is learned by listing)", true, false, true},
		{"snapshot after the drain (the object is in the snapshot)", true, true, true},
		{"no snapshot at all (cold manifest, listing only)", false, false, true},
		{"crash before the drain: the rows are only in the buffer", false, false, false},
		{"snapshot, then a crash before the drain", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newRestartEnv(t)
			e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 10*time.Minute+time.Second), at(rwHour, 10*time.Minute+2*time.Second))
			e.restart(tc.snapshotBefore, tc.snapshotAfter, tc.drain)
			e.ingest("NEW", at(rwHour, 30*time.Minute), at(rwHour, 30*time.Minute+time.Second), at(rwHour, 45*time.Minute))
			from, to := rwWindow()
			e.checkRows("after the restart", from, to, 6)
			e.checkAllStages("after the restart")
		})
	}
}

// Control: cold rows in the previous hour were never affected.
func TestBufferRestart_PreviousHourControl(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("COLD", at(rwHour, -40*time.Minute), at(rwHour, -39*time.Minute), at(rwHour, -38*time.Minute))
	e.restart(true, false, true)
	e.ingest("NEW", at(rwHour, 30*time.Minute), at(rwHour, 31*time.Minute), at(rwHour, 32*time.Minute))
	e.checkAllStages("previous hour")
}

// Rows that straddle the hour boundary: the object of the earlier hour must not
// hide the buffered rows after it, nor those of the next hour.
func TestBufferRestart_StraddlingTheHourBoundary(t *testing.T) {
	e := newRestartEnv(t)
	boundary := rwHour.Add(time.Hour) // 08:00
	e.ingest("COLD", at(boundary, -3*time.Second), at(boundary, -2*time.Second), at(boundary, -time.Second))
	e.restart(true, false, true)
	e.ingest("NEW", at(boundary, -500*time.Millisecond), at(boundary, time.Second), at(boundary, 20*time.Minute))
	e.checkRows("straddling", rwHour, boundary.Add(time.Hour), 6)
	// Narrow windows on either side of the boundary.
	if got := e.run(context.Background(), "*", boundary, boundary.Add(time.Hour)); got != 2 {
		t.Errorf("rows at or after the boundary = %d, want 2", got)
	}
	e.checkAllStages("straddling")
}

// Cold rows spanning two hours (two objects) and buffered rows after the newest.
func TestBufferRestart_TwoObjectsTwoHours(t *testing.T) {
	e := newRestartEnv(t)
	boundary := rwHour.Add(time.Hour)
	e.ingest("COLD", at(boundary, -time.Minute), at(boundary, time.Minute), at(boundary, 2*time.Minute))
	e.restart(true, false, true)
	e.ingest("NEW", at(boundary, 10*time.Minute), at(boundary, 11*time.Minute), at(boundary, 12*time.Minute))
	e.checkRows("two hours", rwHour, boundary.Add(time.Hour), 6)
	e.checkAllStages("two hours")
}

// No double counting: a row in the buffer before a restart and in the objects
// drained from it is counted once, however many times the pod restarts.
func TestBufferRestart_NoDoubleCountAcrossRestarts(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("COLD", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute), at(rwHour, 7*time.Minute))
	e.restart(true, false, true)
	e.check("after the first restart")
	e.restart(true, true, true) // nothing new: the object is already exact
	e.check("after the second restart")
	e.ingest("NEW", at(rwHour, 20*time.Minute))
	e.check("with a new row")
	e.restart(false, false, false)
	e.check("after a crash")
}

// UTC midnight: an object in hour 23 of day D, rows buffered on D+1.
func TestBufferRestart_AcrossUTCMidnight(t *testing.T) {
	e := newRestartEnv(t)
	h23 := time.Date(2026, 9, 30, 23, 0, 0, 0, time.UTC)
	mid := h23.Add(time.Hour)
	e.ingest("COLD", at(h23, 50*time.Minute), at(h23, 59*time.Minute))
	e.restart(true, false, true)
	e.ingest("NEW", at(mid, 10*time.Minute))
	e.checkRows("midnight", h23, mid.Add(26*time.Hour), 3)
	e.checkAllStages("midnight")
}

// A query window that starts in the middle of an object's hour sees only the
// rows inside it.
func TestBufferRestart_WindowStartsMidHour(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 40*time.Minute))
	e.restart(true, false, true)
	from, to := at(rwHour, 30*time.Minute), rwHour.Add(2*time.Hour)
	e.checkRows("mid-hour window", from, to, 1)
	e.flush()
	e.checkRows("mid-hour window, drained", from, to, 1)
}

// Objects with exact bounds are answered from metadata inside the window; the
// buffered rows of the window are added to them, never counted again.
func TestBufferRestart_MetadataAnsweredObjectsAreNotCountedTwice(t *testing.T) {
	e := newRestartEnv(t)
	prev := rwHour.Add(-time.Hour)
	e.ingest("OLD", at(prev, 50*time.Minute), at(prev, 58*time.Minute))
	e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 20*time.Minute))
	e.restart(true, true, true) // the snapshot holds exact bounds
	for _, fi := range e.s.manifest.GetFilesForRange(0, 1<<62) {
		if fi.BoundsInferred {
			t.Fatalf("precondition: exact bounds expected, got %+v", fi)
		}
	}
	from, to := at(prev, 55*time.Minute), rwHour.Add(2*time.Hour)
	e.checkRows("metadata-answered", from, to, 3)
	// The segment of the restart is still live: its rows and its objects are
	// both there, and the objects must not be read on top of the rows.
	e.ingest("NEW", at(rwHour, 30*time.Minute))
	e.checkRows("metadata-answered, one more buffered", from, to, 4)
	e.checkAllStages("metadata-answered")
}

// Same hour: the objects drained before a restart are learned by listing
// (inferred bounds); after the restart the pod drains newer rows of the same
// hour (exact bounds).
func TestBufferRestart_SameHourNewerFlushAfterRestart(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute))
	e.restart(true, false, true)
	e.ingest("NEW", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute))
	e.flush()
	from, to := rwWindow()
	e.checkRows("same hour", from, to, 4)
	if hits := e.levelHits(from, to); hits["COLD"] != 2 || hits["NEW"] != 2 {
		t.Errorf("field_values = %v", hits)
	}
	e.reap()
	e.check("same hour, segments removed")
}

// A filtered query reads only the objects its filter can match and the buffer;
// it must count the matching rows once.
func TestBufferRestart_FilteredQueryCountsMatchingRowsOnce(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("C-1", at(rwHour, 10*time.Minute), at(rwHour, 20*time.Minute))
	e.flush()
	e.ingest("O-1", at(rwHour, 30*time.Minute), at(rwHour, 40*time.Minute))
	e.restart(true, true, true)
	e.ingest("C-1", at(rwHour, 35*time.Minute))
	from, to := rwWindow()
	if got := e.run(context.Background(), `name:="C-1"`, from, to); got != 3 {
		t.Errorf("filtered rows = %d, want 3 (2 cold + the unflushed one)", got)
	}
	e.flush()
	if got := e.run(context.Background(), `name:="C-1"`, from, to); got != 3 {
		t.Errorf("filtered rows after the drain = %d, want 3", got)
	}
}

// A manifest snapshot an older release wrote holds the object's whole hour as
// if it were its exact range. The object's rows must still be counted once, with
// the segment live and after it is removed.
func TestBufferRestart_LegacyHourWideSnapshotEntry(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 10*time.Minute+time.Second), at(rwHour, 10*time.Minute+2*time.Second))
	e.flush()
	files := e.s.manifest.GetFilesForRange(0, 1<<62)
	if len(files) != 1 {
		t.Fatalf("%d objects", len(files))
	}
	legacy := files[0]
	legacy.BoundsInferred = false
	legacy.MinTimeNs = rwHour.UnixNano()
	legacy.MaxTimeNs = rwHour.Add(time.Hour).UnixNano() - 1
	old := manifest.New("test-bucket", "traces/")
	old.AddFile(manifest.ExtractPartition(legacy.Key), legacy)
	if err := old.SaveTo(e.snapshot); err != nil {
		t.Fatal(err)
	}
	e.segs.Close()
	m := manifest.New("test-bucket", "traces/")
	if err := m.LoadFrom(e.snapshot); err != nil {
		t.Fatal(err)
	}
	e.boot(m)
	e.ingest("NEW", at(rwHour, 30*time.Minute), at(rwHour, 31*time.Minute), at(rwHour, 32*time.Minute))
	e.checkRows("legacy snapshot", rwHour.Add(-time.Hour), rwHour.Add(2*time.Hour), 6)
	e.checkAllStages("legacy snapshot")
}

// Property: random sequences of (write, drain, restart with or without each
// snapshot, crash, segment removal) with random times across hour boundaries;
// every written row is visible exactly once on every surface after every step.
func TestBufferRestart_Property_EveryRowVisibleExactlyOnce(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			restartPropertyRun(t, seed, 12)
		})
	}
}

func FuzzBufferRestart(f *testing.F) {
	for _, seed := range []int64{0, 1, 7, 42, 1 << 40} {
		f.Add(seed, uint8(10))
	}
	f.Fuzz(func(t *testing.T, seed int64, steps uint8) {
		restartPropertyRun(t, seed, int(steps%10)+1) // short: the fuzz engine kills an input that runs over 10s
	})
}

func restartPropertyRun(t *testing.T, seed int64, steps int) {
	rng := rand.New(rand.NewSource(seed))
	e := newRestartEnv(t)
	clock := rwHour.Add(time.Duration(rng.Intn(50*60)) * time.Second)
	next := func() time.Time {
		step := time.Duration(1+rng.Intn(40)) * time.Second
		if rng.Intn(5) == 0 {
			step = time.Duration(1+rng.Intn(50)) * time.Minute
		}
		clock = clock.Add(step)
		return clock
	}
	epoch := 0
	for i := 0; i < steps; i++ {
		switch rng.Intn(6) {
		case 0:
			e.restart(rng.Intn(2) == 0, rng.Intn(2) == 0, rng.Intn(2) == 0)
			epoch++
		case 1:
			e.flush()
		case 2:
			e.reap()
		default:
			n := 1 + rng.Intn(3)
			ts := make([]time.Time, n)
			for j := range ts {
				ts[j] = next()
			}
			e.ingest(fmt.Sprintf("E%d", epoch), ts...)
		}
		e.check(fmt.Sprintf("seed %d step %d", seed, i))
		if t.Failed() {
			t.FailNow()
		}
	}
}

// The buffered values of field_values must survive the engine reusing its block
// memory: with the keys aliasing it, rows of different values collapsed into
// one wrong value (seen live on the traces binary: all 15 buffered spans counted
// as "repro-buffer-1", sometimes split 10/5).
func TestBufferRestart_BufferedFieldValuesAreCopied(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("COLD", at(rwHour, 5*time.Minute))
	e.restart(true, true, true)
	e.ingest("ALPHA", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute))
	e.ingest("BETA", at(rwHour, 12*time.Minute))
	// More blocks through the engine, so its block memory is reused.
	for i := 0; i < 20; i++ {
		e.ingest(fmt.Sprintf("FILL%d", i), at(rwHour, time.Duration(13+i)*time.Minute))
	}
	from, to := rwWindow()
	hits := e.levelHits(from, to)
	if hits["ALPHA"] != 2 || hits["BETA"] != 1 || hits["COLD"] != 1 || len(hits) != 23 {
		t.Errorf("field_values over buffered rows = %v, want ALPHA=2 BETA=1 COLD=1 and 20 fillers", hits)
	}
}
