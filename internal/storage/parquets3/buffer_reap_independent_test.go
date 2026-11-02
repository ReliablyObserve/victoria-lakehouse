package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// #611: an insert pod answered the rows of a committed segment twice, once from
// its buffer and once from the object compaction had merged them into.
//
// The hand-off from the buffer to the objects is a time contract. The pod
// serves a committed segment for `grace` after its commit and then retires it
// (membuffer Segments.Reap); compaction leaves the segment's objects alone until
// the segment's marker is 2 x grace old (manifest.SegmentGuard). The first half
// held only while the flusher's tick ran, because the tick reaped after its
// drain: a drain that outlasts the grace (a slow object store, a backlog after
// an outage) kept serving segments compaction was already free to merge. The
// flusher now reaps on its own cadence, so a committed segment is served no
// longer than grace plus one check interval after its commit, whatever a drain
// is doing.
//
// What each test pins (measured against the flusher before the fix, and against
// mutants of it, in both binaries):
//
//   - Fail without the fix: TestReap_ASlowDrainDoesNotKeepAGuardReleasedSegmentServed
//     (a blocked drain; also the bound "retired within the grace plus a check
//     interval", which a loop that reaps at 2 x grace breaks),
//     TestReap_FleetAnswersEveryRowOnce_RandomSchedules (two insert and two
//     select pods running the real flusher, a drain that stalls) and
//     TestReap_StopEndsTheReapLoop (the loop exists, and ends with Stop).
//   - Contract locks, which pass without the loop and are not evidence that it
//     exists: TestReap_EveryAgeOfACommittedSegmentAnswersEachRowOnce (drives Reap
//     itself and pins the guard's 2 x grace) and
//     TestReap_TheOtherSignalIsUnaffected (the bridge's isolation of the other
//     signal). The registry rows for the slow-drain cells reference them too (a
//     pass row may not drop a reference) and say in their notes that they are locks.

// reapGate is a bucket that deletes (the plain mock answers a DELETE like a GET,
// so compaction's inputs would stay listed) and whose PUTs of the keys containing
// the armed match wait until the gate is opened: an object store that is slow for
// one segment. It can be armed again after it was opened.
type reapGate struct {
	*mockS3Server
	matchMu sync.Mutex
	match   string
	hit     chan struct{}
	release chan struct{}
}

func newReapGate() *reapGate {
	g := &reapGate{mockS3Server: newMockS3Server(), hit: make(chan struct{}, 1)}
	g.srv.Close()
	g.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.matchMu.Lock()
		match, release := g.match, g.release
		g.matchMu.Unlock()
		if r.Method == http.MethodDelete {
			g.mu.Lock()
			delete(g.files, strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)[1])
			g.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method == http.MethodPut && match != "" && release != nil && strings.Contains(r.URL.Path, match) {
			select {
			case g.hit <- struct{}{}:
			default:
			}
			<-release
		}
		g.handler(w, r)
	}))
	return g
}

// block arms the gate: the PUTs of keys containing match wait for open.
func (g *reapGate) block(match string) {
	g.matchMu.Lock()
	g.match = match
	g.release = make(chan struct{})
	g.matchMu.Unlock()
	select {
	case <-g.hit:
	default:
	}
}

// open lets the waiting PUTs through and disarms the gate. It is safe to call
// when the gate is not armed.
func (g *reapGate) open() {
	g.matchMu.Lock()
	if g.release != nil {
		close(g.release)
		g.release = nil
	}
	g.match = ""
	g.matchMu.Unlock()
}

// selectPodOver is a select pod over e's bucket whose buffer bridge asks the
// insert pods at peers.
func (e *restartEnv) selectPodOver(peers ...string) *Storage {
	e.t.Helper()
	sel := testStorageWithS3(e.t, e.mock.url())
	sel.cfg.Mode = e.bw.mode
	sel.manifest = manifest.New("test-bucket", e.bw.prefix)
	sel.bufferBridge = NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 5 * time.Second}, e.bw.mode)
	sel.bufferBridge.SetEndpoints(peers)
	return sel
}

// serveBuffer exposes e's segments on the buffer bridge, as the insert pod does.
func (e *restartEnv) serveBuffer() string {
	e.t.Helper()
	srv := httptest.NewServer(buffer.NewHandler(BridgeSource{Segments: e.segs}, ""))
	e.t.Cleanup(srv.Close)
	return srv.URL
}

// countByQuery is the unfiltered single-field count the manifest answers.
func (e *restartEnv) countByQuery() string {
	if e.bw.mode == config.ModeTraces {
		return "* | stats by (name) count() n"
	}
	return "* | stats by (level) count() n"
}

// compactReleased is what compaction does at now to the segment objects the
// guard releases: it merges those of a partition that share a schema
// fingerprint (an object another pod wrote, learned by listing, has none here).
// It returns how many objects it merged.
func (e *restartEnv) compactReleased(markers map[string]time.Time, protect time.Duration, now time.Time) int {
	e.t.Helper()
	guard := &manifest.SegmentGuard{Markers: markers, Listed: true, Protect: protect}
	c := compaction.NewCompactor(compaction.CompactorConfig{
		Pool: e.s.pool, Manifest: e.s.manifest, Prefix: e.bw.prefix, Mode: e.bw.mode,
		RowGroupSize: 1000, CompressionLevel: 3, CompactionConfig: config.CompactionConfig{},
	})
	type group struct{ partition, fingerprint string }
	byGroup := map[group][]manifest.FileInfo{}
	e.s.manifest.RangePartitions(func(part string, files []manifest.FileInfo) bool {
		for _, f := range guard.ReleasedFiles(files, now) {
			if manifest.SegmentNonceOfKey(f.Key) != "" {
				g := group{part, f.SchemaFingerprint}
				byGroup[g] = append(byGroup[g], f)
			}
		}
		return true
	})
	merged := 0
	for g, files := range byGroup {
		if len(files) < 2 {
			continue
		}
		if _, err := c.Compact(context.Background(), g.partition, files, 0); err != nil {
			e.t.Fatalf("compact %s: %v", g.partition, err)
		}
		merged += len(files)
	}
	return merged
}

// rowsOn counts the rows queryStr returns on s over the whole test window.
func rowsOn(t *testing.T, s *Storage, ctx context.Context, queryStr string) int {
	t.Helper()
	from, to := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	q := mustParseQueryWithTime(t, queryStr, from.UnixNano(), to.UnixNano())
	total := 0
	var mu sync.Mutex
	if err := s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		total += db.RowsCount()
		mu.Unlock()
	}); err != nil {
		t.Fatalf("RunQuery(%s): %v", queryStr, err)
	}
	return total
}

// checkPod asserts s answers want rows on the surfaces that count: query=*, the
// timestamp-only count path and the count pushdown.
func checkPod(t *testing.T, name string, s *Storage, e *restartEnv, when string, want int) {
	t.Helper()
	if got := rowsOn(t, s, context.Background(), "*"); got != want {
		t.Errorf("%s, %s: query=* returned %d rows, want %d", name, when, got, want)
	}
	if got := rowsOn(t, s, storage.WithTimestampOnlyHint(context.Background()), "* | stats count() n"); got != want {
		t.Errorf("%s, %s: stats count path returned %d rows, want %d", name, when, got, want)
	}
	if got := rowsOn(t, s, context.Background(), e.countByQuery()); got != want {
		t.Errorf("%s, %s: count pushdown returned %d rows, want %d", name, when, got, want)
	}
}

// A drain that takes longer than twice the grace must not keep a committed
// segment served after compaction merged its objects, and the loop that retires
// the segment must do it within the grace plus one check interval, well before
// the merge window of 2 x grace opens.
func TestReap_ASlowDrainDoesNotKeepAGuardReleasedSegmentServed(t *testing.T) {
	const grace = 800 * time.Millisecond
	gate := newReapGate()
	e := newRestartEnvWith(t, gate.mockS3Server)
	e.f.grace = grace
	t.Cleanup(func() { gate.open(); e.f.Stop() }) // runs before the segments close

	e.ingest("A", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute))
	e.flush()
	e.ingest("B", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute))
	e.flush()
	committed := time.Now() // not before the markers' LastModified: the guard is never early
	markers := e.segmentMarkers(committed)
	e.check("two segments committed")
	if st := e.segs.Stats(time.Now()); st.Committed != 2 {
		t.Fatalf("before the loop runs: %d committed segments are served, want the 2 just committed", st.Committed)
	}

	// A third segment whose object store is slow: its drain does not return.
	e.ingest("C", at(rwHour, 30*time.Minute), at(rwHour, 31*time.Minute))
	third, ok := e.segs.Seal()
	if !ok {
		t.Fatal("no segment to seal")
	}
	gate.block("/" + third.Nonce() + "-")
	e.f.Start(10 * time.Millisecond)
	select {
	case <-gate.hit:
	case <-time.After(10 * time.Second):
		t.Fatal("the flusher never started the third segment's drain")
	}

	// A committed segment is served for the grace plus one check interval and no
	// longer, whatever the drain is doing: 250 ms (25 check intervals) after the
	// grace, well before compaction merges at 2 x grace, the first two segments
	// are retired while the drain still waits.
	for retiredBy := committed.Add(grace + 250*time.Millisecond); e.segs.Stats(time.Now()).Committed > 0 && time.Now().Before(retiredBy); {
		time.Sleep(5 * time.Millisecond)
	}
	if n := e.segs.Stats(time.Now()).Committed; n != 0 {
		t.Fatalf("%d committed segments are still served %s after their commit (grace %s) with the drain in flight: they must be retired within the grace plus a check interval, before the merge window of %s opens", n, time.Since(committed).Round(time.Millisecond), grace, 2*grace)
	}
	if st := e.segs.Stats(time.Now()); st.Pending != 1 {
		t.Fatalf("pending segments = %d, want the third segment alone still waiting on its drain", st.Pending)
	}

	// Honest clocks: the guard releases the first two segments' objects 2 x grace
	// after their commit, on the real clock, while the drain is still in flight.
	time.Sleep(time.Until(committed.Add(2*grace + 100*time.Millisecond)))
	if n := e.compactSegmentObjects(markers, 2*grace, time.Now()); n != 2 {
		t.Fatalf("compaction merged %d objects, want the 2 the guard released", n)
	}
	e.check("drain in flight past 2 x grace, objects merged")

	gate.open()
	deadline := time.Now().Add(10 * time.Second)
	for e.segs.Stats(time.Now()).Pending > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	e.check("drain finished")
}

// waitCommitted waits until the flusher of e has written every sealed segment
// and committed it.
func waitCommitted(t *testing.T, e *restartEnv) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for e.segs.Stats(time.Now()).Pending > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the flusher did not commit the sealed segments in 10 s: %+v", e.segs.Stats(time.Now()))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The same contract over a fleet: two insert pods and two select pods over one
// bucket, every pod reading the unflushed rows of both insert pods over the
// buffer bridge. Each insert pod runs the real flusher (Start): its tick drains the
// sealed segments, its reap loop retires the committed ones, and no step calls
// Reap or drains by hand. Random schedules of ingest, flush,
// time passing, guarded compaction (by either insert pod, with the real guard)
// and a drain that stalls on the object store while compaction merges the
// objects of the segments committed before it, overlap rows of different
// segments in the same hour. After every step every pod answers every row once
// on every counting surface, whatever mix of buffer-only, flushed and compacted
// rows exists (a writer and a select pod agree). Without the reap loop the
// schedule serves a segment next to the object compaction merged it into.
func TestReap_FleetAnswersEveryRowOnce_RandomSchedules(t *testing.T) {
	const (
		grace    = 100 * time.Millisecond
		interval = 10 * time.Millisecond
	)
	seeds := int64(5)
	if testing.Short() {
		seeds = 2 // the heavy job runs all five
	}
	merges, stalledMerges := 0, 0
	for seed := int64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			bucket := newReapGate()
			w := []*restartEnv{newRestartEnvWith(t, bucket.mockS3Server), newRestartEnvWith(t, bucket.mockS3Server)}
			for _, e := range w {
				e.f.grace = grace
				e.f.Start(interval) // the real flusher: its tick drains, its reap loop retires
				t.Cleanup(e.f.Stop) // before the segments close
			}
			t.Cleanup(bucket.open) // before the flushers stop: a drain held by the gate cannot stop
			peers := []string{w[0].serveBuffer(), w[1].serveBuffer()}
			pods := map[string]*Storage{}
			for i, e := range w {
				// An insert pod that serves selects reads every insert pod, itself included.
				e.s.bufferBridge = NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 5 * time.Second}, e.bw.mode)
				e.s.bufferBridge.SetEndpoints(peers)
				pods[fmt.Sprintf("writer-%d", i)] = e.s
			}
			pods["select-0"] = w[0].selectPodOver(peers...)
			pods["select-1"] = w[0].selectPodOver(peers...)

			commit := map[string]time.Time{} // segment nonce -> not before its marker's time
			total := 0
			// flush seals the pod's active segment and waits until the flusher
			// has written it and committed it.
			flush := func(e *restartEnv) {
				t.Helper()
				e.segs.Seal()
				waitCommitted(t, e)
			}
			record := func(e *restartEnv) { // the commit of every segment e has written
				for _, f := range e.objects() {
					if n := manifest.SegmentNonceOfKey(f.Key); n != "" {
						if _, seen := commit[n]; !seen {
							commit[n] = time.Now()
						}
					}
				}
			}
			refreshAll := func() {
				t.Helper()
				for name, s := range pods {
					if err := s.RefreshManifest(context.Background()); err != nil {
						t.Fatalf("%s refresh: %v", name, err)
					}
				}
			}
			checkAll := func(when string) {
				t.Helper()
				refreshAll()
				for name, s := range pods {
					checkPod(t, name, s, w[0], when, total)
				}
			}
			// slowDrain is a drain that stalls on the object store: two segments
			// are committed, a third is sealed and its first PUT waits, and
			// compaction merges the objects of the first two (the guard releases
			// them 2 x grace after their commit) while the drain is in flight.
			var trail []string
			step := 0
			slowDrain := func(i int) (string, int) {
				e := w[i]
				e.ingest("A", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute))
				flush(e)
				e.ingest("B", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute))
				flush(e)
				record(e)
				e.ingest("C", at(rwHour, 30*time.Minute), at(rwHour, 31*time.Minute))
				total += 6
				// The gate is armed before the seal: the running flusher's tick may
				// start the segment's drain the moment it is sealed.
				nonce := e.segs.Active().Nonce()
				bucket.block("/" + nonce + "-")
				third, ok := e.segs.Seal()
				if !ok || third.Nonce() != nonce {
					bucket.open()
					t.Fatalf("sealed segment %v (sealed=%v), want the active segment %s", third, ok, nonce)
				}
				select { // the flusher's tick starts the drain and stays in it
				case <-bucket.hit:
				case <-time.After(10 * time.Second):
					bucket.open()
					t.Fatal("the drain of the third segment never reached the object store")
				}
				time.Sleep(2*grace + 30*time.Millisecond)
				if err := e.s.RefreshManifest(context.Background()); err != nil {
					t.Fatal(err)
				}
				merged := e.compactReleased(commit, 2*grace, time.Now())
				checkAll(fmt.Sprintf("seed %d, step %d, a drain in flight past 2 x grace, objects merged; trail: %s", seed, step, strings.Join(trail, " | ")))
				bucket.open()
				waitCommitted(t, e)
				record(e)
				return fmt.Sprintf("writer-%d: drain stalled past 2 x grace while compaction merged %d objects", i, merged), merged
			}
			for ; step < 24; step++ {
				i := rng.Intn(2)
				e := w[i]
				kind := rng.Intn(5)
				if step == 10 {
					kind = 4 // every schedule has a stalled drain
				}
				var op string
				switch kind {
				case 0: // rows of one or two hours, older rows after newer ones
					n := 1 + rng.Intn(3)
					ts := make([]time.Time, n)
					for k := range ts {
						ts[k] = at(rwHour, time.Duration(rng.Intn(100))*time.Minute)
					}
					e.ingest(fmt.Sprintf("L%d", rng.Intn(3)), ts...)
					total += n
					op = fmt.Sprintf("ingest %d rows on writer-%d", n, i)
				case 1: // seal and write the pod's segments
					flush(e)
					record(e)
					op = fmt.Sprintf("flush writer-%d", i)
				case 2: // time passes
					d := time.Duration(rng.Intn(int(2*grace))) + grace
					time.Sleep(d)
					op = fmt.Sprintf("wait %s", d.Round(time.Millisecond))
				case 3: // one insert pod compacts what the guard released; the reap loops retired the segments
					if err := e.s.RefreshManifest(context.Background()); err != nil {
						t.Fatal(err)
					}
					merged := e.compactReleased(commit, 2*grace, time.Now())
					merges += merged
					op = fmt.Sprintf("writer-%d compacts (%d objects merged)", i, merged)
				default:
					var merged int
					op, merged = slowDrain(i)
					merges += merged
					stalledMerges += merged
				}
				trail = append(trail, op)
				checkAll(fmt.Sprintf("seed %d, step %d (%s); trail: %s", seed, step, op, strings.Join(trail, " | ")))
				if t.Failed() {
					return
				}
			}
		})
	}
	if merges == 0 || stalledMerges == 0 {
		t.Fatalf("the schedules merged %d segment objects, %d of them while a drain stalled: the mixed layers were never exercised", merges, stalledMerges)
	}
	t.Logf("%d segment objects merged over the schedules, %d of them with a drain stalled", merges, stalledMerges)
}

// The two halves of the contract meet at every age of a committed segment, on
// the real clock: compaction merges nothing of it before twice the grace, the
// pod serves it until the grace and then retires it, and the rows are answered
// once at each of those ages and after the merge.
func TestReap_EveryAgeOfACommittedSegmentAnswersEachRowOnce(t *testing.T) {
	const grace = 600 * time.Millisecond
	e := newRestartEnvWith(t, newReapGate().mockS3Server)
	e.f.grace = grace
	e.ingest("A", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute))
	e.flush()
	e.ingest("B", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute))
	e.flush()
	committed := time.Now() // not before the markers' LastModified
	markers := e.segmentMarkers(committed)
	// The clock of the loop and of the guard is passed in, so the ages do not
	// depend on how fast the machine runs the steps.
	for _, age := range []time.Duration{grace / 5, grace / 2, 4 * grace / 5, 6 * grace / 5, 3 * grace / 2, 9 * grace / 5} {
		now := committed.Add(age)
		e.segs.Reap(now, grace) // what the flusher's loop does
		if n := e.compactReleased(markers, 2*grace, now); n != 0 {
			t.Fatalf("compaction merged %d objects of segments %s old (twice the grace is %s)", n, age, 2*grace)
		}
		if c := rowsOn(t, e.s, context.Background(), "*"); c != e.total {
			t.Errorf("segments %s old: query=* returned %d rows, want %d", age, c, e.total)
		}
	}
	now := committed.Add(2*grace + 20*time.Millisecond)
	e.segs.Reap(now, grace)
	if n := e.compactReleased(markers, 2*grace, now); n != 2 {
		t.Fatalf("compaction merged %d objects once the guard released them, want 2", n)
	}
	e.check("merged, segments retired")
}

// The other signal is unaffected: the bridge of this binary answers the other
// signal's query with no rows and no segments, so a fleet of this signal's pods
// cannot add or hide a row of the other.
func TestReap_TheOtherSignalIsUnaffected(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("A", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute))
	e.flush()
	other := string(config.ModeTraces)
	if e.bw.mode == config.ModeTraces {
		other = string(config.ModeLogs)
	}
	ans, err := BridgeSource{Segments: e.segs}.ReadBuffer(context.Background(), buffer.Selection{}, 0, 1<<62, other)
	if err != nil {
		t.Fatal(err)
	}
	if len(ans.Logs) != 0 || len(ans.Traces) != 0 {
		t.Errorf("a %s binary answered %d logs and %d traces to a %s query, want none", e.bw.mode, len(ans.Logs), len(ans.Traces), other)
	}
	e.check("the pod's own signal")
}

// reapCounter wraps the segments the flusher drives and counts the Reap calls.
// A Reap made by the reap loop takes a while and is counted while it runs, so a
// loop that outlives Stop is visible (the tick's own Reap is instant).
type reapCounter struct {
	*membuffer.Segments
	calls    atomic.Int64
	inFlight atomic.Int64 // Reap calls of the reap loop that are running
}

func (c *reapCounter) Reap(now time.Time, grace time.Duration) int {
	c.calls.Add(1)
	if onReapLoop() {
		c.inFlight.Add(1)
		defer c.inFlight.Add(-1)
		time.Sleep(60 * time.Millisecond)
	}
	return c.Segments.Reap(now, grace)
}

// onReapLoop reports whether the caller is the flusher's reap loop.
func onReapLoop() bool {
	var buf [4096]byte
	return bytes.Contains(buf[:runtime.Stack(buf[:], false)], []byte("reapLoop"))
}

// The flusher's loops end with Stop: no Reap of the reap loop is running when
// Stop returns and none starts afterwards (a loop that outlived the flusher
// would keep retiring segments of a buffer the process is closing).
func TestReap_StopEndsTheReapLoop(t *testing.T) {
	e := newRestartEnv(t)
	c := &reapCounter{Segments: e.segs}
	f := newBufferFlusher(e.bw, c, filepath.Join(e.dir, "buffer"), nil, BufferFlusherConfig{
		TargetBytes: 1 << 20, MaxAge: time.Hour, Grace: time.Minute})
	f.Start(5 * time.Millisecond)

	deadline := time.Now().Add(10 * time.Second)
	for c.inFlight.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.inFlight.Load() == 0 {
		t.Fatalf("the flusher made %d Reap calls in 10 s and none from its reap loop: segments are not retired on their own cadence", c.calls.Load())
	}
	f.Stop() // the loop is inside a Reap now: Stop has to wait for it
	if n := c.inFlight.Load(); n != 0 {
		t.Errorf("%d Reap calls of the reap loop are still running when Stop returned", n)
	}
	after := c.calls.Load()
	time.Sleep(200 * time.Millisecond) // 40 check intervals
	if got := c.calls.Load(); got != after {
		t.Errorf("Reap was called %d times after Stop returned: the loop outlived the flusher", got-after)
	}
}
