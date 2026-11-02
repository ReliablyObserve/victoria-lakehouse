package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
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

// reapGate is a bucket that deletes (the plain mock answers a DELETE like a GET,
// so compaction's inputs would stay listed) and whose PUTs of the keys containing
// `match` wait until the gate is opened: an object store that is slow for one
// segment.
type reapGate struct {
	*mockS3Server
	mu      sync.Mutex
	match   string
	hit     chan struct{}
	release chan struct{}
	once    sync.Once
}

func newReapGate() *reapGate {
	g := &reapGate{mockS3Server: newMockS3Server(), hit: make(chan struct{}, 1), release: make(chan struct{})}
	g.mockS3Server.srv.Close()
	g.mockS3Server.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		match := g.match
		g.mu.Unlock()
		if r.Method == http.MethodDelete {
			g.mockS3Server.mu.Lock()
			delete(g.mockS3Server.files, strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)[1])
			g.mockS3Server.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method == http.MethodPut && match != "" && strings.Contains(r.URL.Path, match) {
			select {
			case g.hit <- struct{}{}:
			default:
			}
			<-g.release
		}
		g.mockS3Server.handler(w, r)
	}))
	return g
}

func (g *reapGate) block(match string) {
	g.mu.Lock()
	g.match = match
	g.mu.Unlock()
}

func (g *reapGate) open() { g.once.Do(func() { close(g.release) }) }

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
// segment served after compaction merged its objects.
func TestReap_ASlowDrainDoesNotKeepAGuardReleasedSegmentServed(t *testing.T) {
	const grace = 200 * time.Millisecond
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

// The same contract over a fleet: two insert pods and two select pods over one
// bucket, every pod reading the unflushed rows of both insert pods over the
// buffer bridge. Random schedules of ingest, flush, time passing and compaction
// (by either insert pod, with the real guard) overlap rows of different
// segments in the same hour. After every step every pod answers every row once
// on every counting surface, whatever mix of buffer-only, flushed and compacted
// rows exists (a writer and a select pod agree).
func TestReap_FleetAnswersEveryRowOnce_RandomSchedules(t *testing.T) {
	const grace = 60 * time.Millisecond
	merges := 0
	for seed := int64(1); seed <= 5; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			bucket := newReapGate()
			w := []*restartEnv{newRestartEnvWith(t, bucket.mockS3Server), newRestartEnvWith(t, bucket.mockS3Server)}
			for _, e := range w {
				e.f.grace = grace
			}
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
			var trail []string
			for step := 0; step < 24; step++ {
				i := rng.Intn(2)
				e := w[i]
				var op string
				switch rng.Intn(4) {
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
					e.flush()
					for _, f := range e.objects() {
						if n := manifest.SegmentNonceOfKey(f.Key); n != "" {
							if _, seen := commit[n]; !seen {
								commit[n] = time.Now()
							}
						}
					}
					op = fmt.Sprintf("flush writer-%d", i)
				case 2: // time passes
					d := time.Duration(rng.Intn(int(2*grace))) + grace
					time.Sleep(d)
					op = fmt.Sprintf("wait %s", d.Round(time.Millisecond))
				default: // both insert pods retire what is past its grace, then one compacts
					now := time.Now()
					for _, p := range w {
						p.segs.Reap(now, grace)
					}
					if err := e.s.RefreshManifest(context.Background()); err != nil {
						t.Fatal(err)
					}
					merged := e.compactReleased(commit, 2*grace, now)
					merges += merged
					op = fmt.Sprintf("reap, then writer-%d compacts (%d objects merged)", i, merged)
				}
				trail = append(trail, op)
				checkAll(fmt.Sprintf("seed %d, step %d (%s); trail: %s", seed, step, op, strings.Join(trail, " | ")))
				if t.Failed() {
					return
				}
			}
		})
	}
	if merges == 0 {
		t.Fatal("no schedule compacted anything: the mixed layers were never exercised")
	}
	t.Logf("%d segment objects merged over the schedules", merges)
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
