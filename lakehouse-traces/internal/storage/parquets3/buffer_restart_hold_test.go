package parquets3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// #379, review round 2: the segments a restart restores are held until a
// COMPLETE manifest refresh has been applied. A refresh that fails, that the
// cliff guard rejects or that could not list every tenant must not release
// them, and only the restored segments are held.

const holdGrace = time.Minute // restartEnv's flusher grace

// listFaultMock is the package's mock S3 with LIST fault injection and
// delimited LISTs (tenant discovery).
type listFaultMock struct {
	*mockS3Server
	fmu        sync.Mutex
	failAll    bool            // every LIST answers 403 (not retried)
	failPrefix map[string]bool // a delimited LIST under this prefix answers 403 (not retried)
}

func newListFaultMock() *listFaultMock {
	m := &listFaultMock{mockS3Server: &mockS3Server{files: map[string][]byte{}}, failPrefix: map[string]bool{}}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.Method == http.MethodGet && q.Get("list-type") == "2" {
			m.fmu.Lock()
			fail := m.failAll || (q.Get("delimiter") != "" && m.failPrefix[q.Get("prefix")])
			m.fmu.Unlock()
			if fail {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>x</Message></Error>`))
				return
			}
			if delim := q.Get("delimiter"); delim != "" {
				prefix := q.Get("prefix")
				seen := map[string]bool{}
				m.mu.RLock()
				for k := range m.files {
					if rest, ok := strings.CutPrefix(k, prefix); ok {
						if i := strings.Index(rest, delim); i >= 0 {
							seen[prefix+rest[:i+1]] = true
						}
					}
				}
				m.mu.RUnlock()
				cps := make([]string, 0, len(seen))
				for cp := range seen {
					cps = append(cps, cp)
				}
				sort.Strings(cps)
				var b strings.Builder
				b.WriteString(`<?xml version="1.0"?><ListBucketResult>`)
				for _, cp := range cps {
					fmt.Fprintf(&b, `<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>`, cp)
				}
				b.WriteString(`<IsTruncated>false</IsTruncated></ListBucketResult>`)
				w.Header().Set("Content-Type", "application/xml")
				_, _ = fmt.Fprint(w, b.String())
				return
			}
		}
		m.handler(w, r)
	}))
	return m
}

func (m *listFaultMock) setFailAll(v bool) {
	m.fmu.Lock()
	m.failAll = v
	m.fmu.Unlock()
}

func (m *listFaultMock) setFailPrefix(prefix string, v bool) {
	m.fmu.Lock()
	m.failPrefix[prefix] = v
	m.fmu.Unlock()
}

// held is how many committed segments the buffer holds for the first refresh.
func (e *restartEnv) held() int { return e.segs.Stats(time.Now()).Held }

// phantoms registers n objects the bucket does not have, in a window no check
// reads: what a snapshot holds of objects a peer's compaction has since merged
// away.
func (e *restartEnv) phantoms(m *manifest.Manifest, n int) []string {
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("traces/dt=2026-08-01/hour=00/phantom%d.parquet", i)
		m.AddFile("dt=2026-08-01/hour=00", manifest.FileInfo{Key: k, Size: 1})
		keys = append(keys, k)
	}
	return keys
}

// bucketListing is the bucket's Parquet objects, as a LIST returns them.
func (e *restartEnv) bucketListing() []manifest.ListedObject {
	var out []manifest.ListedObject
	e.mock.mu.RLock()
	for k, v := range e.mock.files {
		if strings.HasSuffix(k, ".parquet") {
			out = append(out, manifest.ListedObject{Key: k, Size: int64(len(v))})
		}
	}
	e.mock.mu.RUnlock()
	return out
}

// staleRestart builds the state after a kill -9: segment 1 ("X") was committed
// and is in the saved snapshot together with `extra` phantom objects; segment 2
// ("B") was committed after the snapshot. The pod restarts (clock: just past the
// grace of both) and loads the snapshot: B's objects are in the bucket, not in
// the manifest.
func staleRestart(t *testing.T, mock *mockS3Server, extra int) (*restartEnv, *manifest.Manifest) {
	t.Helper()
	e := newRestartEnvWith(t, mock)
	e.ingest("X", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute))
	e.flush()
	e.phantoms(e.s.manifest, extra)
	if err := e.s.manifest.SaveTo(e.snapshot); err != nil {
		t.Fatal(err)
	}
	e.ingest("B", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute))
	e.flush()
	commit := time.Now()
	e.segs.Close()
	m := manifest.New("test-bucket", "traces/")
	if err := m.LoadFrom(e.snapshot); err != nil {
		t.Fatal(err)
	}
	restartAt := commit.Add(holdGrace + time.Second)
	e.loadNow = func() time.Time { return restartAt }
	e.boot(m)
	if got := e.held(); got != 2 {
		t.Fatalf("held after the restart = %d, want 2 (both committed segments are restored)", got)
	}
	return e, m
}

// S2-a: the first refresh after a snapshot may find the bucket much smaller than
// the snapshot (a peer compacted most of it). It must be applied (a cold start
// has no such guard) and release the segment, not be rejected and release it on
// a stale manifest.
func TestBufferRestartHold_PeerCompactionShrinkedBucket_NothingLost(t *testing.T) {
	e, _ := staleRestart(t, newMockS3Server(), 5)
	e.check("restored, before the first refresh")
	e.refresh()
	if got := e.held(); got != 0 {
		t.Errorf("held after the first refresh = %d, want 0", got)
	}
	e.check("after the first refresh (the bucket lost 5 of 6 snapshot objects)")
	e.reap()
	e.check("after the segments are removed")
}

// A refresh the cliff guard rejects (not the first one) leaves the manifest
// stale: the held segments stay, and rows stay exact until a refresh is applied.
func TestBufferRestartHold_GuardRejectionKeepsTheHold(t *testing.T) {
	e, m := staleRestart(t, newMockS3Server(), 0)
	// One accepted listing makes the next one guarded; B's objects are in it.
	if !m.ApplyListing(e.bucketListing(), time.Now()) {
		t.Fatal("fixture: the first listing must be accepted")
	}
	keys := e.phantoms(m, 8) // now the manifest has 10 files, the bucket lists 2
	// The 8 are still in the bucket: the listing that omits them is incomplete,
	// which the HEAD sample of the dropped keys finds out.
	m.SetObjectProber(func(_ context.Context, _, key string) (bool, error) {
		for _, k := range keys {
			if k == key {
				return true, nil
			}
		}
		return false, nil
	})
	before := metrics.ManifestRefreshCliffGuardRejections.Get()
	for i := 0; i < 2; i++ {
		e.refresh() // RefreshManifest answers nil: serving is unchanged
		if got := metrics.ManifestRefreshCliffGuardRejections.Get() - before; got != uint64(i+1) {
			t.Fatalf("refresh %d: the guard rejected %d refreshes, want %d (fixture)", i+1, got, i+1)
		}
		if got := e.held(); got != 2 {
			t.Fatalf("refresh %d rejected by the cliff guard released the hold: held=%d, want 2", i+1, got)
		}
		e.check("rejected refresh: the segments still serve their rows")
	}
	// The objects are gone for real now (HEAD answers 404): the shrink is
	// confirmed, the refresh is applied and releases the hold, still exact.
	m.SetObjectProber(nil)
	e.refresh()
	if got := e.held(); got != 0 {
		t.Errorf("held after an accepted refresh = %d, want 0", got)
	}
	e.check("after an accepted refresh")
}

// A shrinking listing whose dropped keys cannot be HEAD-checked (the S3 HEAD
// fails) is not believed: the hold stays, and the first refresh whose sample is
// answered releases it.
func TestBufferRestartHold_HeadErrorKeepsTheHold(t *testing.T) {
	e, m := staleRestart(t, newMockS3Server(), 0)
	if !m.ApplyListing(e.bucketListing(), time.Now()) {
		t.Fatal("fixture: the first listing must be accepted")
	}
	e.phantoms(m, 8)
	m.SetObjectProber(func(context.Context, string, string) (bool, error) { return false, fmt.Errorf("503 SlowDown") })
	before := metrics.ManifestRefreshIncomplete.Get("head_unconfirmed")
	e.refresh()
	if got := metrics.ManifestRefreshIncomplete.Get("head_unconfirmed") - before; got != 1 {
		t.Fatalf("head_unconfirmed ticked %d times, want 1", got)
	}
	if got := e.held(); got != 2 {
		t.Fatalf("an unconfirmed shrink released the hold: held=%d, want 2", got)
	}
	e.check("unconfirmed shrink: the segments still serve their rows")
	m.SetObjectProber(nil) // HEAD works again: 404 for the phantoms
	e.refresh()
	if got := e.held(); got != 0 {
		t.Errorf("held after a confirmed shrink = %d, want 0", got)
	}
	e.check("after the confirmed shrink")
}

// A refresh that fails (LIST errors) keeps the hold, and the first one that
// succeeds releases it.
func TestBufferRestartHold_RefreshFailureKeepsTheHold(t *testing.T) {
	mock := newListFaultMock()
	e, _ := staleRestart(t, mock.mockS3Server, 0)
	mock.setFailAll(true)
	for i := 0; i < 2; i++ {
		if err := e.s.RefreshManifest(context.Background()); err == nil {
			t.Fatal("fixture: a refresh whose LIST fails must return the error")
		}
		if got := e.held(); got != 2 {
			t.Fatalf("a failed refresh released the hold: held=%d, want 2", got)
		}
		e.check("failed refresh: the segments still serve their rows")
	}
	mock.setFailAll(false)
	e.refresh()
	if got := e.held(); got != 0 {
		t.Errorf("held after a successful refresh = %d, want 0", got)
	}
	e.check("after a successful refresh")
}

// A refresh that could not list one tenant's projects is applied (serving
// carries on) but does not release the hold: that tenant's objects are missing
// from the manifest.
func TestBufferRestartHold_PartialListingKeepsTheHold(t *testing.T) {
	mock := newListFaultMock()
	e := newRestartEnvWith(t, mock.mockS3Server)
	e.ingest("A", at(rwHour, 5*time.Minute))
	e.flush()
	e.segs.Close()
	// The bucket as the two-level tenant layout has it; the writer's own
	// objects are not part of this layout.
	mock.mu.Lock()
	mock.files = map[string][]byte{
		"1/0/traces/dt=2026-09-30/hour=07/a.parquet": []byte("x"),
		"2/0/traces/dt=2026-09-30/hour=07/b.parquet": []byte("x"),
	}
	mock.mu.Unlock()
	m := manifest.New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	e.loadNow = func() time.Time { return time.Now().Add(10 * holdGrace) }
	e.boot(m)
	if e.held() != 1 {
		t.Fatalf("fixture: held=%d, want 1", e.held())
	}

	mock.setFailPrefix("2/", true)
	if err := e.s.RefreshManifest(context.Background()); err != nil {
		t.Fatalf("a partial refresh is not an error for serving: %v", err)
	}
	if got := e.s.manifest.TotalFiles(); got != 1 {
		t.Errorf("manifest holds %d files, want 1: the listable tenant is applied", got)
	}
	if got := e.held(); got != 1 {
		t.Fatalf("a partial refresh released the hold: held=%d, want 1", got)
	}

	mock.setFailPrefix("2/", false)
	e.refresh()
	if got := e.s.manifest.TotalFiles(); got != 2 {
		t.Errorf("manifest holds %d files after a full listing, want 2", got)
	}
	if got := e.held(); got != 0 {
		t.Errorf("held after a complete refresh = %d, want 0", got)
	}
}

// S2-b: only the restored segments wait for the refresh. Segments committed
// after the restart are retired by the normal grace even while no refresh has
// succeeded, so a failing refresh does not fill the buffer disk.
func TestBufferRestartHold_OnlyRestoredSegmentsAreHeld(t *testing.T) {
	e := newRestartEnv(t)
	e.ingest("A", at(rwHour, 5*time.Minute))
	e.flush()
	commit := time.Now()
	e.segs.Close()
	e.loadNow = func() time.Time { return commit.Add(holdGrace + time.Second) }
	e.boot(manifest.New("test-bucket", "traces/"))

	// Five new segments after the restart, committed, with no refresh yet.
	for i := 0; i < 5; i++ {
		e.ingest("B", at(rwHour, time.Duration(20+i)*time.Minute))
		e.flush()
	}
	// An hour later on the pod's clock, with no refresh ever having succeeded.
	e.loadNow = func() time.Time { return time.Now().Add(time.Hour) }
	e.f.holdWarnAfter = time.Nanosecond
	e.f.tick(context.Background(), time.Now())

	st := e.segs.Stats(time.Now())
	if st.Committed != 1 || st.Held != 1 {
		t.Fatalf("after a tick an hour on with no refresh: committed=%d held=%d, want 1 and 1 (only the restored segment waits)", st.Committed, st.Held)
	}
	if got := metrics.BufferHeldSegments.Get(); got != 1 {
		t.Errorf("lakehouse_buffer_held_segments = %d, want 1", got)
	}
	if e.f.lastHoldWarn.IsZero() {
		t.Error("no warning was logged for a segment held past holdWarnAfter")
	}
	e.check("one restored segment held, the rest removed")

	e.refresh()
	if got := e.held(); got != 0 {
		t.Errorf("held after the refresh = %d, want 0", got)
	}
	e.f.tick(context.Background(), time.Now())
	if got, age := metrics.BufferHeldSegments.Get(), metrics.BufferOldestHeldAge.Get(); got != 0 || age != 0 {
		t.Errorf("after release: held=%d oldest_age=%d, want 0 and 0", got, age)
	}
	e.check("after the refresh")
}

// S3-a: every read path takes its buffer view BEFORE it lists the objects. A
// refresh that publishes a restored segment's objects and retires the segment
// between the two must neither hide its rows (view taken after the retirement,
// list taken before the publish) nor double them.
func TestBufferRestartHold_ViewIsTakenBeforeTheObjectList(t *testing.T) {
	from, to := rwWindow()
	for _, tc := range []struct {
		name string
		rows func(e *restartEnv) int
	}{
		{"query", func(e *restartEnv) int { return e.run(context.Background(), "*", from, to) }},
		{"field_values", func(e *restartEnv) int {
			n := 0
			for _, h := range e.levelHits(from, to) {
				n += int(h)
			}
			return n
		}},
		{"streams", func(e *restartEnv) int {
			got, err := e.s.GetStreams(context.Background(), nil, e.window("*", from, to), 0)
			if err != nil {
				e.t.Fatal(err)
			}
			n := 0
			for _, v := range got {
				n += int(v.Hits)
			}
			return n
		}},
		{"stream_ids", func(e *restartEnv) int {
			got, err := e.s.GetStreamIDs(context.Background(), nil, e.window("*", from, to), 0)
			if err != nil {
				e.t.Fatal(err)
			}
			n := 0
			for _, v := range got {
				n += int(v.Hits)
			}
			return n
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := staleRestart(t, newMockS3Server(), 0)
			want := e.total
			var once sync.Once
			testHookBetweenViewAndList = func() { once.Do(e.refresh) }
			t.Cleanup(func() { testHookBetweenViewAndList = nil })
			if got := tc.rows(e); got != want {
				t.Errorf("%s with a refresh and a reap between the view and the list: %d rows, want %d", tc.name, got, want)
			}
			if e.held() != 0 {
				t.Errorf("fixture: the hook's refresh did not release the segments (held=%d)", e.held())
			}
		})
	}
}

// Stress: queries running while the first refresh publishes and releases the
// restored segments always see exactly the rows that were acknowledged: none
// lost, none doubled.
func TestBufferRestartHold_ConcurrentQueriesNeverLoseRows(t *testing.T) {
	e, _ := staleRestart(t, newMockS3Server(), 0)
	want := e.total
	from, to := rwWindow()
	workers, rounds := 4, 12
	if testing.Short() {
		workers, rounds = 2, 6
	}
	var low atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range workers {
		qs := make([]*logstorage.Query, rounds)
		for i := range qs {
			qs[i] = e.window("*", from, to)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for _, q := range qs {
				total := 0
				var mu sync.Mutex
				err := e.s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
					mu.Lock()
					total += db.RowsCount()
					mu.Unlock()
				})
				if err != nil {
					t.Errorf("RunQuery: %v", err)
					return
				}
				if total != want {
					low.Add(1)
					t.Errorf("a query concurrent with the refresh returned %d rows, want exactly the %d acknowledged (fewer = lost, more = duplicated)", total, want)
				}
			}
		}()
	}
	close(start)
	if err := e.s.RefreshManifest(context.Background()); err != nil {
		t.Errorf("refresh: %v", err)
	}
	e.f.tick(context.Background(), time.Now())
	wg.Wait()
	if low.Load() > 0 {
		t.Fatalf("%d queries returned a row count other than the acknowledged one", low.Load())
	}
	e.check("after the refresh")
}
