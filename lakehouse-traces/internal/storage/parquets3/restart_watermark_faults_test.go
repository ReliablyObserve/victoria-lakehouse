package parquets3

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// ---------------------------------------------------------------------------
// The exact-bounds resolution under faults and cost limits (#272).
//
// Invariants:
//   - never double count: an object whose bounds cannot be resolved keeps its
//     inferred end in the watermark (rows hide until it resolves);
//   - no metadata-only answer uses an object whose bounds are still inferred;
//   - resolving reads one ranged tail per object at most, never a whole object,
//     only for objects that can change the watermark, bounded in time, and a
//     failing object is not read again on every query.
//
// Twin of internal/storage/parquets3/restart_watermark_faults_test.go (the logs module).
// ---------------------------------------------------------------------------

// rwMock is the package's mock S3 with fault injection and per-key accounting
// of object GETs (parquet keys only): a status switch answers 503 (a transient
// failure) or 404 (an object a peer's compaction retired after this pod listed
// it); full GETs have no Range header, ranged ones do.
type rwMock struct {
	*mockS3Server
	mu          sync.Mutex
	status      map[string]int
	full        map[string]int
	ranged      map[string]int
	rangedBytes int64
	delays      map[string]time.Duration
}

func newRWMock() *rwMock {
	m := &rwMock{mockS3Server: &mockS3Server{files: map[string][]byte{}}, status: map[string]int{}, full: map[string]int{}, ranged: map[string]int{}}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "" {
			if parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2); len(parts) == 2 && strings.HasSuffix(parts[1], ".parquet") {
				key := parts[1]
				m.mu.Lock()
				if rng := r.Header.Get("Range"); rng != "" {
					m.ranged[key]++
					if b := strings.SplitN(strings.TrimPrefix(rng, "bytes="), "-", 2); len(b) == 2 {
						lo, _ := strconv.ParseInt(b[0], 10, 64)
						hi, _ := strconv.ParseInt(b[1], 10, 64)
						m.rangedBytes += hi - lo + 1
					}
				} else {
					m.full[key]++
				}
				st := m.status[key]
				d := m.delays[key]
				m.mu.Unlock()
				if d > 0 {
					select {
					case <-time.After(d):
					case <-r.Context().Done():
						return
					}
				}
				if st != 0 {
					w.WriteHeader(st)
					if st == http.StatusNotFound {
						_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>gone</Message></Error>`))
					} else {
						_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>SlowDown</Code><Message>x</Message></Error>`))
					}
					return
				}
			}
		}
		m.handler(w, r)
	}))
	return m
}

func (m *rwMock) fail(key string, status int) {
	m.mu.Lock()
	m.status[key] = status
	m.mu.Unlock()
}

// gets returns the accounted GETs: whole-object, ranged, and ranged bytes.
func (m *rwMock) gets() (full, ranged int, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range m.full {
		full += n
	}
	for _, n := range m.ranged {
		ranged += n
	}
	return full, ranged, m.rangedBytes
}

func (m *rwMock) keyGets(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.full[key] + m.ranged[key]
}

func (m *rwMock) reset() {
	m.mu.Lock()
	m.full, m.ranged, m.rangedBytes = map[string]int{}, map[string]int{}, 0
	m.mu.Unlock()
}

func newRWRigWith(t *testing.T, mock *mockS3Server) *rwRig {
	t.Helper()
	t.Cleanup(mock.close)
	rwFreezeClock(t)
	r := &rwRig{t: t, mock: mock, buf: &rwBuffer{}, snapshot: filepath.Join(t.TempDir(), "manifest.snapshot")}
	r.s, r.bw = r.boot(manifest.New("test-bucket", "logs/"))
	return r
}

// statsRows runs `* | stats count() n` through the storage select path with the
// timestamp-only hint (the path a real count takes through manifestFastPath)
// and returns the rows the storage emitted: the count the user gets.
func (r *rwRig) statsRows(from, to time.Time) int {
	r.t.Helper()
	total := 0
	var mu sync.Mutex
	q := r.window("* | stats count() n", from, to)
	if err := r.s.RunQuery(storage.WithTimestampOnlyHint(context.Background()), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		total += db.RowsCount()
		mu.Unlock()
	}); err != nil {
		r.t.Fatalf("RunQuery: %v", err)
	}
	return total
}

func (r *rwRig) resetBackoff() {
	r.s.inferredBounds.mu.Lock()
	r.s.inferredBounds.retry = nil
	r.s.inferredBounds.pausedUntil = time.Time{}
	r.s.inferredBounds.mu.Unlock()
}

func (r *rwRig) objects() []manifest.FileInfo {
	return r.s.manifest.GetFilesForRange(0, 1<<62)
}

func rwWindow() (time.Time, time.Time) { return rwHour.Add(-time.Hour), rwHour.Add(2 * time.Hour) }

// F1: an inferred object that cannot be resolved must never be counted twice -
// not by `query=*`, and not by a real `stats count()` that goes through the
// manifest fast path with the row count a footer-only warmup left on the entry.
// At worst the rows hide until the object is readable again.
func TestResolveFaults_UnresolvedObjectNeverDoubleCounts(t *testing.T) {
	for _, st := range []int{http.StatusServiceUnavailable, http.StatusNotFound} {
		t.Run(http.StatusText(st), func(t *testing.T) {
			singleAttemptS3(t)
			mock := newRWMock()
			r := newRWRigWith(t, mock.mockS3Server)
			r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 10*time.Minute+time.Second), at(rwHour, 10*time.Minute+2*time.Second))
			r.restart(true, false) // the object is learned by listing: inferred bounds
			files := r.objects()
			if len(files) != 1 || !files[0].BoundsInferred {
				t.Fatalf("precondition: %+v", files)
			}
			// What the footer prefetch of the warmup leaves: rows, no bounds.
			r.s.manifest.EnrichFileMetadata(files[0].Key, 3, 0, 0)
			mock.fail(files[0].Key, st)
			from, to := rwWindow()

			unresolved0 := metrics.WatermarkInferredUnresolved.Get()
			if got := r.statsRows(from, to); got > 3 {
				t.Errorf("stats count() = %d with the object answering %d, want at most 3: the restored buffer copies of its rows were counted again", got, st)
			}
			if got := r.rows(context.Background(), from, to); got > 3 {
				t.Errorf("query=* emitted %d rows with the object answering %d, want at most 3", got, st)
			}
			if metrics.WatermarkInferredUnresolved.Get() == unresolved0 {
				t.Error("lakehouse_watermark_inferred_unresolved_total did not tick for the unresolved object")
			}

			// The object becomes readable again: everything is visible exactly once.
			delete(mock.status, files[0].Key)
			r.resetBackoff()
			r.bufferOnly("NEW", at(rwHour, 30*time.Minute), at(rwHour, 31*time.Minute), at(rwHour, 32*time.Minute))
			if got := r.statsRows(from, to); got != 6 {
				t.Errorf("stats count() after recovery = %d, want 6 (3 cold + 3 buffered after the restart)", got)
			}
			if got := r.rows(context.Background(), from, to); got != 6 {
				t.Errorf("query=* after recovery = %d, want 6", got)
			}
		})
	}
}

// No metadata-only answer may use an object whose bounds are still inferred.
func TestResolveFaults_InferredObjectIsNotAnsweredFromMetadata(t *testing.T) {
	inferred := manifest.FileInfo{Key: "k", RowCount: 3, MinTimeNs: rwHour.UnixNano(), MaxTimeNs: rwHour.Add(time.Hour).UnixNano() - 1, BoundsInferred: true}
	exact := inferred
	exact.BoundsInferred = false
	from, to := rwHour.Add(-time.Hour).UnixNano(), rwHour.Add(2*time.Hour).UnixNano()
	if fileFullyInRange(inferred, from, to) {
		t.Error("fileFullyInRange accepted an object with inferred bounds")
	}
	if !fileFullyInRange(exact, from, to) {
		t.Error("fileFullyInRange rejected the same object with exact bounds")
	}
	if fileWithinWindow(inferred, from, to) {
		t.Error("fileWithinWindow accepted an object with inferred bounds")
	}
	if !fileWithinWindow(exact, from, to) {
		t.Error("fileWithinWindow rejected the same object with exact bounds")
	}
	// handle404Recovery must not serve the retired object from its row count.
	s := &Storage{}
	ctx := withMetadataOnlyPlan(storage.WithTimestampOnlyHint(context.Background()), planMetadataOnly(mustParseQuery(t, "* | stats count() n")))
	emitted := 0
	s.handle404Recovery(ctx, inferred, nil, false, func(uint, *logstorage.DataBlock) { emitted++ })
	if emitted != 0 {
		t.Errorf("handle404Recovery served %d blocks for an object with inferred bounds", emitted)
	}
}

// The count pushdown (label aggregates) must not answer for an inferred object
// either, and does for the same object once its bounds are exact.
func TestResolveFaults_CountPushdownSkipsInferredObjects(t *testing.T) {
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	mk := func(inferred bool) manifest.FileInfo {
		return manifest.FileInfo{
			Key: "k", RowCount: 3, MinTimeNs: rwHour.UnixNano(), MaxTimeNs: rwHour.Add(time.Hour).UnixNano() - 1, BoundsInferred: inferred,
			LabelAggregates: map[string]map[string]int64{"service.name": {"svc": 3}},
		}
	}
	from, to := rwHour.Add(-time.Hour).UnixNano(), rwHour.Add(2*time.Hour).UnixNano()
	emitted := 0
	write := func(_ uint, db *logstorage.DataBlock) { emitted += db.RowsCount() }
	if rem := s.manifestCountFastPath([]manifest.FileInfo{mk(true)}, from, to, "service.name", write); len(rem) != 1 || emitted != 0 {
		t.Errorf("inferred object: remaining=%d emitted=%d, want it left for the scan (1, 0)", len(rem), emitted)
	}
	if rem := s.manifestCountFastPath([]manifest.FileInfo{mk(false)}, from, to, "service.name", write); len(rem) != 0 || emitted != 3 {
		t.Errorf("exact object: remaining=%d emitted=%d, want it answered from metadata (0, 3)", len(rem), emitted)
	}
}

// F2: bounded time, and a failing object is not re-read on every query.
func TestResolveFaults_FailingObjectIsBoundedAndBackedOff(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute))
	r.restart(true, false)
	files := r.objects()
	key := files[0].Key
	mock.fail(key, http.StatusServiceUnavailable)

	start := time.Now()
	wm := r.s.bufferWatermarksFor(context.Background(), 0, files)
	elapsed := time.Since(start)
	if limit := boundsResolveTimeout + 2*time.Second; elapsed > limit {
		t.Errorf("a failing object held the watermark computation for %v, want at most the %v budget (+slack)", elapsed, boundsResolveTimeout)
	}
	if want := rwHour.Add(time.Hour).UnixNano() - 1; wm[logstorage.TenantID{}] != want {
		t.Errorf("watermark = %d, want the conservative inferred end %d", wm[logstorage.TenantID{}], want)
	}
	if mock.keyGets(key) == 0 {
		t.Fatal("precondition: the first computation never tried the object")
	}

	mock.reset()
	start = time.Now()
	_ = r.s.bufferWatermarksFor(context.Background(), 0, r.objects())
	if n := mock.keyGets(key); n != 0 {
		t.Errorf("the second computation read the failing object %d times, want 0 (back-off)", n)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("the backed-off computation took %v, want ~0", d)
	}

	r.resetBackoff()
	delete(mock.status, key)
	wm = r.s.bufferWatermarksFor(context.Background(), 0, r.objects())
	if want := at(rwHour, 11*time.Minute).UnixNano(); wm[logstorage.TenantID{}] != want {
		t.Errorf("after the back-off expired and the object recovered: watermark %d, want exact %d", wm[logstorage.TenantID{}], want)
	}
}

// F2: only objects that can change the watermark are read, each by at most one
// ranged read (never a whole object), and nothing again afterwards.
func TestResolveFaults_OnlyCandidatesAreReadByRangeOnce(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	const K = 40
	for i := 0; i < K; i++ {
		r.ingest("COLD", at(rwHour.Add(-time.Duration(i)*time.Hour), 10*time.Minute))
	}
	r.restart(true, false)
	files := r.objects()
	if len(files) != K {
		t.Fatalf("manifest holds %d objects, want %d", len(files), K)
	}
	// The buffer holds the last hour: the floor is rwHour-30m, so only the two
	// newest hours (inferred ends rwHour+1h and rwHour) can change the watermark.
	nowFn = func() time.Time { return rwHour.Add(30 * time.Minute) }
	startNs := rwHour.Add(-time.Duration(K+1) * time.Hour).UnixNano()

	mock.reset()
	wm := r.s.bufferWatermarksFor(context.Background(), startNs, files)
	full, ranged, bytes := mock.gets()
	if full != 0 {
		t.Errorf("%d whole-object GETs resolving %d listing-only objects, want 0", full, K)
	}
	if ranged != 2 {
		t.Errorf("%d ranged GETs, want 2 (only the objects that can change the watermark)", ranged)
	}
	if limit := int64(2 * 128 << 10); bytes > limit {
		t.Errorf("%d bytes read, want at most %d (the footer tail per object)", bytes, limit)
	}
	if want := at(rwHour, 10*time.Minute).UnixNano(); wm[logstorage.TenantID{}] != want {
		t.Errorf("watermark = %d, want the newest exact row %d", wm[logstorage.TenantID{}], want)
	}

	mock.reset()
	_ = r.s.bufferWatermarksFor(context.Background(), startNs, r.objects())
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("a second computation read %d+%d times, want 0 (bounds are now exact)", full, ranged)
	}
}

// An inferred object at or below the tenant's exact watermark cannot change it
// and is never read.
func TestResolveFaults_ObjectBelowTheExactWatermarkIsNotRead(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour.Add(-time.Hour), 10*time.Minute))
	r.restart(true, false)
	files := r.objects()
	newer := manifest.FileInfo{Key: "0/0/logs/dt=2026-10-01/hour=09/exact.parquet", Size: 1, RowCount: 1, MinTimeNs: rwHour.Add(2 * time.Hour).UnixNano(), MaxTimeNs: rwHour.Add(2*time.Hour + time.Minute).UnixNano()}
	files = append(files, newer)
	mock.reset()
	wm := r.s.bufferWatermarksFor(context.Background(), 0, files)
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("read %d objects whose inferred end is below the tenant's exact watermark, want 0", full+ranged)
	}
	if wm[logstorage.TenantID{}] != newer.MaxTimeNs {
		t.Errorf("watermark = %d, want %d", wm[logstorage.TenantID{}], newer.MaxTimeNs)
	}
}

// One footer read per object however many queries ask at once.
func TestResolveFaults_ConcurrentQueriesShareOneRead(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute))
	r.restart(true, false)
	files := r.objects()
	mock.reset()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.s.bufferWatermarksFor(context.Background(), 0, files)
		}()
	}
	wg.Wait()
	if full, ranged, _ := mock.gets(); full != 0 || ranged != 1 {
		t.Errorf("32 concurrent computations made %d whole and %d ranged GETs, want 0 and 1", full, ranged)
	}
}

// A cancelled request neither reads nor penalises the object.
func TestResolveFaults_CancelledContextReadsNothing(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	files := r.objects()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mock.reset()
	wm := r.s.bufferWatermarksFor(ctx, 0, files)
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("a cancelled context still made %d GETs", full+ranged)
	}
	if want := rwHour.Add(time.Hour).UnixNano() - 1; wm[logstorage.TenantID{}] != want {
		t.Errorf("watermark = %d, want the conservative inferred end %d", wm[logstorage.TenantID{}], want)
	}
	if r.s.inferredBounds.backedOff(files[0].Key, time.Now()) {
		t.Error("a cancelled request put the object into back-off")
	}
	wm = r.s.bufferWatermarksFor(context.Background(), 0, files)
	if want := at(rwHour, 10*time.Minute).UnixNano(); wm[logstorage.TenantID{}] != want {
		t.Errorf("the next request: watermark %d, want exact %d", wm[logstorage.TenantID{}], want)
	}
}

// The pmeta facet is the first source: exact bounds from RAM, no S3 read.
func TestResolveFaults_FacetResolvesWithoutAnyRead(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 12*time.Minute))
	r.restart(true, false)
	r.s.cfg.Pmeta.Enabled = true
	r.s.catalog = newCatalogStore(r.s.cfg.Pmeta, "logs/")
	files := r.objects()
	exMin, exMax := at(rwHour, 10*time.Minute).UnixNano(), at(rwHour, 12*time.Minute).UnixNano()
	r.s.catalog.OnFileFlush(pmeta.FileContribution{Partition: manifest.ExtractTenantPartition(files[0].Key), FileKey: files[0].Key, RowCount: 2, MinTimeNs: exMin, MaxTimeNs: exMax})
	mock.reset()
	wm := r.s.bufferWatermarksFor(context.Background(), 0, files)
	if wm[logstorage.TenantID{}] != exMax {
		t.Errorf("watermark = %d, want the facet's %d", wm[logstorage.TenantID{}], exMax)
	}
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("resolving from the facet made %d GETs, want 0", full+ranged)
	}
}

// The startup pass resolves recent objects by ranged footer reads alone, also
// where the footer-only prefetch of the warmup cannot supply bounds.
func TestResolveFaults_WarmMetadataResolvesRecentObjectsByRangeOnly(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 12*time.Minute))
	r.restart(true, false)
	r.s.footerCache = nil // an insert-only pod: no footer prefetch phases, only Phase 3c
	nowFn = func() time.Time { return rwHour.Add(30 * time.Minute) }
	if f := r.objects(); len(f) != 1 || !f[0].BoundsInferred {
		t.Fatalf("precondition: %+v", f)
	}
	mock.reset()
	r.s.WarmMetadata(context.Background())
	f := r.objects()
	if f[0].BoundsInferred || f[0].MaxTimeNs != at(rwHour, 12*time.Minute).UnixNano() {
		t.Errorf("after WarmMetadata: %+v, want exact bounds", f[0])
	}
	if full, ranged, _ := mock.gets(); full != 0 || ranged != 1 {
		t.Errorf("WarmMetadata made %d whole and %d ranged object GETs, want 0 and 1", full, ranged)
	}
}

// Phase 3c is bounded to the recent window: older objects wait for the lazy path.
func TestResolveFaults_StartupPassIsBoundedToTheRecentWindow(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour.Add(-10*time.Hour), 10*time.Minute))
	r.restart(true, false)
	nowFn = func() time.Time { return rwHour.Add(30 * time.Minute) }
	mock.reset()
	if n := r.s.enrichRecentInferredBounds(context.Background()); n != 1 {
		t.Errorf("enrichRecentInferredBounds resolved %d objects, want 1 (the old one is outside the window)", n)
	}
	if _, ranged, _ := mock.gets(); ranged != 1 {
		t.Errorf("%d ranged GETs, want 1", ranged)
	}
}

// The watermark is computed only when the buffer is consulted.
type countingSource struct{ n int }

func (c *countingSource) watermarks(context.Context) bufferWatermarks { c.n++; return nil }

func TestResolveFaults_WatermarksAreComputedOnlyWhenTheBufferIsConsulted(t *testing.T) {
	from, to := rwWindow()
	run := func(s *Storage, queryStr string) int {
		src := &countingSource{}
		s.queryBufferBridgeTo(context.Background(), from.UnixNano(), to.UnixNano(), src, mustParseQueryWithTime(t, queryStr, from.UnixNano(), to.UnixNano()), nil, uniformSink(func(uint, *logstorage.DataBlock) {}))
		return src.n
	}
	withBuffer := &Storage{localBuffer: &rwBuffer{}, cfg: testConfig()}
	if got := run(withBuffer, "*"); got != 1 {
		t.Errorf("a query that consults the buffer computed the watermark %d times, want 1", got)
	}
	if got := run(&Storage{cfg: testConfig()}, "*"); got != 0 {
		t.Errorf("a pod with no buffer computed the watermark %d times, want 0", got)
	}
	if got := run(withBuffer, `trace_id:="abc"`); got != 0 {
		t.Errorf("a trace_id lookup (which ignores the watermark) computed it %d times, want 0", got)
	}
}

// A footer-statistics read gives the same bounds as the rows written.
func TestResolveFaults_FooterStatisticsMatchTheWrittenRows(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	lo, hi := at(rwHour, 10*time.Minute), at(rwHour, 40*time.Minute)
	r.ingest("COLD", at(rwHour, 20*time.Minute), lo, hi, at(rwHour, 30*time.Minute))
	r.restart(true, false)
	fi := r.objects()[0]
	rows, minNs, maxNs, err := r.s.readFooterTimeBounds(context.Background(), fi)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 4 || minNs != lo.UnixNano() || maxNs != hi.UnixNano() {
		t.Errorf("footer bounds = rows %d [%d, %d], want 4 rows [%d, %d]", rows, minNs, maxNs, lo.UnixNano(), hi.UnixNano())
	}
}

// Inferred bounds are never exported to the persisted file-metadata cache or to
// the pmeta facet, whichever way the facet is filled.
func TestResolveFaults_InferredBoundsAreNotExported(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 12*time.Minute))
	r.restart(true, false)
	key := r.objects()[0].Key
	r.s.manifest.EnrichFileMetadata(key, 2, 0, 0) // rows known, bounds still inferred
	if fi, _ := r.s.manifest.GetFileByKey(key); !fi.BoundsInferred || fi.RowCount != 2 {
		t.Fatalf("precondition: %+v", fi)
	}

	t.Run("file-metadata cache", func(t *testing.T) {
		p, err := cache.NewPersister(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		r.s.persister = p
		r.s.saveFileMetadataToDisk()
		got, err := p.LoadFileMetadata()
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Entries) != 1 || got.Entries[0].MinTimeNs != 0 || got.Entries[0].MaxTimeNs != 0 {
			t.Errorf("persisted entries = %+v, want one entry without time bounds", got.Entries)
		}
	})

	facetBounds := func(t *testing.T) (int64, int64, bool) {
		v, ok := r.s.catalog.FileMeta(manifest.ExtractTenantPartition(key), key)
		return v.MinTimeNs, v.MaxTimeNs, ok
	}
	r.s.cfg.Pmeta.Enabled = true
	t.Run("catalog replay from the manifest", func(t *testing.T) {
		r.s.catalog = newCatalogStore(r.s.cfg.Pmeta, "logs/")
		r.s.WarmCatalog(context.Background())
		mn, mx, ok := facetBounds(t)
		if !ok || mn != 0 || mx != 0 {
			t.Errorf("facet entry after WarmCatalog = [%d, %d] ok=%v, want present without bounds", mn, mx, ok)
		}
	})
	t.Run("catalog rebuild after a lost bundle", func(t *testing.T) {
		r.s.catalog = newCatalogStore(r.s.cfg.Pmeta, "logs/")
		r.s.WarmCatalogFromS3(context.Background())
		mn, mx, ok := facetBounds(t)
		if !ok || mn != 0 || mx != 0 {
			t.Errorf("facet entry after WarmCatalogFromS3 = [%d, %d] ok=%v, want present without bounds", mn, mx, ok)
		}
	})
}

// A snapshot written before bounds were marked holds the inferred hour as if it
// were exact; it must not hide the buffer.
func TestResolveFaults_LegacyHourWideSnapshotEntryIsInferred(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 10*time.Minute+time.Second), at(rwHour, 10*time.Minute+2*time.Second))
	r.restart(true, false)
	legacy := r.objects()[0]
	legacy.BoundsInferred = false // what v0.143.7 wrote
	legacy.MinTimeNs = rwHour.UnixNano()
	legacy.MaxTimeNs = rwHour.Add(time.Hour).UnixNano() - 1
	old := manifest.New("test-bucket", "logs/")
	old.AddFile("dt=2026-10-01/hour=07", legacy)
	if err := old.SaveTo(r.snapshot); err != nil {
		t.Fatal(err)
	}
	m := manifest.New("test-bucket", "logs/")
	if err := m.LoadFrom(r.snapshot); err != nil {
		t.Fatal(err)
	}
	s, bw := r.boot(m)
	r.s, r.bw = s, bw
	r.bufferOnly("NEW", at(rwHour, 30*time.Minute), at(rwHour, 31*time.Minute), at(rwHour, 32*time.Minute))
	from, to := rwWindow()
	if got := r.rows(context.Background(), from, to); got != 6 {
		t.Errorf("query=* emitted %d rows, want 6: an unflagged hour-wide entry from an older snapshot still hides the buffer", got)
	}
}

var tenantZero = logstorage.TenantID{}

// delay makes GETs of key wait before answering.
func (m *rwMock) delay(key string, d time.Duration) {
	m.mu.Lock()
	if m.delays == nil {
		m.delays = map[string]time.Duration{}
	}
	m.delays[key] = d
	m.mu.Unlock()
}

type rwBytes struct{ b []byte }

func (w *rwBytes) Write(p []byte) (int, error) { w.b = append(w.b, p...); return len(p), nil }
func (w *rwBytes) Len() int                    { return len(w.b) }
func (w *rwBytes) reader() *bytes.Reader       { return bytes.NewReader(w.b) }

func rwPeerRows(ts []time.Time) []schema.TraceRow {
	rows := make([]schema.TraceRow, len(ts))
	for i, t := range ts {
		rows[i] = rwSpan(t, "COLD")
	}
	return rows
}
