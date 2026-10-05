package parquets3

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
)

// ---------------------------------------------------------------------------
// The exact-bounds resolution (bounds_resolve.go) under faults and cost limits.
//
// An object the manifest learned from the bucket listing has an inferred time
// range (the partition hour). The startup pass and resolveFileBounds replace it
// with the exact one from the pmeta facet or from ONE ranged read of the
// footer.
//
// Invariants:
//   - an inferred range is never taken for an exact one (a footer that says
//     exactly the partition hour is not evidence; a row group without
//     statistics, or a partial page index, gives no range);
//   - resolving reads one ranged tail per object at most, never a whole object,
//     only for recent objects, and a failing object is not read again on every
//     attempt (back-off, doubling, capped, forgotten on success or removal);
//   - concurrent callers share one read, a cancelled caller neither reads nor
//     penalises the object, removals racing a read leave no stale state.
//
// Twin of internal/storage/parquets3/bounds_resolve_test.go (the logs module).
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

func (m *rwMock) recover(key string) {
	m.mu.Lock()
	delete(m.status, key)
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

// rwSetClock pins the clock the startup pass reads to find the recent objects.
func rwSetClock(t *testing.T, now time.Time) {
	t.Helper()
	old := nowFn
	nowFn = func() time.Time { return now }
	t.Cleanup(func() { nowFn = old })
}

// listedEnv is a pod that restarted and learned its objects from the bucket
// listing alone (inferred bounds), over a fault-injecting bucket, with the
// clock half an hour into the fixture hour.
func listedEnv(t *testing.T, mock *rwMock, ingest func(e *restartEnv)) *restartEnv {
	t.Helper()
	rwSetClock(t, rwHour.Add(30*time.Minute))
	e := newRestartEnvWith(t, mock.mockS3Server)
	ingest(e)
	e.restart(true, false, true)
	return e
}

func (e *restartEnv) resetBackoff() {
	e.s.inferredBounds.mu.Lock()
	e.s.inferredBounds.retry = nil
	e.s.inferredBounds.mu.Unlock()
}

func (e *restartEnv) backoffEntries() int {
	e.s.inferredBounds.mu.Lock()
	defer e.s.inferredBounds.mu.Unlock()
	return len(e.s.inferredBounds.retry)
}

func oneCold(e *restartEnv) {
	e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute))
}

// A failing object is not read again on every attempt, stays inferred, and
// resolves once the back-off has expired and the object recovers.
func TestResolveBounds_FailingObjectIsBackedOffAndRecovers(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	e := listedEnv(t, mock, oneCold)
	fi := e.objects()[0]
	if !fi.BoundsInferred {
		t.Fatalf("precondition: %+v", fi)
	}
	mock.fail(fi.Key, http.StatusServiceUnavailable)

	if cur := e.s.resolveFileBounds(context.Background(), fi); !cur.BoundsInferred {
		t.Fatalf("an unreadable object was resolved: %+v", cur)
	}
	if mock.keyGets(fi.Key) == 0 {
		t.Fatal("precondition: the first attempt never tried the object")
	}

	mock.reset()
	if cur := e.s.resolveFileBounds(context.Background(), fi); !cur.BoundsInferred {
		t.Fatalf("resolved during the back-off: %+v", cur)
	}
	if n := mock.keyGets(fi.Key); n != 0 {
		t.Errorf("the second attempt read the failing object %d times, want 0 (back-off)", n)
	}

	e.resetBackoff()
	mock.recover(fi.Key)
	if cur := e.s.resolveFileBounds(context.Background(), fi); cur.BoundsInferred || cur.MaxTimeNs != at(rwHour, 11*time.Minute).UnixNano() {
		t.Errorf("after the back-off expired and the object recovered: %+v, want exact bounds", cur)
	}
}

// The startup pass reads only the recent objects, each by at most one ranged
// read (never a whole object), and nothing again afterwards.
func TestResolveBounds_StartupPassReadsRecentObjectsByRangeOnce(t *testing.T) {
	mock := newRWMock()
	const K = 40
	e := listedEnv(t, mock, func(e *restartEnv) {
		for i := 0; i < K; i++ {
			e.ingest("COLD", at(rwHour.Add(-time.Duration(i)*time.Hour), 10*time.Minute))
		}
	})
	if got := len(e.objects()); got != K {
		t.Fatalf("manifest holds %d objects, want %d", got, K)
	}
	// The clock is half an hour into rwHour and the window is six hours back:
	// the partitions of 01:00 to 07:00 overlap it.
	const inWindow = 7

	mock.reset()
	n := e.s.enrichRecentInferredBounds(context.Background())
	full, ranged, bytes := mock.gets()
	if n != inWindow || ranged != inWindow {
		t.Errorf("resolved %d objects with %d ranged GETs, want the %d inside the window", n, ranged, inWindow)
	}
	if full != 0 {
		t.Errorf("%d whole-object GETs resolving listing-only objects, want 0", full)
	}
	if limit := int64(inWindow * 128 << 10); bytes > limit {
		t.Errorf("%d bytes read, want at most %d (the footer tail per object)", bytes, limit)
	}

	mock.reset()
	if again := e.s.enrichRecentInferredBounds(context.Background()); again != 0 {
		t.Errorf("a second pass resolved %d objects, want none left", again)
	}
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("a second pass read %d+%d times, want 0 (bounds are now exact)", full, ranged)
	}
}

// One footer read per object however many callers ask at once, and every caller
// sees the bounds that read resolved.
func TestResolveBounds_ConcurrentCallersShareOneRead(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, oneCold)
	fi := e.objects()[0]
	mock.delay(fi.Key, 300*time.Millisecond)
	mock.reset()
	want := at(rwHour, 11*time.Minute).UnixNano()
	var wg sync.WaitGroup
	got := make([]manifest.FileInfo, 32)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i] = e.s.resolveFileBounds(context.Background(), fi)
		}(i)
	}
	wg.Wait()
	if full, ranged, _ := mock.gets(); full != 0 || ranged != 1 {
		t.Errorf("32 concurrent callers made %d whole and %d ranged GETs, want 0 and 1", full, ranged)
	}
	for i, g := range got {
		if g.BoundsInferred || g.MaxTimeNs != want {
			t.Errorf("caller %d: %+v, want the resolved bounds (a waiter fell back to its stale copy)", i, g)
		}
	}
}

// A cancelled caller neither reads nor penalises the object; the next caller
// resolves it.
func TestResolveBounds_CancelledContextReadsNothingAndDoesNotBackOff(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, oneCold)
	fi := e.objects()[0]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mock.reset()
	if cur := e.s.resolveFileBounds(ctx, fi); !cur.BoundsInferred {
		t.Fatalf("a cancelled caller resolved the object: %+v", cur)
	}
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("a cancelled context still made %d GETs", full+ranged)
	}
	if e.s.inferredBounds.backedOff(fi.Key, time.Now()) {
		t.Error("a cancelled caller put the object into back-off")
	}
	if cur := e.s.resolveFileBounds(context.Background(), fi); cur.BoundsInferred {
		t.Errorf("the next caller did not resolve the object: %+v", cur)
	}
}

// A caller that goes away DURING the read does not put the object into back-off.
func TestResolveBounds_CancelDuringReadDoesNotBackOff(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	e := listedEnv(t, mock, oneCold)
	fi := e.objects()[0]
	mock.delay(fi.Key, 1500*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	_ = e.s.resolveFileBounds(ctx, fi)
	if e.s.inferredBounds.backedOff(fi.Key, time.Now()) {
		t.Error("a cancelled caller put the object into back-off")
	}
}

// The pmeta facet is the first source: exact bounds from RAM, no S3 read.
func TestResolveBounds_FacetResolvesWithoutAnyRead(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) {
		e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 12*time.Minute))
	})
	e.s.cfg.Pmeta.Enabled = true
	e.s.catalog = newCatalogStore(e.s.cfg.Pmeta, "traces/")
	fi := e.objects()[0]
	exMin, exMax := at(rwHour, 10*time.Minute).UnixNano(), at(rwHour, 12*time.Minute).UnixNano()
	e.s.catalog.OnFileFlush(pmeta.FileContribution{Partition: manifest.ExtractTenantPartition(fi.Key), FileKey: fi.Key, RowCount: 2, MinTimeNs: exMin, MaxTimeNs: exMax})
	mock.reset()
	if cur := e.s.resolveFileBounds(context.Background(), fi); cur.BoundsInferred || cur.MaxTimeNs != exMax {
		t.Errorf("resolved %+v, want the facet's bounds ending at %d", cur, exMax)
	}
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("resolving from the facet made %d GETs, want 0", full+ranged)
	}
}

// The warmup resolves recent objects by ranged footer reads alone, also where
// the footer-only prefetch cannot supply bounds.
func TestResolveBounds_WarmMetadataResolvesRecentObjectsByRangeOnly(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) {
		e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 12*time.Minute))
	})
	e.s.footerCache = nil // an insert-only pod: no footer prefetch phases, only the startup pass
	if f := e.objects(); len(f) != 1 || !f[0].BoundsInferred {
		t.Fatalf("precondition: %+v", f)
	}
	mock.reset()
	e.s.WarmMetadata(context.Background())
	f := e.objects()
	if f[0].BoundsInferred || f[0].MaxTimeNs != at(rwHour, 12*time.Minute).UnixNano() {
		t.Errorf("after WarmMetadata: %+v, want exact bounds", f[0])
	}
	if full, ranged, _ := mock.gets(); full != 0 || ranged != 1 {
		t.Errorf("WarmMetadata made %d whole and %d ranged object GETs, want 0 and 1", full, ranged)
	}
}

// The startup pass is bounded to the recent window: older objects wait.
func TestResolveBounds_StartupPassIsBoundedToTheRecentWindow(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) {
		e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour.Add(-10*time.Hour), 10*time.Minute))
	})
	mock.reset()
	if n := e.s.enrichRecentInferredBounds(context.Background()); n != 1 {
		t.Errorf("enrichRecentInferredBounds resolved %d objects, want 1 (the old one is outside the window)", n)
	}
	if _, ranged, _ := mock.gets(); ranged != 1 {
		t.Errorf("%d ranged GETs, want 1", ranged)
	}
}

// With more recent objects than the cap, the startup pass keeps the NEWEST.
func TestResolveBounds_StartupPassKeepsTheNewestObjects(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) {
		for i := 0; i < 4; i++ {
			e.ingest("COLD", at(rwHour.Add(-time.Duration(i)*time.Hour), 10*time.Minute))
		}
	})
	old := recentInferredMaxFiles
	recentInferredMaxFiles = 2
	t.Cleanup(func() { recentInferredMaxFiles = old })
	mock.reset()
	if n := e.s.enrichRecentInferredBounds(context.Background()); n != 2 {
		t.Fatalf("resolved %d objects, want 2", n)
	}
	for _, fi := range e.objects() {
		newest := strings.Contains(fi.Key, "hour=07") || strings.Contains(fi.Key, "hour=06")
		if newest == fi.BoundsInferred {
			t.Errorf("%s: inferred=%v, want the two newest resolved and the rest not", fi.Key, fi.BoundsInferred)
		}
	}
}

// The startup pass covers the whole recent window, not just the last hour.
func TestResolveBounds_StartupPassCoversAFewHoursBack(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) { e.ingest("COLD", at(rwHour.Add(-4*time.Hour), 10*time.Minute)) })
	if n := e.s.enrichRecentInferredBounds(t.Context()); n != 1 {
		t.Errorf("resolved %d objects, want the one from 4 hours back", n)
	}
}

// A footer-statistics read gives the same bounds as the rows written.
func TestResolveBounds_FooterStatisticsMatchTheWrittenRows(t *testing.T) {
	mock := newRWMock()
	lo, hi := at(rwHour, 10*time.Minute), at(rwHour, 40*time.Minute)
	e := listedEnv(t, mock, func(e *restartEnv) {
		e.ingest("COLD", at(rwHour, 20*time.Minute), lo, hi, at(rwHour, 30*time.Minute))
	})
	fi := e.objects()[0]
	rows, minNs, maxNs, err := e.s.readFooterTimeBounds(context.Background(), fi)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 4 || minNs != lo.UnixNano() || maxNs != hi.UnixNano() {
		t.Errorf("footer bounds = rows %d [%d, %d], want 4 rows [%d, %d]", rows, minNs, maxNs, lo.UnixNano(), hi.UnixNano())
	}
}

// A footer larger than the tail read resolves (second, exact-length read).
func TestResolveBounds_OversizeFooterResolves(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) {
		e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 20*time.Minute))
	})
	e.s.cfg.S3.FooterPrefetchBytes = 16 // a tail smaller than the footer
	fi := e.objects()[0]
	mock.reset()
	rows, mn, mx, err := e.s.readFooterTimeBounds(context.Background(), fi)
	if err != nil {
		t.Fatalf("readFooterTimeBounds with a footer larger than the tail: %v", err)
	}
	if rows != 2 || mn != at(rwHour, 10*time.Minute).UnixNano() || mx != at(rwHour, 20*time.Minute).UnixNano() {
		t.Errorf("bounds = %d rows [%d, %d]", rows, mn, mx)
	}
	if full, ranged, _ := mock.gets(); full != 0 || ranged != 2 {
		t.Errorf("%d whole and %d ranged GETs, want 0 and 2 (tail, then the exact footer)", full, ranged)
	}
}

// A cached footer is reused: no GET at all.
func TestResolveBounds_CachedFooterMeansNoRead(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, oneCold)
	fi := e.objects()[0]
	if _, _, _, err := e.s.readFooterTimeBounds(context.Background(), fi); err != nil {
		t.Fatal(err)
	}
	mock.reset()
	if _, _, _, err := e.s.readFooterTimeBounds(context.Background(), fi); err != nil {
		t.Fatal(err)
	}
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("%d GETs with the footer cached, want 0", full+ranged)
	}
}

// A footer that reports exactly the partition hour is not evidence: the object
// stays inferred and is backed off.
func TestResolveBounds_HourShapedFooterStaysInferredAndBacksOff(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) { e.ingest("COLD", rwHour, rwHour.Add(time.Hour-time.Nanosecond)) })
	fi := e.objects()[0]
	mock.reset()
	if cur := e.s.resolveFileBounds(context.Background(), fi); !cur.BoundsInferred {
		t.Fatalf("hour-shaped footer bounds were accepted as exact: %+v", cur)
	}
	if !e.s.inferredBounds.backedOff(fi.Key, time.Now()) {
		t.Error("no back-off after an hour-shaped footer result")
	}
}

// A row group without statistics makes the answer unavailable (never a range
// over the groups that have them).
func TestResolveBounds_FooterBoundsNeedEveryRowGroup(t *testing.T) {
	type row struct {
		T int64  `parquet:"_time"`
		M string `parquet:"_msg"`
	}
	var buf rwBytes
	w := parquet.NewGenericWriter[row](&buf)
	for _, v := range []int64{100, 200, 300} {
		if _, err := w.Write([]row{{v, "x"}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(buf.reader(), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	idx := findColumnIndex(f.Root(), "_time")
	if rows, lo, hi, err := footerTimeBounds(f, idx); err != nil || rows != 3 || lo != 100 || hi != 300 {
		t.Fatalf("all statistics present: rows=%d [%d,%d] err=%v", rows, lo, hi, err)
	}
	md := f.Metadata()
	md.RowGroups[1].Columns[idx].MetaData.Statistics.MinValue = nil
	md.RowGroups[1].Columns[idx].MetaData.Statistics.MaxValue = nil
	md.RowGroups[1].Columns[idx].MetaData.Statistics.Min = nil
	md.RowGroups[1].Columns[idx].MetaData.Statistics.Max = nil
	if _, lo, hi, err := footerTimeBounds(f, idx); err == nil {
		t.Errorf("a row group without statistics was skipped: got [%d,%d], want unavailable", lo, hi)
	}
	if _, _, _, err := footerTimeBounds(f, -1); err == nil {
		t.Error("a missing column must be unavailable")
	}
}

// Statistics edge cases.
func TestResolveBounds_FooterStatisticsEdgeCases(t *testing.T) {
	data := rr3File(t, 100, 300)
	open := func() (*parquet.File, int) {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		return f, findColumnIndex(f.Root(), "_time")
	}
	t.Run("deprecated min/max are used when min_value/max_value are absent", func(t *testing.T) {
		f, idx := open()
		for i := range f.Metadata().RowGroups {
			st := &f.Metadata().RowGroups[i].Columns[idx].MetaData.Statistics
			st.Min, st.Max = st.MinValue, st.MaxValue
			st.MinValue, st.MaxValue = nil, nil
		}
		if rows, lo, hi, err := footerTimeBounds(f, idx); err != nil || rows != 2 || lo != 100 || hi != 300 {
			t.Errorf("rows=%d [%d, %d] err=%v, want 2 [100, 300]", rows, lo, hi, err)
		}
	})
	t.Run("a non-positive minimum is not a time range", func(t *testing.T) {
		f, idx := open()
		var zero [8]byte
		f.Metadata().RowGroups[0].Columns[idx].MetaData.Statistics.MinValue = zero[:]
		if _, lo, hi, err := footerTimeBounds(f, idx); err == nil {
			t.Errorf("min 0 accepted: [%d, %d]", lo, hi)
		}
	})
	t.Run("max below min is rejected", func(t *testing.T) {
		f, idx := open()
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], 50)
		f.Metadata().RowGroups[1].Columns[idx].MetaData.Statistics.MaxValue = b[:]
		f.Metadata().RowGroups[0].Columns[idx].MetaData.Statistics.MaxValue = b[:]
		if _, lo, hi, err := footerTimeBounds(f, idx); err == nil {
			t.Errorf("max < min accepted: [%d, %d]", lo, hi)
		}
	})
}

// The back-off table forgets objects the manifest dropped, and is capped.
func TestResolveBounds_BackoffEntriesDoNotOutliveObjects(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) { e.ingest("COLD", at(rwHour, 10*time.Minute)) })
	fi := e.objects()[0]
	mock.fail(fi.Key, http.StatusNotFound)
	_ = e.s.resolveFileBounds(context.Background(), fi)
	if !e.s.inferredBounds.backedOff(fi.Key, time.Now()) {
		t.Fatal("precondition: the failed object is in back-off")
	}
	e.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key)
	if n := e.backoffEntries(); n != 0 {
		t.Errorf("back-off table holds %d entries for an object that left the manifest", n)
	}
}

func TestResolveBounds_BackoffTableIsCapped(t *testing.T) {
	var r boundsResolver
	past := time.Now().Add(-time.Hour)
	for i := 0; i < maxBoundsRetryEntries; i++ {
		r.failed("old-"+time.Duration(i).String(), past, 0) // expired entries
	}
	r.failed("fresh", time.Now(), 0)
	r.failed("fresh2", time.Now(), 0)
	r.mu.Lock()
	n := len(r.retry)
	r.mu.Unlock()
	if n > 3 {
		t.Errorf("table holds %d entries after the sweep, want only the live ones", n)
	}
}

// The back-off doubles per consecutive failure and is capped.
func TestResolveBounds_BackoffDoublesAndIsCapped(t *testing.T) {
	var r boundsResolver
	base := time.Now()
	want := []time.Duration{boundsBackoffMin, 2 * boundsBackoffMin, 4 * boundsBackoffMin, 8 * boundsBackoffMin}
	for i, w := range want {
		r.failed("k", base, 0)
		r.mu.Lock()
		got := r.retry["k"].until.Sub(base)
		r.mu.Unlock()
		if got != w {
			t.Errorf("failure %d: back-off %v, want %v", i+1, got, w)
		}
	}
	for i := 0; i < 20; i++ {
		r.failed("k", base, 0)
	}
	r.mu.Lock()
	got := r.retry["k"].until.Sub(base)
	r.mu.Unlock()
	if got != boundsBackoffMax {
		t.Errorf("capped back-off = %v, want %v", got, boundsBackoffMax)
	}
}

// A success forgets the back-off entry.
func TestResolveBounds_SuccessForgetsTheBackoffEntry(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, oneCold)
	fi := e.objects()[0]
	e.s.inferredBounds.mu.Lock()
	e.s.inferredBounds.retry = map[string]boundsRetry{fi.Key: {until: time.Now().Add(-time.Second), fails: 3}} // expired
	e.s.inferredBounds.mu.Unlock()
	if cur := e.s.resolveFileBounds(t.Context(), fi); cur.BoundsInferred {
		t.Fatalf("not resolved: %+v", cur)
	}
	if n := e.backoffEntries(); n != 0 {
		t.Errorf("the back-off table still holds %d entries after a success", n)
	}
}

// A stale copy of an entry the manifest already resolved costs no read.
func TestResolveBounds_StaleCopyOfAResolvedObjectCostsNoRead(t *testing.T) {
	mock := newRWMock()
	e := listedEnv(t, mock, oneCold)
	stale := e.objects()[0]
	e.s.manifest.EnrichFileMetadata(stale.Key, 2, at(rwHour, 10*time.Minute).UnixNano(), at(rwHour, 11*time.Minute).UnixNano())
	mock.reset()
	cur := e.s.resolveFileBounds(t.Context(), stale)
	if cur.BoundsInferred || cur.MaxTimeNs != at(rwHour, 11*time.Minute).UnixNano() {
		t.Errorf("stale copy not replaced by the manifest's exact entry: %+v", cur)
	}
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("%d reads for an already resolved object", full+ranged)
	}
}

// A compaction retires the object between selection and the read; the read
// fails (404). No back-off entry is left for a key the manifest no longer holds.
func TestResolveBounds_RemovedDuringResolutionLeavesNoBackoffEntry(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) { e.ingest("COLD", at(rwHour, 10*time.Minute)) })
	fi := e.objects()[0]
	mock.fail(fi.Key, http.StatusNotFound)
	e.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key)
	if cur := e.s.resolveFileBounds(context.Background(), fi); cur.MaxTimeNs != fi.MaxTimeNs {
		t.Errorf("a removed object changed: %+v", cur)
	}
	if n := e.backoffEntries(); n != 0 {
		t.Errorf("back-off map holds %d entries for an object no longer in the manifest", n)
	}
}

// A removal that overlaps an in-flight failing read must not leave a back-off
// entry behind.
func TestResolveBounds_RemovalDuringAFailingReadLeavesNoBackoffEntry(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) { e.ingest("COLD", at(rwHour, 10*time.Minute)) })
	fi := e.objects()[0]
	mock.fail(fi.Key, 404)
	mock.delay(fi.Key, 300*time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.s.resolveFileBounds(t.Context(), fi)
	}()
	time.Sleep(100 * time.Millisecond)
	e.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key)
	<-done
	if n := e.backoffEntries(); n != 0 {
		t.Errorf("%d back-off entries for an object removed while its read was in flight", n)
	}
}

// A removal that lands between the existence check and the failing read must
// leave no back-off entry (the removal counter is taken before the check).
func TestResolveBounds_RemovalAfterTheExistenceCheckLeavesNoBackoffEntry(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) { e.ingest("COLD", at(rwHour, 10*time.Minute)) })
	fi := e.objects()[0]
	mock.fail(fi.Key, 404)
	resolveAfterExistsCheck = func() { e.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key) }
	t.Cleanup(func() { resolveAfterExistsCheck = nil })
	_ = e.s.resolveFileBounds(t.Context(), fi)
	if n := e.backoffEntries(); n != 0 {
		t.Errorf("%d back-off entries for an object removed in the check-then-act gap", n)
	}
}

// An object that leaves the manifest through a REFRESH (a peer's compaction
// deleted it; the listing no longer has it) is not passed to the OnFileRemoved
// hook: refresh replaces m.files wholesale.
func TestResolveBounds_RefreshDropDoesNotPruneBackoff(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) { e.ingest("COLD", at(rwHour, 10*time.Minute)) })
	fi := e.objects()[0]
	mock.fail(fi.Key, http.StatusServiceUnavailable)
	_ = e.s.resolveFileBounds(context.Background(), fi)
	mock.mu.Lock()
	for k := range mock.files {
		delete(mock.files, k)
	}
	mock.mu.Unlock()
	if err := e.s.manifest.RefreshFromS3(context.Background(), e.s.pool.S3Client()); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.s.manifest.GetFileByKey(fi.Key); ok {
		t.Skip("refresh kept the object")
	}
	if n := e.backoffEntries(); n != 0 {
		t.Errorf("back-off map holds %d entries after the object left the manifest via refresh", n)
	}
}

// OnFileRemoved runs under the manifest write lock and takes the resolver mutex.
// Hammer removals concurrently with resolutions (which take the resolver mutex
// and call into the manifest) to look for a deadlock.
func TestResolveBounds_RemovalHookVsResolutionNoDeadlock(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	e := listedEnv(t, mock, func(e *restartEnv) {
		for i := 0; i < 20; i++ {
			e.ingest("COLD", at(rwHour, time.Duration(i)*time.Minute))
			e.flush()
		}
	})
	files := e.objects()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 50; k++ {
					e.resetBackoff()
					for _, fi := range files {
						_ = e.s.resolveFileBounds(context.Background(), fi)
					}
				}
			}()
		}
		for _, fi := range files {
			e.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key)
			e.s.manifest.AddFile(manifest.ExtractPartition(fi.Key), fi)
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: removal hook vs resolution did not finish in 60s")
	}
}

// After a snapshot written after the drain, a restart loads exact bounds; from
// the listing alone the bounds are inferred until the footer resolves them.
func TestResolveBounds_SnapshotAfterTheDrainLoadsExactBounds(t *testing.T) {
	last := at(rwHour, 10*time.Minute+2*time.Second)
	for _, tc := range []struct {
		name          string
		snapshotAfter bool
	}{{"snapshot persisted after the drain", true}, {"snapshot persisted before it only", false}} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newRWMock()
			rwSetClock(t, rwHour.Add(30*time.Minute))
			e := newRestartEnvWith(t, mock.mockS3Server)
			e.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 10*time.Minute+time.Second), last)
			e.restart(true, tc.snapshotAfter, true)
			files := e.objects()
			if len(files) != 1 {
				t.Fatalf("manifest holds %d objects, want 1", len(files))
			}
			if tc.snapshotAfter {
				if files[0].BoundsInferred || files[0].MaxTimeNs != last.UnixNano() {
					t.Errorf("restart from the post-drain snapshot: %+v, want exact MaxTimeNs %d", files[0], last.UnixNano())
				}
				return
			}
			if !files[0].BoundsInferred {
				t.Fatalf("restart from the pre-drain snapshot: %+v, want the listing's inferred bounds", files[0])
			}
			if cur := e.s.resolveFileBounds(context.Background(), files[0]); cur.BoundsInferred || cur.MaxTimeNs != last.UnixNano() {
				t.Errorf("after resolving: %+v, want exact bounds ending at %d", cur, last.UnixNano())
			}
		})
	}
}
