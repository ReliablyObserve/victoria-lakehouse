package parquets3

// Round-3 regression tests for #272: the watermark must cover every object
// that contributes to the answer (also those answered from metadata), same-hour
// flushes after a restart, UTC midnight, tenants, pauses and the back-off table.
//
// Twin of internal/storage/parquets3/restart_watermark_rr3_test.go (the logs module).

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// RR3-A. An exact object fully inside the window is answered from metadata
// (manifestFastPath) and then DROPS OUT of the list the watermark is computed
// over (files = remaining). If the only remaining object is older, the
// watermark is lower than the served object's max and the buffer re-serves the
// served object's rows. Snapshot after the final flush => all bounds exact.
func TestRR3_MetadataServedObjectStillRaisesWatermark(t *testing.T) {
	r := newRWRig(t)
	prev := rwHour.Add(-time.Hour)
	r.ingest("OLD", at(prev, 50*time.Minute), at(prev, 58*time.Minute))
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 20*time.Minute))
	r.restart(true, true)
	for _, fi := range r.objects() {
		if fi.BoundsInferred {
			t.Fatalf("precondition: exact bounds expected, got %+v", fi)
		}
	}
	from, to := at(prev, 55*time.Minute), rwHour.Add(2*time.Hour)
	if got := r.statsRows(from, to); got != 3 {
		t.Errorf("stats count path = %d, want 3 (1 OLD in window + 2 COLD)", got)
	}
	if got := r.rows(context.Background(), from, to); got != 3 {
		t.Errorf("query=* = %d, want 3", got)
	}
}

// RR3-A2. Same, where the remaining object is the INFERRED one (#272 shape):
// the exact newer object is metadata-served, the inferred older one is read.
func TestRR3_MetadataServedExactPlusInferredOlder(t *testing.T) {
	r := newRWRig(t)
	prev := rwHour.Add(-time.Hour)
	r.ingest("OLD", at(prev, 50*time.Minute), at(prev, 58*time.Minute))
	r.restart(true, false) // OLD only via listing: inferred
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 20*time.Minute))
	if err := r.bw.FlushAll(context.Background()); err != nil { // exact, registered by the flush
		t.Fatal(err)
	}
	from, to := at(prev, 55*time.Minute), rwHour.Add(2*time.Hour)
	if got := r.statsRows(from, to); got != 3 {
		t.Errorf("stats count path = %d, want 3", got)
	}
}

// RR3-B. UTC midnight: object in hour 23 of day D learned by listing; clock
// past midnight. Buffer (fake) keeps everything. Rows buffered after the
// restart on D+1 must show; cold rows exactly once.
func TestRR3_AcrossUTCMidnight(t *testing.T) {
	for _, clock := range []time.Duration{20 * time.Minute, 90 * time.Minute, 26 * time.Hour} {
		t.Run(clock.String(), func(t *testing.T) {
			r := newRWRig(t)
			h23 := time.Date(2026, 9, 30, 23, 0, 0, 0, time.UTC)
			mid := h23.Add(time.Hour)
			r.ingest("COLD", at(h23, 50*time.Minute), at(h23, 59*time.Minute))
			r.restart(true, false)
			r.s.cfg.Insert.BufferRetention = time.Hour
			rwSetClock(t, mid.Add(clock))
			r.bufferOnly("NEW", at(mid, 10*time.Minute))
			from, to := h23, mid.Add(clock)
			if got := r.statsRows(from, to); got != 3 {
				t.Errorf("stats = %d, want 3 (2 cold + 1 new)", got)
			}
			if got := r.rows(context.Background(), from, to); got != 3 {
				t.Errorf("query=* = %d, want 3", got)
			}
		})
	}
}

// RR3-C. A query window that STARTS after the inferred object's hour but the
// buffer still holds that hour's rows: the object is not selected, no
// watermark, buffer serves from startNs: correct (rows are out of window).
// Then a window that starts mid-hour: object selected, inferred end hides.
func TestRR3_WindowStartsMidInferredHour(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 40*time.Minute))
	r.restart(true, false)
	mock := r.mock
	_ = mock
	from, to := at(rwHour, 30*time.Minute), rwHour.Add(2*time.Hour)
	if got := r.statsRows(from, to); got != 1 {
		t.Errorf("stats = %d, want 1", got)
	}
	if got := r.rows(context.Background(), from, to); got != 1 {
		t.Errorf("query=* = %d, want 1", got)
	}
}

// RR3-D. Compaction retires the inferred object between selection and the
// watermark computation; the in-flight resolution then fails (404). The
// back-off entry the OnFileRemoved hook pruned is re-created for a key the
// manifest no longer holds.
func TestRR3_RemovedDuringResolutionLeavesBackoffEntry(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	fi := r.objects()[0]
	mock.fail(fi.Key, http.StatusNotFound)
	r.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key)
	wm := r.s.bufferWatermarksFor(context.Background(), rwHour.Add(-time.Hour).UnixNano(), []manifest.FileInfo{fi})
	if wm[tenantZero] != fi.MaxTimeNs {
		t.Errorf("wm = %d, want inferred end %d (hide)", wm[tenantZero], fi.MaxTimeNs)
	}
	r.s.inferredBounds.mu.Lock()
	n := len(r.s.inferredBounds.retry)
	r.s.inferredBounds.mu.Unlock()
	t.Logf("back-off entries after a removed object's failed resolution: %d", n)
	if n != 0 {
		t.Errorf("back-off map holds %d entries for an object no longer in the manifest (hook ran before the read failed)", n)
	}
}

// RR3-E. OnFileRemoved runs under the manifest write lock and takes the
// resolver mutex. Hammer removals concurrently with resolutions (which take
// the resolver mutex and call into the manifest) to look for a deadlock.
func TestRR3_RemovalHookVsResolutionNoDeadlock(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	for i := 0; i < 20; i++ {
		r.ingest("COLD", at(rwHour, time.Duration(i)*time.Minute))
		if err := r.bw.FlushAll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	r.restart(true, false)
	files := r.objects()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 50; k++ {
					r.resetBackoff()
					_ = r.s.bufferWatermarksFor(context.Background(), rwHour.Add(-time.Hour).UnixNano(), files)
				}
			}()
		}
		for _, fi := range files {
			r.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key)
			r.s.manifest.AddFile(manifest.ExtractPartition(fi.Key), fi)
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: removal hook vs resolution did not finish in 60s")
	}
}

// RR3-F. Multi-tenant: tenant 1 has an exact object and an inferred one in a
// later hour; tenant 2 has only an inferred object. No S3 (unresolvable).
// Each tenant must keep the conservative inferred end of its own objects and
// never take another tenant's.
func TestRR3_MultiTenantMixedAndOnlyInferred(t *testing.T) {
	f := newTenantScopeFixture(t)
	f.s.pool = nil
	f.s.footerCache = nil
	h := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	old := nowFn
	nowFn = func() time.Time { return h.Add(30 * time.Minute) }
	t.Cleanup(func() { nowFn = old })
	inferredEnd := h.Add(time.Hour).UnixNano() - 1
	files := []manifest.FileInfo{
		{Key: "1001/0/logs/dt=2026-05-10/hour=13/a.parquet", MinTimeNs: h.Add(-50 * time.Minute).UnixNano(), MaxTimeNs: h.Add(-40 * time.Minute).UnixNano()},
		{Key: "1001/0/logs/dt=2026-05-10/hour=14/b.parquet", MinTimeNs: h.UnixNano(), MaxTimeNs: inferredEnd, BoundsInferred: true},
		{Key: "2002/7/logs/dt=2026-05-10/hour=14/c.parquet", MinTimeNs: h.UnixNano(), MaxTimeNs: inferredEnd, BoundsInferred: true},
		{Key: "3003/0/logs/dt=2026-05-10/hour=13/d.parquet", MinTimeNs: h.Add(-time.Hour).UnixNano(), MaxTimeNs: h.UnixNano() - 1, BoundsInferred: true},
	}
	wm := f.s.bufferWatermarksFor(context.Background(), h.Add(-2*time.Hour).UnixNano(), files)
	if got := wm[logstorage.TenantID{AccountID: 1001}]; got != inferredEnd {
		t.Errorf("1001 wm = %d, want inferred end %d", got, inferredEnd)
	}
	if got := wm[logstorage.TenantID{AccountID: 2002, ProjectID: 7}]; got != inferredEnd {
		t.Errorf("2002:7 wm = %d, want inferred end %d", got, inferredEnd)
	}
	if got := wm[logstorage.TenantID{AccountID: 3003}]; got != h.UnixNano()-1 {
		t.Errorf("3003 wm = %d, want %d", got, h.UnixNano()-1)
	}
	if got := wm[logstorage.TenantID{}]; got != 0 {
		t.Errorf("0:0 wm = %d, want 0 (no objects)", got)
	}
}

// RR3-G. Pause window: after a budget timeout, a NEW inferred object appears
// (a peer flush learned by the next listing). It must hide, not double.
func TestRR3_PauseWindowNewInferredObjectHides(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute))
	r.restart(true, false)
	r.s.inferredBounds.pause(time.Now())
	from, to := rwWindow()
	if got := r.statsRows(from, to); got != 2 {
		t.Errorf("paused stats = %d, want 2", got)
	}
	if got := r.rows(context.Background(), from, to); got != 2 {
		t.Errorf("paused query=* = %d, want 2", got)
	}
}

// RR3-H. Same hour: the shutdown's final flush B is learned by listing
// (inferred); after the restart the pod flushes A (exact, newer, same hour).
// The buffer (logstore engine) holds all four rows. A stats count over a
// window that fully contains A: A is answered from metadata and drops out of
// the watermark list; B (inferred, never metadata-served) resolves to its
// exact max, which is OLDER than A's rows, so the buffer re-serves A's rows.
// On main B kept the hour end (unmarked) and hid them.
func TestRR3_SameHourExactNewerFlushAfterRestart(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute))
	r.restart(true, false)
	r.ingest("NEW", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute))
	if err := r.bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	inf := 0
	for _, fi := range r.objects() {
		if fi.BoundsInferred {
			inf++
		}
	}
	t.Logf("objects=%d inferred=%d", len(r.objects()), inf)
	from, to := rwWindow()
	if got := r.statsRows(from, to); got != 4 {
		t.Errorf("stats count path = %d, want 4", got)
	}
	if got := r.rows(context.Background(), from, to); got != 4 {
		t.Errorf("query=* = %d, want 4", got)
	}
	hits := r.levelHits(from, to)
	if hits["COLD"] != 2 || hits["NEW"] != 2 {
		t.Errorf("field_values = %v", hits)
	}
}

// RR3-I. An object that leaves the manifest through a REFRESH (a peer's
// compaction deleted it; the listing no longer has it) is not passed to the
// OnFileRemoved hook: refresh replaces m.files wholesale.
func TestRR3_RefreshDropDoesNotPruneBackoff(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	fi := r.objects()[0]
	mock.fail(fi.Key, http.StatusServiceUnavailable)
	_ = r.s.bufferWatermarksFor(context.Background(), rwHour.Add(-time.Hour).UnixNano(), []manifest.FileInfo{fi})
	mock.mu.Lock()
	for k := range mock.files {
		delete(mock.files, k)
	}
	mock.mu.Unlock()
	if err := r.s.manifest.RefreshFromS3(context.Background(), r.s.pool.S3Client()); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.s.manifest.GetFileByKey(fi.Key); ok {
		t.Skip("refresh kept the object")
	}
	r.s.inferredBounds.mu.Lock()
	n := len(r.s.inferredBounds.retry)
	r.s.inferredBounds.mu.Unlock()
	if n != 0 {
		t.Errorf("back-off map holds %d entries after the object left the manifest via refresh", n)
	}
}
