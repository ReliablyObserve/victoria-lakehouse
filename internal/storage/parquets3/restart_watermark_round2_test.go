package parquets3

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Regression tests of the second review round (#272): the retention floor must
// only skip a read, never drop an object from the watermark; footers larger
// than the tail resolve; the back-off table is bounded; the startup pass keeps
// the newest objects; cancellations do not penalise an object.
//
// Twin of lakehouse-traces/internal/storage/parquets3/restart_watermark_round2_test.go.

func rwThreeCold() []time.Time {
	return []time.Time{at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute), at(rwHour, 12*time.Minute)}
}

// B1: the object's hour ended well before now (older than the buffer-retention
// floor). The buffer still serves those rows (VL drops buffered data per DAY
// partition), so the object must keep contributing its inferred end: rows hide,
// they are never counted twice. A realistic clock; every way of learning the
// object by listing.
func TestRound2_ObjectOlderThanTheRetentionFloorNeverDoubleCounts(t *testing.T) {
	cases := []struct {
		name string
		boot func(r *rwRig)
	}{
		{"graceful restart, snapshot before the final flush only", func(r *rwRig) { r.restart(true, false) }},
		{"crash or OOM restart: no snapshot at all", func(r *rwRig) { r.restart(false, false) }},
		{"legacy snapshot: the hour stored as if exact", func(r *rwRig) {
			r.restart(true, false)
			legacy := r.objects()[0]
			legacy.BoundsInferred = false
			legacy.MinTimeNs, legacy.MaxTimeNs = rwHour.UnixNano(), rwHour.Add(time.Hour).UnixNano()-1
			old := manifest.New("test-bucket", "logs/")
			old.AddFile("dt=2026-10-01/hour=07", legacy)
			if err := old.SaveTo(r.snapshot); err != nil {
				t.Fatal(err)
			}
			m := manifest.New("test-bucket", "logs/")
			if err := m.LoadFrom(r.snapshot); err != nil {
				t.Fatal(err)
			}
			r.s, r.bw = r.boot(m)
		}},
	}
	for _, tc := range cases {
		for _, q := range []string{"query=*", "stats count()"} {
			t.Run(tc.name+"/"+q, func(t *testing.T) {
				r := newRWRig(t)
				r.ingest("COLD", rwThreeCold()...)
				tc.boot(r)
				rwSetClock(t, rwHour.Add(3*time.Hour)) // two hours past the object's hour
				from, to := rwHour.Add(-time.Hour), rwHour.Add(4*time.Hour)
				for i := 0; i < 2; i++ {
					var got int
					if q == "query=*" {
						got = r.rows(context.Background(), from, to)
					} else {
						got = r.statsRows(from, to)
					}
					if got != 3 {
						t.Errorf("run %d: %s = %d, want 3 (each cold row once)", i, q, got)
					}
				}
			})
		}
	}
}

func TestRound2_OldObjectsAreNotReadButStillHide(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour.Add(-4*time.Hour), 10*time.Minute), at(rwHour.Add(-5*time.Hour), 10*time.Minute))
	r.restart(true, false)
	rwSetClock(t, rwHour.Add(3*time.Hour))
	mock.reset()
	wm := r.s.bufferWatermarksFor(context.Background(), rwHour.Add(-10*time.Hour).UnixNano(), r.objects())
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("%d reads for objects older than the floor, want 0", full+ranged)
	}
	if want := rwHour.Add(-3*time.Hour).UnixNano() - 1; wm[tenantZero] != want {
		t.Errorf("watermark = %d, want the newest inferred end %d", wm[tenantZero], want)
	}
}

// A peer's flush learned by listing, with the buffer bridge, at a late clock.
func TestRound2_PeerObjectOlderThanTheFloorNeverDoubleCounts(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", rwThreeCold()...)
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ := r.boot(manifest.New("test-bucket", "logs/"))
	if err := s.manifest.RefreshFromS3(context.Background(), s.pool.S3Client()); err != nil {
		t.Fatal(err)
	}
	r.s = s
	r.s.localBuffer = nil
	rwSetClock(t, rwHour.Add(3*time.Hour))
	r.s.bufferBridge = fvcPeer(t, rwPeerRows(rwThreeCold()))
	if got := r.rows(context.Background(), rwHour.Add(-time.Hour), rwHour.Add(4*time.Hour)); got != 3 {
		t.Errorf("query=* = %d, want 3 (the peer's buffer copies hide behind the object's inferred end)", got)
	}
}

// M1: a footer larger than the tail read resolves (second, exact-length read).
func TestRound2_OversizeFooterResolves(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 20*time.Minute))
	r.restart(true, false)
	r.s.cfg.S3.FooterPrefetchBytes = 16 // a tail smaller than the footer
	fi := r.objects()[0]
	mock.reset()
	rows, mn, mx, err := r.s.readFooterTimeBounds(context.Background(), fi)
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

// m1: the back-off table forgets objects the manifest dropped, and is capped.
func TestRound2_BackoffEntriesDoNotOutliveObjects(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	fi := r.objects()[0]
	mock.fail(fi.Key, http.StatusNotFound)
	_ = r.s.bufferWatermarksFor(context.Background(), rwHour.Add(-time.Hour).UnixNano(), []manifest.FileInfo{fi})
	if !r.s.inferredBounds.backedOff(fi.Key, time.Now()) {
		t.Fatal("precondition: the failed object is in back-off")
	}
	r.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key)
	r.s.inferredBounds.mu.Lock()
	n := len(r.s.inferredBounds.retry)
	r.s.inferredBounds.mu.Unlock()
	if n != 0 {
		t.Errorf("back-off table holds %d entries for an object that left the manifest", n)
	}
}

func TestRound2_BackoffTableIsCapped(t *testing.T) {
	var r boundsResolver
	past := time.Now().Add(-time.Hour)
	for i := 0; i < maxBoundsRetryEntries; i++ {
		r.failed("old-"+time.Duration(i).String(), past) // expired entries
	}
	r.failed("fresh", time.Now())
	r.failed("fresh2", time.Now())
	r.mu.Lock()
	n := len(r.retry)
	r.mu.Unlock()
	if n > 3 {
		t.Errorf("table holds %d entries after the sweep, want only the live ones", n)
	}
}

// m2: with more recent objects than the cap, the startup pass keeps the NEWEST.
func TestRound2_StartupPassKeepsTheNewestObjects(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	for i := 0; i < 4; i++ {
		r.ingest("COLD", at(rwHour.Add(-time.Duration(i)*time.Hour), 10*time.Minute))
	}
	r.restart(true, false)
	rwSetClock(t, rwHour.Add(30*time.Minute))
	old := recentInferredMaxFiles
	recentInferredMaxFiles = 2
	t.Cleanup(func() { recentInferredMaxFiles = old })
	mock.reset()
	if n := r.s.enrichRecentInferredBounds(context.Background()); n != 2 {
		t.Fatalf("resolved %d objects, want 2", n)
	}
	for _, fi := range r.objects() {
		hour := fi.Key[len(fi.Key)-len("xx/0000000000000000.parquet") : len(fi.Key)-len("/0000000000000000.parquet")]
		newest := hour == "07" || hour == "06"
		if newest == fi.BoundsInferred {
			t.Errorf("object of hour %s: inferred=%v, want the two newest resolved and the rest not", hour, fi.BoundsInferred)
		}
	}
}

// m6: after a computation ran out of its budget no further reads are started
// for a while (rows hide, nothing doubles).
func TestRound2_PausedAfterABudgetTimeout(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	files := r.objects()
	r.s.inferredBounds.pause(time.Now())
	mock.reset()
	wm := r.s.bufferWatermarksFor(context.Background(), 0, files)
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("%d reads while paused, want 0", full+ranged)
	}
	if want := rwHour.Add(time.Hour).UnixNano() - 1; wm[tenantZero] != want {
		t.Errorf("paused watermark = %d, want the inferred end %d", wm[tenantZero], want)
	}
}

// N15: a request that goes away DURING the read does not put the object into
// back-off.
func TestRound2_CancelDuringReadDoesNotBackOff(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	files := r.objects()
	mock.delay(files[0].Key, 1500*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	_ = r.s.bufferWatermarksFor(ctx, 0, files)
	if r.s.inferredBounds.backedOff(files[0].Key, time.Now()) {
		t.Error("a cancelled request put the object into back-off")
	}
}

// N24: a footer that reports exactly the partition hour is not evidence: the
// object stays inferred and is backed off.
func TestRound2_HourShapedFooterStaysInferredAndBacksOff(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", rwHour, rwHour.Add(time.Hour-time.Nanosecond))
	r.restart(true, false)
	files := r.objects()
	mock.reset()
	wm := r.s.bufferWatermarksFor(context.Background(), 0, files)
	if cur, _ := r.s.manifest.GetFileByKey(files[0].Key); !cur.BoundsInferred {
		t.Fatalf("hour-shaped footer bounds were accepted as exact: %+v", cur)
	}
	if !r.s.inferredBounds.backedOff(files[0].Key, time.Now()) {
		t.Error("no back-off after an hour-shaped footer result")
	}
	if want := rwHour.Add(time.Hour).UnixNano() - 1; wm[tenantZero] != want {
		t.Errorf("watermark = %d, want %d", wm[tenantZero], want)
	}
}

// N22: a cached footer is reused: no GET at all.
func TestRound2_CachedFooterMeansNoRead(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	fi := r.objects()[0]
	if _, _, _, err := r.s.readFooterTimeBounds(context.Background(), fi); err != nil {
		t.Fatal(err)
	}
	mock.reset()
	if _, _, _, err := r.s.readFooterTimeBounds(context.Background(), fi); err != nil {
		t.Fatal(err)
	}
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("%d GETs with the footer cached, want 0", full+ranged)
	}
}

// N10: a row group without statistics makes the answer unavailable (never a
// range over the groups that have them).
func TestRound2_FooterBoundsNeedEveryRowGroup(t *testing.T) {
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
