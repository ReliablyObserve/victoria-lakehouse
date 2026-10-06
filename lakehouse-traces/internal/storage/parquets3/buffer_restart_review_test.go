package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// A kill -9 restart serves from the on-disk manifest snapshot
// (serve-while-warming) until the startup S3 refresh completes. A segment
// committed after the last snapshot persist and reaped at load has its rows in
// neither the buffer nor the manifest during that window.
func TestBufferRestartReview_StaleSnapshot_RowsServedUntilFirstRefresh(t *testing.T) {
	for _, tc := range []struct {
		name     string
		downtime time.Duration
		noRecord bool
	}{
		{"recorded, down between grace and guard", time.Minute + time.Second, false},
		{"record-less (upgrade from a state without commits), down 1s", time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newRestartEnv(t)
			e.ingest("A", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute))
			// Periodic persist happened BEFORE the commit (kill -9 later).
			if err := e.s.manifest.SaveTo(e.snapshot); err != nil {
				t.Fatal(err)
			}
			e.flush()
			commit := time.Now()
			if tc.noRecord {
				st, _ := readFlushState(e.f.statePath)
				st.Commits = nil
				if err := e.f.writeState(st); err != nil {
					t.Fatal(err)
				}
			}
			e.segs.Close()
			m := manifest.New("test-bucket", "traces/")
			if err := m.LoadFrom(e.snapshot); err != nil {
				t.Fatal(err)
			}
			restartAt := commit.Add(tc.downtime)
			e.loadNow = func() time.Time { return restartAt }
			e.boot(m)
			// Serving ready; startup S3 refresh still running.
			e.checkRows("served from the stale snapshot before the S3 refresh", rwHour, rwHour.Add(time.Hour), 2)
			// The loop must not retire the segment before the refresh either.
			e.f.tick(context.Background(), restartAt)
			e.checkRows("after a flusher tick, still before the S3 refresh", rwHour, rwHour.Add(time.Hour), 2)
			// The first successful refresh publishes the objects and releases
			// the segment in one step: still exactly once.
			e.refresh()
			e.checkRows("after the first S3 refresh", rwHour, rwHour.Add(time.Hour), 2)
		})
	}
}

// Residual (#406): crash after the marker PUT and before the commit state write. The
// guard counts from the marker; the segment is still pending at restart and is
// served from the buffer while compaction merges its objects.
func TestBufferRestartReview_CrashBetweenMarkerAndState_Duplicates(t *testing.T) {
	t.Skip("known gap, tracked in #406: a crash between the marker PUT and the commit-state write can duplicate rows for about one grace")
	const grace = time.Minute
	e := newRestartEnv(t)
	e.ingest("A", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute))
	e.flush()
	e.ingest("B", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute))
	e.flush()
	commit := time.Now()
	markers := e.segmentMarkers(commit)
	// Rewind the state to "draining, not committed" (crash between the marker
	// PUT and the state write).
	st, _ := readFlushState(e.f.statePath)
	st.DrainingSeq = st.CommittedThroughSeq
	st.CommittedThroughSeq--
	st.Commits = nil
	if err := e.f.writeState(st); err != nil {
		t.Fatal(err)
	}
	restartAt := commit.Add(3 * grace)
	e.loadNow = func() time.Time { return restartAt }
	e.restart(true, true, false)
	if n := e.compactSegmentObjects(markers, 2*grace, restartAt); n == 0 {
		t.Fatal("nothing merged")
	}
	e.check("pending segment after compaction, before re-drain")
	e.flush()
	e.check("after re-drain (served for a new grace)")
}
