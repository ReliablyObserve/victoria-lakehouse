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

// Known gap (#406): a crash after the marker PUT and before the commit-state
// write. The compaction guard counts from the marker; the restarted pod still
// has the segment pending (its state is the PREVIOUS one, which keeps the
// earlier segments' records) and serves it from the buffer while compaction
// merges its objects: the segment's rows are served twice until it is drained
// again. The earlier, committed segment is exact.
//
// This asserts what happens today so the gap stays visible and measured; it
// fails the moment the gap is fixed, so the assertion is flipped to exact counts
// then.
func TestBufferRestartReview_CrashBetweenMarkerAndState_Duplicates(t *testing.T) {
	const grace = time.Minute
	e := newRestartEnv(t)
	e.ingest("A", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute))
	e.flush()
	e.ingest("B", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute))
	e.flush()
	commit := time.Now()
	markers := e.segmentMarkers(commit)
	// Rewind the state to what a crash between B's marker PUT and B's state
	// write leaves: B draining and not committed, A's record kept.
	st, _ := readFlushState(e.f.statePath)
	if len(st.Commits) != 2 {
		t.Fatalf("fixture: %d commit records, want 2", len(st.Commits))
	}
	st.DrainingSeq = st.CommittedThroughSeq
	st.CommittedThroughSeq--
	st.Commits = st.Commits[:1] // A's record
	if err := e.f.writeState(st); err != nil {
		t.Fatal(err)
	}
	restartAt := commit.Add(3 * grace)
	e.loadNow = func() time.Time { return restartAt }
	e.restart(true, true, false)
	if n := e.compactSegmentObjects(markers, 2*grace, restartAt); n == 0 {
		t.Fatal("nothing merged")
	}
	from, to := rwWindow()
	hits := e.levelHits(from, to)
	if hits["A"] != 2 {
		t.Errorf("committed segment A: %d rows, want exactly 2 (the record of A survives the crash)", hits["A"])
	}
	switch {
	case hits["B"] == 2:
		t.Fatal("#406 fixed: flip this assertion to exact counts (A=2, B=2, 4 rows) and drop this test's known-gap framing")
	case hits["B"] != 4:
		t.Errorf("segment B: %d rows, want the known duplicate (4 = 2 from the buffer + 2 from the compacted object)", hits["B"])
	}
	if got := e.run(context.Background(), "*", from, to); got != 6 {
		t.Errorf("rows served = %d, want 6 (4 acknowledged + 2 duplicated, #406)", got)
	}
	// Once the segment is drained again the duplicate is gone.
	e.flush()
	e.reap()
	e.check("after the re-drain and the grace")
}
