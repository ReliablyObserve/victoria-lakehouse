package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// #379: a pod that was down for longer than the compaction guard (2 x grace)
// must not serve the rows of its committed segments from the buffer again,
// because compaction may have merged their objects meanwhile.

// compactSegmentObjects does what compaction does once the guard releases the
// segment objects at now: it merges every released segment object of every
// partition into compacted-L1 objects (no segment nonce in the key). It
// returns how many input objects it merged.
func (e *restartEnv) compactSegmentObjects(markers map[string]time.Time, protect time.Duration, now time.Time) int {
	e.t.Helper()
	guard := &manifest.SegmentGuard{Markers: markers, Listed: true, Protect: protect}
	c := compaction.NewCompactor(compaction.CompactorConfig{
		Pool: e.s.pool, Manifest: e.s.manifest, Prefix: "logs/", Mode: config.ModeLogs,
		RowGroupSize: 1000, CompressionLevel: 3, CompactionConfig: config.CompactionConfig{},
	})
	byPart := map[string][]manifest.FileInfo{}
	e.s.manifest.RangePartitions(func(part string, files []manifest.FileInfo) bool {
		for _, f := range files {
			if manifest.SegmentNonceOfKey(f.Key) != "" {
				byPart[part] = append(byPart[part], f)
			}
		}
		return true
	})
	merged := 0
	for part, files := range byPart {
		files = guard.ReleasedFiles(files, now)
		if len(files) < 2 {
			continue
		}
		if _, err := c.Compact(context.Background(), part, files, 0); err != nil {
			e.t.Fatalf("compact %s: %v", part, err)
		}
		merged += len(files)
	}
	return merged
}

// segmentMarkers returns the commit time of every segment of the pod, as the
// object store would report it for the _segments/<nonce> markers.
func (e *restartEnv) segmentMarkers(commit time.Time) map[string]time.Time {
	m := map[string]time.Time{}
	for _, f := range e.objects() {
		if n := manifest.SegmentNonceOfKey(f.Key); n != "" {
			m[n] = commit
		}
	}
	return m
}

func TestBufferRestart_DowntimeBeyondGuardWithCompaction_CountsExact(t *testing.T) {
	const grace = time.Minute // restartEnv's flusher grace
	for _, down := range []struct {
		name string
		by   time.Duration // downtime after the commit
		// whether the segments are still within grace at the restart
		live bool
	}{
		{"down 3 graces: past the guard", 3 * grace, false},
		{"down just past the guard", 2*grace + time.Second, false},
		{"down just past the grace, inside the guard", grace + time.Second, false},
	} {
		t.Run(down.name, func(t *testing.T) {
			e := newRestartEnv(t)
			e.ingest("A", at(rwHour, 5*time.Minute), at(rwHour, 6*time.Minute), at(rwHour, 7*time.Minute))
			e.flush() // segment 1: committed
			e.ingest("B", at(rwHour, 20*time.Minute), at(rwHour, 21*time.Minute), at(rwHour, 22*time.Minute))
			e.flush() // segment 2: committed
			commit := time.Now()
			markers := e.segmentMarkers(commit)
			e.check("before the restart")

			// Crash/stop, stay down, restart: the clock at the restart is
			// commit + downtime.
			restartAt := commit.Add(down.by)
			e.loadNow = func() time.Time { return restartAt }
			e.restart(true, true, false)
			e.check("after the restart")

			// Compaction runs at the restart's clock; the guard releases the
			// segments' objects once their marker is older than 2 x grace.
			if n := e.compactSegmentObjects(markers, 2*grace, restartAt); n == 0 && down.by >= 2*grace {
				t.Fatal("compaction merged nothing: the guard did not release the objects")
			}
			e.check("after compaction")
			e.checkAllStages("after compaction")
		})
	}
}

// A segment still within its grace at the restart keeps the REMAINING grace
// (not a fresh one) and one past it is removed during the load.
func TestBufferRestart_LoadRestoresCommitTime(t *testing.T) {
	const grace = time.Minute
	e := newRestartEnv(t)
	e.ingest("A", at(rwHour, 5*time.Minute))
	e.flush()
	commit := time.Now()
	committed := func(e *restartEnv) int { return e.segs.Stats(time.Now()).Committed }

	e.loadNow = func() time.Time { return commit.Add(grace / 2) }
	e.restart(true, true, false)
	if got := committed(e); got != 1 {
		t.Fatalf("within grace at the restart: %d committed live segments, want 1", got)
	}
	// Remaining grace is grace/2: reaping at commit+grace retires it.
	if n := e.segs.Reap(commit.Add(grace+time.Second), grace); n != 1 {
		t.Fatalf("Reap at commit+grace removed %d, want 1 (the grace must count from the commit, not the restart)", n)
	}

	e2 := newRestartEnv(t)
	e2.ingest("A", at(rwHour, 5*time.Minute))
	e2.flush()
	commit2 := time.Now()
	e2.loadNow = func() time.Time { return commit2.Add(grace + time.Second) }
	e2.restart(true, true, false)
	if got := committed(e2); got != 0 {
		t.Fatalf("expired at the restart: %d committed live segments, want 0", got)
	}
}
