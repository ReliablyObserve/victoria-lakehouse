package parquets3

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// #379: the commit time of every committed segment survives a restart, so the
// grace (and with it the time the buffer serves the segment's rows) counts from
// the real commit and always ends before the compaction guard (2 x grace)
// releases the segment's objects.

const restoreGrace = time.Minute // segEnv flushers use this grace

// releaseRestored is the pod's first successful S3 manifest refresh, at now: it
// releases the committed segments the flusher restored at load.
func releaseRestored(f *BufferFlusher, now time.Time) {
	f.clock = func() time.Time { return now }
	f.ManifestRefreshed(time.Now())
}

// committedLive is how many committed segments the buffer still serves.
func committedLive(e *segEnv, now time.Time) int { return e.segs.Stats(now).Committed }

// commitTwo ingests and commits two segments and returns the time of the
// commit (both within a few ms of it).
func commitTwo(t *testing.T, e *segEnv, f *BufferFlusher) time.Time {
	t.Helper()
	hour := time.Now().Add(-time.Hour)
	e.ingest(segTenantA, hour, 5)
	e.seal()
	e.ingest(segTenantA, hour.Add(time.Minute), 5)
	e.seal()
	e.drainAll(f)
	if n := len(e.segs.Pending()); n != 0 {
		t.Fatalf("%d segments still pending", n)
	}
	return time.Now()
}

func TestBufferFlusher_CommitTimesPersistedAndRestored(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusher(1000)
	commit := commitTwo(t, e, f)

	st, err := readFlushState(f.statePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Commits) != 2 {
		t.Fatalf("state records %d commits, want 2: %+v", len(st.Commits), st.Commits)
	}
	for _, c := range st.Commits {
		if d := commit.Sub(time.Unix(0, c.AtUnixNano)); d < 0 || d > 10*time.Second {
			t.Fatalf("commit record %+v is %v off the real commit", c, d)
		}
	}

	t.Run("within grace keeps only the remaining grace", func(t *testing.T) {
		e := newSegEnv(t)
		commit := commitTwo(t, e, e.flusher(1000))
		e.restart()
		f2 := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: time.Hour, Grace: restoreGrace})
		if err := f2.load(commit.Add(restoreGrace / 2)); err != nil {
			t.Fatal(err)
		}
		if got := committedLive(e, commit); got != 2 {
			t.Fatalf("%d committed segments live at restart within grace, want 2", got)
		}
		// Grace counts from the commit: gone at commit+grace, not restart+grace.
		if n := e.segs.Reap(commit.Add(restoreGrace+time.Second), restoreGrace); n != 2 {
			t.Fatalf("Reap at commit+grace removed %d, want 2", n)
		}
	})

	t.Run("expired at startup is reaped by the first refresh, not the load", func(t *testing.T) {
		e := newSegEnv(t)
		commit := commitTwo(t, e, e.flusher(1000))
		e.restart()
		f2 := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: time.Hour, Grace: restoreGrace})
		now := commit.Add(restoreGrace + time.Second)
		if err := f2.load(now); err != nil {
			t.Fatal(err)
		}
		if got := committedLive(e, commit); got != 2 {
			t.Fatalf("%d committed segments live after load, want 2: nothing is retired before the first refresh", got)
		}
		f2.clock = func() time.Time { return now }
		f2.tick(context.Background(), now)
		if got := committedLive(e, commit); got < 2 {
			t.Fatalf("%d committed segments live after a tick, want >= 2: the loop must not retire them before the first refresh", got)
		}
		f2.ManifestRefreshed(time.Now().Add(-time.Hour)) // began before this flusher: does not count
		if got := committedLive(e, commit); got < 2 {
			t.Fatalf("%d live after a refresh that began before the flusher, want >= 2", got)
		}
		releaseRestored(f2, now)
		if got := committedLive(e, commit); got != 0 {
			t.Fatalf("%d committed segments live after an expired restart, want 0", got)
		}
	})
}

// A restart during the load window: the state's main file is torn, the backup
// carries the same records.
func TestBufferFlusher_LoadUsesTheBackupsCommitTimes(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusher(1000)
	commit := commitTwo(t, e, f)
	if err := os.WriteFile(f.statePath, []byte(`{"version":5,"comm`), 0o600); err != nil {
		t.Fatal(err)
	}
	e.restart()
	f2 := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: time.Hour, Grace: restoreGrace})
	if err := f2.load(commit.Add(3 * restoreGrace)); err != nil {
		t.Fatal(err)
	}
	releaseRestored(f2, commit.Add(3*restoreGrace))
	if got := committedLive(e, commit); got != 0 {
		t.Fatalf("%d committed segments live; the backup's commit times must expire them", got)
	}
}

// A committed segment with no record, next to records, is older than the
// records' retention, so it is treated as long committed and removed by the
// first refresh.
func TestBufferFlusher_CommittedWithoutRecordIsExpired(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusher(1000)
	commit := commitTwo(t, e, f)
	st, _ := readFlushState(f.statePath)
	st.Commits = st.Commits[1:] // seq 1 loses its record, seq 2 keeps it
	if err := f.writeState(st); err != nil {
		t.Fatal(err)
	}
	e.restart()
	f2 := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: time.Hour, Grace: restoreGrace})
	if err := f2.load(commit); err != nil {
		t.Fatal(err)
	}
	releaseRestored(f2, commit)
	if got := committedLive(e, commit); got != 1 {
		t.Fatalf("%d committed segments live, want 1 (the record-less one is gone, the recorded one is within grace)", got)
	}
}

// A state written before the commit records existed has none: for that one
// start each committed segment gets its grace from the restart, as that release
// gave it, even after a long downtime (documented upgrade behaviour).
func TestBufferFlusher_StateWithoutRecordsKeepsMainBehaviour(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusher(1000)
	commit := commitTwo(t, e, f)
	st, _ := readFlushState(f.statePath)
	st.Commits = nil
	if err := f.writeState(st); err != nil {
		t.Fatal(err)
	}
	e.restart()
	f2 := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: time.Hour, Grace: restoreGrace})
	now := commit.Add(10 * restoreGrace)
	if err := f2.load(now); err != nil {
		t.Fatal(err)
	}
	releaseRestored(f2, now)
	if got := committedLive(e, now); got != 2 {
		t.Fatalf("%d live right after the upgrade restart, want 2", got)
	}
	if n := e.segs.Reap(now.Add(restoreGrace), restoreGrace); n != 2 {
		t.Fatalf("Reap at restart+grace removed %d, want 2", n)
	}
}

// A commit time after the restart's clock (the clock went back) is taken as now:
// the segment still ends its grace at most one grace after the restart.
func TestBufferFlusher_FutureCommitTimeIsClamped(t *testing.T) {
	e := newSegEnv(t)
	f := e.flusher(1000)
	commit := commitTwo(t, e, f)
	st, _ := readFlushState(f.statePath)
	for i := range st.Commits {
		st.Commits[i].AtUnixNano = commit.Add(time.Hour).UnixNano()
	}
	if err := f.writeState(st); err != nil {
		t.Fatal(err)
	}
	e.restart()
	f2 := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: time.Hour, Grace: restoreGrace})
	if err := f2.load(commit); err != nil {
		t.Fatal(err)
	}
	releaseRestored(f2, commit)
	if got := committedLive(e, commit); got != 2 {
		t.Fatalf("%d live, want 2 (within grace of now)", got)
	}
	if n := e.segs.Reap(commit.Add(restoreGrace), restoreGrace); n != 2 {
		t.Fatalf("Reap at now+grace removed %d, want 2 (a future record must not extend the grace)", n)
	}
}

func TestBufferFlusher_CommitRecordsArePruned(t *testing.T) {
	f := &BufferFlusher{grace: time.Minute}
	now := time.Now()
	f.state.Commits = []commitRecord{
		{Seq: 1, AtUnixNano: now.Add(-3 * time.Minute).UnixNano()},  // older than 2 graces
		{Seq: 2, AtUnixNano: now.Add(-90 * time.Second).UnixNano()}, // kept
		{Seq: 3, AtUnixNano: now.Add(-time.Second).UnixNano()},      // re-committed below
	}
	got := f.commitRecordsWith(3, now)
	if len(got) != 2 || got[0].Seq != 2 || got[1].Seq != 3 || got[1].AtUnixNano != now.UnixNano() {
		t.Fatalf("records = %+v, want seq 2 kept, seq 1 pruned, seq 3 replaced", got)
	}
}

// Property: for any commit and any downtime, the buffer serves a segment's rows
// exactly while now-commit < grace, whatever the restarts in between, so it is
// never served once the guard has released the objects (marker older than
// 2 x grace). The delete rewriter and compaction share that guard.
func TestBufferFlusher_Property_VisibleLifetimeBelowGuard(t *testing.T) {
	rng := rand.New(rand.NewSource(379))
	for i := 0; i < 20; i++ {
		down := time.Duration(rng.Int63n(int64(5 * restoreGrace)))
		e := newSegEnv(t)
		f := e.flusher(1000)
		commit := commitTwo(t, e, f)
		markers := map[string]time.Time{}
		var keys []string
		for _, k := range e.storedDataKeys() {
			if n := manifest.SegmentNonceOfKey(k); n != "" {
				markers[n] = commit
				keys = append(keys, k)
			}
		}
		if len(keys) == 0 {
			t.Fatal("no segment objects stored")
		}
		e.restart()
		f2 := newBufferFlusher(e.bw, e.segs, filepath.Join(e.dir, "buffer"), e.keep, BufferFlusherConfig{TargetBytes: 1000 * estBytesPerTraceRow, MaxAge: time.Hour, Grace: restoreGrace})
		now := commit.Add(down)
		if err := f2.load(now); err != nil {
			t.Fatal(err)
		}
		if got := committedLive(e, now); got != 2 {
			t.Fatalf("downtime %v: %d committed segments live before the first refresh, want 2", down, got)
		}
		releaseRestored(f2, now)
		live := committedLive(e, now)
		if want := down < restoreGrace; (live > 0) != want {
			t.Fatalf("downtime %v: %d committed segments live, want live=%v", down, live, want)
		}
		guard := &manifest.SegmentGuard{Markers: markers, Listed: true, Protect: 2 * restoreGrace}
		for _, k := range keys {
			if guard.Released(k, now) && live > 0 {
				t.Fatalf("downtime %v: the guard released %s but the buffer still serves its segment", down, k)
			}
		}
		e.segs.Close()
		e.segs = nil
	}
}
