package delete

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil/storageinvariants"
)

// The crash matrix, with the manifest refresh in it.
//
// A rewrite is killed at every step. The restarted process gets what really
// survives a crash — the bucket, the tombstone store's disk and S3 copies, and
// a manifest snapshot that may be as fresh as the crash, as old as the rewrite,
// or missing along with the disk — resolves the interrupted rewrite, and then
// runs the periodic manifest refresh against the bucket BEFORE the scheduler
// gets another turn. The full invariant set is asserted right there and after
// every later pass (with its own refresh), and the system must converge: every
// object manifested or deleted, the tombstone retired, every kept row visible
// exactly once and no deleted row visible at any point.

var crashHourStart = time.Date(2026, 3, 1, 7, 0, 0, 0, time.UTC)

const (
	crashPartition = "dt=2026-03-01/hour=07"
	crashSource    = "logs/dt=2026-03-01/hour=07/src-0001.parquet"
	crashOther     = "logs/dt=2026-03-01/hour=07/other-0002.parquet"
	crashTombstone = "ts-crash"
)

type restartMode string

const (
	// restartSnapshotAtCrash: the manifest snapshot was written at the instant
	// of the crash — everything the manifest knew is on disk.
	restartSnapshotAtCrash restartMode = "snapshot taken at the crash"
	// restartStaleSnapshot: the last snapshot predates the rewrite.
	restartStaleSnapshot restartMode = "snapshot older than the rewrite"
	// restartDiskLost: no snapshot and no local tombstone copy; the node
	// restores tombstones from S3 and builds its manifest from the listing.
	restartDiskLost restartMode = "disk lost"
)

type crashWorld struct {
	t        *testing.T
	bucket   *mockRewriterPool
	s3       *mockS3Pool
	diskDir  string
	staleAt  string
	crashAt  string
	manifest *lhmanifest.Manifest
	store    *TombstoneStore
	kept     map[string]bool
	deleted  map[string]bool
}

func newCrashWorld(t *testing.T) *crashWorld {
	t.Helper()
	dir := t.TempDir()
	w := &crashWorld{
		t:       t,
		bucket:  newMockRewriterPool(),
		s3:      newMockS3Pool(),
		diskDir: filepath.Join(dir, "tombstones"),
		staleAt: filepath.Join(dir, "manifest-stale.bin"),
		crashAt: filepath.Join(dir, "manifest-crash.bin"),
		kept:    map[string]bool{},
		deleted: map[string]bool{},
	}
	at := func(sec int) int64 { return crashHourStart.Add(time.Duration(sec) * time.Second).UnixNano() }
	src := []schema.LogRow{
		{TimestampUnixNano: at(1), Body: "keep-a", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: at(2), Body: "drop-a", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: at(3), Body: "keep-b", SeverityText: "warn", ServiceName: "api"},
		{TimestampUnixNano: at(4), Body: "drop-b", SeverityText: "error", ServiceName: "api"},
		{TimestampUnixNano: at(5), Body: "keep-c", SeverityText: "info", ServiceName: "web"},
	}
	other := []schema.LogRow{
		{TimestampUnixNano: at(10), Body: "other-a", SeverityText: "info", ServiceName: "db"},
		{TimestampUnixNano: at(11), Body: "other-b", SeverityText: "info", ServiceName: "db"},
	}
	w.manifest = lhmanifest.New("test-bucket", "")
	for key, rows := range map[string][]schema.LogRow{crashSource: src, crashOther: other} {
		data := buildTestParquet(t, rows)
		w.bucket.Put(key, data)
		minNs, maxNs := schema.LogRowTimeBounds(rows)
		w.manifest.AddFile(crashPartition, lhmanifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: int64(len(rows)), MinTimeNs: minNs, MaxTimeNs: maxNs,
			Labels: schema.ExtractLogLabels(rows),
		})
		for _, r := range rows {
			if r.SeverityText == "error" {
				w.deleted[r.Body] = true
			} else {
				w.kept[r.Body] = true
			}
		}
	}
	if err := w.manifest.SaveTo(w.staleAt); err != nil {
		t.Fatalf("stale snapshot: %v", err)
	}

	w.store = NewTombstoneStore()
	w.store.EnablePersistence(w.persistence(w.diskDir))
	w.store.Add(Tombstone{
		ID: crashTombstone, Query: `severity_text:="error"`,
		StartNs: crashHourStart.UnixNano(), EndNs: crashHourStart.Add(time.Hour).UnixNano() - 1,
		AffectedKeys: []string{crashSource}, CreatedAt: time.Now().Add(-2 * time.Hour),
		Mode: "permanent", Reaped: map[string]bool{},
	})
	return w
}

func (w *crashWorld) persistence(dir string) PersistenceConfig {
	return PersistenceConfig{Dir: dir, Pool: w.s3, Prefix: "logs/"}
}

func (w *crashWorld) scheduler(m ManifestUpdater) *RewriteScheduler {
	return NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          w.store,
		Rewriter:       NewRewriter(w.bucket, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       m,
	})
}

// restart replaces the process: a new manifest and store built only from what
// a crash leaves behind, then the startup resolution of interrupted rewrites.
func (w *crashWorld) restart(mode restartMode) {
	w.t.Helper()
	m := lhmanifest.New("test-bucket", "")
	tombstoneDir := w.diskDir
	switch mode {
	case restartSnapshotAtCrash:
		if err := m.LoadFrom(w.crashAt); err != nil {
			w.t.Fatalf("load crash snapshot: %v", err)
		}
	case restartStaleSnapshot:
		if err := m.LoadFrom(w.staleAt); err != nil {
			w.t.Fatalf("load stale snapshot: %v", err)
		}
	case restartDiskLost:
		tombstoneDir = filepath.Join(w.t.TempDir(), "fresh-disk")
	}
	store := NewTombstoneStore()
	if _, err := store.Restore(context.Background(), w.persistence(tombstoneDir)); err != nil {
		w.t.Fatalf("restore tombstones: %v", err)
	}
	store.EnablePersistence(w.persistence(tombstoneDir))
	w.manifest, w.store = m, store
	ResolveInterruptedRewrites(store, m)
}

func (w *crashWorld) refresh() { simulateManifestRefresh(w.t, w.manifest, w.bucket) }

// assertServing is the set that must hold at every observable moment: queries
// see each kept row once and no deleted row, every manifested entry has its
// object and describes it truthfully, and every object outside the manifest is
// one the manifest knows it is waiting to delete.
func (w *crashWorld) assertServing(stage string) {
	w.t.Helper()
	got := visibleBodies(w.t, w.manifest, w.bucket, w.store)
	for body := range w.kept {
		if got[body] != 1 {
			w.t.Fatalf("%s: kept row %q is visible %d times", stage, body, got[body])
		}
	}
	for body := range w.deleted {
		if got[body] > 0 {
			w.t.Fatalf("%s: deleted row %q is visible", stage, body)
		}
	}
	for _, files := range w.manifest.AllFiles() {
		for _, fi := range files {
			if fi.RowCount == 0 {
				continue // adopted from a listing: row count not known yet
			}
			if n := int64(countLogRows(w.t, mustGet(w.t, w.bucket, fi.Key))); n != fi.RowCount {
				w.t.Fatalf("%s: %s is registered with %d rows but holds %d", stage, fi.Key, fi.RowCount, n)
			}
		}
	}
	storageinvariants.Assert(w.t, stage, storageinvariants.State{
		Manifest: w.manifest, Bucket: w.bucket, Tombstones: tombstoneViews(w.store),
		AwaitingDeletion: storageinvariants.AwaitingDeletionIn(w.manifest),
	})
}

// assertConverged is rest: nothing awaits deletion, nothing is unmanifested,
// the tombstone is retired and the rows are exactly the kept ones.
func (w *crashWorld) assertConverged(stage string) {
	w.t.Helper()
	w.assertServing(stage)
	storageinvariants.Assert(w.t, stage+" (strict)", storageinvariants.State{
		Manifest: w.manifest, Bucket: w.bucket, Tombstones: tombstoneViews(w.store),
	})
	if n := w.store.Count(); n != 0 {
		ts, _ := w.store.Get(crashTombstone)
		w.t.Fatalf("%s: the tombstone must have retired: %+v", stage, ts)
	}
	if rk := w.manifest.RetiredKeys(); len(rk) != 0 {
		w.t.Fatalf("%s: retired keys left over: %+v", stage, rk)
	}
	var scanned int
	for _, r := range scanLogRows(w.t, w.bucket) {
		if w.deleted[r.Body] {
			w.t.Fatalf("%s: deleted row %q is still stored", stage, r.Body)
		}
		scanned++
	}
	if scanned != len(w.kept) {
		w.t.Fatalf("%s: the bucket holds %d rows, want the %d kept", stage, scanned, len(w.kept))
	}
}

// converge runs scheduler passes, each followed by a refresh and the serving
// checks, until the tombstone retires.
func (w *crashWorld) converge(stage string, sched func() *RewriteScheduler) {
	w.t.Helper()
	for pass := 1; pass <= 5 && w.store.Count() > 0; pass++ {
		sched().RunOnce(context.Background())
		w.refresh()
		w.assertServing(fmt.Sprintf("%s: pass %d + refresh", stage, pass))
	}
	w.assertConverged(stage + ": converged")
}

var publishPathSteps = []string{
	stepIntentRecorded, stepUploaded, stepSwappedInMemory, stepPublishRecorded, stepNotified, stepSupersededGone,
}

var discardPathSteps = []string{stepIntentRecorded, stepUploaded, stepDiscardRecorded, stepAbandonedDeleted}

// TestRewriteCrashMatrix_WithManifestRefresh kills the rewrite at every step of
// the publish path and of the discard path (a publish that fails), restarts in
// every restart mode, and refreshes before anything else runs.
func TestRewriteCrashMatrix_WithManifestRefresh(t *testing.T) {
	type path struct {
		name    string
		steps   []string
		failPub bool
	}
	for _, p := range []path{{"publish", publishPathSteps, false}, {"discard", discardPathSteps, true}} {
		for _, step := range p.steps {
			for _, mode := range []restartMode{restartSnapshotAtCrash, restartStaleSnapshot, restartDiskLost} {
				t.Run(fmt.Sprintf("%s/crash after %s/%s", p.name, step, mode), func(t *testing.T) {
					w := newCrashWorld(t)
					var m ManifestUpdater = w.manifest
					if p.failPub {
						fm := wrapManifest(w.manifest)
						fm.failReplace = true
						m = fm
					}
					s := w.scheduler(m)
					crashed := false
					s.crashAt = func(st string) bool {
						if st != step || crashed {
							return false
						}
						crashed = true
						if err := w.manifest.SaveTo(w.crashAt); err != nil {
							t.Fatalf("crash snapshot: %v", err)
						}
						return true
					}
					s.RunOnce(context.Background())
					if !crashed {
						t.Fatalf("the rewrite never reached step %s", step)
					}

					w.restart(mode)
					w.refresh()
					w.assertServing("restart + refresh")
					w.converge("after restart", func() *RewriteScheduler { return w.scheduler(w.manifest) })
				})
			}
		}
	}
}

// TestRewriteRefreshAtEveryStep runs the refresh inside the live process at
// every step of an uninterrupted rewrite — the refresh ticker firing mid-rewrite
// — for both the publish path and a publish that fails.
func TestRewriteRefreshAtEveryStep(t *testing.T) {
	for _, failPub := range []bool{false, true} {
		t.Run(fmt.Sprintf("publish fails=%v", failPub), func(t *testing.T) {
			w := newCrashWorld(t)
			var m ManifestUpdater = w.manifest
			if failPub {
				fm := wrapManifest(w.manifest)
				fm.failReplace = true
				m = fm
			}
			s := w.scheduler(m)
			seen := map[string]bool{}
			s.crashAt = func(step string) bool {
				seen[step] = true
				w.refresh()
				w.assertServing("refresh at " + step)
				return false
			}
			s.RunOnce(context.Background())
			want := publishPathSteps
			if failPub {
				want = discardPathSteps
			}
			for _, st := range want {
				if !seen[st] {
					t.Fatalf("the rewrite never reached step %s (saw %v)", st, seen)
				}
			}
			w.refresh()
			w.assertServing("after the pass")
			w.converge("refresh at every step", func() *RewriteScheduler { return w.scheduler(w.manifest) })
		})
	}
}
