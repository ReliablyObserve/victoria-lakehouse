package delete

import (
	"context"
	"strings"
	"sync"
	"testing"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// A rewrite's durable record is what a restart resolves an interrupted rewrite
// from, so no object may be deleted on the strength of a record that has not
// reached durable storage. And a scheduler pass that runs before the manifest
// has listed the bucket in this process cannot tell a deleted file from one the
// manifest has not adopted yet.

// failAfterPreparedPool lets the tombstone's `prepared` record reach S3 and
// fails every later write until recovered — PutObject on _tombstones/ failing
// while DeleteObject on data keeps working.
type failAfterPreparedPool struct {
	*mockS3Pool
	mu        sync.Mutex
	armed     bool
	recovered bool
	failed    int
}

func (p *failAfterPreparedPool) Upload(ctx context.Context, key string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.recovered {
		if strings.Contains(string(data), `"State":"prepared"`) {
			p.armed = true
			return p.mockS3Pool.Upload(ctx, key, data)
		}
		if p.armed {
			p.failed++
			return errInjected
		}
	}
	return p.mockS3Pool.Upload(ctx, key, data)
}

func (p *failAfterPreparedPool) recover() {
	p.mu.Lock()
	p.recovered = true
	p.mu.Unlock()
}

func (p *failAfterPreparedPool) failures() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failed
}

// TestRewriteDurability_SourceSurvivesAPublishedRecordThatNeverReachedS3: the
// published record cannot be written, so the superseded object must not be
// deleted. Then the process dies and restarts from what survived — the S3 copy
// still says `prepared` — and every kept row must still be visible exactly once.
func TestRewriteDurability_SourceSurvivesAPublishedRecordThatNeverReachedS3(t *testing.T) {
	for _, mode := range []restartMode{restartDiskLost, restartSnapshotAtCrash, restartStaleSnapshot} {
		t.Run(string(mode), func(t *testing.T) {
			w := newCrashWorld(t)
			fp := &failAfterPreparedPool{mockS3Pool: w.s3}
			st := NewTombstoneStore()
			st.EnablePersistence(PersistenceConfig{Pool: fp, Prefix: "logs/"})
			orig, _ := w.store.Get(crashTombstone)
			st.Add(orig)
			w.store = st

			w.scheduler(w.manifest).RunOnce(context.Background())
			if fp.failures() == 0 {
				t.Fatal("fixture: no write after the prepared record was failed")
			}
			if !w.bucket.Has(crashSource) {
				t.Fatalf("the superseded object %s was deleted although the record authorising it never reached S3", crashSource)
			}

			// The process dies here.
			if err := w.manifest.SaveTo(w.crashAt); err != nil {
				t.Fatalf("crash snapshot: %v", err)
			}
			w.restart(mode)
			w.refresh()
			w.assertServing("restart + refresh")

			// S3 recovers; the system converges.
			fp.recover()
			w.converge("after S3 recovers", func() *RewriteScheduler { return w.scheduler(w.manifest) })
		})
	}
}

// TestRewriteDurability_LiveProcessWaitsForTheRecordThenFinishes: without a
// crash, a rewrite whose published record is not durable holds, keeps serving
// correctly, and finishes on a later pass once S3 accepts the record.
func TestRewriteDurability_LiveProcessWaitsForTheRecordThenFinishes(t *testing.T) {
	w := newCrashWorld(t)
	fp := &failAfterPreparedPool{mockS3Pool: w.s3}
	st := NewTombstoneStore()
	st.EnablePersistence(PersistenceConfig{Pool: fp, Prefix: "logs/"})
	orig, _ := w.store.Get(crashTombstone)
	st.Add(orig)
	w.store = st

	s := w.scheduler(w.manifest)
	for pass := 0; pass < 3; pass++ {
		s.RunOnce(context.Background())
		w.refresh()
		w.assertServing("pass while S3 rejects the record")
		if !w.bucket.Has(crashSource) {
			t.Fatal("the superseded object was deleted while its published record was not durable")
		}
		if w.store.Count() == 0 {
			t.Fatal("the tombstone retired while its rewrite was unfinished")
		}
	}
	fp.recover()
	w.converge("S3 recovered", func() *RewriteScheduler { return s })
}

// TestRewriteScheduler_PassBeforeTheFirstRefreshDoesNotRetire: a restarted
// process whose manifest has not listed the bucket yet (disk lost, or a snapshot
// older than the tombstone's file) must not read "not in the manifest" as "the
// object is gone".
func TestRewriteScheduler_PassBeforeTheFirstRefreshDoesNotRetire(t *testing.T) {
	w := newCrashWorld(t)
	w.manifest = lhmanifest.New("test-bucket", "")
	w.scheduler(w.manifest).RunOnce(context.Background())
	if w.store.Count() == 0 {
		t.Fatalf("tombstone retired while %s still exists and holds its rows (the manifest had not listed the bucket)", crashSource)
	}
	if ts, _ := w.store.Get(crashTombstone); ts.Reaped[crashSource] {
		t.Fatalf("%s was recorded as gone on the strength of an empty, never-listed manifest", crashSource)
	}
	w.refresh()
	w.assertServing("first refresh after the early pass")
	w.converge("after the refresh", func() *RewriteScheduler { return w.scheduler(w.manifest) })
}
