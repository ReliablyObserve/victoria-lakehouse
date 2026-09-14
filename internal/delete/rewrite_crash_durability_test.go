package delete

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// The crash matrix, with the DURABLE RECORD broken.
//
// The matrix in rewrite_crash_refresh_test.go kills a rewrite at every step
// while everything else works. These cells break the thing the restart reads:
// the tombstone record's durable copies. A rewrite may then be interrupted at a
// step whose record never reached S3 — the state in which deleting an object
// destroys the only copy of the rows it holds — and a restart may find no
// tombstone store at all because the restore's LIST failed.
//
// The rule under test in all of them: an object is deleted only on the strength
// of a record that a restart will read back, and nothing is inferred from a
// manifest that has not listed the bucket in this process.

// fastRestoreRetry shortens the startup restore's backoff for tests that make
// the restore fail on purpose.
func fastRestoreRetry(t *testing.T) {
	t.Helper()
	old := restoreBackoff
	restoreBackoff = time.Millisecond
	t.Cleanup(func() { restoreBackoff = old })
}

// listFailPool fails the LIST the tombstone restore starts with, while reads,
// writes and deletes keep working — what a node sees when the bucket policy or
// its credentials stop it listing the tombstone prefix.
type listFailPool struct {
	*mockS3Pool
	mu     sync.Mutex
	broken bool
	lists  int
}

func newListFailPool(inner *mockS3Pool) *listFailPool {
	return &listFailPool{mockS3Pool: inner, broken: true}
}

func (p *listFailPool) List(ctx context.Context, prefix string) ([]string, error) {
	p.mu.Lock()
	p.lists++
	broken := p.broken
	p.mu.Unlock()
	if broken {
		return nil, errInjected
	}
	return p.mockS3Pool.List(ctx, prefix)
}

func (p *listFailPool) heal() {
	p.mu.Lock()
	p.broken = false
	p.mu.Unlock()
}

func (p *listFailPool) listCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lists
}

// crashAtStep installs the one-shot crash hook the matrices share: it saves the
// manifest snapshot a crash at that instant would leave behind, then stops the
// rewrite. Returns the scheduler and a func reporting whether the step was hit.
func crashAtStep(t *testing.T, w *crashWorld, s *RewriteScheduler, step string) func() bool {
	t.Helper()
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
	return func() bool { return crashed }
}

// TestRewriteCrashMatrix_DurableRecordWritesFailing kills the rewrite at every
// step it can still reach while every tombstone record after `prepared` is
// rejected by S3, restarts in every restart mode with the records STILL being
// rejected, and only then lets S3 recover. No object may be deleted anywhere in
// that window, and the system must converge once the records land.
func TestRewriteCrashMatrix_DurableRecordWritesFailing(t *testing.T) {
	// The two steps that delete an object (stepNotified follows the durable
	// publish record, stepSupersededGone follows the delete itself) are
	// unreachable while the record is not durable — that is the safeguard, and
	// TestRewriteDurability_NoDeleteStepIsReachedWhileTheRecordIsNotDurable
	// asserts it directly.
	for _, step := range []string{stepIntentRecorded, stepUploaded, stepSwappedInMemory, stepPublishRecorded} {
		for _, mode := range []restartMode{restartSnapshotAtCrash, restartStaleSnapshot, restartDiskLost} {
			t.Run(fmt.Sprintf("crash after %s/%s", step, mode), func(t *testing.T) {
				w := newCrashWorld(t)
				fp := &failAfterPreparedPool{mockS3Pool: w.s3}
				w.usePersistPool(fp)

				s := w.scheduler(w.manifest)
				crashed := crashAtStep(t, w, s, step)
				s.RunOnce(context.Background())
				if !crashed() {
					t.Fatalf("the rewrite never reached step %s", step)
				}
				if !w.bucket.Has(crashSource) {
					t.Fatalf("%s was deleted although the record authorising it never reached S3", crashSource)
				}

				w.restart(mode)
				w.refresh()
				w.assertServing("restart + refresh, records still rejected")
				if !w.bucket.Has(crashSource) {
					t.Fatalf("%s was deleted by the restart's resolution while the records were still not durable", crashSource)
				}

				// Passes with the records still failing may retry anything they
				// like, as long as they destroy nothing.
				for pass := 1; pass <= 2; pass++ {
					w.scheduler(w.manifest).RunOnce(context.Background())
					w.refresh()
					stage := fmt.Sprintf("pass %d while the records are rejected", pass)
					w.assertServing(stage)
					if !w.bucket.Has(crashSource) {
						t.Fatalf("%s: %s was deleted with no durable record authorising it", stage, crashSource)
					}
					if w.store.Count() == 0 {
						t.Fatalf("%s: the tombstone retired with its rewrite unfinished", stage)
					}
				}

				fp.recover()
				w.converge("records durable again", func() *RewriteScheduler { return w.scheduler(w.manifest) })
			})
		}
	}
}

// TestRewriteDurability_NoDeleteStepIsReachedWhileTheRecordIsNotDurable is the
// direct form of the rule: with the `published` record failing, the rewrite
// stops before the hand-off and before the delete, every time.
func TestRewriteDurability_NoDeleteStepIsReachedWhileTheRecordIsNotDurable(t *testing.T) {
	w := newCrashWorld(t)
	fp := &failAfterPreparedPool{mockS3Pool: w.s3}
	w.usePersistPool(fp)

	s := w.scheduler(w.manifest)
	seen := map[string]bool{}
	s.crashAt = func(step string) bool {
		seen[step] = true
		return false
	}
	s.RunOnce(context.Background())

	if !seen[stepSwappedInMemory] {
		t.Fatalf("fixture: the rewrite never got as far as the manifest swap (saw %v)", seen)
	}
	for _, step := range []string{stepNotified, stepSupersededGone} {
		if seen[step] {
			t.Fatalf("the rewrite reached %s while its published record was not durable", step)
		}
	}
	if !w.bucket.Has(crashSource) {
		t.Fatalf("%s was deleted although its published record never reached S3", crashSource)
	}
	if fp.failures() == 0 {
		t.Fatal("fixture: no record write was rejected")
	}

	fp.recover()
	w.refresh()
	w.converge("records durable again", func() *RewriteScheduler { return w.scheduler(w.manifest) })
}

// TestRewriteDurability_AbandonedReplacementIsKeptUntilItsDiscardRecordIsDurable
// is the same rule for the other outcome. A publish that is refused abandons the
// replacement, and the record saying so is what a restart reads instead of
// adopting the object — so it is kept, retired, and deleted only once that
// record is durable.
func TestRewriteDurability_AbandonedReplacementIsKeptUntilItsDiscardRecordIsDurable(t *testing.T) {
	w := newCrashWorld(t)
	fp := &failAfterPreparedPool{mockS3Pool: w.s3}
	w.usePersistPool(fp)

	fm := wrapManifest(w.manifest)
	fm.failReplace = true // the publish is refused: the rewrite discards
	w.scheduler(fm).RunOnce(context.Background())

	ts, ok := w.store.Get(crashTombstone)
	if !ok {
		t.Fatal("the tombstone retired although its rewrite was abandoned")
	}
	sup, recorded := ts.Superseded[crashSource]
	if !recorded {
		t.Fatalf("no rewrite record survived the refused publish: the abandoned replacement was deleted and its record cleared "+
			"with nothing durable authorising either; bucket=%v", w.bucket.Keys())
	}
	if sup.State != SupersessionDiscarded {
		t.Fatalf("fixture: the rewrite did not take the discard path: %+v", ts.Superseded)
	}
	if !w.bucket.Has(sup.NewKey) {
		t.Fatalf("the abandoned replacement %s was deleted although the record authorising it never reached S3", sup.NewKey)
	}
	if _, retired := w.manifest.LookupRetired(sup.NewKey); !retired {
		t.Fatalf("the abandoned replacement %s is neither deleted nor retired: the next refresh would adopt it", sup.NewKey)
	}
	w.refresh()
	w.assertServing("publish refused, discard record not durable")

	fp.recover()
	w.converge("records durable again", func() *RewriteScheduler { return w.scheduler(w.manifest) })
	if w.bucket.Has(sup.NewKey) {
		t.Errorf("the abandoned replacement %s was never deleted after the records became durable", sup.NewKey)
	}
}

// TestRewriteCrashMatrix_PassBeforeTheFirstRefresh runs the ordering a restarted
// process really has: the rewrite scheduler's ticker can fire before the
// manifest refresh has listed the bucket even once. Every "the key is not in the
// manifest" it sees then is a statement about a snapshot — possibly an empty one
// — and must not be read as "the object is gone".
func TestRewriteCrashMatrix_PassBeforeTheFirstRefresh(t *testing.T) {
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
					crashed := crashAtStep(t, w, s, step)
					s.RunOnce(context.Background())
					if !crashed() {
						t.Fatalf("the rewrite never reached step %s", step)
					}

					w.restart(mode)
					// The scheduler ticks first; the refresh has not run.
					reapedBefore := reapedKeys(w)
					w.scheduler(w.manifest).RunOnce(context.Background())
					w.assertNothingRetiredBeforeTheFirstListing("pass before the first refresh", reapedBefore)

					w.refresh()
					// Rows only: a tombstone the early pass finished cannot
					// retire until a pass runs against a listed manifest, which
					// is the next one — converge asserts the full set there.
					w.assertRowsVisible("refresh after the early pass")
					w.converge("after the early pass", func() *RewriteScheduler { return w.scheduler(w.manifest) })
				})
			}
		}
	}
}

// reapedKeys is the tombstone's reaped set as it stands — the baseline the
// "did this pass infer anything from an unlisted manifest" check compares
// against. A key reaped by an earlier pass may legitimately still be in the
// bucket: the publish records the reap, the commit deletes the object, and a
// failed delete is retried.
func reapedKeys(w *crashWorld) map[string]bool {
	out := map[string]bool{}
	ts, ok := w.store.Get(crashTombstone)
	if !ok {
		return out
	}
	for k, v := range ts.Reaped {
		if v {
			out[k] = true
		}
	}
	return out
}

// assertNothingRetiredBeforeTheFirstListing is what must hold on a manifest that
// has not listed the bucket in this process: nothing has been inferred from an
// absent key. A pass may still rewrite a file the snapshot knows about — the
// object is there and holds rows — but it may not conclude from a missing entry
// that an object is gone, and it may not retire the tombstone.
func (w *crashWorld) assertNothingRetiredBeforeTheFirstListing(stage string, reapedBefore map[string]bool) {
	w.t.Helper()
	if w.manifest.Listed() {
		w.t.Fatalf("%s: fixture is wrong — the manifest has already applied a listing", stage)
	}
	if w.store.Count() == 0 {
		w.t.Fatalf("%s: the tombstone retired before this process had listed the bucket", stage)
	}
	ts, ok := w.store.Get(crashTombstone)
	if !ok {
		w.t.Fatalf("%s: the tombstone is gone from the store", stage)
	}
	for k, reaped := range ts.Reaped {
		if reaped && !reapedBefore[k] && w.bucket.Has(k) {
			w.t.Fatalf("%s: this pass recorded %s as gone while the bucket still holds it", stage, k)
		}
	}
}

// TestRewriteCrashMatrix_TombstoneRestoreListFailing restarts into a node whose
// tombstone restore cannot LIST S3. Its records may be missing or older than
// what S3 holds, so a pass must change nothing at all until the restore
// succeeds — and then everything must converge.
func TestRewriteCrashMatrix_TombstoneRestoreListFailing(t *testing.T) {
	fastRestoreRetry(t)
	for _, step := range publishPathSteps {
		for _, mode := range []restartMode{restartSnapshotAtCrash, restartStaleSnapshot} {
			t.Run(fmt.Sprintf("crash after %s/%s", step, mode), func(t *testing.T) {
				w := newCrashWorld(t)
				s := w.scheduler(w.manifest)
				crashed := crashAtStep(t, w, s, step)
				s.RunOnce(context.Background())
				if !crashed() {
					t.Fatalf("the rewrite never reached step %s", step)
				}

				lf := newListFailPool(w.s3)
				w.persistPool = lf
				w.allowRestoreFailure = true
				w.restart(mode)
				if w.restoreErr == nil {
					t.Fatal("fixture: the restore did not fail although every LIST was rejected")
				}
				if !w.store.S3RestorePending() {
					t.Fatal("a failed S3 restore must be remembered: the records may be stale")
				}

				before := bucketKeys(w)
				w.scheduler(w.manifest).RunOnce(context.Background())
				if after := bucketKeys(w); !sameKeys(before, after) {
					t.Fatalf("objects changed while the tombstone restore was pending:\n before=%v\n after =%v", before, after)
				}
				if w.store.Count() == 0 {
					t.Fatal("the tombstone retired while the store's S3 copy had not been read")
				}

				lf.heal()
				w.refresh()
				w.converge("restore recovered", func() *RewriteScheduler { return w.scheduler(w.manifest) })
				if lf.listCalls() < restoreAttempts {
					t.Fatalf("the restore was attempted %d times, want at least %d", lf.listCalls(), restoreAttempts)
				}
			})
		}
	}
}

// TestRewriteDurability_DiskLostAndRestoreListFailing is the worst restart: no
// local tombstone copy AND no readable S3 copy. The node knows about no delete
// at all, so it must touch nothing — and pick everything up once the LIST works.
func TestRewriteDurability_DiskLostAndRestoreListFailing(t *testing.T) {
	fastRestoreRetry(t)
	w := newCrashWorld(t)
	s := w.scheduler(w.manifest)
	crashed := crashAtStep(t, w, s, stepUploaded)
	s.RunOnce(context.Background())
	if !crashed() {
		t.Fatalf("the rewrite never reached step %s", stepUploaded)
	}

	lf := newListFailPool(w.s3)
	w.persistPool = lf
	w.allowRestoreFailure = true
	w.restart(restartDiskLost)
	if w.store.Count() != 0 {
		t.Fatalf("fixture: the store restored %d tombstones although every LIST failed", w.store.Count())
	}
	if !w.store.S3RestorePending() {
		t.Fatal("a failed S3 restore must be remembered")
	}

	before := bucketKeys(w)
	w.refresh()
	w.scheduler(w.manifest).RunOnce(context.Background())
	if after := bucketKeys(w); !sameKeys(before, after) {
		t.Fatalf("objects changed while the node had no readable tombstone store:\n before=%v\n after =%v", before, after)
	}

	// The LIST works again: the next pass restores the store, resolves the
	// interrupted rewrite and takes the delete from there.
	lf.heal()
	w.scheduler(w.manifest).RunOnce(context.Background())
	if w.store.S3RestorePending() {
		t.Fatal("the store's S3 copy was still not read after the LIST recovered")
	}
	w.refresh()
	w.converge("restore recovered", func() *RewriteScheduler { return w.scheduler(w.manifest) })
}

func bucketKeys(w *crashWorld) []string {
	keys := append([]string(nil), w.bucket.Keys()...)
	sort.Strings(keys)
	return keys
}

func sameKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
