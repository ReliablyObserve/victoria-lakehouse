package delete

import (
	"context"
	"errors"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Unit-level cover for the durability rules the crash matrices exercise end to
// end: which target decides that a change is durable, what each step does when
// it is not, and what the retry loops do when there is nothing to retry.

// TestEnsureDurable_TheTargetIsTheOneARestoreReads: with S3 configured it is S3
// that decides (a node that starts without this disk reads only that); with only
// a disk it is the disk; with neither there is nothing that could contradict the
// caller, so the answer is yes.
func TestEnsureDurable_TheTargetIsTheOneARestoreReads(t *testing.T) {
	ctx := context.Background()

	t.Run("no durable target", func(t *testing.T) {
		store := NewTombstoneStore()
		store.Add(sampleTombstone("ts-none"))
		if err := store.EnsureDurable(ctx, "ts-none"); err != nil {
			t.Fatalf("EnsureDurable with no target = %v, want nil", err)
		}
	})

	t.Run("disk only", func(t *testing.T) {
		store := NewTombstoneStore()
		store.EnablePersistence(PersistenceConfig{Dir: t.TempDir()})
		store.Add(sampleTombstone("ts-disk"))
		if err := store.EnsureDurable(ctx, "ts-disk"); err != nil {
			t.Fatalf("EnsureDurable with a working disk = %v, want nil", err)
		}
	})

	t.Run("s3 configured and failing", func(t *testing.T) {
		store := NewTombstoneStore()
		store.EnablePersistence(PersistenceConfig{Dir: t.TempDir(), Pool: uploadFailPool{newMockS3Pool()}, Prefix: "logs/"})
		store.Add(sampleTombstone("ts-s3"))
		// The disk copy landed, but S3 is the target a restore reads.
		if err := store.EnsureDurable(ctx, "ts-s3"); !errors.Is(err, ErrNotDurable) {
			t.Fatalf("EnsureDurable with S3 failing = %v, want ErrNotDurable", err)
		}
	})

	t.Run("unknown tombstone", func(t *testing.T) {
		store := NewTombstoneStore()
		store.EnablePersistence(PersistenceConfig{Dir: t.TempDir()})
		if err := store.EnsureDurable(ctx, "never-added"); !errors.Is(err, ErrTombstoneNotFound) {
			t.Fatalf("EnsureDurable for an unknown id = %v, want ErrTombstoneNotFound", err)
		}
	})
}

// uploadFailPool rejects every upload; everything else works.
type uploadFailPool struct{ *mockS3Pool }

func (p uploadFailPool) Upload(context.Context, string, []byte) error { return errInjected }

// TestRetryS3Restore_WithNothingToRetry covers the two short-circuits: a store
// that has read S3 reports done, and one with no pool configured cannot retry.
func TestRetryS3Restore_WithNothingToRetry(t *testing.T) {
	healthy := NewTombstoneStore()
	if !healthy.RetryS3Restore(context.Background()) {
		t.Error("a store with nothing pending must report done")
	}

	fastRestoreRetry(t)
	stuck := NewTombstoneStore()
	if _, err := stuck.Restore(context.Background(), PersistenceConfig{Pool: newListFailPool(newMockS3Pool()), Prefix: "logs/"}); err == nil {
		t.Fatal("fixture: the restore did not fail")
	}
	// Losing the pool (a store rebuilt without one) leaves nothing to retry.
	stuck.mu.Lock()
	stuck.restoreCfg = PersistenceConfig{}
	stuck.mu.Unlock()
	if stuck.RetryS3Restore(context.Background()) {
		t.Error("a pending restore with no pool cannot report success")
	}
	// And the loop returns rather than spinning on it.
	done := make(chan struct{})
	go func() {
		stuck.RunRestoreRetry(context.Background(), 0) // 0 takes the default interval
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunRestoreRetry did not return when there was no pool to retry against")
	}
}

// TestRunRestoreRetry_ReturnsWhenNothingIsPending: a healthy store's loop is a
// no-op, so the binaries can start it unconditionally.
func TestRunRestoreRetry_ReturnsWhenNothingIsPending(t *testing.T) {
	store := NewTombstoneStore()
	done := make(chan struct{})
	go func() {
		store.RunRestoreRetry(context.Background(), time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunRestoreRetry blocked although nothing was pending")
	}
}

// commitFixture is one published rewrite whose record can be made non-durable
// on demand, for the steps that must refuse to delete on it.
type commitFixture struct {
	store    *TombstoneStore
	manifest *lhmanifest.Manifest
	sched    *RewriteScheduler
	pool     *mockRewriterPool
	source   string
	newKey   string
}

func newCommitFixture(t *testing.T) *commitFixture {
	t.Helper()
	const source = "logs/dt=2026-03-01/hour=07/src-0001.parquet"
	const newKey = "logs/dt=2026-03-01/hour=07/b2709b0d.parquet"

	pool := newMockRewriterPool()
	pool.Put(source, buildTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep-a", SeverityText: "info", ServiceName: "web"},
	}))
	pool.Put(newKey, buildTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep-a", SeverityText: "info", ServiceName: "web"},
	}))
	m := newTestManifest(t, map[string]int64{newKey: 1})

	store := NewTombstoneStore()
	store.EnablePersistence(PersistenceConfig{Dir: t.TempDir(), Pool: uploadFailPool{newMockS3Pool()}, Prefix: "logs/"})
	store.Add(Tombstone{
		ID: "ts-commit", Query: `severity_text:="error"`, StartNs: 0, EndNs: 1 << 40,
		AffectedKeys: []string{source}, CreatedAt: time.Now().Add(-2 * time.Hour),
		Mode: "permanent", Reaped: map[string]bool{},
	})
	recordPublished(store, "ts-commit", source, newKey)

	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       NewRewriter(pool, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       m,
	})
	return &commitFixture{store: store, manifest: m, sched: sched, pool: pool, source: source, newKey: newKey}
}

// TestCommit_RefusesWhileTheRecordIsNotDurable is the guard in isolation: the
// superseded object survives and the replacement is held, so an undo is still
// possible and no compaction can merge it in the meantime.
func TestCommit_RefusesWhileTheRecordIsNotDurable(t *testing.T) {
	f := newCommitFixture(t)

	if f.sched.commit(context.Background(), "ts-commit", f.source, f.newKey) {
		t.Fatal("commit must refuse while the record authorising the delete is not durable")
	}
	if !f.pool.Has(f.source) {
		t.Fatalf("the superseded object %s was deleted anyway", f.source)
	}
	if !f.manifest.IsHeld(f.newKey) {
		t.Errorf("the replacement %s must be held while an undo is still possible", f.newKey)
	}
	ts, ok := f.store.Get("ts-commit")
	if !ok || len(ts.Superseded) != 1 {
		t.Fatalf("the rewrite record must survive for the next pass: %+v", ts.Superseded)
	}
}

// TestDiscard_DoesNothingWithoutItsOwnRecord: discard acts only on the record it
// was called for, so a stale call cannot delete the live replacement of a
// rewrite that has published since.
func TestDiscard_DoesNothingWithoutItsOwnRecord(t *testing.T) {
	f := newCommitFixture(t)
	ctx := context.Background()

	// No record for this source at all.
	if f.sched.discard(ctx, "ts-commit", "logs/dt=2026-03-01/hour=07/other.parquet", "x") {
		t.Error("discard acted on a source it holds no record for")
	}
	// A record naming a different replacement.
	if f.sched.discard(ctx, "ts-commit", f.source, "logs/dt=2026-03-01/hour=07/someone-elses.parquet") {
		t.Error("discard acted on a record naming another replacement")
	}
	// The record says published: its replacement is the live copy of the rows.
	if f.sched.discard(ctx, "ts-commit", f.source, f.newKey) {
		t.Error("discard acted on a PUBLISHED record — that deletes the live replacement")
	}
	if !f.pool.Has(f.newKey) {
		t.Fatalf("the live replacement %s was deleted", f.newKey)
	}
	if !f.manifest.HasKey(f.newKey) {
		t.Fatalf("the manifest stopped serving the live replacement %s", f.newKey)
	}
}

// TestRewriteOne_RefusesToUploadWhileTheIntentIsNotDurable: the same rule at the
// first step — nothing is written before the record naming the replacement key
// can be read back, and the claim on that key is given back.
func TestRewriteOne_RefusesToUploadWhileTheIntentIsNotDurable(t *testing.T) {
	const source = "logs/dt=2026-03-01/hour=07/src-0001.parquet"
	pool := newMockRewriterPool()
	pool.Put(source, buildTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep-a", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "drop-a", SeverityText: "error", ServiceName: "web"},
	}))
	m := newTestManifest(t, map[string]int64{source: 2})

	store := NewTombstoneStore()
	store.EnablePersistence(PersistenceConfig{Dir: t.TempDir(), Pool: uploadFailPool{newMockS3Pool()}, Prefix: "logs/"})
	store.Add(Tombstone{
		ID: "ts-intent", Query: `severity_text:="error"`, StartNs: 0, EndNs: 1 << 40,
		AffectedKeys: []string{source}, CreatedAt: time.Now().Add(-2 * time.Hour),
		Mode: "permanent", Reaped: map[string]bool{},
	})

	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       NewRewriter(pool, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       m,
	})
	if results := sched.RunOnce(context.Background()); len(results) != 0 {
		t.Fatalf("the rewrite ran with no durable record: %+v", results)
	}
	if !pool.Has(source) {
		t.Fatalf("the source %s was deleted", source)
	}
	if keys := pool.Keys(); len(keys) != 1 {
		t.Fatalf("an object was written before the record naming it was durable: %v", keys)
	}
	ts, ok := store.Get("ts-intent")
	if !ok {
		t.Fatal("the tombstone retired although nothing was rewritten")
	}
	if len(ts.Superseded) != 0 {
		t.Errorf("a record was left for a rewrite that never started: %+v", ts.Superseded)
	}
	if len(m.PendingKeys()) != 0 {
		t.Errorf("the replacement key claim was not given back: %+v", m.PendingKeys())
	}
}

// TestPublishRewrite_RefusesAKeyTheManifestAlreadyServes covers the last-resort
// check inside the publish: the claim makes it unreachable within one process,
// but if it ever fires the replacement must NOT be deleted — the object under
// that key belongs to the other file.
func TestPublishRewrite_RefusesAKeyTheManifestAlreadyServes(t *testing.T) {
	const source = "logs/dt=2026-03-01/hour=07/src-0001.parquet"
	const taken = "logs/dt=2026-03-01/hour=07/deadbeef.parquet"
	m := newTestManifest(t, map[string]int64{source: 2, taken: 5})

	_, err := publishRewrite(m, &RewriteResult{
		OldKey: source, NewKey: taken, RowsKept: 1, RowsRemoved: 1,
	})
	if !errors.Is(err, errReplacementKeyTaken) {
		t.Fatalf("publishRewrite onto a registered key = %v, want errReplacementKeyTaken", err)
	}
	if fi, ok := m.GetFileByKey(taken); !ok || fi.RowCount != 5 {
		t.Fatalf("the entry for the registered key changed: %+v ok=%v", fi, ok)
	}
	if !m.HasKey(source) {
		t.Fatal("the source's entry was removed by a refused publish")
	}
}

// hookedManifest runs a hook on the first Retire — the point where handling one
// record has already started changing state, so a test can make another record
// advance exactly there.
type hookedManifest struct {
	ManifestUpdater
	onFirstRetire func()
}

func (h *hookedManifest) Retire(key, by string, reclaim bool) bool {
	if h.onFirstRetire != nil {
		fn := h.onFirstRetire
		h.onFirstRetire = nil
		fn()
	}
	return h.ManifestUpdater.Retire(key, by, reclaim)
}

// TestResumeRewrites_ActsOnTheRecordAsItIsUnderTheClaim: the resume loop reads
// the tombstone once and then works through its records one by one, so by the
// time it reaches the last one the record it read may be several steps old. A
// rewrite that published in the meantime owns a REPLACEMENT the manifest is
// serving; undoing it on the strength of the stale `prepared` deletes the live
// copy of those rows. The loop therefore re-reads under the key claim.
func TestResumeRewrites_ActsOnTheRecordAsItIsUnderTheClaim(t *testing.T) {
	const (
		dir      = "logs/dt=2026-03-01/hour=07/"
		sourceA  = dir + "src-a.parquet"
		abandonA = dir + "aaaaaaaa.parquet"
		sourceB  = dir + "src-b.parquet"
		liveB    = dir + "bbbbbbbb.parquet"
	)
	rows := []schema.LogRow{{TimestampUnixNano: 1000, Body: "keep-a", SeverityText: "info", ServiceName: "web"}}
	pool := newMockRewriterPool()
	for _, k := range []string{sourceA, abandonA, liveB} {
		pool.Put(k, buildTestParquet(t, rows))
	}
	// The manifest serves A (whose rewrite is still prepared) and B's
	// replacement (whose rewrite published while this loop was running).
	m := newTestManifest(t, map[string]int64{sourceA: 1, liveB: 1})

	store := NewTombstoneStore()
	store.Add(Tombstone{
		ID: "ts-resume", Query: `severity_text:="error"`, StartNs: 0, EndNs: 1 << 40,
		AffectedKeys: []string{sourceA, sourceB}, CreatedAt: time.Now().Add(-2 * time.Hour),
		Mode: "permanent", Reaped: map[string]bool{},
	})
	store.Update("ts-resume", func(cur *Tombstone) bool {
		cur.Superseded = map[string]Supersession{
			sourceA: {NewKey: abandonA, State: SupersessionPrepared, At: time.Now()},
			sourceB: {NewKey: liveB, State: SupersessionPrepared, At: time.Now()},
		}
		return true
	})

	hooked := &hookedManifest{ManifestUpdater: m, onFirstRetire: func() {
		// B's rewrite finishes here: its replacement is the manifest's copy of
		// those rows from now on.
		recordPublished(store, "ts-resume", sourceB, liveB)
	}}
	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       NewRewriter(pool, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       hooked,
	})

	sched.resumeRewrites(context.Background(), "ts-resume")

	if !pool.Has(liveB) {
		t.Fatalf("%s was deleted: the loop undid a rewrite that had published, taking the manifest's only copy of its rows", liveB)
	}
	if !m.HasKey(liveB) {
		t.Fatalf("the manifest stopped serving %s", liveB)
	}
	// A's own resolution still happened: its abandoned upload is gone.
	if pool.Has(abandonA) {
		t.Errorf("the abandoned replacement %s was not cleaned up", abandonA)
	}
}
