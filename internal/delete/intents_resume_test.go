package delete

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// resumeWorld is a scheduler over a manifest, a bucket and one tombstone whose
// rewrite records a test writes by hand — the states a previous pass or a
// crashed process leaves behind.
func resumeWorld(t *testing.T, records map[string]Supersession) (*RewriteScheduler, *lhmanifest.Manifest, *faultPool, *TombstoneStore) {
	t.Helper()
	pool := newFaultPool(newMockRewriterPool())
	m := lhmanifest.New("test-bucket", "")
	store := NewTombstoneStore()
	store.Add(Tombstone{Tenants: []TenantRef{{}}, ID: "t", Mode: "permanent", Query: "*", StartNs: 0, EndNs: 1,
		CreatedAt: time.Now().Add(-2 * time.Hour), Superseded: records})
	s := NewRewriteScheduler(RewriteSchedulerConfig{
		Store: store, Rewriter: NewRewriter(pool, "logs/", 100, "logs"),
		Detector: NewStorageClassDetector(nil), RewriteDelay: time.Hour, Manifest: m,
	})
	return s, m, pool, store
}

func TestResumeRewrites_UndoesAPreparedRewriteLeftByAnEarlierProcess(t *testing.T) {
	const src, repl = "logs/dt=2026-03-01/hour=07/src.parquet", "logs/dt=2026-03-01/hour=07/repl.parquet"
	s, m, pool, store := resumeWorld(t, map[string]Supersession{src: {NewKey: repl, State: SupersessionPrepared}})
	pool.Put(src, []byte("source"))
	pool.Put(repl, []byte("replacement"))
	m.AddFile("dt=2026-03-01/hour=07", lhmanifest.FileInfo{Key: src, Size: 6})
	before := metrics.DeleteRewriteInterrupted.Get("undone")

	s.resumeRewrites(context.Background(), "t")

	if pool.Has(repl) || !pool.Has(src) || !m.HasKey(src) {
		t.Fatal("an undone rewrite deletes its replacement and keeps the source")
	}
	if ts, _ := store.Get("t"); len(ts.Superseded) != 0 {
		t.Fatalf("the record must clear once the replacement is deleted: %+v", ts.Superseded)
	}
	if metrics.DeleteRewriteInterrupted.Get("undone") <= before {
		t.Error("an undone rewrite is counted")
	}
}

func TestResumeRewrites_SkipsAKeyARewriteInThisProcessHolds(t *testing.T) {
	const src, repl = "logs/dt=2026-03-01/hour=07/src.parquet", "logs/dt=2026-03-01/hour=07/repl.parquet"
	s, _, pool, store := resumeWorld(t, map[string]Supersession{src: {NewKey: repl, State: SupersessionPrepared}})
	pool.Put(repl, []byte("replacement being uploaded"))

	if !store.claimKey(src) {
		t.Fatal("fixture: claim")
	}
	s.resumeRewrites(context.Background(), "t")
	if !pool.Has(repl) {
		t.Fatal("a record owned by an in-flight rewrite must not be resolved under it")
	}
	store.releaseKey(src)

	s.resumeRewrites(context.Background(), "unknown-tombstone") // no-op
}

func TestResumeRewrites_DiscardedRecordRetriesItsDelete(t *testing.T) {
	const src, repl = "logs/dt=2026-03-01/hour=07/src.parquet", "logs/dt=2026-03-01/hour=07/repl.parquet"
	s, m, pool, store := resumeWorld(t, map[string]Supersession{
		src:              {NewKey: repl, State: SupersessionDiscarded},
		"no-replacement": {State: SupersessionDiscarded},
	})
	pool.Put(repl, []byte("abandoned"))
	pool.failDeleteOn = repl
	before := metrics.DeleteRewriteAbandonedObjectErrors.Get()

	s.resumeRewrites(context.Background(), "t")
	if !pool.Has(repl) || !m.IsRetired(repl) {
		t.Fatal("a failed delete leaves the abandoned replacement retired for the next pass")
	}
	if metrics.DeleteRewriteAbandonedObjectErrors.Get() <= before {
		t.Error("a failed delete of an abandoned replacement is counted")
	}
	ts, _ := store.Get("t")
	if _, ok := ts.Superseded[src]; !ok {
		t.Fatal("the record must survive the failed delete")
	}
	if _, ok := ts.Superseded["no-replacement"]; ok {
		t.Fatal("a discarded record with no replacement has nothing to delete and clears")
	}

	s.resumeRewrites(context.Background(), "t")
	if pool.Has(repl) {
		t.Fatal("the retried delete removes the abandoned replacement")
	}
	// The key stays retired as a guard against a listing older than the delete,
	// with nothing owed for it any more.
	if rk, ok := m.LookupRetired(repl); !ok || rk.Reclaim || !rk.Deleted {
		t.Fatalf("the landed delete settles the debt and keeps the guard, got %+v ok=%v", rk, ok)
	}
	if ts, _ := store.Get("t"); len(ts.Superseded) != 0 {
		t.Fatalf("all records clear: %+v", ts.Superseded)
	}
}

func TestRecordHelpers_OnAMissingOrChangedRecord(t *testing.T) {
	store := NewTombstoneStore()
	if recordPublished(store, "missing", "src", "repl") {
		t.Fatal("recording on a missing tombstone reports failure")
	}
	store.Add(Tombstone{Tenants: []TenantRef{{}}, ID: "t", Superseded: map[string]Supersession{"src": {NewKey: "second-attempt", State: SupersessionPrepared}}})
	clearRecord(store, "t", "src", "first-attempt")
	if ts, _ := store.Get("t"); ts.Superseded["src"].NewKey != "second-attempt" {
		t.Fatal("clearing an older attempt's record must not drop a newer one")
	}
	clearRecord(store, "t", "src", "second-attempt")
	if ts, _ := store.Get("t"); ts.Superseded != nil {
		t.Fatalf("the last record clears to nil: %+v", ts.Superseded)
	}
}

// TestRestore_MarkersArrivingAfterRecordsStillWin covers a restore that reads
// S3 before disk: the stale S3 record is merged first and must be dropped when
// the disk copy's removal marker arrives.
func TestRestore_MarkersArrivingAfterRecordsStillWin(t *testing.T) {
	pool := newFlakyS3Pool()
	dir := t.TempDir()
	cfg := PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}
	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	store.Add(sampleTombstone("ts-gone"))
	pool.failDeletes = 1
	store.Remove("ts-gone")

	restored := NewTombstoneStore()
	restored.EnablePersistence(cfg) // armed before the restore this time
	if err := restored.LoadFromS3(context.Background(), pool, "", "logs/"); err != nil {
		t.Fatalf("load s3: %v", err)
	}
	if _, ok := restored.Get("ts-gone"); !ok {
		t.Fatal("fixture: without markers the stale S3 copy loads")
	}
	if err := restored.LoadFromDisk(dir); err != nil {
		t.Fatalf("load disk: %v", err)
	}
	if _, ok := restored.Get("ts-gone"); ok {
		t.Fatal("the disk copy's removal marker must drop the stale record merged from S3")
	}
	if restored.PendingS3Writes() != 1 {
		t.Fatalf("the stale S3 copy's delete must be owed, pending=%d", restored.PendingS3Writes())
	}
	restored.FlushPending(context.Background())
	if pool.Has(TombstonePrefix("logs/") + "ts-gone.json") {
		t.Fatal("the owed delete must land on flush")
	}
}

func TestTombstonesFile_UnreadableContentIsAnError(t *testing.T) {
	for name, content := range map[string]string{
		"not json":            "{nope",
		"a json array":        "[1,2,3]",
		"a broken envelope":   `{"lakehouse_tombstones_format": "two"}`,
		"a legacy wrong type": `{"ts": 5}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "tombstones.json"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := NewTombstoneStore().LoadFromDisk(dir); err == nil {
				t.Fatalf("%q must not load silently", content)
			}
		})
	}
}
