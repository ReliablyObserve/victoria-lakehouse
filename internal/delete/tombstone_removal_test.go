package delete

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// An un-delete (or a retirement) removes a tombstone locally at once and from
// S3 in the same call — but when that S3 delete fails it is only queued, and the
// queue lives in memory. The restore merges the disk copy with S3, so a crash
// before the retry used to bring the removed tombstone back from the S3 copy:
// hidden rows hidden again after the user restored them, and a permanent-mode
// tombstone physically removing them.

func TestUndelete_SurvivesACrashWhileTheS3DeleteIsPending(t *testing.T) {
	pool := newFlakyS3Pool()
	cfg := PersistenceConfig{Dir: t.TempDir(), Pool: pool, Prefix: "logs/"}

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	ts := sampleTombstone("ts-undelete")
	ts.Mode = "permanent"
	store.Add(ts)
	key := TombstonePrefix("logs/") + "ts-undelete.json"
	if !pool.Has(key) {
		t.Fatalf("precondition: the S3 copy %s must exist", key)
	}

	pool.failDeletes = 1 // the S3 delete of the un-delete fails...
	store.Remove("ts-undelete")
	if store.PendingS3Writes() != 1 || !pool.Has(key) {
		t.Fatalf("precondition: the removal must be owed to S3, pending=%d", store.PendingS3Writes())
	}
	// ...and the process dies before the retry.

	restored := NewTombstoneStore()
	n, err := restored.Restore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if n != 0 {
		got, _ := restored.Get("ts-undelete")
		t.Fatalf("the un-deleted tombstone came back from its stale S3 copy: %+v", got)
	}

	// The stale S3 copy is still owed a delete; the restarted node must finish
	// it rather than leave it for the next node that boots without this disk.
	restored.EnablePersistence(cfg)
	if remaining := restored.FlushPending(context.Background()); remaining != 0 {
		t.Fatalf("the stale S3 copy's delete is still pending after a flush: %d", remaining)
	}
	if pool.Has(key) {
		t.Fatalf("the stale S3 copy %s of an un-deleted tombstone was not deleted after the restart", key)
	}
}

func TestRetirement_SurvivesACrashWhileTheS3DeleteIsPending(t *testing.T) {
	pool := newFlakyS3Pool()
	cfg := PersistenceConfig{Dir: t.TempDir(), Pool: pool, Prefix: "logs/"}

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	ts := sampleTombstone("ts-done")
	ts.Mode = "permanent"
	ts.Reaped = map[string]bool{ts.AffectedKeys[0]: true}
	store.Add(ts)

	pool.failDeletes = 1
	if !store.Complete("ts-done") {
		t.Fatal("fixture: a fully reaped permanent tombstone completes")
	}

	restored := NewTombstoneStore()
	if n, _ := restored.Restore(context.Background(), cfg); n != 0 {
		t.Fatalf("a retired tombstone came back from its stale S3 copy; restored %d", n)
	}
}

// TestRemovedMarkers_ReaddingTheSameIDIsNotSuppressed: the removed set must not
// swallow a later delete that reuses the id.
func TestRemovedMarkers_ReaddingTheSameIDIsNotSuppressed(t *testing.T) {
	pool := newFlakyS3Pool()
	cfg := PersistenceConfig{Dir: t.TempDir(), Pool: pool, Prefix: "logs/"}

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	store.Add(sampleTombstone("ts-again"))
	store.Remove("ts-again")
	store.Add(sampleTombstone("ts-again"))

	restored := NewTombstoneStore()
	if n, _ := restored.Restore(context.Background(), cfg); n != 1 {
		t.Fatalf("a tombstone re-added after its removal must survive a restart, restored %d", n)
	}
}

// TestTombstonesFile_PreviousFormatStillLoads: nodes upgraded in place boot
// from a tombstones.json written by the previous release, a bare id → record
// map.
func TestTombstonesFile_PreviousFormatStillLoads(t *testing.T) {
	dir := t.TempDir()
	legacy := map[string]Tombstone{"ts-old": sampleTombstone("ts-old")}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tombstones.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewTombstoneStore()
	if n, err := store.Restore(context.Background(), PersistenceConfig{Dir: dir}); err != nil || n != 1 {
		t.Fatalf("previous-format tombstones.json: restored %d, err %v", n, err)
	}
}
