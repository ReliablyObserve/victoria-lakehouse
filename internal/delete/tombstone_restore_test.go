package delete

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// A tombstone restore that cannot read S3 is a durability failure, not a
// warning: the node serves queries without deletes another node made, and every
// interrupted rewrite it holds records for stays unresolved. It has to be
// counted, remembered, retried, and reported.

func storeTombstoneInS3(t *testing.T, pool *mockS3Pool, prefix string, ts Tombstone) {
	t.Helper()
	data, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("marshal tombstone: %v", err)
	}
	pool.Put(TombstonePrefix(prefix)+ts.ID+".json", data)
}

// TestRestore_S3ListFailureIsCounted: the LIST fails, so the restore fails. It
// is retried a bounded number of times, counted under a kind an alert can fire
// on, reported as an error, and remembered so nothing acts on the records it
// could not read.
func TestRestore_S3ListFailureIsCounted(t *testing.T) {
	fastRestoreRetry(t)
	inner := newMockS3Pool()
	storeTombstoneInS3(t, inner, "logs/", sampleTombstone("ts-restore"))
	lf := newListFailPool(inner)

	beforeKind := metrics.DeleteStartupInconsistencies.Get("s3_restore_failed")
	beforeFailed := metrics.DeleteTombstoneRestoreAttempts.Get("failed")

	store := NewTombstoneStore()
	n, err := store.Restore(context.Background(), PersistenceConfig{Pool: lf, Prefix: "logs/"})
	if err == nil {
		t.Fatal("a restore whose LIST failed must report an error")
	}
	if n != 0 {
		t.Fatalf("restored %d tombstones although every LIST failed", n)
	}
	if got := metrics.DeleteStartupInconsistencies.Get("s3_restore_failed") - beforeKind; got != 1 {
		t.Errorf("lakehouse_delete_startup_inconsistencies_total{kind=\"s3_restore_failed\"} moved by %d, want 1", got)
	}
	if got := metrics.DeleteTombstoneRestoreAttempts.Get("failed") - beforeFailed; got != uint64(restoreAttempts) {
		t.Errorf("counted %d failed restore attempts, want the bounded %d", got, restoreAttempts)
	}
	if lf.listCalls() != restoreAttempts {
		t.Errorf("the restore listed %d times, want %d", lf.listCalls(), restoreAttempts)
	}
	if !store.S3RestorePending() {
		t.Fatal("a failed restore must be remembered: nothing may act on records that may be stale")
	}
	if got := metrics.DeleteTombstoneRestorePending.Get(); got != 1 {
		t.Errorf("lakehouse_delete_tombstone_restore_pending = %d, want 1 — the alert fires on this gauge", got)
	}

	// The self-check reports it, so an operator sees it without reading logs.
	var found bool
	for _, f := range SelfCheck(store, nil) {
		if f.Kind == "s3_restore_failed" {
			found = true
		}
	}
	if !found {
		t.Error("the boot self-check does not report a failed tombstone restore")
	}

	// And it recovers: the retry reads the records and counts the recovery.
	beforeRecovered := metrics.DeleteTombstoneRestoreAttempts.Get("recovered")
	lf.heal()
	if !store.RetryS3Restore(context.Background()) {
		t.Fatal("the retry must succeed once the LIST works")
	}
	if store.S3RestorePending() {
		t.Error("a successful retry must clear the pending state")
	}
	if got := metrics.DeleteTombstoneRestorePending.Get(); got != 0 {
		t.Errorf("lakehouse_delete_tombstone_restore_pending = %d after recovery, want 0", got)
	}
	if store.Count() != 1 {
		t.Errorf("the retry restored %d tombstones, want the 1 in the bucket", store.Count())
	}
	if got := metrics.DeleteTombstoneRestoreAttempts.Get("recovered") - beforeRecovered; got != 1 {
		t.Errorf("lakehouse_delete_tombstone_restore_attempts_total{result=\"recovered\"} moved by %d, want 1", got)
	}
}

// TestRestore_HealthyS3IsNotCounted is the negative control: the same call with
// a working LIST moves no failure counter and leaves nothing pending.
func TestRestore_HealthyS3IsNotCounted(t *testing.T) {
	pool := newMockS3Pool()
	storeTombstoneInS3(t, pool, "logs/", sampleTombstone("ts-ok"))

	beforeKind := metrics.DeleteStartupInconsistencies.Get("s3_restore_failed")
	beforeFailed := metrics.DeleteTombstoneRestoreAttempts.Get("failed")

	store := NewTombstoneStore()
	n, err := store.Restore(context.Background(), PersistenceConfig{Pool: pool, Prefix: "logs/"})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if n != 1 {
		t.Fatalf("restored %d tombstones, want 1", n)
	}
	if store.S3RestorePending() {
		t.Error("nothing failed, so nothing may be pending")
	}
	if got := metrics.DeleteStartupInconsistencies.Get("s3_restore_failed") - beforeKind; got != 0 {
		t.Errorf("a healthy restore counted %d s3_restore_failed", got)
	}
	if got := metrics.DeleteTombstoneRestoreAttempts.Get("failed") - beforeFailed; got != 0 {
		t.Errorf("a healthy restore counted %d failed attempts", got)
	}
}

// TestRunRestoreRetry_KeepsTryingUntilTheListWorks covers the loop the binaries
// run: a node whose rewrite scheduler is disabled has no other retry, and would
// otherwise serve an incomplete tombstone set for the life of the process.
func TestRunRestoreRetry_KeepsTryingUntilTheListWorks(t *testing.T) {
	fastRestoreRetry(t)
	inner := newMockS3Pool()
	storeTombstoneInS3(t, inner, "logs/", sampleTombstone("ts-loop"))
	lf := newListFailPool(inner)

	store := NewTombstoneStore()
	if _, err := store.Restore(context.Background(), PersistenceConfig{Pool: lf, Prefix: "logs/"}); err == nil {
		t.Fatal("fixture: the restore did not fail")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		store.RunRestoreRetry(ctx, time.Millisecond)
		close(done)
	}()
	lf.heal()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the retry loop did not finish after the LIST recovered")
	}
	if store.S3RestorePending() {
		t.Error("the loop returned with the restore still pending")
	}
	if store.Count() != 1 {
		t.Errorf("the loop restored %d tombstones, want 1", store.Count())
	}
}

// TestRunRestoreRetry_StopsWithTheContext: the loop must not outlive shutdown.
func TestRunRestoreRetry_StopsWithTheContext(t *testing.T) {
	fastRestoreRetry(t)
	lf := newListFailPool(newMockS3Pool())
	store := NewTombstoneStore()
	if _, err := store.Restore(context.Background(), PersistenceConfig{Pool: lf, Prefix: "logs/"}); err == nil {
		t.Fatal("fixture: the restore did not fail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		store.RunRestoreRetry(ctx, time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the retry loop ignored its context")
	}
}

// TestTombstonePrefix_NeverDoublesTheSlash pins the key layout the operator docs
// describe and the orphan sweep protects. The original writer built the prefix
// with fmt.Sprintf("%s/_tombstones/", prefix) on a prefix that already ended in
// "/", so the records landed under "logs//_tombstones/" — a path anything
// reading the documented layout misses entirely.
func TestTombstonePrefix_NeverDoublesTheSlash(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", "_tombstones/"},
		{"/", "_tombstones/"},
		{"//", "_tombstones/"},
		{"logs", "logs/_tombstones/"},
		{"logs/", "logs/_tombstones/"},
		{"logs//", "logs/_tombstones/"},
		{"1002/0/logs", "1002/0/logs/_tombstones/"},
		{"1002/0/logs/", "1002/0/logs/_tombstones/"},
	} {
		got := TombstonePrefix(tc.in)
		if got != tc.want {
			t.Errorf("TombstonePrefix(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if strings.Contains(got, "//") {
			t.Errorf("TombstonePrefix(%q) = %q contains a doubled slash", tc.in, got)
		}
	}
}

// TestRestore_WritesAndReadsTheSameKeys is the round trip behind that layout:
// what the store persists under a prefix ending in "/" is what a restore under
// the same prefix finds.
func TestRestore_WritesAndReadsTheSameKeys(t *testing.T) {
	pool := newMockS3Pool()
	cfg := PersistenceConfig{Pool: pool, Prefix: "logs/"}
	writer := NewTombstoneStore()
	writer.EnablePersistence(cfg)
	writer.Add(sampleTombstone("ts-roundtrip"))
	if n := writer.FlushPending(context.Background()); n != 0 {
		t.Fatalf("%d records still owed to S3 after the flush", n)
	}
	if !pool.Has("logs/_tombstones/ts-roundtrip.json") {
		t.Fatalf("the record is not under the documented prefix; bucket=%v", pool.Keys())
	}

	reader := NewTombstoneStore()
	if _, err := reader.Restore(context.Background(), cfg); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := reader.Get("ts-roundtrip"); !ok {
		t.Fatalf("the restore did not find the record it wrote; bucket=%v", pool.Keys())
	}
}
