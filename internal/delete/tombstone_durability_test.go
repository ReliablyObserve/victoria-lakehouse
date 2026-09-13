package delete

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// flakyS3Pool fails a configurable number of uploads before succeeding, so a
// test can prove that a transient S3 outage degrades to "durable locally and
// retried" rather than "lost".
type flakyS3Pool struct {
	*mockS3Pool

	mu          sync.Mutex
	failUploads int
	uploadCalls int
	failDeletes int
	deleteCalls int
}

func newFlakyS3Pool() *flakyS3Pool { return &flakyS3Pool{mockS3Pool: newMockS3Pool()} }

func (f *flakyS3Pool) Upload(ctx context.Context, key string, data []byte) error {
	f.mu.Lock()
	f.uploadCalls++
	if f.failUploads > 0 {
		f.failUploads--
		f.mu.Unlock()
		return errInjected
	}
	f.mu.Unlock()
	return f.mockS3Pool.Upload(ctx, key, data)
}

func (f *flakyS3Pool) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	f.deleteCalls++
	if f.failDeletes > 0 {
		f.failDeletes--
		f.mu.Unlock()
		return errInjected
	}
	f.mu.Unlock()
	return f.mockS3Pool.Delete(ctx, key)
}

func sampleTombstone(id string) Tombstone {
	return Tombstone{
		ID:           id,
		Query:        `service.name:="leaked"`,
		StartNs:      1000,
		EndNs:        9000,
		AffectedKeys: []string{"logs/dt=2026-06-01/hour=00/a.parquet"},
		CreatedAt:    time.Now().Add(-time.Hour),
		Mode:         "hide",
		Reaped:       map[string]bool{},
	}
}

// --- Bug 2: a delete survived only a graceful shutdown ------------------------

func TestTombstoneDurability_SurvivesACrashViaDisk(t *testing.T) {
	dir := t.TempDir()

	live := NewTombstoneStore()
	live.EnablePersistence(PersistenceConfig{Dir: dir, Prefix: "logs/"})
	live.Add(sampleTombstone("ts-crash"))

	// No shutdown, no PersistToDisk call, no SyncToS3 call — the process simply
	// disappears. Before the fix the tombstone reached disk only from
	// runShutdown, so a SIGKILL here silently un-deleted the hidden rows.
	restored := NewTombstoneStore()
	if n, err := restored.Restore(context.Background(), PersistenceConfig{Dir: dir, Prefix: "logs/"}); err != nil || n != 1 {
		t.Fatalf("restore from disk after a crash: n=%d err=%v", n, err)
	}
	got, ok := restored.Get("ts-crash")
	if !ok {
		t.Fatal("the tombstone did not survive the crash")
	}
	if got.Query != `service.name:="leaked"` || got.Mode != "hide" {
		t.Fatalf("restored tombstone lost its content: %+v", got)
	}
}

func TestTombstoneDurability_SurvivesACrashViaS3(t *testing.T) {
	pool := newMockS3Pool()

	live := NewTombstoneStore()
	live.EnablePersistence(PersistenceConfig{Pool: pool, Prefix: "1002/0/logs/"})
	live.Add(sampleTombstone("ts-s3"))

	// A pod that loses its local volume (rescheduled to another node) has only
	// the S3 copy. Before the fix nothing ever wrote one.
	restored := NewTombstoneStore()
	if n, err := restored.Restore(context.Background(), PersistenceConfig{Pool: pool, Prefix: "1002/0/logs/"}); err != nil || n != 1 {
		t.Fatalf("restore from S3: n=%d err=%v", n, err)
	}
	if _, ok := restored.Get("ts-s3"); !ok {
		t.Fatal("the tombstone did not reach S3")
	}
}

func TestTombstoneDurability_S3PrefixHasNoDoubledSlash(t *testing.T) {
	pool := newMockS3Pool()
	store := NewTombstoneStore()
	// Config.AutoPrefix() already ends in "/", which the old
	// fmt.Sprintf("%s/_tombstones/", tenant) turned into "logs//_tombstones/" —
	// a different S3 prefix from the documented one, so LoadFromS3 listed an
	// empty prefix and found nothing.
	store.EnablePersistence(PersistenceConfig{Pool: pool, Prefix: "1002/0/logs/"})
	store.Add(sampleTombstone("ts-prefix"))

	keys := pool.Keys()
	if len(keys) != 1 {
		t.Fatalf("expected exactly one tombstone object, got %v", keys)
	}
	if strings.Contains(keys[0], "//") {
		t.Fatalf("tombstone key has a doubled slash: %s", keys[0])
	}
	if want := "1002/0/logs/_tombstones/ts-prefix.json"; keys[0] != want {
		t.Fatalf("tombstone key = %s, want %s", keys[0], want)
	}
	if got := TombstonePrefix("1002/0/logs"); got != "1002/0/logs/_tombstones/" {
		t.Fatalf("TombstonePrefix without a trailing slash = %q", got)
	}
	if got := TombstonePrefix(""); got != "_tombstones/" {
		t.Fatalf("TombstonePrefix(\"\") = %q", got)
	}
}

func TestTombstoneDurability_RestoreUnionsDiskAndS3(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()

	// Disk holds one record, S3 another — the state a node reaches after one
	// peer wrote a delete it never saw. The old sequence consulted S3 only when
	// disk restored nothing, so the disk-only record hid the S3-only one.
	onlyDisk := NewTombstoneStore()
	onlyDisk.EnablePersistence(PersistenceConfig{Dir: dir})
	onlyDisk.Add(sampleTombstone("ts-disk"))

	onlyS3 := NewTombstoneStore()
	onlyS3.EnablePersistence(PersistenceConfig{Pool: pool, Prefix: "logs/"})
	onlyS3.Add(sampleTombstone("ts-s3"))

	restored := NewTombstoneStore()
	n, err := restored.Restore(context.Background(), PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if n != 2 {
		t.Fatalf("restore found %d tombstones, want the union of both sources (2)", n)
	}
	for _, id := range []string{"ts-disk", "ts-s3"} {
		if _, ok := restored.Get(id); !ok {
			t.Errorf("%s missing from the union", id)
		}
	}
}

func TestTombstoneDurability_MergeNeverWalksBackRewriteProgress(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()

	key := "logs/dt=2026-06-01/hour=00/a.parquet"

	// S3 holds a copy where the key is already reaped; disk holds an older copy
	// where it is not. Rewrite progress is monotonic — the file the key names is
	// gone — so the merge must keep the reaped mark rather than schedule a
	// rewrite of a key that no longer exists.
	ahead := sampleTombstone("ts-merge")
	ahead.Mode = "permanent"
	ahead.Reaped = map[string]bool{key: true}
	s3Store := NewTombstoneStore()
	s3Store.EnablePersistence(PersistenceConfig{Pool: pool, Prefix: "logs/"})
	s3Store.Add(ahead)

	behind := sampleTombstone("ts-merge")
	behind.Mode = "permanent"
	behind.Reaped = map[string]bool{}
	diskStore := NewTombstoneStore()
	diskStore.EnablePersistence(PersistenceConfig{Dir: dir})
	diskStore.Add(behind)

	restored := NewTombstoneStore()
	if _, err := restored.Restore(context.Background(), PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, ok := restored.Get("ts-merge")
	if !ok {
		t.Fatal("tombstone missing after merge")
	}
	if !got.Reaped[key] {
		t.Fatal("the merge lost the reaped mark; the scheduler would retry a key whose object is gone")
	}
}

func TestTombstoneDurability_RemovalIsPersisted(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()
	cfg := PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	store.Add(sampleTombstone("ts-undelete"))
	store.Remove("ts-undelete")

	// An un-delete that is not persisted comes back on the next restart, which
	// re-hides data the user explicitly restored.
	restored := NewTombstoneStore()
	if n, err := restored.Restore(context.Background(), cfg); err != nil || n != 0 {
		t.Fatalf("a removed tombstone came back: n=%d err=%v", n, err)
	}
	if len(pool.Keys()) != 0 {
		t.Fatalf("the S3 copy was not removed: %v", pool.Keys())
	}
}

func TestTombstoneDurability_CompletionIsPersisted(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()
	cfg := PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}

	key := "logs/dt=2026-06-01/hour=00/a.parquet"
	ts := sampleTombstone("ts-done")
	ts.Mode = "permanent"
	ts.Reaped = map[string]bool{key: true}

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	store.Add(ts)
	if !store.Complete("ts-done") {
		t.Fatal("a fully reaped permanent tombstone should complete")
	}

	restored := NewTombstoneStore()
	if n, _ := restored.Restore(context.Background(), cfg); n != 0 {
		t.Fatalf("a completed tombstone was restored (%d); it would go back to being eternally active", n)
	}
}

func TestTombstoneComplete_RefusesHideModeAndUnfinishedWork(t *testing.T) {
	store := NewTombstoneStore()

	hide := sampleTombstone("ts-hide")
	hide.Reaped = map[string]bool{hide.AffectedKeys[0]: true}
	store.Add(hide)
	if store.Complete("ts-hide") {
		t.Error("hide mode is a standing instruction to suppress rows that still exist; it must never auto-complete")
	}

	partial := sampleTombstone("ts-partial")
	partial.Mode = "permanent"
	partial.AffectedKeys = []string{"a.parquet", "b.parquet"}
	partial.Reaped = map[string]bool{"a.parquet": true}
	store.Add(partial)
	if store.Complete("ts-partial") {
		t.Error("a tombstone with un-reaped keys still has work to do")
	}

	if store.Complete("ts-absent") {
		t.Error("completing an unknown id must report false")
	}
}

func TestTombstoneDurability_S3FailureIsRetriedNotLost(t *testing.T) {
	dir := t.TempDir()
	pool := newFlakyS3Pool()
	pool.failUploads = 1
	cfg := PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}

	beforeErr := metrics.DeleteTombstonePersistErrors.Get("s3")

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	store.Add(sampleTombstone("ts-flaky"))

	if metrics.DeleteTombstonePersistErrors.Get("s3") <= beforeErr {
		t.Error("a failed S3 write must be counted")
	}
	if metrics.DeleteTombstonePersistPending.Get() != 1 {
		t.Errorf("pending gauge = %d, want 1 record owed to S3", metrics.DeleteTombstonePersistPending.Get())
	}

	// The local disk copy is authoritative in the meantime: a crash right now
	// still recovers the delete.
	diskOnly := NewTombstoneStore()
	if n, _ := diskOnly.Restore(context.Background(), PersistenceConfig{Dir: dir}); n != 1 {
		t.Fatalf("the disk copy must be written even when S3 fails, restored %d", n)
	}

	// The retry drains the backlog.
	if remaining := store.FlushPending(context.Background()); remaining != 0 {
		t.Fatalf("FlushPending left %d records pending", remaining)
	}
	if metrics.DeleteTombstonePersistPending.Get() != 0 {
		t.Error("pending gauge not cleared after a successful retry")
	}

	s3Only := NewTombstoneStore()
	if n, _ := s3Only.Restore(context.Background(), PersistenceConfig{Pool: pool.mockS3Pool, Prefix: "logs/"}); n != 1 {
		t.Fatalf("the retried write did not reach S3, restored %d", n)
	}
}

func TestTombstoneDurability_NoPoolDoesNotGrowABacklog(t *testing.T) {
	store := NewTombstoneStore()
	store.EnablePersistence(PersistenceConfig{Dir: t.TempDir()})
	for i := 0; i < 5; i++ {
		store.Add(sampleTombstone("ts-nopool"))
	}
	if n := store.FlushPending(context.Background()); n != 0 {
		t.Fatalf("a disk-only store reported %d records owed to an S3 target it does not have", n)
	}
}

func TestTombstoneDurability_DisabledPersistenceIsANoOp(t *testing.T) {
	// Persistence is opt-in; a store without it must still work, just without
	// surviving a restart. SelfCheck is what makes that visible.
	store := NewTombstoneStore()
	store.Add(sampleTombstone("ts-mem"))
	if store.Count() != 1 {
		t.Fatal("the store must work without persistence")
	}
	if n := store.FlushPending(context.Background()); n != 0 {
		t.Fatalf("FlushPending on an unconfigured store returned %d", n)
	}
	if store.PersistenceEnabled() {
		t.Error("PersistenceEnabled must report false before EnablePersistence")
	}
}

func TestTombstoneDurability_UnreadableObjectDoesNotAbortTheRestore(t *testing.T) {
	pool := newMockS3Pool()
	good := NewTombstoneStore()
	good.EnablePersistence(PersistenceConfig{Pool: pool, Prefix: "logs/"})
	good.Add(sampleTombstone("ts-good"))

	// One corrupt object must not un-delete the 99 good ones.
	pool.Put("logs/_tombstones/corrupt.json", []byte("{not json"))

	restored := NewTombstoneStore()
	n, err := restored.Restore(context.Background(), PersistenceConfig{Pool: pool, Prefix: "logs/"})
	if err == nil {
		t.Error("a partial restore must be reported to the operator")
	}
	if n != 1 {
		t.Fatalf("restored %d tombstones, want the 1 readable record", n)
	}
	if _, ok := restored.Get("ts-good"); !ok {
		t.Error("the readable record was discarded along with the corrupt one")
	}
}

func TestTombstoneDurability_ConcurrentMutationsAreAllPersisted(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()
	cfg := PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ts := sampleTombstone("ts-" + string(rune('a'+i)))
			store.Add(ts)
		}(i)
	}
	wg.Wait()
	store.FlushPending(context.Background())

	restored := NewTombstoneStore()
	got, err := restored.Restore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got != n {
		t.Fatalf("restored %d of %d concurrently added tombstones", got, n)
	}
}

// --- filter parse cache -------------------------------------------------------

func TestTombstoneFilterIsParsedOnce(t *testing.T) {
	ts := Tombstone{Query: `severity_text:="error"`, StartNs: 0, EndNs: 100}
	first := ts.Filter()
	if first == nil {
		t.Fatal("a valid query must parse")
	}
	if second := ts.Filter(); second != first {
		t.Error("the parsed filter must be cached; re-parsing per row dominates the cost of having any tombstone")
	}
}

func TestTombstoneUnparseableQueryMatchesNothing(t *testing.T) {
	ts := Tombstone{Query: `severity_text:=="`, StartNs: 0, EndNs: 100}
	if ts.Filter() != nil {
		t.Error("an unparseable query has no filter")
	}
	// Fail closed: a tombstone we cannot evaluate hides nothing rather than
	// hiding everything.
	if ts.MatchesRow(map[string]string{"severity_text": "error"}, 50) {
		t.Error("an unparseable tombstone must not match")
	}
	if ts.MatchesFields(nil, 50) {
		t.Error("an unparseable tombstone must not match")
	}
}

func TestTombstoneMatchesFieldsAgreesWithMatchesRow(t *testing.T) {
	ts := Tombstone{Query: `service.name:="web"`, StartNs: 0, EndNs: 100}
	row := map[string]string{"service.name": "web"}
	fields := fieldsFromMap(row)

	for _, tsNs := range []int64{-1, 0, 50, 100, 101} {
		want := ts.MatchesRow(row, tsNs)
		if got := ts.MatchesFields(fields, tsNs); got != want {
			t.Errorf("at ts=%d MatchesFields=%v but MatchesRow=%v", tsNs, got, want)
		}
	}
}

func TestTombstoneFullyReaped(t *testing.T) {
	ts := Tombstone{AffectedKeys: []string{"a", "b"}, Reaped: map[string]bool{"a": true}}
	if ts.FullyReaped() {
		t.Error("one of two keys reaped is not fully reaped")
	}
	ts.Reaped["b"] = true
	if !ts.FullyReaped() {
		t.Error("both keys reaped is fully reaped")
	}
	// A tombstone that names no file has no work to complete and must not be
	// retired as if it did — it may still be hiding rows in the buffer.
	empty := Tombstone{}
	if empty.FullyReaped() {
		t.Error("a tombstone with no affected keys must not report itself reaped")
	}
}
