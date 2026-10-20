package delete

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// What a pod's view does with a listing that is short, sparse or late. The view
// only grows from a listing, takes no destructive step on its own, and learns
// every key within the poll window at the next poll and every other key at the
// next full listing.

// A pod that has never seen a delete, whose first listing silently misses it,
// learns it at the next listing; nothing is lost or removed meanwhile.
func TestTombstoneSync_SilentDropBeforeDiscovery(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	for i := 0; i < 5; i++ {
		create(t, writer, fmt.Sprintf("d%d", i))
	}
	reader, pod := newPod(t, b, "reader")
	pod.ShortListOnce(2)
	mustSync(t, reader, true)
	if reader.Count() >= 5 {
		t.Fatalf("fixture: the short listing should have hidden some keys, view holds %d", reader.Count())
	}
	if c := pod.Counts(); c.Puts != 0 || c.Deletes != 0 {
		t.Fatalf("the pod wrote after a short first listing: %+v", c)
	}
	mustSync(t, reader, true)
	if reader.Count() != 5 {
		t.Fatalf("the next listing left the view at %d, want 5", reader.Count())
	}
}

// What happens to a tombstone the full listing did not list, by what the store
// answers to a HEAD of its definition: it is kept when the key exists or the
// answer is an error, and forgotten only on a 404.
func TestTombstoneSync_AbsentKeyOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(b *deletetest.Bucket, pod *deletetest.Pod, key string)
		wantInView bool
	}{
		{"listed (control)", func(*deletetest.Bucket, *deletetest.Pod, string) {}, true},
		{"unlisted but HEAD finds it", func(b *deletetest.Bucket, pod *deletetest.Pod, key string) { pod.ShortListOnce(1) }, true},
		{"HEAD fails", func(b *deletetest.Bucket, pod *deletetest.Pod, key string) {
			b.Remove(key)
			pod.FailNext("head", 100)
		}, true},
		{"HEAD says 404", func(b *deletetest.Bucket, pod *deletetest.Pod, key string) { b.Remove(key) }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := deletetest.NewBucket()
			writer, _ := newSyncedPod(t, b, "writer")
			create(t, writer, "only")
			create(t, writer, "other")
			reader, pod := newSyncedPod(t, b, "reader")
			var key string
			for _, k := range b.Keys(tombPrefix()) {
				if pk, ok := parseKey(tombPrefix(), k); ok && pk.kind == kindDefinition && pk.id == "only" {
					key = k
				}
			}
			if key == "" {
				t.Fatal("fixture: no definition key")
			}
			tc.setup(b, pod, key)
			mustSync(t, reader, true)
			if got := has(reader, "only"); got != tc.wantInView {
				t.Fatalf("in view = %v, want %v", got, tc.wantInView)
			}
			if !has(reader, "other") {
				t.Fatal("an unrelated tombstone left the view")
			}
			for _, op := range b.Log() {
				if op.Kind == "DELETE" {
					t.Fatalf("%s deleted %s", op.Pod, op.Key)
				}
			}
		})
	}
}

// One key silently missing from an otherwise complete listing (a small drop that
// raises no alarm) keeps every tombstone: a HEAD finds the key, so nothing is
// forgotten, and the pod issues no DELETE.
func TestTombstoneSync_SilentSmallDropKeepsEveryTombstone(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	const n = 10
	for i := 0; i < n; i++ {
		create(t, writer, fmt.Sprintf("d%d", i))
	}
	reader, pod := newSyncedPod(t, b, "reader")
	pod.ResetCounts()
	pod.ShortListOnce(n - 1)
	mustSync(t, reader, true)
	if reader.Count() != n {
		t.Fatalf("a silent small drop changed the view: %d, want %d", reader.Count(), n)
	}
	if c := pod.Counts(); c.Deletes != 0 || c.Puts != 0 || c.Heads > 2 {
		t.Fatalf("a one-key drop cost %+v, want no writes and a HEAD or two", c)
	}
}

// A delete created while another pod's listing is in flight is not lost to that
// pod: its next poll starts from before the last listing began, so the key is
// listed even though the first listing predated it.
func TestTombstoneSync_DeleteCreatedDuringAListingIsLearnedNextPoll(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	reader, _ := newSyncedPod(t, b, "reader")
	// The tombstone is created right after the reader's listing began, in the
	// same store second.
	create(t, writer, "late")
	if has(reader, "late") {
		t.Fatal("fixture: the reader should not know it yet")
	}
	mustSync(t, reader, false)
	if !has(reader, "late") {
		t.Fatal("the poll after the listing did not learn the delete created during it")
	}
}

// A key older than the poll window is not listed by a poll. A pod learns it at
// the full listing (every 60 polls) and not before, counts it as learned late, and
// exports how old it was. This is what a creating pod whose clock is more than the
// window behind looks like to everybody else.
func TestTombstoneSync_KeyOutsidePollWindow_LearnedAtFullListingOnly(t *testing.T) {
	b := deletetest.NewBucket()
	writer, wpod := newSyncedPod(t, b, "writer")
	wpod.SetSkew(-30 * time.Minute) // the writer's clock is 30 minutes behind
	reader, _ := newSyncedPod(t, b, "reader")
	lateBefore := metrics.DeleteTombstonesLearnedLate.Get()

	create(t, writer, "skewed")
	if _, ok := writer.Get("skewed"); !ok {
		t.Fatal("fixture")
	}
	for i := 1; i <= fullEvery-1; i++ {
		b.Advance(time.Minute)
		mustSync(t, reader, false)
		if has(reader, "skewed") {
			t.Fatalf("poll %d learned a key that is older than the poll window", i)
		}
	}
	if metrics.DeleteTombstonesLearnedLate.Get() != lateBefore {
		t.Fatal("learned-late counted before the key was learned")
	}
	b.Advance(time.Minute) // the 60th interval: a full listing is due
	mustSync(t, reader, false)
	if !has(reader, "skewed") {
		t.Fatal("the full listing did not learn the key")
	}
	if got := metrics.DeleteTombstonesLearnedLate.Get() - lateBefore; got != 1 {
		t.Fatalf("learned-late counted %d, want 1", got)
	}
	if skew := metrics.DeleteTombstoneKeySkew.Get(); skew < int64(30*time.Minute/time.Second) {
		t.Fatalf("key skew gauge = %ds, want at least the 30 minutes the key was behind", skew)
	}
}

// A pod whose own clock is behind learns a normal key without counting it late.
func TestTombstoneSync_OnTimeKeyIsNotLate(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	reader, _ := newSyncedPod(t, b, "reader")
	before := metrics.DeleteTombstonesLearnedLate.Get()
	create(t, writer, "prompt")
	b.Advance(time.Minute)
	mustSync(t, reader, false)
	if !has(reader, "prompt") {
		t.Fatal("the poll did not learn the key")
	}
	if metrics.DeleteTombstonesLearnedLate.Get() != before {
		t.Fatal("a key learned within the window was counted late")
	}
}

// A first listing that fails sweeps nothing and collects nothing: expiry and the GC
// run on the pod's clock only after a listing has succeeded, so a wrong clock
// cannot act on a view that was never confirmed.
func TestTombstoneSync_FailedFirstListing_SweepsAndCollectsNothing(t *testing.T) {
	dir := t.TempDir()
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	old := hideTombstone("old")
	old.EndNs = b.Now().UnixNano()
	mustCreate(t, writer, old)
	warm, _ := newSyncedPod(t, b, "warm", withRetention, func(o *Options) { o.CacheDir = dir }) // fills the cache
	if !has(warm, "old") {
		t.Fatal("fixture")
	}

	b.Advance(testMaxTTL + 10*retentionMargin) // the tombstone is long past its retention
	restarted, rpod := newPod(t, b, "maint", withRetention, maintenance, func(o *Options) { o.CacheDir = dir })
	rpod.FailNext("list", 1)
	if err := restarted.SyncNow(context.Background(), true); err == nil {
		t.Fatal("fixture: the first listing must fail")
	}
	if !has(restarted, "old") {
		t.Fatal("an expired tombstone left the view before any listing succeeded")
	}
	if len(deletes(b)) != 0 {
		t.Fatalf("the GC deleted keys before any listing succeeded: %v", deletes(b))
	}
	mustSync(t, restarted, true)
	if has(restarted, "old") || len(deletes(b)) == 0 {
		t.Fatal("after a successful listing the expired tombstone should be dropped and collected")
	}
}

// The same upstream task id arriving with different content is refused, whichever
// pod it reaches and whatever that pod's clock says: the key is the id, so the
// store arbitrates and the second filter is never silently lost.
func TestRunTask_SameIDDifferentContent_SecondIsAlreadyRegistered(t *testing.T) {
	b := deletetest.NewBucket()
	podA, apod := newSyncedPod(t, b, "a")
	podB, bpod := newSyncedPod(t, b, "b")
	apod.SetSkew(-2 * time.Hour)
	bpod.SetSkew(3 * time.Hour)
	const id = "1792310275123456789"
	ctx := context.Background()
	if err := RunTask(ctx, podA, false, id, 1792310275123456789, []TenantRef{{AccountID: 5}}, "level:error", "hide"); err != nil {
		t.Fatalf("first: %v", err)
	}
	err := RunTask(ctx, podB, false, id, 1792310275123456789, []TenantRef{{AccountID: 5}}, "level:warn", "hide")
	if !errors.Is(err, ErrTaskExists) {
		t.Fatalf("a different filter under one id = %v, want ErrTaskExists", err)
	}
	if n := len(b.Keys(tombPrefix())); n != 1 {
		t.Fatalf("%d keys for one task id, want 1", n)
	}
	// The identical task from the skewed pod is a success and writes nothing new.
	if err := RunTask(ctx, podB, false, id, 1792310275123456789, []TenantRef{{AccountID: 5}}, "level:error", "hide"); err != nil {
		t.Fatalf("identical repeat on another pod: %v", err)
	}
	if n := len(b.Keys(tombPrefix())); n != 1 {
		t.Fatalf("%d keys after the repeat, want 1", n)
	}
}

// A key created while this pod's listings were failing is not "late", however long
// the outage lasted: the polls could not have listed it, but its creator's clock is
// fine. Only a key older than the window that followed the last good listing is.
func TestTombstoneSync_OutageDoesNotCountKeysAsLate(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	reader, rpod := newSyncedPod(t, b, "reader")
	before := metrics.DeleteTombstonesLearnedLate.Get()

	rpod.FailNext("list", 1_000_000)
	for i := 0; i < 5; i++ {
		b.Advance(time.Minute)
		_ = reader.SyncNow(context.Background(), false) // fails: the store is unreachable
	}
	create(t, writer, "during-outage")
	for i := 0; i < 25; i++ {
		b.Advance(time.Minute)
		_ = reader.SyncNow(context.Background(), false)
	}
	rpod.FailNext("list", 0)
	mustSync(t, reader, false)
	if !has(reader, "during-outage") {
		t.Fatal("the reader did not learn the delete once its listings worked again")
	}
	if got := metrics.DeleteTombstonesLearnedLate.Get() - before; got != 0 {
		t.Fatalf("learned-late counted %d for a key created during this pod's outage, want 0", got)
	}
}

// An undo whose definition cannot be read in a full listing is kept and applied
// when the definition is read on the next sync: the undo key may be older than the
// next poll's window, so dropping it would leave the un-deleted tombstone in force
// until the next full listing.
func TestTombstoneSync_UndoSurvivesAFailedDefinitionRead(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	create(t, writer, "spent")
	if err := writer.Undelete(context.Background(), "spent"); err != nil {
		t.Fatal(err)
	}
	b.Advance(15 * time.Minute) // the undo key is older than a poll's window
	reader, rpod := newPod(t, b, "fresh")
	rpod.FailNext("get", 1) // the definition's first read fails
	if err := reader.SyncNow(context.Background(), true); err == nil {
		t.Fatal("fixture: the first sync should report the unreadable definition")
	}
	b.Advance(time.Minute) // the next sync is a poll, whose window no longer holds the undo key
	mustSync(t, reader, false)
	if has(reader, "spent") {
		t.Fatal("a tombstone that was un-deleted is in force after its definition was finally read: the undo was lost")
	}
}
