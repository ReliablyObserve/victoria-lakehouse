package delete

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

const testMaxTTL = 30 * 24 * time.Hour

func withRetention(o *Options) {
	o.Expiry = func(t Tombstone) (time.Time, bool) { return ExpiryAt(t, testMaxTTL) }
}

func maintenance(o *Options) { o.Maintenance = true }

func deletes(b *deletetest.Bucket) (ops []deletetest.Op) {
	for _, op := range b.Log() {
		if op.Kind == "DELETE" {
			ops = append(ops, op)
		}
	}
	return ops
}

func TestExpiryAt(t *testing.T) {
	end := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	got, ok := ExpiryAt(Tombstone{EndNs: end}, testMaxTTL)
	if !ok || got.UnixNano() != end+int64(testMaxTTL+retentionMargin) {
		t.Fatalf("expiry = %v %v, want range end + retention + 1d", got, ok)
	}
	// A range that ends in the far future never expires.
	if _, ok := ExpiryAt(Tombstone{EndNs: math.MaxInt64 - 5}, testMaxTTL); ok {
		t.Fatal("an overflowing expiry must mean never")
	}
	// A task covers (-inf, timestamp]: its expiry follows the timestamp.
	if got, ok := ExpiryAt(Tombstone{StartNs: math.MinInt64, EndNs: end}, testMaxTTL); !ok || got.UnixNano() != end+int64(testMaxTTL+retentionMargin) {
		t.Fatalf("task expiry = %v %v", got, ok)
	}
}

// The GC is a deterministic predicate over the listing: once the data a
// tombstone covers is past retention, a maintenance pod removes its keys (the
// undo key first, the definition last), exactly once, and idempotently across
// restarts. A pod without maintenance never deletes.
func TestGC_SteadyState(t *testing.T) {
	b := deletetest.NewBucket()
	w1, w1pod := newSyncedPod(t, b, "gc-writer", withRetention, maintenance)
	sel, selpod := newSyncedPod(t, b, "gc-select", withRetention)

	now := b.Now().UnixNano()
	old := hideTombstone("old")
	old.EndNs = now
	young := hideTombstone("young")
	young.EndNs = now + int64(40*24*time.Hour)
	undone := hideTombstone("undone")
	undone.EndNs = now
	mustCreate(t, w1, old)
	mustCreate(t, w1, young)
	mustCreate(t, w1, undone)
	if err := w1.Undelete(context.Background(), "undone"); err != nil {
		t.Fatal(err)
	}
	startKeys := len(b.Keys(tombPrefix()))

	// Before retention passes nothing is deleted anywhere.
	b.Advance(testMaxTTL)
	mustSync(t, w1, true)
	mustSync(t, sel, true)
	if len(deletes(b)) != 0 {
		t.Fatalf("keys were deleted before their retention passed: %v", deletes(b))
	}

	// Retention + the margin pass for "old" and "undone", not for "young".
	b.Advance(retentionMargin + time.Hour)
	w1pod.ResetCounts()
	selpod.ResetCounts()
	mustSync(t, sel, true)
	if selpod.Counts().Deletes != 0 {
		t.Fatal("a pod without maintenance deleted keys")
	}
	if has(sel, "old") {
		t.Fatal("a reader still enforces a tombstone past its retention")
	}
	if !has(sel, "young") {
		t.Fatal("a reader dropped a tombstone that is not expired")
	}
	mustSync(t, w1, true)
	ops := deletes(b)
	// old has only its definition; undone has its definition and its undo key. The
	// GC deletes only keys it listed.
	removed := map[string]int{}
	for i, op := range ops {
		pk, ok := parseKey(tombPrefix(), op.Key)
		if !ok {
			t.Fatalf("deleted a key that is not a tombstone object: %s", op.Key)
		}
		if pk.id == "young" {
			t.Fatalf("deleted a key of an unexpired tombstone: %s", op.Key)
		}
		removed[pk.id]++
		if pk.kind == kindDefinition {
			// The definition goes after every other key of the tombstone.
			for _, later := range ops[i+1:] {
				if lk, _ := parseKey(tombPrefix(), later.Key); lk.id == pk.id {
					t.Fatalf("%s was deleted before its other keys: %s still to go", op.Key, later.Key)
				}
			}
		}
	}
	if removed["old"] == 0 || removed["undone"] == 0 {
		t.Fatalf("expired tombstones not collected: %v", removed)
	}
	left := b.Keys(tombPrefix())
	for _, k := range left {
		if pk, _ := parseKey(tombPrefix(), k); pk.id != "young" {
			t.Fatalf("key %s of an expired tombstone survived the GC", k)
		}
	}
	if len(left) >= startKeys {
		t.Fatal("nothing was collected")
	}

	// Steady state: another pass, and another maintenance pod (the GC runs on every
	// one of them, with no placement), delete nothing more.
	before := len(deletes(b))
	mustSync(t, w1, true)
	w2, _ := newSyncedPod(t, b, "gc-writer-2", withRetention, maintenance)
	mustSync(t, w2, true)
	if len(deletes(b)) != before {
		t.Fatalf("GC is not idempotent: %d deletes after, %d before", len(deletes(b)), before)
	}
}

// The GC removes the definition last, so a crash leaves it (and the work) rather
// than keys without a definition; a restarted pod finishes the job.
func TestGC_CrashLeavesTheDefinitionAndARestartFinishes(t *testing.T) {
	b := deletetest.NewBucket()
	w, wpod := newSyncedPod(t, b, "w", withRetention, maintenance)
	ts := hideTombstone("old")
	ts.EndNs = b.Now().UnixNano()
	mustCreate(t, w, ts)
	if err := w.Undelete(context.Background(), "old"); err != nil {
		t.Fatal(err)
	}
	b.Advance(testMaxTTL + 2*retentionMargin)

	// Deletes fail after the second one: the definition is still there.
	wpod.FailNext("delete", 100)
	_ = w.SyncNow(context.Background(), true)
	wpod.FailNext("delete", 0)
	keys := b.Keys(tombPrefix())
	foundDef := false
	for _, k := range keys {
		if pk, _ := parseKey(tombPrefix(), k); pk.kind == kindDefinition {
			foundDef = true
		}
	}
	if !foundDef {
		t.Fatal("the definition was removed before the failed deletes of its other keys")
	}
	// The same pod retries at its next full listing.
	mustSync(t, w, true)
	if n := len(b.Keys(tombPrefix())); n != 0 {
		t.Fatalf("%d keys left after the retry", n)
	}

	// A crash that lost the pod's memory: a fresh pod finishes the work from the
	// listing alone.
	ts2 := hideTombstone("old2")
	ts2.EndNs = b.Now().Add(-testMaxTTL - 2*retentionMargin).UnixNano()
	mustCreate(t, w, ts2)
	fresh, _ := newSyncedPod(t, b, "w-restarted", withRetention, maintenance)
	if n := len(b.Keys(tombPrefix())); n != 0 {
		t.Fatalf("a restarted maintenance pod left %d expired keys", n)
	}
	if has(fresh, "old2") {
		t.Fatal("an expired tombstone is enforced")
	}
}

// With retention off nothing expires, and nothing is ever deleted.
func TestGC_NoRetentionNeverDeletes(t *testing.T) {
	b := deletetest.NewBucket()
	w, _ := newSyncedPod(t, b, "w", maintenance)
	mustCreate(t, w, hideTombstone("forever"))
	b.Advance(100 * 365 * 24 * time.Hour)
	mustSync(t, w, true)
	if len(deletes(b)) != 0 || !has(w, "forever") {
		t.Fatal("a tombstone expired with retention off")
	}
}

// A short listing at GC time must not delete anything that is live.
func TestGC_ShortListingNeverDeletesLiveKeys(t *testing.T) {
	b := deletetest.NewBucket()
	w, pod := newSyncedPod(t, b, "w", withRetention, maintenance)
	ts := hideTombstone("live")
	ts.EndNs = b.Now().Add(time.Hour).UnixNano()
	mustCreate(t, w, ts)
	for i := 0; i < 5; i++ {
		mustCreate(t, w, ts2(i, b))
	}
	pod.ShortListOnce(1)
	mustSync(t, w, true)
	if len(deletes(b)) != 0 {
		t.Fatalf("a short listing caused DELETEs: %v", deletes(b))
	}
	if !strings.Contains(strings.Join(b.Keys(tombPrefix()), ","), "-live.json") {
		t.Fatal("the live definition is gone")
	}
}

func ts2(i int, b *deletetest.Bucket) Tombstone {
	t := hideTombstone("n" + string(rune('a'+i)))
	t.EndNs = b.Now().Add(time.Hour).UnixNano()
	return t
}

// The GC acts on full listings only. An undo key is older than a poll's window by
// the time its tombstone expires, so a poll never lists it: a GC that ran after a
// poll would delete the definition and leave the undo key behind for ever. The
// tombstone expires during a poll here; nothing may be deleted then, and the next
// full listing removes both keys.
func TestGC_RunsOnFullListingsAndCollectsAnOldUndoKey(t *testing.T) {
	b := deletetest.NewBucket()
	w, wpod := newSyncedPod(t, b, "gc-writer", withRetention, maintenance)
	ts := hideTombstone("undone")
	ts.EndNs = b.Now().UnixNano() - int64(testMaxTTL+retentionMargin) + int64(30*time.Second)
	mustCreate(t, w, ts)
	if err := w.Undelete(context.Background(), "undone"); err != nil {
		t.Fatal(err)
	}
	if n := len(b.Keys(tombPrefix())); n != 2 {
		t.Fatalf("fixture: %d keys, want the definition and its undo", n)
	}
	wpod.ResetCounts()

	b.Advance(time.Minute) // the tombstone is now past retention; this sync is a poll
	mustSync(t, w, false)
	if wpod.Counts().Deletes != 0 {
		t.Fatal("the GC ran after a poll, whose listing holds no old undo key")
	}
	if n := len(b.Keys(tombPrefix())); n != 2 {
		t.Fatalf("%d keys after the poll, want both still there", n)
	}
	mustSync(t, w, true)
	if n := len(b.Keys(tombPrefix())); n != 0 {
		t.Fatalf("%d keys left after the full listing, want the undo key and the definition both collected: %v", n, b.Keys(tombPrefix()))
	}
}
