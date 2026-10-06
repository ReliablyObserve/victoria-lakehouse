package delete

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// r4World is a bucket of n objects, each holding one kept and one deleted row,
// a manifest that has listed them, and a permanent tombstone over the deleted
// rows.
type r4World struct {
	pool  *mockRewriterPool
	m     *manifest.Manifest
	store *TombstoneStore
	keys  []string
}

func newR4World(t *testing.T, n int, createdAt time.Time) *r4World {
	t.Helper()
	w := &r4World{pool: newMockRewriterPool(), store: NewTombstoneStore()}
	rows := map[string]int64{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("logs/dt=2026-03-01/hour=07/r4-%02d.parquet", i)
		w.pool.Put(k, buildTestParquet(t, []schema.LogRow{
			{TimestampUnixNano: int64(1000 + i), Body: fmt.Sprintf("keep-%d", i), SeverityText: "info", ServiceName: "web"},
			{TimestampUnixNano: int64(2000 + i), Body: fmt.Sprintf("drop-%d", i), SeverityText: "error", ServiceName: "web"},
		}))
		rows[k] = 2
		w.keys = append(w.keys, k)
	}
	w.m = newTestManifest(t, rows) // listed once
	// A zero createdAt: the tombstone is created after that listing began.
	if createdAt.IsZero() {
		time.Sleep(2 * time.Millisecond)
		createdAt = time.Now()
	}
	w.store.Add(Tombstone{
		Tenants: []TenantRef{{}}, ID: "ts-r4", Query: `severity_text:="error"`,
		StartNs: 0, EndNs: 1 << 62, AffectedKeys: append([]string(nil), w.keys...),
		CreatedAt: createdAt, Mode: "permanent", Reaped: map[string]bool{},
	})
	return w
}

func (w *r4World) listing(skip string) []manifest.ListedObject {
	var out []manifest.ListedObject
	for _, k := range w.pool.Keys() {
		if k == skip {
			continue
		}
		data, _ := w.pool.Get(k)
		out = append(out, manifest.ListedObject{Key: k, Size: int64(len(data))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (w *r4World) scheduler() *RewriteScheduler {
	return NewRewriteScheduler(RewriteSchedulerConfig{
		Store: w.store, Rewriter: NewRewriter(w.pool, "logs/", 1000, "logs"),
		Detector: NewStorageClassDetector(nil), RewriteDelay: time.Hour,
		AllowedClasses: []string{"STANDARD"}, Manifest: w.m,
	})
}

// Review regression (#404 round 4, F3): a complete-looking listing that
// silently misses one live object (less than half dropped, so no HEAD sample)
// made the scheduler read the absent key as "already reaped" and complete the
// tombstone; the next listing adopted the object back with its deleted row.
// The scheduler now HEADs an absent, unretired key: the object exists, so the
// key stays pending and the tombstone stays active until a listing adopts it.
func TestReview404R4_SchedulerSilentSmallDropKeepsKeyPending(t *testing.T) {
	w := newR4World(t, 10, time.Now().Add(-2*time.Hour))
	missed := w.keys[0]
	time.Sleep(2 * time.Millisecond)
	if !w.m.ApplyListing(w.listing(missed), time.Now()) {
		t.Fatal("fixture: a 10% drop must be believed (no HEAD sample below half)")
	}
	if w.m.HasKey(missed) {
		t.Fatal("fixture: the silently dropped key is still tracked")
	}
	sched := w.scheduler()
	before := metrics.DeleteRewriteDeferred.Get("absent_but_exists")
	sched.RunOnce(context.Background())
	if _, active := w.store.Get("ts-r4"); !active {
		drop, _ := bodiesIn(t, w.pool)
		t.Fatalf("the tombstone completed while %s, which a listing silently missed, still holds %d deleted row(s)", missed, drop)
	}
	if got := metrics.DeleteRewriteDeferred.Get("absent_but_exists") - before; got == 0 {
		t.Fatal("the deferral of the absent-but-existing key was not counted")
	}
	ts, _ := w.store.Get("ts-r4")
	if ts.Handled(missed) {
		t.Fatalf("%s was marked handled although its object still exists", missed)
	}
	// A listing that sees the object again lets the tombstone finish exactly.
	for i := 0; i < 3 && w.store.Count() > 0; i++ {
		time.Sleep(2 * time.Millisecond)
		if !w.m.ApplyListing(w.listing(""), time.Now()) {
			t.Fatal("fixture: full listing rejected")
		}
		sched.RunOnce(context.Background())
	}
	if w.store.Count() != 0 {
		t.Fatal("the tombstone did not complete once the object was listed again")
	}
	if drop, keep := bodiesIn(t, w.pool); drop != 0 || keep != len(w.keys) {
		t.Fatalf("after completion: deleted rows=%d (want 0), kept rows=%d (want %d)", drop, keep, len(w.keys))
	}
}

// An absent key whose object is really gone (404) is still reaped, and an
// absent key the manifest retired is reaped without a HEAD; an absent key whose
// existence cannot be checked (no HEAD) or whose HEAD fails stays pending.
func TestReview404R4_SchedulerAbsentKeyOutcomes(t *testing.T) {
	ctx := context.Background()
	t.Run("gone", func(t *testing.T) {
		w := newR4World(t, 4, time.Now().Add(-2*time.Hour))
		gone := w.keys[0]
		_ = w.pool.Delete(ctx, gone)
		time.Sleep(2 * time.Millisecond)
		w.m.ApplyListing(w.listing(""), time.Now())
		w.scheduler().RunOnce(ctx)
		if ts, ok := w.store.Get("ts-r4"); ok && !ts.Handled(gone) {
			t.Fatalf("a key whose object is gone was not reaped: %+v", ts)
		}
	})
	t.Run("retired", func(t *testing.T) {
		w := newR4World(t, 4, time.Now().Add(-2*time.Hour))
		k := w.keys[0]
		w.m.Retire(k, "logs/dt=2026-03-01/hour=07/compacted.parquet", true)
		calls := 0
		s := w.scheduler()
		s.objectExists = func(context.Context, string) (bool, error) { calls++; return true, nil }
		if !s.absentObjectSettled(ctx, k) || calls != 0 {
			t.Fatalf("a retired key must settle without a HEAD (calls=%d)", calls)
		}
	})
	t.Run("unknown", func(t *testing.T) {
		w := newR4World(t, 4, time.Now().Add(-2*time.Hour))
		s := w.scheduler()
		s.objectExists = nil
		if s.absentObjectSettled(ctx, "logs/dt=2026-03-01/hour=07/nothing.parquet") {
			t.Fatal("without an existence check an unretired absent key must stay pending")
		}
		s.objectExists = func(context.Context, string) (bool, error) { return false, fmt.Errorf("503") }
		if s.absentObjectSettled(ctx, "logs/dt=2026-03-01/hour=07/nothing.parquet") {
			t.Fatal("a failed HEAD must keep the key pending")
		}
	})
}

// Review regression (#404 round 4, mutation M2): the completion gate is a
// complete listing that began after the tombstone was created, not merely "a
// complete listing has run". A file a peer publishes after the last complete
// listing began and before the tombstone is created is in no work list; a
// gate on Listed() alone completes the tombstone over it and its deleted rows
// show once a listing adopts it.
func TestReview404R4_SchedulerWaitsForAListingAfterTheTombstone(t *testing.T) {
	ctx := context.Background()
	w := newR4World(t, 2, time.Time{}) // tombstone created after the fixture's listing
	// A peer published this object between that listing and the tombstone.
	peer := "logs/dt=2026-03-01/hour=07/r4-peer.parquet"
	w.pool.Put(peer, buildTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 3000, Body: "keep-peer", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 3001, Body: "drop-peer", SeverityText: "error", ServiceName: "web"},
	}))
	// Rewrite delay passed: make the tombstone eligible without moving CreatedAt.
	sched := w.scheduler()
	sched.rewriteDelay = 0
	if ts, _ := w.store.Get("ts-r4"); w.m.CompleteSince(ts.CreatedAt) {
		t.Fatal("fixture: the last complete listing must predate the tombstone")
	}
	sched.RunOnce(ctx)
	if w.store.Count() == 0 {
		drop, _ := bodiesIn(t, w.pool)
		t.Fatalf("the tombstone completed without a complete listing after it was created; %d deleted row(s) remain in the bucket", drop)
	}
	for i := 0; i < 4 && w.store.Count() > 0; i++ {
		time.Sleep(2 * time.Millisecond)
		if !w.m.ApplyListing(w.listing(""), time.Now()) {
			t.Fatal("fixture: listing rejected")
		}
		sched.RunOnce(ctx)
	}
	if w.store.Count() != 0 {
		t.Fatal("the tombstone did not complete after a complete listing")
	}
	if drop, keep := bodiesIn(t, w.pool); drop != 0 || keep != 3 {
		t.Fatalf("deleted rows=%d (want 0), kept=%d (want 3)", drop, keep)
	}
}

// Residual (documented, #404 round 4): the scheduler's per-key HEAD protects
// the keys in a tombstone's work list. An object that enters the manifest and
// is silently dropped again by the next complete-looking listing BEFORE any
// pass has discovered it never joins the work list, so the tombstone completes
// over it and its deleted rows show once a later listing adopts it. Needs two
// consecutive listings, the second silently missing that object, between two
// scheduler passes. This test asserts today's behaviour; when the residual is
// fixed it fails with "fixed": flip it to assert the tombstone stays active.
func TestReview404R4_Residual_SilentDropBeforeDiscovery(t *testing.T) {
	ctx := context.Background()
	w := newR4World(t, 2, time.Time{})
	peer := "logs/dt=2026-03-01/hour=07/r4-peer.parquet"
	w.pool.Put(peer, buildTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 3000, Body: "keep-peer", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 3001, Body: "drop-peer", SeverityText: "error", ServiceName: "web"},
	}))
	sched := w.scheduler()
	sched.rewriteDelay = 0
	time.Sleep(2 * time.Millisecond)
	if !w.m.ApplyListing(w.listing(""), time.Now()) || !w.m.HasKey(peer) {
		t.Fatal("fixture: the peer object was not adopted")
	}
	time.Sleep(2 * time.Millisecond)
	if !w.m.ApplyListing(w.listing(peer), time.Now()) || w.m.HasKey(peer) {
		t.Fatal("fixture: the silent drop was not believed")
	}
	sched.RunOnce(ctx)
	drop, _ := bodiesIn(t, w.pool)
	if w.store.Count() != 0 || drop != 1 {
		t.Fatalf("fixed: the tombstone no longer completes over an object a listing silently dropped before discovery (active=%d, deleted rows left=%d); flip this test", w.store.Count(), drop)
	}
}
