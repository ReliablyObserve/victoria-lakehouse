package delete

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// writeCountPool counts the writes that reach the bucket and can fail the
// operations a test names.
type writeCountPool struct {
	*mockS3Pool
	mu         sync.Mutex
	uploads    int
	deletes    int
	failDelete bool
	failList   bool
}

func (p *writeCountPool) Upload(ctx context.Context, key string, data []byte) error {
	p.mu.Lock()
	p.uploads++
	p.mu.Unlock()
	return p.mockS3Pool.Upload(ctx, key, data)
}

func (p *writeCountPool) Delete(ctx context.Context, key string) error {
	p.mu.Lock()
	p.deletes++
	fail := p.failDelete
	p.mu.Unlock()
	if fail {
		return errInjected
	}
	return p.mockS3Pool.Delete(ctx, key)
}

func (p *writeCountPool) List(ctx context.Context, prefix string) ([]string, error) {
	p.mu.Lock()
	fail := p.failList
	p.mu.Unlock()
	if fail {
		return nil, errInjected
	}
	return p.mockS3Pool.List(ctx, prefix)
}

func (p *writeCountPool) writes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.uploads + p.deletes
}

// reloadStore is one pod's tombstone store over the shared bucket.
func reloadStore(t *testing.T, bucket *mockS3Pool, reaper bool, grace time.Duration) (*TombstoneStore, *writeCountPool) {
	t.Helper()
	pool := &writeCountPool{mockS3Pool: bucket}
	s := NewTombstoneStore()
	s.SetReaper(reaper)
	s.SetRetireGrace(grace)
	cfg := PersistenceConfig{Dir: t.TempDir(), Pool: pool, Prefix: "0/0/logs/"}
	if _, err := s.Restore(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	s.EnablePersistence(cfg)
	return s, pool
}

func permanentTombstone(id string) Tombstone {
	return Tombstone{ID: id, Query: "*", Mode: "permanent", CreatedAt: time.Now(), Tenants: []TenantRef{{}}, StartNs: 1, EndNs: 2}
}

func tombstoneKey(id string) string { return "0/0/logs/_tombstones/" + id + ".json" }

// A delete taken on one pod is read by another pod's reload, and the reload
// writes nothing.
func TestReload_AddsADeleteTakenElsewhere_WritingNothing(t *testing.T) {
	bucket := newMockS3Pool()
	a, _ := reloadStore(t, bucket, false, 0)
	w, wp := reloadStore(t, bucket, true, 0)
	a.Add(permanentTombstone("t1"))

	res, err := w.ReloadFromS3(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 1 || w.Count() != 1 {
		t.Fatalf("reload added %d, store holds %d, want 1 and 1", res.Added, w.Count())
	}
	if wp.writes() != 0 {
		t.Fatalf("the reload wrote %d times, want 0", wp.writes())
	}
	// A second reload finds nothing changed.
	res, err = w.ReloadFromS3(context.Background())
	if err != nil || res.Added+res.Updated+res.Retired+res.Dropped != 0 {
		t.Fatalf("second reload = %+v, %v; want nothing", res, err)
	}
}

// Progress made on one pod (a key reaped) is merged into another's copy.
func TestReload_MergesProgressMadeElsewhere(t *testing.T) {
	bucket := newMockS3Pool()
	a, _ := reloadStore(t, bucket, false, 0)
	w, _ := reloadStore(t, bucket, true, 0)
	ts := permanentTombstone("t1")
	ts.AffectedKeys = []string{"k1", "k2"}
	a.Add(ts)
	if _, err := w.ReloadFromS3(context.Background()); err != nil {
		t.Fatal(err)
	}
	a.Update("t1", func(cur *Tombstone) bool { cur.MarkReaped("k1"); return true })
	res, err := w.ReloadFromS3(context.Background())
	if err != nil || res.Updated != 1 {
		t.Fatalf("reload = %+v, %v; want one update", res, err)
	}
	got, _ := w.Get("t1")
	if !got.Reaped["k1"] {
		t.Fatalf("the reaped key did not arrive: %+v", got.Reaped)
	}
}

// A removal is a retired record first: another pod reads it and drops the
// tombstone; the object is deleted only after the grace; a failed delete leaves
// the retired record, never an active one, and a fresh pod does not adopt it.
func TestReload_RetiredRecordDropsTheTombstoneAndASurvivingObjectIsNotResurrected(t *testing.T) {
	bucket := newMockS3Pool()
	grace := 80 * time.Millisecond
	a, ap := reloadStore(t, bucket, true, grace)
	b, _ := reloadStore(t, bucket, false, grace)
	a.Add(permanentTombstone("t1"))
	if _, err := b.ReloadFromS3(context.Background()); err != nil || b.Count() != 1 {
		t.Fatalf("b did not load the tombstone: %v, count %d", err, b.Count())
	}

	ap.mu.Lock()
	ap.failDelete = true
	ap.mu.Unlock()
	a.Remove("t1") // retire: PUT the retired record; the delete waits for the grace

	raw, ok := bucket.Get(tombstoneKey("t1"))
	if !ok {
		t.Fatal("the object is gone at once: the retired record never reached S3")
	}
	var rec Tombstone
	if err := json.Unmarshal(raw, &rec); err != nil || !rec.Retired || rec.RetiredAt.IsZero() {
		t.Fatalf("the object holds %+v (%v), want a retired record", rec, err)
	}

	res, err := b.ReloadFromS3(context.Background())
	if err != nil || res.Retired != 1 || b.Count() != 0 {
		t.Fatalf("b reload = %+v, %v, count %d; want the tombstone dropped as retired", res, err, b.Count())
	}

	// The grace passes; the delete fails; the object stays, still retired.
	time.Sleep(2 * grace)
	a.FlushPending(context.Background())
	if !bucket.Has(tombstoneKey("t1")) {
		t.Fatal("the failing delete removed the object")
	}
	// A pod that starts now, and a pod that reloads now, do not adopt it.
	fresh, _ := reloadStore(t, bucket, false, grace)
	if fresh.Count() != 0 {
		t.Fatalf("a fresh pod adopted the retired tombstone (count %d)", fresh.Count())
	}
	if _, err := b.ReloadFromS3(context.Background()); err != nil || b.Count() != 0 {
		t.Fatalf("b resurrected it: %v, count %d", err, b.Count())
	}

	// The bucket recovers; the owed delete lands.
	ap.mu.Lock()
	ap.failDelete = false
	ap.mu.Unlock()
	if n := a.FlushPending(context.Background()); n != 0 {
		t.Fatalf("%d writes still owed", n)
	}
	if bucket.Has(tombstoneKey("t1")) {
		t.Fatal("the object survived the delete after the bucket recovered")
	}
}

// An un-delete on one pod reaches another within a reload, long before the
// rewrite delay.
func TestReload_UnDeletePropagates(t *testing.T) {
	bucket := newMockS3Pool()
	a, _ := reloadStore(t, bucket, false, time.Hour)
	w, _ := reloadStore(t, bucket, true, time.Hour)
	a.Add(permanentTombstone("t1"))
	if _, err := w.ReloadFromS3(context.Background()); err != nil || w.Count() != 1 {
		t.Fatal("w did not load the tombstone")
	}
	if err := a.TryRemove("t1"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ReloadFromS3(context.Background()); err != nil || w.Count() != 0 {
		t.Fatalf("the un-delete did not reach w: %v, count %d", err, w.Count())
	}
}

// A listing that failed says nothing about the objects: nothing is dropped.
func TestReload_FailedListingDropsNothing(t *testing.T) {
	bucket := newMockS3Pool()
	a, _ := reloadStore(t, bucket, false, 0)
	w, wp := reloadStore(t, bucket, true, 0)
	a.Add(permanentTombstone("t1"))
	if _, err := w.ReloadFromS3(context.Background()); err != nil || w.Count() != 1 {
		t.Fatal("w did not load the tombstone")
	}
	wp.mu.Lock()
	wp.failList = true
	wp.mu.Unlock()
	_ = bucket.Delete(context.Background(), tombstoneKey("t1")) // gone, but the listing cannot say so
	if _, err := w.ReloadFromS3(context.Background()); err == nil {
		t.Fatal("a failed listing reported success")
	}
	if w.Count() != 1 {
		t.Fatalf("a failed listing dropped the tombstone (count %d)", w.Count())
	}
	// The listing recovers and is complete: now the absence counts.
	wp.mu.Lock()
	wp.failList = false
	wp.mu.Unlock()
	res, err := w.ReloadFromS3(context.Background())
	if err != nil || res.Dropped != 1 || w.Count() != 0 {
		t.Fatalf("complete listing: %+v, %v, count %d; want the vanished tombstone dropped", res, err, w.Count())
	}
}

// What was never seen in S3, and what this pod still owes S3, is not dropped.
func TestReload_KeepsWhatIsNotYetInS3(t *testing.T) {
	bucket := newMockS3Pool()
	w, wp := reloadStore(t, bucket, true, 0)

	// A tombstone only on this pod (the S3 write is failing): owed, never seen.
	wp.mu.Lock()
	wp.failList = false
	wp.mu.Unlock()
	failing := &failUploadPool{writeCountPool: wp, fail: true}
	w.mu.Lock()
	w.persist.pool = failing
	w.mu.Unlock()
	w.Add(permanentTombstone("local"))
	if w.PendingS3Writes() == 0 {
		t.Fatal("the failed upload left nothing owed; the test proves nothing")
	}
	if _, err := w.ReloadFromS3(context.Background()); err != nil || w.Count() != 1 {
		t.Fatalf("reload dropped a tombstone S3 has not received yet: %v, count %d", err, w.Count())
	}

	// A tombstone this pod saw in S3 and still owes a newer write for: kept.
	other, _ := reloadStore(t, bucket, false, 0)
	other.Add(permanentTombstone("shared"))
	failing.fail = false
	if _, err := w.ReloadFromS3(context.Background()); err != nil {
		t.Fatal(err)
	}
	failing.fail = true
	w.Update("shared", func(cur *Tombstone) bool { cur.AffectedKeys = append(cur.AffectedKeys, "k"); return true })
	_ = bucket.Delete(context.Background(), tombstoneKey("shared"))
	if _, err := w.ReloadFromS3(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.Get("shared"); !ok {
		t.Fatal("reload dropped a tombstone this pod has a write pending for")
	}
}

type failUploadPool struct {
	*writeCountPool
	fail bool
}

func (p *failUploadPool) Upload(ctx context.Context, key string, data []byte) error {
	if p.fail {
		return errInjected
	}
	return p.writeCountPool.Upload(ctx, key, data)
}

// A retirement never drops a tombstone with an unfinished rewrite here: its
// records are the only trace of a replacement object.
func TestReload_RetiredRecordKeepsATombstoneWithAnUnfinishedRewrite(t *testing.T) {
	bucket := newMockS3Pool()
	w, wp := reloadStore(t, bucket, true, time.Hour)
	ts := permanentTombstone("t1")
	ts.Superseded = map[string]Supersession{"src": {NewKey: "new", State: SupersessionPrepared, At: time.Now()}}
	w.Add(ts)
	before := wp.writes()

	retired := ts
	retired.Retired, retired.RetiredAt = true, time.Now()
	data, _ := json.Marshal(retired)
	bucket.Put(tombstoneKey("t1"), data)

	if _, err := w.ReloadFromS3(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, ok := w.Get("t1")
	if !ok || got.Superseded["src"].State != SupersessionPrepared {
		t.Fatalf("the retirement dropped or changed a tombstone with an unfinished rewrite: %+v, %v", got, ok)
	}
	if wp.writes() != before {
		t.Fatalf("the reload wrote %d times, want 0", wp.writes()-before)
	}
}

// The reaper deletes the object of a tombstone that was retired long ago and
// whose delete never landed; a pod that is not a reaper leaves it.
func TestReload_ReaperDeletesAnOldRetiredObject(t *testing.T) {
	bucket := newMockS3Pool()
	old := permanentTombstone("t1")
	old.Retired, old.RetiredAt = true, time.Now().Add(-time.Hour)
	data, _ := json.Marshal(old)
	bucket.Put(tombstoneKey("t1"), data)

	notReaper, _ := reloadStore(t, bucket, false, time.Minute)
	notReaper.FlushPending(context.Background())
	if !bucket.Has(tombstoneKey("t1")) {
		t.Fatal("a pod that is not a reaper deleted the object")
	}
	reaper, _ := reloadStore(t, bucket, true, time.Minute)
	reaper.FlushPending(context.Background())
	if bucket.Has(tombstoneKey("t1")) {
		t.Fatal("the reaper left an hour-old retired object")
	}
}

type stubOwner map[string]bool

func (o stubOwner) OwnsPartition(p string) bool { return o[p] }

// The rewrite scheduler rewrites the partitions its pod owns.
func TestRewriteScheduler_RewritesOnlyOwnedPartitions(t *testing.T) {
	s := &RewriteScheduler{ownership: stubOwner{"dt=2026-10-15/hour=17": true}}
	if !s.owns("0/0/logs/dt=2026-10-15/hour=17/a.parquet") {
		t.Fatal("the owned partition is not rewritten")
	}
	if s.owns("0/0/logs/dt=2026-10-15/hour=18/a.parquet") {
		t.Fatal("a partition another writer owns is rewritten")
	}
	if !(&RewriteScheduler{}).owns("0/0/logs/dt=2026-10-15/hour=18/a.parquet") {
		t.Fatal("a scheduler with no ownership must rewrite everything (a single writer)")
	}
}
