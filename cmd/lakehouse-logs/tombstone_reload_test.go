package main

// Multi-instance tests of the tombstone reload: pods share one bucket, each with
// its own endpoint (its requests are counted on their own) and its own tombstone
// store, wired the way run() wires it (restore, persistence, the reload loop, the
// write retry, the rewrite scheduler with writer ownership).

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
)

type reloadPod struct {
	name  string
	cfg   *config.Config
	store *parquets3.Storage
	ts    *delete.TombstoneStore
	ep    *fakeS3
	stop  chan struct{}
}

// newReloadPod builds a pod over the shared bucket with the tombstone wiring of
// run(): reaper and retire grace, restore, persistence, the store on the storage,
// the write retry and the reload loop.
func newReloadPod(t *testing.T, bucket *fakeS3, name string, role config.Role) *reloadPod {
	t.Helper()
	ep := newFakeS3On(bucket)
	t.Cleanup(ep.Close)
	cfg := podConfig(t, role, ep.URL())
	cfg.Compaction.Enabled = false
	cfg.Retention.Enabled = false
	cfg.Delete.Enabled = true
	cfg.Delete.RewriteDelay = 0
	cfg.Delete.VerifyInterval = 50 * time.Millisecond
	cfg.Delete.TombstoneReloadInterval = 50 * time.Millisecond
	store := newPod(t, cfg)
	ts := delete.NewTombstoneStore()
	ts.SetReaper(cfg.RunsMaintenance())
	ts.SetRetireGrace(tombstoneRetireGrace(cfg))
	pc := delete.PersistenceConfig{Dir: t.TempDir(), Pool: &s3PoolAdapter{pool: store.Pool()}, Prefix: cfg.AutoPrefix()}
	if _, err := ts.Restore(context.Background(), pc); err != nil {
		t.Fatal(err)
	}
	ts.EnablePersistence(pc)
	store.SetTombstoneStore(ts)
	p := &reloadPod{name: name, cfg: cfg, store: store, ts: ts, ep: ep, stop: make(chan struct{})}
	t.Cleanup(func() { close(p.stop) })
	return p
}

// startLoops starts the loops run() starts for the tombstone store.
func (p *reloadPod) startLoops() {
	startTombstoneWriteRetry(p.cfg, p.ts, p.stop)
	startTombstoneReload(p.cfg, p.ts, p.stop)
}

func (p *reloadPod) tombstoneKey(id string) string {
	return delete.TombstonePrefix(p.cfg.AutoPrefix()) + id + ".json"
}

// partialDelete is a permanent delete of the first three rows of the first
// seeded object (rows at base, base+1s, base+2s).
func partialDelete(id string) delete.Tombstone {
	base := hourBase(48 * time.Hour)
	return delete.Tombstone{
		ID: id, Query: "*", Mode: "permanent", CreatedAt: time.Now().Add(-2 * time.Hour),
		Tenants: []delete.TenantRef{{}}, StartNs: base.UnixNano(), EndNs: base.Add(2 * time.Second).UnixNano(),
	}
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Select pod A takes a delete. Writer W, which never saw the request, reloads it
// and rewrites the objects; select pod B, which never loaded the tombstone, returns
// the rewritten data after a manifest refresh. Before the reload a delete taken by
// a select pod was enforced elsewhere, and rewritten, only after a restart.
func TestTombstoneReload_SelectTakesTheDelete_WriterRewrites_OtherSelectSeesIt(t *testing.T) {
	bucket := newFakeS3()
	defer bucket.Close()
	w := newReloadPod(t, bucket, "w", config.RoleInsert)
	wantRows := seedFlushes(t, w.store, w.cfg, 5, seedGroup{files: 3, age: 48 * time.Hour})
	a := newReloadPod(t, bucket, "a", config.RoleSelect)
	b := newReloadPod(t, bucket, "b", config.RoleSelect)
	bucket.Age(time.Hour)
	ctx := context.Background()
	for _, p := range []*reloadPod{w, a, b} {
		if err := p.store.RefreshManifest(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// W: reload loop plus the rewrite scheduler, as run() starts them.
	w.store.SetWriterPeers([]string{"w:9428"})
	w.startLoops()
	rewrite := startDeleteRewrite(w.cfg, w.store, nil, w.ts, newStorageClassDetector(w.cfg), "w:9428")
	if rewrite == nil {
		t.Fatal("no rewrite scheduler on the writer")
	}
	defer rewrite.Stop()
	a.startLoops()
	b.startLoops()

	a.ts.Add(partialDelete("t-del"))

	waitFor(t, "the writer to rewrite the objects", 30*time.Second, func() bool {
		rows, _ := bucketRows(t, bucket)
		return rows == int64(wantRows-3)
	})
	waitFor(t, "the tombstone to retire", 30*time.Second, func() bool { return w.ts.Count() == 0 })

	// B has no tombstone store content of its own that hides anything: what it
	// returns is the rewritten data.
	if err := b.store.RefreshManifest(ctx); err != nil {
		t.Fatal(err)
	}
	if got := queryAll(t, b.store); got != wantRows-3 {
		t.Errorf("select pod B returns %d rows after the writer rewrote, want %d", got, wantRows-3)
	}
	// The retirement reaches A, which took the delete, through its reload.
	waitFor(t, "select pod A to drop the retired tombstone", 10*time.Second, func() bool { return a.ts.Count() == 0 })
	// The object holds a retired record until the grace has passed.
	raw, ok := bucket.get("test-bucket", w.tombstoneKey("t-del"))
	if ok {
		var rec delete.Tombstone
		if err := json.Unmarshal(raw, &rec); err != nil || !rec.Retired {
			t.Errorf("the tombstone object holds %+v (%v), want a retired record until its delete", rec, err)
		}
	}
	// Select pods wrote only the tombstone itself (A), never an object of data.
	for _, p := range []*reloadPod{a, b} {
		for _, r := range bucketWrites(p.ep.Requests()) {
			if !strings.Contains(r.Key, "_tombstones/") {
				t.Errorf("select pod %s wrote %s %s", p.name, r.Op, r.Key)
			}
		}
	}
	if w := bucketWrites(b.ep.Requests()); len(w) != 0 {
		t.Errorf("select pod B (it only reloads) wrote %d times: %v", len(w), writesOf(w))
	}
}

// An un-delete taken on a select pod reaches a reading pod within a reload: the
// rows it hid come back, long before the rewrite delay.
func TestTombstoneReload_UnDeletePropagatesToReaders(t *testing.T) {
	bucket := newFakeS3()
	defer bucket.Close()
	w := newReloadPod(t, bucket, "w", config.RoleInsert)
	wantRows := seedFlushes(t, w.store, w.cfg, 5, seedGroup{files: 3, age: 48 * time.Hour})
	a := newReloadPod(t, bucket, "a", config.RoleSelect)
	b := newReloadPod(t, bucket, "b", config.RoleSelect)
	bucket.Age(time.Hour)
	ctx := context.Background()
	for _, p := range []*reloadPod{a, b} {
		if err := p.store.RefreshManifest(ctx); err != nil {
			t.Fatal(err)
		}
	}
	a.startLoops()
	b.startLoops()

	ts := partialDelete("t-undo")
	ts.CreatedAt = time.Now() // inside the rewrite delay
	ts.Mode = "permanent"
	a.cfg.Delete.RewriteDelay = time.Hour
	a.ts.Add(ts)

	waitFor(t, "B to enforce the delete", 10*time.Second, func() bool { return queryAll(t, b.store) == wantRows-3 })
	if err := a.ts.TryRemove("t-undo"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "B to see the rows again", 10*time.Second, func() bool { return queryAll(t, b.store) == wantRows })
	if rows, _ := bucketRows(t, bucket); rows != int64(wantRows) {
		t.Errorf("the bucket holds %d rows, want %d: nothing was rewritten", rows, wantRows)
	}
}

// A failed DELETE of a retired tombstone's object does not bring the tombstone
// back on another pod, or on a pod that starts later.
func TestTombstoneReload_FailedDeleteDoesNotResurrect(t *testing.T) {
	bucket := newFakeS3()
	defer bucket.Close()
	w := newReloadPod(t, bucket, "w", config.RoleInsert)
	b := newReloadPod(t, bucket, "b", config.RoleSelect)
	w.startLoops()
	b.startLoops()

	w.ts.Add(partialDelete("t-gone"))
	waitFor(t, "B to load the tombstone", 10*time.Second, func() bool { return b.ts.Count() == 1 })

	bucket.FailDeletes("_tombstones/")
	w.ts.Remove("t-gone") // retire: the record is written, the delete waits and then fails
	defer func() {
		if t.Failed() {
			for _, r := range append(w.ep.Requests(), b.ep.Requests()...) {
				if strings.Contains(r.Key, "t-gone") {
					t.Logf("%s %s %d", r.Op, r.Key, r.Status)
				}
			}
		}
	}()
	waitFor(t, "B to drop it", 10*time.Second, func() bool { return b.ts.Count() == 0 })
	time.Sleep(2 * tombstoneRetireGrace(w.cfg)) // the delete is due and keeps failing
	if _, ok := bucket.get("test-bucket", w.tombstoneKey("t-gone")); !ok {
		for _, r := range append(w.ep.Requests(), b.ep.Requests()...) {
			if strings.Contains(r.Key, "t-gone") {
				t.Logf("%s %s %d", r.Op, r.Key, r.Status)
			}
		}
		t.Fatal("the failing delete removed the object; the test proves nothing")
	}

	// A pod that starts now restores from the bucket: the retired record is not a tombstone.
	c := newReloadPod(t, bucket, "c", config.RoleSelect)
	if c.ts.Count() != 0 {
		t.Errorf("a pod that started after the failed delete adopted the tombstone (count %d)", c.ts.Count())
	}
	time.Sleep(300 * time.Millisecond)
	if b.ts.Count() != 0 {
		t.Errorf("select pod B resurrected the tombstone (count %d)", b.ts.Count())
	}
	bucket.FailDeletes("")
	waitFor(t, "the owed delete to land", 15*time.Second, func() bool {
		_, ok := bucket.get("test-bucket", w.tombstoneKey("t-gone"))
		return !ok
	})
}

// A reload during a rewrite writes nothing, and a retirement that arrives while
// the rewrite is unfinished does not drop its records.
func TestTombstoneReload_DuringARewriteIssuesNoPuts(t *testing.T) {
	bucket := newFakeS3()
	defer bucket.Close()
	w := newReloadPod(t, bucket, "w", config.RoleInsert)
	ts := partialDelete("t-busy")
	ts.Superseded = map[string]delete.Supersession{"src.parquet": {NewKey: "new.parquet", State: delete.SupersessionPrepared, At: time.Now()}}
	w.ts.Add(ts)
	w.ts.FlushPending(context.Background())
	w.ep.Reset()

	if _, err := w.ts.ReloadFromS3(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Another pod retires it (the record is overwritten with a retired copy).
	retired := ts
	retired.Retired, retired.RetiredAt = true, time.Now()
	data, _ := json.Marshal(retired)
	bucket.Put("test-bucket", w.tombstoneKey("t-busy"), data)
	if _, err := w.ts.ReloadFromS3(context.Background()); err != nil {
		t.Fatal(err)
	}
	if wr := bucketWrites(w.ep.Requests()); len(wr) != 0 {
		t.Errorf("the reloads wrote %d times: %v", len(wr), writesOf(wr))
	}
	got, ok := w.ts.Get("t-busy")
	if !ok || got.Superseded["src.parquet"].State != delete.SupersessionPrepared {
		t.Errorf("the rewrite's record did not survive: %+v, %v", got, ok)
	}
}

// Two writers hold the same tombstone; exactly one of them rewrites an object, so
// exactly one replacement exists.
func TestTombstoneReload_TwoWritersRewriteOnceEach(t *testing.T) {
	bucket := newFakeS3()
	defer bucket.Close()
	w1 := newReloadPod(t, bucket, "w1", config.RoleInsert)
	wantRows := seedFlushes(t, w1.store, w1.cfg, 5, seedGroup{files: 3, age: 48 * time.Hour})
	w2 := newReloadPod(t, bucket, "w2", config.RoleInsert)
	a := newReloadPod(t, bucket, "a", config.RoleSelect)
	bucket.Age(time.Hour)
	ctx := context.Background()
	before := parquetKeys(bucket)
	if len(before) != 3 {
		t.Fatalf("seed: %d objects, want 3", len(before))
	}
	writers := []string{"w1:9428", "w2:9428"}
	for _, p := range []*reloadPod{w1, w2} {
		if err := p.store.RefreshManifest(ctx); err != nil {
			t.Fatal(err)
		}
		p.store.SetWriterPeers(writers)
		p.startLoops()
	}
	r1 := startDeleteRewrite(w1.cfg, w1.store, nil, w1.ts, newStorageClassDetector(w1.cfg), "w1:9428")
	r2 := startDeleteRewrite(w2.cfg, w2.store, nil, w2.ts, newStorageClassDetector(w2.cfg), "w2:9428")
	defer r1.Stop()
	defer r2.Stop()
	a.startLoops()
	bucket.HoldDeletes(300 * time.Millisecond) // a second rewrite of the same object would upload before the first one's delete lands
	a.ts.Add(partialDelete("t-two"))

	waitFor(t, "the rewrite", 30*time.Second, func() bool {
		rows, _ := bucketRows(t, bucket)
		return rows == int64(wantRows-3)
	})
	time.Sleep(1500 * time.Millisecond) // a late second rewrite would land here
	rows, objects := bucketRows(t, bucket)
	if rows != int64(wantRows-3) {
		t.Errorf("the bucket holds %d rows, want %d (a second rewrite would leave the deleted rows' neighbours twice)", rows, wantRows-3)
	}
	now := parquetKeys(bucket)
	var added []string
	seen := map[string]bool{}
	for _, k := range before {
		seen[k] = true
	}
	for _, k := range now {
		if !seen[k] {
			added = append(added, k)
		}
	}
	if len(added) != 1 || objects != 3 {
		t.Errorf("after the rewrite the bucket holds %d objects with %d replacements %v, want 3 objects and exactly 1 replacement", objects, len(added), added)
	}
}

func parquetKeys(b *fakeS3) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var keys []string
	for k := range b.objs {
		if strings.HasSuffix(k, ".parquet") {
			keys = append(keys, k)
		}
	}
	return keys
}
