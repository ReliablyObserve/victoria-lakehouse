package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Review regression (#404 round 4, F2): Tier B deleted a LIVE object that one
// complete-looking listing silently missed (< half dropped, so no HEAD sample).
// Tier B's own LIST + HEAD confirm only that the object EXISTS; nothing per
// object proved it unreferenced. It now needs a retirement record.
func TestReview404R4_TierBKeepsLiveObjectAfterSilentSmallDrop(t *testing.T) {
	pool := newListingPool()
	ctx := context.Background()
	m := manifest.New("bkt", "logs/")
	var objs []manifest.ListedObject
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("logs/dt=2026-01-01/hour=00/f%02d.parquet", i)
		_ = pool.UploadWithMtime(ctx, k, []byte("x"), time.Now().Add(-10*time.Hour))
		m.AddFile("dt=2026-01-01/hour=00", manifest.FileInfo{Key: k, Size: 1, RowCount: 1})
		objs = append(objs, manifest.ListedObject{Key: k, Size: 1})
	}
	if !m.ApplyListing(objs, time.Now()) {
		t.Fatal("fixture")
	}
	// One successful LIST, every prefix covered, silently misses one live key.
	if !m.ApplyListing(objs[1:], time.Now()) {
		t.Fatal("fixture: small drop rejected")
	}
	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Hour,
	})
	deleted, err := sweep.RunTierB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deleted > 0 {
		t.Fatalf("Tier B deleted %d LIVE object(s) the manifest only lost through one silently short listing", deleted)
	}
}

// Review regression (#404 round 4, F4): compaction's canRetire was Listed()
// only. A tombstone created AFTER the last complete listing began was completed
// by compaction although the manifest cannot know a file published between
// that listing's start and the tombstone (the scheduler refuses exactly this
// via CompleteSince). Compaction now uses the same per-tombstone condition, and
// still completes the tombstone once such a listing has been applied.
func TestReview404R4_CompactionRetiresTombstoneOnlyAfterCompleteSince(t *testing.T) {
	m := manifest.New("bkt", "logs/")
	a := "logs/dt=2026-01-01/hour=00/a.parquet"
	b := "logs/dt=2026-01-01/hour=00/b.parquet"
	m.AddFile("dt=2026-01-01/hour=00", manifest.FileInfo{Key: a, Size: 1, RowCount: 1})
	m.AddFile("dt=2026-01-01/hour=00", manifest.FileInfo{Key: b, Size: 1, RowCount: 1})
	if !m.ApplyListing([]manifest.ListedObject{{Key: a, Size: 1}, {Key: b, Size: 1}}, time.Now()) {
		t.Fatal("fixture")
	}
	time.Sleep(2 * time.Millisecond)
	// A peer publishes logs/.../peer.parquet now (not in this manifest), then
	// the tombstone is created; its work list is what this manifest holds.
	store := delete.NewTombstoneStore()
	ts := delete.Tombstone{ID: "t1", Query: "*", Mode: "rewrite", StartNs: 0, EndNs: 1 << 62,
		CreatedAt: time.Now(), AffectedKeys: []string{a, b}}
	store.Add(ts)
	if m.CompleteSince(ts.CreatedAt) {
		t.Fatal("fixture: the last complete listing must predate the tombstone")
	}
	out := "logs/dt=2026-01-01/hour=00/out.parquet"
	reconcileTombstones(store, []string{a, b}, out, nil,
		map[string]bool{"t1": true}, tombstoneRetireGate(m, store), nil)
	got, ok := store.Get("t1")
	if !ok {
		t.Fatal("compaction completed a tombstone created after the last complete listing began: a peer file published in between still holds its rows and they reappear when it is adopted")
	}
	if !got.Reaped[a] || !got.Reaped[b] {
		t.Fatalf("the bookkeeping must still be recorded (sources reaped): %+v", got)
	}
	// A complete listing that began after the tombstone lets the next merge
	// that touches it complete it.
	time.Sleep(2 * time.Millisecond)
	if !m.ApplyListing([]manifest.ListedObject{{Key: out, Size: 1}}, time.Now()) {
		t.Fatal("fixture: listing rejected")
	}
	if !tombstoneRetireGate(m, store)(got) {
		t.Fatal("the gate must open once a complete listing began after the tombstone")
	}
}

// Review regression (#404 round 4, F1 defence in depth): a merge group that
// names a source twice (what a manifest tracking a key twice hands the
// planner) is refused: merged, it writes the source's rows twice and then
// deletes the source, which makes the duplication permanent.
func TestReview404R4_CompactionRefusesADuplicatedSource(t *testing.T) {
	ctx := context.Background()
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	const partition = "dt=2026-05-04/hour=10"
	var files []manifest.FileInfo
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("logs/%s/dup-%d.parquet", partition, i)
		data := makeTestParquet(t, []schema.LogRow{{TimestampUnixNano: int64(1000 + i), Body: "row", ServiceName: "svc"}})
		if err := pool.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1, MinTimeNs: 1000, MaxTimeNs: 2000, SchemaFingerprint: "fp"}
		m.AddFile(partition, fi)
		files = append(files, fi)
	}
	c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "logs/", Mode: config.ModeLogs, RowGroupSize: 1000, CompressionLevel: 7})
	before := metrics.DuplicateFileKeys.Get("compaction_input")
	if _, err := c.Compact(ctx, partition, []manifest.FileInfo{files[0], files[1], files[0]}, 0); err == nil {
		t.Fatal("a merge group with a duplicated source was compacted")
	}
	if got := metrics.DuplicateFileKeys.Get("compaction_input") - before; got != 1 {
		t.Fatalf("compaction_input duplicates counted %d, want 1", got)
	}
	for _, fi := range files {
		if pool.get(fi.Key) == nil || !m.HasKey(fi.Key) {
			t.Fatalf("source %s was deleted or unregistered by a refused merge", fi.Key)
		}
	}
	if got := len(m.FilesForPartition(partition)); got != 2 {
		t.Fatalf("partition holds %d files after the refused merge, want 2", got)
	}
}
