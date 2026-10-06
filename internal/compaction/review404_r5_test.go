package compaction

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// headHookLister runs onHead before answering a HEAD: the window between Tier
// B's first look at the retirement record and its DELETE.
type headHookLister struct {
	*listingPool
	onHead func(key string)
}

func (l headHookLister) HeadObject(ctx context.Context, key string) (int64, time.Time, error) {
	if l.onHead != nil {
		l.onHead(key)
	}
	return l.listingPool.HeadObject(ctx, key)
}

// Review regression (#404 round 5, R2): Tier B re-reads the retirement record
// right before the DELETE. The record can change after the first lookup: a
// rewrite whose replacement was abandoned gives the source back
// (UnretireIfReplacedBy), so its object is live again; or the key is retired
// anew, which restarts its window. Deleting on the stale record would remove a
// live object (the first) or one retired too recently (the second).
func TestReview404R5_TierBRechecksTheRetirementRecordBeforeDelete(t *testing.T) {
	const partition = "dt=2026-01-01/hour=00"
	key := "logs/" + partition + "/src.parquet"
	repl := "logs/" + partition + "/repl.parquet"
	cases := []struct {
		name       string
		onHead     func(m *manifest.Manifest, key string)
		wantDelete bool
	}{
		{"record unchanged (control)", nil, true},
		{"rewrite undone: the source is live again", func(m *manifest.Manifest, key string) {
			if !m.UnretireIfReplacedBy(key, repl) {
				t.Error("fixture: unretire refused")
			}
		}, false},
		{"retired again: the window restarts", func(m *manifest.Manifest, key string) {
			m.Retire(key, repl, true)
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pool := newListingPool()
			_ = pool.UploadWithMtime(ctx, key, []byte("x"), time.Now().Add(-10*time.Hour))
			m := manifest.New("bkt", "logs/")
			m.AddFile(partition, manifest.FileInfo{Key: key, Size: 1, RowCount: 1})
			m.Retire(key, repl, true)
			time.Sleep(5 * time.Millisecond)
			lister := headHookLister{listingPool: pool}
			if tc.onHead != nil {
				lister.onHead = func(k string) {
					if k == key {
						time.Sleep(2 * time.Millisecond) // a re-retirement gets a later At
						tc.onHead(m, k)
					}
				}
			}
			sweep := NewOrphanSweep(OrphanSweepConfig{
				Manifest: m, Pool: pool, Ownership: NewOwnershipResolver("self", staticPeers("self")),
				Policy: NewLevelPolicy(10, 20, 0), Lister: lister, Prefix: "logs/", Mode: config.ModeLogs,
				Interval: time.Minute, OrphanTTL: time.Millisecond,
			})
			before := metrics.CompactionOrphansSkipped.Get("manifest_drift_race")
			deleted, err := sweep.RunTierB(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, _, headErr := pool.HeadObject(ctx, key)
			if tc.wantDelete {
				if deleted != 1 || headErr == nil {
					t.Fatalf("control: deleted=%d, object present=%v; the fixture must reach the DELETE", deleted, headErr == nil)
				}
				return
			}
			if deleted != 0 || headErr != nil {
				t.Fatalf("Tier B deleted %s on a retirement record that changed after it read it (deleted=%d)", key, deleted)
			}
			if got := metrics.CompactionOrphansSkipped.Get("manifest_drift_race") - before; got != 1 {
				t.Fatalf("manifest_drift_race skips = %d, want 1", got)
			}
		})
	}
}

// togglePool is a tombstone S3 pool whose LIST fails while failing is set.
type togglePool struct {
	*listingPool
	failing atomic.Bool
}

func (p *togglePool) List(ctx context.Context, prefix string) ([]string, error) {
	if p.failing.Load() {
		return nil, errors.New("injected: tombstone LIST failed")
	}
	return p.listingPool.List(ctx, prefix)
}

// Review regression (#404 round 5, R5): compaction completes a tombstone only
// when the tombstone store was restored completely. While the S3 restore is
// pending the store may lack records that name files this merge does not
// know, so even a complete listing that began after the tombstone does not
// open the gate; a successful retry does.
func TestReview404R5_CompactionKeepsTombstonesWhileRestorePending(t *testing.T) {
	m := manifest.New("bkt", "logs/")
	a := "logs/dt=2026-01-01/hour=00/a.parquet"
	b := "logs/dt=2026-01-01/hour=00/b.parquet"
	m.AddFile("dt=2026-01-01/hour=00", manifest.FileInfo{Key: a, Size: 1, RowCount: 1})
	m.AddFile("dt=2026-01-01/hour=00", manifest.FileInfo{Key: b, Size: 1, RowCount: 1})

	pool := &togglePool{listingPool: newListingPool()}
	pool.failing.Store(true)
	store := delete.NewTombstoneStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // one restore attempt, no backoff
	if _, err := store.Restore(ctx, delete.PersistenceConfig{Pool: pool, Prefix: "logs/"}); err == nil {
		t.Fatal("fixture: the restore must fail")
	}
	if !store.S3RestorePending() {
		t.Fatal("fixture: restore not pending")
	}
	ts := delete.Tombstone{ID: "t1", Query: "*", Mode: "rewrite", StartNs: 0, EndNs: 1 << 62,
		CreatedAt: time.Now(), AffectedKeys: []string{a, b}}
	store.Add(ts)
	time.Sleep(2 * time.Millisecond)
	if !m.ApplyListing([]manifest.ListedObject{{Key: a, Size: 1}, {Key: b, Size: 1}}, time.Now()) {
		t.Fatal("fixture: listing rejected")
	}
	if !m.CompleteSince(ts.CreatedAt) {
		t.Fatal("fixture: a complete listing must have begun after the tombstone")
	}

	gate := tombstoneRetireGate(m, store)
	if gate(ts) {
		t.Fatal("the gate opened while the tombstone restore is pending: a record still to be loaded may name files this merge does not know")
	}
	out := "logs/dt=2026-01-01/hour=00/out.parquet"
	reconcileTombstones(store, []string{a, b}, out, nil, map[string]bool{"t1": true}, gate, nil)
	got, ok := store.Get("t1")
	if !ok {
		t.Fatal("compaction completed a tombstone while the tombstone restore was pending")
	}
	if !got.Reaped[a] || !got.Reaped[b] {
		t.Fatalf("the bookkeeping must still be recorded (sources reaped): %+v", got)
	}

	pool.failing.Store(false)
	if !store.RetryS3Restore(context.Background()) || store.S3RestorePending() {
		t.Fatal("fixture: retry did not complete the restore")
	}
	if !gate(got) {
		t.Fatal("the gate must open once the restore completed and a complete listing began after the tombstone")
	}
}
