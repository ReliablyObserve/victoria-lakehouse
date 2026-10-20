package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
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
