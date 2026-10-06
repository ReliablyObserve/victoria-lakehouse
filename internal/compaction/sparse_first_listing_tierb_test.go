package compaction

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// REVIEW #404: Tier B deletes every old .parquet object the in-memory manifest
// does not hold, with no "the manifest is complete" gate. After a restart the
// first listing is unguarded, so a sparse first LIST that stays the latest
// applied state (later refreshes failing) lets Tier B delete live objects.
func TestReview404Regression_TierBAfterSparseFirstListingDeletesLiveObjects(t *testing.T) {
	pool := newListingPool()
	ctx := context.Background()
	prev := manifest.New("bkt", "logs/")
	var objs []manifest.ListedObject
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("logs/dt=2026-01-01/hour=00/f%02d.parquet", i)
		_ = pool.UploadWithMtime(ctx, k, []byte("x"), time.Now().Add(-10*time.Hour))
		prev.AddFile("dt=2026-01-01/hour=00", manifest.FileInfo{Key: k, Size: 1, RowCount: 1})
		objs = append(objs, manifest.ListedObject{Key: k, Size: 1})
	}
	if !prev.ApplyListing(objs, time.Now()) {
		t.Fatal("fixture")
	}
	snap := filepath.Join(t.TempDir(), "m.snap")
	if err := prev.SaveTo(snap); err != nil {
		t.Fatal(err)
	}
	m := manifest.New("bkt", "logs/")
	if err := m.LoadFrom(snap); err != nil {
		t.Fatal(err)
	}
	accepted := m.ApplyListing(objs[:1], time.Now()) // truncated first LIST
	t.Logf("sparse first listing accepted=%v files=%d", accepted, m.TotalFiles())

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
		t.Fatalf("Tier B deleted %d live objects that a sparse first listing left out of the manifest", deleted)
	}
}
