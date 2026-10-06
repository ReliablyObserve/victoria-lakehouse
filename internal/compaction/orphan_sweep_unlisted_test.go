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

// #418: right after a restart the manifest is a snapshot, older than the
// objects peers flushed since. Tier B reads "not in the manifest" as "orphan",
// so before any bucket listing it deleted every live object newer than the
// snapshot. It waits for a complete listing.
func TestIssue418_TierBWaitsForACompleteListing(t *testing.T) {
	ctx := context.Background()
	pool := newListingPool()
	prev := manifest.New("bkt", "logs/")
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("logs/dt=2026-01-01/hour=00/snap%02d.parquet", i)
		_ = pool.UploadWithMtime(ctx, k, []byte("x"), time.Now().Add(-10*time.Hour))
		prev.AddFile("dt=2026-01-01/hour=00", manifest.FileInfo{Key: k, Size: 1, RowCount: 1})
	}
	snap := filepath.Join(t.TempDir(), "m.snap")
	if err := prev.SaveTo(snap); err != nil {
		t.Fatal(err)
	}
	var flushedByPeers []string
	for i := 0; i < 3; i++ { // flushed after the snapshot, live, not in it
		k := fmt.Sprintf("logs/dt=2026-01-01/hour=00/peer%02d.parquet", i)
		_ = pool.UploadWithMtime(ctx, k, []byte("x"), time.Now().Add(-5*time.Hour))
		flushedByPeers = append(flushedByPeers, k)
	}
	m := manifest.New("bkt", "logs/")
	if err := m.LoadFrom(snap); err != nil {
		t.Fatal(err)
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
	if deleted != 0 {
		t.Fatalf("Tier B deleted %d live objects before the manifest had listed the bucket", deleted)
	}
	for _, k := range flushedByPeers {
		if pool.get(k) == nil {
			t.Fatalf("live object %s was deleted", k)
		}
	}
}
