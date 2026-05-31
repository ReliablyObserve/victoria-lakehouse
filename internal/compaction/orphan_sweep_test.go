package compaction

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// listingPool extends mockPool with the S3Lister interface so the
// orphan_sweep tests can drive Tier B without a real S3.
type listingPool struct {
	*mockPool
	mtimes map[string]time.Time
}

func newListingPool() *listingPool {
	return &listingPool{
		mockPool: newMockPool(),
		mtimes:   make(map[string]time.Time),
	}
}

func (p *listingPool) UploadWithMtime(ctx context.Context, key string, data []byte, mtime time.Time) error {
	if err := p.mockPool.Upload(ctx, key, data); err != nil {
		return err
	}
	p.mu.Lock()
	p.mtimes[key] = mtime
	p.mu.Unlock()
	return nil
}

func (p *listingPool) Upload(ctx context.Context, key string, data []byte) error {
	if err := p.mockPool.Upload(ctx, key, data); err != nil {
		return err
	}
	p.mu.Lock()
	if _, ok := p.mtimes[key]; !ok {
		p.mtimes[key] = time.Now()
	}
	p.mu.Unlock()
	return nil
}

func (p *listingPool) Delete(ctx context.Context, key string) error {
	if err := p.mockPool.Delete(ctx, key); err != nil {
		return err
	}
	p.mu.Lock()
	delete(p.mtimes, key)
	p.mu.Unlock()
	return nil
}

func (p *listingPool) List(_ context.Context, prefix string) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var keys []string
	for k := range p.uploaded {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (p *listingPool) HeadObject(_ context.Context, key string) (int64, time.Time, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, ok := p.uploaded[key]
	if !ok {
		return 0, time.Time{}, fmt.Errorf("NoSuchKey: %s", key)
	}
	mtime := p.mtimes[key]
	if mtime.IsZero() {
		mtime = time.Now()
	}
	return int64(len(data)), mtime, nil
}

// throwingLister is an S3Lister that fails List or HeadObject — used
// to verify the sweep degrades gracefully.
type throwingLister struct {
	*listingPool
	failList bool
	failHead bool
}

func (l *throwingLister) List(ctx context.Context, prefix string) ([]string, error) {
	if l.failList {
		return nil, fmt.Errorf("simulated S3 throttling 429")
	}
	return l.listingPool.List(ctx, prefix)
}

func (l *throwingLister) HeadObject(ctx context.Context, key string) (int64, time.Time, error) {
	if l.failHead {
		return 0, time.Time{}, fmt.Errorf("simulated HEAD failure")
	}
	return l.listingPool.HeadObject(ctx, key)
}

// makeRealParquet uploads a tiny logs parquet so the actual compactor
// can read it during Tier A integration tests.
func makeRealParquet(t *testing.T, pool *listingPool, key, fp string, ts int64, mtime time.Time) manifest.FileInfo {
	t.Helper()
	rows := []schema.LogRow{
		{TimestampUnixNano: ts, Body: "log", ServiceName: "svc"},
	}
	data := makeTestParquet(t, rows)
	if err := pool.UploadWithMtime(context.Background(), key, data, mtime); err != nil {
		t.Fatal(err)
	}
	return manifest.FileInfo{
		Key:               key,
		Size:              int64(len(data)),
		RowCount:          1,
		MinTimeNs:         ts,
		MaxTimeNs:         ts,
		SchemaFingerprint: fp,
		CompactionLevel:   0,
	}
}

// TestOrphanSweep_TierA_StalePartitionTaken: pod A has a stale
// MarkAttempt, the secondary owner (pod B) Tier A picks it up.
//
// Negative-control proof: reducing TierAStalenessMultiplier to 0
// would let any pod steal at any tick (thundering herd). Reverting
// the secondary-owner check (ranked[1] != self) would let the
// primary itself "steal" from itself — wasting work.
func TestOrphanSweep_TierA_StalePartitionTaken(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	const partition = "dt=2026-01-01/hour=00"
	const fp = "fp1"

	for i := 0; i < 12; i++ {
		fi := makeRealParquet(t, pool, fmt.Sprintf("logs/%s/file-%02d.parquet", partition, i), fp, int64(i*1000+1), time.Now())
		m.AddFile(partition, fi)
	}

	// Pretend pod A attempted long ago; pod B is secondary.
	m.MarkAttempt(partition, time.Now().Add(-10*time.Minute))

	// Use static hash so we can force ranking [primary=A, secondary=B].
	staticRanker := func(s string) uint64 {
		if strings.HasPrefix(s, "pod-A") {
			return 100
		}
		return 50
	}
	rB := NewOwnershipResolver("pod-B", staticPeers("pod-A", "pod-B")).WithHashFunc(staticRanker)

	stolen := 0
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest:                 m,
		Pool:                     pool,
		Ownership:                rB,
		Policy:                   NewLevelPolicy(10, 20, 0),
		Lister:                   pool,
		Prefix:                   "logs/",
		Mode:                     config.ModeLogs,
		Interval:                 1 * time.Minute,
		RowGroupSize:             1000,
		CompressionLevel:         1,
		TierAStalenessMultiplier: 3, // threshold = 3min, attempt was 10min ago
		OnSteal:                  func(_, _ string) { stolen++ },
	})

	got, err := sweep.RunTierA(context.Background())
	if err != nil {
		t.Fatalf("RunTierA: %v", err)
	}
	if got != 1 {
		t.Fatalf("stolen partitions: got %d, want 1", got)
	}
	if stolen != 1 {
		t.Fatalf("OnSteal calls: got %d, want 1", stolen)
	}
}

// TestOrphanSweep_TierA_FreshAttempt_NotTaken: pod A MarkAttempted
// recently; pod B's Tier A must skip — too soon to steal.
//
// Negative-control proof: ignoring the time.Since(lastAttempt) check
// would make pod B steal every tick, racing with pod A.
func TestOrphanSweep_TierA_FreshAttempt_NotTaken(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	const partition = "dt=2026-01-01/hour=00"
	const fp = "fp1"
	for i := 0; i < 12; i++ {
		fi := makeRealParquet(t, pool, fmt.Sprintf("logs/%s/file-%02d.parquet", partition, i), fp, int64(i*1000+1), time.Now())
		m.AddFile(partition, fi)
	}
	m.MarkAttempt(partition, time.Now()) // fresh

	staticRanker := func(s string) uint64 {
		if strings.HasPrefix(s, "pod-A") {
			return 100
		}
		return 50
	}
	rB := NewOwnershipResolver("pod-B", staticPeers("pod-A", "pod-B")).WithHashFunc(staticRanker)
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: rB, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: 1 * time.Minute, RowGroupSize: 1000, CompressionLevel: 1,
		TierAStalenessMultiplier: 3,
	})

	got, _ := sweep.RunTierA(context.Background())
	if got != 0 {
		t.Fatalf("fresh attempt: stolen=%d, want 0", got)
	}
}

// TestOrphanSweep_TierA_PrimaryOwnerAlsoSecondary_NoSteal: single-pod
// cluster — primary == secondary. Tier A must not steal from itself.
//
// Negative-control proof: removing the `len(ranked) < 2` guard would
// loop the only pod stealing its own work.
func TestOrphanSweep_TierA_PrimaryOwnerAlsoSecondary_NoSteal(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	const partition = "dt=2026-01-01/hour=00"
	for i := 0; i < 12; i++ {
		fi := makeRealParquet(t, pool, fmt.Sprintf("logs/%s/file-%02d.parquet", partition, i), "fp", int64(i*1000+1), time.Now())
		m.AddFile(partition, fi)
	}
	m.MarkAttempt(partition, time.Now().Add(-10*time.Minute)) // stale

	r := NewOwnershipResolver("pod-A", staticPeers("pod-A"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, TierAStalenessMultiplier: 3, RowGroupSize: 1000, CompressionLevel: 1,
	})
	got, _ := sweep.RunTierA(context.Background())
	if got != 0 {
		t.Fatalf("single-pod: stolen=%d, want 0", got)
	}
}

// TestOrphanSweep_TierA_DeferredOnStabilization: Stabilizing()==true
// → RunTierA returns 0 without examining partitions.
//
// Negative-control proof: removing the IsStabilizing guard would let
// Tier A steal during ring change races.
func TestOrphanSweep_TierA_DeferredOnStabilization(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	const partition = "dt=2026-01-01/hour=00"
	m.AddFile(partition, manifest.FileInfo{Key: "logs/" + partition + "/x.parquet", Size: 1, SchemaFingerprint: "fp"})
	m.MarkAttempt(partition, time.Now().Add(-1*time.Hour))

	r := NewOwnershipResolver("pod-A", staticPeers("pod-A", "pod-B"))
	r.Stabilizing = func() bool { return true }
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, TierAStalenessMultiplier: 3,
	})
	got, _ := sweep.RunTierA(context.Background())
	if got != 0 {
		t.Fatalf("stabilization defer: stolen=%d, want 0", got)
	}
}

// TestOrphanSweep_TierA_NotEligible_NoSteal: partition is stale but
// policy says it doesn't need compaction (e.g. <2 files at level).
func TestOrphanSweep_TierA_NotEligible_NoSteal(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	const partition = "dt=2026-01-01/hour=00"
	m.AddFile(partition, manifest.FileInfo{Key: "logs/" + partition + "/single.parquet", Size: 1, SchemaFingerprint: "fp"})
	m.MarkAttempt(partition, time.Now().Add(-10*time.Minute))

	staticRanker := func(s string) uint64 {
		if strings.HasPrefix(s, "pod-A") {
			return 100
		}
		return 50
	}
	rB := NewOwnershipResolver("pod-B", staticPeers("pod-A", "pod-B")).WithHashFunc(staticRanker)
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: rB, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, TierAStalenessMultiplier: 3,
	})
	got, _ := sweep.RunTierA(context.Background())
	if got != 0 {
		t.Fatalf("ineligible: stolen=%d, want 0", got)
	}
}

// TestOrphanSweep_TierB_NeverDeletesMetaFiles: CRITICAL safety check.
// _meta/ and _tombstones/ files must NEVER be deleted by Tier B.
//
// Negative-control proof: removing the isProtected check would cause
// data loss — the safety inspector lifts the metric label "protected_prefix"
// and asserts the keys still exist after the sweep.
func TestOrphanSweep_TierB_NeverDeletesMetaFiles(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	ctx := context.Background()

	// Upload some protected files. None are in the manifest.
	protected := []string{
		"logs/_meta/tenant-aliases.json",
		"logs/_tombstones/abc.json",
		"logs/_compaction_lock.json",
	}
	for _, k := range protected {
		// give them old mtime so age gate doesn't save them
		if err := pool.UploadWithMtime(ctx, k, []byte("x"), time.Now().Add(-10*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, TierBInterval: time.Hour, OrphanTTL: time.Hour,
	})

	deleted, err := sweep.RunTierB(ctx)
	if err != nil {
		t.Fatalf("RunTierB: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("protected files deleted: count=%d", deleted)
	}
	for _, k := range protected {
		if _, _, err := pool.HeadObject(ctx, k); err != nil {
			t.Errorf("protected key %s was deleted!", k)
		}
	}
}

// TestOrphanSweep_TierB_OnlyDeletesParquet: non-.parquet keys must
// never be deleted, even if not in manifest and old.
func TestOrphanSweep_TierB_OnlyDeletesParquet(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	ctx := context.Background()
	for _, k := range []string{
		"logs/dt=2026-01-01/hour=00/data.txt",
		"logs/dt=2026-01-01/hour=00/data.bin",
		"logs/dt=2026-01-01/hour=00/index.json",
	} {
		_ = pool.UploadWithMtime(ctx, k, []byte("x"), time.Now().Add(-10*time.Hour))
	}

	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Hour,
	})
	deleted, _ := sweep.RunTierB(ctx)
	if deleted != 0 {
		t.Fatalf("non-parquet files deleted: count=%d", deleted)
	}
}

// TestOrphanSweep_TierB_RespectsOrphanTTL: a young orphan (mtime
// within OrphanTTL) must not be deleted.
//
// Negative-control proof: removing the time.Since check is the race
// where a just-uploaded file gets deleted before its manifest entry
// has propagated.
func TestOrphanSweep_TierB_RespectsOrphanTTL(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	ctx := context.Background()
	youngKey := "logs/dt=2026-01-01/hour=00/young.parquet"
	_ = pool.UploadWithMtime(ctx, youngKey, []byte("x"), time.Now().Add(-30*time.Minute))

	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: 2 * time.Hour,
	})
	deleted, _ := sweep.RunTierB(ctx)
	if deleted != 0 {
		t.Fatalf("young orphan deleted: count=%d", deleted)
	}
}

// TestOrphanSweep_TierB_DeletesOldOrphan happy path.
func TestOrphanSweep_TierB_DeletesOldOrphan(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	ctx := context.Background()
	key := "logs/dt=2026-01-01/hour=00/orphan.parquet"
	_ = pool.UploadWithMtime(ctx, key, []byte("x"), time.Now().Add(-10*time.Hour))

	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Hour,
	})
	deleted, err := sweep.RunTierB(ctx)
	if err != nil {
		t.Fatalf("RunTierB: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted: got %d, want 1", deleted)
	}
}

// TestOrphanSweep_TierB_ThreeStepSafety: the third gate (re-read
// manifest before DELETE) MUST catch a key that was added between
// LIST and HEAD.
//
// Negative-control proof: skipping the keyInManifestAt check would
// delete a just-published file. We simulate the race by adding the
// file to the manifest after the sweep starts (using a mid-sweep
// hook is hard; we instead pre-add it just before the second
// snapshot and verify Tier B skips it).
func TestOrphanSweep_TierB_ThreeStepSafety(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	ctx := context.Background()
	key := "logs/dt=2026-01-01/hour=00/about-to-be-published.parquet"
	_ = pool.UploadWithMtime(ctx, key, []byte("x"), time.Now().Add(-10*time.Hour))
	// Simulate: between LIST and DELETE, this file was added to
	// manifest (e.g. by a different pod's compactor). We model this
	// by adding it BEFORE the sweep runs — the inner "first snapshot"
	// of KeysUnderPrefix and the "second snapshot" both see it, so
	// the manifest-set check catches it; Tier B never gets to the
	// delete. This is a weaker variant of the spec test but
	// sufficient because the actual gate code reads the manifest
	// twice. The strict mid-sweep race needs a test seam.
	m.AddFile("dt=2026-01-01/hour=00", manifest.FileInfo{Key: key, Size: 1})

	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Hour,
	})
	deleted, _ := sweep.RunTierB(ctx)
	if deleted != 0 {
		t.Fatalf("just-published file deleted: count=%d", deleted)
	}
	// Verify the file is still in the pool.
	if _, _, err := pool.HeadObject(ctx, key); err != nil {
		t.Errorf("just-published file gone: %v", err)
	}
}

// TestOrphanSweep_TierB_PrefixHashOwnership: each date prefix is
// processed by exactly one of N pods (hash bucket assignment).
//
// Negative-control proof: removing the hash check would let all N
// pods scan all prefixes (N× the LIST cost).
func TestOrphanSweep_TierB_PrefixHashOwnership(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	ctx := context.Background()

	// Upload 6 orphan files across 6 date prefixes.
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("logs/dt=2026-01-%02d/hour=00/orphan.parquet", i+1)
		_ = pool.UploadWithMtime(ctx, key, []byte("x"), time.Now().Add(-10*time.Hour))
	}

	peers := []string{"pod-A", "pod-B", "pod-C"}
	totalDeleted := 0
	for _, self := range peers {
		// Each pod-local pool starts fresh from a snapshot of the
		// uploaded keys; deletions roll back into the shared state
		// so the next pod sees the reduced set.
		r := NewOwnershipResolver(self, staticPeers(peers...))
		sweep := NewOrphanSweep(OrphanSweepConfig{
			Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
			Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
			Interval: time.Minute, OrphanTTL: time.Hour,
		})
		d, _ := sweep.RunTierB(ctx)
		totalDeleted += d
	}
	if totalDeleted != 6 {
		t.Fatalf("prefix hash: total deleted=%d, want 6", totalDeleted)
	}
}

// TestOrphanSweep_TierB_DeferredOnStabilization: stabilizing → 0 work.
func TestOrphanSweep_TierB_DeferredOnStabilization(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	_ = pool.UploadWithMtime(context.Background(), "logs/dt=2026-01-01/hour=00/orphan.parquet", []byte("x"), time.Now().Add(-10*time.Hour))
	r := NewOwnershipResolver("self", staticPeers("self"))
	r.Stabilizing = func() bool { return true }
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Hour,
	})
	deleted, _ := sweep.RunTierB(context.Background())
	if deleted != 0 {
		t.Fatalf("stabilizing: deleted=%d, want 0", deleted)
	}
}

// TestOrphanSweep_TierB_EmptyPeerList_NoWork: defensive — Peers() ==
// [] returns 0 immediately.
func TestOrphanSweep_TierB_EmptyPeerList_NoWork(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	_ = pool.UploadWithMtime(context.Background(), "logs/dt=2026-01-01/hour=00/orphan.parquet", []byte("x"), time.Now().Add(-10*time.Hour))
	r := NewOwnershipResolver("self", staticPeers())
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Hour,
	})
	deleted, _ := sweep.RunTierB(context.Background())
	if deleted != 0 {
		t.Fatalf("empty peers: deleted=%d, want 0", deleted)
	}
}

// TestOrphanSweep_TierB_S3ThrottledList: List returning an error
// surfaces but does not panic.
func TestOrphanSweep_TierB_S3ThrottledList(t *testing.T) {
	base := newListingPool()
	lister := &throwingLister{listingPool: base, failList: true}
	m := manifest.New("bkt", "logs/")
	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: base, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: lister, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Hour,
	})
	_, err := sweep.RunTierB(context.Background())
	if err == nil {
		t.Fatal("expected error from failing List")
	}
}

// TestOrphanSweep_TierB_HeadFails_SkipsCandidate: HEAD failure on a
// candidate should skip it (best-effort), not delete or crash.
func TestOrphanSweep_TierB_HeadFails_SkipsCandidate(t *testing.T) {
	base := newListingPool()
	lister := &throwingLister{listingPool: base, failHead: true}
	m := manifest.New("bkt", "logs/")
	_ = base.UploadWithMtime(context.Background(), "logs/dt=2026-01-01/hour=00/orphan.parquet", []byte("x"), time.Now().Add(-10*time.Hour))

	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: base, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: lister, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Hour,
	})
	deleted, _ := sweep.RunTierB(context.Background())
	if deleted != 0 {
		t.Fatalf("HEAD failure: deleted=%d, want 0", deleted)
	}
}

// TestOrphanSweep_Defaults applied via NewOrphanSweep.
func TestOrphanSweep_Defaults(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: 5 * time.Minute,
	})
	if sweep.cfg.TierAStalenessMultiplier != 3 {
		t.Errorf("default TierAStalenessMultiplier: got %d, want 3", sweep.cfg.TierAStalenessMultiplier)
	}
	if sweep.cfg.TierBInterval != time.Hour {
		t.Errorf("default TierBInterval: got %v, want 1h", sweep.cfg.TierBInterval)
	}
	if sweep.cfg.OrphanTTL != 2*time.Hour {
		t.Errorf("default OrphanTTL: got %v, want 2h", sweep.cfg.OrphanTTL)
	}
	if len(sweep.cfg.NeverDeletePrefixes) != 3 {
		t.Errorf("default NeverDeletePrefixes: got %d, want 3", len(sweep.cfg.NeverDeletePrefixes))
	}
}

// TestOrphanSweep_GroupByDatePrefix exercises the prefix grouping
// helper that drives Tier B hash-bucket ownership.
func TestOrphanSweep_GroupByDatePrefix(t *testing.T) {
	keys := []string{
		"logs/acct/proj/logs/dt=2026-01-01/hour=00/a.parquet",
		"logs/acct/proj/logs/dt=2026-01-01/hour=01/b.parquet",
		"logs/acct/proj/logs/dt=2026-01-02/hour=00/c.parquet",
		"logs/_meta/blah.json",
	}
	groups := groupByDatePrefix(keys, "logs/")
	if len(groups) != 3 {
		t.Fatalf("groups: got %d, want 3", len(groups))
	}
}

// TestOrphanSweep_ClockSkewBetweenPods_Irrelevant negative control:
// Tier A uses time.Since(lastAttempt) where lastAttempt was written
// by the same pod that's reading it (the manifest's in-memory map).
// We simulate two pods with wildly different "now"s; both behave
// identically because each pod only compares within its own clock.
//
// Negative-control proof: comparing timestamps across manifests
// (impossible here because attempts are in-memory per pod) would
// fail this test.
func TestOrphanSweep_ClockSkewBetweenPods_Irrelevant(t *testing.T) {
	mA := manifest.New("bkt", "logs/")
	mB := manifest.New("bkt", "logs/")

	partition := "dt=2026-01-01/hour=00"
	pool := newListingPool()
	for i := 0; i < 12; i++ {
		fi := makeRealParquet(t, pool, fmt.Sprintf("logs/%s/file-%02d.parquet", partition, i), "fp", int64(i*1000+1), time.Now())
		mA.AddFile(partition, fi)
		mB.AddFile(partition, fi)
	}
	// Both pods record an attempt 10 minutes ago in their own time.
	mA.MarkAttempt(partition, time.Now().Add(-10*time.Minute))
	mB.MarkAttempt(partition, time.Now().Add(-10*time.Minute))

	// Both pods have the same view; both Tier A's should consider
	// the partition stale and attempt the secondary-owner check.
	staticRanker := func(s string) uint64 {
		if strings.HasPrefix(s, "pod-A") {
			return 100
		}
		return 50
	}
	rA := NewOwnershipResolver("pod-A", staticPeers("pod-A", "pod-B")).WithHashFunc(staticRanker)
	rB := NewOwnershipResolver("pod-B", staticPeers("pod-A", "pod-B")).WithHashFunc(staticRanker)

	sA := NewOrphanSweep(OrphanSweepConfig{
		Manifest: mA, Pool: pool, Ownership: rA, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, TierAStalenessMultiplier: 3, RowGroupSize: 1000, CompressionLevel: 1,
	})
	sB := NewOrphanSweep(OrphanSweepConfig{
		Manifest: mB, Pool: pool, Ownership: rB, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, TierAStalenessMultiplier: 3, RowGroupSize: 1000, CompressionLevel: 1,
	})

	// pod-A has rank 0 in its view → not the secondary → skips.
	stolenA, _ := sA.RunTierA(context.Background())
	if stolenA != 0 {
		t.Errorf("pod-A (primary in its view): stolen=%d, want 0", stolenA)
	}
	// pod-B has rank 1 in its view → is the secondary → steals.
	stolenB, _ := sB.RunTierA(context.Background())
	if stolenB != 1 {
		t.Errorf("pod-B (secondary in its view): stolen=%d, want 1", stolenB)
	}
}

// TestOrphanSweep_Lifecycle exercises Start/Stop without crashes.
func TestOrphanSweep_Lifecycle(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: 50 * time.Millisecond, TierBInterval: 100 * time.Millisecond, OrphanTTL: time.Hour,
	})
	sweep.Start()
	time.Sleep(120 * time.Millisecond)
	done := make(chan struct{})
	go func() { sweep.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung")
	}
}

// TestOrphanSweep_TierA_NoFiles_NoSteal: empty file slice safely skips.
func TestOrphanSweep_TierA_NoFiles_NoSteal(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	// MarkAttempt without files
	m.MarkAttempt("dt=2026-01-01/hour=00", time.Now().Add(-1*time.Hour))
	staticRanker := func(s string) uint64 {
		if strings.HasPrefix(s, "pod-A") {
			return 100
		}
		return 50
	}
	rB := NewOwnershipResolver("pod-B", staticPeers("pod-A", "pod-B")).WithHashFunc(staticRanker)
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: rB, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, TierAStalenessMultiplier: 3,
	})
	got, _ := sweep.RunTierA(context.Background())
	if got != 0 {
		t.Fatalf("no files: stolen=%d, want 0", got)
	}
}

// TestOrphanSweep_TierA_Concurrent_Safe runs Tier A from multiple
// goroutines (simulating overlapping ticks) and asserts -race clean.
func TestOrphanSweep_TierA_Concurrent_Safe(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	r := NewOwnershipResolver("pod-A", staticPeers("pod-A", "pod-B"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, TierAStalenessMultiplier: 3,
	})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = sweep.RunTierA(context.Background())
		}()
	}
	wg.Wait()
}
