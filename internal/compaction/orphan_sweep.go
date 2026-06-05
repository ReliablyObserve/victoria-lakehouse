// OrphanSweep is the two-tier garbage collector that closes the
// "duplicate work is safe" backstop of the election-free design.
//
//   - Tier A (partition-staleness): every Interval, the secondary
//     HRW owner checks every partition whose last MarkAttempt is
//     older than 3×Interval. If still eligible and primary owner is
//     unhealthy, the secondary takes over via the same compactor
//     path. Cheap — reads in-memory AttemptsView only.
//
//   - Tier B (S3 prefix sweep): hourly, each pod scans the date
//     prefixes it owns (hash(prefix) % len(peers) == selfIdx) for
//     .parquet files that are not in the manifest, older than
//     OrphanTTL, and not in NeverDeletePrefixes. Three-step deletion
//     safety: (a) NOT in manifest, (b) age + protected-prefix gate,
//     (c) re-read manifest before DELETE.
//
// See spec §2.4 and §3.7.
package compaction

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// S3Lister abstracts the S3 LIST + HEAD operations Tier B needs. The
// production implementation is cmd/lakehouse-*/main.go's
// s3PoolAdapter, which wraps internal/s3reader.ClientPool.
type S3Lister interface {
	List(ctx context.Context, prefix string) ([]string, error)
	HeadObject(ctx context.Context, key string) (size int64, mtime time.Time, err error)
}

// OrphanSweepConfig holds the dependencies the sweep needs.
type OrphanSweepConfig struct {
	Manifest  *manifest.Manifest
	Pool      CompactorPool // for Tier A re-compaction and Tier B Delete
	Ownership *OwnershipResolver
	Policy    *LevelPolicy
	Lister    S3Lister

	Prefix           string
	Mode             config.Mode
	Interval         time.Duration
	RowGroupSize     int
	CompressionLevel int

	// TierAStalenessMultiplier sets the partition-staleness threshold
	// as a multiplier of Interval. Default 3 — a primary that ticks
	// normally MarkAttempts every Interval, so 3× means three missed
	// opportunities. Spec §2.4.1.
	TierAStalenessMultiplier int

	// TierBInterval is the wall-clock cadence for the prefix sweep.
	// Default 1h (much less frequent than scheduler ticks because the
	// LIST calls are expensive).
	TierBInterval time.Duration

	// OrphanTTL is the minimum age an S3 key must reach before Tier B
	// considers it for deletion. Default 2h — long enough to cover
	// S3 eventual consistency + most pod restart scenarios. Spec Q12.
	OrphanTTL time.Duration

	// NeverDeletePrefixes is the whitelist of substrings that immunize
	// keys from Tier B. Default ["_meta/", "_tombstones/",
	// "_compaction_lock"]. The last entry preserves old sentinel
	// objects left over by the previous design until bucket lifecycle
	// reaps them (spec Q10).
	NeverDeletePrefixes []string

	// OnSteal is fired when Tier A successfully reclaims a partition.
	// Optional; used by tests and operator log lines.
	OnSteal func(partition string, primaryOwner string)

	// BloomRebuilder, if non-nil, is invoked by the inner Compactor on
	// successful Tier A re-compaction so bloom indexes stay current.
	BloomRebuilder BloomRebuilder

	// OnCompacted, when set, is invoked after a Tier A steal succeeds.
	// Mirrors Scheduler.OnCompacted so manifest pushers learn about
	// the new file just like during a normal scan.
	OnCompacted func(added []manifest.FileInfo, removed []string)
}

// OrphanSweep is the runtime that schedules Tier A on every Interval
// tick and Tier B on its own slower cadence.
type OrphanSweep struct {
	cfg OrphanSweepConfig

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewOrphanSweep validates cfg and returns a ready-to-Start sweep.
// Defaults: TierAStalenessMultiplier=3, TierBInterval=1h, OrphanTTL=2h.
func NewOrphanSweep(cfg OrphanSweepConfig) *OrphanSweep {
	if cfg.TierAStalenessMultiplier <= 0 {
		cfg.TierAStalenessMultiplier = 3
	}
	if cfg.TierBInterval <= 0 {
		cfg.TierBInterval = time.Hour
	}
	if cfg.OrphanTTL <= 0 {
		cfg.OrphanTTL = 2 * time.Hour
	}
	if len(cfg.NeverDeletePrefixes) == 0 {
		cfg.NeverDeletePrefixes = []string{"_meta/", "_tombstones/", "_compaction_lock"}
	}
	return &OrphanSweep{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Start launches both tier goroutines. Stop closes stopCh and waits.
func (o *OrphanSweep) Start() {
	o.wg.Add(2)
	go o.loopTierA()
	go o.loopTierB()
}

// Stop signals both loops to exit and blocks until they do.
func (o *OrphanSweep) Stop() {
	close(o.stopCh)
	o.wg.Wait()
}

func (o *OrphanSweep) loopTierA() {
	defer o.wg.Done()
	tick := time.NewTicker(o.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-o.stopCh:
			return
		case <-tick.C:
			ctx := context.Background()
			if _, err := o.RunTierA(ctx); err != nil {
				logger.Warnf("tier_a sweep error: %s", err)
			}
		}
	}
}

func (o *OrphanSweep) loopTierB() {
	defer o.wg.Done()
	tick := time.NewTicker(o.cfg.TierBInterval)
	defer tick.Stop()
	for {
		select {
		case <-o.stopCh:
			return
		case <-tick.C:
			ctx := context.Background()
			if _, err := o.RunTierB(ctx); err != nil {
				logger.Warnf("tier_b sweep error: %s", err)
			}
		}
	}
}

// RunTierA scans every known partition; whenever the last MarkAttempt
// is older than TierAStalenessMultiplier * Interval AND this pod is
// the secondary HRW owner AND the partition is still eligible, run
// the same compactor path as the scheduler would. Returns the number
// of stolen partitions.
//
// Skipped entirely while the ring is stabilizing (spec §11.4 +
// edge case 3) — we never make data-mutating decisions on a flapping
// ring.
func (o *OrphanSweep) RunTierA(ctx context.Context) (int, error) {
	if o.cfg.Ownership.IsStabilizing() {
		metrics.CompactionSweepDeferredStabilizing.Inc("tier_a")
		return 0, nil
	}

	threshold := time.Duration(o.cfg.TierAStalenessMultiplier) * o.cfg.Interval

	attempts := o.cfg.Manifest.AttemptsView()
	stolen := 0
	for partition, lastAttempt := range attempts {
		select {
		case <-o.stopCh:
			return stolen, nil
		default:
		}

		// Spec §2.4.1: zero-time attempt (never ticked) means cold
		// partition — apply the same threshold against time.Since
		// time.Time{} which is huge, so always counts as stale. The
		// secondary-owner check below then gates whether THIS pod
		// should take it.
		if !lastAttempt.IsZero() && time.Since(lastAttempt) < threshold {
			continue
		}

		files := o.cfg.Manifest.FilesForPartition(partition)
		if len(files) == 0 {
			continue
		}
		pt, err := manifest.ParsePartitionTime(partition)
		if err != nil {
			continue
		}
		level, eligible := o.cfg.Policy.Eligible(files, pt)
		if !eligible {
			continue // nothing to do; primary will pick it up when
			// the partition becomes eligible again.
		}

		// Secondary-owner gate prevents a thundering herd: only the
		// next-ranked HRW owner takes over. If primary == secondary
		// (single-pod cluster) we skip — we'd just be stealing from
		// ourselves.
		ranked := o.cfg.Ownership.RankedOwners(partition)
		if len(ranked) < 2 {
			continue
		}
		if ranked[1] != o.cfg.Ownership.Self {
			continue
		}
		primary := ranked[0]

		fp := MajoritySchemaFingerprint(files, level)
		selected := o.cfg.Policy.SelectFiles(files, level, fp)
		if len(selected) < 2 {
			continue
		}

		// Mark our own attempt BEFORE invoking compactor so a crash
		// during steal leaves a fresh attempt timestamp — symmetric
		// with the scheduler's contract.
		o.cfg.Manifest.MarkAttempt(partition, time.Now())

		compactor := NewCompactor(CompactorConfig{
			Pool:             o.cfg.Pool,
			Manifest:         o.cfg.Manifest,
			Prefix:           o.cfg.Prefix,
			Mode:             o.cfg.Mode,
			RowGroupSize:     o.cfg.RowGroupSize,
			CompressionLevel: o.cfg.CompressionLevel,
			BloomRebuilder:   o.cfg.BloomRebuilder,
		})
		if _, err := compactor.Compact(ctx, partition, selected, level); err != nil {
			logger.Warnf("tier_a steal failed; partition=%s primary=%s: %s",
				partition, primary, err)
			metrics.CompactionErrorsTotal.Inc()
			continue
		}

		metrics.CompactionStolenTotal.Inc()
		if o.cfg.OnSteal != nil {
			o.cfg.OnSteal(partition, primary)
		}
		if o.cfg.OnCompacted != nil {
			added := o.cfg.Manifest.FilesForPartition(partition)
			removed := make([]string, 0, len(selected))
			for _, s := range selected {
				removed = append(removed, s.Key)
			}
			o.cfg.OnCompacted(added, removed)
		}
		logger.Infof("tier_a: stolen partition; partition=%s primary_owner=%s last_attempt=%v",
			partition, primary, lastAttempt)
		stolen++
	}
	return stolen, nil
}

// RunTierB walks the prefix layout described in spec §1.1 and deletes
// .parquet keys that pass the three-step safety gate. Returns the
// number of deleted orphans.
//
// Prefix layout assumption: top-level prefixes correspond to date
// directories — when traversing under cfg.Prefix the lister returns
// nested keys whose path includes "dt=YYYY-MM-DD/...". We extract the
// date prefix from the first-level segments to hash-bucket prefix
// ownership across pods (spec §2.4.1 Tier B body sketch).
func (o *OrphanSweep) RunTierB(ctx context.Context) (int, error) {
	if o.cfg.Ownership.IsStabilizing() {
		metrics.CompactionSweepDeferredStabilizing.Inc("tier_b")
		return 0, nil
	}

	peers := o.cfg.Ownership.peersForOwnership()
	if len(peers) == 0 {
		// Edge case 10: no peers means no defensible ownership; skip.
		metrics.CompactionOrphansSkipped.Inc("empty_peers")
		return 0, nil
	}
	selfIdx := -1
	for i, p := range peers {
		if p == o.cfg.Ownership.Self {
			selfIdx = i
			break
		}
	}
	if selfIdx < 0 {
		// Self is not in the peer set — same case as ownership.go's
		// "stale self" gauge; skip work and let the operator alert.
		metrics.CompactionOrphansSkipped.Inc("self_not_in_peers")
		return 0, nil
	}

	keys, err := o.cfg.Lister.List(ctx, o.cfg.Prefix)
	if err != nil {
		return 0, fmt.Errorf("tier_b list %s: %w", o.cfg.Prefix, err)
	}

	// Bucket keys by date prefix so we can hash-assign each prefix to
	// a single pod (avoids 3 pods all LIST+HEADing the same date).
	byPrefix := groupByDatePrefix(keys, o.cfg.Prefix)
	hashFn := o.cfg.Ownership.hashFor()

	deleted := 0
	for datePrefix, dpKeys := range byPrefix {
		select {
		case <-o.stopCh:
			return deleted, nil
		default:
		}

		// Hash-bucket the prefix to a single peer. xxhash distributes
		// dates much more uniformly than CRC32 at small N (spec §2.1.1).
		if int(hashFn(datePrefix)%uint64(len(peers))) != selfIdx {
			continue
		}

		// Snapshot manifest keys under THIS date prefix once; the
		// three-step safety re-snapshot happens just before DELETE.
		manifestKeys := o.cfg.Manifest.KeysUnderPrefix(datePrefix)
		manifestSet := make(map[string]struct{}, len(manifestKeys))
		for _, k := range manifestKeys {
			manifestSet[k] = struct{}{}
		}

		for _, key := range dpKeys {
			// Step (a): NOT in manifest.
			if _, in := manifestSet[key]; in {
				metrics.CompactionOrphansSkipped.Inc("in_manifest")
				continue
			}
			// Step (b): protected prefix + extension gate. NEVER
			// delete anything that doesn't end in .parquet; never
			// touch keys matching the protected substrings.
			if o.isProtected(key) {
				if !strings.HasSuffix(key, ".parquet") {
					metrics.CompactionOrphansSkipped.Inc("not_parquet")
				} else {
					metrics.CompactionOrphansSkipped.Inc("protected_prefix")
				}
				continue
			}

			// Age gate: HEAD for LastModified.
			_, mtime, err := o.cfg.Lister.HeadObject(ctx, key)
			if err != nil {
				// HEAD failure is best-effort; skip and retry next tick.
				continue
			}
			if time.Since(mtime) < o.cfg.OrphanTTL {
				metrics.CompactionOrphansSkipped.Inc("too_young")
				continue
			}

			// Step (c): re-snapshot manifest at delete time. Guards
			// against the race where a peer just published this key
			// between our LIST and HEAD.
			if o.keyInManifestAt(datePrefix, key) {
				metrics.CompactionOrphansSkipped.Inc("manifest_drift_race")
				continue
			}

			if err := o.cfg.Pool.Delete(ctx, key); err != nil {
				logger.Warnf("tier_b orphan delete failed; key=%s: %s", key, err)
				continue
			}
			metrics.CompactionOrphansDeleted.Inc()
			deleted++
			logger.Infof("tier_b: deleted orphan; key=%s age=%v", key, time.Since(mtime))
		}
	}
	return deleted, nil
}

// keyInManifestAt re-reads the manifest snapshot and reports whether
// `key` appears in the manifest. Used only by Tier B's third safety
// check, where we re-confirm a key is still absent right before issuing
// the DELETE — guarding against the race where a peer just published
// this key between our LIST and HEAD.
//
// Originally iterated every key under the date prefix (O(files under
// prefix)). Now uses the byKey index for an O(1) point lookup; the
// prefix argument is preserved for backward compatibility but only used
// as a documentation hint (which prefix the caller thought it was
// scanning). Manifest membership is a global property — if the key is
// in m.byKey, it's in the manifest regardless of the prefix.
func (o *OrphanSweep) keyInManifestAt(_ string, key string) bool {
	return o.cfg.Manifest.HasKey(key)
}

// isProtected returns true when the key MUST NOT be deleted by Tier B,
// for any of:
//   - Not a .parquet file (we never delete arbitrary objects).
//   - Contains a protected substring (default _meta/, _tombstones/,
//     _compaction_lock — spec §3.2 case 16).
func (o *OrphanSweep) isProtected(key string) bool {
	if !strings.HasSuffix(key, ".parquet") {
		return true
	}
	for _, p := range o.cfg.NeverDeletePrefixes {
		if strings.Contains(key, p) {
			return true
		}
	}
	return false
}

// groupByDatePrefix collects S3 keys by their date-level prefix.
// "Date prefix" is everything up to and including the first
// "dt=YYYY-MM-DD/" segment in the key path. Keys that don't contain
// a dt= segment are placed under the cfg.Prefix bucket (rare; usually
// meta files that isProtected gates anyway).
func groupByDatePrefix(keys []string, basePrefix string) map[string][]string {
	out := make(map[string][]string)
	for _, k := range keys {
		dp := datePrefixOf(k, basePrefix)
		out[dp] = append(out[dp], k)
	}
	return out
}

// datePrefixOf returns "<basePrefix>...dt=YYYY-MM-DD/" for keys that
// contain a dt= segment, otherwise returns basePrefix unchanged. The
// returned prefix is intentionally stable across all keys belonging
// to the same day so the hash bucket is stable.
func datePrefixOf(key, basePrefix string) string {
	idx := strings.Index(key, "/dt=")
	if idx < 0 {
		// Some key shapes start with dt=… (no leading slash).
		if strings.HasPrefix(key, "dt=") {
			idx = 0
		} else {
			return basePrefix
		}
	} else {
		idx++ // skip the slash
	}
	end := strings.Index(key[idx:], "/")
	if end < 0 {
		return basePrefix
	}
	return key[:idx+end+1]
}
