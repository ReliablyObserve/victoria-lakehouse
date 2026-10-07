// Package compaction now drives compaction without leader election.
// Each pod independently decides which partitions it owns via the HRW
// OwnershipResolver, and OrphanSweep + the manifest's AddFile
// idempotency cover the rare dual-ownership cases (ring flap, DNS lag).
//
// for the scheduler design and §11.1 / §11.2 / §11.4 for the
// drain + ring-thrash gates.
package compaction

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// SchedulerConfig holds all dependencies for the Scheduler. Compared
// to the pre-PR-A shape this drops Leader, Sentinel, and Sharding;
// replaces them with Ownership (HRW) + optional FairShare for
// per-tenant slotting.
type SchedulerConfig struct {
	Manifest         *manifest.Manifest
	Pool             CompactorPool
	Ownership        *OwnershipResolver  // required
	FairShare        *FairShareScheduler // optional; nil = no tenant fairness
	Policy           *LevelPolicy
	BloomRebuilder   BloomRebuilder
	Prefix           string
	Mode             config.Mode
	Interval         time.Duration
	MaxConcurrent    int
	RowGroupSize     int
	CompressionLevel int

	// ScanBudget: a scan starts no new merge once it has run this long. 0
	// means the scan interval; negative disables the budget.
	ScanBudget time.Duration

	// Freeze keeps objects that S3 lifecycle has moved, or is about to move,
	// out of STANDARD out of every merge. nil still never rewrites an object
	// whose manifest entry records a non-rewritable class.
	Freeze *LifecycleFreeze

	// CurrentSchemaFingerprint is the fingerprint new files are written with. The
	// scheduler flags files carrying any OTHER fingerprint as stale and recompacts
	// them (re-promotion to dedicated columns) even when the level policy would skip
	// the partition — so old / poorly-compacted areas heal without waiting for new
	// input files. Empty disables hint-driven recompaction (only the level policy runs).
	CurrentSchemaFingerprint string

	// CompactionConfig carries the full compaction-section config so
	// the per-tick Compactor constructor can read progressive-
	// compression schedule + any future per-output knobs without
	// new fields here. The embedder fills it from cfg.Compaction.
	CompactionConfig config.CompactionConfig

	// TenantCompressionLookup resolves the per-output-level
	// compression override for a given tenant prefix. Forwarded
	// to every Compactor constructed in the scheduler loop;
	// optional (nil = use the global schedule for every tenant).
	TenantCompressionLookup func(tenantPrefix string) []int

	// Tombstones is forwarded to every Compactor the scheduler builds, making
	// compaction drop tombstoned rows rather than copy them forward. Optional;
	// nil keeps the pre-existing behaviour.
	Tombstones *delete.TombstoneStore
	// TombstoneRewriteDelay is forwarded with it: the un-delete window inside
	// which compaction must carry a tombstone's rows forward (see
	// CompactorConfig.TombstoneRewriteDelay).
	TombstoneRewriteDelay time.Duration
	// OnCompacted is fired after a successful compaction. blooms carries the
	// combined pmeta bloom of each output (outputKey -> column -> values) so the
	// embedder can feed the bloom facet (compacted files stay bloom-prunable).
	OnCompacted func(added []manifest.FileInfo, removed []string, blooms map[string]map[string][]string)

	// OnRingChange is fired by the embedder (main.go) when peer-cache
	// observes a ring change. Used to (a) increment the ring-change
	// counter and (b) feed the §11.4 sliding-window rate gate.
	// Optional.
	OnRingChange func(register func(eventType string))

	// RingChangeRateLimit (§11.4): when more than this many ring
	// change events occur in the trailing 5 minutes, the scheduler
	// defers every tick until the rate drops. Default 6 (= 1 per
	// 50s sustained). Set 0 to disable.
	RingChangeRateLimit int

	// DrainTimeout is the upper bound on Drain() waiting for
	// inFlight to drain. Default 90s — matches the preStop hook's
	// practical limit before terminationGracePeriodSeconds fires.
	DrainTimeout time.Duration
}

// recompactionLevel decides whether a partition the level policy skipped still needs
// (re)compaction from the compaction hints, and at which level. Two triggers the
// level policy ignores: stale-schema files (a fingerprint other than currentFP →
// re-promotion to dedicated columns) and top-level fragmentation (>= L2 with 2+ files
// the policy never re-merges). Returns (level, true) to compact; the Scan loop's
// existing SelectFiles(level, majority-fingerprint) picks the group and the compactor
// re-promotes during the merge. The selection needs 2+ files (a merge), so a lone
// stale file is left until it has a peer at the same level/fingerprint.
func recompactionLevel(files []manifest.FileInfo, currentFP string) (int, bool) {
	maxLevel := 0
	stale := false
	for _, f := range files {
		if f.CompactionLevel > maxLevel {
			maxLevel = f.CompactionLevel
		}
		if currentFP != "" && f.SchemaFingerprint != currentFP {
			stale = true
		}
	}
	if stale {
		return maxLevel, true
	}
	if maxLevel >= 2 {
		cnt := 0
		for _, f := range files {
			if f.CompactionLevel == maxLevel {
				cnt++
			}
		}
		if cnt >= 2 {
			return maxLevel, true
		}
	}
	return 0, false
}

// Scheduler runs periodic compaction scans.
type Scheduler struct {
	manifest         *manifest.Manifest
	pool             CompactorPool
	ownership        *OwnershipResolver
	fairShare        *FairShareScheduler
	policy           *LevelPolicy
	bloomRebuilder   BloomRebuilder
	prefix           string
	mode             config.Mode
	interval         time.Duration
	maxConcurrent    int
	rowGroupSize     int
	compressionLevel int
	currentFP        string
	freeze           *LifecycleFreeze
	scanBudget       time.Duration
	backoff          planBackoff
	compactionCfg    config.CompactionConfig
	tenantLookup     func(tenantPrefix string) []int
	tombstones       *delete.TombstoneStore
	tombstoneDelay   time.Duration
	onCompacted      func(added []manifest.FileInfo, removed []string, blooms map[string]map[string][]string)

	ringChangeRate int
	drainTimeout   time.Duration

	// segmentLister lists the "segment committed" markers; segmentProtect is
	// how long after a commit a buffer segment can still be served from its
	// insert pod (its grace, with margin). See manifest.SegmentGuard.
	segmentLister  SegmentMarkerLister
	segmentProtect time.Duration

	stopCh chan struct{}
	wg     sync.WaitGroup

	// Drain state (spec §11.1)
	draining  atomic.Bool
	drainCh   chan struct{}
	inFlight  sync.WaitGroup
	drainOnce sync.Once

	// Ring-change sliding window (spec §11.4)
	ringEvents   []time.Time
	ringEventsMu sync.Mutex
}

// NewScheduler creates a Scheduler from the given config. Panics if
// Ownership is nil — the new design has no notion of "leader-only" so
// ownership is mandatory.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	if cfg.Ownership == nil {
		panic("compaction: SchedulerConfig.Ownership is required (HRW resolver)")
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = 5 * time.Minute
	}
	maxConc := cfg.MaxConcurrent
	if maxConc == 0 {
		maxConc = 1
	}
	rate := cfg.RingChangeRateLimit
	if rate < 0 {
		rate = 0
	} else if rate == 0 && cfg.RingChangeRateLimit == 0 {
		// Default = 6 events / 5 min (spec §11.4). Operators can
		// disable by setting explicitly to a sentinel — but the
		// zero-value case picks the default.
		rate = 6
	}
	scanBudget := cfg.ScanBudget
	if scanBudget == 0 {
		scanBudget = interval
	}
	drainTimeout := cfg.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 90 * time.Second
	}

	s := &Scheduler{
		manifest:         cfg.Manifest,
		pool:             cfg.Pool,
		ownership:        cfg.Ownership,
		fairShare:        cfg.FairShare,
		policy:           cfg.Policy,
		bloomRebuilder:   cfg.BloomRebuilder,
		prefix:           cfg.Prefix,
		mode:             cfg.Mode,
		interval:         interval,
		maxConcurrent:    maxConc,
		rowGroupSize:     cfg.RowGroupSize,
		compressionLevel: cfg.CompressionLevel,
		currentFP:        cfg.CurrentSchemaFingerprint,
		freeze:           cfg.Freeze,
		scanBudget:       scanBudget,
		compactionCfg:    cfg.CompactionConfig,
		tenantLookup:     cfg.TenantCompressionLookup,
		tombstones:       cfg.Tombstones,
		tombstoneDelay:   cfg.TombstoneRewriteDelay,
		onCompacted:      cfg.OnCompacted,
		ringChangeRate:   rate,
		drainTimeout:     drainTimeout,
		stopCh:           make(chan struct{}),
		drainCh:          make(chan struct{}),
	}

	if cfg.OnRingChange != nil {
		// Register a recorder; main.go bridges peer-cache events
		// to this callback via the register parameter.
		cfg.OnRingChange(func(eventType string) {
			s.recordRingChange(eventType)
		})
	}

	return s
}

// Start launches the background tick goroutine.
func (s *Scheduler) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				ctx := context.Background()
				n, err := s.Scan(ctx)
				if err != nil {
					logger.Errorf("scan failed: %s", err)
				} else if n > 0 {
					logger.Infof("scan completed; compactions=%d", n)
				}
			}
		}
	}()
}

// Stop signals the background goroutine to stop and waits for it.
// Stop is idempotent.
func (s *Scheduler) Stop() {
	select {
	case <-s.stopCh:
		// already stopped
	default:
		close(s.stopCh)
	}
	s.wg.Wait()
}

// Drain initiates a graceful shutdown of the scheduler. After Drain
// returns, no new partitions will be started; in-flight compactions
// are allowed to reach their partition boundary or until the
// DrainTimeout elapses (whichever is first). After Drain, callers
// should still invoke Stop to terminate the tick loop.
//
// Drain is idempotent. Safe to call from a signal handler.
func (s *Scheduler) Drain() {
	s.drainOnce.Do(func() {
		s.draining.Store(true)
		metrics.CompactionDraining.Set(1)
		close(s.drainCh)
		logger.Infof("compaction: drain initiated; timeout=%v", s.drainTimeout)
	})

	// Wait for inFlight with timeout.
	done := make(chan struct{})
	go func() {
		s.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
		logger.Infof("compaction: drain complete (all in-flight finished)")
	case <-time.After(s.drainTimeout):
		logger.Warnf("compaction: drain timed out after %v with in-flight still running", s.drainTimeout)
		metrics.CompactionAbortedDuringDrain.Inc()
	}
}

// IsDraining reports the current drain state. Tests + sweep coordination.
func (s *Scheduler) IsDraining() bool { return s.draining.Load() }

// maxReclaimPerScan bounds the retired-object deletes one scan attempts, so a
// long outage's backlog drains over several ticks instead of stalling one.
const maxReclaimPerScan = 1000

// withoutHeld drops the files a rewrite has swapped in but not yet recorded
// durably. Merging one would carry its rows into an output that the undo of
// that rewrite — still possible until its record lands — knows nothing about,
// and the publish would be refused anyway. It also drops the objects the
// forward fence already found to hold a column this version does not model
// (fenceLog): they are never merged, so a plan made of them must not form and
// take the tenant's merge slot on every scan.
func withoutHeld(m *manifest.Manifest, files []manifest.FileInfo) []manifest.FileInfo {
	out := files[:0:0]
	for _, fi := range files {
		if m.IsHeld(fi.Key) || fenceLog.Has(fenceKey(fi)) {
			continue
		}
		out = append(out, fi)
	}
	return out
}

// SegmentMarkerLister lists keys under a prefix with their LastModified time;
// s3reader.ClientPool has it.
type SegmentMarkerLister interface {
	ListModTimes(ctx context.Context, prefix string) (map[string]time.Time, error)
}

// SetSegmentGuard makes compaction leave the objects of insert-buffer segments
// alone until their segment is committed and protect has passed since (see
// manifest.SegmentGuard): merged into an object without the segment's nonce,
// their rows would be served twice while the segment is still live. Without
// it, such objects are merged only SegmentReleaseAfter after their segment
// was created.
func (s *Scheduler) SetSegmentGuard(l SegmentMarkerLister, protect time.Duration) {
	s.segmentLister, s.segmentProtect = l, protect
}

// SegmentGuard lists the segment markers once and returns the guard a merge
// path applies at now (see segmentGuard); the orphan sweep's Tier A steal uses
// it so a steal leaves live buffer segments alone as a scan does.
func (s *Scheduler) SegmentGuard(ctx context.Context, now time.Time) *manifest.SegmentGuard {
	return s.segmentGuard(ctx, now)
}

// maxMarkerDeletesPerScan bounds the old markers one scan removes.
const maxMarkerDeletesPerScan = 1000

// segmentGuard lists the segment markers once for a scan. A failed listing
// releases no segment object (until SegmentReleaseAfter). Markers older than
// SegmentReleaseAfter plus a day are no longer needed and are deleted.
func (s *Scheduler) segmentGuard(ctx context.Context, now time.Time) *manifest.SegmentGuard {
	g := &manifest.SegmentGuard{Protect: s.segmentProtect}
	if s.segmentLister == nil {
		return g
	}
	dir := s.prefix + manifest.SegmentMarkerDir
	listed, err := s.segmentLister.ListModTimes(ctx, dir)
	if err != nil {
		metrics.CompactionSegmentGuardErrors.Inc()
		logger.Warnf("compaction: cannot list the buffer segment markers; objects of unconfirmed segments are left alone this scan: %s", err)
		return g
	}
	g.Markers = make(map[string]time.Time, len(listed))
	g.Listed = true
	deleted := 0
	for key, mod := range listed {
		nonce := key[len(dir):]
		if now.Sub(mod) >= manifest.SegmentReleaseAfter+24*time.Hour && s.pool != nil && deleted < maxMarkerDeletesPerScan {
			if err := s.pool.Delete(ctx, key); err == nil {
				deleted++
			}
			continue
		}
		g.Markers[nonce] = mod
	}
	return g
}

// partitionCandidate is a planned merge as the fair-share scheduler sees it.
type partitionCandidate = mergePlan

// Scan runs one compaction cycle: defer if stabilizing or thrashing,
// enumerate owned partitions, apply fair-share, compact up to
// MaxConcurrent of them. Returns the count of completed compactions.
//
// Spec §2.3.2 + §11.1 + §11.4.
func (s *Scheduler) Scan(ctx context.Context) (int, error) {
	// (A) Drain check — no new work after Drain().
	if s.draining.Load() {
		return 0, nil
	}

	// Retry the deletes of objects this node superseded or abandoned whose
	// first delete failed (merged sources, refused outputs). They are retired
	// in the manifest, so no refresh adopts them meanwhile; until they are
	// deleted they cost storage and keep their retirement record alive.
	if s.pool != nil {
		if deleted, failed := s.manifest.ReclaimRetired(ctx, s.pool.Delete, maxReclaimPerScan); deleted+failed > 0 {
			logger.Infof("retired objects reclaimed; deleted=%d, failed=%d", deleted, failed)
		}
	}

	// (B) Stabilization check (spec §3.1 cases 3 + 22).
	if s.ownership.IsStabilizing() {
		metrics.CompactionDeferredStabilizing.Inc()
		return 0, nil
	}

	// (C) Ring-thrash rate gate (spec §11.4).
	if s.ringChangeRate > 0 && s.recentRingChanges() > s.ringChangeRate {
		metrics.CompactionDeferredRingThrash.Inc()
		return 0, nil
	}

	now := planClock()
	scanStart := time.Now()
	guard := s.segmentGuard(ctx, now)

	// Held keys are snapshotted once: checking them per file would take the
	// manifest lock once per file per scan.
	var held map[string]bool
	if keys := s.manifest.HeldKeys(); len(keys) > 0 {
		held = make(map[string]bool, len(keys))
		for _, k := range keys {
			held[k] = true
		}
	}

	// (D) HRW-based ownership, then a plan per (tenant, partition): the
	// compactor writes one output per tenant group, so files are counted and
	// selected per tenant group, never across the tenants of a partition
	// (issue #343). The planner reads the manifest in place (RangePartitions)
	// and copies only the files of a group it plans a merge for.
	owned := 0
	frozen := map[string]int{frozenStorageClass: 0, frozenAge: 0, frozenSizeAge: 0}
	pl := newPlanner(s.policy, s.currentFP, now, held, s.freeze, func(reason string, n int) { frozen[reason] += n })
	var candidates []partitionCandidate
	s.manifest.RangePartitions(func(partition string, files []manifest.FileInfo) bool {
		if !s.ownership.OwnsPartition(partition) {
			return true
		}
		owned++
		files = guard.ReleasedFiles(files, now)
		pt, err := manifest.ParsePartitionTime(partition)
		if err != nil {
			logger.Warnf("skip partition: cannot parse time; partition=%s, error=%s", partition, err)
			return true
		}
		candidates = append(candidates, pl.partition(partition, files, pt)...)
		return true
	})
	metrics.CompactionPartitionsOwned.Set(int64(owned))
	metrics.CompactionOwnershipSelfInPeers.Set(s.ownership.SelfInPeersGauge())
	for reason, n := range frozen {
		metrics.CompactionFrozenFiles.Set(reason, int64(n))
	}

	// (D2) A plan that failed recently waits out its backoff, so a merge that
	// fails every time (an object S3 cannot serve) does not take its tenant's
	// slot on every scan; the tenant's next plan goes instead.
	candidates = s.backoff.filter(candidates, now)

	// (E) Priority: open-hour merges, then small-file debt, then oldest.
	sortPlans(candidates)

	// (F) Per-tenant fair share (spec §12.2). MaxConcurrent is merges per
	// tenant per scan: the budget grows with the tenants that have work, as the
	// old per-partition unit (every tenant of one partition) did, while the
	// fair-share cursor decides who goes first.
	budget := s.maxConcurrent * tenantsWithWork(candidates)
	picked := candidates
	if s.fairShare != nil {
		picked = s.fairShare.PickCandidates(candidates, budget)
	} else if len(candidates) > budget {
		picked = candidates[:budget]
	}

	compacted := 0
	served := make(map[string]struct{})
	for i, c := range picked {
		// Bail at a merge boundary if draining (spec §11.1 invariant: never
		// mid-merge).
		if s.draining.Load() {
			break
		}
		// The scan budget: no new merge starts once a scan has run for it, so
		// a scan with thousands of tenants with work cannot run for hours. The
		// fair-share cursor moves past the tenants served, so the next scan
		// starts with the ones this scan did not reach.
		if i > 0 && s.scanBudget > 0 && time.Since(scanStart) >= s.scanBudget {
			metrics.CompactionScanBudgetExhausted.Inc()
			if s.fairShare != nil && len(served) > 1 {
				s.fairShare.Advance(len(served) - 1)
			}
			logger.Infof("compaction scan budget %v reached; merges=%d, plans left=%d", s.scanBudget, compacted, len(picked)-i)
			break
		}
		served[c.tenant] = struct{}{}
		// The plan was made from a snapshot; drop any file that left the
		// manifest or became held since, and re-check there is still a merge.
		selected := compactableNow(s.manifest, c.partition, c.files, s.freeze)
		if len(selected) < 2 {
			continue
		}
		r, err := s.runMerge(ctx, c.partition, selected, c.level, "compacted partition")
		if err != nil {
			s.backoff.failed(c, now, s.interval)
			logger.Errorf("compaction failed: %s; partition=%s, tenant=%s", err, c.partition, c.tenant)
			continue
		}
		if len(r.OutputFiles) == 0 {
			continue // all inputs fenced; they are out of the next plan
		}
		s.backoff.succeeded(c)
		compacted++
	}

	return compacted, nil
}

// tenantsWithWork counts the distinct fair-share tenants among the plans.
func tenantsWithWork(plans []mergePlan) int {
	seen := make(map[string]struct{}, len(plans))
	for _, p := range plans {
		seen[p.tenant] = struct{}{}
	}
	return len(seen)
}

// stillLive returns current metadata for planned files still registered and
// not held. A listing refresh may have changed their storage class.
func stillLive(m *manifest.Manifest, partition string, planned []manifest.FileInfo) []manifest.FileInfo {
	live := make(map[string]manifest.FileInfo)
	for _, f := range withoutHeld(m, m.FilesForPartition(partition)) {
		live[f.Key] = f
	}
	out := planned[:0:0]
	for _, f := range planned {
		if current, ok := live[f.Key]; ok && current.Bucket == f.Bucket {
			out = append(out, current)
		}
	}
	return out
}

// compactableNow rechecks the freeze immediately before each merge, including
// later tenants of a forced recompact or Tier A steal.
func compactableNow(m *manifest.Manifest, partition string, planned []manifest.FileInfo, freeze *LifecycleFreeze) []manifest.FileInfo {
	live := stillLive(m, partition, planned)
	pt, _ := manifest.ParsePartitionTime(partition)
	now := planClock()
	out := live[:0]
	for _, f := range live {
		if frozen, _ := freeze.frozen(f, pt, now); !frozen {
			out = append(out, f)
		}
	}
	return out
}

// runMerge compacts one tenant group's selected files with the scheduler's
// compactor settings and does the shared bookkeeping: attempt mark, in-flight
// gauge, counters, the OnCompacted feed and the log line.
func (s *Scheduler) runMerge(ctx context.Context, partition string, selected []manifest.FileInfo, level int, logMsg string) (*CompactResult, error) {
	// Record the attempt BEFORE compaction so a crash leaves a fresh
	// timestamp (Tier A waits 3*Interval before stealing from us).
	s.manifest.MarkAttempt(partition, time.Now())

	s.inFlight.Add(1)
	metrics.CompactionPartitionsInFlight.Inc()
	compStart := time.Now()

	compactor := NewCompactor(CompactorConfig{
		Pool:                    s.pool,
		Manifest:                s.manifest,
		Prefix:                  s.prefix,
		Mode:                    s.mode,
		RowGroupSize:            s.rowGroupSize,
		CompressionLevel:        s.compressionLevel,
		BloomRebuilder:          s.bloomRebuilder,
		CompactionConfig:        s.compactionCfg,
		TenantCompressionLookup: s.tenantLookup,
		Tombstones:              s.tombstones,
		TombstoneRewriteDelay:   s.tombstoneDelay,
	})
	result, err := compactor.Compact(ctx, partition, selected, level)

	metrics.CompactionPartitionsInFlight.Dec()
	metrics.CompactionInFlightDuration.Observe(time.Since(compStart).Seconds())
	s.inFlight.Done()

	if err != nil {
		metrics.CompactionErrorsTotal.Inc()
		return nil, err
	}

	if len(result.OutputFiles) == 0 {
		// Every input was fenced off (compactGroup): nothing was merged, so
		// this is not a run, and the fenced objects leave the next plan.
		return result, nil
	}

	metrics.CompactionRunsTotal.Inc()
	metrics.CompactionFilesInputTotal.Add(len(result.InputFiles))
	metrics.CompactionFilesOutputTotal.Add(len(result.OutputFiles))
	metrics.CompactionBytesReadTotal.Add(int(result.BytesRead))
	metrics.CompactionBytesWrittenTotal.Add(int(result.BytesWritten))
	metrics.CompactionRowsMergedTotal.Add(int(result.RowsMerged))
	metrics.CompactionDuration.Observe(time.Since(compStart).Seconds())

	if s.onCompacted != nil {
		s.onCompacted(outputsOf(s.manifest, partition, result), result.InputFiles, result.OutputBlooms)
	}

	logger.Infof("%s; partition=%s, level=%d, input_files=%d, output=%s, rows=%d",
		logMsg, partition, level, len(result.InputFiles), result.OutputFile, result.RowsMerged)
	return result, nil
}

// outputsOf returns the manifest entries of a merge's outputs. Only these are
// new: re-announcing every file of the partition on each merge would make the
// pmeta feed and the peer push O(tenants) per merge, O(tenants²) per hour.
func outputsOf(m *manifest.Manifest, partition string, result *CompactResult) []manifest.FileInfo {
	want := make(map[string]bool, len(result.OutputFiles))
	for _, k := range result.OutputFiles {
		want[k] = true
	}
	var out []manifest.FileInfo
	for _, f := range m.FilesForPartition(partition) {
		if want[f.Key] {
			out = append(out, f)
		}
	}
	return out
}

// ForceCompactPartition compacts a partition NOW, bypassing the level-policy
// eligibility gate — the manual-trigger path behind POST /lakehouse/compaction/recompact
// for the compaction hints. The CALLER must verify ownership (RecompactHandler does).
// level <= 0 derives it from the hints (recompactionLevel) or the partition's max
// level. Runs synchronously through the SAME SelectFiles + compactor (+ re-promote)
// path as a scheduled compaction, with identical metrics/onCompacted bookkeeping.
// Returns the result, or an error (draining / not found / fewer than 2 compactable files).
func (s *Scheduler) ForceCompactPartition(ctx context.Context, partition string, level int) (*CompactResult, error) {
	if s.draining.Load() {
		return nil, fmt.Errorf("scheduler is draining; no new compaction accepted")
	}
	now := planClock()
	files := s.segmentGuard(ctx, now).ReleasedFiles(withoutHeld(s.manifest, s.manifest.FilesForPartition(partition)), now)
	if len(files) == 0 {
		return nil, fmt.Errorf("partition not found or empty: %s", partition)
	}
	pt, _ := manifest.ParsePartitionTime(partition)

	// The force bypasses the level thresholds, not the tenant split, the
	// two-file minimum or the lifecycle freeze: each tenant group with two or
	// more compactable files at the level is merged on its own; a lone file is
	// never rewritten.
	result := &CompactResult{Partition: partition}
	merged := 0
	var firstErr error
	for _, g := range groupFilesByTenant(files) {
		var live []manifest.FileInfo
		for _, f := range g.Files {
			if ok, _ := s.freeze.frozen(f, pt, now); !ok {
				live = append(live, f)
			}
		}
		lvl := level
		if lvl <= 0 {
			lvl = 0
			if l, ok := recompactionLevel(live, s.currentFP); ok {
				lvl = l
			} else {
				for _, f := range live {
					if f.CompactionLevel > lvl {
						lvl = f.CompactionLevel
					}
				}
			}
		}
		selected := s.policy.SelectFiles(live, lvl, MajoritySchemaFingerprint(live, lvl))
		selected = compactableNow(s.manifest, partition, selected, s.freeze)
		if len(selected) < 2 {
			continue
		}
		r, err := s.runMerge(ctx, partition, selected, lvl, "forced compaction")
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		result.FencedFiles = append(result.FencedFiles, r.FencedFiles...)
		if len(r.OutputFiles) == 0 {
			continue
		}
		merged++
		result.InputFiles = append(result.InputFiles, r.InputFiles...)
		result.OutputFiles = append(result.OutputFiles, r.OutputFiles...)
		result.RowsMerged += r.RowsMerged
		result.BytesRead += r.BytesRead
		result.BytesWritten += r.BytesWritten
		result.OutputLevel = r.OutputLevel
		result.Duration += r.Duration
		for k, v := range r.OutputBlooms {
			if result.OutputBlooms == nil {
				result.OutputBlooms = make(map[string]map[string][]string)
			}
			result.OutputBlooms[k] = v
		}
	}
	if merged == 0 {
		if firstErr != nil {
			return nil, fmt.Errorf("forced compaction of %s: %w", partition, firstErr)
		}
		if len(result.FencedFiles) > 0 {
			return nil, fmt.Errorf("partition %s: nothing was merged; %d object(s) hold columns this version does not know and were left alone (%s)",
				partition, len(result.FencedFiles), strings.Join(result.FencedFiles, ", "))
		}
		return nil, fmt.Errorf("partition %s has fewer than 2 compactable files at level %d in any tenant", partition, level)
	}
	if len(result.OutputFiles) > 0 {
		result.OutputFile = result.OutputFiles[0]
	}
	if firstErr != nil {
		return result, fmt.Errorf("forced compaction of %s partially completed: %w", partition, firstErr)
	}
	return result, nil
}

// OwnsPartition reports whether this pod is the HRW owner of the partition (true
// when ownership is unset — single-node). Exposed for the recompact trigger's gate.
func (s *Scheduler) OwnsPartition(partition string) bool {
	if s.ownership == nil {
		return true
	}
	return s.ownership.OwnsPartition(partition)
}

// OwnerOf returns the HRW owner peer of the partition ("" when ownership is unset).
func (s *Scheduler) OwnerOf(partition string) string {
	if s.ownership == nil {
		return ""
	}
	return s.ownership.OwnerOf(partition)
}

// recordRingChange ticks the per-type counter and adds the event to
// the sliding-window for §11.4 rate-limit gating. Called from the
// peer-cache OnRingChange callback wired in main.go.
func (s *Scheduler) recordRingChange(eventType string) {
	metrics.CompactionRingChangesTotal.Inc(eventType)
	now := time.Now()
	s.ringEventsMu.Lock()
	defer s.ringEventsMu.Unlock()
	s.pruneRingEventsLocked(now)
	s.ringEvents = append(s.ringEvents, now)
}

// recentRingChanges returns the number of ring-change events observed
// in the trailing 5-minute window.
func (s *Scheduler) recentRingChanges() int {
	now := time.Now()
	s.ringEventsMu.Lock()
	defer s.ringEventsMu.Unlock()
	s.pruneRingEventsLocked(now)
	return len(s.ringEvents)
}

// pruneRingEventsLocked drops events older than 5 minutes. Caller must
// hold ringEventsMu.
func (s *Scheduler) pruneRingEventsLocked(now time.Time) {
	cutoff := now.Add(-5 * time.Minute)
	i := 0
	for ; i < len(s.ringEvents); i++ {
		if s.ringEvents[i].After(cutoff) {
			break
		}
	}
	if i > 0 {
		s.ringEvents = s.ringEvents[i:]
	}
}
