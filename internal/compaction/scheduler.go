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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
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
	OnCompacted             func(added []manifest.FileInfo, removed []string)

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
	compactionCfg    config.CompactionConfig
	tenantLookup     func(tenantPrefix string) []int
	onCompacted      func(added []manifest.FileInfo, removed []string)

	ringChangeRate int
	drainTimeout   time.Duration

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
		compactionCfg:    cfg.CompactionConfig,
		tenantLookup:     cfg.TenantCompressionLookup,
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

// partitionCandidate pairs a partition name with its eligible compaction level.
type partitionCandidate struct {
	partition string
	level     int
	time      time.Time
}

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

	allFiles := s.manifest.AllFiles()

	// (D) HRW-based ownership + eligibility.
	owned := 0
	var candidates []partitionCandidate
	for partition, files := range allFiles {
		if !s.ownership.OwnsPartition(partition) {
			continue
		}
		owned++
		pt, err := manifest.ParsePartitionTime(partition)
		if err != nil {
			logger.Warnf("skip partition: cannot parse time; partition=%s, error=%s", partition, err)
			continue
		}
		level, eligible := s.policy.Eligible(files, pt)
		if !eligible {
			// Hint-driven recompaction: the level policy only merges L0/L1, so
			// stale-schema files (re-promotion targets) and top-level fragmentation
			// are never re-picked. Consume the compaction hints so old /
			// poorly-compacted areas heal without waiting for new input files — the
			// existing SelectFiles + compactor path below does the merge + re-promote,
			// and a partition self-resolves in one pass (becomes non-stale /
			// non-fragmented, so it isn't re-picked).
			if lvl, needs := recompactionLevel(files, s.currentFP); needs {
				level, eligible = lvl, true
			}
		}
		if !eligible {
			continue
		}
		candidates = append(candidates, partitionCandidate{
			partition: partition,
			level:     level,
			time:      pt,
		})
	}
	metrics.CompactionPartitionsOwned.Set(int64(owned))
	metrics.CompactionOwnershipSelfInPeers.Set(s.ownership.SelfInPeersGauge())

	// (E) Sort candidates by partition time, oldest first — same
	// ordering as the pre-PR-A scheduler.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].time.Before(candidates[j].time)
	})

	// (F) Apply per-tenant fair-share (spec §12.2) if configured.
	picked := candidates
	if s.fairShare != nil {
		picked = s.fairShare.PickCandidates(candidates, s.maxConcurrent)
	} else if len(candidates) > s.maxConcurrent {
		picked = candidates[:s.maxConcurrent]
	}

	compacted := 0
	for _, c := range picked {
		// Bail at partition boundary if draining (spec §11.1
		// invariant: never mid-merge).
		if s.draining.Load() {
			break
		}

		// (G) Record attempt BEFORE compaction so a crash leaves a
		// fresh timestamp (Tier A waits 3*Interval before stealing
		// from us).
		s.manifest.MarkAttempt(c.partition, time.Now())

		partFiles := s.manifest.FilesForPartition(c.partition)
		fp := MajoritySchemaFingerprint(partFiles, c.level)
		selected := s.policy.SelectFiles(partFiles, c.level, fp)
		if len(selected) < 2 {
			continue
		}

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
		})

		result, err := compactor.Compact(ctx, c.partition, selected, c.level)

		metrics.CompactionPartitionsInFlight.Dec()
		metrics.CompactionInFlightDuration.Observe(time.Since(compStart).Seconds())
		s.inFlight.Done()

		if err != nil {
			logger.Errorf("compaction failed: %s; partition=%s", err, c.partition)
			metrics.CompactionErrorsTotal.Inc()
			continue
		}

		metrics.CompactionRunsTotal.Inc()
		metrics.CompactionFilesInputTotal.Add(len(selected))
		metrics.CompactionFilesOutputTotal.Inc()
		metrics.CompactionBytesReadTotal.Add(int(result.BytesRead))
		metrics.CompactionBytesWrittenTotal.Add(int(result.BytesWritten))
		metrics.CompactionRowsMergedTotal.Add(int(result.RowsMerged))
		metrics.CompactionDuration.Observe(time.Since(compStart).Seconds())

		if s.onCompacted != nil {
			addedFiles := s.manifest.FilesForPartition(c.partition)
			removedKeys := make([]string, 0, len(selected))
			for _, sel := range selected {
				removedKeys = append(removedKeys, sel.Key)
			}
			s.onCompacted(addedFiles, removedKeys)
		}

		logger.Infof("compacted partition; partition=%s, level=%d, input_files=%d, output=%s, rows=%d",
			c.partition, c.level, len(selected), result.OutputFile, result.RowsMerged)
		compacted++
	}

	return compacted, nil
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
	files := s.manifest.FilesForPartition(partition)
	if len(files) == 0 {
		return nil, fmt.Errorf("partition not found or empty: %s", partition)
	}
	if level <= 0 {
		if lvl, ok := recompactionLevel(files, s.currentFP); ok {
			level = lvl
		} else {
			for _, f := range files {
				if f.CompactionLevel > level {
					level = f.CompactionLevel
				}
			}
		}
	}
	fp := MajoritySchemaFingerprint(files, level)
	selected := s.policy.SelectFiles(files, level, fp)
	if len(selected) < 2 {
		return nil, fmt.Errorf("partition %s has fewer than 2 compactable files at level %d", partition, level)
	}

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
	})
	result, err := compactor.Compact(ctx, partition, selected, level)

	metrics.CompactionPartitionsInFlight.Dec()
	metrics.CompactionInFlightDuration.Observe(time.Since(compStart).Seconds())
	s.inFlight.Done()

	if err != nil {
		metrics.CompactionErrorsTotal.Inc()
		return nil, fmt.Errorf("forced compaction of %s: %w", partition, err)
	}

	metrics.CompactionRunsTotal.Inc()
	metrics.CompactionFilesInputTotal.Add(len(selected))
	metrics.CompactionFilesOutputTotal.Inc()
	metrics.CompactionBytesReadTotal.Add(int(result.BytesRead))
	metrics.CompactionBytesWrittenTotal.Add(int(result.BytesWritten))
	metrics.CompactionRowsMergedTotal.Add(int(result.RowsMerged))
	metrics.CompactionDuration.Observe(time.Since(compStart).Seconds())

	if s.onCompacted != nil {
		added := s.manifest.FilesForPartition(partition)
		removed := make([]string, 0, len(selected))
		for _, sel := range selected {
			removed = append(removed, sel.Key)
		}
		s.onCompacted(added, removed)
	}
	logger.Infof("forced compaction; partition=%s, level=%d, input_files=%d, output=%s, rows=%d",
		partition, level, len(selected), result.OutputFile, result.RowsMerged)
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
