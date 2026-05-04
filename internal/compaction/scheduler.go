package compaction

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/election"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// SchedulerConfig holds all dependencies for the Scheduler.
type SchedulerConfig struct {
	Leader           election.Leader
	Manifest         *manifest.Manifest
	Pool             CompactorPool
	Sentinel         *Sentinel
	Policy           *LevelPolicy
	Prefix           string
	Mode             config.Mode
	Interval         time.Duration
	MaxConcurrent    int
	RowGroupSize     int
	CompressionLevel int
	Logger           *slog.Logger
	OnCompacted      func(added []manifest.FileInfo, removed []string)
}

// Scheduler runs periodic compaction scans.
type Scheduler struct {
	leader           election.Leader
	manifest         *manifest.Manifest
	pool             CompactorPool
	sentinel         *Sentinel
	policy           *LevelPolicy
	prefix           string
	mode             config.Mode
	interval         time.Duration
	maxConcurrent    int
	rowGroupSize     int
	compressionLevel int
	logger           *slog.Logger
	onCompacted      func(added []manifest.FileInfo, removed []string)

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewScheduler creates a Scheduler from the given config.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	interval := cfg.Interval
	if interval == 0 {
		interval = 5 * time.Minute
	}
	maxConc := cfg.MaxConcurrent
	if maxConc == 0 {
		maxConc = 1
	}
	return &Scheduler{
		leader:           cfg.Leader,
		manifest:         cfg.Manifest,
		pool:             cfg.Pool,
		sentinel:         cfg.Sentinel,
		policy:           cfg.Policy,
		prefix:           cfg.Prefix,
		mode:             cfg.Mode,
		interval:         interval,
		maxConcurrent:    maxConc,
		rowGroupSize:     cfg.RowGroupSize,
		compressionLevel: cfg.CompressionLevel,
		logger:           cfg.Logger.With("component", "scheduler"),
		onCompacted:      cfg.OnCompacted,
		stopCh:           make(chan struct{}),
	}
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
					s.logger.Error("scan failed", "error", err)
				} else if n > 0 {
					s.logger.Info("scan completed", "compactions", n)
				}
			}
		}
	}()
}

// Stop signals the background goroutine to stop and waits for it.
func (s *Scheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// partitionCandidate pairs a partition name with its eligible compaction level.
type partitionCandidate struct {
	partition string
	level     int
	time      time.Time
}

// Scan runs one compaction cycle: check leadership, find eligible partitions,
// and compact up to MaxConcurrent of them.
func (s *Scheduler) Scan(ctx context.Context) (int, error) {
	if !s.leader.IsLeader() {
		s.logger.Debug("not leader, skipping scan")
		return 0, nil
	}

	allFiles := s.manifest.AllFiles()

	// Find eligible partitions.
	var candidates []partitionCandidate
	for partition, files := range allFiles {
		pt, err := manifest.ParsePartitionTime(partition)
		if err != nil {
			s.logger.Warn("skip partition: cannot parse time", "partition", partition, "error", err)
			continue
		}
		level, eligible := s.policy.Eligible(files, pt)
		if !eligible {
			continue
		}
		candidates = append(candidates, partitionCandidate{
			partition: partition,
			level:     level,
			time:      pt,
		})
	}

	// Sort by partition time, oldest first.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].time.Before(candidates[j].time)
	})

	compacted := 0
	for _, c := range candidates {
		if compacted >= s.maxConcurrent {
			break
		}

		locked, err := s.sentinel.IsLocked(ctx, s.prefix, c.partition)
		if err != nil {
			s.logger.Warn("sentinel check failed", "partition", c.partition, "error", err)
			continue
		}
		if locked {
			s.logger.Debug("partition locked, skipping", "partition", c.partition)
			continue
		}

		ok, err := s.sentinel.Acquire(ctx, s.prefix, c.partition, "scheduler")
		if err != nil {
			s.logger.Warn("sentinel acquire failed", "partition", c.partition, "error", err)
			continue
		}
		if !ok {
			continue
		}

		// Find majority schema fingerprint at this level.
		partFiles := s.manifest.FilesForPartition(c.partition)
		fp := MajoritySchemaFingerprint(partFiles, c.level)

		// Select files at the eligible level with that fingerprint.
		selected := s.policy.SelectFiles(partFiles, c.level, fp)
		if len(selected) < 2 {
			if err := s.sentinel.Release(ctx, s.prefix, c.partition); err != nil {
				s.logger.Warn("sentinel release failed", "partition", c.partition, "error", err)
			}
			continue
		}

		// Run compaction.
		compactor := NewCompactor(CompactorConfig{
			Pool:             s.pool,
			Manifest:         s.manifest,
			Prefix:           s.prefix,
			Mode:             s.mode,
			RowGroupSize:     s.rowGroupSize,
			CompressionLevel: s.compressionLevel,
			Logger:           s.logger,
		})

		result, err := compactor.Compact(ctx, c.partition, selected, c.level)
		if err != nil {
			s.logger.Error("compaction failed", "partition", c.partition, "error", err)
			if relErr := s.sentinel.Release(ctx, s.prefix, c.partition); relErr != nil {
				s.logger.Warn("sentinel release after failure", "partition", c.partition, "error", relErr)
			}
			continue
		}

		if err := s.sentinel.Release(ctx, s.prefix, c.partition); err != nil {
			s.logger.Warn("sentinel release failed", "partition", c.partition, "error", err)
		}

		// Call OnCompacted callback if set.
		if s.onCompacted != nil {
			addedFiles := s.manifest.FilesForPartition(c.partition)
			var removedKeys []string
			for _, sel := range selected {
				removedKeys = append(removedKeys, sel.Key)
			}
			s.onCompacted(addedFiles, removedKeys)
		}

		s.logger.Info("compacted partition",
			"partition", c.partition,
			"level", c.level,
			"input_files", len(selected),
			"output", result.OutputFile,
			"rows", result.RowsMerged,
		)
		compacted++
	}

	return compacted, nil
}
