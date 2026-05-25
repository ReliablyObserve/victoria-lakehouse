package lifecycle

import (
	"os"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// StalenessDetector checks whether a pod's persistent volume is stale
// (i.e. the manifest snapshot is older than the configured threshold)
// and orchestrates WAL reconciliation and cache invalidation when needed.
type StalenessDetector struct {
	cfg              config.StartupConfig
	manifestPath     string
	staleDetected    bool
	stalenessAge     time.Duration
	walReconciled    bool
	cacheRevalidated bool
	manifestTime     time.Time
}

// NewStalenessDetector creates a detector bound to the given config and manifest path.
func NewStalenessDetector(cfg config.StartupConfig, manifestPath string) *StalenessDetector {
	return &StalenessDetector{
		cfg:          cfg,
		manifestPath: manifestPath,
	}
}

// Check inspects the manifest file's modification time and marks the PV
// as stale when it exceeds the configured threshold.
func (d *StalenessDetector) Check() {
	info, err := os.Stat(d.manifestPath)
	if err != nil {
		logger.Infof("staleness check: no manifest snapshot at %s; treating as fresh start", d.manifestPath)
		return
	}

	d.manifestTime = info.ModTime()
	d.stalenessAge = time.Since(d.manifestTime)

	metrics.StartupStalenessHours.Set(d.stalenessAge.Hours())

	if d.stalenessAge > d.cfg.StaleThreshold {
		d.staleDetected = true
		metrics.StartupStalePVDetected.Set(1)
		logger.Warnf("stale PV detected; manifest_age=%s, threshold=%s", d.stalenessAge, d.cfg.StaleThreshold)
	} else {
		logger.Infof("staleness check passed; manifest_age=%s, threshold=%s", d.stalenessAge, d.cfg.StaleThreshold)
	}
}

// IsStale returns true when the manifest age exceeds the stale threshold.
func (d *StalenessDetector) IsStale() bool {
	return d.staleDetected
}

// StalenessAge returns the age of the manifest snapshot at check time.
func (d *StalenessDetector) StalenessAge() time.Duration {
	return d.stalenessAge
}

// ManifestTime returns the modification time of the manifest file.
func (d *StalenessDetector) ManifestTime() time.Time {
	return d.manifestTime
}

// WALReconciled returns true after a successful ReconcileWAL call.
func (d *StalenessDetector) WALReconciled() bool {
	return d.walReconciled
}

// CacheRevalidated returns true after InvalidateCache has run.
func (d *StalenessDetector) CacheRevalidated() bool {
	return d.cacheRevalidated
}

// Info returns a snapshot of the staleness state for observability endpoints.
func (d *StalenessDetector) Info() *StalenessInfo {
	return &StalenessInfo{
		StaleDetected:     d.staleDetected,
		StalenessAge:      d.stalenessAge,
		WALReconciled:     d.walReconciled,
		CacheRevalidated:  d.cacheRevalidated,
		ManifestTimestamp: d.manifestTime,
	}
}

// WALEntry represents a single WAL entry for reconciliation purposes.
type WALEntry struct {
	TimestampNs int64
	IsLog       bool
}

// ManifestChecker abstracts the manifest's ability to confirm whether
// a given timestamp is already covered by a flushed S3 file.
type ManifestChecker interface {
	HasFileForTimestamp(timestampNs int64) bool
}

// ReconcileWAL checks WAL entries against the manifest and returns
// the count of entries that need re-flushing (entries not yet in S3).
// It does not perform the actual re-flush — that's the caller's responsibility.
func (d *StalenessDetector) ReconcileWAL(entries []WALEntry, checker ManifestChecker) int64 {
	if !d.cfg.WALReconciliation || !d.staleDetected {
		return 0
	}

	var needsReflush int64
	for _, entry := range entries {
		if !checker.HasFileForTimestamp(entry.TimestampNs) {
			needsReflush++
		}
	}

	metrics.StartupWALReconciledRows.Add(int(needsReflush))
	d.walReconciled = true
	logger.Infof("WAL reconciliation complete; total_entries=%d, needs_reflush=%d", len(entries), needsReflush)
	return needsReflush
}

// InvalidateCache marks cache entries as requiring revalidation after a stale PV is detected.
func (d *StalenessDetector) InvalidateCache(entryCount int) {
	if !d.cfg.CacheRevalidation || !d.staleDetected {
		return
	}
	metrics.StartupCacheInvalidated.Add(entryCount)
	d.cacheRevalidated = true
	logger.Infof("cache invalidation marked; entries=%d", entryCount)
}
