package delete

import (
	"context"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// RewriteSchedulerConfig holds the configuration for a RewriteScheduler.
type RewriteSchedulerConfig struct {
	Store          *TombstoneStore
	Rewriter       *Rewriter
	Detector       *StorageClassDetector
	RewriteDelay   time.Duration
	AllowedClasses []string
	MaxConcurrent  int
}

// RewriteScheduler periodically processes pending tombstones by rewriting
// affected Parquet files to permanently remove deleted rows.
type RewriteScheduler struct {
	store          *TombstoneStore
	rewriter       *Rewriter
	detector       *StorageClassDetector
	rewriteDelay   time.Duration
	allowedClasses map[string]bool
	maxConcurrent  int
	stopCh         chan struct{}
}

// NewRewriteScheduler creates a RewriteScheduler from the given config.
func NewRewriteScheduler(cfg RewriteSchedulerConfig) *RewriteScheduler {
	allowed := make(map[string]bool)
	classes := cfg.AllowedClasses
	if len(classes) == 0 {
		classes = []string{"STANDARD"}
	}
	for _, c := range classes {
		allowed[c] = true
	}

	maxConc := cfg.MaxConcurrent
	if maxConc <= 0 {
		maxConc = 1
	}

	return &RewriteScheduler{
		store:          cfg.Store,
		rewriter:       cfg.Rewriter,
		detector:       cfg.Detector,
		rewriteDelay:   cfg.RewriteDelay,
		allowedClasses: allowed,
		maxConcurrent:  maxConc,
		stopCh:         make(chan struct{}),
	}
}

// Start launches a background goroutine that calls RunOnce at the given interval.
func (s *RewriteScheduler) Start(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				ctx := context.Background()
				s.RunOnce(ctx)
			}
		}
	}()
}

// Stop signals the background loop to exit.
func (s *RewriteScheduler) Stop() {
	close(s.stopCh)
}

// RunOnce processes all eligible active tombstones, rewriting affected files.
func (s *RewriteScheduler) RunOnce(ctx context.Context) []RewriteResult {
	now := time.Now()
	active := s.store.Active()

	var results []RewriteResult

	for i := range active {
		ts := active[i]

		// Hide mode never rewrites.
		if ts.Mode == "hide" {
			continue
		}

		// Skip if tombstone is too recent (not past rewrite delay).
		if now.Sub(ts.CreatedAt) < s.rewriteDelay {
			continue
		}

		updated := false
		for _, key := range ts.AffectedKeys {
			// Skip already reaped keys.
			if ts.Reaped != nil && ts.Reaped[key] {
				continue
			}

			// Detect storage class from file age, honoring per-tenant
			// lifecycle overrides when the key carries a tenant prefix.
			fileAgeHours := now.Sub(ts.CreatedAt).Hours()
			class := s.detector.DetectForKey(fileAgeHours, key)

			if !s.allowedClasses[string(class)] {
				metrics.DeleteRewriteSkippedGlacier.Inc()
				logger.Infof("skipping rewrite: storage class not allowed; key=%s, class=%s", key, string(class))
				continue
			}

			// Perform the rewrite.
			result, err := s.rewriter.RewriteFile(ctx, key, []Tombstone{ts})
			if err != nil {
				metrics.DeleteRewriteErrors.Inc()
				logger.Errorf("rewrite failed: %s; key=%s", err, key)
				continue
			}

			metrics.DeleteRewriteTotal.Inc()
			results = append(results, *result)

			// Mark key as reaped.
			if ts.Reaped == nil {
				ts.Reaped = make(map[string]bool)
			}
			ts.Reaped[key] = true
			updated = true
		}

		// Persist updated reaped state back to store.
		if updated {
			s.store.Add(ts)
		}
	}

	// Run verification pass.
	s.Verify(ctx)

	return results
}

// Verify checks active tombstones for effectiveness.
// Placeholder: increments DeleteVerifyTotal for each active tombstone.
func (s *RewriteScheduler) Verify(_ context.Context) {
	active := s.store.Active()
	for range active {
		metrics.DeleteVerifyTotal.Inc()
	}
}
