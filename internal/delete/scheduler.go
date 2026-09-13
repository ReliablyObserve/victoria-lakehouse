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

	// Manifest receives the rewritten object's registration. It is NOT
	// optional for rewriting: a rewrite that cannot be published leaves the
	// replacement unmanifested (the orphan sweep reclaims it, losing the kept
	// rows) and the manifest pointing at a deleted key. When Manifest is nil
	// the scheduler refuses to rewrite anything and counts
	// lakehouse_delete_rewrite_skipped_no_manifest_total instead.
	Manifest ManifestUpdater
}

// RewriteScheduler periodically processes pending tombstones by rewriting
// affected Parquet files to permanently remove deleted rows.
type RewriteScheduler struct {
	store          *TombstoneStore
	rewriter       *Rewriter
	detector       *StorageClassDetector
	manifest       ManifestUpdater
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
		manifest:       cfg.Manifest,
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
//
// Each key goes through the full prepare → publish → commit sequence; a failure
// at any step leaves the key un-reaped so the next tick retries it, and never
// leaves the manifest disagreeing with what is actually in the bucket.
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

		// Rewriting without a manifest orphans the replacement. Refuse the
		// whole tombstone rather than destroy data; the rows stay hidden by
		// the query-time filter in the meantime, which is the safe state.
		if s.manifest == nil {
			metrics.DeleteRewriteSkippedNoManifest.Inc()
			logger.Warnf("skipping rewrite: no manifest wired into the delete scheduler; tombstone=%s", ts.ID)
			continue
		}

		updated := false
		for _, key := range ts.AffectedKeys {
			// Skip already reaped keys.
			if ts.Reaped != nil && ts.Reaped[key] {
				continue
			}

			// Self-healing: a crash between the manifest swap and the reaped
			// bookkeeping leaves a key that is already gone from both the
			// manifest and the bucket. Retrying its download would fail
			// forever; recognise it as done instead.
			if !s.manifest.HasKey(key) {
				metrics.DeleteRewriteAlreadyReaped.Inc()
				logger.Infof("rewrite key already superseded; key=%s, tombstone=%s", key, ts.ID)
				markReaped(&ts, key)
				updated = true
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

			result, ok := s.rewriteOne(ctx, key, ts)
			if !ok {
				continue
			}

			results = append(results, *result)
			markReaped(&ts, key)
			updated = true
		}

		// Persist updated reaped state back to store.
		if updated {
			s.store.Add(ts)
			// A tombstone whose every key has been rewritten has no work left.
			// Retiring it is what keeps Active() bounded and lets the manifest
			// metadata fast paths come back; leaving it active forever was the
			// second half of the original bug.
			s.store.Complete(ts.ID)
		}
	}

	// Drain any tombstone durability writes S3 rejected earlier.
	s.store.FlushPending(ctx)

	// Run verification pass.
	s.Verify(ctx)

	return results
}

// rewriteOne performs the three-step rewrite of a single key. It returns the
// result only when the object has been rewritten AND published AND the
// superseded object cleaned up, so the caller may mark the key reaped.
func (s *RewriteScheduler) rewriteOne(ctx context.Context, key string, ts Tombstone) (*RewriteResult, bool) {
	// Step 1 — prepare: write the replacement object. The source is untouched.
	result, err := s.rewriter.RewriteFile(ctx, key, []Tombstone{ts})
	if err != nil {
		metrics.DeleteRewriteErrors.Inc()
		logger.Errorf("rewrite failed: %s; key=%s", err, key)
		return nil, false
	}

	if result.RowsRemoved == 0 {
		// The tombstone named this file but no row in it matches. Nothing was
		// written and nothing must be deleted; the key is done.
		metrics.DeleteRewriteTotal.Inc()
		return result, true
	}

	// Step 2 — publish: swap the manifest entry. Until this succeeds the
	// superseded object is the ONLY copy of the kept rows and must survive.
	if err := publishRewrite(s.manifest, result); err != nil {
		metrics.DeleteRewriteErrors.Inc()
		logger.Errorf("rewrite manifest publish failed: %s; key=%s, new=%s", err, key, result.NewKey)
		return nil, false
	}

	// Step 3 — commit: drop the superseded object. A failure here costs storage
	// (the orphan sweep reclaims it) but cannot lose data, so the key still
	// counts as reaped.
	if err := s.rewriter.Commit(ctx, result); err != nil {
		metrics.DeleteRewriteOldObjectErrors.Inc()
		logger.Warnf("superseded object not deleted (orphan sweep will reclaim it): %s", err)
	}

	metrics.DeleteRewriteTotal.Inc()
	if saved := result.BytesBefore - result.BytesAfter; saved > 0 {
		metrics.DeleteRewriteBytesSaved.Add(int(saved))
	}
	return result, true
}

func markReaped(ts *Tombstone, key string) {
	if ts.Reaped == nil {
		ts.Reaped = make(map[string]bool)
	}
	ts.Reaped[key] = true
}

// Verify checks active tombstones for effectiveness.
// Placeholder: increments DeleteVerifyTotal for each active tombstone.
func (s *RewriteScheduler) Verify(_ context.Context) {
	active := s.store.Active()
	for range active {
		metrics.DeleteVerifyTotal.Inc()
	}
}
