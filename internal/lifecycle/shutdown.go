package lifecycle

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// ShutdownPhase identifies each step in the graceful shutdown sequence.
type ShutdownPhase string

const (
	PhaseDrain   ShutdownPhase = "drain"
	PhaseFlush   ShutdownPhase = "flush"
	PhasePersist ShutdownPhase = "persist"
	PhaseRelease ShutdownPhase = "release"
	PhaseExit    ShutdownPhase = "exit"
)

// ShutdownHooks contains callbacks for each shutdown phase.
type ShutdownHooks struct {
	// Phase 1: Drain — stop accepting new requests.
	OnDrain func(ctx context.Context) error

	// Phase 2: Flush — flush all buffered data to S3.
	OnFlush func(ctx context.Context) (rowsFlushed int64, err error)

	// Phase 3: Persist — save manifest, cache, stats snapshots.
	OnPersist func(ctx context.Context) error

	// Phase 4: Release — release leader lease, notify peers.
	OnRelease func(ctx context.Context) error
}

// ShutdownOrchestrator runs a phased graceful shutdown with per-phase timeouts
// and Prometheus metrics for observability during K8s scaling events.
type ShutdownOrchestrator struct {
	cfg      config.ShutdownConfig
	hooks    ShutdownHooks
	draining atomic.Bool
}

// NewShutdownOrchestrator creates an orchestrator with the given config and hooks.
func NewShutdownOrchestrator(cfg config.ShutdownConfig, hooks ShutdownHooks) *ShutdownOrchestrator {
	return &ShutdownOrchestrator{
		cfg:   cfg,
		hooks: hooks,
	}
}

// IsDraining returns true once Execute has been called (drain phase started).
func (s *ShutdownOrchestrator) IsDraining() bool {
	return s.draining.Load()
}

// Execute runs the 4-phase shutdown: drain -> flush -> persist -> release.
// Each phase runs with its own timeout. Phases continue even if earlier ones
// fail; the first error is returned.
func (s *ShutdownOrchestrator) Execute(ctx context.Context) error {
	logger.Infof("shutdown orchestrator starting; phases: drain(%s) flush(%s) persist(%s) release(%s)",
		s.cfg.Delay, s.cfg.FlushTimeout, s.cfg.PersistTimeout, s.cfg.ReleaseTimeout)

	s.draining.Store(true)
	metrics.ShutdownSuccess.Set(0)

	var firstErr error
	record := func(phase ShutdownPhase, err error) {
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("shutdown phase %s: %w", phase, err)
		}
	}

	// Phase 1: Drain
	record(PhaseDrain, s.runPhase(ctx, PhaseDrain, s.cfg.Delay, s.hooks.OnDrain))

	// Phase 2: Flush
	record(PhaseFlush, s.runPhase(ctx, PhaseFlush, s.cfg.FlushTimeout, func(ctx context.Context) error {
		if s.hooks.OnFlush == nil {
			return nil
		}
		rows, err := s.hooks.OnFlush(ctx)
		if rows > 0 {
			metrics.ShutdownFlushRows.Add(int(rows))
		}
		return err
	}))

	// Phase 3: Persist
	record(PhasePersist, s.runPhase(ctx, PhasePersist, s.cfg.PersistTimeout, s.hooks.OnPersist))

	// Phase 4: Release
	record(PhaseRelease, s.runPhase(ctx, PhaseRelease, s.cfg.ReleaseTimeout, s.hooks.OnRelease))

	if firstErr == nil {
		metrics.ShutdownSuccess.Set(1)
		logger.Infof("shutdown orchestrator completed successfully")
	} else {
		logger.Errorf("shutdown orchestrator completed with errors: %s", firstErr)
	}

	return firstErr
}

// runPhase executes a single shutdown phase with timeout and metrics.
func (s *ShutdownOrchestrator) runPhase(ctx context.Context, phase ShutdownPhase, timeout time.Duration, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}

	phaseLabel := string(phase)
	metrics.ShutdownPhaseActive.Set(phaseLabel, 1)
	defer metrics.ShutdownPhaseActive.Set(phaseLabel, 0)

	start := time.Now()
	defer func() {
		dur := time.Since(start).Seconds()
		metrics.ShutdownPhaseDuration.Observe(dur)
		logger.Infof("shutdown phase %s completed; duration=%s", phase, time.Since(start))
	}()

	if timeout <= 0 {
		return fn(ctx)
	}

	phaseCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- fn(phaseCtx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-phaseCtx.Done():
		return fmt.Errorf("phase %s timed out after %s", phase, timeout)
	}
}
