package startup

import (
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

func TestWarmupCompleteTransitionsPhaseAndTiming(t *testing.T) {
	m := NewManager(0)
	m.SetPhase(PhaseDiskRecovery)
	m.SetServingReady()
	m.SetPhase(PhaseS3Refresh)
	m.SetWarmupComplete()
	if m.Phase() != PhaseReady {
		t.Errorf("phase after background warmup = %s, want ready", m.Phase())
	}
	if !m.IsReady() || !m.ServingReady() || !m.WarmupComplete() {
		t.Fatal("completed warmup must preserve both readiness dimensions")
	}
	// Two adjacent monotonic clock reads can coincide; a zero refresh duration
	// is valid, but the total must be recorded and include disk recovery.
	if m.TotalSeconds() <= 0 || m.RefreshSeconds() < 0 || m.TotalSeconds() < m.RecoverySeconds() {
		t.Errorf("completion timings not recorded: total=%g refresh=%g", m.TotalSeconds(), m.RefreshSeconds())
	}
	if metrics.StartupPhase.Get() != int64(PhaseReady) || metrics.StartupTotalSeconds.Get() != m.TotalSeconds() {
		t.Fatal("startup phase and completion timing metrics disagree with manager")
	}
	if metrics.Ready.Get() != 1 {
		t.Fatal("ready metric must reflect completed serving and warmup")
	}
}

func TestWarmupCompleteDoesNotBypassManifestGate(t *testing.T) {
	m := NewManager(5)
	m.SetServingReady()
	m.SetWarmupComplete()
	if m.IsReady() || m.ServingReady() {
		t.Fatal("warmup completion must not bypass the manifest gate")
	}
	m.SetManifestFiles(5)
	if !m.IsReady() {
		t.Fatal("an open manifest gate must make both readiness dimensions pass")
	}
}

func TestWarmupTimingConcurrentReaders(t *testing.T) {
	m := NewManager(0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 1000 {
			_ = m.RecoverySeconds()
			_ = m.RefreshSeconds()
			_ = m.TotalSeconds()
		}
	}()
	m.SetPhase(PhaseS3Refresh)
	m.SetWarmupComplete()
	wg.Wait()
}

func TestWarmupCompleteDoesNotGrantServingReadiness(t *testing.T) {
	m := NewManager(100)
	m.SetPhase(PhaseS3Refresh)
	m.SetWarmupComplete()
	if m.ServingReady() || m.IsReady() {
		t.Fatal("warmup completion must not bypass serving readiness")
	}
	if metrics.Ready.Get() != 0 {
		t.Fatal("ready metric must honor minimum manifest gate")
	}
	m.SetServingReady()
	if m.ServingReady() || m.IsReady() {
		t.Fatal("warmup completion must not bypass the minimum manifest gate")
	}
	m.SetManifestFiles(100)
	if !m.ServingReady() || !m.IsReady() {
		t.Fatal("both readiness dimensions must pass when manifest gate is satisfied")
	}
}
