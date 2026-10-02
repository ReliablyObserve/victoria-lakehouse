package startup

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"sync"
	"testing"
	"time"
)

func clearReviewMetrics() {
	metrics.Ready.Set(0)
	metrics.ServingReady.Set(0)
	metrics.WarmupComplete.Set(0)
}

func TestReviewStartup_WarmupCannotGrantServing(t *testing.T) {
	clearReviewMetrics()
	m := NewManager(0)
	m.SetPhase(PhaseS3Refresh)
	m.SetWarmupComplete()
	if m.ServingReady() || m.IsReady() || metrics.Ready.Get() != 0 {
		t.Fatal("warmup granted serving before foreground recovery")
	}
	m.SetServingReady()
	if !m.IsReady() || metrics.Ready.Get() != 1 {
		t.Fatal("ready metric did not follow serving transition")
	}
	clearReviewMetrics()
	n := NewManager(0)
	n.SetServingReady()
	n.SetPhase(PhaseS3Refresh)
	n.SetWarmupComplete()
	if n.Phase() != PhaseReady || metrics.StartupPhase.Get() != int64(PhaseReady) || metrics.Ready.Get() != 1 {
		t.Fatal("completed background path did not publish ready state and metrics")
	}
	if n.TotalSeconds() < n.RecoverySeconds() || n.RefreshSeconds() < 0 || metrics.StartupTotalSeconds.Get() != n.TotalSeconds() {
		t.Fatal("completion timing not coherent")
	}
}

func TestReviewStartup_ManifestGateMetricsMatchState(t *testing.T) {
	clearReviewMetrics()
	m := NewManager(3)
	m.SetServingReady()
	m.SetPhase(PhaseS3Refresh)
	m.SetWarmupComplete()
	if m.ServingReady() || m.IsReady() || metrics.Ready.Get() != 0 || metrics.ServingReady.Get() != 0 {
		t.Fatal("manifest gate bypassed")
	}
	m.SetManifestFiles(3)
	if !m.IsReady() || metrics.Ready.Get() != 1 || metrics.ServingReady.Get() != 1 {
		t.Fatalf("after the manifest gate opened: ready=%v ready_metric=%d serving=%v serving_metric=%d", m.IsReady(), metrics.Ready.Get(), m.ServingReady(), metrics.ServingReady.Get())
	}
}

func TestReviewStartup_LegacyDimensionMetricsMatchState(t *testing.T) {
	clearReviewMetrics()
	m := NewManager(0)
	m.SetPhase(PhaseDiskRecovery)
	m.SetPhase(PhaseS3Refresh)
	m.SetPhase(PhaseReady)
	if !m.IsReady() || metrics.Ready.Get() != 1 || metrics.ServingReady.Get() != 1 || metrics.WarmupComplete.Get() != 1 {
		t.Fatalf("legacy readiness metrics: ready=%d serving=%d warmup=%d", metrics.Ready.Get(), metrics.ServingReady.Get(), metrics.WarmupComplete.Get())
	}
}

func TestReviewStartup_CompletedTimingsAndGates(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		clearReviewMetrics()
		m := NewManager(10)
		m.SetPhase(PhaseDiskRecovery)
		m.startTime = time.Now().Add(-time.Second)
		m.SetPhase(PhaseS3Refresh)
		recovery := m.RecoverySeconds()
		if !legacy {
			m.SetWarmupComplete()
		} else {
			m.SetPhase(PhaseReady)
		}
		if m.Phase() != PhaseReady || m.TotalSeconds() < recovery || m.RefreshSeconds() < 0 {
			t.Fatalf("bad completion chronology: r=%g f=%g t=%g", recovery, m.RefreshSeconds(), m.TotalSeconds())
		}
		if m.IsReady() || metrics.Ready.Get() != 0 {
			t.Fatal("manifest gate bypassed")
		}
		if !legacy {
			m.SetServingReady()
		}
		m.SetManifestFiles(10)
		if !m.IsReady() || metrics.Ready.Get() != 1 {
			t.Fatal("manifest gate did not open readiness")
		}
		m.SetManifestFiles(0)
		if m.IsReady() || metrics.Ready.Get() != 0 {
			t.Fatal("ready did not follow manifest gate downgrade")
		}
	}
}

func TestReviewStartup_ConcurrentTimingAndReadinessReaders(t *testing.T) {
	clearReviewMetrics()
	m := NewManager(10)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10000 {
				_ = m.Phase()
				_ = m.RecoverySeconds()
				_ = m.RefreshSeconds()
				_ = m.TotalSeconds()
				_ = m.ServingReady()
				_ = m.IsReady()
			}
		}()
	}
	m.SetPhase(PhaseS3Refresh)
	m.SetServingReady()
	m.SetManifestFiles(9)
	m.SetWarmupComplete()
	if m.IsReady() {
		t.Fatal("concurrent readers affected the manifest gate")
	}
	m.SetManifestFiles(10)
	wg.Wait()
	if !m.IsReady() {
		t.Fatal("startup did not finish")
	}
}

func TestReviewStartup_ConcurrentGateMetricConverges(t *testing.T) {
	for round := 0; round < 2000; round++ {
		m := NewManager(10)
		m.servingReady.Store(true)
		m.warmupComplete.Store(true)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for _, n := range []int64{0, 10} {
			wg.Add(1)
			go func(n int64) {
				defer wg.Done()
				<-start
				for range 32 {
					m.SetManifestFiles(n)
				}
			}(n)
		}
		close(start)
		wg.Wait()
		want := int64(0)
		if m.IsReady() {
			want = 1
		}
		if metrics.Ready.Get() != want {
			t.Fatalf("round %d: settled ready=%v but ready gauge=%d", round, m.IsReady(), metrics.Ready.Get())
		}
	}
}
