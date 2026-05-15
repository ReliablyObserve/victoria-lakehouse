package startup

import (
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type Phase int32

const (
	PhaseInit         Phase = 0
	PhaseDiskRecovery Phase = 1
	PhaseS3Refresh    Phase = 2
	PhaseReady        Phase = 3
)

func (p Phase) String() string {
	switch p {
	case PhaseInit:
		return "init"
	case PhaseDiskRecovery:
		return "disk_recovery"
	case PhaseS3Refresh:
		return "s3_refresh"
	case PhaseReady:
		return "ready"
	default:
		return "unknown"
	}
}

type Manager struct {
	phase        atomic.Int32
	ready        atomic.Bool
	startTime    time.Time
	recoveryTime time.Duration
	refreshTime  time.Duration
	totalTime    time.Duration
	catchupFiles int64
}

func NewManager() *Manager {
	m := &Manager{
		startTime: time.Now(),
	}
	m.phase.Store(int32(PhaseInit))
	return m
}

func (m *Manager) Phase() Phase {
	return Phase(m.phase.Load())
}

func (m *Manager) IsReady() bool {
	return m.ready.Load()
}

func (m *Manager) SetPhase(p Phase) {
	old := Phase(m.phase.Swap(int32(p)))
	logger.Infof("startup phase transition; from=%s, to=%s, elapsed=%v", old.String(), p.String(), time.Since(m.startTime))
	metrics.StartupPhase.Set(int64(p))

	switch p {
	case PhaseDiskRecovery:
		// nothing extra
	case PhaseS3Refresh:
		m.recoveryTime = time.Since(m.startTime)
	case PhaseReady:
		m.totalTime = time.Since(m.startTime)
		m.refreshTime = m.totalTime - m.recoveryTime
		m.ready.Store(true)
		metrics.Ready.Set(1)
		metrics.StartupTotalSeconds.Set(m.totalTime.Seconds())
		logger.Infof("startup complete; recovery_seconds=%v, refresh_seconds=%v, total_seconds=%v, catchup_files=%d", m.recoveryTime.Seconds(), m.refreshTime.Seconds(), m.totalTime.Seconds(), m.catchupFiles)
	}
}

func (m *Manager) SetCatchupFiles(n int64) {
	m.catchupFiles = n
}

func (m *Manager) RecoverySeconds() float64 { return m.recoveryTime.Seconds() }
func (m *Manager) RefreshSeconds() float64  { return m.refreshTime.Seconds() }
func (m *Manager) TotalSeconds() float64    { return m.totalTime.Seconds() }
func (m *Manager) CatchupFiles() int64      { return m.catchupFiles }
