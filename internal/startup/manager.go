package startup

import (
	"log/slog"
	"sync/atomic"
	"time"

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
	phase          atomic.Int32
	ready          atomic.Bool
	startTime      time.Time
	recoveryTime   time.Duration
	refreshTime    time.Duration
	totalTime      time.Duration
	catchupFiles   int64
	logger         *slog.Logger
}

func NewManager(logger *slog.Logger) *Manager {
	m := &Manager{
		startTime: time.Now(),
		logger:    logger.With("component", "startup"),
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
	m.logger.Info("startup phase transition", "from", old.String(), "to", p.String(), "elapsed", time.Since(m.startTime))
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
		m.logger.Info("startup complete",
			"recovery_seconds", m.recoveryTime.Seconds(),
			"refresh_seconds", m.refreshTime.Seconds(),
			"total_seconds", m.totalTime.Seconds(),
			"catchup_files", m.catchupFiles,
		)
	}
}

func (m *Manager) SetCatchupFiles(n int64) {
	m.catchupFiles = n
}

func (m *Manager) Logger() *slog.Logger     { return m.logger }
func (m *Manager) RecoverySeconds() float64 { return m.recoveryTime.Seconds() }
func (m *Manager) RefreshSeconds() float64  { return m.refreshTime.Seconds() }
func (m *Manager) TotalSeconds() float64    { return m.totalTime.Seconds() }
func (m *Manager) CatchupFiles() int64      { return m.catchupFiles }
