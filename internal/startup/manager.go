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
	PhaseStaleCheck   Phase = 2
	PhaseS3Refresh    Phase = 3
	PhasePeerSync     Phase = 4
	PhaseCacheWarmup  Phase = 5
	PhaseReady        Phase = 6
)

func (p Phase) String() string {
	switch p {
	case PhaseInit:
		return "init"
	case PhaseDiskRecovery:
		return "disk_recovery"
	case PhaseStaleCheck:
		return "stale_check"
	case PhaseS3Refresh:
		return "s3_refresh"
	case PhasePeerSync:
		return "peer_sync"
	case PhaseCacheWarmup:
		return "cache_warmup"
	case PhaseReady:
		return "ready"
	default:
		return "unknown"
	}
}

// Manager tracks the pod's lifecycle state and decides what `/ready` should
// report. There are two independent readiness dimensions:
//
//  1. **ServingReady** — the HTTP handler is bound and the lifecycle has
//     completed disk recovery. Queries may return partial data while
//     background warmup catches up to S3. Reported as HTTP 204 on /ready
//     when WarmupComplete is still false.
//
//  2. **WarmupComplete** — background S3 refresh + cache warmup are done.
//     Combined with ServingReady, reported as HTTP 200 on /ready. Strict
//     load-balancer setups (helm readinessProbe with successThreshold>1)
//     can gate routing on 200 specifically.
//
// On top of these, the optional MinManifestFiles gate refuses to flip
// ServingReady until the manifest holds at least that many files —
// catches the first-ever-boot scenario where a pod with no snapshot
// would otherwise lie about being ready while the manifest is empty.
type Manager struct {
	phase           atomic.Int32
	servingReady    atomic.Bool
	warmupComplete  atomic.Bool
	walReplayDone   atomic.Bool
	manifestFiles   atomic.Int64
	minReadyFiles   int64
	walReplayNeeded atomic.Bool
	startTime       time.Time
	recoveryTime    time.Duration
	refreshTime     time.Duration
	totalTime       time.Duration
	catchupFiles    int64
}

// NewManager returns a fresh lifecycle manager. minReadyFiles is the
// readiness gate threshold — /ready reports not-ready until the
// manifest holds at least this many files. Pass 0 to disable the gate
// (current behaviour, fine for dev/CI; production at PB scale should
// set this to a value larger than any single-partition empty state
// but smaller than the smallest healthy cluster's manifest).
func NewManager(minReadyFiles int64) *Manager {
	m := &Manager{
		startTime:     time.Now(),
		minReadyFiles: minReadyFiles,
	}
	m.phase.Store(int32(PhaseInit))
	metrics.MinManifestFilesGate.Set(minReadyFiles)
	return m
}

func (m *Manager) Phase() Phase {
	return Phase(m.phase.Load())
}

// IsReady is the legacy boolean that flips true on PhaseReady. Kept
// for callers that don't distinguish serving-ready from
// warmup-complete (e.g. metrics.Ready gauge). New /ready handler
// uses ServingReady + WarmupComplete instead.
func (m *Manager) IsReady() bool {
	return m.servingReady.Load() && m.WarmupComplete()
}

// ServingReady is true when the HTTP layer + disk recovery + WAL
// replay are done AND the manifest holds enough files to honestly
// answer queries. Background warmup (S3 refresh, cache warmup)
// may still be in progress.
func (m *Manager) ServingReady() bool {
	if !m.servingReady.Load() {
		return false
	}
	if m.walReplayNeeded.Load() && !m.walReplayDone.Load() {
		return false
	}
	if m.minReadyFiles > 0 && m.manifestFiles.Load() < m.minReadyFiles {
		return false
	}
	return true
}

// WarmupComplete is true once background S3 refresh + cache warmup
// + bloom backfill are done. Strict load balancers can gate routing
// on (ServingReady && WarmupComplete) instead of just ServingReady.
func (m *Manager) WarmupComplete() bool {
	return m.warmupComplete.Load()
}

// SetServingReady flips the "queries may be answered" bit. Called
// after disk recovery completes; the gate's other preconditions
// (WAL replay, MinManifestFiles) are checked lazily by ServingReady.
func (m *Manager) SetServingReady() {
	m.servingReady.Store(true)
	if m.ServingReady() {
		metrics.ServingReady.Set(1)
	}
	logger.Infof("startup: serving-ready flipped; warmup may still be in progress")
}

// SetWarmupComplete is called once the background goroutine that
// runs S3 refresh + cache warmup + bloom backfill finishes. After
// this, /ready returns 200 (was 204 while warming).
func (m *Manager) SetWarmupComplete() {
	m.warmupComplete.Store(true)
	metrics.WarmupComplete.Set(1)
	logger.Infof("startup: warmup complete; /ready will report 200")
}

// SetManifestFiles updates the file-count gauge that the MinManifestFiles
// gate consults. Called on every successful manifest load/refresh so
// /ready can flip live without restarting the pod. Also drives the
// ServingReady and ManifestFiles metrics so operators can spot the gate
// flipping live and watch the manifest-size health gauge.
func (m *Manager) SetManifestFiles(n int64) {
	m.manifestFiles.Store(n)
	metrics.ManifestFiles.Set(n)
	if m.ServingReady() {
		metrics.ServingReady.Set(1)
	} else {
		metrics.ServingReady.Set(0)
	}
}

// SetWALReplayNeeded marks this pod as one that needs WAL replay
// before serving (insert role). select-only roles never call this.
func (m *Manager) SetWALReplayNeeded() {
	m.walReplayNeeded.Store(true)
}

// SetWALReplayDone is called after the insert path finishes replaying
// the on-disk WAL. ServingReady becomes true only after this is set
// (when WALReplayNeeded was true).
func (m *Manager) SetWALReplayDone() {
	m.walReplayDone.Store(true)
	logger.Infof("startup: WAL replay complete")
}

func (m *Manager) SetPhase(p Phase) {
	old := Phase(m.phase.Swap(int32(p)))
	logger.Infof("startup phase transition; from=%s, to=%s, elapsed=%v", old.String(), p.String(), time.Since(m.startTime))
	metrics.StartupPhase.Set(int64(p))

	switch p {
	case PhaseDiskRecovery:
		// nothing extra
	case PhaseStaleCheck:
		logger.Infof("startup: entering stale check phase")
	case PhaseS3Refresh:
		m.recoveryTime = time.Since(m.startTime)
	case PhasePeerSync:
		logger.Infof("startup: entering peer sync phase")
	case PhaseCacheWarmup:
		logger.Infof("startup: entering cache warmup phase")
	case PhaseReady:
		m.totalTime = time.Since(m.startTime)
		m.refreshTime = m.totalTime - m.recoveryTime
		// Legacy: the ready gauge stays set for the old IsReady()
		// callers. New code reads ServingReady / WarmupComplete
		// directly. Both branches keep monotonic semantics — once
		// true, never goes back to false (until process restart).
		m.servingReady.Store(true)
		m.warmupComplete.Store(true)
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

// MinManifestFiles returns the configured readiness gate threshold.
// Exposed for /lakehouse/info so operators can see what their pod
// is gating on.
func (m *Manager) MinManifestFiles() int64 { return m.minReadyFiles }
