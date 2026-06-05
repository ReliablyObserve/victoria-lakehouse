package startup

import (
	"testing"
)

// TestServingReady_RequiresAllPreconditions pins the contract: the
// /ready handler returns 503 ("not ready") until disk recovery,
// (optional) WAL replay, and (optional) MinManifestFiles gate all
// pass. Any one of them being unmet keeps ServingReady false. The
// honesty layer for fresh-PVC + still-replaying restart scenarios
// hangs on this invariant.
func TestServingReady_RequiresAllPreconditions(t *testing.T) {
	cases := []struct {
		name             string
		minFiles         int64
		setServingReady  bool
		walNeeded        bool
		walDone          bool
		manifestFiles    int64
		wantServingReady bool
	}{
		{
			name:             "fresh manager — nothing set, not ready",
			wantServingReady: false,
		},
		{
			name:             "serving flipped, no WAL, no min — ready",
			setServingReady:  true,
			wantServingReady: true,
		},
		{
			name:             "serving flipped, WAL needed but not done — not ready",
			setServingReady:  true,
			walNeeded:        true,
			walDone:          false,
			wantServingReady: false,
		},
		{
			name:             "serving flipped, WAL needed AND done — ready",
			setServingReady:  true,
			walNeeded:        true,
			walDone:          true,
			wantServingReady: true,
		},
		{
			name:             "min files gate not met — not ready",
			minFiles:         100,
			setServingReady:  true,
			manifestFiles:    50,
			wantServingReady: false,
		},
		{
			name:             "min files gate exactly met — ready",
			minFiles:         100,
			setServingReady:  true,
			manifestFiles:    100,
			wantServingReady: true,
		},
		{
			name:             "min files gate exceeded — ready",
			minFiles:         100,
			setServingReady:  true,
			manifestFiles:    101,
			wantServingReady: true,
		},
		{
			name:             "all gates present and met — ready",
			minFiles:         50,
			setServingReady:  true,
			walNeeded:        true,
			walDone:          true,
			manifestFiles:    1000,
			wantServingReady: true,
		},
		{
			name:             "all gates, WAL missing — not ready",
			minFiles:         50,
			setServingReady:  true,
			walNeeded:        true,
			walDone:          false,
			manifestFiles:    1000,
			wantServingReady: false,
		},
		{
			name:             "all gates, min files missing — not ready",
			minFiles:         5000,
			setServingReady:  true,
			walNeeded:        true,
			walDone:          true,
			manifestFiles:    1000,
			wantServingReady: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := NewManager(c.minFiles)
			if c.setServingReady {
				m.SetServingReady()
			}
			if c.walNeeded {
				m.SetWALReplayNeeded()
			}
			if c.walDone {
				m.SetWALReplayDone()
			}
			if c.manifestFiles > 0 {
				m.SetManifestFiles(c.manifestFiles)
			}
			if got := m.ServingReady(); got != c.wantServingReady {
				t.Errorf("ServingReady = %v, want %v", got, c.wantServingReady)
			}
		})
	}
}

// TestWarmupComplete_OnlyTrueAfterSet pins that WarmupComplete stays
// false until SetWarmupComplete is explicitly called, even if every
// other readiness gate is satisfied. The /ready 200 vs 204 split
// depends on this.
func TestWarmupComplete_OnlyTrueAfterSet(t *testing.T) {
	m := NewManager(0)
	m.SetServingReady()
	m.SetManifestFiles(1_000_000)

	if m.WarmupComplete() {
		t.Errorf("WarmupComplete = true before SetWarmupComplete — should be false")
	}
	if m.IsReady() {
		t.Errorf("IsReady = true while WarmupComplete is false — combined gate broke")
	}

	m.SetWarmupComplete()

	if !m.WarmupComplete() {
		t.Errorf("WarmupComplete = false after SetWarmupComplete — set didn't stick")
	}
	if !m.IsReady() {
		t.Errorf("IsReady = false after both serving + warmup complete — combined gate broke")
	}
}

// TestIsReady_StaysFalseWhileServingFalse pins that the legacy
// IsReady accessor stays false while ServingReady is false. Callers
// of the legacy bool (metrics.Ready gauge, /lakehouse/info) get a
// consistent view even before the new gates flip.
func TestIsReady_StaysFalseWhileServingFalse(t *testing.T) {
	m := NewManager(0)
	m.SetWarmupComplete() // warmup done before serving — out-of-order; should still gate
	if m.IsReady() {
		t.Errorf("IsReady true when ServingReady=false — gate inverted")
	}
}

// TestSetPhase_PhaseReady_LegacyPath confirms the legacy SetPhase
// transition to PhaseReady still flips ServingReady AND
// WarmupComplete together — keeps backwards compat for code that
// uses the old single-phase model (calls SetPhase(PhaseReady) at
// the end without the new SetServingReady/SetWarmupComplete pair).
func TestSetPhase_PhaseReady_LegacyPath(t *testing.T) {
	m := NewManager(0)
	m.SetPhase(PhaseDiskRecovery)
	m.SetPhase(PhaseReady)
	if !m.ServingReady() {
		t.Errorf("SetPhase(PhaseReady) didn't flip ServingReady — legacy callers break")
	}
	if !m.WarmupComplete() {
		t.Errorf("SetPhase(PhaseReady) didn't flip WarmupComplete — legacy callers break")
	}
	if !m.IsReady() {
		t.Errorf("SetPhase(PhaseReady) didn't make IsReady true — metrics.Ready gauge stays 0")
	}
}

// TestMinManifestFiles_NoLiveDowngrade checks that once
// ServingReady is true, a subsequent drop in manifest files (e.g.
// after orphan-sweep removes entries) flips it back to false. The
// honesty layer treats readiness as a continuous condition, not a
// one-shot — operators see when a partial outage is in progress.
func TestMinManifestFiles_NoLiveDowngrade(t *testing.T) {
	m := NewManager(100)
	m.SetServingReady()
	m.SetManifestFiles(200)
	if !m.ServingReady() {
		t.Fatalf("ServingReady should be true with 200 files vs gate 100")
	}
	// Simulate orphan-sweep dropping files below threshold.
	m.SetManifestFiles(50)
	if m.ServingReady() {
		t.Errorf("ServingReady didn't track manifest drop below threshold — pod will lie about readiness")
	}
}

// TestNewManager_DefaultGateOff confirms that NewManager(0) means
// "no min files gate" — the legacy default. Production callers
// must opt into the gate by passing > 0.
func TestNewManager_DefaultGateOff(t *testing.T) {
	m := NewManager(0)
	m.SetServingReady()
	if !m.ServingReady() {
		t.Errorf("NewManager(0) with no manifest set should be ServingReady — gate=0 means disabled")
	}
	if m.MinManifestFiles() != 0 {
		t.Errorf("MinManifestFiles() = %d, want 0", m.MinManifestFiles())
	}
}

// TestSetWALReplayDone_WithoutNeeded — calling SetWALReplayDone
// without SetWALReplayNeeded first should be harmless (idempotent),
// not panic. Defensive coverage for select-only roles that may
// call the API in their init paths.
func TestSetWALReplayDone_WithoutNeeded(t *testing.T) {
	m := NewManager(0)
	m.SetWALReplayDone() // should be no-op for select-only path
	m.SetServingReady()
	if !m.ServingReady() {
		t.Errorf("SetWALReplayDone without WALReplayNeeded broke ServingReady — should be inert")
	}
}
