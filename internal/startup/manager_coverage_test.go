package startup

import (
	"testing"
	"time"
)

func TestManager_RefreshSeconds(t *testing.T) {
	m := NewManager(0)

	// Before any phase transitions, RefreshSeconds should be 0.
	if m.RefreshSeconds() != 0 {
		t.Errorf("initial RefreshSeconds = %f, want 0", m.RefreshSeconds())
	}

	m.SetPhase(PhaseDiskRecovery)
	time.Sleep(10 * time.Millisecond)
	m.SetPhase(PhaseS3Refresh)
	time.Sleep(10 * time.Millisecond)
	m.SetPhase(PhaseReady)

	// After ready, RefreshSeconds should be positive (time from S3Refresh to Ready).
	if m.RefreshSeconds() <= 0 {
		t.Errorf("RefreshSeconds after ready = %f, want > 0", m.RefreshSeconds())
	}
}
