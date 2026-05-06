package startup

import (
	"testing"
	"time"
)

func TestPhaseString(t *testing.T) {
	tests := []struct {
		phase Phase
		want  string
	}{
		{PhaseInit, "init"},
		{PhaseDiskRecovery, "disk_recovery"},
		{PhaseS3Refresh, "s3_refresh"},
		{PhaseReady, "ready"},
		{Phase(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.phase.String(); got != tt.want {
			t.Errorf("Phase(%d).String() = %q, want %q", tt.phase, got, tt.want)
		}
	}
}

func TestManager_Lifecycle(t *testing.T) {
	m := NewManager()

	if m.Phase() != PhaseInit {
		t.Errorf("initial phase = %v, want init", m.Phase())
	}
	if m.IsReady() {
		t.Error("should not be ready at init")
	}

	m.SetPhase(PhaseDiskRecovery)
	if m.Phase() != PhaseDiskRecovery {
		t.Errorf("phase = %v, want disk_recovery", m.Phase())
	}
	if m.IsReady() {
		t.Error("should not be ready during disk recovery")
	}

	time.Sleep(10 * time.Millisecond)

	m.SetPhase(PhaseS3Refresh)
	if m.Phase() != PhaseS3Refresh {
		t.Errorf("phase = %v, want s3_refresh", m.Phase())
	}
	if m.RecoverySeconds() == 0 {
		t.Error("recovery seconds should be > 0 after Phase 2")
	}

	m.SetCatchupFiles(42)

	m.SetPhase(PhaseReady)
	if m.Phase() != PhaseReady {
		t.Errorf("phase = %v, want ready", m.Phase())
	}
	if !m.IsReady() {
		t.Error("should be ready after Phase 3")
	}
	if m.TotalSeconds() == 0 {
		t.Error("total seconds should be > 0")
	}
	if m.CatchupFiles() != 42 {
		t.Errorf("catchup files = %d, want 42", m.CatchupFiles())
	}
}
