package startup

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// TestEmitStartupHints_HealthyClusterSilent pins the rule that a
// well-configured cluster gets zero tuning hints — operators trust
// the warn-level "hint:" lines to mean something is actually
// suboptimal, not to be noise on every healthy startup.
func TestEmitStartupHints_HealthyClusterSilent(t *testing.T) {
	out := captureLogs(t, func() {
		EmitStartupHints(HintInputs{
			ManifestFiles:     100_000,
			MinManifestFiles:  10_000,
			FooterCacheMax:    50_000, // 50% coverage — healthy
			BufferBridgePeers: 3,
			SnapshotAge:       2 * time.Minute, // well under 6× persist
			PersistInterval:   5 * time.Minute,
			S3RefreshDuration: 5 * time.Second,
			WarmupDuration:    10 * time.Second,
		})
	})
	if strings.Contains(out, "hint:") {
		t.Errorf("healthy cluster produced hint lines — should be silent:\n%s", out)
	}
}

// TestEmitStartupHints_FooterCacheLow fires when the footer cache
// holds <10% of manifest files at scale. PB-scale fragmented L0
// hot zones produce 200k+ recent files; default cache cap of 10k
// covers 5% — wide-window queries will pay an S3 footer fetch on
// every uncached file (minutes per query).
func TestEmitStartupHints_FooterCacheLow(t *testing.T) {
	out := captureLogs(t, func() {
		EmitStartupHints(HintInputs{
			ManifestFiles:     200_000,
			FooterCacheMax:    10_000, // 5% coverage
			BufferBridgePeers: 3,
			PersistInterval:   5 * time.Minute,
		})
	})
	if !strings.Contains(out, "hint:footer-cache") {
		t.Errorf("expected footer-cache hint, got:\n%s", out)
	}
	if !strings.Contains(out, "footer_max_items=") {
		t.Errorf("hint must name the config knob to change")
	}
	if !strings.Contains(out, "MB RAM") {
		t.Errorf("hint must quantify the memory cost of the recommendation")
	}
}

// TestEmitStartupHints_StaleSnapshot fires when the disk snapshot
// loaded at startup is older than 6× the persist interval — symptom
// of long downtime or a shutdown that failed to persist before
// SIGKILL.
func TestEmitStartupHints_StaleSnapshot(t *testing.T) {
	out := captureLogs(t, func() {
		EmitStartupHints(HintInputs{
			ManifestFiles:     1000,
			BufferBridgePeers: 3,
			SnapshotAge:       45 * time.Minute, // 9x persist — well past threshold
			PersistInterval:   5 * time.Minute,
		})
	})
	if !strings.Contains(out, "hint:snapshot-staleness") {
		t.Errorf("expected snapshot-staleness hint, got:\n%s", out)
	}
	if !strings.Contains(out, "persist_timeout") {
		t.Errorf("hint must point at persist_timeout as the likely lever")
	}
}

// TestEmitStartupHints_SinglePeer fires when there's only one
// insert pod visible — BufferBridge has no peer with a warm buffer
// to fall back to during this pod's restart.
func TestEmitStartupHints_SinglePeer(t *testing.T) {
	out := captureLogs(t, func() {
		EmitStartupHints(HintInputs{
			ManifestFiles:     1000,
			FooterCacheMax:    10_000, // healthy
			BufferBridgePeers: 1,
			PersistInterval:   5 * time.Minute,
		})
	})
	if !strings.Contains(out, "hint:buffer-peers") {
		t.Errorf("expected buffer-peers hint, got:\n%s", out)
	}
}

// TestEmitStartupHints_SlowWarmup fires when warmup takes longer
// than the threshold — operator needs to tune warmup config or
// shorten persist cadence so the gap is smaller.
func TestEmitStartupHints_SlowWarmup(t *testing.T) {
	out := captureLogs(t, func() {
		EmitStartupHints(HintInputs{
			ManifestFiles:     1000,
			FooterCacheMax:    10_000,
			BufferBridgePeers: 3,
			PersistInterval:   5 * time.Minute,
			S3RefreshDuration: 10 * time.Second,
			WarmupDuration:    45 * time.Second,
		})
	})
	if !strings.Contains(out, "hint:warmup-time") {
		t.Errorf("expected warmup-time hint, got:\n%s", out)
	}
}

// TestEmitStartupHints_ReadyGateOffAtScale fires when the operator
// disabled the MinManifestFiles gate but the cluster has >10k
// files — the gate would catch fresh-PVC lying scenarios.
func TestEmitStartupHints_ReadyGateOffAtScale(t *testing.T) {
	out := captureLogs(t, func() {
		EmitStartupHints(HintInputs{
			ManifestFiles:     50_000,
			MinManifestFiles:  0, // gate disabled
			FooterCacheMax:    25_000,
			BufferBridgePeers: 3,
			PersistInterval:   5 * time.Minute,
		})
	})
	if !strings.Contains(out, "hint:ready-gate") {
		t.Errorf("expected ready-gate hint, got:\n%s", out)
	}
	if !strings.Contains(out, "min_manifest_files=") {
		t.Errorf("hint must name min_manifest_files")
	}
}

// captureLogs swaps VL's logger output to a buffer for the
// duration of fn. The logger doesn't expose a public buffer
// setter, so we use VL's StdErrorLogger redirection via the
// SetOutput helper that exists for tests in the upstream tree.
func captureLogs(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	logger.SetOutputForTests(&buf)
	defer logger.ResetOutputForTest()
	fn()
	return buf.String()
}
