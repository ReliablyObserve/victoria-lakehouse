package election

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestNoopElector_Start exercises the NoopElector.Start method (previously 0% covered).
func TestNoopElector_Start(t *testing.T) {
	e := NewNoopElector()
	ctx := context.Background()
	e.Start(ctx) // should not panic

	if !e.IsLeader() {
		t.Error("NoopElector should always be leader after Start")
	}
}

// TestNoopElector_Stop exercises the NoopElector.Stop method (previously 0% covered).
func TestNoopElector_Stop(t *testing.T) {
	e := NewNoopElector()
	e.Stop() // should not panic

	if !e.IsLeader() {
		t.Error("NoopElector should always be leader after Stop")
	}
}

// TestNoopElector_FullLifecycle exercises Start then Stop in sequence.
func TestNoopElector_FullLifecycle(t *testing.T) {
	e := NewNoopElector()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)
	if !e.IsLeader() {
		t.Error("expected leader after Start")
	}

	e.Stop()
	if !e.IsLeader() {
		t.Error("expected still leader after Stop (noop never loses leadership)")
	}
}

// TestAutoElector_DelegatesStart exercises AutoElector.Start delegation.
func TestAutoElector_DelegatesStart(t *testing.T) {
	ae := NewAutoElector(AutoElectorConfig{Mode: "none"})
	ctx := context.Background()
	ae.Start(ctx) // delegates to NoopElector.Start

	if !ae.IsLeader() {
		t.Error("expected noop elector to be leader")
	}
}

// TestAutoElector_DelegatesStop exercises AutoElector.Stop delegation.
func TestAutoElector_DelegatesStop(t *testing.T) {
	ae := NewAutoElector(AutoElectorConfig{Mode: "none"})
	ae.Stop() // delegates to NoopElector.Stop

	if !ae.IsLeader() {
		t.Error("expected noop elector to still be leader after Stop")
	}
}

// failingK8sElector returns an error from NewK8sElector to exercise error branches.
func failingK8sElector(_ K8sElectorConfig) (*K8sElector, error) {
	return nil, fmt.Errorf("simulated k8s elector creation failure")
}

// TestAutoElector_K8sMode_FailureFallbackToNoop exercises the k8s mode error
// branch where NewK8sElector fails and falls back to noop.
func TestAutoElector_K8sMode_FailureFallbackToNoop(t *testing.T) {
	ae := NewAutoElector(AutoElectorConfig{
		Mode:          "k8s",
		newK8sElector: failingK8sElector,
	})
	if ae == nil {
		t.Fatal("expected non-nil AutoElector")
	}
	// Should fall back to noop (always leader).
	if !ae.IsLeader() {
		t.Error("k8s failure should fall back to noop (always leader)")
	}
}

// TestAutoElector_AutoModeK8sFailure_FallbackToS3 exercises the auto mode
// branch where K8s env is set but K8s elector fails, falling back to S3.
func TestAutoElector_AutoModeK8sFailure_FallbackToS3(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")

	store := newMockS3Store()
	ae := NewAutoElector(AutoElectorConfig{
		Mode:          "auto",
		S3Store:       store,
		newK8sElector: failingK8sElector,
		S3Config: S3ElectorConfig{
			LockKey:           "election/auto-k8s-fail.json",
			Identity:          "auto-k8s-fail-node",
			HeartbeatInterval: 100 * time.Millisecond,
			LockTTL:           1 * time.Second,
		},
	})
	if ae == nil {
		t.Fatal("expected non-nil AutoElector")
	}

	// Should have fallen back to S3 elector.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ae.Start(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if ae.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ae.IsLeader() {
		t.Fatal("expected S3 fallback to acquire leadership")
	}
	ae.Stop()
}
