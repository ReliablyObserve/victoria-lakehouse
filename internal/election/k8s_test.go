// internal/election/k8s_test.go
package election

import (
	"context"
	"testing"
	"time"
)

func TestK8sElectorConfig_Defaults(t *testing.T) {
	cfg := K8sElectorConfig{LeaseName: "test", LeaseNamespace: "default", Identity: "pod-0"}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseDuration != 15*time.Second {
		t.Fatalf("expected 15s LeaseDuration, got %v", e.cfg.LeaseDuration)
	}
	if e.cfg.RenewDeadline != 10*time.Second {
		t.Fatalf("expected 10s RenewDeadline, got %v", e.cfg.RenewDeadline)
	}
	if e.cfg.RetryPeriod != 2*time.Second {
		t.Fatalf("expected 2s RetryPeriod, got %v", e.cfg.RetryPeriod)
	}
}

func TestK8sElector_ImplementsLeader(t *testing.T) {
	var _ Leader = (*K8sElector)(nil)
}

func TestK8sElector_NotLeaderBeforeStart(t *testing.T) {
	e := &K8sElector{}
	if e.IsLeader() {
		t.Fatal("should not be leader before Start")
	}
}

func TestK8sElector_CustomDurations(t *testing.T) {
	cfg := K8sElectorConfig{
		LeaseName:      "custom-lease",
		LeaseNamespace: "my-namespace",
		Identity:       "my-pod",
		LeaseDuration:  30 * time.Second,
		RenewDeadline:  20 * time.Second,
		RetryPeriod:    5 * time.Second,
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseDuration != 30*time.Second {
		t.Errorf("LeaseDuration = %v, want 30s", e.cfg.LeaseDuration)
	}
	if e.cfg.RenewDeadline != 20*time.Second {
		t.Errorf("RenewDeadline = %v, want 20s", e.cfg.RenewDeadline)
	}
	if e.cfg.RetryPeriod != 5*time.Second {
		t.Errorf("RetryPeriod = %v, want 5s", e.cfg.RetryPeriod)
	}
	if e.cfg.LeaseNamespace != "my-namespace" {
		t.Errorf("LeaseNamespace = %q, want my-namespace", e.cfg.LeaseNamespace)
	}
}

func TestK8sElector_DefaultNamespaceFromEnv(t *testing.T) {
	// With empty namespace and no POD_NAMESPACE env, defaults to "default".
	cfg := K8sElectorConfig{
		LeaseName: "test-lease",
		Identity:  "pod-x",
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// When POD_NAMESPACE is not set, it should fall back to "default".
	if e.cfg.LeaseNamespace != "default" {
		t.Errorf("LeaseNamespace = %q, want default", e.cfg.LeaseNamespace)
	}
}

func TestK8sElector_StopBeforeStart(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{
		LeaseName: "test",
		Identity:  "pod-0",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Stop before Start should not panic.
	e.Stop()
	if e.IsLeader() {
		t.Error("should not be leader after Stop without Start")
	}
}

func TestK8sElector_StopClearsLeaderFlag(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{
		LeaseName: "test",
		Identity:  "pod-0",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate leadership being set (e.g., by the callback).
	e.leader.Store(true)
	if !e.IsLeader() {
		t.Fatal("expected leader after manual set")
	}
	e.Stop()
	if e.IsLeader() {
		t.Error("Stop should clear leader flag")
	}
}

func TestK8sElector_ZeroDurationsGetDefaults(t *testing.T) {
	cfg := K8sElectorConfig{
		LeaseName: "zero-test",
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseDuration != 15*time.Second {
		t.Errorf("LeaseDuration = %v, want 15s", e.cfg.LeaseDuration)
	}
	if e.cfg.RenewDeadline != 10*time.Second {
		t.Errorf("RenewDeadline = %v, want 10s", e.cfg.RenewDeadline)
	}
	if e.cfg.RetryPeriod != 2*time.Second {
		t.Errorf("RetryPeriod = %v, want 2s", e.cfg.RetryPeriod)
	}
	if e.cfg.LeaseNamespace != "default" {
		t.Errorf("LeaseNamespace = %q, want default", e.cfg.LeaseNamespace)
	}
	// Identity should be set from hostname.
	if e.cfg.Identity == "" {
		t.Error("Identity should be set from hostname, not empty")
	}
}

func TestK8sElector_IsLeaderFalseByDefault(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{
		LeaseName: "test",
		Identity:  "pod-0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if e.IsLeader() {
		t.Fatal("should not be leader by default")
	}
}

func TestK8sElector_StartThenStop(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{
		LeaseName: "lifecycle-test",
		Identity:  "pod-0",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Start will launch a goroutine that calls run(), which will fail
	// to get in-cluster config and return immediately. This tests
	// the cancel path.
	ctx := context.Background()
	e.Start(ctx)

	// Give the goroutine a moment to start and fail.
	time.Sleep(50 * time.Millisecond)

	// Stop should cancel context and clear leader flag.
	e.Stop()
	if e.IsLeader() {
		t.Error("should not be leader after Stop")
	}
}

func TestK8sElector_StopIdempotent(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{
		LeaseName: "idempotent-test",
		Identity:  "pod-0",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Stop multiple times without Start should not panic.
	e.Stop()
	e.Stop()
	e.Stop()
	if e.IsLeader() {
		t.Error("should not be leader")
	}
}

func TestK8sElector_EmptyNamespaceNoPodEnv(t *testing.T) {
	// Ensure POD_NAMESPACE is not set (it shouldn't be in test env).
	cfg := K8sElectorConfig{
		LeaseName: "ns-test",
		Identity:  "pod-x",
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseNamespace != "default" {
		t.Errorf("expected namespace 'default', got %q", e.cfg.LeaseNamespace)
	}
}

func TestK8sElector_EmptyIdentityGetsHostname(t *testing.T) {
	cfg := K8sElectorConfig{
		LeaseName: "identity-test",
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// On any machine, hostname should be non-empty.
	if e.cfg.Identity == "" {
		t.Error("expected identity to be set from hostname")
	}
}
