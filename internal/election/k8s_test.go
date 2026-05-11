// internal/election/k8s_test.go
package election

import (
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
