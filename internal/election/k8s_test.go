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
