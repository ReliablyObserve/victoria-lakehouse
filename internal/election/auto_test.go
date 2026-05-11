// internal/election/auto_test.go
package election

import (
	"context"
	"testing"
	"time"
)

func TestAutoElector_FallsBackToS3(t *testing.T) {
	store := newMockS3Store()
	e := NewAutoElector(AutoElectorConfig{
		Mode:    "s3",
		S3Store: store,
		S3Config: S3ElectorConfig{
			LockKey:           "test/_lock.json",
			Identity:          "pod-0",
			Address:           "10.0.0.1:9428",
			HeartbeatInterval: 100 * time.Millisecond,
			LockTTL:           1 * time.Second,
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	if !e.IsLeader() {
		t.Fatal("expected S3 fallback to acquire leadership")
	}
	e.Stop()
}

func TestAutoElector_NoneMode(t *testing.T) {
	e := NewAutoElector(AutoElectorConfig{Mode: "none"})
	if !e.IsLeader() {
		t.Fatal("none mode must always be leader")
	}
}

func TestAutoElector_ImplementsLeader(t *testing.T) {
	var _ Leader = (*AutoElector)(nil)
}

func TestAutoElector_EmptyMode(t *testing.T) {
	e := NewAutoElector(AutoElectorConfig{Mode: ""})
	if !e.IsLeader() {
		t.Fatal("empty mode must behave as noop (always leader)")
	}
}

func TestAutoElector_UnknownMode(t *testing.T) {
	e := NewAutoElector(AutoElectorConfig{Mode: "invalid-mode"})
	if !e.IsLeader() {
		t.Fatal("unknown mode must fall back to noop (always leader)")
	}
}

func TestAutoElector_AutoModeWithoutK8s_S3Store(t *testing.T) {
	// When KUBERNETES_SERVICE_HOST is empty and S3Store is provided,
	// auto mode should select S3 elector.
	store := newMockS3Store()
	e := NewAutoElector(AutoElectorConfig{
		Mode:    "auto",
		S3Store: store,
		S3Config: S3ElectorConfig{
			LockKey:           "test/_lock.json",
			Identity:          "auto-s3",
			Address:           "10.0.0.1:9428",
			HeartbeatInterval: 100 * time.Millisecond,
			LockTTL:           1 * time.Second,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	if !e.IsLeader() {
		t.Fatal("auto mode with S3 store should acquire leadership")
	}
	e.Stop()
}

func TestAutoElector_AutoModeWithoutK8s_NoS3Store(t *testing.T) {
	// When KUBERNETES_SERVICE_HOST is empty and no S3Store is provided,
	// auto mode should fall back to noop.
	e := NewAutoElector(AutoElectorConfig{
		Mode:    "auto",
		S3Store: nil,
	})

	if !e.IsLeader() {
		t.Fatal("auto mode without K8s or S3 must fall back to noop (always leader)")
	}
}

func TestAutoElector_K8sMode_WrapsK8sElector(t *testing.T) {
	// Outside a K8s cluster, NewK8sElector succeeds (it only validates config),
	// but the inner K8sElector never becomes leader because run() fails
	// to get in-cluster config. It is NOT a noop, so IsLeader is false.
	e := NewAutoElector(AutoElectorConfig{
		Mode: "k8s",
		K8sConfig: K8sElectorConfig{
			LeaseName: "test-lease",
		},
	})

	// The K8sElector hasn't started, so it should not be leader.
	if e.IsLeader() {
		t.Fatal("k8s mode elector should not be leader before Start")
	}
}

func TestAutoElector_StartStop_S3(t *testing.T) {
	store := newMockS3Store()
	e := NewAutoElector(AutoElectorConfig{
		Mode:    "s3",
		S3Store: store,
		S3Config: S3ElectorConfig{
			LockKey:           "test/_lock.json",
			Identity:          "pod-1",
			HeartbeatInterval: 50 * time.Millisecond,
			LockTTL:           1 * time.Second,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	// Wait for leadership.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("S3 auto elector should acquire leadership")
	}

	e.Stop()

	if e.IsLeader() {
		t.Error("should not be leader after Stop")
	}
}
