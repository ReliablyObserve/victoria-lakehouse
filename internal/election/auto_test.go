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
