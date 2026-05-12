// internal/election/leader_test.go
package election

import (
	"context"
	"testing"
)

func TestNoopElector_IsAlwaysLeader(t *testing.T) {
	e := NewNoopElector()
	if !e.IsLeader() {
		t.Fatal("NoopElector must be leader before Start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	if !e.IsLeader() {
		t.Fatal("NoopElector must be leader after Start")
	}

	e.Stop()
	if !e.IsLeader() {
		t.Fatal("NoopElector must be leader after Stop")
	}
}

func TestNoopElector_ImplementsLeader(t *testing.T) {
	var _ Leader = (*NoopElector)(nil)
}

func TestNewNoopElector_ReturnsNonNil(t *testing.T) {
	e := NewNoopElector()
	if e == nil {
		t.Fatal("NewNoopElector returned nil")
	}
}

func TestNoopElector_StartIsNoOp(t *testing.T) {
	e := NewNoopElector()
	// Start should not panic or change behavior.
	e.Start(context.Background())
	if !e.IsLeader() {
		t.Fatal("should still be leader after Start")
	}
}

func TestNoopElector_StopIsNoOp(t *testing.T) {
	e := NewNoopElector()
	// Stop should not panic or change leader status.
	e.Stop()
	if !e.IsLeader() {
		t.Fatal("should still be leader after Stop")
	}
}

func TestNoopElector_MultipleStartStop(t *testing.T) {
	e := NewNoopElector()
	for i := 0; i < 5; i++ {
		e.Start(context.Background())
		if !e.IsLeader() {
			t.Fatalf("iteration %d: not leader after Start", i)
		}
		e.Stop()
		if !e.IsLeader() {
			t.Fatalf("iteration %d: not leader after Stop", i)
		}
	}
}
