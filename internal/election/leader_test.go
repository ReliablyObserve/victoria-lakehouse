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
