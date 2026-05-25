package lifecycle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func testShutdownConfig() config.ShutdownConfig {
	return config.ShutdownConfig{
		Delay:          100 * time.Millisecond,
		MaxGraceful:    200 * time.Millisecond,
		FlushTimeout:   500 * time.Millisecond,
		PersistTimeout: 200 * time.Millisecond,
		ReleaseTimeout: 100 * time.Millisecond,
	}
}

func TestShutdownOrchestrator_HappyPath(t *testing.T) {
	var order []string
	hooks := ShutdownHooks{
		OnDrain:   func(ctx context.Context) error { order = append(order, "drain"); return nil },
		OnFlush:   func(ctx context.Context) (int64, error) { order = append(order, "flush"); return 42, nil },
		OnPersist: func(ctx context.Context) error { order = append(order, "persist"); return nil },
		OnRelease: func(ctx context.Context) error { order = append(order, "release"); return nil },
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	err := orch.Execute(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := []string{"drain", "flush", "persist", "release"}
	if len(order) != len(expected) {
		t.Fatalf("phases = %v, want %v", order, expected)
	}
	for i, phase := range expected {
		if order[i] != phase {
			t.Errorf("phase[%d] = %s, want %s", i, order[i], phase)
		}
	}
}

func TestShutdownOrchestrator_IsDraining(t *testing.T) {
	hooks := ShutdownHooks{
		OnDrain: func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	}
	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)

	if orch.IsDraining() {
		t.Error("should not be draining before Execute")
	}

	done := make(chan struct{})
	go func() {
		_ = orch.Execute(context.Background())
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	if !orch.IsDraining() {
		t.Error("should be draining during Execute")
	}
	<-done
}

func TestShutdownOrchestrator_PhaseTimeout(t *testing.T) {
	hooks := ShutdownHooks{
		OnFlush: func(ctx context.Context) (int64, error) {
			// Block longer than flush timeout
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(5 * time.Second):
				return 0, nil
			}
		},
	}

	cfg := testShutdownConfig()
	cfg.FlushTimeout = 100 * time.Millisecond

	orch := NewShutdownOrchestrator(cfg, hooks)
	err := orch.Execute(context.Background())
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestShutdownOrchestrator_ContinuesAfterError(t *testing.T) {
	var persistCalled atomic.Bool
	hooks := ShutdownHooks{
		OnFlush:   func(ctx context.Context) (int64, error) { return 0, errors.New("flush failed") },
		OnPersist: func(ctx context.Context) error { persistCalled.Store(true); return nil },
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	err := orch.Execute(context.Background())

	if err == nil {
		t.Fatal("expected error from flush phase")
	}
	if !persistCalled.Load() {
		t.Error("persist should still run after flush error")
	}
}

func TestShutdownOrchestrator_NilHooksOK(t *testing.T) {
	orch := NewShutdownOrchestrator(testShutdownConfig(), ShutdownHooks{})
	err := orch.Execute(context.Background())
	if err != nil {
		t.Fatalf("nil hooks should be fine, got: %v", err)
	}
}

func TestShutdownOrchestrator_FlushRowsCounted(t *testing.T) {
	hooks := ShutdownHooks{
		OnFlush: func(ctx context.Context) (int64, error) { return 1234, nil },
	}
	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	_ = orch.Execute(context.Background())
	// Just verify it doesn't panic — metric assertion would need metric access
}
