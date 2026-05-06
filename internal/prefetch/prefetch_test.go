package prefetch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngine_Enqueue(t *testing.T) {
	var fetched sync.Map
	var count atomic.Int32

	e := NewEngine(2, 10, func(_ context.Context, key string) error {
		fetched.Store(key, true)
		count.Add(1)
		return nil
	})
	defer e.Close()

	e.Enqueue(Task{Key: "a", Type: TypeCorrelated})
	e.Enqueue(Task{Key: "b", Type: TypeReadAhead})
	e.Enqueue(Task{Key: "c", Type: TypeWarmup})

	deadline := time.After(5 * time.Second)
	for count.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for fetches")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	for _, key := range []string{"a", "b", "c"} {
		if _, ok := fetched.Load(key); !ok {
			t.Errorf("key %q not fetched", key)
		}
	}
}

func TestEngine_DeduplicatesKeys(t *testing.T) {
	blocker := make(chan struct{})
	var count atomic.Int32
	e := NewEngine(1, 10, func(_ context.Context, _ string) error {
		count.Add(1)
		<-blocker
		return nil
	})
	defer func() {
		close(blocker)
		e.Close()
	}()

	e.Enqueue(Task{Key: "first"})
	time.Sleep(20 * time.Millisecond)

	e.Enqueue(Task{Key: "same"})
	ok := e.Enqueue(Task{Key: "same"})
	if ok {
		t.Error("duplicate key should return false")
	}
}

func TestEngine_MaxQueue(t *testing.T) {
	blocker := make(chan struct{})
	e := NewEngine(1, 3, func(_ context.Context, _ string) error {
		<-blocker
		return nil
	})
	defer func() {
		close(blocker)
		e.Close()
	}()

	e.Enqueue(Task{Key: "blocking"})

	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 5; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i)})
	}

	if ql := e.QueueLen(); ql > 3 {
		t.Errorf("queue len = %d, want <= 3", ql)
	}
}

func TestEngine_MaxConcurrent(t *testing.T) {
	var maxActive atomic.Int32
	var current atomic.Int32

	e := NewEngine(3, 20, func(_ context.Context, _ string) error {
		c := current.Add(1)
		for {
			old := maxActive.Load()
			if c <= old || maxActive.CompareAndSwap(old, c) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		current.Add(-1)
		return nil
	})
	defer e.Close()

	for i := 0; i < 10; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i)})
	}

	time.Sleep(300 * time.Millisecond)

	if m := maxActive.Load(); m > 3 {
		t.Errorf("max concurrent = %d, want <= 3", m)
	}
}

func TestEngine_CorrelatedEnqueue(t *testing.T) {
	var fetched sync.Map
	e := NewEngine(4, 20, func(_ context.Context, key string) error {
		fetched.Store(key, true)
		return nil
	})
	defer e.Close()

	n := e.EnqueueCorrelated([]string{"a", "b", "c"})
	if n != 3 {
		t.Errorf("enqueued = %d, want 3", n)
	}

	time.Sleep(100 * time.Millisecond)

	for _, key := range []string{"a", "b", "c"} {
		if _, ok := fetched.Load(key); !ok {
			t.Errorf("key %q not fetched", key)
		}
	}
}

func TestEngine_ReadAheadEnqueue(t *testing.T) {
	var count atomic.Int32
	e := NewEngine(2, 10, func(_ context.Context, _ string) error {
		count.Add(1)
		return nil
	})
	defer e.Close()

	n := e.EnqueueReadAhead([]string{"x", "y"})
	if n != 2 {
		t.Errorf("enqueued = %d, want 2", n)
	}

	time.Sleep(100 * time.Millisecond)
	if c := count.Load(); c != 2 {
		t.Errorf("fetched = %d, want 2", c)
	}
}

func TestEngine_WarmupEnqueue(t *testing.T) {
	var count atomic.Int32
	e := NewEngine(2, 10, func(_ context.Context, _ string) error {
		count.Add(1)
		return nil
	})
	defer e.Close()

	n := e.EnqueueWarmup([]string{"w1", "w2", "w3"})
	if n != 3 {
		t.Errorf("enqueued = %d, want 3", n)
	}

	time.Sleep(100 * time.Millisecond)
	if c := count.Load(); c != 3 {
		t.Errorf("fetched = %d, want 3", c)
	}
}

func TestEngine_ErrorCounting(t *testing.T) {
	e := NewEngine(2, 10, func(_ context.Context, _ string) error {
		return fmt.Errorf("simulated error")
	})
	defer e.Close()

	e.Enqueue(Task{Key: "fail1"})
	e.Enqueue(Task{Key: "fail2"})

	time.Sleep(100 * time.Millisecond)

	triggered, completed, errors, _ := e.Stats()
	if triggered != 2 {
		t.Errorf("triggered = %d, want 2", triggered)
	}
	if completed != 0 {
		t.Errorf("completed = %d, want 0", completed)
	}
	if errors != 2 {
		t.Errorf("errors = %d, want 2", errors)
	}
}

func TestEngine_MarkUseful(t *testing.T) {
	e := NewEngine(2, 10, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	e.Enqueue(Task{Key: "useful-key"})
	e.Enqueue(Task{Key: "unused-key"})

	time.Sleep(100 * time.Millisecond)

	e.MarkUseful("useful-key")
	e.MarkUseful("never-prefetched")

	_, _, _, useful := e.Stats()
	if useful != 1 {
		t.Errorf("useful = %d, want 1", useful)
	}
}

func TestEngine_Close(t *testing.T) {
	var started atomic.Int32
	e := NewEngine(2, 10, func(ctx context.Context, _ string) error {
		started.Add(1)
		<-ctx.Done()
		return ctx.Err()
	})

	e.Enqueue(Task{Key: "long-running"})
	time.Sleep(50 * time.Millisecond)

	e.Close()

	if s := started.Load(); s != 1 {
		t.Errorf("started = %d, want 1", s)
	}
}

func TestEngine_Stats(t *testing.T) {
	e := NewEngine(4, 10, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	triggered, completed, errors, useful := e.Stats()
	if triggered != 0 || completed != 0 || errors != 0 || useful != 0 {
		t.Errorf("initial stats not zero: %d %d %d %d", triggered, completed, errors, useful)
	}

	e.Enqueue(Task{Key: "s1"})
	e.Enqueue(Task{Key: "s2"})
	time.Sleep(100 * time.Millisecond)

	triggered, completed, _, _ = e.Stats()
	if triggered != 2 {
		t.Errorf("triggered = %d, want 2", triggered)
	}
	if completed != 2 {
		t.Errorf("completed = %d, want 2", completed)
	}
}

func TestEngine_Active(t *testing.T) {
	blocker := make(chan struct{})
	e := NewEngine(2, 10, func(_ context.Context, _ string) error {
		<-blocker
		return nil
	})
	defer func() {
		close(blocker)
		e.Close()
	}()

	e.Enqueue(Task{Key: "a1"})
	e.Enqueue(Task{Key: "a2"})
	time.Sleep(50 * time.Millisecond)

	if a := e.Active(); a != 2 {
		t.Errorf("active = %d, want 2", a)
	}
}

func TestType_String(t *testing.T) {
	tests := []struct {
		typ  Type
		want string
	}{
		{TypeCorrelated, "correlated"},
		{TypeReadAhead, "read_ahead"},
		{TypeWarmup, "warmup"},
		{Type(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("Type(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

func BenchmarkEngine_Enqueue(b *testing.B) {
	e := NewEngine(4, 1000, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i), Type: TypeCorrelated})
	}
}
