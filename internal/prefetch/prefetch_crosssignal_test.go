package prefetch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestTypeCrossSignal_String(t *testing.T) {
	if TypeCrossSignal.String() != "cross_signal" {
		t.Errorf("TypeCrossSignal.String() = %q, want %q", TypeCrossSignal.String(), "cross_signal")
	}
}

func TestEnqueueCrossSignal(t *testing.T) {
	var fetched atomic.Int64
	engine := NewEngine(2, 64, func(ctx context.Context, key string) error {
		fetched.Add(1)
		return nil
	})
	defer engine.Close()

	n := engine.EnqueueCrossSignal([]string{"trace-file-1", "trace-file-2"})
	if n != 2 {
		t.Errorf("enqueued = %d, want 2", n)
	}

	time.Sleep(100 * time.Millisecond)

	if fetched.Load() != 2 {
		t.Errorf("fetched = %d, want 2", fetched.Load())
	}
}

func TestPriorityDequeue_CrossSignalBeforeReadAhead(t *testing.T) {
	var order []string
	orderCh := make(chan string, 10)
	// Use a gate to hold the first task until all tasks are enqueued,
	// ensuring the priority queue has all items before the second dequeue.
	gate := make(chan struct{})

	engine := NewEngine(1, 64, func(ctx context.Context, key string) error {
		orderCh <- key
		// Block until gate is opened — this lets us enqueue all tasks before
		// the first worker finishes and picks the next item via priority.
		select {
		case <-gate:
		case <-ctx.Done():
		}
		return nil
	})
	defer engine.Close()

	engine.Enqueue(Task{Key: "readahead-1", Type: TypeReadAhead, Priority: 2})
	engine.Enqueue(Task{Key: "readahead-2", Type: TypeReadAhead, Priority: 2})
	engine.Enqueue(Task{Key: "cross-1", Type: TypeCrossSignal, Priority: 1})
	engine.Enqueue(Task{Key: "cross-2", Type: TypeCrossSignal, Priority: 1})

	// Collect first key (whichever started first due to the race)
	timeout := time.After(2 * time.Second)
	select {
	case key := <-orderCh:
		order = append(order, key)
	case <-timeout:
		t.Fatal("timeout waiting for first task")
	}

	// Now open the gate — the worker finishes and picks the next item by priority
	close(gate)

	// Collect remaining 3 keys
	for i := 1; i < 4; i++ {
		select {
		case key := <-orderCh:
			order = append(order, key)
		case <-timeout:
			t.Fatalf("timeout waiting for task %d, got %v", i, order)
		}
	}

	// The 2nd and 3rd items in order should both be cross-signal (priority 1 beats priority 2)
	crossCount := 0
	for i := 1; i < 3; i++ {
		if order[i] == "cross-1" || order[i] == "cross-2" {
			crossCount++
		}
	}
	if crossCount < 2 {
		t.Errorf("expected cross-signal tasks to be prioritized after first dequeue, order was: %v", order)
	}
}

func TestEnqueueCrossSignal_ClosedEngine(t *testing.T) {
	engine := NewEngine(2, 64, func(ctx context.Context, key string) error {
		return nil
	})
	engine.Close()

	n := engine.EnqueueCrossSignal([]string{"key1", "key2"})
	if n != 0 {
		t.Errorf("expected 0 enqueued on closed engine, got %d", n)
	}
}

func TestEnqueueCrossSignal_DuplicateKeys(t *testing.T) {
	// Use maxConcurrent=1 and a gate to hold the worker so keys remain in-queue
	// when the second batch is submitted, exercising the dedup path.
	gate := make(chan struct{})
	var fetched atomic.Int64
	engine := NewEngine(1, 64, func(ctx context.Context, key string) error {
		fetched.Add(1)
		select {
		case <-gate:
		case <-ctx.Done():
		}
		return nil
	})
	defer engine.Close()

	// First batch: enqueue key1 and key2; key1 starts processing immediately,
	// key2 stays in the queue.
	n1 := engine.EnqueueCrossSignal([]string{"key1", "key2"})
	if n1 != 2 {
		t.Errorf("first batch: enqueued = %d, want 2", n1)
	}

	// Second batch: key1 is being processed (not in queue), key2 is still in queue.
	// key2 should be deduplicated (already queued); key1 would be re-enqueued if
	// dedup only checks the queue (not in-flight).
	n2 := engine.EnqueueCrossSignal([]string{"key1", "key2"})
	// key2 should be 0 (duplicate in queue); key1 may or may not be re-enqueued
	// depending on timing, but total should not exceed 4.
	_ = n2

	// Open gate — let workers proceed
	close(gate)

	time.Sleep(300 * time.Millisecond)

	total := fetched.Load()
	// key2 dedup must hold: at most key1 (1 or 2 times) + key2 (1 time) = 3
	if total > 3 {
		t.Errorf("expected at most 3 fetches (key2 deduped), got %d", total)
	}
}

func TestTypeConstants_Ordering(t *testing.T) {
	// TypeCrossSignal should be first (lowest value)
	if TypeCrossSignal >= TypeCorrelated {
		t.Errorf("TypeCrossSignal (%d) should be < TypeCorrelated (%d)", TypeCrossSignal, TypeCorrelated)
	}
	if TypeCorrelated >= TypeReadAhead {
		t.Errorf("TypeCorrelated (%d) should be < TypeReadAhead (%d)", TypeCorrelated, TypeReadAhead)
	}
	if TypeReadAhead >= TypeWarmup {
		t.Errorf("TypeReadAhead (%d) should be < TypeWarmup (%d)", TypeReadAhead, TypeWarmup)
	}
}
