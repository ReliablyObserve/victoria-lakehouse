package cache

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroup_Do(t *testing.T) {
	g := NewGroup()

	val, err, shared := g.Do("k1", func() ([]byte, error) {
		return []byte("result"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if shared {
		t.Error("first call should not be shared")
	}
	if string(val) != "result" {
		t.Errorf("val = %q, want %q", val, "result")
	}
}

func TestGroup_DoError(t *testing.T) {
	g := NewGroup()

	_, err, _ := g.Do("k1", func() ([]byte, error) {
		return nil, errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGroup_DoCoalesces(t *testing.T) {
	g := NewGroup()

	var calls atomic.Int32
	block := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err, _ := g.Do("k1", func() ([]byte, error) {
				calls.Add(1)
				<-block
				return []byte("result"), nil
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if string(val) != "result" {
				t.Errorf("val = %q, want %q", val, "result")
			}
		}()
	}

	for g.Inflight() < 1 {
		runtime.Gosched()
	}
	time.Sleep(50 * time.Millisecond)

	close(block)
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("fn called %d times, want 1 (should coalesce)", calls.Load())
	}
}

func TestGroup_DoDifferentKeys(t *testing.T) {
	g := NewGroup()

	var calls atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			_, err, _ := g.Do(k, func() ([]byte, error) {
				calls.Add(1)
				return []byte(k), nil
			})
			if err != nil {
				t.Errorf("unexpected error for key %s: %v", k, err)
			}
		}(key)
	}
	wg.Wait()

	if calls.Load() != 5 {
		t.Errorf("fn called %d times, want 5 (different keys)", calls.Load())
	}
}

func TestGroup_DoRemovesAfterDone(t *testing.T) {
	g := NewGroup()

	_, _, _ = g.Do("k1", func() ([]byte, error) {
		return []byte("first"), nil
	})

	val, _, shared := g.Do("k1", func() ([]byte, error) {
		return []byte("second"), nil
	})

	if shared {
		t.Error("second call should not be shared (first completed)")
	}
	if string(val) != "second" {
		t.Errorf("val = %q, want %q", val, "second")
	}
}

func TestGroup_Inflight(t *testing.T) {
	g := NewGroup()

	if g.Inflight() != 0 {
		t.Errorf("inflight = %d, want 0", g.Inflight())
	}

	block := make(chan struct{})
	go func() {
		_, _, _ = g.Do("k1", func() ([]byte, error) {
			<-block
			return nil, nil
		})
	}()

	for g.Inflight() < 1 {
	}
	if g.Inflight() != 1 {
		t.Errorf("inflight = %d, want 1", g.Inflight())
	}

	close(block)
	for g.Inflight() > 0 {
	}
}

// A caller that joins a call in flight waits no longer than its own context.
func TestGroup_DoCtx_WaiterStopsAtItsOwnContext(t *testing.T) {
	g := NewGroup()
	release := make(chan struct{})
	started := make(chan struct{})
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, _ = g.DoCtx(context.Background(), "k", func(context.Context) ([]byte, error) {
			close(started)
			<-release
			return []byte("v"), nil
		})
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	type result struct {
		err    error
		shared bool
	}
	waiter := make(chan result, 1)
	go func() {
		_, err, shared := g.DoCtx(ctx, "k", func(context.Context) ([]byte, error) { t.Error("the waiter ran the call"); return nil, nil })
		waiter <- result{err, shared}
	}()
	select {
	case r := <-waiter:
		if !errors.Is(r.err, context.DeadlineExceeded) || !r.shared {
			t.Fatalf("waiter: err %v shared %v, want its own deadline", r.err, r.shared)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("a caller that joined a call in flight waited past its own deadline")
	}
	close(release)
	select {
	case <-leaderDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the leader did not finish")
	}
}

// The first caller's deadline is not a failure of the work: a caller that joined its
// call, and whose own context is live, runs the call on its own context.
func TestGroup_DoCtx_LeaderContextErrorIsNotInherited(t *testing.T) {
	g := NewGroup()
	started := make(chan struct{})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err, _ := g.DoCtx(leaderCtx, "k", func(ctx context.Context) ([]byte, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		leaderErr <- err
	}()
	<-started

	type result struct {
		val []byte
		err error
	}
	res := make(chan result, 1)
	go func() {
		v, err, _ := g.DoCtx(context.Background(), "k", func(context.Context) ([]byte, error) { return []byte("mine"), nil })
		res <- result{v, err}
	}()
	time.Sleep(20 * time.Millisecond) // let the second caller join
	cancelLeader()
	select {
	case r := <-res:
		if r.err != nil || string(r.val) != "mine" {
			t.Fatalf("second caller: %q, %v: want its own read", r.val, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second caller did not finish")
	}
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v, want its own cancellation", err)
	}
	if g.Inflight() != 0 {
		t.Fatalf("%d calls left in flight", g.Inflight())
	}
}

// An error that is not a context error is the work's, and is shared; the call
// runs once.
func TestGroup_DoCtx_OtherErrorsAreShared(t *testing.T) {
	g := NewGroup()
	var runs atomic.Int32
	boom := errors.New("boom")
	started := make(chan struct{})
	release := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err, _ := g.DoCtx(context.Background(), "k", func(context.Context) ([]byte, error) {
			runs.Add(1)
			close(started)
			<-release
			return nil, boom
		})
		first <- err
	}()
	<-started
	second := make(chan error, 1)
	go func() {
		_, err, _ := g.DoCtx(context.Background(), "k", func(context.Context) ([]byte, error) { runs.Add(1); return []byte("x"), nil })
		second <- err
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	if err := <-first; !errors.Is(err, boom) {
		t.Fatalf("first: %v", err)
	}
	if err := <-second; !errors.Is(err, boom) {
		t.Fatalf("second: %v, want the shared error", err)
	}
	if runs.Load() != 1 {
		t.Fatalf("the call ran %d times, want 1", runs.Load())
	}
}
