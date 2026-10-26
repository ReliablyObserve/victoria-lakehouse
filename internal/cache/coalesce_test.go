package cache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroup_Do(t *testing.T) {
	g := NewGroup()

	val, err, shared := g.DoCtx(context.Background(), "k1", func(context.Context) ([]byte, error) {
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

	_, err, _ := g.DoCtx(context.Background(), "k1", func(context.Context) ([]byte, error) {
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
			val, err, _ := g.DoCtx(context.Background(), "k1", func(context.Context) ([]byte, error) {
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
			_, err, _ := g.DoCtx(context.Background(), k, func(context.Context) ([]byte, error) {
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

	_, _, _ = g.DoCtx(context.Background(), "k1", func(context.Context) ([]byte, error) {
		return []byte("first"), nil
	})

	val, _, shared := g.DoCtx(context.Background(), "k1", func(context.Context) ([]byte, error) {
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
		_, _, _ = g.DoCtx(context.Background(), "k1", func(context.Context) ([]byte, error) {
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

// waitForWaiters blocks until n callers have joined the call in flight for key, so a
// test acts only once the join it is about has happened.
func waitForWaiters(t *testing.T, g *Group, key string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for g.Waiters(key) < n {
		if time.Now().After(deadline) {
			t.Fatalf("%d callers joined the call, want %d", g.Waiters(key), n)
		}
		time.Sleep(time.Millisecond)
	}
}

type doResult struct {
	val    []byte
	err    error
	shared bool
}

func doAsync(g *Group, ctx context.Context, key string, fn func(context.Context) ([]byte, error)) <-chan doResult {
	ch := make(chan doResult, 1)
	go func() {
		v, err, shared := g.DoCtx(ctx, key, fn)
		ch <- doResult{v, err, shared}
	}()
	return ch
}

func receive(t *testing.T, ch <-chan doResult, what string) doResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
		return doResult{}
	}
}

// A caller that joins a call in flight waits no longer than its own context.
func TestGroup_DoCtx_WaiterStopsAtItsOwnContext(t *testing.T) {
	g := NewGroup()
	release := make(chan struct{})
	started := make(chan struct{})
	leader := doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) {
		close(started)
		<-release
		return []byte("v"), nil
	})
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	waiter := doAsync(g, ctx, "k", func(context.Context) ([]byte, error) { t.Error("the waiter ran the call"); return nil, nil })
	r := receive(t, waiter, "a caller that joined a call in flight, past its own deadline")
	if !errors.Is(r.err, context.DeadlineExceeded) || !r.shared {
		close(release)
		t.Fatalf("waiter: err %v shared %v, want its own deadline", r.err, r.shared)
	}
	if g.Waiters("k") != 0 {
		t.Fatalf("%d waiters left on the call after the waiter returned", g.Waiters("k"))
	}
	close(release)
	if lr := receive(t, leader, "the leader"); lr.err != nil || string(lr.val) != "v" || lr.shared {
		t.Fatalf("leader: %q, %v, shared %v", lr.val, lr.err, lr.shared)
	}
}

// The first caller's deadline or cancellation is not a failure of the work: a caller
// that joined its call, and whose own context is live, runs the call on its own
// context. Joined first (waited for, not slept for), so it is a retry and not a
// fresh lead, and the call runs once for it.
func TestGroup_DoCtx_LeaderContextErrorIsNotInherited(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  func(cancel context.CancelFunc) // how the leader's context ends
		ctx  func() (context.Context, context.CancelFunc)
		want error
	}{
		{"canceled", func(cancel context.CancelFunc) { cancel() }, func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }, context.Canceled},
		{"deadline", func(context.CancelFunc) {}, func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 200*time.Millisecond)
		}, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewGroup()
			started := make(chan struct{})
			leaderCtx, cancelLeader := tc.ctx()
			defer cancelLeader()
			leader := doAsync(g, leaderCtx, "k", func(ctx context.Context) ([]byte, error) {
				close(started)
				<-ctx.Done()
				return nil, fmt.Errorf("read s3 body: %w", ctx.Err()) // as the SDK wraps it
			})
			<-started

			var runs atomic.Int32
			joiner := doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) { runs.Add(1); return []byte("mine"), nil })
			waitForWaiters(t, g, "k", 1)
			tc.end(cancelLeader)

			r := receive(t, joiner, "the joined caller")
			if r.err != nil || string(r.val) != "mine" || runs.Load() != 1 {
				t.Fatalf("joined caller: %q, %v, ran %d times: want its own single read", r.val, r.err, runs.Load())
			}
			if r.shared {
				t.Fatal("the joined caller ran the call itself, it is not a sharer of the first caller's result")
			}
			if lr := receive(t, leader, "the leader"); !errors.Is(lr.err, tc.want) || lr.shared {
				t.Fatalf("leader: %v shared %v, want its own %v", lr.err, lr.shared, tc.want)
			}
			if g.Inflight() != 0 || g.Waiters("k") != 0 {
				t.Fatalf("%d calls in flight, %d waiters left", g.Inflight(), g.Waiters("k"))
			}
		})
	}
}

// A caller whose own context is done too does not start a doomed call, and gets its
// own context's error, not the first caller's.
func TestGroup_DoCtx_BothContextsDone_ReturnsOwnErrorAndRunsNothing(t *testing.T) {
	g := NewGroup()
	started := make(chan struct{})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	var runs atomic.Int32
	leader := doAsync(g, leaderCtx, "k", func(ctx context.Context) ([]byte, error) {
		runs.Add(1)
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	<-started
	joinerCtx, cancelJoiner := context.WithTimeout(context.Background(), time.Hour)
	joiner := doAsync(g, joinerCtx, "k", func(context.Context) ([]byte, error) { runs.Add(1); return []byte("doomed"), nil })
	waitForWaiters(t, g, "k", 1)
	// Both end together: the joiner's own context first, so that it is done when the
	// leader's error reaches it.
	cancelJoiner()
	cancelLeader()
	r := receive(t, joiner, "the joined caller")
	if !errors.Is(r.err, context.Canceled) || r.val != nil {
		t.Fatalf("joined caller: %q, %v", r.val, r.err)
	}
	receive(t, leader, "the leader")
	if runs.Load() != 1 {
		t.Fatalf("the call ran %d times, want 1 (the leader's)", runs.Load())
	}
}

// lateCtx is a context that becomes done without its Done channel ever closing, so a
// joiner holding it can only wake through the call it joined, and finds its own
// context done after that.
type lateCtx struct {
	context.Context
	dead  atomic.Bool
	never chan struct{}
}

func (c *lateCtx) Done() <-chan struct{} { return c.never }
func (c *lateCtx) Err() error {
	if c.dead.Load() {
		return context.DeadlineExceeded
	}
	return nil
}

// The joiner's own context is done by the time the call it joined reports the first
// caller's context error: it returns ITS error (not the first caller's), and does not
// run a call on a dead context.
func TestGroup_DoCtx_BothContextsDone_JoinerWakesThroughTheCall(t *testing.T) {
	g := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	joinerCtx := &lateCtx{Context: context.Background(), never: make(chan struct{})}
	leader := doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) {
		runs.Add(1)
		close(started)
		<-release
		joinerCtx.dead.Store(true)
		return nil, context.Canceled // the first caller's own cancellation
	})
	<-started
	joiner := doAsync(g, joinerCtx, "k", func(context.Context) ([]byte, error) { runs.Add(1); return []byte("doomed"), nil })
	waitForWaiters(t, g, "k", 1)
	close(release)
	r := receive(t, joiner, "the joined caller")
	if !errors.Is(r.err, context.DeadlineExceeded) || r.val != nil {
		t.Fatalf("joined caller: %q, %v: want its own deadline, not the first caller's cancellation", r.val, r.err)
	}
	receive(t, leader, "the leader")
	if runs.Load() != 1 {
		t.Fatalf("the call ran %d times, want 1 (the first caller's)", runs.Load())
	}
}

// An error that is not a context error is the work's, and is shared; the call runs once.
func TestGroup_DoCtx_OtherErrorsAreShared(t *testing.T) {
	g := NewGroup()
	var runs atomic.Int32
	boom := errors.New("boom")
	started := make(chan struct{})
	release := make(chan struct{})
	first := doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) {
		runs.Add(1)
		close(started)
		<-release
		return nil, boom
	})
	<-started
	second := doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) { runs.Add(1); return []byte("x"), nil })
	waitForWaiters(t, g, "k", 1)
	close(release)
	if r := receive(t, first, "the leader"); !errors.Is(r.err, boom) || r.shared {
		t.Fatalf("first: %v shared %v", r.err, r.shared)
	}
	if r := receive(t, second, "the joined caller"); !errors.Is(r.err, boom) || !r.shared {
		t.Fatalf("second: %v shared %v, want the shared error", r.err, r.shared)
	}
	if runs.Load() != 1 {
		t.Fatalf("the call ran %d times, want 1", runs.Load())
	}
}

// shared marks the callers that joined, never the one that ran the call: it feeds the
// dedup counter, which must count joiners only.
func TestGroup_DoCtx_SharedMarksJoinersOnly(t *testing.T) {
	g := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	leader := doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) {
		close(started)
		<-release
		return []byte("v"), nil
	})
	<-started
	const joiners = 3
	var chans []<-chan doResult
	for i := 0; i < joiners; i++ {
		chans = append(chans, doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) { t.Error("a joiner ran the call"); return nil, nil }))
	}
	waitForWaiters(t, g, "k", joiners)
	close(release)
	shared := 0
	for _, ch := range chans {
		r := receive(t, ch, "a joiner")
		if r.err != nil || string(r.val) != "v" {
			t.Fatalf("joiner: %q, %v", r.val, r.err)
		}
		if r.shared {
			shared++
		}
	}
	lr := receive(t, leader, "the leader")
	if lr.shared {
		shared++
	}
	if shared != joiners {
		t.Fatalf("%d callers reported shared, want the %d joiners only", shared, joiners)
	}
}

// The call leaves the group before its waiters are woken: a waiter that sees the
// call finished never finds it still registered (it would run it again against the
// finished call, or spin). Deterministic: with the group's lock held by the test, the
// finishing call must not wake anyone (it has to take the lock to leave first).
func TestGroup_DoCtx_CallLeavesTheGroupBeforeItsWaitersWake(t *testing.T) {
	g := NewGroup()
	release := make(chan struct{})
	started := make(chan struct{})
	leader := doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) {
		close(started)
		<-release
		return nil, nil
	})
	<-started
	g.mu.Lock()
	c := g.calls["k"]
	close(release) // the call ends and starts to leave the group, which the held lock blocks
	woken := false
	select {
	case <-c.done:
		woken = true
	case <-time.After(100 * time.Millisecond):
	}
	_, registered := g.calls["k"]
	g.mu.Unlock()
	if woken {
		t.Fatalf("the waiters were woken while the call was still registered=%v: it must leave the group first", registered)
	}
	receive(t, leader, "the leader")
	if g.Inflight() != 0 {
		t.Fatalf("%d call(s) left in flight", g.Inflight())
	}
}

// A call that panics does not stay in flight, and a caller that joined it gets an
// error instead of an empty success.
func TestGroup_DoCtx_PanicLeavesNothingInFlight(t *testing.T) {
	g := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_, _, _ = g.DoCtx(context.Background(), "k", func(context.Context) ([]byte, error) {
			close(started)
			<-release
			panic("boom")
		})
	}()
	<-started
	joiner := doAsync(g, context.Background(), "k", func(context.Context) ([]byte, error) { return []byte("x"), nil })
	waitForWaiters(t, g, "k", 1)
	close(release)
	if p := <-panicked; p != "boom" {
		t.Fatalf("the panic was swallowed: %v", p)
	}
	if r := receive(t, joiner, "the joined caller"); r.err == nil {
		t.Fatalf("a caller that joined a panicking call got %q and no error", r.val)
	}
	if g.Inflight() != 0 {
		t.Fatalf("%d call(s) left in flight after a panic", g.Inflight())
	}
	if v, err, _ := g.DoCtx(context.Background(), "k", func(context.Context) ([]byte, error) { return []byte("ok"), nil }); err != nil || string(v) != "ok" {
		t.Fatalf("the key is stuck after a panic: %q, %v", v, err)
	}
}

// The cost of the group: uncontended, distinct keys in parallel, one key in parallel.
func BenchmarkGroupDoCtx(b *testing.B) {
	fn := func(context.Context) ([]byte, error) { return nil, nil }
	ctx := context.Background()
	b.Run("uncontended", func(b *testing.B) {
		g := NewGroup()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _ = g.DoCtx(ctx, "k", fn)
		}
	})
	b.Run("distinct_keys_parallel", func(b *testing.B) {
		g := NewGroup()
		var next atomic.Int64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			key := fmt.Sprintf("k%d", next.Add(1))
			for pb.Next() {
				_, _, _ = g.DoCtx(ctx, key, fn)
			}
		})
	})
	b.Run("same_key_parallel", func(b *testing.B) {
		g := NewGroup()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _, _ = g.DoCtx(ctx, "k", fn)
			}
		})
	})
}
