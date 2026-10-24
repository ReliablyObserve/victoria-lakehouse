package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type call struct {
	done    chan struct{}
	val     []byte
	err     error
	waiters atomic.Int32 // callers that joined this call and have not yet returned
}

// Group collapses concurrent calls for one key onto a single run.
type Group struct {
	mu    sync.Mutex
	calls map[string]*call
}

func NewGroup() *Group {
	return &Group{calls: make(map[string]*call)}
}

// DoCtx runs fn once for concurrent callers of key and shares its result (the
// returned bytes are read-only for every caller). The caller that starts the
// call runs fn on its own context and gets shared=false; a caller that joins a
// call in flight gets shared=true.
//
// A joining caller waits no longer than its own context. The first caller's
// deadline or cancellation is not a failure of the work: when the call it joined
// ended with a context error and its own context is still live, it runs the call
// itself, on its own context, instead of returning that error. When its own
// context is done as well it returns its own context's error, never another
// caller's.
func (g *Group) DoCtx(ctx context.Context, key string, fn func(context.Context) ([]byte, error)) ([]byte, error, bool) {
	for {
		g.mu.Lock()
		c, ok := g.calls[key]
		if !ok {
			c = &call{done: make(chan struct{})}
			g.calls[key] = c
			g.mu.Unlock()
			return g.lead(ctx, key, c, fn)
		}
		c.waiters.Add(1)
		g.mu.Unlock()

		select {
		case <-c.done:
		case <-ctx.Done():
			c.waiters.Add(-1)
			return nil, ctx.Err(), true
		}
		c.waiters.Add(-1)
		if c.err != nil && isContextError(c.err) {
			if err := ctx.Err(); err != nil {
				return nil, err, true
			}
			continue
		}
		return c.val, c.err, true
	}
}

// lead runs fn as the call c of key. The call leaves the group before its waiters
// are woken, also when fn panics, so that a waiter that runs the call again finds
// no finished call and a panic never leaves the key in flight.
func (g *Group) lead(ctx context.Context, key string, c *call, fn func(context.Context) ([]byte, error)) ([]byte, error, bool) {
	completed := false
	defer func() {
		if !completed {
			c.err = errCallPanicked // what a caller that joined the call sees
		}
		g.finish(key, c)
	}()
	c.val, c.err = fn(ctx)
	completed = true
	return c.val, c.err, false
}

var errCallPanicked = errors.New("cache: the call this caller joined panicked")

func (g *Group) finish(key string, c *call) {
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	close(c.done)
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Inflight is the number of keys with a call in flight.
func (g *Group) Inflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

// Waiters is the number of callers that have joined the call in flight for key
// and not yet returned (0 when none is in flight).
func (g *Group) Waiters(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.calls[key]; ok {
		return int(c.waiters.Load())
	}
	return 0
}
