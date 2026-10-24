package cache

import (
	"context"
	"errors"
	"sync"
)

type call struct {
	done chan struct{}
	val  []byte
	err  error
}

type Group struct {
	mu    sync.Mutex
	calls map[string]*call
}

func NewGroup() *Group {
	return &Group{calls: make(map[string]*call)}
}

func (g *Group) Do(key string, fn func() ([]byte, error)) ([]byte, error, bool) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		<-c.done
		return c.val, c.err, true
	}

	c := &call{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	g.finish(key, c)

	return c.val, c.err, false
}

// DoCtx is Do for a call that runs on its caller's context. A caller that joins a
// call in flight waits no longer than its own context. When the call it joined ended
// with a context error and its own context is still live, the error is the first
// caller's (that caller's deadline or cancellation, not a failure of the work), so
// it runs the call itself, on its own context, instead of returning that error.
func (g *Group) DoCtx(ctx context.Context, key string, fn func(context.Context) ([]byte, error)) ([]byte, error, bool) {
	for {
		g.mu.Lock()
		c, ok := g.calls[key]
		if !ok {
			c = &call{done: make(chan struct{})}
			g.calls[key] = c
			g.mu.Unlock()

			c.val, c.err = fn(ctx)
			g.finish(key, c)
			return c.val, c.err, false
		}
		g.mu.Unlock()

		select {
		case <-c.done:
		case <-ctx.Done():
			return nil, ctx.Err(), true
		}
		if c.err != nil && ctx.Err() == nil && (errors.Is(c.err, context.Canceled) || errors.Is(c.err, context.DeadlineExceeded)) {
			continue
		}
		return c.val, c.err, true
	}
}

// finish removes the call from the group before it wakes its waiters, so a waiter
// that runs the call again finds no finished call.
func (g *Group) finish(key string, c *call) {
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	close(c.done)
}

func (g *Group) Inflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}
