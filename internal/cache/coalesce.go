package cache

import "sync"

type call struct {
	wg  sync.WaitGroup
	val []byte
	err error
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
		c.wg.Wait()
		return c.val, c.err, true
	}

	c := &call{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return c.val, c.err, false
}

func (g *Group) Inflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}
