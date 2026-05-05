package cache

import (
	"container/list"
	"sync"
)

type entry struct {
	key  string
	val  []byte
	size int64
}

type LRU struct {
	mu        sync.Mutex
	items     map[string]*list.Element
	order     *list.List
	curSize   int64
	maxSize   int64
	hits      uint64
	misses    uint64
	evictions uint64
}

func NewLRU(maxSize int64) *LRU {
	return &LRU{
		items:   make(map[string]*list.Element),
		order:   list.New(),
		maxSize: maxSize,
	}
}

func (c *LRU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.order.MoveToFront(el)
		c.hits++
		e := el.Value.(*entry)
		dst := make([]byte, len(e.val))
		copy(dst, e.val)
		return dst, true
	}
	c.misses++
	return nil, false
}

func (c *LRU) Put(key string, val []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := int64(len(val))

	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		c.curSize -= e.size
		e.val = make([]byte, len(val))
		copy(e.val, val)
		e.size = size
		c.curSize += size
		c.order.MoveToFront(el)
	} else {
		e := &entry{key: key, val: make([]byte, len(val)), size: size}
		copy(e.val, val)
		el := c.order.PushFront(e)
		c.items[key] = el
		c.curSize += size
	}

	for c.curSize > c.maxSize && c.order.Len() > 0 {
		c.evictOldest()
	}
}

func (c *LRU) evictOldest() {
	el := c.order.Back()
	if el == nil {
		return
	}
	e := el.Value.(*entry)
	c.order.Remove(el)
	delete(c.items, e.key)
	c.curSize -= e.size
	c.evictions++
}

func (c *LRU) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		c.order.Remove(el)
		delete(c.items, e.key)
		c.curSize -= e.size
	}
}

func (c *LRU) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *LRU) Size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curSize
}

func (c *LRU) MaxSize() int64 {
	return c.maxSize
}

type Stats struct {
	Entries   int
	Size      int64
	MaxSize   int64
	Hits      uint64
	Misses    uint64
	Evictions uint64
}

func (c *LRU) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Entries:   c.order.Len(),
		Size:      c.curSize,
		MaxSize:   c.maxSize,
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
	}
}

func (c *LRU) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element)
	c.order.Init()
	c.curSize = 0
}
