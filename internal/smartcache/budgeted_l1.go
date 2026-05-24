package smartcache

import (
	"container/list"
	"sync"
)

// BudgetedL1 is a memory-budgeted L1 cache that uses LRU ordering.
// When the budget is exceeded on Put, the least-recently-used entries
// are evicted (spilled) to the configured L2 cache.
//
// BudgetedL1 implements the L1Cache interface and is safe for
// concurrent use.
type BudgetedL1 struct {
	mu       sync.Mutex
	maxBytes int64
	used     int64
	items    map[string]*list.Element
	order    *list.List // front = most recently used
	l2       L2Cache
}

type l1Entry struct {
	key  string
	data []byte
}

// NewBudgetedL1 creates a BudgetedL1 with the given memory budget.
// The l2 parameter may be nil; if nil, evicted entries are simply
// discarded instead of spilled.
func NewBudgetedL1(maxBytes int64, l2 L2Cache) *BudgetedL1 {
	return &BudgetedL1{
		maxBytes: maxBytes,
		items:    make(map[string]*list.Element),
		order:    list.New(),
		l2:       l2,
	}
}

// Get retrieves a value from the cache. On hit the entry is promoted
// to the front of the LRU list. Returns the data and true on hit,
// or nil and false on miss.
func (c *BudgetedL1) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	entry := elem.Value.(*l1Entry)
	// Return a copy so callers cannot mutate cached data.
	cp := make([]byte, len(entry.data))
	copy(cp, entry.data)
	return cp, true
}

// Put stores a value in the cache. If the key already exists its value
// is replaced and the budget adjusted. After insertion, if the total
// used bytes exceed maxBytes, the least-recently-used entries are evicted
// to L2 until the budget is satisfied (or the cache is empty).
func (c *BudgetedL1) Put(key string, val []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := int64(len(val))

	// Overwrite path: remove old entry first.
	if elem, ok := c.items[key]; ok {
		old := elem.Value.(*l1Entry)
		c.used -= int64(len(old.data))
		c.order.Remove(elem)
		delete(c.items, key)
	}

	// Store a copy of the value.
	cp := make([]byte, len(val))
	copy(cp, val)

	entry := &l1Entry{key: key, data: cp}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
	c.used += size

	// Evict LRU entries until under budget.
	c.evictLocked()
}

// UsedBytes returns the current number of bytes stored in the cache.
func (c *BudgetedL1) UsedBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

// Len returns the number of entries in the cache.
func (c *BudgetedL1) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// evictLocked removes LRU entries until used <= maxBytes, spilling
// each evicted entry to L2 if configured. Caller must hold c.mu.
func (c *BudgetedL1) evictLocked() {
	for c.used > c.maxBytes && c.order.Len() > 0 {
		tail := c.order.Back()
		if tail == nil {
			break
		}
		entry := tail.Value.(*l1Entry)

		// Spill to L2 before removing.
		if c.l2 != nil {
			_ = c.l2.Put(entry.key, entry.data)
		}

		c.used -= int64(len(entry.data))
		c.order.Remove(tail)
		delete(c.items, entry.key)
	}
}
