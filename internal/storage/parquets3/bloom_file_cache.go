package parquets3

import (
	"container/list"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
)

// BloomFileCache is a bounded LRU cache for per-file bloom-index sidecars.
//
// A nil *bloomindex.Index value is a valid cache entry — it represents a
// negative cache for files whose .bloom sidecar is absent or unparseable,
// avoiding repeated S3 fetches. Get returns (nil, true) for such entries.
//
// The cache replaces an earlier unbounded sync.Map that grew without limit
// — every bloom sidecar ever fetched stayed resident, which on long-running
// select nodes scanning many partitions could grow to several GB of heap.
type BloomFileCache struct {
	mu       sync.RWMutex
	items    map[string]*bloomFileEntry
	lru      *list.List
	maxItems int
}

type bloomFileEntry struct {
	key   string
	index *bloomindex.Index
	elem  *list.Element
}

// NewBloomFileCache returns a BloomFileCache with the given maximum number
// of entries. If maxItems <= 0, defaults to 1024 — sized to comfortably
// cover the working set of an active select node without unbounded growth.
func NewBloomFileCache(maxItems int) *BloomFileCache {
	if maxItems <= 0 {
		maxItems = 1024
	}
	return &BloomFileCache{
		items:    make(map[string]*bloomFileEntry, maxItems),
		lru:      list.New(),
		maxItems: maxItems,
	}
}

// Get returns the cached bloom index for key.
// The second return value indicates whether the key was present
// (a present-but-nil index is a negative cache entry).
func (c *BloomFileCache) Get(key string) (*bloomindex.Index, bool) {
	c.mu.Lock()
	entry, ok := c.items[key]
	if !ok {
		c.mu.Unlock()
		return nil, false
	}
	c.lru.MoveToFront(entry.elem)
	idx := entry.index
	c.mu.Unlock()
	return idx, true
}

// Put inserts or updates the bloom index for key, evicting the oldest
// entry if the cache is at capacity. A nil idx is stored as a negative
// cache entry (file has no usable bloom sidecar).
func (c *BloomFileCache) Put(key string, idx *bloomindex.Index) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		entry.index = idx
		c.lru.MoveToFront(entry.elem)
		return
	}

	for c.lru.Len() >= c.maxItems {
		back := c.lru.Back()
		if back == nil {
			break
		}
		evicted := back.Value.(*bloomFileEntry)
		c.lru.Remove(back)
		delete(c.items, evicted.key)
	}

	entry := &bloomFileEntry{key: key, index: idx}
	entry.elem = c.lru.PushFront(entry)
	c.items[key] = entry
}

// Len returns the number of entries currently in the cache.
func (c *BloomFileCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
