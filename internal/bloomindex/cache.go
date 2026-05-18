package bloomindex

import (
	"context"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type BloomCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	maxSize int
	loader  func(ctx context.Context, partition string) (*Index, error)
}

type cacheEntry struct {
	idx      *Index
	size     int
	lastUsed time.Time
}

func NewBloomCache(maxSize int, loader func(ctx context.Context, partition string) (*Index, error)) *BloomCache {
	return &BloomCache{
		entries: make(map[string]*cacheEntry),
		maxSize: maxSize,
		loader:  loader,
	}
}

func (c *BloomCache) Get(ctx context.Context, partition string) (*Index, error) {
	c.mu.RLock()
	entry, ok := c.entries[partition]
	c.mu.RUnlock()

	if ok {
		c.mu.Lock()
		entry.lastUsed = time.Now()
		c.mu.Unlock()
		return entry.idx, nil
	}

	if c.loader == nil {
		return nil, nil
	}

	idx, err := c.loader(ctx, partition)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}

	data := idx.Marshal()
	size := len(data)

	c.mu.Lock()
	c.evictLocked(size)
	c.entries[partition] = &cacheEntry{
		idx:      idx,
		size:     size,
		lastUsed: time.Now(),
	}
	metrics.BloomBytesMemory.Set(int64(c.currentSizeLocked()))
	c.mu.Unlock()

	return idx, nil
}

func (c *BloomCache) Put(partition string, idx *Index) {
	data := idx.Marshal()
	size := len(data)

	c.mu.Lock()
	c.evictLocked(size)
	c.entries[partition] = &cacheEntry{
		idx:      idx,
		size:     size,
		lastUsed: time.Now(),
	}
	metrics.BloomBytesMemory.Set(int64(c.currentSizeLocked()))
	c.mu.Unlock()
}

func (c *BloomCache) Invalidate(partition string) {
	c.mu.Lock()
	delete(c.entries, partition)
	metrics.BloomBytesMemory.Set(int64(c.currentSizeLocked()))
	c.mu.Unlock()
}

func (c *BloomCache) Warm(ctx context.Context, partitions []string) error {
	for _, p := range partitions {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := c.Get(ctx, p)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *BloomCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func (c *BloomCache) PartitionNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.entries))
	for k := range c.entries {
		names = append(names, k)
	}
	return names
}

func (c *BloomCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := 0
	for _, e := range c.entries {
		total += e.size
	}
	return total
}

func (c *BloomCache) evictLocked(needed int) {
	for c.currentSizeLocked()+needed > c.maxSize && len(c.entries) > 0 {
		var oldestKey string
		var oldestTime time.Time
		first := true
		for k, e := range c.entries {
			if first || e.lastUsed.Before(oldestTime) {
				oldestKey = k
				oldestTime = e.lastUsed
				first = false
			}
		}
		delete(c.entries, oldestKey)
	}
}

func (c *BloomCache) currentSizeLocked() int {
	total := 0
	for _, e := range c.entries {
		total += e.size
	}
	return total
}
