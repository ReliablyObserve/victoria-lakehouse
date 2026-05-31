package cache

import (
	"container/list"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
)

type entry struct {
	key  string
	val  []byte
	size int64
	// boundRelease releases the K8s-style cache-memory Bound slot
	// reserved when this entry was admitted. Nil for entries admitted
	// before SetBound was called; ignored on Delete/Clear when nil.
	// Invoked on eviction (LRU size limit), explicit Delete, and Clear.
	boundRelease func()
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
	// bound is the K8s-style cache-memory admission gate, set via
	// SetBound at storage construction. When non-nil every Put/PutNoCopy
	// attempts a TryAcquire on the bound BEFORE the LRU's own size
	// accounting; rejection causes the Put to silently no-op (cache
	// becomes best-effort), matching the LRU's existing fail-soft
	// semantics. The bound's Release runs on eviction/Delete/Clear via
	// the per-entry boundRelease closure.
	//
	// nil bound preserves pre-bound behaviour exactly (every Put
	// admits; LRU eviction is the only memory-pressure response).
	bound *resourcebounds.Bound
	// rejected counts the number of Put/PutNoCopy calls that were
	// rejected by the bound (admit failed) so callers can verify the
	// bound is load-bearing in tests; the per-bound rejected_total
	// Prometheus counter is the operator-facing signal.
	rejected uint64
}

func NewLRU(maxSize int64) *LRU {
	return &LRU{
		items:   make(map[string]*list.Element),
		order:   list.New(),
		maxSize: maxSize,
	}
}

// SetBound attaches a K8s-style cache-memory admission gate. Call once
// at storage construction (NOT thread-safe with concurrent Puts —
// LRU is normally constructed before any worker goroutines are spawned).
// Subsequent Put/PutNoCopy attempts TryAcquire on the bound; if the
// bound is full the Put is dropped (cache is best-effort), and the
// rejected_total metric on the bound increments by 1. Release runs
// when the entry is evicted, deleted, or cleared.
func (c *LRU) SetBound(b *resourcebounds.Bound) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bound = b
}

// RejectedByBound returns the cumulative count of Put/PutNoCopy calls
// rejected by the cache-memory bound. Exposed for regression tests that
// assert the bound is load-bearing; operators read the same signal via
// the bound's rejected_total Prometheus counter.
func (c *LRU) RejectedByBound() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rejected
}

// Get returns the cached value for key. The returned []byte is the
// cache-owned buffer — callers MUST NOT mutate it. This share-by-reference
// behaviour avoids the N-workers × file-size memory blowup that copied
// every cache hit; with N=16 workers scanning a 24h wildcard window and
// ~58 cached files (~2 MB each) the copies alone added up to >1 GiB of
// transient heap pressure, OOM-killing the 2 GiB container.
//
// All current call sites (storage.parquets3.getFileData,
// smartcache.Controller.Get) pass the bytes straight to
// parquet.OpenFile(bytes.NewReader(...)), which never mutates the input,
// so sharing is safe. Future call sites that need a mutable copy must do
// the copy explicitly.
func (c *LRU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.order.MoveToFront(el)
		c.hits++
		e := el.Value.(*entry)
		return e.val, true
	}
	c.misses++
	return nil, false
}

// Put stores val under key. The cache stores a copy of val so the caller
// may continue to mutate the input buffer safely. For high-throughput
// paths that have just allocated val and will not mutate it further
// (e.g. the parquet-file download pipeline), use PutNoCopy to avoid
// duplicating the buffer — this is the difference between ~250 MiB
// transient memory and ~125 MiB during a 16-worker wildcard scan over
// 100+ files.
func (c *LRU) Put(key string, val []byte) {
	buf := make([]byte, len(val))
	copy(buf, val)
	c.putBuffer(key, buf)
}

// PutNoCopy stores val under key WITHOUT copying. The caller transfers
// ownership of val to the cache: after this call returns, val MUST NOT
// be mutated (the cache may hand it out via Get to other goroutines).
//
// This is the canonical API for the S3-download → cache path: the
// downloaded buffer is allocated by io.ReadAll for the sole purpose of
// caching, and the caller never mutates it. Sharing the buffer between
// the singleflight result and the cache slot halves the transient
// memory footprint of a wildcard scan, which is the difference between
// fitting and OOM-killing on a 2 GiB container under production-shape
// load (per heap-diff at near-OOM: io.ReadAll=253 MiB, LRU.Put=382 MiB
// — roughly 50% overlap that this API eliminates).
func (c *LRU) PutNoCopy(key string, val []byte) {
	c.putBuffer(key, val)
}

func (c *LRU) putBuffer(key string, buf []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := int64(len(buf))

	// K8s-style cache-memory admission. TryAcquire (non-blocking) on the
	// bound BEFORE making any state changes — the cache is best-effort:
	// rejection means "skip caching this entry", not "block the write".
	// The LRU's own size-driven eviction is preserved as the
	// load-bearing memory-pressure response; the bound is an EXTRA
	// process-wide admission ceiling that triggers a 429-shaped signal
	// (rejected_total++) when the operator-visible Limit is exceeded.
	//
	// For updates (key already present), TryAcquire against the size
	// DELTA to avoid double-counting the bytes already held under this
	// key's existing reservation. Update path: release old slot first,
	// then try-acquire the new size. If new acquire fails, no-op (old
	// entry stays, bound accounting consistent).
	var newRelease func()
	if c.bound != nil {
		if el, ok := c.items[key]; ok {
			// Update path: release existing slot, then try to reserve
			// the new size. If reservation fails, keep the old entry
			// (the in-place size update is dropped).
			oldEntry := el.Value.(*entry)
			if oldEntry.boundRelease != nil {
				oldEntry.boundRelease()
				oldEntry.boundRelease = nil
			}
		}
		rel, err := c.bound.TryAcquire(size)
		if err != nil {
			c.rejected++
			return
		}
		newRelease = rel
	}

	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		c.curSize -= e.size
		e.val = buf
		e.size = size
		e.boundRelease = newRelease
		c.curSize += size
		c.order.MoveToFront(el)
	} else {
		e := &entry{key: key, val: buf, size: size, boundRelease: newRelease}
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
	if e.boundRelease != nil {
		e.boundRelease()
		e.boundRelease = nil
	}
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
		if e.boundRelease != nil {
			e.boundRelease()
			e.boundRelease = nil
		}
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
	// Release all bound slots before dropping references — without this
	// the bound's outstanding counters would leak by the size of the
	// cache on every Clear, eventually exhausting the limit and
	// silently rejecting all future Puts.
	for _, el := range c.items {
		e := el.Value.(*entry)
		if e.boundRelease != nil {
			e.boundRelease()
			e.boundRelease = nil
		}
	}
	c.items = make(map[string]*list.Element)
	c.order.Init()
	c.curSize = 0
}
