package s3reader

import "sync"

// PoolRegistry hands out per-bucket ClientPools backed by a single
// shared s3.Client. Every distinct bucket returned by a tenant's
// override resolves to one cached pool — no goroutine ever creates
// more than one HTTP transport / credential chain.
//
// Used by the tenant bucket router in main.go to keep one process
// serving many buckets without per-tenant resource cost.
type PoolRegistry struct {
	defaultPool *ClientPool
	mu          sync.Mutex
	pools       map[string]*ClientPool // bucket name -> pool
}

// NewPoolRegistry wraps an existing default pool. The default's
// bucket is also added to the cache so PoolFor(default) returns the
// existing pool unchanged.
func NewPoolRegistry(def *ClientPool) *PoolRegistry {
	return &PoolRegistry{
		defaultPool: def,
		pools:       map[string]*ClientPool{def.Bucket(): def},
	}
}

// PoolFor returns the pool for the given bucket. Empty bucket or the
// default bucket name returns the default pool. Any other bucket is
// cached on first sight via WithBucket — subsequent calls return the
// same pool.
func (r *PoolRegistry) PoolFor(bucket string) *ClientPool {
	if r == nil {
		return nil
	}
	if bucket == "" || bucket == r.defaultPool.Bucket() {
		return r.defaultPool
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.pools[bucket]; ok {
		return p
	}
	p := r.defaultPool.WithBucket(bucket)
	r.pools[bucket] = p
	return p
}

// Default returns the registry's default pool.
func (r *PoolRegistry) Default() *ClientPool {
	if r == nil {
		return nil
	}
	return r.defaultPool
}

// Buckets returns the list of cached bucket names — useful for
// /healthz and operator dashboards. Sorted? No; callers sort if
// they need to.
func (r *PoolRegistry) Buckets() []string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.pools))
	for b := range r.pools {
		out = append(out, b)
	}
	return out
}
