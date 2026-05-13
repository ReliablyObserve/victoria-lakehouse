package stats

import (
	"sync"
	"sync/atomic"
)

// CardinalityLimiter caps the number of distinct tenants tracked for
// per-tenant Prometheus metrics. This prevents high-cardinality label
// explosions when many tenants share a single Lakehouse instance.
//
// Semantics of maxTenants:
//   - positive: allow up to N distinct tenants
//   - 0: disable all per-tenant metrics (reject everything)
//   - negative: unlimited (no cap)
type CardinalityLimiter struct {
	mu         sync.RWMutex
	maxTenants int
	tracked    map[string]bool
	overflow   atomic.Int64
}

// NewCardinalityLimiter returns a limiter with the given tenant cap.
func NewCardinalityLimiter(maxTenants int) *CardinalityLimiter {
	return &CardinalityLimiter{
		maxTenants: maxTenants,
		tracked:    make(map[string]bool),
	}
}

// Allow reports whether tenant should be tracked. It uses double-check
// locking: a fast-path RLock for already-tracked tenants, upgrading to
// a full Lock only when a new tenant must be inserted.
func (cl *CardinalityLimiter) Allow(tenant string) bool {
	// Zero cap means all per-tenant metrics are disabled.
	if cl.maxTenants == 0 {
		cl.overflow.Add(1)
		return false
	}

	// Fast path: tenant already tracked.
	cl.mu.RLock()
	if cl.tracked[tenant] {
		cl.mu.RUnlock()
		return true
	}
	cl.mu.RUnlock()

	// Negative cap means unlimited.
	if cl.maxTenants < 0 {
		cl.mu.Lock()
		cl.tracked[tenant] = true
		cl.mu.Unlock()
		return true
	}

	// Slow path: try to add a new tenant under the cap.
	cl.mu.Lock()
	// Double-check: another goroutine may have added it.
	if cl.tracked[tenant] {
		cl.mu.Unlock()
		return true
	}
	if len(cl.tracked) >= cl.maxTenants {
		cl.mu.Unlock()
		cl.overflow.Add(1)
		return false
	}
	cl.tracked[tenant] = true
	cl.mu.Unlock()
	return true
}

// TrackedCount returns the number of distinct tenants currently tracked.
func (cl *CardinalityLimiter) TrackedCount() int {
	cl.mu.RLock()
	n := len(cl.tracked)
	cl.mu.RUnlock()
	return n
}

// OverflowCount returns the cumulative number of rejected Allow calls
// since creation. The counter is never reset by Reset.
func (cl *CardinalityLimiter) OverflowCount() int64 {
	return cl.overflow.Load()
}

// Reset clears the tracked tenant set so new tenants can be admitted.
// The overflow counter is cumulative and is NOT cleared.
func (cl *CardinalityLimiter) Reset() {
	cl.mu.Lock()
	cl.tracked = make(map[string]bool)
	cl.mu.Unlock()
}
