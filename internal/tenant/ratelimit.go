package tenant

import (
	"sync"
	"sync/atomic"
	"time"
)

// IngestRateLimiter enforces per-tenant ingest rate caps using a
// simple token-bucket per (account, project). Caps come from
// PolicyRegistry; tenants without an override are unlimited.
//
// Two independent buckets per tenant: one for bytes/sec, one for
// rows/sec. The middleware (or any callsite) calls Allow before
// admitting a request and consumes the request's bytes/rows. A
// returned false means the caller should respond 429.
type IngestRateLimiter struct {
	policy   *PolicyRegistry
	buckets  sync.Map // TenantID -> *tenantBuckets
	now      func() time.Time
	rejected atomic.Int64
}

type tenantBuckets struct {
	mu        sync.Mutex
	bytes     float64 // current tokens (bytes)
	rows      float64 // current tokens (rows)
	lastBytes time.Time
	lastRows  time.Time
}

// NewIngestRateLimiter returns a limiter bound to the given
// PolicyRegistry. nil registry disables enforcement.
func NewIngestRateLimiter(policy *PolicyRegistry) *IngestRateLimiter {
	return &IngestRateLimiter{policy: policy, now: time.Now}
}

// Allow consumes bytes + rows from the tenant's buckets. Returns true
// when both buckets have enough tokens, false if either is exhausted.
// Unlimited tenants (no MaxBytesPerSec / MaxRowsPerSec) always pass.
func (l *IngestRateLimiter) Allow(accountID, projectID uint32, bytes, rows int64) bool {
	if l == nil || l.policy == nil {
		return true
	}
	eff := l.policy.For(accountID, projectID)
	if eff == nil || (eff.MaxBytesPerSec <= 0 && eff.MaxRowsPerSec <= 0) {
		return true
	}

	tb := l.bucketsFor(TenantID{AccountID: accountID, ProjectID: projectID})
	now := l.now()

	tb.mu.Lock()
	defer tb.mu.Unlock()

	if eff.MaxBytesPerSec > 0 {
		refill(&tb.bytes, &tb.lastBytes, now, eff.MaxBytesPerSec)
		if tb.bytes < float64(bytes) {
			l.rejected.Add(1)
			return false
		}
	}
	if eff.MaxRowsPerSec > 0 {
		refill(&tb.rows, &tb.lastRows, now, eff.MaxRowsPerSec)
		if tb.rows < float64(rows) {
			l.rejected.Add(1)
			return false
		}
	}

	// Both checks passed — debit.
	if eff.MaxBytesPerSec > 0 {
		tb.bytes -= float64(bytes)
	}
	if eff.MaxRowsPerSec > 0 {
		tb.rows -= float64(rows)
	}
	return true
}

// Rejected returns cumulative rejection count since process start.
func (l *IngestRateLimiter) Rejected() int64 {
	if l == nil {
		return 0
	}
	return l.rejected.Load()
}

func (l *IngestRateLimiter) bucketsFor(tid TenantID) *tenantBuckets {
	if v, ok := l.buckets.Load(tid); ok {
		return v.(*tenantBuckets)
	}
	// Leave last{Bytes,Rows} as the zero time so the first refill
	// initializes the bucket at full capacity.
	fresh := &tenantBuckets{}
	actual, _ := l.buckets.LoadOrStore(tid, fresh)
	return actual.(*tenantBuckets)
}

// refill tops up the bucket from elapsed wall time against the
// configured per-second cap. Cap is also the burst size (capacity)
// so a fresh tenant can consume up to one second's worth instantly.
func refill(tokens *float64, last *time.Time, now time.Time, capacityPerSec int64) {
	if last.IsZero() {
		*last = now
		*tokens = float64(capacityPerSec)
		return
	}
	elapsed := now.Sub(*last).Seconds()
	if elapsed <= 0 {
		return
	}
	*tokens += elapsed * float64(capacityPerSec)
	if *tokens > float64(capacityPerSec) {
		*tokens = float64(capacityPerSec)
	}
	*last = now
}
