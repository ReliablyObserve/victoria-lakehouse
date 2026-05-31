// Package resourcebounds implements K8s-style request/limit resource
// controls for Victoria Lakehouse subsystems (S3 download concurrency,
// query file workers, cache memory, smart cache disk, query max rows).
//
// The model mirrors Kubernetes container resources: every resource
// surface declares a `Request` (always-reserved baseline, never queues
// below this), a `Limit` (hard upper bound, never exceeded), and a
// `ScalingPolicy` describing how usage grows from request → limit
// under load. Callers acquire against the bound, the bound blocks when
// the limit is exhausted, and the caller observes overflow events via
// the metrics emitted by NewBound.
//
// Design parity with the existing in-tree fileBudget (see
// internal/storage/parquets3/query_memory_budget.go): the bound is
// (size, count) aware and admits a single outlier larger than the
// limit when no other holders are outstanding — this preserves the
// load-bearing semantics that keep individual large parquet files
// processable even when they exceed the cumulative ceiling. New
// surfaces that only care about count (S3 concurrent downloads, file
// workers) acquire with size=1 and ignore the byte accounting; size-aware
// surfaces (file budget, cache memory) get both axes for free.
package resourcebounds

import (
	"context"
	"errors"
	"sync"
)

// ErrBoundFull is returned by TryAcquire when the bound is at its
// configured ceiling. Use errors.Is to test in callers; the cache /
// disk-cache hot paths use this signal to skip caching gracefully
// rather than wedging the write.
var ErrBoundFull = errors.New("resource bound full")

// ScalingPolicy describes how a bound's effective ceiling grows from
// Request toward Limit as load on the bound increases.
//
// All policies enforce Request ≤ effective ≤ Limit. Fixed always
// returns Limit (i.e., no scaling — the bound behaves like a flat
// ceiling at Limit and Request is reserved baseline). LinearGrowth and
// ExponentialBackoff are reserved for future signal-driven scaling
// (e.g., scaling file workers from request toward limit based on
// queue depth), per the design spec; the current acquire path treats
// them identically to Fixed for backwards compatibility — the wired
// limit is always Limit, and Request shows up as the operator-visible
// baseline in metrics.
type ScalingPolicy int

const (
	// Fixed: effective ceiling is always Limit (flat behaviour).
	// Request is the operator-visible baseline reservation, exposed
	// in metrics so dashboards show request vs limit vs usage.
	Fixed ScalingPolicy = iota
	// LinearGrowth: effective ceiling grows linearly from Request to
	// Limit as the bound's `scale_on` signal moves from 0 to its
	// configured pressure threshold. Reserved for future use; the
	// acquire path currently treats this as Fixed.
	LinearGrowth
	// ExponentialBackoff: effective ceiling grows on the inverse of
	// queue wait time (longer waits → expand toward Limit faster).
	// Reserved for future use.
	ExponentialBackoff
)

// String returns the canonical name for the scaling policy, used in
// metric labels and config validation errors.
func (s ScalingPolicy) String() string {
	switch s {
	case Fixed:
		return "fixed"
	case LinearGrowth:
		return "linear"
	case ExponentialBackoff:
		return "expbackoff"
	default:
		return "unknown"
	}
}

// Metrics is the per-bound metric sink. The resourcebounds package
// does not import the lakehouse metrics package directly; callers
// inject implementations from internal/metrics (see NewBound). This
// keeps the package leaf-importable from cmd/, internal/storage/, and
// internal/config/ without introducing cycles.
//
// All methods MUST be safe for concurrent use. nil receivers are
// allowed and skipped — bounds with no metrics still operate
// correctly but are invisible to operators.
type Metrics interface {
	// AcquiredAdd increments the cumulative count of successful
	// acquisitions by n.
	AcquiredAdd(n int64)
	// RejectedAdd increments the cumulative count of rejected
	// acquisitions (overflow or context cancellation) by n.
	RejectedAdd(n int64)
	// OutstandingBytesSet records the current resident bytes held
	// against the bound (gauge).
	OutstandingBytesSet(v int64)
	// OutstandingCountSet records the current count of in-flight
	// holders against the bound (gauge).
	OutstandingCountSet(v int64)
}

// Config describes a bound's K8s-style request/limit shape. All
// fields are populated by config.go / main.go from operator-facing
// flags or YAML; defaults are applied by the surface owner before
// constructing the bound.
type Config struct {
	// Request is the always-reserved baseline. Operators see this
	// as the floor in metrics; it is NOT a soft minimum on usage
	// (the bound does not pre-allocate), only a documented
	// reservation that the operator should size the container/pod
	// resources against.
	Request int64
	// Limit is the hard ceiling. Acquires block (or are rejected
	// with a non-nil error when ctx is cancelled) once cumulative
	// outstanding bytes would exceed Limit.
	Limit int64
	// LimitCount caps the number of concurrent holders. Like
	// Limit/Request this is a hard ceiling; an acquire blocks
	// while outstanding count is at LimitCount. Zero means "no
	// count cap" — only byte accounting applies.
	LimitCount int
	// Policy is the scaling policy. Currently only Fixed is wired
	// through to acquire; LinearGrowth and ExponentialBackoff are
	// reserved for future signal-driven scaling and treated as
	// Fixed today (effective ceiling = Limit, Request shown in
	// metrics).
	Policy ScalingPolicy
}

// Bound is the K8s-style request/limit gate for one resource surface.
//
// Bound is safe for concurrent use. The zero value is NOT useful;
// callers must construct via NewBound, which captures the config and
// (optionally) a Metrics sink.
//
// Semantics:
//   - Acquire(ctx, n) blocks until cumulative outstanding bytes + n
//     ≤ Limit AND outstanding count < LimitCount (when LimitCount > 0).
//   - An empty pool always admits ONE outlier holder (the
//     legacy fileBudget semantics): if outstanding count is 0, any
//     single acquire is admitted regardless of size, and the request
//     is internally clamped to Limit so the accounting stays sane.
//     This preserves the load-bearing path for individual large
//     parquet files that exceed the cumulative ceiling.
//   - Cancelling ctx during a wait returns the cancellation error
//     and a no-op release.
//   - The returned release MUST be called exactly once when the
//     holder is done; double-release is safe (idempotent) and
//     under-release leaks slots until process exit.
type Bound struct {
	cfg     Config
	metrics Metrics

	mu       sync.Mutex
	cond     *sync.Cond
	outBytes int64
	outCount int

	// counters are tracked separately so OutstandingBytesSet/
	// OutstandingCountSet read consistent point-in-time values
	// without re-acquiring mu (see Outstanding).
	acquiredTotal int64
	rejectedTotal int64
}

// NewBound constructs a Bound with the supplied config and (optional)
// metric sink. Both Request ≤ Limit and LimitCount ≥ 0 are required;
// caller validates before construction (no panic — the surface owner
// is responsible for sane defaults). If LimitCount is 0 the count
// gate is disabled; if Limit is 0 the byte gate is disabled (only
// count enforced). Setting both to 0 is permitted and results in an
// unbounded passthrough — useful for tests but logged at INFO by
// the surface owner.
func NewBound(cfg Config, m Metrics) *Bound {
	b := &Bound{cfg: cfg, metrics: m}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Acquire reserves n bytes + 1 count slot against the bound. Blocks
// until the gates allow or ctx is cancelled.
//
// n ≤ 0 is normalised to 1 (every acquire consumes at least one count
// slot even when the surface only cares about count). A request that
// exceeds Limit alone — i.e., n > Limit and outstanding count is 0 —
// is admitted as the single outlier (see Bound type docs); the
// internal accounting clamps n to Limit so outstanding never exceeds
// the configured ceiling on the wire.
//
// Returns a release func that must be called when the holder is done.
// On context cancellation, returns ctx.Err() and a no-op release.
func (b *Bound) Acquire(ctx context.Context, n int64) (func(), error) {
	if n <= 0 {
		n = 1
	}
	limit := b.cfg.Limit
	if limit > 0 && n > limit {
		// Outlier: admit alone but block others while in flight.
		// Mirrors the legacy fileBudget semantics — see
		// query_memory_budget.go for the heap-diff rationale.
		n = limit
	}

	// Signal cond.Broadcast on ctx cancellation so a waiting acquire
	// wakes up and observes ctx.Err().
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		case <-done:
		}
	}()

	b.mu.Lock()
	for {
		bytesOK := limit <= 0 || b.outBytes+n <= limit || b.outCount == 0
		countOK := b.cfg.LimitCount <= 0 || b.outCount < b.cfg.LimitCount
		if bytesOK && countOK {
			break
		}
		if ctx.Err() != nil {
			b.rejectedTotal++
			if b.metrics != nil {
				b.metrics.RejectedAdd(1)
			}
			b.mu.Unlock()
			return func() {}, ctx.Err()
		}
		b.cond.Wait()
	}
	if ctx.Err() != nil {
		b.rejectedTotal++
		if b.metrics != nil {
			b.metrics.RejectedAdd(1)
		}
		b.mu.Unlock()
		return func() {}, ctx.Err()
	}
	b.outBytes += n
	b.outCount++
	b.acquiredTotal++
	if b.metrics != nil {
		b.metrics.AcquiredAdd(1)
		b.metrics.OutstandingBytesSet(b.outBytes)
		b.metrics.OutstandingCountSet(int64(b.outCount))
	}
	b.mu.Unlock()

	released := false
	var releaseMu sync.Mutex
	return func() {
		releaseMu.Lock()
		defer releaseMu.Unlock()
		if released {
			return
		}
		released = true
		b.mu.Lock()
		b.outBytes -= n
		b.outCount--
		if b.outBytes < 0 {
			b.outBytes = 0
		}
		if b.outCount < 0 {
			b.outCount = 0
		}
		if b.metrics != nil {
			b.metrics.OutstandingBytesSet(b.outBytes)
			b.metrics.OutstandingCountSet(int64(b.outCount))
		}
		b.cond.Broadcast()
		b.mu.Unlock()
	}, nil
}

// TryAcquire attempts to reserve n bytes + 1 count slot WITHOUT
// blocking. If the bound is at capacity TryAcquire returns
// ErrBoundFull and a no-op release; the rejected_total metric is
// incremented (parity with Acquire's cancellation path).
//
// Use TryAcquire from callers that cannot tolerate blocking — most
// commonly the cache.Put hot path, which prefers to skip caching
// (best-effort) over wedging the write. Outlier admission (n > Limit
// with empty pool) still applies, matching Acquire.
//
// Returns ErrBoundFull when at capacity, nil on successful admission.
// The release function MUST be called exactly once when the holder is
// done; double-release is idempotent.
func (b *Bound) TryAcquire(n int64) (func(), error) {
	if n <= 0 {
		n = 1
	}
	limit := b.cfg.Limit
	if limit > 0 && n > limit {
		n = limit
	}

	b.mu.Lock()
	bytesOK := limit <= 0 || b.outBytes+n <= limit || b.outCount == 0
	countOK := b.cfg.LimitCount <= 0 || b.outCount < b.cfg.LimitCount
	if !bytesOK || !countOK {
		b.rejectedTotal++
		if b.metrics != nil {
			b.metrics.RejectedAdd(1)
		}
		b.mu.Unlock()
		return func() {}, ErrBoundFull
	}
	b.outBytes += n
	b.outCount++
	b.acquiredTotal++
	if b.metrics != nil {
		b.metrics.AcquiredAdd(1)
		b.metrics.OutstandingBytesSet(b.outBytes)
		b.metrics.OutstandingCountSet(int64(b.outCount))
	}
	b.mu.Unlock()

	released := false
	var releaseMu sync.Mutex
	return func() {
		releaseMu.Lock()
		defer releaseMu.Unlock()
		if released {
			return
		}
		released = true
		b.mu.Lock()
		b.outBytes -= n
		b.outCount--
		if b.outBytes < 0 {
			b.outBytes = 0
		}
		if b.outCount < 0 {
			b.outCount = 0
		}
		if b.metrics != nil {
			b.metrics.OutstandingBytesSet(b.outBytes)
			b.metrics.OutstandingCountSet(int64(b.outCount))
		}
		b.cond.Broadcast()
		b.mu.Unlock()
	}, nil
}

// Outstanding returns a point-in-time snapshot of the bound's
// outstanding bytes and count. Safe for concurrent use.
func (b *Bound) Outstanding() (int64, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.outBytes, b.outCount
}

// Stats returns a point-in-time snapshot of the bound's lifetime
// counters: total acquired, total rejected, current outstanding
// bytes, current outstanding count. Useful for tests.
func (b *Bound) Stats() (acquired, rejected, outBytes int64, outCount int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.acquiredTotal, b.rejectedTotal, b.outBytes, b.outCount
}

// Config returns the bound's configured request/limit shape. Used by
// metric exporters that expose request/limit alongside outstanding
// usage (matching the k8s_container_memory_request/limit dashboard
// shape).
func (b *Bound) Config() Config {
	return b.cfg
}
