package tenant

import (
	"sync"
	"sync/atomic"
)

// CardinalityLimiter tracks distinct streams and field names per
// tenant, rejecting new ones once a tenant exceeds its configured
// cap. Reads its caps from the PolicyRegistry; tenants without an
// override are unbounded.
//
// Designed for the insert hot path: Allow returns in O(1) for known
// streams via a sync.Map. New streams take the slow path through a
// per-tenant mutex only on first sight.
type CardinalityLimiter struct {
	policy  *PolicyRegistry
	tenants sync.Map // TenantID -> *tenantState

	rejected atomic.Int64
}

type tenantState struct {
	mu      sync.RWMutex
	streams map[string]struct{}
	fields  map[string]struct{}
}

// NewCardinalityLimiter returns a limiter backed by the given
// PolicyRegistry. A nil registry disables enforcement entirely
// (every AllowStream / AllowField returns true).
func NewCardinalityLimiter(policy *PolicyRegistry) *CardinalityLimiter {
	return &CardinalityLimiter{policy: policy}
}

// AllowStream reports whether the (account, project, stream) triple
// may be admitted. The first MaxStreams distinct streams for a tenant
// are tracked; subsequent new streams are rejected and counted as
// drops. Returns true unconditionally when policy is nil or the
// tenant has no MaxStreams override.
func (l *CardinalityLimiter) AllowStream(accountID, projectID uint32, stream string) bool {
	if l == nil || l.policy == nil {
		return true
	}
	eff := l.policy.For(accountID, projectID)
	if eff == nil || eff.MaxStreams <= 0 {
		return true
	}
	tid := TenantID{AccountID: accountID, ProjectID: projectID}
	st := l.stateFor(tid)

	st.mu.RLock()
	_, known := st.streams[stream]
	st.mu.RUnlock()
	if known {
		return true
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if _, known := st.streams[stream]; known {
		return true
	}
	if len(st.streams) >= eff.MaxStreams {
		l.rejected.Add(1)
		return false
	}
	st.streams[stream] = struct{}{}
	return true
}

// AllowField mirrors AllowStream for distinct field names, gated by
// the tenant's MaxFields override.
func (l *CardinalityLimiter) AllowField(accountID, projectID uint32, field string) bool {
	if l == nil || l.policy == nil {
		return true
	}
	eff := l.policy.For(accountID, projectID)
	if eff == nil || eff.MaxFields <= 0 {
		return true
	}
	tid := TenantID{AccountID: accountID, ProjectID: projectID}
	st := l.stateFor(tid)

	st.mu.RLock()
	_, known := st.fields[field]
	st.mu.RUnlock()
	if known {
		return true
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if _, known := st.fields[field]; known {
		return true
	}
	if len(st.fields) >= eff.MaxFields {
		l.rejected.Add(1)
		return false
	}
	st.fields[field] = struct{}{}
	return true
}

// Rejected returns the cumulative count of rejected AllowStream +
// AllowField calls since process start. Surfaced via the metrics
// package for operator dashboards.
func (l *CardinalityLimiter) Rejected() int64 {
	if l == nil {
		return 0
	}
	return l.rejected.Load()
}

// StreamCount returns the number of distinct streams currently
// tracked for the tenant — useful for /api/v1/tenants/{id} dashboards.
func (l *CardinalityLimiter) StreamCount(accountID, projectID uint32) int {
	if l == nil {
		return 0
	}
	tid := TenantID{AccountID: accountID, ProjectID: projectID}
	v, ok := l.tenants.Load(tid)
	if !ok {
		return 0
	}
	st := v.(*tenantState)
	st.mu.RLock()
	n := len(st.streams)
	st.mu.RUnlock()
	return n
}

func (l *CardinalityLimiter) stateFor(tid TenantID) *tenantState {
	if v, ok := l.tenants.Load(tid); ok {
		return v.(*tenantState)
	}
	fresh := &tenantState{
		streams: make(map[string]struct{}),
		fields:  make(map[string]struct{}),
	}
	actual, _ := l.tenants.LoadOrStore(tid, fresh)
	return actual.(*tenantState)
}
