package storage

import "github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

// TenantStreamMap maps (tenant, stream hash) to a value. It exists because
// upstream's stream hash is computed from the stream tags alone, without the
// tenant, and one insert batch can carry several tenants, so a decision made for
// a stream must be kept per tenant. A batch almost always has one tenant, so the
// first tenant seen keeps a plain map keyed by the hash (as cheap as before the
// tenant mattered), and a map per further tenant is allocated only when one
// appears. The zero value is an empty map and allocates nothing until the first
// Set.
type TenantStreamMap[V any] struct {
	first    logstorage.TenantID
	hasFirst bool
	byHash   map[uint64]V
	more     map[logstorage.TenantID]map[uint64]V
}

// Get returns the value stored for the tenant's stream.
func (m *TenantStreamMap[V]) Get(tenant logstorage.TenantID, hash uint64) (v V, ok bool) {
	if !m.hasFirst || tenant == m.first {
		v, ok = m.byHash[hash]
		return v, ok
	}
	v, ok = m.more[tenant][hash]
	return v, ok
}

// Set stores v for the tenant's stream.
func (m *TenantStreamMap[V]) Set(tenant logstorage.TenantID, hash uint64, v V) {
	if !m.hasFirst {
		m.first, m.hasFirst = tenant, true
	}
	if tenant == m.first {
		if m.byHash == nil {
			m.byHash = map[uint64]V{}
		}
		m.byHash[hash] = v
		return
	}
	if m.more == nil {
		m.more = map[logstorage.TenantID]map[uint64]V{}
	}
	inner := m.more[tenant]
	if inner == nil {
		inner = map[uint64]V{}
		m.more[tenant] = inner
	}
	inner[hash] = v
}

// Each calls f for every stored value, in no particular order.
func (m *TenantStreamMap[V]) Each(f func(V)) {
	for _, v := range m.byHash {
		f(v)
	}
	for _, inner := range m.more {
		for _, v := range inner {
			f(v)
		}
	}
}
