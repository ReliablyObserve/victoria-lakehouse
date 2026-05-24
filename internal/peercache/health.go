package peercache

import (
	"sync"
	"time"
)

// PeerHealth tracks the health state of a single peer.
type PeerHealth struct {
	LastSeen  time.Time
	FailCount int
	IsHealthy bool
}

// HealthAwareRing wraps a Ring and tracks peer health via heartbeat/failure
// counting. Unhealthy peers can be temporarily removed from consideration
// and re-added when they recover.
type HealthAwareRing struct {
	mu       sync.RWMutex
	ring     *Ring
	health   map[string]*PeerHealth
	maxFails int
	timeout  time.Duration
}

// NewHealthAwareRing creates a HealthAwareRing wrapping the given ring.
// maxFails is the number of consecutive failures before a peer is marked unhealthy.
// timeout is the duration after which a peer with no heartbeat is considered stale.
// ring may be nil for testing purposes.
func NewHealthAwareRing(ring *Ring, maxFails int, timeout time.Duration) *HealthAwareRing {
	return &HealthAwareRing{
		ring:     ring,
		health:   make(map[string]*PeerHealth),
		maxFails: maxFails,
		timeout:  timeout,
	}
}

// RecordSuccess marks a peer as healthy and resets its failure count.
func (h *HealthAwareRing) RecordSuccess(peer string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ph, ok := h.health[peer]
	if !ok {
		ph = &PeerHealth{IsHealthy: true}
		h.health[peer] = ph
	}
	ph.LastSeen = time.Now()
	ph.FailCount = 0
	ph.IsHealthy = true
}

// RecordFailure increments the failure count for a peer. If the count reaches
// maxFails the peer is marked unhealthy.
func (h *HealthAwareRing) RecordFailure(peer string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ph, ok := h.health[peer]
	if !ok {
		ph = &PeerHealth{IsHealthy: true}
		h.health[peer] = ph
	}
	ph.FailCount++
	if ph.FailCount >= h.maxFails {
		ph.IsHealthy = false
	}
}

// IsHealthy returns whether a peer is considered healthy.
// Unknown peers (not yet tracked) are assumed healthy.
func (h *HealthAwareRing) IsHealthy(peer string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ph, ok := h.health[peer]
	if !ok {
		return true // unknown peers assumed healthy
	}
	return ph.IsHealthy
}

// HealthyPeerCount returns the number of tracked peers that are currently healthy.
func (h *HealthAwareRing) HealthyPeerCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for _, ph := range h.health {
		if ph.IsHealthy {
			count++
		}
	}
	return count
}

// GetHealth returns a copy of the PeerHealth for a peer, or nil if untracked.
func (h *HealthAwareRing) GetHealth(peer string) *PeerHealth {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ph, ok := h.health[peer]
	if !ok {
		return nil
	}
	cp := *ph
	return &cp
}
