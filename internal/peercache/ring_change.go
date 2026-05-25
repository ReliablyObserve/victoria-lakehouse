package peercache

import (
	"sort"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// RingChangeType indicates whether a peer joined or left the ring.
type RingChangeType string

const (
	RingChangeJoin  RingChangeType = "join"
	RingChangeLeave RingChangeType = "leave"
)

// RingChangeEvent describes a single membership change in the hash ring.
type RingChangeEvent struct {
	Type           RingChangeType
	Peer           string
	Timestamp      time.Time
	OldMemberCount int
	NewMemberCount int
}

// ringChangeManager handles subscriber notification, shadow ring stabilization,
// and draining peer tracking for PeerCache.
type ringChangeManager struct {
	mu                sync.RWMutex
	subscribers       []func(RingChangeEvent)
	shadowMembers     map[string]bool
	stabilizeExpiry   time.Time
	stabilizeDuration time.Duration
	drainingPeers     map[string]time.Time
	stabilizeActive   bool // tracks whether stabilize gauge is set to 1

	// nowFunc allows tests to override time.Now
	nowFunc func() time.Time
}

func newRingChangeManager(stabilizeDuration time.Duration) *ringChangeManager {
	return &ringChangeManager{
		shadowMembers:     make(map[string]bool),
		stabilizeDuration: stabilizeDuration,
		drainingPeers:     make(map[string]time.Time),
		nowFunc:           time.Now,
	}
}

// OnRingChange registers a callback to be invoked when ring membership changes.
// Callbacks are called asynchronously in a goroutine to avoid blocking the refresh loop.
func (m *ringChangeManager) OnRingChange(fn func(RingChangeEvent)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribers = append(m.subscribers, fn)
}

// detectChanges compares old and new member sets and returns events for each change.
func (m *ringChangeManager) detectChanges(oldMembers, newMembers []string) []RingChangeEvent {
	oldSet := make(map[string]bool, len(oldMembers))
	for _, p := range oldMembers {
		oldSet[p] = true
	}
	newSet := make(map[string]bool, len(newMembers))
	for _, p := range newMembers {
		newSet[p] = true
	}

	now := m.now()
	var events []RingChangeEvent

	// Detect joins: in new but not in old
	for _, p := range newMembers {
		if !oldSet[p] {
			events = append(events, RingChangeEvent{
				Type:           RingChangeJoin,
				Peer:           p,
				Timestamp:      now,
				OldMemberCount: len(oldMembers),
				NewMemberCount: len(newMembers),
			})
		}
	}

	// Detect leaves: in old but not in new
	// Sort old members for deterministic event ordering
	sortedOld := make([]string, len(oldMembers))
	copy(sortedOld, oldMembers)
	sort.Strings(sortedOld)
	for _, p := range sortedOld {
		if !newSet[p] {
			events = append(events, RingChangeEvent{
				Type:           RingChangeLeave,
				Peer:           p,
				Timestamp:      now,
				OldMemberCount: len(oldMembers),
				NewMemberCount: len(newMembers),
			})
		}
	}

	return events
}

// processChanges emits metrics, updates shadow ring for stabilization, and notifies subscribers.
func (m *ringChangeManager) processChanges(events []RingChangeEvent, oldMembers []string, newMemberCount int) {
	if len(events) == 0 {
		return
	}

	// Emit per-event metrics
	for _, ev := range events {
		metrics.RingChangeEventsTotal.Inc(string(ev.Type))
	}

	// Update total peers gauge
	metrics.RingPeersTotal.Set(int64(newMemberCount))

	// Enter stabilization period: keep old members as shadow set
	m.mu.Lock()
	m.shadowMembers = make(map[string]bool, len(oldMembers))
	for _, p := range oldMembers {
		m.shadowMembers[p] = true
	}
	m.stabilizeExpiry = m.now().Add(m.stabilizeDuration)
	m.stabilizeActive = true
	m.mu.Unlock()

	// Set stabilization gauge
	metrics.RingStabilizeInProgress.Set(1)

	// Notify subscribers asynchronously
	m.mu.RLock()
	subs := make([]func(RingChangeEvent), len(m.subscribers))
	copy(subs, m.subscribers)
	m.mu.RUnlock()

	if len(subs) > 0 {
		for _, ev := range events {
			ev := ev
			for _, fn := range subs {
				fn := fn
				go fn(ev)
			}
		}
	}

	// Log changes
	for _, ev := range events {
		logger.Infof("ring change: type=%s peer=%s old_count=%d new_count=%d",
			ev.Type, ev.Peer, ev.OldMemberCount, ev.NewMemberCount)
	}
}

// checkExpiry checks if the stabilization period has expired and lazily cleans
// up the shadow set and gauge. Must be called with m.mu held for writing.
func (m *ringChangeManager) checkExpiry() {
	if m.stabilizeActive && !m.stabilizeExpiry.IsZero() && !m.now().Before(m.stabilizeExpiry) {
		m.shadowMembers = make(map[string]bool)
		m.stabilizeExpiry = time.Time{}
		m.stabilizeActive = false
		metrics.RingStabilizeInProgress.Set(0)
	}
}

// IsStabilizing returns true if the ring is currently in a stabilization period.
func (m *ringChangeManager) IsStabilizing() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkExpiry()
	return m.stabilizeActive
}

// IsShadowMember returns true if the given peer was in the ring before the
// most recent change and the stabilization period is still active.
func (m *ringChangeManager) IsShadowMember(peer string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkExpiry()
	if !m.stabilizeActive {
		return false
	}
	return m.shadowMembers[peer]
}

// ShadowMembers returns the current shadow member set. Returns nil outside stabilization.
func (m *ringChangeManager) ShadowMembers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkExpiry()
	if !m.stabilizeActive {
		return nil
	}
	members := make([]string, 0, len(m.shadowMembers))
	for p := range m.shadowMembers {
		members = append(members, p)
	}
	sort.Strings(members)
	return members
}

// RecordDraining records that a peer responded with X-Lakehouse-Draining header.
func (m *ringChangeManager) RecordDraining(peer string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drainingPeers[peer] = m.now()
}

// IsDraining returns true if the peer has been recorded as draining.
func (m *ringChangeManager) IsDraining(peer string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.drainingPeers[peer]
	return ok
}

// DrainingPeers returns a copy of all currently draining peers and their timestamps.
func (m *ringChangeManager) DrainingPeers() map[string]time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make(map[string]time.Time, len(m.drainingPeers))
	for k, v := range m.drainingPeers {
		cp[k] = v
	}
	return cp
}

func (m *ringChangeManager) now() time.Time {
	if m.nowFunc != nil {
		return m.nowFunc()
	}
	return time.Now()
}
