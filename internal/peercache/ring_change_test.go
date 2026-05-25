package peercache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRingChangeManager_DetectChanges_Join(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)
	events := m.detectChanges(
		[]string{"a:1", "b:1"},
		[]string{"a:1", "b:1", "c:1"},
	)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.Type != RingChangeJoin {
		t.Errorf("type = %s, want join", ev.Type)
	}
	if ev.Peer != "c:1" {
		t.Errorf("peer = %s, want c:1", ev.Peer)
	}
	if ev.OldMemberCount != 2 {
		t.Errorf("old_count = %d, want 2", ev.OldMemberCount)
	}
	if ev.NewMemberCount != 3 {
		t.Errorf("new_count = %d, want 3", ev.NewMemberCount)
	}
}

func TestRingChangeManager_DetectChanges_Leave(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)
	events := m.detectChanges(
		[]string{"a:1", "b:1", "c:1"},
		[]string{"a:1", "c:1"},
	)

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.Type != RingChangeLeave {
		t.Errorf("type = %s, want leave", ev.Type)
	}
	if ev.Peer != "b:1" {
		t.Errorf("peer = %s, want b:1", ev.Peer)
	}
	if ev.OldMemberCount != 3 {
		t.Errorf("old_count = %d, want 3", ev.OldMemberCount)
	}
	if ev.NewMemberCount != 2 {
		t.Errorf("new_count = %d, want 2", ev.NewMemberCount)
	}
}

func TestRingChangeManager_DetectChanges_JoinAndLeave(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)
	events := m.detectChanges(
		[]string{"a:1", "b:1"},
		[]string{"a:1", "c:1"},
	)

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// First event should be the join (c:1), second the leave (b:1)
	if events[0].Type != RingChangeJoin || events[0].Peer != "c:1" {
		t.Errorf("event[0] = %s %s, want join c:1", events[0].Type, events[0].Peer)
	}
	if events[1].Type != RingChangeLeave || events[1].Peer != "b:1" {
		t.Errorf("event[1] = %s %s, want leave b:1", events[1].Type, events[1].Peer)
	}
}

func TestRingChangeManager_DetectChanges_NoChange(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)
	events := m.detectChanges(
		[]string{"a:1", "b:1"},
		[]string{"a:1", "b:1"},
	)

	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}

func TestRingChangeManager_DetectChanges_EmptyToPopulated(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)
	events := m.detectChanges(
		nil,
		[]string{"a:1", "b:1"},
	)

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	for _, ev := range events {
		if ev.Type != RingChangeJoin {
			t.Errorf("expected join event, got %s", ev.Type)
		}
	}
}

func TestRingChangeManager_DetectChanges_PopulatedToEmpty(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)
	events := m.detectChanges(
		[]string{"a:1", "b:1"},
		nil,
	)

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	for _, ev := range events {
		if ev.Type != RingChangeLeave {
			t.Errorf("expected leave event, got %s", ev.Type)
		}
	}
}

func TestRingChangeManager_Subscribers(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)

	var mu sync.Mutex
	var received []RingChangeEvent
	m.OnRingChange(func(ev RingChangeEvent) {
		mu.Lock()
		received = append(received, ev)
		mu.Unlock()
	})

	events := m.detectChanges(
		[]string{"a:1"},
		[]string{"a:1", "b:1"},
	)
	m.processChanges(events, []string{"a:1"}, 2)

	// Wait for async notification
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(received))
	}
	if received[0].Type != RingChangeJoin {
		t.Errorf("type = %s, want join", received[0].Type)
	}
	if received[0].Peer != "b:1" {
		t.Errorf("peer = %s, want b:1", received[0].Peer)
	}
}

func TestRingChangeManager_MultipleSubscribers(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)

	var count1, count2 atomic.Int32
	m.OnRingChange(func(ev RingChangeEvent) {
		count1.Add(1)
	})
	m.OnRingChange(func(ev RingChangeEvent) {
		count2.Add(1)
	})

	events := m.detectChanges(
		[]string{"a:1"},
		[]string{"a:1", "b:1", "c:1"},
	)
	m.processChanges(events, []string{"a:1"}, 3)

	time.Sleep(50 * time.Millisecond)

	if count1.Load() != 2 {
		t.Errorf("subscriber1 got %d events, want 2", count1.Load())
	}
	if count2.Load() != 2 {
		t.Errorf("subscriber2 got %d events, want 2", count2.Load())
	}
}

func TestRingChangeManager_Stabilization(t *testing.T) {
	m := newRingChangeManager(200 * time.Millisecond)

	oldMembers := []string{"a:1", "b:1", "c:1"}
	events := m.detectChanges(oldMembers, []string{"a:1", "b:1"})
	m.processChanges(events, oldMembers, 2)

	// During stabilization, old members should be in shadow set
	if !m.IsStabilizing() {
		t.Error("expected stabilization to be active")
	}
	if !m.IsShadowMember("c:1") {
		t.Error("c:1 should be a shadow member")
	}
	if !m.IsShadowMember("a:1") {
		t.Error("a:1 should be a shadow member (was in old set)")
	}

	shadow := m.ShadowMembers()
	if len(shadow) != 3 {
		t.Errorf("shadow members = %d, want 3", len(shadow))
	}

	// After stabilization expires, shadow should clear
	time.Sleep(300 * time.Millisecond)
	if m.IsStabilizing() {
		t.Error("stabilization should have expired")
	}
	if m.IsShadowMember("c:1") {
		t.Error("c:1 should not be a shadow member after stabilization ends")
	}
	shadow = m.ShadowMembers()
	if len(shadow) != 0 {
		t.Errorf("shadow members = %d, want 0 after stabilization", len(shadow))
	}
}

func TestRingChangeManager_StabilizationZeroDuration(t *testing.T) {
	m := newRingChangeManager(0)

	events := m.detectChanges(
		[]string{"a:1", "b:1"},
		[]string{"a:1"},
	)
	m.processChanges(events, []string{"a:1", "b:1"}, 1)

	// With zero duration, stabilization should still be set (expiry is now+0 = now)
	// but shadow should be empty since it expires immediately
	time.Sleep(10 * time.Millisecond)
	if m.IsStabilizing() {
		t.Error("should not be stabilizing with zero duration")
	}
}

func TestRingChangeManager_Draining(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)

	if m.IsDraining("peer:1") {
		t.Error("peer:1 should not be draining initially")
	}

	m.RecordDraining("peer:1")

	if !m.IsDraining("peer:1") {
		t.Error("peer:1 should be draining after RecordDraining")
	}
	if m.IsDraining("peer:2") {
		t.Error("peer:2 should not be draining")
	}

	draining := m.DrainingPeers()
	if len(draining) != 1 {
		t.Fatalf("draining peers = %d, want 1", len(draining))
	}
	if _, ok := draining["peer:1"]; !ok {
		t.Error("peer:1 should be in draining peers map")
	}
}

// --- PeerCache integration tests ---

func TestPeerCache_UpdatePeers_ChangeDetection(t *testing.T) {
	pc := NewWithStabilize("self:9428", "", 5*time.Second, 10, 200*time.Millisecond)

	var mu sync.Mutex
	var events []RingChangeEvent
	pc.OnRingChange(func(ev RingChangeEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})

	// Initial population
	pc.UpdatePeers([]string{"a:1", "b:1"})
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(events) != 2 {
		t.Fatalf("expected 2 join events, got %d", len(events))
	}
	for _, ev := range events {
		if ev.Type != RingChangeJoin {
			t.Errorf("expected join, got %s", ev.Type)
		}
	}
	events = nil
	mu.Unlock()

	// Wait for stabilization to clear
	time.Sleep(250 * time.Millisecond)

	// Add a peer
	pc.UpdatePeers([]string{"a:1", "b:1", "c:1"})
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(events) != 1 {
		t.Fatalf("expected 1 join event, got %d", len(events))
	}
	if events[0].Peer != "c:1" {
		t.Errorf("peer = %s, want c:1", events[0].Peer)
	}
	events = nil
	mu.Unlock()

	// Wait for stabilization to clear
	time.Sleep(250 * time.Millisecond)

	// Remove a peer
	pc.UpdatePeers([]string{"a:1", "c:1"})
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(events) != 1 {
		t.Fatalf("expected 1 leave event, got %d", len(events))
	}
	if events[0].Type != RingChangeLeave {
		t.Errorf("type = %s, want leave", events[0].Type)
	}
	if events[0].Peer != "b:1" {
		t.Errorf("peer = %s, want b:1", events[0].Peer)
	}
	mu.Unlock()
}

func TestPeerCache_UpdatePeers_NoChangeNoEvent(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	var called atomic.Int32
	pc.OnRingChange(func(ev RingChangeEvent) {
		called.Add(1)
	})

	pc.UpdatePeers([]string{"a:1", "b:1"})
	time.Sleep(50 * time.Millisecond)

	// Same set again should not trigger events
	called.Store(0)
	pc.UpdatePeers([]string{"a:1", "b:1"})
	time.Sleep(50 * time.Millisecond)

	if called.Load() != 0 {
		t.Errorf("expected 0 events for no-change update, got %d", called.Load())
	}
}

func TestPeerCache_UpdatePeersWithZones_ChangeDetection(t *testing.T) {
	pc := NewWithStabilize("self:9428", "", 5*time.Second, 10, 200*time.Millisecond)

	var mu sync.Mutex
	var events []RingChangeEvent
	pc.OnRingChange(func(ev RingChangeEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})

	// Initial population with zones
	pc.UpdatePeersWithZones(map[string]string{
		"a:1": "us-east-1a",
		"b:1": "us-east-1b",
	}, "us-east-1a")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(events) != 2 {
		t.Fatalf("expected 2 join events, got %d", len(events))
	}
	events = nil
	mu.Unlock()

	// Wait for stabilization
	time.Sleep(250 * time.Millisecond)

	// Add a peer in a new zone
	pc.UpdatePeersWithZones(map[string]string{
		"a:1": "us-east-1a",
		"b:1": "us-east-1b",
		"c:1": "us-east-1c",
	}, "us-east-1a")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if len(events) != 1 {
		t.Fatalf("expected 1 join event, got %d", len(events))
	}
	if events[0].Peer != "c:1" {
		t.Errorf("peer = %s, want c:1", events[0].Peer)
	}
	mu.Unlock()
}

func TestPeerCache_Stabilization_ShadowMembers(t *testing.T) {
	pc := NewWithStabilize("self:9428", "", 5*time.Second, 10, 300*time.Millisecond)

	// Set initial peers
	pc.UpdatePeers([]string{"a:1", "b:1", "c:1"})
	// Wait for first stabilization to clear
	time.Sleep(350 * time.Millisecond)

	// Remove c:1 -- triggers new stabilization
	pc.UpdatePeers([]string{"a:1", "b:1"})

	if !pc.IsStabilizing() {
		t.Error("expected stabilization to be active")
	}

	// c:1 should still be in shadow
	if !pc.IsShadowMember("c:1") {
		t.Error("c:1 should be a shadow member during stabilization")
	}

	shadow := pc.ShadowMembers()
	sort.Strings(shadow)
	expected := []string{"a:1", "b:1", "c:1"}
	if len(shadow) != len(expected) {
		t.Fatalf("shadow = %v, want %v", shadow, expected)
	}
	for i := range expected {
		if shadow[i] != expected[i] {
			t.Errorf("shadow[%d] = %s, want %s", i, shadow[i], expected[i])
		}
	}

	// After stabilization period, shadow should be empty
	time.Sleep(350 * time.Millisecond)
	if pc.IsStabilizing() {
		t.Error("stabilization should have ended")
	}
	shadow = pc.ShadowMembers()
	if len(shadow) != 0 {
		t.Errorf("shadow after stabilization = %v, want empty", shadow)
	}
}

func TestPeerCache_Fetch_DrainingHeader(t *testing.T) {
	// Server that responds with X-Lakehouse-Draining header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Lakehouse-Draining", "true")
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	pc := New("self:9428", "", 5*time.Second, 10)
	peerAddr := srv.Listener.Addr().String()

	if pc.IsDraining(peerAddr) {
		t.Error("peer should not be draining initially")
	}

	// Fetch -- the 404 will be a miss, but the draining header should be detected
	_, _, _ = pc.Fetch(context.Background(), peerAddr, "test-key")

	if !pc.IsDraining(peerAddr) {
		t.Error("peer should be draining after receiving X-Lakehouse-Draining header")
	}

	draining := pc.DrainingPeers()
	if len(draining) != 1 {
		t.Fatalf("draining peers = %d, want 1", len(draining))
	}
	if _, ok := draining[peerAddr]; !ok {
		t.Errorf("expected %s in draining peers", peerAddr)
	}
}

func TestPeerCache_Fetch_NoDrainingHeader(t *testing.T) {
	handler := NewHandler("", "")
	handler.Put("k", []byte("v"))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	pc := New("self:9428", "", 5*time.Second, 10)
	peerAddr := srv.Listener.Addr().String()

	_, _, err := pc.Fetch(context.Background(), peerAddr, "k")
	if err != nil {
		t.Fatal(err)
	}

	if pc.IsDraining(peerAddr) {
		t.Error("peer should not be draining without X-Lakehouse-Draining header")
	}
}

func TestPeerCache_Fetch_DrainingHeaderOnSuccess(t *testing.T) {
	// Server returns data but also signals draining
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Lakehouse-Draining", "graceful")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	}))
	defer srv.Close()

	pc := New("self:9428", "", 5*time.Second, 10)
	peerAddr := srv.Listener.Addr().String()

	data, found, err := pc.Fetch(context.Background(), peerAddr, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("expected found=true")
	}
	if string(data) != "data" {
		t.Errorf("data = %q, want data", data)
	}
	if !pc.IsDraining(peerAddr) {
		t.Error("peer should be draining")
	}
}

func TestRingChangeEvent_Timestamp(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)
	fixedTime := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	m.nowFunc = func() time.Time { return fixedTime }

	events := m.detectChanges(nil, []string{"a:1"})
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !events[0].Timestamp.Equal(fixedTime) {
		t.Errorf("timestamp = %v, want %v", events[0].Timestamp, fixedTime)
	}
}

func TestRingChangeManager_ConcurrentAccess(t *testing.T) {
	m := newRingChangeManager(100 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(3)

	// Concurrent subscriber registration
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			m.OnRingChange(func(ev RingChangeEvent) {})
		}
	}()

	// Concurrent change detection
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			events := m.detectChanges(
				[]string{"a:1"},
				[]string{"a:1", "b:1"},
			)
			m.processChanges(events, []string{"a:1"}, 2)
		}
	}()

	// Concurrent reads
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_ = m.IsStabilizing()
			_ = m.IsShadowMember("a:1")
			_ = m.ShadowMembers()
			_ = m.IsDraining("a:1")
			_ = m.DrainingPeers()
		}
	}()

	wg.Wait()
}

func TestPeerCache_MultipleJoinsLeavesInOneUpdate(t *testing.T) {
	pc := NewWithStabilize("self:9428", "", 5*time.Second, 10, 200*time.Millisecond)

	var mu sync.Mutex
	var events []RingChangeEvent
	pc.OnRingChange(func(ev RingChangeEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})

	// Set initial peers
	pc.UpdatePeers([]string{"a:1", "b:1", "c:1"})
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	events = nil
	mu.Unlock()
	time.Sleep(250 * time.Millisecond)

	// Swap b:1 and c:1 for d:1 and e:1
	pc.UpdatePeers([]string{"a:1", "d:1", "e:1"})
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	joins := 0
	leaves := 0
	for _, ev := range events {
		switch ev.Type {
		case RingChangeJoin:
			joins++
		case RingChangeLeave:
			leaves++
		}
	}
	if joins != 2 {
		t.Errorf("joins = %d, want 2", joins)
	}
	if leaves != 2 {
		t.Errorf("leaves = %d, want 2", leaves)
	}
}

func TestRingChangeManager_DrainingPeersReturnsCopy(t *testing.T) {
	m := newRingChangeManager(60 * time.Second)
	m.RecordDraining("peer:1")

	draining := m.DrainingPeers()
	draining["peer:2"] = time.Now()

	// Original should not be affected
	if m.IsDraining("peer:2") {
		t.Error("modifying returned map should not affect internal state")
	}
}

func TestRingChangeManager_ShadowMembersReturnsCopy(t *testing.T) {
	m := newRingChangeManager(5 * time.Second)
	events := m.detectChanges([]string{"a:1", "b:1"}, []string{"a:1"})
	m.processChanges(events, []string{"a:1", "b:1"}, 1)

	shadow := m.ShadowMembers()
	if len(shadow) == 0 {
		t.Fatal("expected shadow members")
	}

	// Modify returned slice -- should not affect internal state
	shadow[0] = "modified"
	shadow2 := m.ShadowMembers()
	for _, s := range shadow2 {
		if s == "modified" {
			t.Error("modifying returned slice should not affect internal state")
		}
	}
}
