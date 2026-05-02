package peercache

import (
	"fmt"
	"testing"
)

func TestRing_ZeroVnodes(t *testing.T) {
	r := NewRing("self:9428", 0)
	if r.vnodes != defaultVnodes {
		t.Errorf("vnodes = %d, want default %d", r.vnodes, defaultVnodes)
	}
}

func TestRing_NegativeVnodes(t *testing.T) {
	r := NewRing("self:9428", -10)
	if r.vnodes != defaultVnodes {
		t.Errorf("vnodes = %d, want default %d", r.vnodes, defaultVnodes)
	}
}

func TestRing_SetEmpty(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"a:9428", "b:9428"})
	r.Set(nil)

	if r.MemberCount() != 0 {
		t.Errorf("after Set(nil), members = %d, want 0", r.MemberCount())
	}

	peer, isLocal := r.Lookup("any-key")
	if peer != "self:9428" || !isLocal {
		t.Errorf("empty ring Lookup = (%q, %v), want (self:9428, true)", peer, isLocal)
	}
}

func TestRing_SetDuplicatePeers(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"a:9428", "a:9428", "b:9428"})

	if r.MemberCount() != 2 {
		t.Errorf("members = %d, want 2 (deduped)", r.MemberCount())
	}
}

func TestRing_LookupConsistency_1000Keys(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "a:9428", "b:9428", "c:9428"})

	results := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		peer, _ := r.Lookup(key)
		results[key] = peer
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		peer, _ := r.Lookup(key)
		if peer != results[key] {
			t.Errorf("Lookup(%q) changed: %q -> %q", key, results[key], peer)
		}
	}
}

func TestRing_AddPeerMinimalDisruption(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "a:9428", "b:9428"})

	before := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		peer, _ := r.Lookup(key)
		before[key] = peer
	}

	r.Set([]string{"self:9428", "a:9428", "b:9428", "c:9428"})

	changed := 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		peer, _ := r.Lookup(key)
		if peer != before[key] {
			changed++
		}
	}

	changeRate := float64(changed) / 1000.0
	if changeRate > 0.5 {
		t.Errorf("adding 1 peer moved %.1f%% of keys (want <50%%)", changeRate*100)
	}
}

func TestRing_RemovePeerMinimalDisruption(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "a:9428", "b:9428", "c:9428"})

	before := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		peer, _ := r.Lookup(key)
		before[key] = peer
	}

	r.Set([]string{"self:9428", "a:9428", "b:9428"})

	changed := 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		peer, _ := r.Lookup(key)
		if peer != before[key] {
			changed++
		}
	}

	changeRate := float64(changed) / 1000.0
	if changeRate > 0.5 {
		t.Errorf("removing 1 peer moved %.1f%% of keys (want <50%%)", changeRate*100)
	}
}

func TestRing_LargeKeySpace(t *testing.T) {
	r := NewRing("self:9428", 150)
	peers := make([]string, 10)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer%d:9428", i)
	}
	r.Set(peers)

	dist := make(map[string]int)
	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		peer, _ := r.Lookup(fmt.Sprintf("key-%d", i))
		dist[peer]++
	}

	for peer, count := range dist {
		pct := float64(count) / float64(numKeys) * 100
		if pct < 3 || pct > 25 {
			t.Errorf("peer %q has %.1f%% of keys (want 3-25%%)", peer, pct)
		}
	}
}

func TestRing_SelfAddr_IsLocal(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428"})

	for i := 0; i < 100; i++ {
		peer, isLocal := r.Lookup(fmt.Sprintf("key-%d", i))
		if !isLocal {
			t.Errorf("Lookup(%q) isLocal=false with only self in ring", fmt.Sprintf("key-%d", i))
		}
		if peer != "self:9428" {
			t.Errorf("Lookup returned %q, want self:9428", peer)
		}
	}
}
