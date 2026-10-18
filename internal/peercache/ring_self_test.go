package peercache

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
)

func podEnv(host, ip string) selfaddr.Env {
	return selfaddr.Env{
		Hostnames: func() []string { return []string{host} },
		IPs:       func() []netip.Addr { return []netip.Addr{netip.MustParseAddr(ip)} },
	}
}

func ringKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	return keys
}

// A pod listens on ":9428" and discovery lists it as a StatefulSet hostname: the
// ring recognises the keys it owns itself (no HTTP fetch from itself), and the
// share of the keys it owns is what its position in the ring says.
func TestPeercacheRing_IsSelf_Hostname(t *testing.T) {
	members := []string{
		"lh-0.lh-headless.ns.svc.cluster.local:9428",
		"lh-1.lh-headless.ns.svc.cluster.local:9428",
		"lh-2.lh-headless.ns.svc.cluster.local:9428",
	}
	local := 0
	for i := range members {
		r := NewRing(":9428", 0)
		r.SetSelfEnv(podEnv(fmt.Sprintf("lh-%d", i), fmt.Sprintf("10.0.0.%d", i+1)))
		r.Set(members)
		for _, k := range ringKeys(300) {
			peer, isLocal := r.Lookup(k)
			if isLocal != (peer == members[i]) {
				t.Fatalf("pod %d: Lookup(%q) = (%q, %v), isLocal must be true exactly for its own member", i, k, peer, isLocal)
			}
			if isLocal {
				local++
			}
		}
	}
	// Across the three pods every key is local to exactly one.
	if local != 300 {
		t.Errorf("%d key lookups were local across 3 pods for 300 keys, want 300 (each key owned by exactly one)", local)
	}
}

func TestPeercacheRing_IsSelf_IPAndZones(t *testing.T) {
	members := []string{"10.0.0.1:9428", "10.0.0.2:9428", "10.0.0.3:9428"}
	zones := map[string]string{members[0]: "a", members[1]: "b", members[2]: "a"}
	r := NewRing(":9428", 0)
	r.SetSelfEnv(podEnv("x", "10.0.0.3"))
	r.SetWithZones(zones, "a")
	sawLocal, sawLocalAZ := false, false
	for _, k := range ringKeys(300) {
		if peer, isLocal := r.Lookup(k); isLocal != (peer == members[2]) {
			t.Fatalf("Lookup(%q) = (%q, %v)", k, peer, isLocal)
		} else if isLocal {
			sawLocal = true
		}
		if peer, isLocal, _ := r.LookupAZ(k); isLocal != (peer == members[2]) {
			t.Fatalf("LookupAZ(%q) = (%q, %v)", k, peer, isLocal)
		} else if isLocal {
			sawLocalAZ = true
		}
	}
	if !sawLocal || !sawLocalAZ {
		t.Error("the pod never recognised a key as its own (the test proves nothing)")
	}
	// Without zone info the same path.
	r2 := NewRing(":9428", 0)
	r2.SetSelfEnv(podEnv("x", "10.0.0.2"))
	r2.SetWithZones(map[string]string{members[0]: "", members[1]: ""}, "")
	for _, k := range ringKeys(100) {
		if peer, isLocal, _ := r2.LookupAZ(k); isLocal != (peer == members[1]) {
			t.Fatalf("LookupAZ without zones (%q) = (%q, %v)", k, peer, isLocal)
		}
	}
}

// A pod that is not found keeps asking its peers (slower, right) and reports it.
func TestPeercacheRing_SelfUnresolved_NeverLocal(t *testing.T) {
	r := NewRing(":9428", 0)
	r.SetSelfEnv(podEnv("other", "10.0.0.9"))
	r.Set([]string{"10.0.0.1:9428", "10.0.0.2:9428"})
	for _, k := range ringKeys(100) {
		if peer, isLocal := r.Lookup(k); isLocal || peer == ":9428" {
			t.Fatalf("Lookup(%q) = (%q, %v), want a peer and not local", k, peer, isLocal)
		}
	}
	if got := metrics.RingSelfResolved.Get("peercache"); got != 0 {
		t.Errorf("lakehouse_ring_self_resolved{ring=peercache} = %d, want 0", got)
	}
	if got := metrics.RingSelfInPeers.Get("peercache"); got != 0 {
		t.Errorf("lakehouse_ring_self_in_peers{ring=peercache} = %d, want 0", got)
	}

	// Found now: the gauges follow.
	r.SetSelfEnv(podEnv("other", "10.0.0.2"))
	r.Set([]string{"10.0.0.1:9428", "10.0.0.2:9428"})
	if got := metrics.RingSelfResolved.Get("peercache"); got != 1 {
		t.Errorf("resolved = %d after the pod was found, want 1", got)
	}
	if got := metrics.RingSelfInPeers.Get("peercache"); got != 1 {
		t.Errorf("in_peers = %d after the pod was found, want 1", got)
	}
}

// An empty ring is a pod alone: every key is local, as before.
func TestPeercacheRing_EmptyRing_IsSelf(t *testing.T) {
	r := NewRing(":9428", 0)
	r.Set(nil)
	if peer, isLocal := r.Lookup("k"); peer != ":9428" || !isLocal {
		t.Errorf("Lookup on an empty ring = (%q, %v)", peer, isLocal)
	}
}
