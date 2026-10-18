package main

// The compaction ring is the list of writer pods exactly as discovery returns
// it: hostnames of a StatefulSet (SRV records), IPs (A records), dashed-IP names.
// Every pod listens on ":9428" and has to find its own entry in that list.
// A pod that does not find itself used to hash as an extra member next to its
// own entry, so a share of the partitions had no owner and another share was
// owned by every writer at once (compacted several times, random output keys,
// rows duplicated).

import (
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
)

// ringMembers returns the members discovery lists for w writers in one of the
// address forms a cluster can produce.
func ringMembers(form string, w int) []string {
	out := make([]string, w)
	for i := range out {
		out[i] = ringMember(form, i)
	}
	return out
}

func ringMember(form string, i int) string {
	switch form {
	case "sts-srv":
		return fmt.Sprintf("lh-logs-insert-%d.lh-logs-insert-headless.ns.svc.cluster.local:9428", i)
	case "ip":
		return fmt.Sprintf("10.0.1.%d:9428", i+10)
	case "dashed-srv":
		return fmt.Sprintf("10-0-1-%d.lh-logs-insert-headless.ns.svc.cluster.local:9428", i+10)
	case "ipv6":
		return fmt.Sprintf("[fd00::%x]:9428", i+10)
	case "dashed-srv-v6":
		return fmt.Sprintf("fd00--%x.lh-logs-insert-headless.ns.svc.cluster.local:9428", i+10)
	}
	panic("unknown form " + form)
}

// ringForms are the address forms the ring test runs over.
var ringForms = []string{"sts-srv", "ip", "dashed-srv", "ipv6", "dashed-srv-v6"}

// podEnv is what writer i of the form knows about itself: the pod's address and
// its StatefulSet hostname. Every pod has its own, which is the point.
func podEnv(form string, i int) selfaddr.Env {
	ip := netip.MustParseAddr(fmt.Sprintf("10.0.1.%d", i+10))
	if form == "ipv6" || form == "dashed-srv-v6" {
		ip = netip.MustParseAddr(fmt.Sprintf("fd00::%x", i+10))
	}
	return selfaddr.Env{
		Hostnames: func() []string { return []string{fmt.Sprintf("lh-logs-insert-%d", i)} },
		IPs:       func() []netip.Addr { return []netip.Addr{ip} },
	}
}

// ringPartitions is 8 tenants x 84 days.
func ringPartitions() []string {
	var out []string
	for tenant := 0; tenant < 8; tenant++ {
		for d := 0; d < 84; d++ {
			out = append(out, fmt.Sprintf("%d/0/logs/dt=2026-%02d-%02d/hour=00", tenant, d/28+1, d%28+1))
		}
	}
	return out
}

// ownerCounts is, per partition, how many of the pods own it.
func ownerCounts(parts []string, pods []func(string) bool) []int {
	owners := make([]int, len(parts))
	for _, owns := range pods {
		for j, p := range parts {
			if owns(p) {
				owners[j]++
			}
		}
	}
	return owners
}

func shares(owners []int) (ownerless, multi float64) {
	for _, n := range owners {
		switch {
		case n == 0:
			ownerless++
		case n > 1:
			multi++
		}
	}
	return ownerless / float64(len(owners)), multi / float64(len(owners))
}

// ringOwnerShares counts, per partition, how many of the w writers own it, each
// writer being a pod with its own address that listens on ":9428".
func ringOwnerShares(form string, w int) (ownerless, multi float64) {
	members := ringMembers(form, w)
	var pods []func(string) bool
	for i := 0; i < w; i++ {
		own := newCompactionOwnership(":9428", nil, func() []string { return members }, podEnv(form, i))
		pods = append(pods, own.OwnsPartition)
	}
	return shares(ownerCounts(ringPartitions(), pods))
}

func TestOwnership_EveryForm_ExactlyOneOwner(t *testing.T) {
	for _, form := range ringForms {
		for _, w := range []int{1, 2, 3, 8} {
			ownerless, multi := ringOwnerShares(form, w)
			t.Logf("%s W=%d: ownerless %.1f%%, owned by several writers %.1f%%", form, w, 100*ownerless, 100*multi)
			if ownerless != 0 || multi != 0 {
				t.Errorf("%s W=%d: %.1f%% of 672 partitions have no owner and %.1f%% are owned by several writers, want 0 and 0",
					form, w, 100*ownerless, 100*multi)
			}
		}
	}
	checkZonesExactlyOneOwner(t)
}

// zonedOwnerShares is ringOwnerShares for writers that run the peer cache with
// availability zones, the way a balanced profile does (peer.az_aware): azs zones
// of perAZ writers each, every pod with its own PeerCache. Compaction does not
// care where a pod runs, so a zone must not shrink the ring a pod hashes over:
// a partition has one owner among all the writers, not one per zone.
func zonedOwnerShares(t *testing.T, azs, perAZ int, azAware bool) (ownerless, multi float64) {
	t.Helper()
	w := azs * perAZ
	members := ringMembers("sts-srv", w)
	zones := map[string]string{}
	for i, m := range members {
		zones[m] = fmt.Sprintf("az-%d", i/perAZ)
	}
	var pods []func(string) bool
	for i := 0; i < w; i++ {
		pc := peercache.NewWithStabilize(":9428", "k", time.Second, 1, time.Millisecond)
		pc.SetSelfEnv(podEnv("sts-srv", i))
		if azAware {
			pc.UpdatePeersWithZones(zones, fmt.Sprintf("az-%d", i/perAZ))
		} else {
			pc.UpdatePeers(members)
		}
		own := newCompactionOwnership(":9428", pc, func() []string { return members }, podEnv("sts-srv", i))
		pods = append(pods, own.OwnsPartition)
	}
	time.Sleep(20 * time.Millisecond) // the ring's stabilization window
	return shares(ownerCounts(ringPartitions(), pods))
}

// checkZonesExactlyOneOwner: zones do not change who owns a partition: 2 zones x 2 writers and 3 zones x 1
// writer (the shape of the shipped cluster compose), AZ-aware or not, still give
// every partition exactly one owner.
func checkZonesExactlyOneOwner(t *testing.T) {
	for _, tc := range []struct {
		name       string
		azs, perAZ int
		azAware    bool
	}{
		{"2 zones x 2 writers, az_aware", 2, 2, true},
		{"2 zones x 2 writers, not az_aware", 2, 2, false},
		{"3 zones x 1 writer, az_aware", 3, 1, true},
		{"3 zones x 1 writer, not az_aware", 3, 1, false},
		{"1 zone x 3 writers, az_aware", 1, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ownerless, multi := zonedOwnerShares(t, tc.azs, tc.perAZ, tc.azAware)
			t.Logf("ownerless %.1f%%, owned by several writers %.1f%%", 100*ownerless, 100*multi)
			if ownerless != 0 || multi != 0 {
				t.Errorf("%.1f%% of 672 partitions have no owner and %.1f%% are owned by several writers, want 0 and 0", 100*ownerless, 100*multi)
			}
		})
	}
}

// The listen address is never added to a discovered list: a pod that is not in
// the list owns nothing and says so, where adding ":9428" made it an extra member
// that a share of the partitions hashed to.
func TestNewCompactionOwnership_NeverAppendsListenAddr(t *testing.T) {
	members := ringMembers("sts-srv", 3)
	// A pod of another StatefulSet, or one DNS has not listed yet.
	stranger := selfaddr.Env{
		Hostnames: func() []string { return []string{"lh-logs-insert-9"} },
		IPs:       func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("10.9.9.9")} },
	}
	own := newCompactionOwnership(":9428", nil, func() []string { return members }, stranger)
	for _, p := range ringPartitions() {
		if own.OwnsPartition(p) {
			t.Fatalf("a pod that is not in the discovered ring owns %s; it must own nothing", p)
		}
		for _, o := range own.RankedOwners(p) {
			if o == ":9428" {
				t.Fatalf("the listen address is a member of the ring (ranked owners of %s: %v)", p, own.RankedOwners(p))
			}
		}
	}
	if got := own.SelfResolvedGauge(); got != 0 {
		t.Errorf("lakehouse_ring_self_resolved = %d for a pod that is not in the ring, want 0", got)
	}
	if got := own.SelfInPeersGauge(); got != 0 {
		t.Errorf("lakehouse_ring_self_in_peers = %d for a pod that is not in the ring, want 0 (the old gauge read 1 because the listen address was appended)", got)
	}

	// The same through the peer ring (no writer list).
	pc := peercache.NewWithStabilize(":9428", "k", time.Second, 1, time.Millisecond)
	pc.UpdatePeers(members)
	time.Sleep(15 * time.Millisecond)
	viaPeers := newCompactionOwnership(":9428", pc, nil, stranger)
	for _, p := range ringPartitions() {
		if viaPeers.OwnsPartition(p) {
			t.Fatalf("a pod that is not in the peer ring owns %s", p)
		}
	}
}

// A pod with no discovery at all is alone and owns everything.
func TestNewCompactionOwnership_SinglePodNoDiscovery_OwnsAll(t *testing.T) {
	for name, own := range map[string]func() bool{
		"no writers, no peer cache": func() bool {
			o := newCompactionOwnership(":9428", nil, nil, selfaddr.Env{})
			return ownsAll(o.OwnsPartition)
		},
		"empty writer list and empty peer ring": func() bool {
			pc := peercache.NewWithStabilize(":9428", "k", time.Second, 1, time.Millisecond)
			o := newCompactionOwnership(":9428", pc, func() []string { return nil }, selfaddr.Env{})
			return ownsAll(o.OwnsPartition)
		},
	} {
		if !own() {
			t.Errorf("%s: the only pod does not own every partition", name)
		}
	}
}

func ownsAll(owns func(string) bool) bool {
	for _, p := range ringPartitions() {
		if !owns(p) {
			return false
		}
	}
	return true
}

// A writer restarts and gets a new IP. While the other pods already list the new
// address and the restarted pod has not re-read its own, nobody may own a
// partition twice (the restarted pod owns nothing); once its address is re-read
// (the Env is cached for a minute) every partition has exactly one owner again.
func TestOwnership_RollingRestart_NewIP_ReResolves(t *testing.T) {
	const w = 3
	clock := time.Now()
	var mu sync.Mutex
	ips := []string{"10.0.1.10", "10.0.1.11", "10.0.1.12"} // what discovery lists
	podIP := make([]string, w)                             // what each pod believes its address is
	copy(podIP, ips)
	list := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, w)
		for i := range out {
			out[i] = ips[i] + ":9428"
		}
		return out
	}
	var pods []func(string) bool
	for i := 0; i < w; i++ {
		i := i
		env := selfaddr.Env{
			Now: func() time.Time { mu.Lock(); defer mu.Unlock(); return clock },
			IPs: func() []netip.Addr {
				mu.Lock()
				defer mu.Unlock()
				return []netip.Addr{netip.MustParseAddr(podIP[i])}
			},
		}
		pods = append(pods, newCompactionOwnership(":9428", nil, list, env).OwnsPartition)
	}
	parts := ringPartitions()
	check := func(stage string, wantOwnerless bool) {
		t.Helper()
		ownerless, multi := shares(ownerCounts(parts, pods))
		if multi != 0 {
			t.Fatalf("%s: %.1f%% of the partitions are owned by several writers, want 0", stage, 100*multi)
		}
		if wantOwnerless != (ownerless > 0) {
			t.Fatalf("%s: ownerless share %.1f%%, want ownerless=%v", stage, 100*ownerless, wantOwnerless)
		}
	}
	check("steady state", false)

	// Pod 1 restarts: the list has its new address, the pod has not noticed yet.
	mu.Lock()
	ips[1] = "10.0.1.99"
	clock = clock.Add(time.Second)
	mu.Unlock()
	check("pod 1 restarted, its Env still has the old address", true)

	mu.Lock()
	podIP[1] = "10.0.1.99"
	clock = clock.Add(time.Minute)
	mu.Unlock()
	check("pod 1 re-read its address", false)
}
