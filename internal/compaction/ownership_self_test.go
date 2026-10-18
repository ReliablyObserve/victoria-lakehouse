package compaction

import (
	"context"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
)

func selfEnv(host, ip string) selfaddr.Env {
	return selfaddr.Env{
		Hostnames: func() []string { return []string{host} },
		IPs:       func() []netip.Addr { return []netip.Addr{netip.MustParseAddr(ip)} },
	}
}

func selfParts(n int) []string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("0/0/logs/dt=2026-10-%02d/hour=%02d", i%28+1, i%24)
	}
	return parts
}

// The pods of a StatefulSet call themselves ":9428" and discovery lists them by
// hostname: every partition has exactly one owner among them.
func TestOwnership_ListenAddrAndHostnameMembers_OneOwnerEach(t *testing.T) {
	members := []string{
		"lh-0.lh-headless.ns.svc.cluster.local:9428",
		"lh-1.lh-headless.ns.svc.cluster.local:9428",
		"lh-2.lh-headless.ns.svc.cluster.local:9428",
	}
	pods := make([]*OwnershipResolver, len(members))
	for i := range pods {
		pods[i] = NewOwnershipResolver(":9428", staticPeers(members...))
		pods[i].Env = selfEnv(fmt.Sprintf("lh-%d", i), fmt.Sprintf("10.0.0.%d", i+1))
	}
	for _, p := range selfParts(300) {
		owners := 0
		for _, r := range pods {
			if r.OwnsPartition(p) {
				owners++
			}
		}
		if owners != 1 {
			t.Fatalf("partition %s has %d owners, want 1", p, owners)
		}
	}
	for i, r := range pods {
		if r.SelfResolvedGauge() != 1 || r.SelfInPeersGauge() != 1 {
			t.Errorf("pod %d gauges resolved=%d in_peers=%d, want 1 and 1", i, r.SelfResolvedGauge(), r.SelfInPeersGauge())
		}
		if self, ok := r.SelfMember(); !ok || self != members[i] {
			t.Errorf("pod %d SelfMember = (%q, %v), want %q", i, self, ok, members[i])
		}
	}
}

// A pod that cannot find itself in a non-empty ring owns nothing and never
// takes the listen address for a member.
func TestOwnership_SelfUnresolved_OwnsNothing_GaugeZero(t *testing.T) {
	members := []string{"10.0.0.1:9428", "10.0.0.2:9428"}
	for name, env := range map[string]selfaddr.Env{
		"not listed": selfEnv("other", "10.0.0.9"),
		"ambiguous": {
			IPs: func() []netip.Addr {
				return []netip.Addr{netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("10.0.0.2")}
			},
		},
		"nothing known": {},
	} {
		t.Run(name, func(t *testing.T) {
			r := NewOwnershipResolver(":9428", staticPeers(members...))
			r.Env = env
			for _, p := range selfParts(100) {
				if r.OwnsPartition(p) {
					t.Fatalf("an unidentified pod owns %s", p)
				}
				for _, o := range r.RankedOwners(p) {
					if o == ":9428" {
						t.Fatalf("the listen address became a member: %v", r.RankedOwners(p))
					}
				}
			}
			if self, ok := r.SelfMember(); ok || self != "" {
				t.Errorf("SelfMember = (%q, %v), want none", self, ok)
			}
			if r.SelfResolvedGauge() != 0 || r.SelfInPeersGauge() != 0 {
				t.Errorf("gauges resolved=%d in_peers=%d, want 0 and 0", r.SelfResolvedGauge(), r.SelfInPeersGauge())
			}
		})
	}
}

// Single pod, no discovery: the ring is the pod's own listen address and it owns all.
func TestOwnership_SinglePodNoDiscovery_OwnsAll(t *testing.T) {
	r := NewOwnershipResolver(":9428", staticPeers(":9428"))
	for _, p := range selfParts(50) {
		if !r.OwnsPartition(p) {
			t.Fatalf("the only pod does not own %s", p)
		}
	}
	if r.SelfResolvedGauge() != 1 || r.SelfInPeersGauge() != 1 {
		t.Errorf("gauges resolved=%d in_peers=%d, want 1 and 1", r.SelfResolvedGauge(), r.SelfInPeersGauge())
	}
}

// A draining pod is itself (resolved) but is filtered out of the set ownership
// hashes over (in_peers 0), and owns nothing.
func TestOwnership_DrainingSelf_ResolvedButNotInPeers(t *testing.T) {
	members := []string{"10.0.0.1:9428", "10.0.0.2:9428"}
	r := NewOwnershipResolver(":9428", staticPeers(members...))
	r.Env = selfEnv("x", "10.0.0.1")
	r.IsDraining = func(p string) bool { return p == "10.0.0.1:9428" }
	for _, p := range selfParts(50) {
		if r.OwnsPartition(p) {
			t.Fatalf("a draining pod owns %s", p)
		}
	}
	if r.SelfResolvedGauge() != 1 || r.SelfInPeersGauge() != 0 {
		t.Errorf("gauges resolved=%d in_peers=%d, want 1 and 0", r.SelfResolvedGauge(), r.SelfInPeersGauge())
	}
}

// Tier B deletes nothing from a pod that is not in the ring.
func TestOrphanSweep_TierB_UnresolvedSelf_DoesNothing(t *testing.T) {
	pool := newListingPool()
	m := manifest.New("bkt", "logs/")
	ctx := context.Background()
	const partition = "dt=2026-01-01/hour=00"
	key := "logs/" + partition + "/orphan.parquet"
	_ = pool.UploadWithMtime(ctx, key, []byte("x"), time.Now().Add(-10*time.Hour))
	m.RemoveFile(partition, key)
	ttl := 30 * time.Millisecond
	time.Sleep(2 * ttl)

	r := NewOwnershipResolver(":9428", staticPeers("10.0.0.1:9428"))
	r.Env = selfEnv("other", "10.0.0.9")
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute, OrphanTTL: ttl,
	})
	skipped := metrics.CompactionOrphansSkipped.Get("self_not_in_peers")
	if deleted, err := sweep.RunTierB(ctx); err != nil || deleted != 0 {
		t.Fatalf("RunTierB = (%d, %v), want nothing deleted by a pod outside the ring", deleted, err)
	}
	if got := metrics.CompactionOrphansSkipped.Get("self_not_in_peers"); got != skipped+1 {
		t.Errorf("self_not_in_peers skips = %d, want %d: the sweep must stop before it lists the bucket", got, skipped+1)
	}
	// The control: the same sweep by the pod that is the member deletes it.
	r2 := NewOwnershipResolver(":9428", staticPeers("10.0.0.1:9428"))
	r2.Env = selfEnv("other", "10.0.0.1")
	sweep2 := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r2, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute, OrphanTTL: ttl,
	})
	if deleted, err := sweep2.RunTierB(ctx); err != nil || deleted != 1 {
		t.Fatalf("control RunTierB = (%d, %v), want 1 (the test proves nothing otherwise)", deleted, err)
	}
}

// Tier A: the secondary owner, found by IP while listening on ":9428", steals a
// stale partition; a pod outside the ring does not.
func TestOrphanSweep_TierA_SecondaryFoundByIP(t *testing.T) {
	staticRanker := func(s string) uint64 {
		if strings.HasPrefix(s, "10.0.0.1") {
			return 100
		}
		return 50
	}
	for name, tc := range map[string]struct {
		env  selfaddr.Env
		want int
	}{
		"secondary resolved by its ip": {selfEnv("b", "10.0.0.2"), 1},
		"pod outside the ring":         {selfEnv("c", "10.0.0.9"), 0},
	} {
		t.Run(name, func(t *testing.T) {
			pool := newListingPool()
			m := manifest.New("bkt", "logs/")
			const partition = "dt=2026-01-01/hour=00"
			for i := 0; i < 12; i++ {
				fi := makeRealParquet(t, pool, fmt.Sprintf("logs/%s/file-%02d.parquet", partition, i), "fp1", int64(i*1000+1), time.Now())
				m.AddFile(partition, fi)
			}
			m.MarkAttempt(partition, time.Now().Add(-10*time.Minute))
			r := NewOwnershipResolver(":9428", staticPeers("10.0.0.1:9428", "10.0.0.2:9428")).WithHashFunc(staticRanker)
			r.Env = tc.env
			sweep := NewOrphanSweep(OrphanSweepConfig{
				Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0), Lister: pool,
				Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute, RowGroupSize: 1000, CompressionLevel: 1,
				TierAStalenessMultiplier: 3,
			})
			got, err := sweep.RunTierA(context.Background())
			if err != nil || got != tc.want {
				t.Fatalf("RunTierA = (%d, %v), want %d", got, err, tc.want)
			}
		})
	}
}

// The zero Env means the process's own: the pod finds itself by its real
// addresses. (The hostname "localhost" is not in the ring, so it is unresolved.)
func TestOwnership_ZeroEnvIsTheSystemEnv(t *testing.T) {
	r := NewOwnershipResolver(":9428", staticPeers("192.0.2.1:9428"))
	if r.OwnsPartition("0/0/logs/dt=2026-10-01/hour=00") {
		t.Error("a pod owns a partition of a ring that holds only a documentation address")
	}
	if r.SelfResolvedGauge() != 0 {
		t.Errorf("resolved = %d, want 0", r.SelfResolvedGauge())
	}
}
