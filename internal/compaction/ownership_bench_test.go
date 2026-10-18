package compaction

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
)

// BenchmarkOwnsPartition is the cost of one ownership decision for a pod in a
// ring of w writers: the scheduler asks once per partition per scan.
//
// "exact" gives the pod's own member as its listen address, the only form main
// could match. "hostname" is the production shape: the pod listens on ":9428",
// discovery lists StatefulSet SRV hostnames, and the pod is found by its
// hostname (main cannot run this case: it never finds itself).
func BenchmarkOwnsPartition(b *testing.B) {
	for _, w := range []int{8, 100} {
		members := make([]string, w)
		for i := range members {
			members[i] = fmt.Sprintf("10.0.%d.%d:9428", i/200, i%200+1)
		}
		r := NewOwnershipResolver(members[w/2], staticPeers(members...))
		b.Run(fmt.Sprintf("W=%d", w), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r.OwnsPartition("0/0/logs/dt=2026-10-01/hour=00")
			}
		})
	}
	for _, w := range []int{8, 100} {
		members := make([]string, w)
		for i := range members {
			members[i] = fmt.Sprintf("lh-logs-insert-%d.lh-logs-insert-headless.ns.svc.cluster.local:9428", i)
		}
		r := NewOwnershipResolver(":9428", staticPeers(members...))
		r.Env = selfaddr.Env{
			Hostnames: func() []string { return []string{fmt.Sprintf("lh-logs-insert-%d", w/2)} },
			IPs:       func() []netip.Addr { return []netip.Addr{netip.MustParseAddr("10.0.0.1")} },
		}
		b.Run(fmt.Sprintf("hostname/W=%d", w), func(b *testing.B) {
			r.OwnsPartition("0/0/logs/dt=2026-10-01/hour=00")
			if r.SelfResolvedGauge() != 1 {
				b.Fatal("the pod did not find itself by its hostname; the benchmark would measure the unresolved path")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.OwnsPartition("0/0/logs/dt=2026-10-01/hour=00")
			}
		})
	}
}
