package compaction

import (
	"fmt"
	"testing"
)

// BenchmarkOwnsPartition is the cost of one ownership decision for a pod in a
// ring of w writers: the scheduler asks once per partition per scan.
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
}
