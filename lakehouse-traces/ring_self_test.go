package main

// The compaction ring is the list of writer pods exactly as discovery returns
// it: hostnames of a StatefulSet (SRV records), IPs (A records), dashed-IP names.
// Every pod listens on ":10428" and has to find its own entry in that list.
// A pod that does not find itself hashes as an extra member next to its own
// entry, so a share of the partitions has no owner and another share is owned
// by every writer at once (compacted several times, random output keys, rows
// duplicated).

import (
	"fmt"
	"testing"
)

// ringMembers returns the members discovery lists for w writers in one of the
// address forms a cluster can produce.
func ringMembers(form string, w int) []string {
	out := make([]string, w)
	for i := range out {
		switch form {
		case "sts-srv":
			out[i] = fmt.Sprintf("lh-traces-insert-%d.lh-traces-insert-headless.ns.svc.cluster.local:10428", i)
		case "ip":
			out[i] = fmt.Sprintf("10.0.1.%d:10428", i+10)
		case "dashed-srv":
			out[i] = fmt.Sprintf("10-0-1-%d.lh-traces-insert-headless.ns.svc.cluster.local:10428", i+10)
		case "ipv6":
			out[i] = fmt.Sprintf("[fd00::%x]:10428", i+10)
		}
	}
	return out
}

// ringPartitions is 8 tenants x 84 days.
func ringPartitions() []string {
	var out []string
	for tenant := 0; tenant < 8; tenant++ {
		for d := 0; d < 84; d++ {
			out = append(out, fmt.Sprintf("%d/0/traces/dt=2026-%02d-%02d/hour=00", tenant, d/28+1, d%28+1))
		}
	}
	return out
}

// ringOwnerShares counts, per partition, how many of the writers own it.
func ringOwnerShares(form string, w int) (ownerless, multi float64) {
	members := ringMembers(form, w)
	parts := ringPartitions()
	owners := make([]int, len(parts))
	for i := 0; i < w; i++ {
		own := newCompactionOwnership(":10428", nil, func() []string { return members })
		for j, p := range parts {
			if own.OwnsPartition(p) {
				owners[j]++
			}
		}
	}
	for _, n := range owners {
		switch {
		case n == 0:
			ownerless++
		case n > 1:
			multi++
		}
	}
	return ownerless / float64(len(parts)), multi / float64(len(parts))
}

func TestOwnership_EveryForm_ExactlyOneOwner(t *testing.T) {
	for _, form := range []string{"sts-srv", "ip", "dashed-srv", "ipv6"} {
		for _, w := range []int{1, 2, 3, 8} {
			ownerless, multi := ringOwnerShares(form, w)
			t.Logf("%s W=%d: ownerless %.1f%%, owned by several writers %.1f%%", form, w, 100*ownerless, 100*multi)
			if ownerless != 0 || multi != 0 {
				t.Errorf("%s W=%d: %.1f%% of 672 partitions have no owner and %.1f%% are owned by several writers, want 0 and 0",
					form, w, 100*ownerless, 100*multi)
			}
		}
	}
}
