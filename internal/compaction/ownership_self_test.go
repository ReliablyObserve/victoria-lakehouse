package compaction

import (
	"fmt"
	"testing"
)

func TestSelfMember(t *testing.T) {
	local := func(ips ...string) func() []string { return func() []string { return ips } }
	members := []string{"10.0.0.1:9428", "10.0.0.2:9428", "10.0.0.3:9428"}
	for _, tc := range []struct {
		name  string
		addr  string
		mem   []string
		local func() []string
		want  string
	}{
		{"exact", "10.0.0.2:9428", members, local(), "10.0.0.2:9428"},
		{"by local ip and port", ":9428", members, local("10.0.0.2"), "10.0.0.2:9428"},
		{"port differs", ":10428", members, local("10.0.0.2"), ""},
		{"not local", ":9428", members, local("192.168.1.1"), ""},
		{"two local entries are ambiguous", ":9428", members, local("10.0.0.1", "10.0.0.2"), ""},
		{"no local ips", ":9428", members, nil, ""},
		{"bad addr", "nope", members, local("10.0.0.2"), ""},
	} {
		if got := SelfMember(tc.addr, tc.mem, tc.local); got != tc.want {
			t.Errorf("%s: SelfMember = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Pods that call themselves ":9428" while discovery lists them by IP each find
// their own entry: every partition has exactly one owner. Without ResolveSelf each
// pod hashes as an extra ":9428" member and claims the partitions it wins.
func TestOwnership_ResolveSelfGivesEveryPartitionOneOwner(t *testing.T) {
	members := []string{"10.0.0.1:9428", "10.0.0.2:9428", "10.0.0.3:9428"}
	parts := make([]string, 300)
	for i := range parts {
		parts[i] = fmt.Sprintf("dt=2026-10-%02d/hour=%02d", i%28+1, i%24)
	}
	pod := func(ip string, resolve bool) *OwnershipResolver {
		withSelf := func() []string {
			if SelfMember(":9428", members, func() []string { return []string{ip} }) != "" {
				return members
			}
			return append(append([]string(nil), members...), ":9428")
		}
		r := NewOwnershipResolver(":9428", withSelf)
		if resolve {
			r.ResolveSelf = func(peers []string) string {
				return SelfMember(":9428", peers, func() []string { return []string{ip} })
			}
		}
		return r
	}
	owners := func(resolve bool) []int {
		count := make([]int, len(parts))
		for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"} {
			r := pod(ip, resolve)
			for i, p := range parts {
				if r.OwnsPartition(p) {
					count[i]++
				}
			}
		}
		return count
	}
	for i, n := range owners(true) {
		if n != 1 {
			t.Fatalf("with ResolveSelf partition %d has %d owners, want 1", i, n)
		}
	}
	// The premise: without it the pods do not agree (here nobody owns the partitions
	// that hash to the shared ":9428" entry, or all of them claim it).
	bad := 0
	for _, n := range owners(false) {
		if n != 1 {
			bad++
		}
	}
	if bad == 0 {
		t.Fatal("without ResolveSelf every partition still has one owner; the test proves nothing")
	}
}
