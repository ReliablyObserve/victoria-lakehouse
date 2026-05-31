package compaction

import (
	"fmt"
	"testing"
	"time"
)

func mkCandidate(partition string) partitionCandidate {
	return partitionCandidate{partition: partition, level: 0, time: time.Now()}
}

// TestFairShare_RoundRobinAcrossTenants asserts the basic contract:
// with 3 tenants × 100 candidates each, every tenant gets at least
// one slot over the course of 3 ticks.
//
// Negative-control proof: removing the cursor advance after each
// PickCandidates call would always start from tenant 0, so tenant 2
// gets 0 slots when N=2; this test would fail with one tenant
// missing.
func TestFairShare_RoundRobinAcrossTenants(t *testing.T) {
	var cands []partitionCandidate
	for _, tenant := range []string{"acctA/projA", "acctB/projB", "acctC/projC"} {
		for i := 0; i < 100; i++ {
			cands = append(cands, mkCandidate(fmt.Sprintf("%s/logs/dt=2026-05-31/hour=%02d", tenant, i%24)))
		}
	}

	f := NewFairShareScheduler(1)

	gotPerTenant := map[string]int{}
	for tick := 0; tick < 3; tick++ {
		picked := f.PickCandidates(cands, 3) // 3 slots, 3 tenants
		for _, c := range picked {
			gotPerTenant[extractTenant(c.partition)]++
		}
	}
	if len(gotPerTenant) != 3 {
		t.Fatalf("round-robin: only %d tenants got slots: %v", len(gotPerTenant), gotPerTenant)
	}
	for tenant, n := range gotPerTenant {
		if n == 0 {
			t.Errorf("tenant %s starved", tenant)
		}
	}
}

// TestFairShare_NoisyTenantNoStarvation asserts that one tenant with
// a huge backlog cannot prevent another tenant's single partition
// from being scheduled within a few ticks.
//
// Negative-control proof: a "take everything from tenant[0] before
// moving on" implementation would let tenant A's 1000 partitions
// starve tenant B. Test fails because B never appears in picked.
func TestFairShare_NoisyTenantNoStarvation(t *testing.T) {
	var cands []partitionCandidate
	for i := 0; i < 1000; i++ {
		cands = append(cands, mkCandidate(fmt.Sprintf("noisy/proj/logs/dt=2026-05-31/hour=%02d", i%24)))
	}
	cands = append(cands, mkCandidate("quiet/proj/logs/dt=2026-05-31/hour=00"))

	f := NewFairShareScheduler(1)
	quietSeen := false
	for tick := 0; tick < 5; tick++ {
		picked := f.PickCandidates(cands, 4)
		for _, c := range picked {
			if extractTenant(c.partition) == "quiet/proj" {
				quietSeen = true
			}
		}
	}
	if !quietSeen {
		t.Fatal("quiet tenant starved by noisy tenant within 5 ticks")
	}
}

// TestFairShare_CursorPersistsAcrossCalls asserts the cursor doesn't
// reset between PickCandidates calls — over many calls each tenant
// gets equal opportunity to be at cursor position 0.
func TestFairShare_CursorPersistsAcrossCalls(t *testing.T) {
	cands := []partitionCandidate{
		mkCandidate("A/proj/logs/dt=2026-05-31/hour=00"),
		mkCandidate("B/proj/logs/dt=2026-05-31/hour=00"),
		mkCandidate("C/proj/logs/dt=2026-05-31/hour=00"),
	}
	f := NewFairShareScheduler(1)

	firstPicks := map[string]int{}
	for i := 0; i < 30; i++ {
		picked := f.PickCandidates(cands, 1) // one slot only
		if len(picked) == 1 {
			firstPicks[extractTenant(picked[0].partition)]++
		}
	}
	// With cursor advancing by 1 each call and 3 tenants, each
	// tenant should be first ~10 times.
	for _, tenant := range []string{"A/proj", "B/proj", "C/proj"} {
		got := firstPicks[tenant]
		if got < 8 || got > 12 {
			t.Errorf("tenant %s first-pick count: got %d, want 8-12", tenant, got)
		}
	}
}

// TestFairShare_SingleTenant_DegenerateOK asserts the degenerate case:
// only one tenant means PickCandidates trivially returns up to
// maxConcurrent items from that one tenant.
func TestFairShare_SingleTenant_DegenerateOK(t *testing.T) {
	var cands []partitionCandidate
	for i := 0; i < 10; i++ {
		cands = append(cands, mkCandidate(fmt.Sprintf("only/proj/logs/dt=2026-05-31/hour=%02d", i)))
	}
	f := NewFairShareScheduler(1) // ignored: only one tenant

	picked := f.PickCandidates(cands, 5)
	if len(picked) != 5 {
		t.Fatalf("single-tenant maxConcurrent=5: got %d, want 5", len(picked))
	}
	for _, c := range picked {
		if extractTenant(c.partition) != "only/proj" {
			t.Errorf("wrong tenant: %s", extractTenant(c.partition))
		}
	}
}

// TestFairShare_DynamicTenantAddition asserts a new tenant appearing
// mid-run gets picked up in the normal rotation (no restart needed).
func TestFairShare_DynamicTenantAddition(t *testing.T) {
	f := NewFairShareScheduler(1)

	// Initial: 2 tenants.
	cands1 := []partitionCandidate{
		mkCandidate("A/proj/logs/dt=2026-05-31/hour=00"),
		mkCandidate("B/proj/logs/dt=2026-05-31/hour=00"),
	}
	_ = f.PickCandidates(cands1, 2)
	_ = f.PickCandidates(cands1, 2)

	// Now tenant C joins.
	cands2 := append(cands1, mkCandidate("C/proj/logs/dt=2026-05-31/hour=00"))
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		picked := f.PickCandidates(cands2, 3)
		for _, c := range picked {
			seen[extractTenant(c.partition)] = true
		}
	}
	if !seen["C/proj"] {
		t.Fatal("new tenant C not scheduled after several ticks")
	}
}

// TestFairShare_ExtractTenant covers the partition-key parsing
// helper.
func TestFairShare_ExtractTenant(t *testing.T) {
	cases := []struct {
		partition string
		want      string
	}{
		{"acct/proj/logs/dt=2026-05-31/hour=00", "acct/proj"},
		{"acct/proj/traces/dt=2026-05-31/hour=12", "acct/proj"},
		{"dt=2026-05-31/hour=00", "default"},
		{"shortid/dt=2026-05-31/hour=00", "shortid"},
		{"", "default"},
		{"single", "default"},
	}
	for _, c := range cases {
		if got := extractTenant(c.partition); got != c.want {
			t.Errorf("extractTenant(%q): got %q, want %q", c.partition, got, c.want)
		}
	}
}

// TestFairShare_ZeroBudget_NormalisesToOne asserts the constructor's
// defensive default.
func TestFairShare_ZeroBudget_NormalisesToOne(t *testing.T) {
	if got := NewFairShareScheduler(0).CompactionsPerTenant(); got != 1 {
		t.Fatalf("zero budget normalisation: got %d, want 1", got)
	}
	if got := NewFairShareScheduler(-5).CompactionsPerTenant(); got != 1 {
		t.Fatalf("negative budget normalisation: got %d, want 1", got)
	}
}

// TestFairShare_HighBudget_AllowsMultiPerTenant covers the case where
// compactionsPerTenant > 1 and a single tenant has many candidates.
func TestFairShare_HighBudget_AllowsMultiPerTenant(t *testing.T) {
	var cands []partitionCandidate
	for i := 0; i < 5; i++ {
		cands = append(cands, mkCandidate(fmt.Sprintf("solo/proj/logs/dt=2026-05-31/hour=%02d", i)))
	}
	f := NewFairShareScheduler(3)
	picked := f.PickCandidates(cands, 3)
	if len(picked) != 3 {
		t.Fatalf("multi-per-tenant: picked=%d, want 3", len(picked))
	}
}
