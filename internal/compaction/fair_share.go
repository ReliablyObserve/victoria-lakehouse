package compaction

import (
	"strings"
	"sync"
)

// FairShareScheduler implements per-tenant round-robin partition
// selection (spec §12.2). Each tick, it groups eligible partition
// candidates by tenant prefix, then cycles across tenants with a
// persistent cursor so no tenant gets starved by another with a large
// backlog. compactionsPerTenant slots per tenant per tick (default 1).
//
// Tenant identity is derived from the FIRST TWO path segments of a
// candidate's partition string (the standard "{accountID}/{projectID}/..."
// shape that lakehouse uses) or "default" when the partition has no
// slash prefix. This makes the scheduler tenant-aware without needing
// to know the tenant config — the prefix is already in the partition
// key.
type FairShareScheduler struct {
	mu                   sync.Mutex
	cursor               int
	compactionsPerTenant int
}

// NewFairShareScheduler creates a scheduler with the given per-tenant
// slot budget. compactionsPerTenant <= 0 falls back to 1.
func NewFairShareScheduler(compactionsPerTenant int) *FairShareScheduler {
	if compactionsPerTenant <= 0 {
		compactionsPerTenant = 1
	}
	return &FairShareScheduler{compactionsPerTenant: compactionsPerTenant}
}

// CompactionsPerTenant returns the per-tick slot budget for each tenant.
func (f *FairShareScheduler) CompactionsPerTenant() int { return f.compactionsPerTenant }

// PickCandidates groups the input by tenant and returns up to
// maxConcurrent candidates by cycling across tenants from the
// persistent cursor. Each tenant gets at most compactionsPerTenant
// per call before the cursor advances.
//
// The candidates returned are a stable subset of the input (no
// reordering inside a tenant — the caller should pre-sort by recency
// before calling). The internal cursor is advanced by 1 per call so
// over N ticks, every tenant gets equal slot opportunities.
func (f *FairShareScheduler) PickCandidates(
	candidates []partitionCandidate,
	maxConcurrent int,
) []partitionCandidate {
	if len(candidates) == 0 || maxConcurrent <= 0 {
		return nil
	}

	byTenant := groupCandidatesByTenant(candidates)
	tenants := sortedTenantList(byTenant)
	if len(tenants) == 0 {
		return nil
	}

	f.mu.Lock()
	cursor := f.cursor % len(tenants)
	f.cursor = (f.cursor + 1) % len(tenants)
	f.mu.Unlock()

	picked := make([]partitionCandidate, 0, maxConcurrent)
	slotsRemaining := maxConcurrent

	// Iterate across all tenants starting at cursor; take up to
	// compactionsPerTenant from each in a single pass. If we still
	// have slots after a full round-trip, do another loop — fairness
	// holds as long as every tenant got its first slot before any
	// tenant gets a second. The loop terminates when either we fill
	// maxConcurrent or every tenant's bucket is drained.
	for slotsRemaining > 0 {
		drained := true
		for i := 0; i < len(tenants) && slotsRemaining > 0; i++ {
			t := tenants[(cursor+i)%len(tenants)]
			bucket := byTenant[t]
			if len(bucket) == 0 {
				continue
			}
			take := f.compactionsPerTenant
			if take > len(bucket) {
				take = len(bucket)
			}
			if take > slotsRemaining {
				take = slotsRemaining
			}
			picked = append(picked, bucket[:take]...)
			byTenant[t] = bucket[take:]
			slotsRemaining -= take
			if len(byTenant[t]) > 0 {
				drained = false
			}
		}
		if drained {
			break
		}
	}

	return picked
}

// extractTenant derives the tenant identifier from a partition string
// or partition candidate. For "<acct>/<proj>/...", returns "<acct>/<proj>".
// For "...dt=YYYY-MM-DD/hour=HH" without a tenant prefix, returns
// "default" so single-tenant deployments still round-robin trivially.
func extractTenant(partition string) string {
	idx := strings.Index(partition, "/dt=")
	if idx < 0 {
		// No dt= segment → no tenant prefix to extract. Could be a
		// short partition key from tests; fall back to "default".
		if strings.HasPrefix(partition, "dt=") {
			return "default"
		}
		// No tenant separator at all.
		first := strings.IndexByte(partition, '/')
		if first < 0 {
			return "default"
		}
		return partition[:first]
	}
	// idx points to "/dt="; the tenant prefix is everything before it.
	tenantPrefix := partition[:idx]
	if tenantPrefix == "" {
		return "default"
	}
	// Convention: tenant = first two segments (accountID/projectID).
	parts := strings.SplitN(tenantPrefix, "/", 3)
	switch len(parts) {
	case 0:
		return "default"
	case 1:
		return parts[0]
	default:
		return parts[0] + "/" + parts[1]
	}
}

func groupCandidatesByTenant(candidates []partitionCandidate) map[string][]partitionCandidate {
	out := make(map[string][]partitionCandidate)
	for _, c := range candidates {
		t := extractTenant(c.partition)
		out[t] = append(out[t], c)
	}
	return out
}

// sortedTenantList returns the tenant keys in deterministic order so
// cursor rotation is consistent across pods that observe the same
// tenant set.
func sortedTenantList(byTenant map[string][]partitionCandidate) []string {
	tenants := make([]string, 0, len(byTenant))
	for t := range byTenant {
		tenants = append(tenants, t)
	}
	// Avoid importing sort here (avoids the indirect dep) — use a
	// simple insertion sort which is fine for the small tenant
	// counts (<100) we expect in practice.
	for i := 1; i < len(tenants); i++ {
		j := i
		for j > 0 && tenants[j-1] > tenants[j] {
			tenants[j-1], tenants[j] = tenants[j], tenants[j-1]
			j--
		}
	}
	return tenants
}
