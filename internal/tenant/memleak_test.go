package tenant

import (
	"fmt"
	"runtime"
	"testing"
)

func tenantForceGC() {
	runtime.GC()
	runtime.GC()
}

func tenantHeapInUse() uint64 {
	var m runtime.MemStats
	tenantForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestMemLeak_TenantResolver_AddAliasResolve(t *testing.T) {
	r := NewResolver(ResolverConfig{AutoRegister: false})

	// Pre-populate with a bounded set
	for i := 0; i < 100; i++ {
		_ = r.AddAlias(fmt.Sprintf("org-%d", i), TenantID{AccountID: uint32(i), ProjectID: 1})
	}
	tenantForceGC()

	before := tenantHeapInUse()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		orgID := fmt.Sprintf("org-%d", i%100)
		tid, ok := r.Resolve(orgID)
		if !ok {
			t.Fatalf("expected to resolve %s", orgID)
		}
		_ = tid.AccountID
	}

	tenantForceGC()
	after := tenantHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d Resolve cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_TenantResolver_AddRemoveCycles(t *testing.T) {
	r := NewResolver(ResolverConfig{})

	// Warm up
	for i := 0; i < 100; i++ {
		orgID := fmt.Sprintf("org-%d", i)
		_ = r.AddAlias(orgID, TenantID{AccountID: uint32(i), ProjectID: uint32(i + 1)})
		r.RemoveAlias(orgID)
	}
	tenantForceGC()

	before := tenantHeapInUse()

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		orgID := fmt.Sprintf("org-%d", i%200) // bounded key space
		_ = r.AddAlias(orgID, TenantID{AccountID: uint32(i % 200), ProjectID: 1})
		if i%2 == 0 {
			r.RemoveAlias(orgID)
		}
	}
	// Clean up remaining entries
	for i := 0; i < 200; i++ {
		r.RemoveAlias(fmt.Sprintf("org-%d", i))
	}

	tenantForceGC()
	after := tenantHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d AddAlias/RemoveAlias cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_TenantResolver_DisplayNameCycles(t *testing.T) {
	r := NewResolver(ResolverConfig{MetricsFormat: MetricsFormatBoth})

	for i := 0; i < 100; i++ {
		_ = r.AddAlias(fmt.Sprintf("prod-team-%d", i), TenantID{AccountID: uint32(i), ProjectID: 1})
	}
	tenantForceGC()

	before := tenantHeapInUse()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		name := r.DisplayName(uint32(i%100), 1)
		_ = name
		label := r.MetricLabel(uint32(i%100), 1)
		_ = label
	}

	tenantForceGC()
	after := tenantHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d DisplayName/MetricLabel cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_TenantResolver_AllAliasesCycles(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	for i := 0; i < 50; i++ {
		_ = r.AddAlias(fmt.Sprintf("org-%d", i), TenantID{AccountID: uint32(i), ProjectID: 1})
	}

	// Warm up
	for i := 0; i < 20; i++ {
		_ = r.AllAliases()
	}
	tenantForceGC()

	before := tenantHeapInUse()

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		aliases := r.AllAliases()
		_ = len(aliases)
	}

	tenantForceGC()
	after := tenantHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d AllAliases cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_ValidateOrgID_Cycles(t *testing.T) {
	validIDs := []string{
		"org-prod-001", "tenant-eu-west", "my-org",
		"UPPER-CASE-ORG", "org123", "x",
	}
	invalidIDs := []string{
		"", "org with spaces", "org/slash",
		"org@email", ".", "..",
	}

	// Warm up
	for i := 0; i < 100; i++ {
		for _, id := range validIDs {
			_ = ValidateOrgID(id)
		}
	}
	tenantForceGC()

	before := tenantHeapInUse()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		if i%2 == 0 {
			_ = ValidateOrgID(validIDs[i%len(validIDs)])
		} else {
			_ = ValidateOrgID(invalidIDs[i%len(invalidIDs)])
		}
	}

	tenantForceGC()
	after := tenantHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d ValidateOrgID cycles (max %d)", growth, iterations, maxAllowed)
	}
}
