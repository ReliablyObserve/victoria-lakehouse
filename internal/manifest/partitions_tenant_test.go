package manifest

import "testing"

// TestGetPartitionsForTenant_ScopesToTenant locks the tenant-detail partition
// drill-down fix: GetPartitions("","") returned the global partition list for
// every tenant; GetPartitionsForTenant must return only the requesting tenant's
// partitions, with file/byte counts scoped to that tenant — even when two tenants
// share the same date partition.
func TestGetPartitionsForTenant_ScopesToTenant(t *testing.T) {
	m := New("bucket", "")

	// Tenant 1:1 has data on 2026-06-01 and 2026-06-02.
	m.AddFile("dt=2026-06-01/hour=00", FileInfo{Key: "1/1/dt=2026-06-01/hour=00/a.parquet", Size: 1000})
	m.AddFile("dt=2026-06-02/hour=05", FileInfo{Key: "1/1/dt=2026-06-02/hour=05/b.parquet", Size: 2000})
	// Tenant 2:2 shares the 2026-06-01 partition and adds 2026-06-03.
	m.AddFile("dt=2026-06-01/hour=00", FileInfo{Key: "2/2/dt=2026-06-01/hour=00/c.parquet", Size: 9000})
	m.AddFile("dt=2026-06-03/hour=00", FileInfo{Key: "2/2/dt=2026-06-03/hour=00/d.parquet", Size: 4000})

	got := m.GetPartitionsForTenant("1", "1")

	dates := map[string]PartitionSummary{}
	for _, ps := range got {
		dates[ps.Date] = ps
	}
	if len(got) != 2 {
		t.Fatalf("tenant 1:1 partitions = %d (%v), want 2 (06-01, 06-02)", len(got), dates)
	}
	if _, ok := dates["2026-06-03"]; ok {
		t.Error("tenant 1:1 leaked tenant 2:2's 2026-06-03 partition (global list bug)")
	}
	// The shared 06-01 partition must count ONLY tenant 1:1's file/bytes, not
	// tenant 2:2's 9000-byte file.
	if d := dates["2026-06-01"]; d.Files != 1 || d.Bytes != 1000 {
		t.Errorf("2026-06-01 for tenant 1:1 = {files:%d bytes:%d}, want {1 1000} (must exclude tenant 2:2)", d.Files, d.Bytes)
	}

	// Sanity: the global view still sees all three dates.
	if g := m.GetPartitions("", ""); len(g) != 3 {
		t.Errorf("global GetPartitions = %d dates, want 3", len(g))
	}

	// PartitionCount collapses tenants (3 distinct dt/hour buckets), but
	// TenantPartitionCount must equal the SUM of per-tenant partitions so the
	// overview reconciles with the tenant detail views.
	if pc := m.PartitionCount(); pc != 3 {
		t.Errorf("PartitionCount (collapsed) = %d, want 3", pc)
	}
	sumPerTenant := len(m.GetPartitionsForTenant("1", "1")) + len(m.GetPartitionsForTenant("2", "2"))
	if tpc := m.TenantPartitionCount(); tpc != 4 || tpc != sumPerTenant {
		t.Errorf("TenantPartitionCount = %d, want 4 == sum-per-tenant %d (06-01 shared by both)", tpc, sumPerTenant)
	}
}
