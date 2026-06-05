package manifest

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestTenantAggregates_MatchesLegacyLinearScan pins behavior parity
// between the incremental cache and the original full-scan
// implementation. We rebuild the aggregates from scratch (which
// matches the legacy behavior bit-for-bit) and compare to the
// incremental-maintenance result on the same corpus. Any divergence
// means an AddFile/RemoveFile/EnrichFileMetadata path is leaking
// state into one side and not the other.
func TestTenantAggregates_MatchesLegacyLinearScan(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	add := func(acc, proj, partition, suffix string, size, rows int64) {
		m.AddFile(partition, FileInfo{
			Key:      fmt.Sprintf("%s/%s/%s", acc, proj, suffix),
			Size:     size,
			RowCount: rows,
			RawBytes: size * 3,
		})
	}

	add("0", "0", "dt=2026-01-01/hour=00", "a", 100, 1000)
	add("0", "0", "dt=2026-01-01/hour=01", "b", 200, 2000)
	add("0", "0", "dt=2026-01-02/hour=00", "c", 300, 3000)
	add("1", "1", "dt=2026-01-01/hour=00", "d", 400, 4000)
	add("1001", "0", "dt=2026-01-01/hour=05", "e", 500, 5000)

	incremental := m.TenantSummaries()

	// Force a full rebuild (the path RefreshFromS3 uses) and compare.
	m.mu.Lock()
	m.rebuildTenantAggregates()
	m.mu.Unlock()
	rebuilt := m.TenantSummaries()

	if len(incremental) != len(rebuilt) {
		t.Fatalf("tenant count mismatch: incremental=%d rebuilt=%d", len(incremental), len(rebuilt))
	}
	for i := range incremental {
		a, b := incremental[i], rebuilt[i]
		if a.AccountID != b.AccountID || a.ProjectID != b.ProjectID {
			t.Errorf("tenant order diverged at index %d", i)
		}
		if a.TotalFiles != b.TotalFiles || a.TotalBytes != b.TotalBytes ||
			a.TotalRows != b.TotalRows || a.RawBytes != b.RawBytes ||
			a.Partitions != b.Partitions ||
			!a.MinTime.Equal(b.MinTime) || !a.MaxTime.Equal(b.MaxTime) {
			t.Errorf("tenant %s:%s differs:\n  incremental=%+v\n  rebuilt    =%+v",
				a.AccountID, a.ProjectID, a, b)
		}
	}
}

// TestTenantAggregates_TimeBoundsRecomputeOnPartitionLastRemove pins
// the trickiest case: removing the LAST file a tenant has in its
// earliest (or latest) partition must shift the tenant's MinTime
// (or MaxTime) by walking the remaining partitions — NOT the manifest's
// full file list, which would be O(50M) at PB-scale.
func TestTenantAggregates_TimeBoundsRecomputeOnPartitionLastRemove(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	addAt := func(partition, suffix string) {
		m.AddFile(partition, FileInfo{
			Key:  "5/0/" + suffix,
			Size: 1,
		})
	}
	addAt("dt=2026-01-01/hour=00", "a")
	addAt("dt=2026-01-02/hour=12", "b")
	addAt("dt=2026-01-03/hour=23", "c")

	tk := tenantAccumKey{account: "5", project: "0"}
	a := m.tenantAggregates[tk]
	wantMin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	wantMax := time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC).UnixNano()
	if a.minTimeNs != wantMin {
		t.Errorf("initial minTimeNs=%d want=%d", a.minTimeNs, wantMin)
	}
	if a.maxTimeNs != wantMax {
		t.Errorf("initial maxTimeNs=%d want=%d", a.maxTimeNs, wantMax)
	}

	// Remove the file in the earliest partition. minTime must shift
	// to the next-earliest, not stay at Jan 1.
	m.RemoveFile("dt=2026-01-01/hour=00", "5/0/a")
	a = m.tenantAggregates[tk]
	wantMin2 := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC).UnixNano()
	if a.minTimeNs != wantMin2 {
		t.Errorf("after remove min, minTimeNs=%d want=%d", a.minTimeNs, wantMin2)
	}
	if a.maxTimeNs != wantMax {
		t.Errorf("after remove min, maxTimeNs=%d shouldn't have moved", a.maxTimeNs)
	}

	// Remove the file in the latest partition. maxTime must shift.
	m.RemoveFile("dt=2026-01-03/hour=23", "5/0/c")
	a = m.tenantAggregates[tk]
	wantMax3 := time.Date(2026, 1, 2, 13, 0, 0, 0, time.UTC).UnixNano()
	if a.maxTimeNs != wantMax3 {
		t.Errorf("after remove max, maxTimeNs=%d want=%d", a.maxTimeNs, wantMax3)
	}

	// Remove the last file. Tenant must be evicted entirely so future
	// TenantSummaries calls don't emit phantoms.
	m.RemoveFile("dt=2026-01-02/hour=12", "5/0/b")
	if _, present := m.tenantAggregates[tk]; present {
		t.Error("empty tenant not evicted from aggregates")
	}
}

// TestTenantAggregates_EnrichRowCountUpdatesCache pins the case
// where a file's RowCount was 0 at insert (manifest reload from
// listing) and gets enriched after the first footer read. The cache
// must reflect the new row count.
func TestTenantAggregates_EnrichRowCountUpdatesCache(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	m.AddFile("dt=2026-01-01/hour=00", FileInfo{
		Key:      "7/0/a",
		Size:     100,
		RowCount: 0,
	})

	summaries := m.TenantSummaries()
	if summaries[0].TotalRows != 0 {
		t.Errorf("initial rows=%d, want 0", summaries[0].TotalRows)
	}

	m.EnrichFileMetadata("7/0/a", 50000, 0, 0)

	summaries = m.TenantSummaries()
	if summaries[0].TotalRows != 50000 {
		t.Errorf("post-enrich rows=%d, want 50000", summaries[0].TotalRows)
	}
}

// TestTenantAggregates_OrgIDPrefixTemplate covers the alternate prefix
// template ({OrgID}/) used by some deployments. The cache parser must
// handle both layouts; without the special case it would mis-attribute
// every file to a phantom tenant.
func TestTenantAggregates_OrgIDPrefixTemplate(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{OrgID}/"

	m.AddFile("dt=2026-01-01/hour=00", FileInfo{Key: "acme/data/x", Size: 1})
	m.AddFile("dt=2026-01-01/hour=00", FileInfo{Key: "globex/data/y", Size: 1})

	summaries := m.TenantSummaries()
	if len(summaries) != 2 {
		t.Fatalf("expected 2 OrgID tenants, got %d: %+v", len(summaries), summaries)
	}
	got := map[string]bool{}
	for _, s := range summaries {
		got[s.AccountID] = true
		if s.ProjectID != "" {
			t.Errorf("OrgID layout should have empty ProjectID, got %q", s.ProjectID)
		}
	}
	if !got["acme"] || !got["globex"] {
		t.Errorf("OrgID parsing missed a tenant: %+v", got)
	}
}

// TestTenantAggregates_RaceConcurrentMutation drives many goroutines
// through AddFile/RemoveFile/EnrichFileMetadata simultaneously and
// asserts the cache invariant (cache totals == legacy full scan)
// holds at quiescence. Catches missing-lock-acquire bugs the
// -race detector won't see.
func TestTenantAggregates_RaceConcurrentMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("race test")
	}
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	const N = 500
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			acc := i % 5
			partition := fmt.Sprintf("dt=2026-01-01/hour=%02d", i%24)
			m.AddFile(partition, FileInfo{
				Key:      fmt.Sprintf("%d/0/k%d", acc, i),
				Size:     int64(i + 1),
				RowCount: int64(i * 10),
				RawBytes: int64(i + 1),
			})
		}(i)
	}
	wg.Wait()

	for i := 0; i < N; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			m.EnrichFileMetadata(fmt.Sprintf("%d/0/k%d", i%5, i), int64(i*100), 0, 0)
		}(i)
		go func(i int) {
			defer wg.Done()
			if i%3 == 0 {
				partition := fmt.Sprintf("dt=2026-01-01/hour=%02d", i%24)
				m.RemoveFile(partition, fmt.Sprintf("%d/0/k%d", i%5, i))
			}
		}(i)
	}
	wg.Wait()

	cached := m.TenantSummaries()
	m.mu.Lock()
	m.rebuildTenantAggregates()
	m.mu.Unlock()
	rebuilt := m.TenantSummaries()

	if len(cached) != len(rebuilt) {
		t.Fatalf("race drift: cached=%d tenants, rebuilt=%d", len(cached), len(rebuilt))
	}
	for i := range cached {
		if cached[i].TotalFiles != rebuilt[i].TotalFiles ||
			cached[i].TotalBytes != rebuilt[i].TotalBytes ||
			cached[i].TotalRows != rebuilt[i].TotalRows {
			t.Errorf("race drift on tenant %d: cached=%+v rebuilt=%+v",
				i, cached[i], rebuilt[i])
		}
	}
}
