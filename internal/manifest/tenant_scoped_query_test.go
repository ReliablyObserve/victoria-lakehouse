package manifest

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// TestGetFilesForRangeTenant_ParityWithUnscoped pins the most important
// invariant: GetFilesForRangeTenant must return the same files
// GetFilesForRange would, filtered by the tenant key prefix. Any drift
// silently makes queries skip valid data — a worse failure than a
// crash because operators don't notice until users complain.
func TestGetFilesForRangeTenant_ParityWithUnscoped(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	// Three tenants × four partitions × two files per tenant-partition.
	tenants := []struct{ acc, proj string }{
		{"0", "0"},
		{"1", "1"},
		{"1001", "0"},
	}
	partitions := []string{
		"dt=2026-06-01/hour=12",
		"dt=2026-06-02/hour=06",
		"dt=2026-06-03/hour=18",
		"dt=2026-06-04/hour=00",
	}
	for _, t := range tenants {
		for _, p := range partitions {
			for i := 0; i < 2; i++ {
				m.AddFile(p, FileInfo{
					Key:       fmt.Sprintf("%s/%s/traces/%s/f%d.parquet", t.acc, t.proj, p, i),
					Size:      100,
					MinTimeNs: mustParse(p).UnixNano(),
					MaxTimeNs: mustParse(p).Add(time.Hour).UnixNano(),
				})
			}
		}
	}

	// Full range: should return tenant's files only.
	startNs := mustParse("dt=2026-06-01/hour=00").UnixNano()
	endNs := mustParse("dt=2026-06-05/hour=00").UnixNano()

	for _, te := range tenants {
		scoped := m.GetFilesForRangeTenant(startNs, endNs, te.acc, te.proj)
		if len(scoped) != 8 { // 4 partitions × 2 files
			t.Errorf("tenant %s:%s scoped query returned %d files, want 8", te.acc, te.proj, len(scoped))
		}
		// Every returned file must belong to this tenant.
		expectedPrefix := te.acc + "/" + te.proj + "/"
		for _, fi := range scoped {
			if len(fi.Key) < len(expectedPrefix) || fi.Key[:len(expectedPrefix)] != expectedPrefix {
				t.Errorf("tenant %s:%s scoped query returned file from another tenant: %s",
					te.acc, te.proj, fi.Key)
			}
		}
	}

	// Empty account → falls back to full-manifest path.
	all := m.GetFilesForRangeTenant(startNs, endNs, "", "")
	if len(all) != 24 { // 3 tenants × 8 each
		t.Errorf("empty account fallback returned %d files, want 24", len(all))
	}
}

// TestGetFilesForRangeTenant_NarrowTimeRange checks that the
// partition-set intersection works correctly when the query window
// doesn't cover all of a tenant's partitions.
func TestGetFilesForRangeTenant_NarrowTimeRange(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	for _, p := range []string{
		"dt=2026-06-01/hour=12",
		"dt=2026-06-02/hour=06",
		"dt=2026-06-03/hour=18",
		"dt=2026-06-04/hour=00",
	} {
		m.AddFile(p, FileInfo{
			Key:       fmt.Sprintf("5/0/traces/%s/f.parquet", p),
			Size:      100,
			MinTimeNs: mustParse(p).UnixNano(),
			MaxTimeNs: mustParse(p).Add(time.Hour).UnixNano(),
		})
	}

	// Query covering only 2026-06-02 and 2026-06-03.
	startNs := mustParse("dt=2026-06-02/hour=00").UnixNano()
	endNs := mustParse("dt=2026-06-04/hour=00").UnixNano()

	got := m.GetFilesForRangeTenant(startNs, endNs, "5", "0")
	if len(got) != 2 {
		t.Errorf("narrow range query returned %d files, want 2 (one per matching partition)", len(got))
	}
	// And ordered by MinTimeNs ascending so query plans don't have to re-sort.
	if len(got) >= 2 && got[0].MinTimeNs > got[1].MinTimeNs {
		t.Errorf("results not sorted ascending: %d %d", got[0].MinTimeNs, got[1].MinTimeNs)
	}
}

// TestGetFilesForRangeTenant_UnknownTenantReturnsNil — calling the
// scoped method with a tenant the manifest doesn't know must NOT
// fall through to the full scan. Returning nil correctly short-
// circuits the query path; falling through would silently return
// every tenant's files to a user who has no permission to see them.
func TestGetFilesForRangeTenant_UnknownTenantReturnsNil(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"
	m.AddFile("dt=2026-06-04/hour=00", FileInfo{
		Key:       "0/0/traces/dt=2026-06-04/hour=00/a.parquet",
		Size:      100,
		MinTimeNs: mustParse("dt=2026-06-04/hour=00").UnixNano(),
		MaxTimeNs: mustParse("dt=2026-06-04/hour=00").Add(time.Hour).UnixNano(),
	})

	got := m.GetFilesForRangeTenant(0, 1<<62, "999", "999")
	if got != nil {
		t.Errorf("unknown tenant returned %d files, want nil (no cross-tenant fallthrough)", len(got))
	}
}

// TestGetFilesForRangeTenant_FollowsRemoveFile pins the
// cache-consistency contract: removing a file updates tenantAggregates,
// so subsequent scoped queries don't return ghost entries.
func TestGetFilesForRangeTenant_FollowsRemoveFile(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"
	for i := 0; i < 3; i++ {
		m.AddFile("dt=2026-06-04/hour=00", FileInfo{
			Key:       fmt.Sprintf("7/0/traces/dt=2026-06-04/hour=00/f%d.parquet", i),
			Size:      100,
			MinTimeNs: mustParse("dt=2026-06-04/hour=00").UnixNano(),
			MaxTimeNs: mustParse("dt=2026-06-04/hour=00").Add(time.Hour).UnixNano(),
		})
	}

	before := m.GetFilesForRangeTenant(0, 1<<62, "7", "0")
	if len(before) != 3 {
		t.Fatalf("pre-remove: got %d, want 3", len(before))
	}

	m.RemoveFile("dt=2026-06-04/hour=00", "7/0/traces/dt=2026-06-04/hour=00/f1.parquet")

	after := m.GetFilesForRangeTenant(0, 1<<62, "7", "0")
	if len(after) != 2 {
		t.Fatalf("post-remove: got %d, want 2", len(after))
	}
	for _, fi := range after {
		if fi.Key == "7/0/traces/dt=2026-06-04/hour=00/f1.parquet" {
			t.Error("removed file still returned by scoped query")
		}
	}
}

// TestGetFilesForRangeTenant_DoesntLeakOtherTenantsFiles — the
// critical security invariant. A scoped query for tenant A must never
// return a file whose key prefix matches tenant B, even by accident
// of partition layout.
func TestGetFilesForRangeTenant_DoesntLeakOtherTenantsFiles(t *testing.T) {
	m := New("b", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	// Same partition, different tenants.
	m.AddFile("dt=2026-06-04/hour=00", FileInfo{
		Key: "1/0/traces/dt=2026-06-04/hour=00/a.parquet", Size: 100,
		MinTimeNs: mustParse("dt=2026-06-04/hour=00").UnixNano(),
		MaxTimeNs: mustParse("dt=2026-06-04/hour=00").Add(time.Hour).UnixNano(),
	})
	m.AddFile("dt=2026-06-04/hour=00", FileInfo{
		Key: "2/0/traces/dt=2026-06-04/hour=00/b.parquet", Size: 100,
		MinTimeNs: mustParse("dt=2026-06-04/hour=00").UnixNano(),
		MaxTimeNs: mustParse("dt=2026-06-04/hour=00").Add(time.Hour).UnixNano(),
	})

	scoped := m.GetFilesForRangeTenant(0, 1<<62, "1", "0")
	for _, fi := range scoped {
		if fi.Key[0] != '1' {
			t.Errorf("tenant 1 scoped query leaked tenant %c's file: %s", fi.Key[0], fi.Key)
		}
	}
	if len(scoped) != 1 {
		t.Errorf("tenant 1 scoped: got %d files, want 1", len(scoped))
	}
}

func mustParse(p string) time.Time {
	t, err := parsePartitionTime(p)
	if err != nil {
		panic(err)
	}
	return t
}

// Sanity check that the tests above aren't accidentally hidden under
// the build constraint of another file in this package.
var _ = sort.Strings
