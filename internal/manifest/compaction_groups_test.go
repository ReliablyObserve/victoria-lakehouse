package manifest

import (
	"fmt"
	"testing"
)

func groupedFile(acct int, name string, level int, size int64) FileInfo {
	return FileInfo{
		Key:               fmt.Sprintf("%d/0/logs/dt=2026-06-01/hour=00/%s.parquet", acct, name),
		Size:              size,
		CompactionLevel:   level,
		SchemaFingerprint: "v1",
	}
}

func TestCompactionStats_FragmentationIsPerTenantGroup(t *testing.T) {
	// Guards fragmentedGroups: one L2 file per tenant is fully compacted, however
	// many tenants share the hour (issue #343: the partition-wide count of two
	// L2 files reported every busy hour as fragmented forever).
	m := New("bucket", "logs/")
	const part = "dt=2026-06-01/hour=00"
	m.AddFile(part, groupedFile(1001, "a", 2, 100))
	m.AddFile(part, groupedFile(1002, "b", 2, 200))
	st := m.ComputeCompactionStats("v1", nil)
	if st.FragmentedPartitions != 0 || len(st.Candidates) != 0 {
		t.Fatalf("two tenants x one L2 reported fragmented: partitions=%d candidates=%+v", st.FragmentedPartitions, st.Candidates)
	}
}

func TestCompactionStats_FragmentedTenantReportsItsBytes(t *testing.T) {
	// Guards the bytes of the fragmented groups: only the fragmented tenant's
	// top-level bytes drive the estimated saving, not the whole partition's.
	m := New("bucket", "logs/")
	const part = "dt=2026-06-01/hour=00"
	m.AddFile(part, groupedFile(1001, "a", 2, 1000))
	m.AddFile(part, groupedFile(1001, "b", 2, 3000))
	m.AddFile(part, groupedFile(1002, "c", 2, 50_000)) // lone: compacted
	m.AddFile(part, groupedFile(1003, "d", 1, 7))      // lone, lower level
	st := m.ComputeCompactionStats("v1", nil)
	if st.FragmentedPartitions != 1 || len(st.Candidates) != 1 {
		t.Fatalf("fragmented=%d candidates=%d, want 1/1", st.FragmentedPartitions, len(st.Candidates))
	}
	c := st.Candidates[0]
	want := int64(float64(4000) * mergeOverheadGainEstimate)
	if c.EstimatedSavingsBytes != want {
		t.Fatalf("savings %d, want %d (4000 bytes of tenant 1001's L2 files)", c.EstimatedSavingsBytes, want)
	}
	if len(c.Reasons) != 1 || c.Reasons[0] != "fragmented" {
		t.Fatalf("reasons %v", c.Reasons)
	}
}

func TestCompactionStats_BucketsAreSeparateGroups(t *testing.T) {
	// Guards groupKey{prefix, bucket}: compaction writes one output per bucket,
	// so two L2 files of one tenant in different buckets are not fragmented.
	m := New("bucket", "logs/")
	const part = "dt=2026-06-01/hour=00"
	a, b := groupedFile(1001, "a", 2, 10), groupedFile(1001, "b", 2, 10)
	b.Bucket = "second"
	m.AddFile(part, a)
	m.AddFile(part, b)
	if st := m.ComputeCompactionStats("v1", nil); st.FragmentedPartitions != 0 {
		t.Fatalf("different buckets reported fragmented")
	}
}

func TestCompactionStats_LegacyKeysShareOneGroup(t *testing.T) {
	// Guards the legacy fallback: un-prefixed keys form ONE group, as in the compactor.
	m := New("bucket", "logs/")
	const part = "dt=2026-06-01/hour=00"
	m.AddFile(part, FileInfo{Key: "logs/dt=2026-06-01/hour=00/a.parquet", Size: 5, CompactionLevel: 3, SchemaFingerprint: "v1"})
	m.AddFile(part, FileInfo{Key: "logs/dt=2026-06-01/hour=00/b.parquet", Size: 5, CompactionLevel: 3, SchemaFingerprint: "v1"})
	if st := m.ComputeCompactionStats("v1", nil); st.FragmentedPartitions != 1 {
		t.Fatalf("two legacy L3 files must be fragmented, got %d", st.FragmentedPartitions)
	}
}

func TestCompactionGroupPrefix(t *testing.T) {
	// Guards the shared grouping: the compactor and the stats must agree on
	// what one output group is.
	for key, want := range map[string]string{
		"1001/0/logs/dt=2026-06-01/hour=00/a.parquet": "1001/0/logs/",
		"7/9/traces/dt=2026-06-01/hour=00/a.parquet":  "7/9/traces/",
		"logs/dt=2026-06-01/hour=00/a.parquet":        "",
		"abc/0/logs/dt=2026-06-01/hour=00/a.parquet":  "",
		"1001/x/logs/dt=2026-06-01/hour=00/a.parquet": "",
		"4294967296/0/logs/dt=2026-06-01/hour=00/a":   "", // > uint32
		"1001/0/logs": "", // fewer than 4 segments
		"":            "",
		"0/0/logs/dt=2026-06-01/hour=00/compacted.parq": "0/0/logs/",
	} {
		if got := CompactionGroupPrefix(key); got != want {
			t.Errorf("CompactionGroupPrefix(%q) = %q, want %q", key, got, want)
		}
	}
}
