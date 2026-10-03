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

// TestCompactionGroupPrefix_Edges guards the allocation-free parser against
// the shapes the strconv version rejected or accepted: overflow, empty or
// non-numeric segments, a missing fourth segment, a trailing slash.
func TestCompactionGroupPrefix_Edges(t *testing.T) {
	for key, want := range map[string]string{
		"4294967295/0/logs/x":      "4294967295/0/logs/", // max uint32
		"4294967296/0/logs/x":      "",                   // overflow
		"99999999999/0/logs/x":     "",                   // 11 digits
		"0/4294967296/logs/x":      "",
		"/0/logs/x":                "", // empty account
		"1//logs/x":                "", // empty project
		"1/0//x":                   "", // empty mode segment
		"1/0/logs/":                "", // nothing after the mode: no 4th segment
		"1/0/logs":                 "",
		"1/0":                      "",
		"1":                        "",
		"-1/0/logs/x":              "",
		"+1/0/logs/x":              "",
		"1a/0/logs/x":              "",
		"007/008/logs/x":           "007/008/logs/", // leading zeros parse
		"1/0/logs/dt=x/hour=00/a":  "1/0/logs/",
		"1/0/traces/a/b/c/d":       "1/0/traces/",
		"logs/dt=2026-01-01/a.par": "",
	} {
		if got := CompactionGroupPrefix(key); got != want {
			t.Errorf("CompactionGroupPrefix(%q) = %q, want %q", key, got, want)
		}
	}
}

// TestCompactionGroupPrefix_NoAllocation guards the per-file scan cost.
func TestCompactionGroupPrefix_NoAllocation(t *testing.T) {
	key := "1001/0/logs/dt=2026-06-01/hour=00/compacted-L1-abcdef01.parquet"
	if n := testing.AllocsPerRun(100, func() { _ = CompactionGroupPrefix(key) }); n != 0 {
		t.Fatalf("CompactionGroupPrefix allocates %v per call", n)
	}
}

// TestCompactionStats_FragmentedMatchesPlanner guards that the stats flag only
// what the planner would merge (M19): many L0/L1 files (top level below L2) are
// the level thresholds' business, not fragmentation; two mature (>= 32 MiB)
// top-level files are never rewritten by the hint; two small ones are, and only
// their bytes count. Without these rules the compaction panel reports a
// fragmented partition that compaction will never touch.
func TestCompactionStats_FragmentedMatchesPlanner(t *testing.T) {
	const part = "dt=2026-06-01/hour=00"
	build := func(files ...FileInfo) CompactionStats {
		m := New("bucket", "logs/")
		for _, f := range files {
			m.AddFile(part, f)
		}
		return m.ComputeCompactionStats("v1", nil)
	}
	l := func(name string, level int, size int64) FileInfo { return groupedFile(1001, name, level, size) }

	if st := build(l("a", 0, 10), l("b", 0, 10), l("c", 1, 10), l("d", 1, 10)); st.FragmentedPartitions != 0 {
		t.Fatal("several L0/L1 files reported as fragmented")
	}
	if st := build(l("a", 2, MatureObjectBytes), l("b", 2, MatureObjectBytes)); st.FragmentedPartitions != 0 {
		t.Fatal("two mature L2 files reported as fragmented")
	}
	if st := build(l("a", 2, MatureObjectBytes), l("b", 2, 100)); st.FragmentedPartitions != 0 {
		t.Fatal("one mature plus one small L2 file reported as fragmented (the small one is alone)")
	}
	st := build(l("a", 2, MatureObjectBytes), l("b", 2, 100), l("c", 2, 300))
	if st.FragmentedPartitions != 1 || len(st.Candidates) != 1 {
		t.Fatalf("two small L2 files next to a mature one: fragmented=%d", st.FragmentedPartitions)
	}
	if want := int64(float64(400) * mergeOverheadGainEstimate); st.Candidates[0].EstimatedSavingsBytes != want {
		t.Fatalf("savings %d, want %d (only the two small files' 400 bytes)", st.Candidates[0].EstimatedSavingsBytes, want)
	}
	// One byte under the threshold is not mature.
	if st := build(l("a", 2, MatureObjectBytes-1), l("b", 2, MatureObjectBytes-1)); st.FragmentedPartitions != 1 {
		t.Fatal("files just under 32 MiB must count")
	}
}
