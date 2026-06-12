package manifest

import (
	"fmt"
	"testing"
)

func TestComputeCompactionStats(t *testing.T) {
	m := New("bucket", "logs/")
	// Partition A: 3 stale (v1) files at L2 → stale_schema + fragmented, large bytes.
	for i := 0; i < 3; i++ {
		m.AddFile("dt=2026-06-01/hour=00", FileInfo{
			Key:               fmt.Sprintf("logs/dt=2026-06-01/hour=00/f%d.parquet", i),
			Size:              1_000_000,
			RawBytes:          10_000_000,
			CompactionLevel:   2,
			SchemaFingerprint: "v1",
		})
	}
	// Partition B: 2 current (v2) files at L2 → fragmented only, small bytes.
	for i := 0; i < 2; i++ {
		m.AddFile("dt=2026-06-01/hour=01", FileInfo{
			Key:               fmt.Sprintf("logs/dt=2026-06-01/hour=01/f%d.parquet", i),
			Size:              100_000,
			RawBytes:          500_000,
			CompactionLevel:   2,
			SchemaFingerprint: "v2",
		})
	}
	// Partition C: 1 current file at L0 → pending, not a candidate.
	m.AddFile("dt=2026-06-01/hour=02", FileInfo{
		Key:               "logs/dt=2026-06-01/hour=02/f0.parquet",
		Size:              50_000,
		RawBytes:          200_000,
		CompactionLevel:   0,
		SchemaFingerprint: "v2",
	})

	st := m.ComputeCompactionStats("v2")

	if st.TotalFiles != 6 {
		t.Errorf("TotalFiles=%d, want 6", st.TotalFiles)
	}
	if st.StaleSchemaFiles != 3 {
		t.Errorf("StaleSchemaFiles=%d, want 3", st.StaleSchemaFiles)
	}
	if st.FragmentedPartitions != 2 {
		t.Errorf("FragmentedPartitions=%d, want 2 (A and B both have 2+ files at L2)", st.FragmentedPartitions)
	}
	if st.PendingBytes != 50_000 {
		t.Errorf("PendingBytes=%d, want 50000 (the one L0 file)", st.PendingBytes)
	}
	if st.CompactedBytes != 3_200_000 {
		t.Errorf("CompactedBytes=%d, want 3200000 (all L2 bytes)", st.CompactedBytes)
	}

	if len(st.Candidates) != 2 {
		t.Fatalf("Candidates=%d, want 2 (A stale+fragmented, B fragmented; C is L0)", len(st.Candidates))
	}
	// Priority: A first — its stale-schema savings (~3MB×gain) dwarfs B's small merge gain.
	if st.Candidates[0].Partition != "dt=2026-06-01/hour=00" {
		t.Errorf("top-priority candidate = %s, want partition A (biggest estimated savings)", st.Candidates[0].Partition)
	}
	if st.Candidates[0].EstimatedSavingsBytes <= st.Candidates[1].EstimatedSavingsBytes {
		t.Errorf("candidates not sorted by savings desc: %d <= %d",
			st.Candidates[0].EstimatedSavingsBytes, st.Candidates[1].EstimatedSavingsBytes)
	}
	if got := st.Candidates[0].Reasons; len(got) != 2 {
		t.Errorf("partition A reasons = %v, want [stale_schema fragmented]", got)
	}
	if st.EstimatedReclaimableBytes != st.Candidates[0].EstimatedSavingsBytes+st.Candidates[1].EstimatedSavingsBytes {
		t.Errorf("EstimatedReclaimableBytes=%d != sum of candidate savings", st.EstimatedReclaimableBytes)
	}

	// Per-level stats: L0 (1 file) and L2 (5 files) present, with ratios.
	byLevel := map[int]LevelStats{}
	for _, ls := range st.ByLevel {
		byLevel[ls.Level] = ls
	}
	if byLevel[2].Files != 5 {
		t.Errorf("L2 file count = %d, want 5", byLevel[2].Files)
	}
	if byLevel[2].CompressionRatio <= 1 {
		t.Errorf("L2 compression ratio = %v, want > 1", byLevel[2].CompressionRatio)
	}
	if byLevel[0].Files != 1 {
		t.Errorf("L0 file count = %d, want 1", byLevel[0].Files)
	}

	// CompactionCandidates() returns the same prioritized work-list.
	if cs := m.CompactionCandidates("v2"); len(cs) != 2 || cs[0].Partition != st.Candidates[0].Partition {
		t.Errorf("CompactionCandidates work-list mismatch: %+v", cs)
	}
}
