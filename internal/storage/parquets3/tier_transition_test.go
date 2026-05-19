package parquets3

import (
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
)

func TestTierTransition_HotToWarm(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)

	partition := "dt=2026-05-02/hour=10"
	numFiles := 360
	numRGs := 10

	// Simulate Tier 1: per-RG bloom entries
	for f := 0; f < numFiles; f++ {
		for rg := 0; rg < numRGs; rg++ {
			key := bloomindex.PerRGKey(fmt.Sprintf("%s/file%d.parquet", partition, f), rg)
			vals := make([]string, 200)
			for j := range vals {
				vals[j] = fmt.Sprintf("trace-%d-%d-%d", f, rg, j)
			}
			pi.AddFile(partition, key, map[string][]string{"trace_id": vals})
		}
	}

	idx := pi.GetPartition(partition)
	tier1Size := len(idx.Marshal())
	tier1Entries := idx.Len()
	t.Logf("Tier 1: %d entries, %d bytes (%.1f KB)", tier1Entries, tier1Size, float64(tier1Size)/1024)

	// Downgrade to Tier 2 (per-file)
	merged := bloomindex.DowngradeToPerFile(idx)
	tier2Size := len(merged.Marshal())
	tier2Entries := merged.Len()
	t.Logf("Tier 2: %d entries, %d bytes (%.1f KB)", tier2Entries, tier2Size, float64(tier2Size)/1024)

	if tier2Entries != numFiles {
		t.Errorf("tier 2 should have %d entries (per-file), got %d", numFiles, tier2Entries)
	}

	// Size reduction should be significant
	ratio := float64(tier1Size) / float64(tier2Size)
	t.Logf("Size reduction: %.1fx", ratio)
	if ratio < 2 {
		t.Errorf("tier2 should be at least 2x smaller, got %.1fx", ratio)
	}

	// Verify queries still work — no false negatives
	keys := make([]string, numFiles)
	for f := 0; f < numFiles; f++ {
		keys[f] = fmt.Sprintf("%s/file%d.parquet", partition, f)
	}

	for f := 0; f < numFiles; f += 50 { // spot check every 50th file
		for rg := 0; rg < numRGs; rg += 5 {
			val := fmt.Sprintf("trace-%d-%d-0", f, rg)
			result := merged.MayContain(keys, "trace_id", val)
			if !containsKey(result, keys[f]) {
				t.Fatalf("tier 2 missing trace in file%d: %s", f, val)
			}
		}
	}
}

func TestTierTransition_WarmToCold(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)

	partition := "dt=2026-05-02/hour=10"
	numFiles := 360

	// Tier 2: per-file entries
	for f := 0; f < numFiles; f++ {
		vals := make([]string, 200)
		for j := range vals {
			vals[j] = fmt.Sprintf("trace-%d-%d", f, j)
		}
		key := fmt.Sprintf("%s/file%d.parquet", partition, f)
		pi.AddFile(partition, key, map[string][]string{"trace_id": vals})
	}

	idx := pi.GetPartition(partition)
	tier2Size := len(idx.Marshal())

	// Downgrade to Tier 3 (summary)
	summary := bloomindex.DowngradeToSummary(idx)
	tier3Size := len(summary.Marshal())
	t.Logf("Tier 2→3: %d bytes → %d bytes (%.1f KB)", tier2Size, tier3Size, float64(tier3Size)/1024)

	if summary.Len() != 1 {
		t.Errorf("summary should have 1 entry, got %d", summary.Len())
	}

	// Summary answers "does this partition contain trace X?" — not "which file?"
	// So we check against SummaryKey
	for f := 0; f < numFiles; f += 50 {
		val := fmt.Sprintf("trace-%d-0", f)
		result := summary.MayContain([]string{bloomindex.SummaryKey}, "trace_id", val)
		if len(result) == 0 {
			t.Fatalf("summary should contain trace from file%d", f)
		}
	}

	// Non-inserted value should (usually) not match
	fpCount := 0
	for i := 0; i < 1000; i++ {
		result := summary.MayContain([]string{bloomindex.SummaryKey}, "trace_id", fmt.Sprintf("notinserted-%d", i))
		if len(result) > 0 {
			fpCount++
		}
	}
	// Summary has higher FPR due to many merged values, but should be bounded
	t.Logf("Summary FPR: %d/1000 = %.1f%%", fpCount, float64(fpCount)/10)
}

func TestTierTransition_ColdToArchive(t *testing.T) {
	// At archive tier, bloom is deleted. Only labels remain.
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"

	pi.AddFile(partition, "f1", map[string][]string{"trace_id": {"aaa"}})
	if pi.GetPartition(partition) == nil {
		t.Fatal("partition should exist")
	}

	// Simulate archive: remove partition bloom
	pi.RemovePartition(partition)
	if pi.GetPartition(partition) != nil {
		t.Error("archive tier should have no bloom")
	}
}

func TestTierTransition_QueryCorrectness_AllTiers(t *testing.T) {
	partition := "dt=2026-05-02/hour=10"
	numFiles := 50
	targetFile := 25
	targetTrace := "trace-25-0"

	// Build per-RG bloom (Tier 1)
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	for f := 0; f < numFiles; f++ {
		for rg := 0; rg < 5; rg++ {
			key := bloomindex.PerRGKey(fmt.Sprintf("%s/file%d.parquet", partition, f), rg)
			vals := make([]string, 50)
			for j := range vals {
				vals[j] = fmt.Sprintf("trace-%d-%d", f, j)
			}
			pi.AddFile(partition, key, map[string][]string{"trace_id": vals})
		}
	}
	idx := pi.GetPartition(partition)

	fileKeys := make([]string, numFiles)
	for f := 0; f < numFiles; f++ {
		fileKeys[f] = fmt.Sprintf("%s/file%d.parquet", partition, f)
	}

	// Tier 1: per-RG — query uses RG keys
	rgKeys := make([]string, 0)
	for f := 0; f < numFiles; f++ {
		for rg := 0; rg < 5; rg++ {
			rgKeys = append(rgKeys, bloomindex.PerRGKey(fileKeys[f], rg))
		}
	}
	tier1Result := idx.MayContain(rgKeys, "trace_id", targetTrace)
	t.Logf("Tier 1 (per-RG): %d/%d entries match", len(tier1Result), len(rgKeys))
	tier1Found := false
	for _, k := range tier1Result {
		fk, _, ok := bloomindex.ParseRGKey(k)
		if ok && fk == fileKeys[targetFile] {
			tier1Found = true
			break
		}
	}
	if !tier1Found {
		t.Error("Tier 1 should find target trace in target file's RG entries")
	}

	// Tier 2: per-file
	tier2 := bloomindex.DowngradeToPerFile(idx)
	tier2Result := tier2.MayContain(fileKeys, "trace_id", targetTrace)
	if !containsKey(tier2Result, fileKeys[targetFile]) {
		t.Error("Tier 2 should find target trace")
	}
	t.Logf("Tier 2 (per-file): %d/%d files match", len(tier2Result), numFiles)

	// Tier 3: summary — partition-level only
	tier3 := bloomindex.DowngradeToSummary(tier2)
	tier3Result := tier3.MayContain([]string{bloomindex.SummaryKey}, "trace_id", targetTrace)
	if len(tier3Result) == 0 {
		t.Error("Tier 3 summary should find target trace in partition")
	}

	// Tier 4: no bloom — would fall back to full scan (all files included)
}

func TestTierForAge_WithRealDurations(t *testing.T) {
	cfg := bloomindex.DefaultTierConfig()

	tests := []struct {
		desc string
		age  time.Duration
		want bloomindex.Tier
	}{
		{"just written", 0, bloomindex.TierHot},
		{"1 hour old", time.Hour, bloomindex.TierHot},
		{"6 days old", 6 * 24 * time.Hour, bloomindex.TierHot},
		{"8 days old", 8 * 24 * time.Hour, bloomindex.TierWarm},
		{"29 days old", 29 * 24 * time.Hour, bloomindex.TierWarm},
		{"45 days old", 45 * 24 * time.Hour, bloomindex.TierCold},
		{"89 days old", 89 * 24 * time.Hour, bloomindex.TierCold},
		{"100 days old", 100 * 24 * time.Hour, bloomindex.TierArchive},
		{"1 year old", 365 * 24 * time.Hour, bloomindex.TierArchive},
	}

	for _, tt := range tests {
		got := bloomindex.TierForAge(tt.age, cfg)
		if got != tt.want {
			t.Errorf("%s (age=%v): got %v, want %v", tt.desc, tt.age, got, tt.want)
		}
	}
}
