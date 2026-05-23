package compaction

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func makeFiles(level int, fp string, count int) []manifest.FileInfo {
	files := make([]manifest.FileInfo, count)
	for i := range files {
		files[i] = manifest.FileInfo{
			Key:               "file",
			CompactionLevel:   level,
			SchemaFingerprint: fp,
		}
	}
	return files
}

func TestLevelPolicy_EligibleL0(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)
	files := makeFiles(0, "fp1", 12)
	partitionTime := time.Now().Add(-2 * time.Hour)
	level, eligible := p.Eligible(files, partitionTime)
	if !eligible {
		t.Fatal("expected eligible=true")
	}
	if level != 0 {
		t.Fatalf("expected level=0, got %d", level)
	}
}

func TestLevelPolicy_NotEligibleTooFewFiles(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)
	files := makeFiles(0, "fp1", 5)
	partitionTime := time.Now().Add(-2 * time.Hour)
	_, eligible := p.Eligible(files, partitionTime)
	if eligible {
		t.Fatal("expected eligible=false with only 5 L0 files")
	}
}

func TestLevelPolicy_NotEligibleTooRecent(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)
	files := makeFiles(0, "fp1", 12)
	partitionTime := time.Now() // too recent
	_, eligible := p.Eligible(files, partitionTime)
	if eligible {
		t.Fatal("expected eligible=false for recent partition")
	}
}

func TestLevelPolicy_EligibleL1(t *testing.T) {
	p := NewLevelPolicy(10, 10, time.Hour)
	partitionTime := time.Now().Add(-2 * time.Hour)
	// 5 L0 files (below threshold) + 10 L1 files (at threshold)
	files := append(makeFiles(0, "fp1", 5), makeFiles(1, "fp1", 10)...)
	level, eligible := p.Eligible(files, partitionTime)
	if !eligible {
		t.Fatal("expected eligible=true for L1 compaction")
	}
	if level != 1 {
		t.Fatalf("expected level=1, got %d", level)
	}
}

func TestLevelPolicy_L0PrioritizedOverL1(t *testing.T) {
	p := NewLevelPolicy(10, 10, time.Hour)
	partitionTime := time.Now().Add(-2 * time.Hour)
	// 12 L0 (above threshold) + 13 L1 (above threshold) → L0 wins
	files := append(makeFiles(0, "fp1", 12), makeFiles(1, "fp1", 13)...)
	level, eligible := p.Eligible(files, partitionTime)
	if !eligible {
		t.Fatal("expected eligible=true")
	}
	if level != 0 {
		t.Fatalf("expected level=0 (L0 prioritized), got %d", level)
	}
}

func TestLevelPolicy_EligibleDailyRollup(t *testing.T) {
	p := &LevelPolicy{
		MinFilesL0:     10,
		MinFilesL1:     15,
		MinAge:         time.Hour,
		DailyRollupAge: 24 * time.Hour,
	}
	// Partition 48h old with 3 L1 files — eligible for daily rollup
	files := makeFiles(1, "fp1", 3)
	partitionTime := time.Now().Add(-48 * time.Hour)
	level, eligible := p.Eligible(files, partitionTime)
	if !eligible {
		t.Fatal("expected eligible=true for daily rollup (48h old, 3 L1 files)")
	}
	if level != 1 {
		t.Fatalf("expected level=1, got %d", level)
	}
}

func TestLevelPolicy_DailyRollupNotEligibleTooRecent(t *testing.T) {
	p := &LevelPolicy{
		MinFilesL0:     10,
		MinFilesL1:     15,
		MinAge:         time.Hour,
		DailyRollupAge: 24 * time.Hour,
	}
	// Partition only 6h old with 3 L1 files — not eligible (too recent for daily rollup)
	files := makeFiles(1, "fp1", 3)
	partitionTime := time.Now().Add(-6 * time.Hour)
	_, eligible := p.Eligible(files, partitionTime)
	if eligible {
		t.Fatal("expected eligible=false for partition only 6h old")
	}
}

func TestLevelPolicy_DailyRollupNeedsMultipleFiles(t *testing.T) {
	p := &LevelPolicy{
		MinFilesL0:     10,
		MinFilesL1:     15,
		MinAge:         time.Hour,
		DailyRollupAge: 24 * time.Hour,
	}
	// Only 1 L1 file in a 48h old partition — nothing to merge
	files := makeFiles(1, "fp1", 1)
	partitionTime := time.Now().Add(-48 * time.Hour)
	_, eligible := p.Eligible(files, partitionTime)
	if eligible {
		t.Fatal("expected eligible=false with only 1 L1 file")
	}
}

func TestLevelPolicy_SelectFiles(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)
	files := []manifest.FileInfo{
		{Key: "a", CompactionLevel: 0, SchemaFingerprint: "fp1"},
		{Key: "b", CompactionLevel: 0, SchemaFingerprint: "fp2"},
		{Key: "c", CompactionLevel: 1, SchemaFingerprint: "fp1"},
		{Key: "d", CompactionLevel: 0, SchemaFingerprint: "fp1"},
	}
	selected := p.SelectFiles(files, 0, "fp1")
	if len(selected) != 2 {
		t.Fatalf("expected 2 files at level=0 with fp1, got %d", len(selected))
	}
	for _, f := range selected {
		if f.CompactionLevel != 0 || f.SchemaFingerprint != "fp1" {
			t.Errorf("unexpected file in selection: %+v", f)
		}
	}
}
