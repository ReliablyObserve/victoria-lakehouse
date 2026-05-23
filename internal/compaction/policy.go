package compaction

import (
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

type LevelPolicy struct {
	MinFilesL0     int
	MinFilesL1     int
	MinAge         time.Duration
	DailyRollupAge time.Duration
}

func NewLevelPolicy(minFilesL0, minFilesL1 int, minAge time.Duration) *LevelPolicy {
	return &LevelPolicy{MinFilesL0: minFilesL0, MinFilesL1: minFilesL1, MinAge: minAge}
}

// Eligible checks if a partition needs compaction. L0→L1 is prioritized over L1→L2.
func (p *LevelPolicy) Eligible(files []manifest.FileInfo, partitionTime time.Time) (level int, eligible bool) {
	if time.Since(partitionTime) < p.MinAge {
		return 0, false
	}
	l0Count := countAtLevel(files, 0)
	l1Count := countAtLevel(files, 1)
	if l0Count >= p.MinFilesL0 {
		return 0, true
	}
	if l1Count >= p.MinFilesL1 {
		return 1, true
	}
	// Daily rollup: merge any L1 files (≥2) in partitions older than DailyRollupAge.
	if p.DailyRollupAge > 0 && time.Since(partitionTime) >= p.DailyRollupAge && l1Count >= 2 {
		return 1, true
	}
	return 0, false
}

// SelectFiles returns files at the given level matching the schema fingerprint.
func (p *LevelPolicy) SelectFiles(files []manifest.FileInfo, level int, schemaFP string) []manifest.FileInfo {
	var selected []manifest.FileInfo
	for _, f := range files {
		if f.CompactionLevel == level && f.SchemaFingerprint == schemaFP {
			selected = append(selected, f)
		}
	}
	return selected
}

func countAtLevel(files []manifest.FileInfo, level int) int {
	n := 0
	for _, f := range files {
		if f.CompactionLevel == level {
			n++
		}
	}
	return n
}

// MajoritySchemaFingerprint returns the most common schema fingerprint at the given level.
func MajoritySchemaFingerprint(files []manifest.FileInfo, level int) string {
	counts := make(map[string]int)
	for _, f := range files {
		if f.CompactionLevel == level {
			counts[f.SchemaFingerprint]++
		}
	}
	var best string
	var bestCount int
	for fp, c := range counts {
		if c > bestCount {
			best = fp
			bestCount = c
		}
	}
	return best
}
