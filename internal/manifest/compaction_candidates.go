package manifest

import "sort"

// Estimated per-byte storage gains used to PRIORITIZE recompaction work (highest
// saving first). Rough hints for ranking + UI, not guarantees:
//   - re-promotion of stale-schema files into dedicated columns: ~9.5% logs / ~8%
//     traces measured; use a conservative blended value.
//   - merging fragmented top-level files: cross-file dictionary/dedup + per-file
//     footer & row-group overhead.
const (
	repromoteGainEstimate     = 0.09
	mergeOverheadGainEstimate = 0.03
)

// LevelStats summarizes all files at one compaction level — the shape of how
// compacted the data is (most bytes should live at the top level; a lot at L0/L1
// means a compaction backlog).
type LevelStats struct {
	Level            int     `json:"level"`
	Files            int     `json:"files"`
	Bytes            int64   `json:"bytes"`
	RawBytes         int64   `json:"raw_bytes"`
	AvgFileBytes     int64   `json:"avg_file_bytes"`
	CompressionRatio float64 `json:"compression_ratio"` // raw/compressed
	// ConfiguredZstd is the zstd level the compactor applies when WRITING this output
	// level (CompactionConfig.CompressionLevelByOutputLevel) — higher levels compress
	// harder. 0 when the schedule is unknown.
	ConfiguredZstd int `json:"configured_zstd"`
}

// CompactionCandidate flags a partition worth (re)compacting beyond the normal level
// policy, with an estimated storage saving so callers can pick the highest-value
// work first.
type CompactionCandidate struct {
	Partition string `json:"partition"`
	Files     int    `json:"files"`
	MaxLevel  int    `json:"max_level"`
	// StaleFiles: files written under an older schema fingerprint — they still carry
	// promoted attributes in the attribute map and need a re-promotion pass to gain
	// dedicated-column compression + cardinality.
	StaleFiles int `json:"stale_files"`
	// MaxLevelFiles: file count at the top level. The level policy only merges L0/L1,
	// so 2+ files stuck at >= L2 sit fragmented until a forced recompaction.
	MaxLevelFiles         int   `json:"max_level_files"`
	Bytes                 int64 `json:"bytes"`
	EstimatedSavingsBytes int64 `json:"estimated_savings_bytes"`
	// EstimatedBytesAfter is the partition's approximate size AFTER the next
	// recompaction (Bytes − EstimatedSavingsBytes) — the before/after for the UI.
	EstimatedBytesAfter int64 `json:"estimated_bytes_after"`
	// NextLevel is the output level a recompaction would write (MaxLevel+1) and
	// NextLevelZstd the zstd it would apply there — shows WHY recompaction helps.
	NextLevel     int      `json:"next_level"`
	NextLevelZstd int      `json:"next_level_zstd"`
	Reasons       []string `json:"reasons"`
}

// CompactionStats is the overall compaction-efficiency picture for the stats UI:
// how data is distributed across levels (and how it compresses at each), how much
// is stale (a re-promotion opportunity), how much is already well-compacted vs
// pending, and a PRIORITIZED list of partitions to recompact first for the best
// storage win — the work-list a forced trigger / future auto-compaction job picks
// up. Manifest-derived; no file reads.
type CompactionStats struct {
	TotalFiles       int          `json:"total_files"`
	TotalBytes       int64        `json:"total_bytes"`
	TotalRawBytes    int64        `json:"total_raw_bytes"`
	CompressionRatio float64      `json:"compression_ratio"`
	ByLevel          []LevelStats `json:"by_level"`
	// CompactedBytes are at >= L2 (well rolled-up); PendingBytes at L0/L1 await rollup.
	CompactedBytes       int64 `json:"compacted_bytes"`
	PendingBytes         int64 `json:"pending_bytes"`
	StaleSchemaFiles     int   `json:"stale_schema_files"`
	StaleSchemaBytes     int64 `json:"stale_schema_bytes"`
	FragmentedPartitions int   `json:"fragmented_partitions"`
	// EstimatedReclaimableBytes ≈ what a full recompaction pass would save (sum of the
	// per-candidate estimates). A hint for sizing the win, not a guarantee.
	EstimatedReclaimableBytes int64                 `json:"estimated_reclaimable_bytes"`
	Candidates                []CompactionCandidate `json:"candidates"`
}

// ComputeCompactionStats scans the manifest once and returns the full efficiency
// picture plus the prioritized candidate work-list. currentFP is the fingerprint new
// files use (parquets3.CurrentSchemaFingerprint); empty disables the stale check.
// zstdForLevel resolves the configured zstd level per output level (nil → 0 in the
// zstd fields). Read-only; safe from an HTTP handler.
func (m *Manifest) ComputeCompactionStats(currentFP string, zstdForLevel func(level int) int) CompactionStats {
	var st CompactionStats
	levelAgg := map[int]*LevelStats{}

	for partition, files := range m.AllFiles() {
		if len(files) == 0 {
			continue
		}
		maxLevel, stale := 0, 0
		var staleBytes, partBytes int64
		levelCounts := make(map[int]int, 4)
		for _, f := range files {
			st.TotalFiles++
			st.TotalBytes += f.Size
			st.TotalRawBytes += f.RawBytes
			if f.CompactionLevel >= 2 {
				st.CompactedBytes += f.Size
			} else {
				st.PendingBytes += f.Size
			}
			ls := levelAgg[f.CompactionLevel]
			if ls == nil {
				ls = &LevelStats{Level: f.CompactionLevel}
				levelAgg[f.CompactionLevel] = ls
			}
			ls.Files++
			ls.Bytes += f.Size
			ls.RawBytes += f.RawBytes

			if f.CompactionLevel > maxLevel {
				maxLevel = f.CompactionLevel
			}
			if currentFP != "" && f.SchemaFingerprint != currentFP {
				stale++
				staleBytes += f.Size
			}
			levelCounts[f.CompactionLevel]++
			partBytes += f.Size
		}

		var maxLevelBytes int64
		for _, f := range files {
			if f.CompactionLevel == maxLevel {
				maxLevelBytes += f.Size
			}
		}

		var reasons []string
		var savings int64
		if stale > 0 {
			reasons = append(reasons, "stale_schema")
			savings += int64(float64(staleBytes) * repromoteGainEstimate)
		}
		if maxLevel >= 2 && levelCounts[maxLevel] >= 2 {
			reasons = append(reasons, "fragmented")
			savings += int64(float64(maxLevelBytes) * mergeOverheadGainEstimate)
			st.FragmentedPartitions++
		}
		st.StaleSchemaFiles += stale
		st.StaleSchemaBytes += staleBytes

		if len(reasons) > 0 {
			next := maxLevel + 1
			nz := 0
			if zstdForLevel != nil {
				nz = zstdForLevel(next)
			}
			st.Candidates = append(st.Candidates, CompactionCandidate{
				Partition:             partition,
				Files:                 len(files),
				MaxLevel:              maxLevel,
				StaleFiles:            stale,
				MaxLevelFiles:         levelCounts[maxLevel],
				Bytes:                 partBytes,
				EstimatedSavingsBytes: savings,
				EstimatedBytesAfter:   partBytes - savings,
				NextLevel:             next,
				NextLevelZstd:         nz,
				Reasons:               reasons,
			})
		}
	}

	for _, ls := range levelAgg {
		if ls.Files > 0 {
			ls.AvgFileBytes = ls.Bytes / int64(ls.Files)
		}
		if ls.Bytes > 0 {
			ls.CompressionRatio = float64(ls.RawBytes) / float64(ls.Bytes)
		}
		if zstdForLevel != nil {
			ls.ConfiguredZstd = zstdForLevel(ls.Level)
		}
		st.ByLevel = append(st.ByLevel, *ls)
	}
	sort.Slice(st.ByLevel, func(i, j int) bool { return st.ByLevel[i].Level < st.ByLevel[j].Level })

	if st.TotalBytes > 0 {
		st.CompressionRatio = float64(st.TotalRawBytes) / float64(st.TotalBytes)
	}
	for _, c := range st.Candidates {
		st.EstimatedReclaimableBytes += c.EstimatedSavingsBytes
	}
	// Priority: highest estimated saving first — "what to compact first for the best
	// storage win" (ties broken by partition for stable output).
	sort.Slice(st.Candidates, func(i, j int) bool {
		if st.Candidates[i].EstimatedSavingsBytes != st.Candidates[j].EstimatedSavingsBytes {
			return st.Candidates[i].EstimatedSavingsBytes > st.Candidates[j].EstimatedSavingsBytes
		}
		return st.Candidates[i].Partition < st.Candidates[j].Partition
	})
	return st
}

// CompactionCandidates returns just the prioritized candidate work-list — the subset
// of ComputeCompactionStats the forced-recompaction trigger consumes.
func (m *Manifest) CompactionCandidates(currentFP string) []CompactionCandidate {
	return m.ComputeCompactionStats(currentFP, nil).Candidates
}
