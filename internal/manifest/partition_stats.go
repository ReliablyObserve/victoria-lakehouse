package manifest

import (
	"sort"
	"time"
)

// PartitionStats holds pre-aggregated statistics for a single partition.
type PartitionStats struct {
	TotalRows  int64
	FileCount  int
	TotalBytes int64
}

// PartitionRowCount holds the row count for a single partition along with its time bounds.
type PartitionRowCount struct {
	StartNs  int64
	EndNs    int64
	RowCount int64
}

// GetPartitionStats returns pre-aggregated stats for every partition in the manifest.
func (m *Manifest) GetPartitionStats() map[string]PartitionStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]PartitionStats, len(m.files))
	for key, files := range m.files {
		var ps PartitionStats
		for _, fi := range files {
			ps.TotalRows += fi.RowCount
			ps.TotalBytes += fi.Size
			ps.FileCount++
		}
		result[key] = ps
	}
	return result
}

// GetRowCountForRange returns the total row count across all partitions whose
// time range overlaps [startNs, endNs). Uses the same binary search pattern
// as GetFilesForRange.
func (m *Manifest) GetRowCountForRange(startNs, endNs int64) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := time.Unix(0, startNs)
	end := time.Unix(0, endNs)

	// Binary search: find first partition whose end is after query start.
	idx := sort.Search(len(m.sortedPartitions), func(i int) bool {
		return m.sortedPartitions[i].end.After(start)
	})

	var total int64
	for i := idx; i < len(m.sortedPartitions); i++ {
		p := &m.sortedPartitions[i]
		// Stop once partition start is at or after query end.
		if !p.start.Before(end) {
			break
		}
		for _, fi := range m.files[p.key] {
			total += fi.RowCount
		}
	}
	return total
}

// GetRowCountsByPartition returns per-partition row counts with time bounds for
// all partitions whose time range overlaps [startNs, endNs). Results are
// returned in ascending start-time order. Used by hits/histogram queries to
// avoid S3 file reads.
func (m *Manifest) GetRowCountsByPartition(startNs, endNs int64) []PartitionRowCount {
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := time.Unix(0, startNs)
	end := time.Unix(0, endNs)

	// Binary search: find first partition whose end is after query start.
	idx := sort.Search(len(m.sortedPartitions), func(i int) bool {
		return m.sortedPartitions[i].end.After(start)
	})

	var result []PartitionRowCount
	for i := idx; i < len(m.sortedPartitions); i++ {
		p := &m.sortedPartitions[i]
		// Stop once partition start is at or after query end.
		if !p.start.Before(end) {
			break
		}
		var rowCount int64
		for _, fi := range m.files[p.key] {
			rowCount += fi.RowCount
		}
		result = append(result, PartitionRowCount{
			StartNs:  p.start.UnixNano(),
			EndNs:    p.end.UnixNano(),
			RowCount: rowCount,
		})
	}
	return result
}
