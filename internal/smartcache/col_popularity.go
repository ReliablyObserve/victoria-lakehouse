package smartcache

import (
	"sort"
	"sync"
)

// ColumnPopularity tracks column access frequency for adaptive prefetch decisions.
// It is safe for concurrent use.
type ColumnPopularity struct {
	mu     sync.RWMutex
	counts map[string]int64
}

// NewColumnPopularity creates a new ColumnPopularity tracker.
func NewColumnPopularity() *ColumnPopularity {
	return &ColumnPopularity{counts: make(map[string]int64)}
}

// Record increments the access count for the given column.
func (cp *ColumnPopularity) Record(column string) {
	cp.mu.Lock()
	cp.counts[column]++
	cp.mu.Unlock()
}

// TopN returns the n most frequently accessed columns, ordered by descending count.
// If fewer than n columns have been recorded, all columns are returned.
func (cp *ColumnPopularity) TopN(n int) []string {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	type kv struct {
		col   string
		count int64
	}
	sorted := make([]kv, 0, len(cp.counts))
	for c, cnt := range cp.counts {
		sorted = append(sorted, kv{c, cnt})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].count != sorted[j].count {
			return sorted[i].count > sorted[j].count
		}
		// Stable tie-breaking by column name for deterministic output.
		return sorted[i].col < sorted[j].col
	})
	if n > len(sorted) {
		n = len(sorted)
	}
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = sorted[i].col
	}
	return result
}
