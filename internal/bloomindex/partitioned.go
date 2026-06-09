package bloomindex

import (
	"strings"
	"sync"
)

type Granularity int

const (
	GranularityHour Granularity = iota
	GranularityDay
)

type PartitionedIndex struct {
	mu          sync.RWMutex
	partitions  map[string]*Index
	dirty       map[string]bool
	granularity Granularity
	fpRate      float64
}

func NewPartitionedIndex(g Granularity, fpRate float64) *PartitionedIndex {
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}
	return &PartitionedIndex{
		partitions:  make(map[string]*Index),
		dirty:       make(map[string]bool),
		granularity: g,
		fpRate:      fpRate,
	}
}

func (pi *PartitionedIndex) AddFile(partition, key string, columnValues map[string][]string) {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	idx, ok := pi.partitions[partition]
	if !ok {
		idx = New()
		pi.partitions[partition] = idx
	}

	cols := BuildFileColumns(columnValues, pi.fpRate)
	if len(cols) > 0 {
		idx.AddColumns(key, cols)
		pi.dirty[partition] = true
	}
}

// BuildFileColumns builds the per-column bloom filters for a single file's
// distinct column values, skipping columns below the bloom threshold. Shared by
// PartitionedIndex.AddFile and the pmeta bloomFacet so both paths produce
// identical blooms — the dual-write parity gate depends on this.
func BuildFileColumns(columnValues map[string][]string, fpRate float64) map[string]*Filter {
	cols := make(map[string]*Filter, len(columnValues))
	for col, vals := range columnValues {
		if ShouldSkipBloom(len(vals)) {
			continue
		}
		f := NewFilter(len(vals), fpRate)
		for _, v := range vals {
			f.Add(v)
		}
		cols[col] = f
	}
	return cols
}

func (pi *PartitionedIndex) GetPartition(partition string) *Index {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return pi.partitions[partition]
}

func (pi *PartitionedIndex) SetPartition(partition string, idx *Index) {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	pi.partitions[partition] = idx
	pi.dirty[partition] = true
}

func (pi *PartitionedIndex) MarshalPartition(partition string) []byte {
	pi.mu.RLock()
	idx, ok := pi.partitions[partition]
	pi.mu.RUnlock()
	if !ok {
		return nil
	}
	return idx.Marshal()
}

func (pi *PartitionedIndex) DirtyPartitions() []string {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	result := make([]string, 0, len(pi.dirty))
	for p := range pi.dirty {
		result = append(result, p)
	}
	return result
}

func (pi *PartitionedIndex) ClearDirty(partition string) {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	delete(pi.dirty, partition)
}

func (pi *PartitionedIndex) PartitionKey(fileKey string) string {
	switch pi.granularity {
	case GranularityDay:
		if idx := strings.Index(fileKey, "/hour="); idx >= 0 {
			return fileKey[:idx]
		}
		if idx := strings.Index(fileKey, "dt="); idx >= 0 {
			end := strings.IndexByte(fileKey[idx:], '/')
			if end < 0 {
				return fileKey[idx:]
			}
			return fileKey[idx : idx+end]
		}
		return fileKey
	default: // GranularityHour
		if idx := strings.Index(fileKey, "/hour="); idx >= 0 {
			hourEnd := idx + len("/hour=")
			for hourEnd < len(fileKey) && fileKey[hourEnd] != '/' {
				hourEnd++
			}
			return fileKey[:hourEnd]
		}
		return fileKey
	}
}

func (pi *PartitionedIndex) Len() int {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return len(pi.partitions)
}

func (pi *PartitionedIndex) RemovePartition(partition string) {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	delete(pi.partitions, partition)
	delete(pi.dirty, partition)
}
