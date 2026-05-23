package s3reader

import (
	"io"
	"sort"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type readRange struct {
	off    int64
	length int
}

func mergeRanges(ranges []readRange, gapThreshold int64) []readRange {
	if len(ranges) <= 1 {
		return ranges
	}
	// Copy to avoid mutating the caller's slice.
	sorted := make([]readRange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].off < sorted[j].off
	})
	merged := []readRange{sorted[0]}
	for _, r := range sorted[1:] {
		last := &merged[len(merged)-1]
		lastEnd := last.off + int64(last.length)
		gap := r.off - lastEnd
		if gap <= gapThreshold {
			newEnd := r.off + int64(r.length)
			if newEnd > lastEnd {
				last.length = int(newEnd - last.off)
			}
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

// CoalescingReaderAt wraps an io.ReaderAt and merges nearby range reads
// into single fetches to reduce request count.
type CoalescingReaderAt struct {
	inner        io.ReaderAt
	fileSize     int64
	gapThreshold int64
	mu           sync.Mutex
	cache        map[int64][]byte
}

// NewCoalescingReaderAt creates a CoalescingReaderAt wrapping inner.
// Ranges within gapThreshold bytes of each other are merged into a single read.
// Default gap threshold is 64KB if gapThreshold <= 0.
func NewCoalescingReaderAt(inner io.ReaderAt, fileSize int64, gapThreshold int64) *CoalescingReaderAt {
	if gapThreshold <= 0 {
		gapThreshold = 64 * 1024
	}
	const maxGapThreshold = 1024 * 1024 // 1MB safety cap
	if gapThreshold > maxGapThreshold {
		gapThreshold = maxGapThreshold
	}
	return &CoalescingReaderAt{
		inner:        inner,
		fileSize:     fileSize,
		gapThreshold: gapThreshold,
		cache:        make(map[int64][]byte),
	}
}

// PreloadRanges merges the given ranges and fetches them from the inner reader,
// caching the results for subsequent ReadAt calls.
func (c *CoalescingReaderAt) PreloadRanges(ranges []readRange) error {
	if len(ranges) == 0 {
		return nil
	}
	merged := mergeRanges(ranges, c.gapThreshold)
	metrics.S3CoalescedRanges.Add(len(ranges) - len(merged))
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, mr := range merged {
		buf := make([]byte, mr.length)
		n, err := c.inner.ReadAt(buf, mr.off)
		if err != nil && err != io.EOF {
			return err
		}
		c.cache[mr.off] = buf[:n]
	}
	return nil
}

// ReadAt reads len(p) bytes at offset off. If the range is covered by a
// preloaded cache entry, the data is served from cache without an inner read.
func (c *CoalescingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	for cacheOff, data := range c.cache {
		cacheEnd := cacheOff + int64(len(data))
		if off >= cacheOff && off+int64(len(p)) <= cacheEnd {
			n := copy(p, data[off-cacheOff:])
			c.mu.Unlock()
			return n, nil
		}
	}
	c.mu.Unlock()
	return c.inner.ReadAt(p, off)
}

// Clear evicts all cached data, allowing the GC to reclaim memory
// after a query completes.
func (c *CoalescingReaderAt) Clear() {
	c.mu.Lock()
	c.cache = make(map[int64][]byte)
	c.mu.Unlock()
}
