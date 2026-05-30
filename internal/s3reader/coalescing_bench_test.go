package s3reader

import (
	"testing"
)

func BenchmarkCoalescingReaderPreload(b *testing.B) {
	// Create a 1MB inner reader
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)
		ranges := make([]readRange, 100)
		for j := range ranges {
			ranges[j] = readRange{off: int64(j * 1024), length: 512}
		}
		_ = c.PreloadRanges(ranges)
		c.Clear()
	}
}

func BenchmarkCoalescingReaderCacheHit(b *testing.B) {
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	c := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)
	_ = c.PreloadRanges([]readRange{{off: 0, length: 1024 * 1024}})

	buf := make([]byte, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.ReadAt(buf, int64(i%1000)*1024)
	}
}

func BenchmarkCoalescingReaderCacheMiss(b *testing.B) {
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	c := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)
	// No preload — every read is a cache miss falling through to inner

	buf := make([]byte, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.ReadAt(buf, int64(i%250)*4096)
	}
}

func BenchmarkMergeRanges(b *testing.B) {
	ranges := make([]readRange, 200)
	for i := range ranges {
		ranges[i] = readRange{off: int64(i * 512), length: 256}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mergeRanges(ranges, 64*1024)
	}
}

// TestCoalescingReaderCacheClearedAfterQuery verifies Clear() releases memory.
func TestCoalescingReaderCacheClearedAfterQuery(t *testing.T) {
	data := make([]byte, 10*1024*1024) // 10MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	c := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)

	// Preload many ranges
	ranges := make([]readRange, 1000)
	for i := range ranges {
		ranges[i] = readRange{off: int64(i * 10240), length: 10240}
	}
	if err := c.PreloadRanges(ranges); err != nil {
		t.Fatalf("PreloadRanges: %v", err)
	}

	// Cache should have entries
	c.mu.Lock()
	beforeClear := len(c.cache)
	c.mu.Unlock()
	if beforeClear == 0 {
		t.Fatal("expected cache entries after PreloadRanges")
	}

	// Clear should empty cache
	c.Clear()
	c.mu.Lock()
	afterClear := len(c.cache)
	c.mu.Unlock()
	if afterClear != 0 {
		t.Errorf("cache should be empty after Clear(), got %d entries", afterClear)
	}
}

// TestCoalescingReaderUnboundedCacheGrowth documents that cache grows without bound.
func TestCoalescingReaderUnboundedCacheGrowth(t *testing.T) {
	data := make([]byte, 100*1024*1024) // 100MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	// Use gapThreshold of 1 to minimize merging, forcing many distinct cache entries.
	c := NewCoalescingReaderAt(inner, inner.Size(), 1)

	// Preload 10000 individual small ranges = many cache entries
	for i := 0; i < 10000; i++ {
		off := int64(i * 10240) // space ranges far enough apart to avoid merging
		if off+10 > int64(len(data)) {
			break
		}
		if err := c.PreloadRanges([]readRange{{off: off, length: 10}}); err != nil {
			t.Fatalf("PreloadRanges at iteration %d: %v", i, err)
		}
	}

	c.mu.Lock()
	entries := len(c.cache)
	c.mu.Unlock()

	// Document: cache grows to many entries with no eviction
	t.Logf("cache entries after 10000 preloads: %d (no eviction policy)", entries)
	if entries < 1000 {
		t.Logf("GOOD: cache appears bounded (entries=%d)", entries)
	}
}
