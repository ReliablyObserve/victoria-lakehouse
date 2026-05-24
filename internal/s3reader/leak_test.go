package s3reader

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// memReaderAt is a simple in-memory ReaderAtSizer for leak tests,
// avoiding any S3 dependency.
type memReaderAt struct {
	data      []byte
	readCalls atomic.Int64
}

func (m *memReaderAt) ReadAt(p []byte, off int64) (int, error) {
	m.readCalls.Add(1)
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	if end > int64(len(m.data)) {
		end = int64(len(m.data))
		n := copy(p, m.data[off:end])
		return n, io.EOF
	}
	n := copy(p, m.data[off:end])
	return n, nil
}

func (m *memReaderAt) Size() int64 {
	return int64(len(m.data))
}

func forceGC() {
	runtime.GC()
	runtime.GC()
}

func heapInUse() uint64 {
	var m runtime.MemStats
	forceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// --- BufferedS3ReaderAt ---

func TestBufferedReaderAt_NoMemoryLeak_ReadCycles(t *testing.T) {
	data := make([]byte, 1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &memReaderAt{data: data}

	// Warm up: create, use, discard several times.
	for i := 0; i < 50; i++ {
		br := NewBufferedReaderAt(inner, inner.Size(), 64*1024)
		buf := make([]byte, 4*1024)
		for off := int64(0); off < inner.Size(); off += int64(len(buf)) * 4 {
			_, _ = br.ReadAt(buf, off)
		}
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 1000; i++ {
		br := NewBufferedReaderAt(inner, inner.Size(), 64*1024)
		buf := make([]byte, 4*1024)
		for off := int64(0); off < inner.Size(); off += int64(len(buf)) * 4 {
			_, _ = br.ReadAt(buf, off)
		}
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 1000 BufferedReaderAt create/use cycles (max %d)", growth, maxGrowth)
	}
}

func TestBufferedReaderAt_NoMemoryLeak_ConcurrentReads(t *testing.T) {
	data := make([]byte, 2*1024*1024) // 2MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &memReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 256*1024)

	// Warm up with concurrent reads.
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1024)
			for i := 0; i < 100; i++ {
				off := int64(i*1024) % inner.Size()
				_, _ = br.ReadAt(buf, off)
			}
		}()
	}
	wg.Wait()
	forceGC()

	before := heapInUse()

	for round := 0; round < 50; round++ {
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				buf := make([]byte, 1024)
				for i := 0; i < 200; i++ {
					off := int64(i*1024) % inner.Size()
					_, _ = br.ReadAt(buf, off)
				}
			}()
		}
		wg.Wait()
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after concurrent read cycles (max %d)", growth, maxGrowth)
	}
}

// --- CoalescingReaderAt ---

func TestCoalescingReaderAt_NoMemoryLeak_PreloadClearCycles(t *testing.T) {
	data := make([]byte, 512*1024) // 512KB
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &memReaderAt{data: data}

	// Warm up.
	for i := 0; i < 50; i++ {
		cr := NewCoalescingReaderAt(inner, inner.Size(), 16*1024)
		ranges := make([]readRange, 10)
		for j := range ranges {
			ranges[j] = readRange{off: int64(j) * 4096, length: 4096}
		}
		_ = cr.PreloadRanges(ranges)
		buf := make([]byte, 4096)
		for _, r := range ranges {
			_, _ = cr.ReadAt(buf, r.off)
		}
		cr.Clear()
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 1000; i++ {
		cr := NewCoalescingReaderAt(inner, inner.Size(), 16*1024)
		ranges := make([]readRange, 10)
		for j := range ranges {
			ranges[j] = readRange{off: int64(j) * 4096, length: 4096}
		}
		_ = cr.PreloadRanges(ranges)
		buf := make([]byte, 4096)
		for _, r := range ranges {
			_, _ = cr.ReadAt(buf, r.off)
		}
		cr.Clear()
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 1000 CoalescingReaderAt preload/clear cycles (max %d)", growth, maxGrowth)
	}
}

func TestCoalescingReaderAt_NoMemoryLeak_RepeatedPreloads(t *testing.T) {
	data := make([]byte, 256*1024) // 256KB
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &memReaderAt{data: data}
	cr := NewCoalescingReaderAt(inner, inner.Size(), 8*1024)

	// Warm up: preload, read, clear many times.
	for i := 0; i < 100; i++ {
		ranges := []readRange{{off: int64(i%50) * 1024, length: 1024}}
		_ = cr.PreloadRanges(ranges)
		cr.Clear()
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 50_000; i++ {
		ranges := []readRange{{off: int64(i%200) * 1024, length: 1024}}
		_ = cr.PreloadRanges(ranges)
		if i%100 == 0 {
			cr.Clear()
		}
	}
	cr.Clear()
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 50K preload cycles (max %d)", growth, maxGrowth)
	}
}

func TestMergeRanges_NoMemoryLeak(t *testing.T) {
	// Warm up.
	for i := 0; i < 100; i++ {
		ranges := make([]readRange, 50)
		for j := range ranges {
			ranges[j] = readRange{off: int64(j) * 100, length: 50}
		}
		_ = mergeRanges(ranges, 60)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		ranges := make([]readRange, 20)
		for j := range ranges {
			ranges[j] = readRange{off: int64(j) * 100, length: 50}
		}
		_ = mergeRanges(ranges, 60)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 100K mergeRanges cycles (max %d)", growth, maxGrowth)
	}
}

func TestBufferedReaderAt_NoGoroutineLeak(t *testing.T) {
	data := make([]byte, 64*1024)
	inner := &memReaderAt{data: data}

	before := runtime.NumGoroutine()

	for i := 0; i < 100; i++ {
		br := NewBufferedReaderAt(inner, inner.Size(), 8*1024)
		buf := make([]byte, 1024)
		_, _ = br.ReadAt(buf, 0)
		_, _ = br.ReadAt(buf, 4096)
		// BufferedReaderAt has no Close/goroutines; just discard.
		_ = fmt.Sprintf("%v", br.Size()) // ensure not optimized away
	}

	runtime.GC()
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}
