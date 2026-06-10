package s3reader

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"testing"
)

func s3ForceGC() {
	runtime.GC()
	runtime.GC()
}

func s3HeapInUse() uint64 {
	var m runtime.MemStats
	s3ForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// fakeReaderAt is a simple in-memory io.ReaderAt backed by a byte slice.
type fakeReaderAt struct {
	data []byte
}

func (f *fakeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if off+int64(n) >= int64(len(f.data)) {
		return n, io.EOF
	}
	return n, nil
}

func (f *fakeReaderAt) Size() int64 {
	return int64(len(f.data))
}

func TestMemLeak_BufferedReaderAt_ReadCycles(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 1*1024*1024) // 1MB
	inner := &fakeReaderAt{data: data}

	// Warm up
	for i := 0; i < 20; i++ {
		br := NewBufferedReaderAt(inner, inner.Size(), 64*1024, 64*1024)
		buf := make([]byte, 512)
		_, _ = br.ReadAt(buf, 0)
	}
	s3ForceGC()

	before := s3HeapInUse()

	const iterations = 5000
	buf := make([]byte, 512)
	for i := 0; i < iterations; i++ {
		br := NewBufferedReaderAt(inner, inner.Size(), 64*1024, 64*1024)
		off := int64((i * 1024) % (len(data) - 512))
		_, _ = br.ReadAt(buf, off)
		// br goes out of scope here — GC should reclaim it
	}

	s3ForceGC()
	after := s3HeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d BufferedReaderAt create/read cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_CoalescingReaderAt_CreateDiscard(t *testing.T) {
	data := bytes.Repeat([]byte("y"), 512*1024) // 512KB
	inner := &fakeReaderAt{data: data}

	// Warm up
	for i := 0; i < 20; i++ {
		cr := NewCoalescingReaderAt(inner, inner.Size(), 0)
		cr.Clear()
	}
	s3ForceGC()

	before := s3HeapInUse()

	const iterations = 5000
	buf := make([]byte, 256)
	for i := 0; i < iterations; i++ {
		cr := NewCoalescingReaderAt(inner, inner.Size(), 0)
		off := int64((i * 256) % (len(data) - 256))
		_, _ = cr.ReadAt(buf, off)
		cr.Clear()
		// cr goes out of scope — GC reclaims
	}

	s3ForceGC()
	after := s3HeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d CoalescingReaderAt create/clear cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_CoalescingReaderAt_PreloadAndClear(t *testing.T) {
	const fileSize = 1 * 1024 * 1024 // 1MB
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &fakeReaderAt{data: data}

	// Warm up
	cr := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)
	for i := 0; i < 10; i++ {
		ranges := []readRange{{off: int64(i * 1024), length: 512}}
		_ = cr.PreloadRanges(ranges)
		cr.Clear()
	}
	s3ForceGC()

	before := s3HeapInUse()

	const iterations = 2000
	for i := 0; i < iterations; i++ {
		// Each iteration: preload N ranges then clear
		ranges := make([]readRange, 5)
		for j := 0; j < 5; j++ {
			ranges[j] = readRange{
				off:    int64(((i*5 + j) * 1024) % (fileSize - 4096)),
				length: 512,
			}
		}
		if err := cr.PreloadRanges(ranges); err != nil {
			t.Fatalf("PreloadRanges failed: %v", err)
		}
		cr.Clear()
	}

	s3ForceGC()
	after := s3HeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d PreloadRanges+Clear cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_MergeRanges_Repeated(t *testing.T) {
	// Warm up
	for i := 0; i < 50; i++ {
		ranges := []readRange{{off: 0, length: 100}, {off: 110, length: 100}}
		_ = mergeRanges(ranges, 64*1024)
	}
	s3ForceGC()

	before := s3HeapInUse()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		ranges := []readRange{
			{off: int64(i * 100), length: 50},
			{off: int64(i*100 + 60), length: 50},
			{off: int64(i*100 + 200), length: 50},
		}
		result := mergeRanges(ranges, 64*1024)
		_ = fmt.Sprintf("%d", len(result)) // prevent optimizer
	}

	s3ForceGC()
	after := s3HeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d mergeRanges cycles (max %d)", growth, iterations, maxAllowed)
	}
}
