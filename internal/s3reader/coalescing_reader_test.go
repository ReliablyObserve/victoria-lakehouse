package s3reader

import (
	"testing"
)

func TestMergeRanges_AdjacentMerge(t *testing.T) {
	ranges := []readRange{
		{off: 100, length: 100},
		{off: 250, length: 100},
		{off: 400, length: 100},
	}
	merged := mergeRanges(ranges, 64*1024)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged range, got %d", len(merged))
	}
	if merged[0].off != 100 || merged[0].length != 400 {
		t.Fatalf("expected [100, 500), got [%d, %d)", merged[0].off, merged[0].off+int64(merged[0].length))
	}
}

func TestMergeRanges_LargeGapNoMerge(t *testing.T) {
	ranges := []readRange{
		{off: 0, length: 100},
		{off: 100 * 1024, length: 100},
	}
	merged := mergeRanges(ranges, 64*1024)
	if len(merged) != 2 {
		t.Fatalf("expected 2 ranges (no merge), got %d", len(merged))
	}
}

func TestMergeRanges_SingleRange(t *testing.T) {
	ranges := []readRange{{off: 0, length: 100}}
	merged := mergeRanges(ranges, 64*1024)
	if len(merged) != 1 {
		t.Fatalf("expected 1 range, got %d", len(merged))
	}
}

func TestMergeRanges_OverlappingMerge(t *testing.T) {
	ranges := []readRange{
		{off: 100, length: 200},
		{off: 250, length: 200},
	}
	merged := mergeRanges(ranges, 64*1024)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged range, got %d", len(merged))
	}
	if merged[0].off != 100 || merged[0].length != 350 {
		t.Fatalf("expected [100, 450), got [%d, %d)", merged[0].off, merged[0].off+int64(merged[0].length))
	}
}

func TestMergeRanges_Empty(t *testing.T) {
	merged := mergeRanges(nil, 64*1024)
	if len(merged) != 0 {
		t.Fatalf("expected 0 ranges, got %d", len(merged))
	}
}

func TestCoalescingReaderAt_PreloadAndRead(t *testing.T) {
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	cr := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)

	err := cr.PreloadRanges([]readRange{
		{off: 1000, length: 500},
		{off: 2000, length: 500},
		{off: 3000, length: 500},
	})
	if err != nil {
		t.Fatalf("PreloadRanges: %v", err)
	}

	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 merged read, got %d", inner.readCalls.Load())
	}

	buf := make([]byte, 500)
	n, err := cr.ReadAt(buf, 2000)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 500 {
		t.Fatalf("expected 500 bytes, got %d", n)
	}
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 inner read (cache hit), got %d", inner.readCalls.Load())
	}

	for i := 0; i < 500; i++ {
		if buf[i] != data[2000+i] {
			t.Fatalf("data mismatch at %d", 2000+i)
		}
	}
}

func TestCoalescingReaderAt_FallbackToInner(t *testing.T) {
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	cr := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)

	// Read without preloading — should fall back to inner
	buf := make([]byte, 100)
	n, err := cr.ReadAt(buf, 500)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 100 {
		t.Fatalf("expected 100 bytes, got %d", n)
	}
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 inner read (cache miss), got %d", inner.readCalls.Load())
	}
	for i := 0; i < 100; i++ {
		if buf[i] != data[500+i] {
			t.Fatalf("data mismatch at %d", 500+i)
		}
	}
}

func TestCoalescingReaderAt_Clear(t *testing.T) {
	data := make([]byte, 1024)
	inner := &mockReaderAt{data: data}
	cr := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)

	_ = cr.PreloadRanges([]readRange{{off: 0, length: 100}})
	cr.Clear()

	// After clear, reads should fall through to inner
	buf := make([]byte, 100)
	before := inner.readCalls.Load()
	_, _ = cr.ReadAt(buf, 0)
	after := inner.readCalls.Load()
	if after <= before {
		t.Fatal("expected inner read after Clear(), got cache hit")
	}
}

func TestCoalescingReaderAt_PreloadEmpty(t *testing.T) {
	inner := &mockReaderAt{data: make([]byte, 1024)}
	cr := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)

	err := cr.PreloadRanges(nil)
	if err != nil {
		t.Fatalf("PreloadRanges(nil): %v", err)
	}
	if inner.readCalls.Load() != 0 {
		t.Fatalf("expected 0 reads for empty preload, got %d", inner.readCalls.Load())
	}
}
