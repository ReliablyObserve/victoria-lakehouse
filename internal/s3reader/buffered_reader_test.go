package s3reader

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

// mockReaderAt tracks ReadAt calls for verifying buffered reader reduces S3 requests.
type mockReaderAt struct {
	data      []byte
	readCalls atomic.Int64
}

func (m *mockReaderAt) ReadAt(p []byte, off int64) (int, error) {
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

func (m *mockReaderAt) Size() int64 {
	return int64(len(m.data))
}

func TestBufferedReaderAt_BufferHit(t *testing.T) {
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 256*1024)

	buf := make([]byte, 100)
	n, err := br.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if n != 100 {
		t.Fatalf("expected 100 bytes, got %d", n)
	}
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 inner read, got %d", inner.readCalls.Load())
	}

	n, err = br.ReadAt(buf, 100)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if n != 100 {
		t.Fatalf("expected 100 bytes, got %d", n)
	}
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 inner read (buffer hit), got %d", inner.readCalls.Load())
	}

	for i := 0; i < 100; i++ {
		if buf[i] != data[100+i] {
			t.Fatalf("data mismatch at offset %d: got %d, want %d", 100+i, buf[i], data[100+i])
		}
	}
}

func TestBufferedReaderAt_BufferMiss(t *testing.T) {
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 256*1024)

	buf := make([]byte, 100)
	_, _ = br.ReadAt(buf, 0)
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", inner.readCalls.Load())
	}

	_, _ = br.ReadAt(buf, 500*1024)
	if inner.readCalls.Load() != 2 {
		t.Fatalf("expected 2 calls (buffer miss), got %d", inner.readCalls.Load())
	}

	for i := 0; i < 100; i++ {
		expected := data[500*1024+i]
		if buf[i] != expected {
			t.Fatalf("data mismatch at %d: got %d, want %d", 500*1024+i, buf[i], expected)
		}
	}
}

func TestBufferedReaderAt_EOF(t *testing.T) {
	data := []byte("hello world")
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024)

	buf := make([]byte, 100)
	n, err := br.ReadAt(buf, 5)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 6 {
		t.Fatalf("expected 6 bytes, got %d", n)
	}
	if string(buf[:n]) != " world" {
		t.Fatalf("expected ' world', got %q", string(buf[:n]))
	}

	_, err = br.ReadAt(buf, 100)
	if err != io.EOF {
		t.Fatalf("expected io.EOF for read past end, got %v", err)
	}
}

func TestBufferedReaderAt_ReducesS3Calls(t *testing.T) {
	fileSize := 5 * 1024 * 1024
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 2*1024*1024)

	buf := make([]byte, 8*1024)
	for i := 0; i < 50; i++ {
		off := int64(i) * 8 * 1024
		n, err := br.ReadAt(buf, off)
		if err != nil && err != io.EOF {
			t.Fatalf("read %d: %v", i, err)
		}
		if n != 8*1024 {
			t.Fatalf("read %d: expected 8KB, got %d bytes", i, n)
		}
	}

	calls := inner.readCalls.Load()
	if calls > 3 {
		t.Fatalf("expected ≤3 S3 calls for 50 sequential reads with 2MB buffer, got %d", calls)
	}
	t.Logf("50 sequential 8KB reads → %d S3 calls (down from 50)", calls)
}

func TestBufferedReaderAt_ConcurrentReads(t *testing.T) {
	fileSize := 2 * 1024 * 1024 // 2MB
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 256*1024)

	const numGoroutines = 4
	const readsPerGoroutine = 100

	var wg sync.WaitGroup
	errs := make(chan error, numGoroutines*readsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			buf := make([]byte, 1024)
			for i := 0; i < readsPerGoroutine; i++ {
				// Each goroutine reads from a different region to force buffer misses.
				off := int64((id*readsPerGoroutine + i) * 1024 % fileSize)
				n, err := br.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					errs <- err
					return
				}
				// Verify data correctness.
				for j := 0; j < n; j++ {
					expected := byte((int64(j) + off) % 256)
					if buf[j] != expected {
						errs <- io.ErrUnexpectedEOF // signal data corruption
						return
					}
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent read error: %v", err)
	}
	t.Logf("%d goroutines × %d reads completed without data corruption", numGoroutines, readsPerGoroutine)
}

func TestBufferedReaderAt_ZeroLengthRead(t *testing.T) {
	data := []byte("hello")
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024)

	n, err := br.ReadAt([]byte{}, 0)
	if n != 0 || err != nil {
		t.Fatalf("zero-length read: got n=%d, err=%v, want n=0, err=nil", n, err)
	}
}

func TestBufferedReaderAt_DefaultPrefetch(t *testing.T) {
	data := make([]byte, 4*1024*1024) // 4MB
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 0) // 0 → should default to 2MB

	buf := make([]byte, 100)
	_, _ = br.ReadAt(buf, 0)
	// Read at 1.5MB — should be within 2MB default prefetch
	_, _ = br.ReadAt(buf, 1500*1024)
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 inner read (within 2MB default prefetch), got %d", inner.readCalls.Load())
	}
	// Read at 2.5MB — outside default prefetch, should trigger new fetch
	_, _ = br.ReadAt(buf, 2500*1024)
	if inner.readCalls.Load() != 2 {
		t.Fatalf("expected 2 inner reads (past 2MB default prefetch), got %d", inner.readCalls.Load())
	}
}

func TestBufferedReaderAt_LargeReadBeyondPrefetch(t *testing.T) {
	// Verify that a read larger than the prefetch window succeeds mid-file
	// (the io.ReaderAt contract fix).
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	// Small prefetch (4KB) with a large read request (64KB).
	br := NewBufferedReaderAt(inner, inner.Size(), 4*1024)

	buf := make([]byte, 64*1024)
	n, err := br.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("large read at offset 0: expected nil error, got %v", err)
	}
	if n != 64*1024 {
		t.Fatalf("expected %d bytes, got %d", 64*1024, n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte(i%256) {
			t.Fatalf("data mismatch at offset %d: got %d, want %d", i, buf[i], byte(i%256))
		}
	}

	// Mid-file large read should also work without returning io.EOF.
	n, err = br.ReadAt(buf, 100*1024)
	if err != nil {
		t.Fatalf("large read at offset 100KB: expected nil error, got %v", err)
	}
	if n != 64*1024 {
		t.Fatalf("expected %d bytes, got %d", 64*1024, n)
	}
}
