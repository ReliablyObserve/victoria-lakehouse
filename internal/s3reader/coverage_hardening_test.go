package s3reader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestDownloadRange_ReadsPartialObject exercises DownloadRange (previously 0% covered).
func TestDownloadRange_ReadsPartialObject(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	handler.objects["data/range.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	data, err := pool.DownloadRange(context.Background(), "data/range.bin", 10, 4)
	if err != nil {
		t.Fatalf("DownloadRange error: %v", err)
	}
	if string(data) != "ABCD" {
		t.Errorf("got %q, want %q", string(data), "ABCD")
	}
}

// TestDownloadRange_FullObject exercises DownloadRange reading the entire object.
func TestDownloadRange_FullObject(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("complete-data")
	handler.objects["data/full-range.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	data, err := pool.DownloadRange(context.Background(), "data/full-range.bin", 0, int64(len(testData)))
	if err != nil {
		t.Fatalf("DownloadRange error: %v", err)
	}
	if string(data) != string(testData) {
		t.Errorf("got %q, want %q", string(data), string(testData))
	}
}

// TestDownloadRange_ServerError exercises the error path when GetObject fails.
func TestDownloadRange_ServerError(t *testing.T) {
	handler := newFailingMockHandler()
	handler.failGet = true
	pool, ts := newFailingTestPool(t, handler)
	defer ts.Close()

	_, err := pool.DownloadRange(context.Background(), "data/fail.bin", 0, 10)
	if err == nil {
		t.Fatal("expected error from failing DownloadRange")
	}
}

// TestDownloadRange_ReadBodyError exercises the read body error path.
func TestDownloadRange_ReadBodyError(t *testing.T) {
	// Create a server that returns a partial/corrupted response body for range reads.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			rangeHeader := r.Header.Get("Range")
			if rangeHeader != "" {
				w.Header().Set("Content-Length", "100") // claim 100 bytes
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("short")) // but only write 5
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       srv.URL,
		AccessKey:      "test",
		SecretKey:      "test",
		ForcePathStyle: true,
	}
	pool, err := NewClientPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}

	// DownloadRange uses io.ReadAll which may return partial data
	// depending on how the connection behaves.
	_, _ = pool.DownloadRange(context.Background(), "data/trunc.bin", 0, 100)
	// We just verify no panic occurs.
}

// TestNewBufferedReaderAt_DefaultPrefetch exercises the default prefetch branch
// when prefetch <= 0.
func TestNewBufferedReaderAt_DefaultPrefetch(t *testing.T) {
	inner := &covMockReaderAt{data: make([]byte, 100)}
	b := NewBufferedReaderAt(inner, 100, 0, 0) // should default to 2MB
	if b == nil {
		t.Fatal("expected non-nil BufferedS3ReaderAt")
	}
	if b.base != 2*1024*1024 {
		t.Errorf("prefetch = %d, want %d", b.base, 2*1024*1024)
	}
}

// TestNewBufferedReaderAt_NegativePrefetch exercises the negative prefetch branch.
func TestNewBufferedReaderAt_NegativePrefetch(t *testing.T) {
	inner := &covMockReaderAt{data: make([]byte, 100)}
	b := NewBufferedReaderAt(inner, 100, -100, 0) // should default to 2MB
	if b.base != 2*1024*1024 {
		t.Errorf("prefetch = %d, want %d", b.base, 2*1024*1024)
	}
}

// TestNewBufferedReaderAt_MaxPrefetchCap exercises the 64MB safety cap.
func TestNewBufferedReaderAt_MaxPrefetchCap(t *testing.T) {
	inner := &covMockReaderAt{data: make([]byte, 100)}
	b := NewBufferedReaderAt(inner, 100, 100*1024*1024, 0) // 100MB, should be capped to 64MB
	if b.base != 64*1024*1024 {
		t.Errorf("prefetch = %d, want %d (64MB cap)", b.base, 64*1024*1024)
	}
}

// TestNewCoalescingReaderAt_DefaultGap exercises the default gap threshold branch.
// Default is 1MB — BDP-priced for real S3 latency (s3-optimization research).
func TestNewCoalescingReaderAt_DefaultGap(t *testing.T) {
	inner := &covMockReaderAt{data: make([]byte, 100)}
	c := NewCoalescingReaderAt(inner, 100, 0) // should default to 1MB
	if c == nil {
		t.Fatal("expected non-nil CoalescingReaderAt")
	}
	if c.gapThreshold != 1024*1024 {
		t.Errorf("gapThreshold = %d, want %d", c.gapThreshold, 1024*1024)
	}
}

// TestNewCoalescingReaderAt_NegativeGap exercises the negative gap threshold.
func TestNewCoalescingReaderAt_NegativeGap(t *testing.T) {
	inner := &covMockReaderAt{data: make([]byte, 100)}
	c := NewCoalescingReaderAt(inner, 100, -10) // should default to 1MB
	if c.gapThreshold != 1024*1024 {
		t.Errorf("gapThreshold = %d, want %d", c.gapThreshold, 1024*1024)
	}
}

// TestNewCoalescingReaderAt_MaxGapCap exercises the 16MB safety cap
// (lifted from 1MB when the default gap became 1MB; AnyBlob's optimal
// range size tops out at 16MiB).
func TestNewCoalescingReaderAt_MaxGapCap(t *testing.T) {
	inner := &covMockReaderAt{data: make([]byte, 100)}
	c := NewCoalescingReaderAt(inner, 100, 32*1024*1024) // 32MB, should be capped to 16MB
	if c.gapThreshold != 16*1024*1024 {
		t.Errorf("gapThreshold = %d, want %d (16MB cap)", c.gapThreshold, 16*1024*1024)
	}
}

// TestNewClientPool_DefaultMaxConns exercises the MaxConnections <= 0 default.
func TestNewClientPool_DefaultMaxConns(t *testing.T) {
	handler := newMockS3Handler()
	ts := httptest.NewServer(handler)
	defer ts.Close()

	cfg := &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       ts.URL,
		AccessKey:      "test-key",
		SecretKey:      "test-secret",
		ForcePathStyle: true,
		MaxConnections: 0, // should default to 128
	}

	pool, err := NewClientPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

// TestNewClientPool_NegativeMaxConns exercises negative MaxConnections.
func TestNewClientPool_NegativeMaxConns(t *testing.T) {
	handler := newMockS3Handler()
	ts := httptest.NewServer(handler)
	defer ts.Close()

	cfg := &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       ts.URL,
		AccessKey:      "test-key",
		SecretKey:      "test-secret",
		ForcePathStyle: true,
		MaxConnections: -10, // should default to 128
	}

	pool, err := NewClientPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

// TestNewClientPool_WithEndpoint exercises the endpoint configuration branch.
func TestNewClientPool_WithEndpoint(t *testing.T) {
	handler := newMockS3Handler()
	ts := httptest.NewServer(handler)
	defer ts.Close()

	cfg := &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       ts.URL,
		ForcePathStyle: true,
	}

	pool, err := NewClientPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

// TestPreloadRanges_InnerReadError exercises the error path in PreloadRanges.
func TestPreloadRanges_InnerReadError(t *testing.T) {
	inner := &errorReaderAt{err: fmt.Errorf("read failed")}
	c := NewCoalescingReaderAt(inner, 1000, 64*1024)

	err := c.PreloadRanges([]readRange{{off: 0, length: 100}})
	if err == nil {
		t.Fatal("expected error from PreloadRanges with failing inner reader")
	}
}

// covMockReaderAt is a simple in-memory io.ReaderAt for coverage testing.
type covMockReaderAt struct {
	data []byte
}

func (m *covMockReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if off+int64(n) >= int64(len(m.data)) {
		return n, io.EOF
	}
	return n, nil
}

func (m *covMockReaderAt) Size() int64 {
	return int64(len(m.data))
}

// errorReaderAt always returns an error.
type errorReaderAt struct {
	err error
}

func (e *errorReaderAt) ReadAt(_ []byte, _ int64) (int, error) {
	return 0, e.err
}

// TestDownloadRange_FromOffset exercises DownloadRange starting from a non-zero offset.
func TestDownloadRange_FromOffset(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	handler.objects["data/alpha.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	// Read 5 bytes starting from offset 5
	data, err := pool.DownloadRange(context.Background(), "data/alpha.bin", 5, 5)
	if err != nil {
		t.Fatalf("DownloadRange error: %v", err)
	}
	if string(data) != "fghij" {
		t.Errorf("got %q, want %q", string(data), "fghij")
	}
}

// TestDownloadRange_NonExistentKey exercises DownloadRange with missing key.
func TestDownloadRange_NonExistentKey(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	_, err := pool.DownloadRange(context.Background(), "does/not/exist", 0, 10)
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
}

// TestReadAt_ShortRead exercises the ReadAt path where the response is
// shorter than requested (end clamp to file size).
func TestReadAt_ShortRead(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("12345")
	handler.objects["data/short.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	reader := pool.NewReaderAt(context.Background(), "data/short.bin", int64(len(testData)))

	// Request 10 bytes from offset 3, but only 2 bytes are available (indices 3,4)
	buf := make([]byte, 10)
	n, err := reader.ReadAt(buf, 3)
	if n != 2 {
		t.Errorf("expected 2 bytes, got %d", n)
	}
	if string(buf[:n]) != "45" {
		t.Errorf("got %q, want %q", string(buf[:n]), "45")
	}
	_ = err // may be EOF
}

// TestNewClientPool_NoEndpoint exercises the code path where Endpoint is empty.
func TestNewClientPool_NoEndpoint(t *testing.T) {
	// With no endpoint, the SDK uses default AWS endpoints.
	// We can't easily test without real AWS, but verify the pool creates
	// successfully with all other fields set.
	cfg := &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		AccessKey:      "test-key",
		SecretKey:      "test-secret",
		ForcePathStyle: false,
		Endpoint:       "", // no custom endpoint
	}

	pool, err := NewClientPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClientPool without endpoint: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}
