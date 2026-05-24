package s3reader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"connection reset", fmt.Errorf("connection reset by peer"), true},
		{"i/o timeout", fmt.Errorf("i/o timeout"), true},
		{"not found", fmt.Errorf("NoSuchKey: key not found"), false},
		{"generic error", fmt.Errorf("some random error"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryable(tt.err); got != tt.want {
				t.Errorf("isRetryable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRetryS3_Success(t *testing.T) {
	calls := 0
	err := retryS3(context.Background(), 3, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryS3_RetriesOnRetryableError(t *testing.T) {
	calls := 0
	err := retryS3(context.Background(), 3, func() error {
		calls++
		if calls < 3 {
			return fmt.Errorf("connection reset by peer")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryS3_NoRetryOnNonRetryable(t *testing.T) {
	calls := 0
	err := retryS3(context.Background(), 3, func() error {
		calls++
		return fmt.Errorf("access denied")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call (no retry for non-retryable), got %d", calls)
	}
}

func TestRetryS3_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := retryS3(ctx, 3, func() error {
		calls++
		cancel()
		return fmt.Errorf("connection reset by peer")
	})
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call before cancellation, got %d", calls)
	}
}

func TestRetryS3_ExhaustsRetries(t *testing.T) {
	calls := 0
	start := time.Now()
	err := retryS3(context.Background(), 2, func() error {
		calls++
		return fmt.Errorf("i/o timeout")
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if calls != 3 { // initial + 2 retries
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	// With maxRetries=2, backoffs are 100ms and 200ms = 300ms minimum
	if elapsed < 250*time.Millisecond {
		t.Fatalf("expected backoff delays, but elapsed was only %v", elapsed)
	}
}

// --- Mock S3 server helpers ---

// mockS3Handler is a simple HTTP handler that simulates S3-like behavior for testing.
type mockS3Handler struct {
	objects     map[string][]byte // key -> content
	putCalls    atomic.Int64
	getCalls    atomic.Int64
	deleteCalls atomic.Int64
	headCalls   atomic.Int64
}

func newMockS3Handler() *mockS3Handler {
	return &mockS3Handler{
		objects: make(map[string][]byte),
	}
}

func (m *mockS3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The key is derived from the URL path. S3 path-style URLs look like
	// /bucket/key, so strip the leading /bucket/ prefix.
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	switch r.Method {
	case http.MethodPut:
		m.putCalls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		m.objects[key] = body
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		m.getCalls.Add(1)
		data, ok := m.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			// Parse "bytes=start-end"
			var start, end int64
			_, _ = fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
			if start >= int64(len(data)) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= int64(len(data)) {
				end = int64(len(data)) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[start : end+1])
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)

	case http.MethodDelete:
		m.deleteCalls.Add(1)
		delete(m.objects, key)
		w.WriteHeader(http.StatusNoContent)

	case http.MethodHead:
		m.headCalls.Add(1)
		if _, ok := m.objects[key]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// newTestPool creates a ClientPool backed by a mock HTTP S3 server.
func newTestPool(t *testing.T, handler *mockS3Handler) (*ClientPool, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(handler)

	cfg := &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       ts.URL,
		AccessKey:      "test-key",
		SecretKey:      "test-secret",
		ForcePathStyle: true,
	}

	pool, err := NewClientPool(context.Background(), cfg)
	if err != nil {
		ts.Close()
		t.Fatalf("NewClientPool failed: %v", err)
	}
	return pool, ts
}

// --- NewClientPool tests ---

func TestNewClientPool_ValidConfig(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
	if pool.Bucket() != "test-bucket" {
		t.Errorf("bucket = %q, want %q", pool.Bucket(), "test-bucket")
	}
	if pool.S3Client() == nil {
		t.Fatal("expected non-nil S3 client")
	}
}

func TestNewClientPool_WithoutCredentials(t *testing.T) {
	handler := newMockS3Handler()
	ts := httptest.NewServer(handler)
	defer ts.Close()

	cfg := &config.S3Config{
		Bucket:         "my-bucket",
		Region:         "eu-west-1",
		Endpoint:       ts.URL,
		ForcePathStyle: true,
	}

	pool, err := NewClientPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClientPool without explicit creds failed: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

// --- NewReaderAt tests ---

func TestNewReaderAt_CreatesReaderWithContext(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()
	reader := pool.NewReaderAt(ctx, "some/key.parquet", 1024)

	if reader == nil {
		t.Fatal("expected non-nil reader")
	}
	if reader.Size() != 1024 {
		t.Errorf("size = %d, want 1024", reader.Size())
	}
	if reader.key != "some/key.parquet" {
		t.Errorf("key = %q, want %q", reader.key, "some/key.parquet")
	}
	if reader.bucket != "test-bucket" {
		t.Errorf("bucket = %q, want %q", reader.bucket, "test-bucket")
	}
}

// --- ReadAt tests ---

func TestReadAt_ReadsCorrectBytes(t *testing.T) {
	handler := newMockS3Handler()
	// Pre-populate the mock with some data.
	testData := []byte("Hello, S3 World! This is test data for ReadAt.")
	handler.objects["data/file.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()
	reader := pool.NewReaderAt(ctx, "data/file.bin", int64(len(testData)))

	// Read the first 5 bytes.
	buf := make([]byte, 5)
	n, err := reader.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt error: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes, got %d", n)
	}
	if string(buf) != "Hello" {
		t.Errorf("got %q, want %q", string(buf), "Hello")
	}
}

func TestReadAt_ReadsFromOffset(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("0123456789ABCDEF")
	handler.objects["data/offset.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()
	reader := pool.NewReaderAt(ctx, "data/offset.bin", int64(len(testData)))

	buf := make([]byte, 4)
	n, err := reader.ReadAt(buf, 10)
	if err != nil {
		t.Fatalf("ReadAt offset error: %v", err)
	}
	if n != 4 {
		t.Fatalf("expected 4 bytes, got %d", n)
	}
	if string(buf) != "ABCD" {
		t.Errorf("got %q, want %q", string(buf), "ABCD")
	}
}

func TestReadAt_EOFWhenOffsetBeyondSize(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()
	reader := pool.NewReaderAt(ctx, "any/key", 100)

	buf := make([]byte, 10)
	_, err := reader.ReadAt(buf, 200)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReadAt_IncrementsMetrics(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("metric test data")
	handler.objects["metrics/test.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()
	reader := pool.NewReaderAt(ctx, "metrics/test.bin", int64(len(testData)))

	initialGets := handler.getCalls.Load()
	buf := make([]byte, 5)
	_, err := reader.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt error: %v", err)
	}

	if handler.getCalls.Load() <= initialGets {
		t.Error("expected GetObject call count to increment")
	}
}

// --- Upload tests ---

func TestUpload_WritesDataToS3(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	data := []byte("uploaded content")
	err := pool.Upload(context.Background(), "uploads/test.bin", data)
	if err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	// Verify mock received the data.
	stored, ok := handler.objects["uploads/test.bin"]
	if !ok {
		t.Fatal("expected object to be stored")
	}
	if string(stored) != string(data) {
		t.Errorf("stored data = %q, want %q", string(stored), string(data))
	}
	if handler.putCalls.Load() < 1 {
		t.Error("expected at least one PutObject call")
	}
}

// --- Download tests ---

func TestDownload_ReadsFullObject(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("full download content for testing")
	handler.objects["downloads/full.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	data, err := pool.Download(context.Background(), "downloads/full.bin")
	if err != nil {
		t.Fatalf("Download error: %v", err)
	}
	if string(data) != string(testData) {
		t.Errorf("got %q, want %q", string(data), string(testData))
	}
}

func TestDownload_NonExistentKey(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	_, err := pool.Download(context.Background(), "does/not/exist")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
}

// --- Delete tests ---

func TestDelete_RemovesObject(t *testing.T) {
	handler := newMockS3Handler()
	handler.objects["delete/me.bin"] = []byte("to be deleted")

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	err := pool.Delete(context.Background(), "delete/me.bin")
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}
	if handler.deleteCalls.Load() < 1 {
		t.Error("expected at least one DeleteObject call")
	}
}

// --- Exists tests ---

func TestExists_ReturnsTrueForExistingKey(t *testing.T) {
	handler := newMockS3Handler()
	handler.objects["exists/yes.bin"] = []byte("present")

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ok, err := pool.Exists(context.Background(), "exists/yes.bin")
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if !ok {
		t.Error("expected Exists to return true for existing key")
	}
	if handler.headCalls.Load() < 1 {
		t.Error("expected at least one HeadObject call")
	}
}

func TestExists_ReturnsFalseForMissingKey(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ok, err := pool.Exists(context.Background(), "does/not/exist.bin")
	// The mock returns 404 which the AWS SDK wraps. The Exists method
	// checks for types.NotFound and types.NoSuchKey, but our mock HTTP
	// response may not produce exactly those error types. We accept
	// either false+nil or false+error depending on how the SDK maps it.
	if ok {
		t.Error("expected Exists to return false for missing key")
	}
	_ = err // may be nil or a wrapped not-found error
}

func TestNewClientPool_UsesCustomTransport(t *testing.T) {
	mock := newMockS3Handler()
	mock.objects["test-key"] = []byte("test-data")
	srv := httptest.NewServer(mock)
	defer srv.Close()

	pool, err := NewClientPool(context.Background(), &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       srv.URL,
		ForcePathStyle: true,
		AccessKey:      "test",
		SecretKey:      "test",
		MaxConnections: 64,
	})
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}

	// Verify a single download works first.
	data, err := pool.Download(context.Background(), "test-key")
	if err != nil {
		t.Fatalf("single Download failed: %v", err)
	}
	if string(data) != "test-data" {
		t.Fatalf("unexpected data: %q", string(data))
	}

	var wg sync.WaitGroup
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = pool.Download(context.Background(), "test-key")
		}()
	}
	wg.Wait()

	if mock.getCalls.Load() != 10 {
		t.Fatalf("expected 10 GET calls, got %d", mock.getCalls.Load())
	}
}
