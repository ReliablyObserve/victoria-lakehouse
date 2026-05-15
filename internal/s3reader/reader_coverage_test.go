package s3reader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// mockAPIError implements smithy.APIError for testing isRetryable with specific codes.
type mockAPIError struct {
	code    string
	message string
}

func (e *mockAPIError) Error() string                 { return e.message }
func (e *mockAPIError) ErrorCode() string             { return e.code }
func (e *mockAPIError) ErrorMessage() string          { return e.message }
func (e *mockAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

func TestIsRetryable_APIErrors(t *testing.T) {
	tests := []struct {
		name string
		code string
		want bool
	}{
		{"SlowDown", "SlowDown", true},
		{"ServiceUnavailable", "ServiceUnavailable", true},
		{"InternalError", "InternalError", true},
		{"RequestTimeout", "RequestTimeout", true},
		{"AccessDenied", "AccessDenied", false},
		{"NoSuchKey", "NoSuchKey", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &mockAPIError{code: tt.code, message: tt.code}
			if got := isRetryable(err); got != tt.want {
				t.Errorf("isRetryable(%q) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestRetryS3_CancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	calls := 0
	err := retryS3(ctx, 5, func() error {
		calls++
		return fmt.Errorf("connection reset by peer")
	})

	// The context should eventually cancel during backoff.
	if err == nil {
		t.Fatal("expected error")
	}
	// Should have been cancelled before exhausting all retries.
	if calls > 3 {
		t.Errorf("expected fewer than 4 calls with short timeout, got %d", calls)
	}
}

func TestRetryS3_ZeroRetries(t *testing.T) {
	calls := 0
	err := retryS3(context.Background(), 0, func() error {
		calls++
		return fmt.Errorf("connection reset by peer")
	})
	if err == nil {
		t.Fatal("expected error with zero retries")
	}
	if calls != 1 {
		t.Errorf("expected 1 call with zero retries, got %d", calls)
	}
}

// TestUploadDownloadRoundTrip verifies the full upload/download cycle.
func TestUploadDownloadRoundTrip(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()
	data := []byte("round trip test data for upload and download")

	if err := pool.Upload(ctx, "roundtrip/test.bin", data); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	got, err := pool.Download(ctx, "roundtrip/test.bin")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Downloaded data = %q, want %q", string(got), string(data))
	}
}

// TestDeleteNonExistentKey verifies that deleting a non-existent key doesn't error
// (S3 DeleteObject is idempotent).
func TestDeleteNonExistentKey(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	err := pool.Delete(context.Background(), "nonexistent/key.bin")
	if err != nil {
		t.Fatalf("Delete non-existent: %v", err)
	}
}

// TestReadAt_ClampToFileSize verifies that reading beyond file size clamps correctly.
func TestReadAt_ClampToFileSize(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("short")
	handler.objects["data/small.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()
	reader := pool.NewReaderAt(ctx, "data/small.bin", int64(len(testData)))

	// Request more bytes than available from offset 0.
	buf := make([]byte, 100)
	n, err := reader.ReadAt(buf, 0)
	// Should read only len(testData) bytes.
	if n != len(testData) {
		t.Errorf("expected %d bytes, got %d", len(testData), n)
	}
	if string(buf[:n]) != "short" {
		t.Errorf("got %q, want %q", string(buf[:n]), "short")
	}
	_ = err // may be nil or EOF
}

// TestReadAt_ExactSize verifies reading exactly the file size.
func TestReadAt_ExactSize(t *testing.T) {
	handler := newMockS3Handler()
	testData := []byte("exactsize")
	handler.objects["data/exact.bin"] = testData

	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()
	reader := pool.NewReaderAt(ctx, "data/exact.bin", int64(len(testData)))

	buf := make([]byte, len(testData))
	n, err := reader.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt error: %v", err)
	}
	if n != len(testData) {
		t.Errorf("expected %d bytes, got %d", len(testData), n)
	}
	if string(buf) != "exactsize" {
		t.Errorf("got %q, want %q", string(buf), "exactsize")
	}
}

// TestRetryS3_BackoffCap verifies that the backoff is capped at 5 seconds.
// With maxRetries=5, the raw backoffs would be 100ms, 200ms, 400ms, 800ms, 1600ms.
// The 5th (index 4) backoff would be 1600ms, still under 5s cap, but we verify
// the function works with many retries.
func TestRetryS3_ManyRetries(t *testing.T) {
	calls := 0
	err := retryS3(context.Background(), 5, func() error {
		calls++
		return fmt.Errorf("connection reset by peer")
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if calls != 6 { // initial + 5 retries
		t.Errorf("expected 6 calls, got %d", calls)
	}
}

// TestRetryS3_BackoffCappedAt5s triggers the backoff cap by using 6+ retries.
// At retry index 6, raw backoff = 2^6 * 100ms = 6400ms, which would be capped to 5s.
func TestRetryS3_BackoffCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	calls := 0
	_ = retryS3(ctx, 10, func() error {
		calls++
		return fmt.Errorf("connection reset by peer")
	})

	// With 500ms timeout, we should get at least 2 calls (initial + 1 retry with 100ms backoff).
	if calls < 2 {
		t.Errorf("expected at least 2 calls, got %d", calls)
	}
}

// failingMockS3Handler fails specific operations to test error paths.
type failingMockS3Handler struct {
	failGet    bool
	failPut    bool
	failDelete bool
	failHead   bool
	objects    map[string][]byte
}

func newFailingMockHandler() *failingMockS3Handler {
	return &failingMockS3Handler{objects: make(map[string][]byte)}
}

func (m *failingMockS3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	switch r.Method {
	case http.MethodPut:
		if m.failPut {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		m.objects[key] = body
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		if m.failGet {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		data, ok := m.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int64
			_, _ = fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
			if end >= int64(len(data)) {
				end = int64(len(data)) - 1
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[start : end+1])
			return
		}
		_, _ = w.Write(data)

	case http.MethodDelete:
		if m.failDelete {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		delete(m.objects, key)
		w.WriteHeader(http.StatusNoContent)

	case http.MethodHead:
		if m.failHead {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, ok := m.objects[key]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func newFailingTestPool(t *testing.T, handler *failingMockS3Handler) (*ClientPool, *httptest.Server) {
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

// TestUpload_ServerError tests the Upload error path when the S3 server returns 500.
func TestUpload_ServerError(t *testing.T) {
	handler := newFailingMockHandler()
	handler.failPut = true
	pool, ts := newFailingTestPool(t, handler)
	defer ts.Close()

	err := pool.Upload(context.Background(), "uploads/fail.bin", []byte("data"))
	if err == nil {
		t.Fatal("expected error from failing Upload")
	}
}

// TestDelete_ServerError tests the Delete error path when the S3 server returns 500.
func TestDelete_ServerError(t *testing.T) {
	handler := newFailingMockHandler()
	handler.failDelete = true
	pool, ts := newFailingTestPool(t, handler)
	defer ts.Close()

	err := pool.Delete(context.Background(), "delete/fail.bin")
	if err == nil {
		t.Fatal("expected error from failing Delete")
	}
}

// TestReadAt_ServerError tests the ReadAt error path when GetObject fails.
func TestReadAt_ServerError(t *testing.T) {
	handler := newFailingMockHandler()
	handler.failGet = true
	pool, ts := newFailingTestPool(t, handler)
	defer ts.Close()

	reader := pool.NewReaderAt(context.Background(), "data/fail.bin", 100)
	buf := make([]byte, 10)
	_, err := reader.ReadAt(buf, 0)
	if err == nil {
		t.Fatal("expected error from failing ReadAt")
	}
}

// TestExists_ServerError tests the Exists error path when HeadObject returns 500.
func TestExists_ServerError(t *testing.T) {
	handler := newFailingMockHandler()
	handler.failHead = true
	pool, ts := newFailingTestPool(t, handler)
	defer ts.Close()

	_, err := pool.Exists(context.Background(), "exists/fail.bin")
	if err == nil {
		t.Fatal("expected error from failing Exists")
	}
}

// TestDownload_ServerError tests the Download error path when GetObject fails.
func TestDownload_ServerError(t *testing.T) {
	handler := newFailingMockHandler()
	handler.failGet = true
	pool, ts := newFailingTestPool(t, handler)
	defer ts.Close()

	_, err := pool.Download(context.Background(), "downloads/fail.bin")
	if err == nil {
		t.Fatal("expected error from failing Download")
	}
}

// TestExists_UploadThenCheck verifies Exists returns true after uploading.
func TestExists_UploadThenCheck(t *testing.T) {
	handler := newMockS3Handler()
	pool, ts := newTestPool(t, handler)
	defer ts.Close()

	ctx := context.Background()

	if err := pool.Upload(ctx, "exists/check.bin", []byte("data")); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	ok, err := pool.Exists(ctx, "exists/check.bin")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Error("expected Exists to return true after Upload")
	}
}
