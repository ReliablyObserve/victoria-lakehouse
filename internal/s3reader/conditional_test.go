package s3reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// etagS3 is a single-bucket S3 with ETags and conditional PutObject.
type etagS3 struct {
	mu      sync.Mutex
	data    map[string][]byte
	etag    map[string]string
	n       int
	getFail bool
	// putStatuses are returned, one per PutObject, before the real handling.
	putStatuses []int
	putBodies   []string
}

func (s *etagS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
	s.mu.Lock()
	defer s.mu.Unlock()
	xmlErr := func(status int, code string) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><Error><Code>%s</Code></Error>`, code)
	}
	switch r.Method {
	case http.MethodGet:
		if s.getFail {
			xmlErr(http.StatusForbidden, "AccessDenied")
			return
		}
		d, ok := s.data[key]
		if !ok {
			xmlErr(http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", s.etag[key])
		_, _ = w.Write(d)
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		s.putBodies = append(s.putBodies, string(body))
		if len(s.putStatuses) > 0 {
			st := s.putStatuses[0]
			s.putStatuses = s.putStatuses[1:]
			if st == http.StatusServiceUnavailable {
				xmlErr(st, "SlowDown")
				return
			}
			if st != http.StatusOK {
				xmlErr(st, "InternalFailure")
				return
			}
		}
		cur, exists := s.etag[key]
		if r.Header.Get("If-None-Match") == "*" && exists {
			xmlErr(http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		if im := r.Header.Get("If-Match"); im != "" && (!exists || im != cur) {
			xmlErr(http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		s.n++
		s.data[key] = body
		s.etag[key] = fmt.Sprintf("\"e%d\"", s.n)
		w.Header().Set("ETag", s.etag[key])
		w.WriteHeader(http.StatusOK)
	}
}

func newEtagPool(t *testing.T) (*ClientPool, *etagS3) {
	t.Helper()
	s := &etagS3{data: map[string][]byte{}, etag: map[string]string{}}
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	pool, err := NewClientPool(context.Background(), &config.S3Config{
		Bucket: "test-bucket", Region: "us-east-1", Endpoint: ts.URL,
		AccessKey: "k", SecretKey: "s", ForcePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return pool, s
}

func TestDownloadWithETag_MissingFoundAndError(t *testing.T) {
	pool, s := newEtagPool(t)
	ctx := context.Background()

	data, etag, found, err := pool.DownloadWithETag(ctx, "k")
	if err != nil || found || data != nil || etag != "" {
		t.Fatalf("missing object: %q %q %v %v", data, etag, found, err)
	}

	s.data["k"], s.etag["k"] = []byte("payload"), `"abc"`
	data, etag, found, err = pool.DownloadWithETag(ctx, "k")
	if err != nil || !found || string(data) != "payload" || etag != `"abc"` {
		t.Fatalf("existing object: %q %q %v %v", data, etag, found, err)
	}

	s.getFail = true
	if _, _, found, err = pool.DownloadWithETag(ctx, "k"); err == nil || found {
		t.Fatalf("a failing GET must be an error, got found=%v err=%v", found, err)
	}
}

func TestUploadConditional_CreateUpdateAndLostRace(t *testing.T) {
	pool, s := newEtagPool(t)
	ctx := context.Background()

	if err := pool.UploadConditional(ctx, "k", []byte("one"), ""); err != nil {
		t.Fatalf("create: %v", err)
	}
	err := pool.UploadConditional(ctx, "k", []byte("two"), "")
	if err == nil || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("a second create must lose with a precondition failure, got %v", err)
	}
	_, etag, _, _ := pool.DownloadWithETag(ctx, "k")
	if err := pool.UploadConditional(ctx, "k", []byte("three"), etag); err != nil {
		t.Fatalf("If-Match update: %v", err)
	}
	if err := pool.UploadConditional(ctx, "k", []byte("four"), etag); err == nil {
		t.Fatal("a stale ETag must lose")
	}
	if got := string(s.data["k"]); got != "three" {
		t.Fatalf("stored %q, want three", got)
	}
}

// A retried PutObject (the first attempt got 503 SlowDown) must carry the whole
// body again, not an already-consumed reader.
func TestUploadConditional_RetryResendsTheBody(t *testing.T) {
	pool, s := newEtagPool(t)
	s.putStatuses = []int{http.StatusServiceUnavailable}
	if err := pool.UploadConditional(context.Background(), "k", []byte("full-body"), ""); err != nil {
		t.Fatalf("upload after a throttled attempt: %v", err)
	}
	if len(s.putBodies) != 2 || s.putBodies[0] != "full-body" || s.putBodies[1] != "full-body" {
		t.Fatalf("PutObject bodies = %q, want the full body twice", s.putBodies)
	}
	if got := string(s.data["k"]); got != "full-body" {
		t.Fatalf("stored %q", got)
	}
}

func TestUploadConditional_ServerError(t *testing.T) {
	pool, s := newEtagPool(t)
	s.putStatuses = []int{http.StatusForbidden}
	err := pool.UploadConditional(context.Background(), "k", []byte("x"), "")
	if err == nil {
		t.Fatal("expected an error")
	}
	var ue interface{ Unwrap() error }
	if !errors.As(err, &ue) {
		t.Fatalf("the error must wrap the S3 error: %v", err)
	}
}
