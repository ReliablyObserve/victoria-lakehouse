package tenant

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

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// condS3 is a minimal S3 that honours If-Match / If-None-Match on PutObject the
// way AWS S3 and RustFS do: 412 PreconditionFailed when the condition is not met.
type condS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	etags   map[string]string
	n       int
	puts    atomic.Int32
	refused atomic.Int32
	// ignoreConditions makes the server behave like a backend without
	// conditional write support (used by the mutation check).
	ignoreConditions bool
}

func newCondS3() *condS3 {
	return &condS3{objects: map[string][]byte{}, etags: map[string]string{}}
}

func (s *condS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		s.puts.Add(1)
		body, _ := io.ReadAll(r.Body)
		cur, exists := s.etags[key]
		if !s.ignoreConditions {
			if inm := r.Header.Get("If-None-Match"); inm == "*" && exists {
				s.refused.Add(1)
				s.fail(w, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
			if im := r.Header.Get("If-Match"); im != "" && (!exists || im != cur) {
				s.refused.Add(1)
				s.fail(w, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
		}
		s.n++
		s.objects[key] = body
		s.etags[key] = fmt.Sprintf("\"etag-%d\"", s.n)
		w.Header().Set("ETag", s.etags[key])
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		data, ok := s.objects[key]
		if !ok {
			s.fail(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", s.etags[key])
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *condS3) fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func newCondPool(t *testing.T, s *condS3) *s3reader.ClientPool {
	t.Helper()
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	pool, err := s3reader.NewClientPool(context.Background(), &config.S3Config{
		Bucket: "test-bucket", Region: "us-east-1", Endpoint: ts.URL,
		AccessKey: "k", SecretKey: "s", ForcePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestClientPool_ConditionalWriteMapsTo412(t *testing.T) {
	s := newCondS3()
	pool := newCondPool(t, s)
	ctx := context.Background()

	if _, _, found, err := pool.DownloadWithETag(ctx, "k"); err != nil || found {
		t.Fatalf("missing object: found=%v err=%v", found, err)
	}
	if err := pool.UploadConditional(ctx, "k", []byte(`["v1"]`), ""); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := pool.UploadConditional(ctx, "k", []byte(`["x"]`), ""); !IsPreconditionFailed(err) {
		t.Fatalf("second If-None-Match create = %v, want a precondition failure", err)
	}
	data, etag, found, err := pool.DownloadWithETag(ctx, "k")
	if err != nil || !found || etag == "" || string(data) != `["v1"]` {
		t.Fatalf("read back: %q %q %v %v", data, etag, found, err)
	}
	if err := pool.UploadConditional(ctx, "k", []byte(`["v2"]`), etag); err != nil {
		t.Fatalf("If-Match update: %v", err)
	}
	if err := pool.UploadConditional(ctx, "k", []byte(`["v3"]`), etag); !IsPreconditionFailed(err) {
		t.Fatalf("stale If-Match = %v, want a precondition failure", err)
	}
	if got := string(s.objects["k"]); got != `["v2"]` {
		t.Fatalf("stored %q, the lost write must not land", got)
	}
}

// The real S3 client path end to end: 16 pods, one registry object, one winner
// per write, every pod ends up with its own unique ID.
func TestRegistry_SixteenPodsOverConditionalS3(t *testing.T) {
	s := newCondS3()
	pool := newCondPool(t, s)
	rng := AutoRange{Min: 9000, Max: 9999}
	const n = 16
	ids := make([]uint32, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		_, g := newTestRegistry(t, pool, rng, nil)
		wg.Add(1)
		go func(i int, g *Registry) {
			defer wg.Done()
			<-start
			tid, err := g.Allocate(context.Background(), fmt.Sprintf("pod-tenant-%02d", i))
			if err != nil {
				t.Errorf("pod %d: %v", i, err)
				return
			}
			ids[i] = tid.AccountID
		}(i, g)
	}
	close(start)
	wg.Wait()
	seen := map[uint32]int{}
	for i, id := range ids {
		if id == 0 {
			continue
		}
		if j, dup := seen[id]; dup {
			t.Fatalf("pods %d and %d got the same ID %d", j, i, id)
		}
		seen[id] = i
	}
	if len(seen) != n {
		t.Fatalf("%d distinct IDs, want %d", len(seen), n)
	}
	t.Logf("puts=%d refused(412)=%d", s.puts.Load(), s.refused.Load())
}
