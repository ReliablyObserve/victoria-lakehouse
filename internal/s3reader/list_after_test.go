package s3reader

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// listServer answers ListObjectsV2 with pages of keys.
type listServer struct {
	keys     []string
	pageSize int
	calls    atomic.Int32
	starts   []string
	failures atomic.Int32
}

func (l *listServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.calls.Add(1)
	if l.failures.Load() > 0 {
		l.failures.Add(-1)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	q := r.URL.Query()
	l.starts = append(l.starts, q.Get("start-after"))
	from := 0
	if tok := q.Get("continuation-token"); tok != "" {
		_, _ = fmt.Sscanf(tok, "t%d", &from)
	} else if sa := q.Get("start-after"); sa != "" {
		for from < len(l.keys) && l.keys[from] <= sa {
			from++
		}
	}
	to := from + l.pageSize
	truncated := to < len(l.keys)
	if to > len(l.keys) {
		to = len(l.keys)
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name>`)
	fmt.Fprintf(&b, "<IsTruncated>%v</IsTruncated>", truncated)
	if truncated {
		fmt.Fprintf(&b, "<NextContinuationToken>t%d</NextContinuationToken>", to)
	}
	for _, k := range l.keys[from:to] {
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><LastModified>2026-01-02T03:04:05.000Z</LastModified><Size>1</Size></Contents>", k)
	}
	b.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(b.String()))
}

func newListPool(t *testing.T, l *listServer) *ClientPool {
	t.Helper()
	ts := httptest.NewServer(l)
	t.Cleanup(ts.Close)
	pool, err := NewClientPool(context.Background(), &config.S3Config{
		Bucket: "b", Region: "us-east-1", Endpoint: ts.URL, AccessKey: "k", SecretKey: "s", ForcePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestClientPool_ListAfter_PagesAndResumes(t *testing.T) {
	l := &listServer{pageSize: 2, keys: []string{"p/a", "p/b", "p/c", "p/d", "p/e"}}
	pool := newListPool(t, l)

	var got []string
	mod := time.Time{}
	err := pool.ListAfter(context.Background(), "p/", "p/b", func(key string, lm time.Time) error {
		got = append(got, key)
		mod = lm
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "p/c,p/d,p/e" {
		t.Fatalf("listed %v, want the keys after p/b", got)
	}
	if mod.Year() != 2026 {
		t.Fatalf("LastModified = %v", mod)
	}
	if l.calls.Load() != 2 {
		t.Fatalf("%d requests for 3 keys at 2 per page, want 2", l.calls.Load())
	}
	if l.starts[0] != "p/b" {
		t.Fatalf("the first request carried start-after %q, want p/b", l.starts[0])
	}
}

func TestClientPool_ListAfter_AllKeysAndCallbackError(t *testing.T) {
	l := &listServer{pageSize: 10, keys: []string{"p/a", "p/b"}}
	pool := newListPool(t, l)
	n := 0
	if err := pool.ListAfter(context.Background(), "p/", "", func(string, time.Time) error { n++; return nil }); err != nil || n != 2 {
		t.Fatalf("listed %d keys, err %v", n, err)
	}
	stop := errors.New("stop")
	if err := pool.ListAfter(context.Background(), "p/", "", func(string, time.Time) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("callback error not returned: %v", err)
	}
}

func TestClientPool_ListAfter_Error(t *testing.T) {
	l := &listServer{pageSize: 10, keys: []string{"p/a"}}
	l.failures.Store(100)
	pool := newListPool(t, l)
	if err := pool.ListAfter(context.Background(), "p/", "", func(string, time.Time) error { return nil }); err == nil {
		t.Fatal("a failing listing returned no error")
	}
}

// A page that claims more results but gives no continuation token is an error,
// never a complete listing.
func TestClientPool_ListAfter_TruncatedWithoutTokenIsAnError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0"?><ListBucketResult><Name>b</Name><IsTruncated>true</IsTruncated><Contents><Key>p/a</Key><Size>1</Size></Contents></ListBucketResult>`))
	}))
	t.Cleanup(ts.Close)
	pool, err := NewClientPool(context.Background(), &config.S3Config{Bucket: "b", Region: "us-east-1", Endpoint: ts.URL, AccessKey: "k", SecretKey: "s", ForcePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.ListAfter(context.Background(), "p/", "", func(string, time.Time) error { return nil }); err == nil {
		t.Fatal("a truncated page without a continuation token was taken for a complete listing")
	}
}

// A store that answers "truncated" with an empty token, or with the token it was
// just asked with, must produce an error after a bounded number of requests, not an
// endless listing (a poll loop holds the sync lock while it lists).
func TestClientPool_ListAfter_RepeatedOrEmptyTokenIsAnError(t *testing.T) {
	for name, token := range map[string]string{"empty": "<NextContinuationToken></NextContinuationToken>", "repeated": "<NextContinuationToken>same</NextContinuationToken>"} {
		var calls atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0"?><ListBucketResult><Name>b</Name><IsTruncated>true</IsTruncated>` + token + `<Contents><Key>p/a</Key><Size>1</Size></Contents></ListBucketResult>`))
		}))
		pool, err := NewClientPool(context.Background(), &config.S3Config{Bucket: "b", Region: "us-east-1", Endpoint: ts.URL, AccessKey: "k", SecretKey: "s", ForcePathStyle: true})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- pool.ListAfter(context.Background(), "p/", "", func(string, time.Time) error { return nil })
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("%s token: the listing ended without an error", name)
			}
			if n := calls.Load(); n > 3 {
				t.Errorf("%s token: %d requests before giving up, want at most 3", name, n)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("%s token: the listing does not end (%d requests)", name, calls.Load())
		}
		ts.Close()
	}
}
