package delete

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// miniS3 is a single-bucket S3 over HTTP with exactly what the tombstone
// backend uses: conditional create, GET/HEAD/DELETE, and ListObjectsV2 with
// StartAfter, continuation tokens and a page size. It records every list request.
type miniS3 struct {
	mu        sync.Mutex
	objs      map[string][]byte
	noCond    bool
	pageSize  int
	listQuery []string
	puts      int
	// conflicts is the number of conditional creates answered 409
	// ConditionalRequestConflict (a concurrent write in flight) before one is honoured.
	conflicts int
}

func newMiniS3(t *testing.T) (*miniS3, *s3reader.ClientPool) {
	t.Helper()
	m := &miniS3{objs: map[string][]byte{}, pageSize: 1000}
	srv := httptest.NewServer(m)
	t.Cleanup(srv.Close)
	pool, err := s3reader.NewClientPool(context.Background(), &config.S3Config{
		Bucket: "bkt", Region: "us-east-1", Endpoint: srv.URL, ForcePathStyle: true, AccessKey: "a", SecretKey: "s",
	})
	if err != nil {
		t.Fatal(err)
	}
	return m, pool
}

func (m *miniS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/bkt")
	key = strings.TrimPrefix(key, "/")
	xmlErr := func(status int, code string) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><Error><Code>%s</Code></Error>`, code)
	}
	switch {
	case r.Method == http.MethodGet && key == "":
		m.listQuery = append(m.listQuery, r.URL.RawQuery)
		q := r.URL.Query()
		prefix := q.Get("prefix")
		after := q.Get("continuation-token")
		if after == "" {
			after = q.Get("start-after")
		}
		var keys []string
		for k := range m.objs {
			if strings.HasPrefix(k, prefix) && k > after {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		truncated := false
		if len(keys) > m.pageSize {
			keys, truncated = keys[:m.pageSize], true
		}
		var b strings.Builder
		b.WriteString(`<?xml version="1.0"?><ListBucketResult><Name>bkt</Name>`)
		for _, k := range keys {
			var esc strings.Builder
			_ = xml.EscapeText(&esc, []byte(k))
			fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-10-18T00:00:00Z</LastModified></Contents>", esc.String(), len(m.objs[k]))
		}
		fmt.Fprintf(&b, "<KeyCount>%d</KeyCount><IsTruncated>%t</IsTruncated>", len(keys), truncated)
		if truncated {
			fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", keys[len(keys)-1])
		}
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, b.String())
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		m.puts++
		if m.noCond && r.Header.Get("If-None-Match") != "" {
			xmlErr(http.StatusNotImplemented, "NotImplemented")
			return
		}
		if m.conflicts > 0 && r.Header.Get("If-None-Match") == "*" {
			m.conflicts--
			xmlErr(http.StatusConflict, "ConditionalRequestConflict")
			return
		}
		if _, exists := m.objs[key]; exists && r.Header.Get("If-None-Match") == "*" {
			xmlErr(http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		m.objs[key] = body
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		d, ok := m.objs[key]
		if !ok {
			xmlErr(http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(d)))
		w.Header().Set("ETag", `"x"`)
		if r.Method == http.MethodGet {
			_, _ = w.Write(d)
		}
	case r.Method == http.MethodDelete:
		delete(m.objs, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		xmlErr(http.StatusNotImplemented, "NotImplemented")
	}
}

func TestS3Backend_ConditionalCreate(t *testing.T) {
	m, pool := newMiniS3(t)
	b := NewS3Backend(pool)
	if v := metrics.DeleteConditionalWrites.Get(); v != 1 {
		t.Fatalf("conditional writes gauge = %d, want 1", v)
	}
	created, err := b.PutIfAbsent(context.Background(), "p/_tombstones/k", []byte("one"))
	if err != nil || !created {
		t.Fatalf("first create = %v, %v", created, err)
	}
	created, err = b.PutIfAbsent(context.Background(), "p/_tombstones/k", []byte("two"))
	if err != nil || created {
		t.Fatalf("second create = %v, %v, want not created and no error", created, err)
	}
	if string(m.objs["p/_tombstones/k"]) != "one" {
		t.Fatal("the second create overwrote the key")
	}
	// Racing creates: exactly one wins.
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := b.PutIfAbsent(context.Background(), "p/_tombstones/race", []byte(fmt.Sprint(i)))
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			if ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d creates of one key won, want 1", wins)
	}
}

// A store that does not implement conditional writes is switched to a check
// followed by a plain write, once, with the gauge saying so.
func TestS3Backend_FallbackWithoutConditionalWrites(t *testing.T) {
	m, pool := newMiniS3(t)
	m.noCond = true
	b := NewS3Backend(pool)
	created, err := b.PutIfAbsent(context.Background(), "p/_tombstones/k", []byte("one"))
	if err != nil || !created {
		t.Fatalf("create on a store without conditional writes = %v, %v", created, err)
	}
	if v := metrics.DeleteConditionalWrites.Get(); v != 0 {
		t.Fatalf("gauge = %d, want 0 (fallback)", v)
	}
	created, err = b.PutIfAbsent(context.Background(), "p/_tombstones/k", []byte("two"))
	if err != nil || created {
		t.Fatalf("second create = %v, %v, want not created", created, err)
	}
	if string(m.objs["p/_tombstones/k"]) != "one" {
		t.Fatal("the fallback overwrote an existing key")
	}
	m.mu.Lock()
	putsBefore := m.puts
	m.mu.Unlock()
	if _, err := b.PutIfAbsent(context.Background(), "p/_tombstones/k3", []byte("x")); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.puts-putsBefore != 1 {
		t.Fatalf("a create after the switch sent %d PUTs, want 1 (no conditional attempt)", m.puts-putsBefore)
	}
}

func TestS3Backend_GetExistsDelete(t *testing.T) {
	_, pool := newMiniS3(t)
	b := NewS3Backend(pool)
	if _, found, err := b.Get(context.Background(), "nope"); err != nil || found {
		t.Fatalf("get missing = %v %v", found, err)
	}
	if ok, err := b.Exists(context.Background(), "nope"); err != nil || ok {
		t.Fatalf("exists missing = %v %v", ok, err)
	}
	if _, err := b.PutIfAbsent(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if d, found, err := b.Get(context.Background(), "k"); err != nil || !found || string(d) != "v" {
		t.Fatalf("get = %q %v %v", d, found, err)
	}
	if ok, err := b.Exists(context.Background(), "k"); err != nil || !ok {
		t.Fatalf("exists = %v %v", ok, err)
	}
	if err := b.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := b.Exists(context.Background(), "k"); ok {
		t.Fatal("still exists after delete")
	}
}

// The first list request carries StartAfter; the later pages carry only the
// continuation token. Every key after the resume point is returned exactly once.
func TestS3Backend_ListAfterPaginates(t *testing.T) {
	m, pool := newMiniS3(t)
	m.pageSize = 10
	b := NewS3Backend(pool)
	for i := 0; i < 25; i++ {
		m.objs[fmt.Sprintf("p/_tombstones/t%016x-x.json", i+1)] = []byte("v")
	}
	m.objs["other/key"] = []byte("v")

	var got []string
	if err := b.ListAfter(context.Background(), "p/_tombstones/", "p/_tombstones/t"+hexTime(5), func(k string) error {
		got = append(got, k)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 21 { // the keys at or after time 5: 5..25
		t.Fatalf("listed %d keys, want 21", len(got))
	}
	if !sort.StringsAreSorted(got) || got[0] != "p/_tombstones/t0000000000000005-x.json" {
		t.Fatalf("keys not in order from the resume point: first %q", got[0])
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.listQuery) != 3 { // 21 keys at 10 per page
		t.Fatalf("%d list requests: %v", len(m.listQuery), m.listQuery)
	}
	if !strings.Contains(m.listQuery[0], "start-after=") || strings.Contains(m.listQuery[0], "continuation-token") {
		t.Fatalf("first page query %q", m.listQuery[0])
	}
	for _, q := range m.listQuery[1:] {
		if strings.Contains(q, "start-after=") || !strings.Contains(q, "continuation-token=") {
			t.Fatalf("later page query %q must carry only the continuation token", q)
		}
	}
}

// The whole protocol over the S3 wire: a delete created through one pod is
// learned by another by listing, an un-delete too, and a restart from the cache
// writes nothing.
func TestS3Backend_ProtocolEndToEnd(t *testing.T) {
	m, pool := newMiniS3(t)
	cache := t.TempDir()
	mk := func(withCache bool) *TombstoneStore {
		s := NewTombstoneStore()
		o := Options{Backend: NewS3Backend(pool), Prefix: "logs/", Interval: time.Minute}
		if withCache {
			o.CacheDir = cache
		}
		if err := s.Attach(o); err != nil {
			t.Fatal(err)
		}
		mustSync(t, s, true)
		return s
	}
	a, c := mk(false), mk(true)
	mustCreate(t, a, hideTombstone("d1"))
	mustSync(t, c, false)
	if !has(c, "d1") {
		t.Fatal("the other pod did not learn the delete over S3")
	}
	if err := c.Undelete(context.Background(), "d1"); err != nil {
		t.Fatal(err)
	}
	mustSync(t, a, false)
	if has(a, "d1") {
		t.Fatal("the un-delete did not reach the other pod over S3")
	}
	m.mu.Lock()
	m.puts = 0
	m.mu.Unlock()
	d := mk(true)
	if has(d, "d1") {
		t.Fatal("a restarted pod with a warm cache enforces an un-deleted tombstone")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.puts != 0 {
		t.Fatalf("a restart made %d PUTs", m.puts)
	}
}

// A 409 ConditionalRequestConflict means another write of the key is in flight, not
// that the key exists: the create is retried and then succeeds; a store that keeps
// answering 409 yields an error, never "already exists".
func TestS3Backend_ConditionalRequestConflictIsRetried(t *testing.T) {
	old := conflictBackoff
	conflictBackoff = time.Millisecond
	t.Cleanup(func() { conflictBackoff = old })
	m, pool := newMiniS3(t)
	b := NewS3Backend(pool)
	m.conflicts = 2
	created, err := b.PutIfAbsent(context.Background(), "p/_tombstones/c", []byte("x"))
	if err != nil || !created {
		t.Fatalf("create after two conflicts = %v, %v, want created", created, err)
	}
	m.conflicts = 100
	created, err = b.PutIfAbsent(context.Background(), "p/_tombstones/c2", []byte("x"))
	if err == nil || created {
		t.Fatalf("create under endless conflicts = %v, %v, want an error and not created", created, err)
	}
	if _, ok := m.objs["p/_tombstones/c2"]; ok {
		t.Fatal("a conflicting create stored the key")
	}

	// The backoff honours the caller's context: a cancelled create returns at once.
	conflictBackoff = time.Hour
	m.conflicts = 100
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.PutIfAbsent(ctx, "p/_tombstones/c3", []byte("x"))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled create reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the backoff ignored the cancelled context")
	}
}
