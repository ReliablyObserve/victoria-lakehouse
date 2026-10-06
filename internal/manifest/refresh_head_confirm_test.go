package manifest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// The cliff rule (#404 round 3, #418): a listing that drops more than half of
// the tracked files is believed only when HEAD requests on a random sample of
// the dropped keys all answer 404. Applies to every refresh, the first one
// after a snapshot load included.

// headBucket is a bucket whose LIST answers `listed` and whose HEAD answers
// 200 for `live` keys, 404 otherwise, or 503 when headErr is set.
type headBucket struct {
	mu      sync.Mutex
	listed  []string
	live    map[string]bool
	headErr bool
	heads   atomic.Int64
	srv     *httptest.Server
}

func newHeadBucket(t *testing.T, listed []string, live ...string) *headBucket {
	t.Helper()
	b := &headBucket{listed: listed, live: map[string]bool{}}
	for _, k := range live {
		b.live[k] = true
	}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if r.Method == http.MethodHead {
			b.heads.Add(1)
			if b.headErr {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			key := strings.TrimPrefix(r.URL.Path, "/b/")
			if b.live[key] {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var sb strings.Builder
		sb.WriteString(`<ListBucketResult>`)
		for _, k := range b.listed {
			sb.WriteString(`<Contents><Key>` + k + `</Key><Size>100</Size></Contents>`)
		}
		sb.WriteString(`<IsTruncated>false</IsTruncated></ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(sb.String()))
	}))
	t.Cleanup(b.srv.Close)
	return b
}

func manifestWithKeys(n int) (*Manifest, []string) {
	m := New("b", "")
	keys := make([]string, n)
	for i := range keys {
		keys[i] = refreshKey(fmt.Sprintf("f%03d", i))
		m.AddFile(refreshPartition, enriched(keys[i], 1))
	}
	return m, keys
}

func snapshotLoaded(t *testing.T, src *Manifest) *Manifest {
	t.Helper()
	path := filepath.Join(t.TempDir(), "m.snap")
	if err := src.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	m := New("b", "")
	if err := m.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	return m
}

func incomplete(reason string) uint64 { return metrics.ManifestRefreshIncomplete.Get(reason) }

// A bucket that really shrank (a peer's compaction removed most files) is
// accepted on the first listing after a snapshot, and later: no 50% rule that
// sticks forever.
func TestCliffHead_LegitimateShrinkIsAcceptedFirstAndLater(t *testing.T) {
	src, keys := manifestWithKeys(40)
	m := snapshotLoaded(t, src)
	b := newHeadBucket(t, keys[:5]) // the other 35 return 404
	if err := m.RefreshFromS3(context.Background(), coverageS3Client(t, b.srv.URL)); err != nil {
		t.Fatalf("a confirmed shrink on the first listing was not applied: %v", err)
	}
	if m.TotalFiles() != 5 || !m.Listed() || m.LastCompleteRefresh().Generation != 1 {
		t.Fatalf("files=%d listed=%v gen=%d", m.TotalFiles(), m.Listed(), m.LastCompleteRefresh().Generation)
	}
	if n := b.heads.Load(); n == 0 || n > cliffProbeSample {
		t.Fatalf("HEAD calls = %d, want 1..%d", n, cliffProbeSample)
	}

	// A later legitimate shrink (>50% again) is accepted too: the guard does
	// not stick.
	b.mu.Lock()
	b.listed = keys[:1]
	b.mu.Unlock()
	if err := m.RefreshFromS3(context.Background(), coverageS3Client(t, b.srv.URL)); err != nil {
		t.Fatalf("a later confirmed shrink was rejected: %v", err)
	}
	if m.TotalFiles() != 1 {
		t.Fatalf("files=%d, want 1", m.TotalFiles())
	}
}

// A sparse listing whose dropped keys are still live is rejected on every
// refresh, including the first after a snapshot; nothing becomes "listed".
func TestCliffHead_SparseListingWithLiveKeysIsRejected(t *testing.T) {
	src, keys := manifestWithKeys(40)
	m := snapshotLoaded(t, src)
	before := incomplete("rejected")
	b := newHeadBucket(t, keys[:2], keys...) // everything still exists
	err := m.RefreshFromS3(context.Background(), coverageS3Client(t, b.srv.URL))
	if !errors.Is(err, ErrRefreshRejected) {
		t.Fatalf("err=%v, want ErrRefreshRejected", err)
	}
	if m.TotalFiles() != 40 || m.Listed() || m.LastCompleteRefresh().Generation != 0 {
		t.Fatalf("a rejected listing changed state: files=%d listed=%v gen=%d", m.TotalFiles(), m.Listed(), m.LastCompleteRefresh().Generation)
	}
	if incomplete("rejected") != before+1 {
		t.Fatalf("rejected counter did not tick")
	}
}

// A HEAD that fails (5xx, timeout) is treated as incomplete, never as "gone".
func TestCliffHead_HeadErrorIsUnconfirmed(t *testing.T) {
	src, keys := manifestWithKeys(40)
	m := snapshotLoaded(t, src)
	before := incomplete("head_unconfirmed")
	b := newHeadBucket(t, keys[:2])
	b.headErr = true
	err := m.RefreshFromS3(context.Background(), coverageS3Client(t, b.srv.URL))
	if !errors.Is(err, ErrRefreshRejected) {
		t.Fatalf("err=%v, want ErrRefreshRejected", err)
	}
	if m.TotalFiles() != 40 || m.Listed() {
		t.Fatalf("files=%d listed=%v", m.TotalFiles(), m.Listed())
	}
	if incomplete("head_unconfirmed") != before+1 {
		t.Fatalf("head_unconfirmed counter did not tick")
	}
}

// ApplyListing has no S3 client: without an ObjectProber a shrinking listing
// cannot be confirmed and is rejected; with one it follows the same rule.
func TestCliffHead_ApplyListingNeedsAProber(t *testing.T) {
	src, keys := manifestWithKeys(10)
	m := snapshotLoaded(t, src)
	sparse := []ListedObject{{Key: keys[0], Size: 1}}
	if m.ApplyListing(sparse, time.Now()) {
		t.Fatal("a shrinking listing was applied without any way to confirm it")
	}
	var dead atomic.Bool
	m.SetObjectProber(func(_ context.Context, _, key string) (bool, error) { return !dead.Load(), nil })
	if m.ApplyListing(sparse, time.Now()) {
		t.Fatal("a listing whose dropped keys answer 200 was applied")
	}
	dead.Store(true)
	if !m.ApplyListing(sparse, time.Now()) {
		t.Fatal("a confirmed shrink was rejected")
	}
	if !m.Listed() || m.TotalFiles() != 1 {
		t.Fatalf("listed=%v files=%d", m.Listed(), m.TotalFiles())
	}
}

// One live key among the sample settles it, however many others are gone.
func TestCliffHead_OneLiveKeyInTheSampleRejects(t *testing.T) {
	src, keys := manifestWithKeys(8) // <= sample size: every dropped key is probed
	m := snapshotLoaded(t, src)
	b := newHeadBucket(t, keys[:1], keys[5]) // only one dropped key still exists
	err := m.RefreshFromS3(context.Background(), coverageS3Client(t, b.srv.URL))
	if !errors.Is(err, ErrRefreshRejected) {
		t.Fatalf("err=%v", err)
	}
}

// A partial listing keeps the skipped account's previous entries, does not set
// listed or advance the complete-refresh generation, and returns ErrRefreshPartial.
func TestPartial_KeepsPreviousEntriesAndIsNotListed(t *testing.T) {
	m, srv, failing := partialFixture(t)
	if err := m.RefreshFromS3(context.Background(), srv); err != nil {
		t.Fatal(err)
	}
	gen := m.LastCompleteRefresh()
	if gen.Generation != 1 || !m.Listed() {
		t.Fatalf("fixture: gen=%d listed=%v", gen.Generation, m.Listed())
	}
	before := incomplete("partial")
	failing.Store(true)
	err := m.RefreshFromS3(context.Background(), srv)
	if !errors.Is(err, ErrRefreshPartial) {
		t.Fatalf("err=%v, want ErrRefreshPartial", err)
	}
	if m.TotalFiles() != 3 {
		t.Fatalf("the skipped account's files were dropped: %d files, want 3", m.TotalFiles())
	}
	if got := m.LastCompleteRefresh(); got != gen {
		t.Fatalf("a partial listing advanced the complete refresh: %+v -> %+v", gen, got)
	}
	if incomplete("partial") != before+1 {
		t.Fatal("partial counter did not tick")
	}
	failing.Store(false)
	if err := m.RefreshFromS3(context.Background(), srv); err != nil {
		t.Fatal(err)
	}
	if m.LastCompleteRefresh().Generation != gen.Generation+1 {
		t.Fatal("a complete listing after a partial one must advance the generation")
	}
}

// A partial FIRST listing after a snapshot does not mark the manifest listed.
func TestPartial_FirstListingAfterSnapshotIsNotListed(t *testing.T) {
	m0, srv, failing := partialFixture(t)
	if err := m0.RefreshFromS3(context.Background(), srv); err != nil {
		t.Fatal(err)
	}
	m := snapshotLoaded(t, m0)
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	failing.Store(true)
	if err := m.RefreshFromS3(context.Background(), srv); !errors.Is(err, ErrRefreshPartial) {
		t.Fatalf("err=%v", err)
	}
	if m.Listed() || m.LastCompleteRefresh().Generation != 0 {
		t.Fatalf("a partial first listing marked the manifest listed (gen=%d)", m.LastCompleteRefresh().Generation)
	}
	if m.TotalFiles() != 3 {
		t.Fatalf("files=%d, want the snapshot's 3 kept", m.TotalFiles())
	}
}

// partialFixture is a two-account bucket (1 file for account 1, 2 for account
// 2) whose project listing of account 2 fails while the returned flag is set.
func partialFixture(t *testing.T) (*Manifest, *s3.Client, *atomic.Bool) {
	t.Helper()
	keys := map[string]int64{
		fmt.Sprintf("1/0/logs/%s/a.parquet", refreshPartition): 100,
		fmt.Sprintf("2/0/logs/%s/b.parquet", refreshPartition): 100,
		fmt.Sprintf("2/0/logs/%s/c.parquet", refreshPartition): 100,
	}
	b := newMockBucket(keys)
	failing := &atomic.Bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failing.Load() && r.URL.Query().Get("delimiter") != "" && r.URL.Query().Get("prefix") == "2/" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code></Error>`))
			return
		}
		b.handler(w, r)
	}))
	t.Cleanup(srv.Close)
	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	m.SetSignalSuffix("logs/")
	return m, coverageS3Client(t, srv.URL), failing
}

func TestCompleteSince(t *testing.T) {
	m, keys := manifestWithKeys(3)
	if m.CompleteSince(time.Time{}) {
		t.Fatal("no listing yet")
	}
	t0 := time.Now()
	time.Sleep(2 * time.Millisecond)
	var objs []ListedObject
	for _, k := range keys {
		objs = append(objs, ListedObject{Key: k, Size: 1})
	}
	start := time.Now()
	if !m.ApplyListing(objs, start) {
		t.Fatal("fixture")
	}
	if !m.CompleteSince(t0) {
		t.Fatal("a complete listing that began after t0 must count")
	}
	if m.CompleteSince(start.Add(time.Millisecond)) {
		t.Fatal("a listing that began before t must not count")
	}
}
