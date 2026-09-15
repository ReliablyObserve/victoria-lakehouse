package manifest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// multiBucketS3 serves ListObjectsV2 for several buckets (path-style
// /<bucket>?list-type=2&prefix=...), records which buckets were listed, and can
// fail a bucket to exercise the refresh's error handling.
type multiBucketS3 struct {
	mu      sync.Mutex
	buckets map[string]*mockS3Bucket
	fail    map[string]bool
	listed  map[string]int
	delay   map[string]time.Duration
	// inflight/maxInflight record the LIST concurrency the refresh drives.
	inflight    int
	maxInflight int
	// rendezvous makes that concurrency OBSERVED rather than hoped for: the
	// first rendezvousN requests block until all of them have arrived, so they
	// are provably in flight together. Reading maxInflight without it asks the
	// Go scheduler to interleave two goroutines and calls the test failed when
	// it does not — which is why this test failed only under full-suite load,
	// where one LIST can finish before the next one starts.
	rendezvousN   int
	rendezvousCh  chan struct{}
	arrived       int
	defaultBucket string
}

// rendezvous blocks until rendezvousN callers have arrived, or until the
// deadline passes. The deadline is the SERIAL case: an implementation that
// issues the LISTs one at a time can never assemble the group, and must not
// hang the suite — it falls through, maxInflight stays 1, and the assertion
// reports exactly that.
func (m *multiBucketS3) rendezvous(bucket string) {
	m.mu.Lock()
	// Only the DEDICATED-bucket LISTs are the parallel group. The default
	// bucket is listed on its own, so holding it here would wait out the whole
	// deadline on every run for nothing.
	if m.rendezvousN == 0 || m.rendezvousCh == nil || bucket == m.defaultBucket {
		m.mu.Unlock()
		return
	}
	m.arrived++
	if m.arrived == m.rendezvousN {
		close(m.rendezvousCh)
	}
	ch := m.rendezvousCh
	m.mu.Unlock()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
	}
}

func newMultiBucketS3(t *testing.T, buckets map[string]map[string]int64) (*multiBucketS3, *httptest.Server) {
	t.Helper()
	m := &multiBucketS3{buckets: map[string]*mockS3Bucket{}, fail: map[string]bool{}, listed: map[string]int{}, delay: map[string]time.Duration{}}
	for name, keys := range buckets {
		m.buckets[name] = newMockBucket(keys)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)[0]
		m.mu.Lock()
		m.listed[bucket]++
		m.inflight++
		if m.inflight > m.maxInflight {
			m.maxInflight = m.inflight
		}
		b, fail, delay := m.buckets[bucket], m.fail[bucket], m.delay[bucket]
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			m.inflight--
			m.mu.Unlock()
		}()
		m.rendezvous(bucket)
		if delay > 0 {
			time.Sleep(delay)
		}
		if fail {
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>AccessDenied</Code></Error>`)
			return
		}
		if b == nil {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>NoSuchBucket</Code></Error>`)
			return
		}
		b.handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return m, srv
}

func tenantBucketManifest() *Manifest {
	m := New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	m.SetSignalSuffix("logs/")
	return m
}

const tbPart = "dt=2026-06-04/hour=00"

// TestRefresh_ListsDedicatedTenantBuckets: objects that live only in a
// tenant's dedicated bucket survive a refresh, carry their bucket, and are
// visible to that tenant's scoped lookup.
func TestRefresh_ListsDedicatedTenantBuckets(t *testing.T) {
	s3m, srv := newMultiBucketS3(t, map[string]map[string]int64{
		"test-bucket": {
			"0/0/logs/" + tbPart + "/a.parquet": 100,
			"1/1/logs/" + tbPart + "/b.parquet": 100,
		},
		"bucket-tenant-1002": {
			"1002/0/logs/" + tbPart + "/c.parquet":   300,
			"1002/0/traces/" + tbPart + "/t.parquet": 999, // other signal: not listed by the logs manifest
		},
	})
	client := coverageS3Client(t, srv.URL)

	m := tenantBucketManifest()
	m.SetTenantBuckets([]TenantBucket{{Bucket: "bucket-tenant-1002", Prefix: "1002/0/"}})

	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}
	if got := m.TotalFiles(); got != 3 {
		t.Errorf("TotalFiles = %d, want 3 (2 in the default bucket + 1 in the dedicated bucket)", got)
	}
	if got := m.TotalBytes(); got != 500 {
		t.Errorf("TotalBytes = %d, want 500", got)
	}
	files := m.GetFilesForRangeTenant(0, 1<<62, "1002", "0")
	if len(files) != 1 || files[0].Key != "1002/0/logs/"+tbPart+"/c.parquet" || files[0].Bucket != "bucket-tenant-1002" {
		t.Errorf("tenant 1002:0 objects = %+v, want its dedicated-bucket object stamped with the bucket", files)
	}
	if other := m.GetFilesForRangeTenant(0, 1<<62, "1", "1"); len(other) != 1 || other[0].Bucket != "" {
		t.Errorf("tenant 1:1 objects = %+v, want its default-bucket object only", other)
	}
	s3m.mu.Lock()
	defer s3m.mu.Unlock()
	if s3m.listed["bucket-tenant-1002"] == 0 {
		t.Error("the dedicated bucket was never listed")
	}
}

// TestRefresh_DedicatedBucketDuplicateKeyKeptOnce: a key present in both the
// default bucket and the tenant's dedicated bucket (a migration in progress)
// is kept once, as the dedicated-bucket copy.
func TestRefresh_DedicatedBucketDuplicateKeyKeptOnce(t *testing.T) {
	key := "1002/0/logs/" + tbPart + "/c.parquet"
	_, srv := newMultiBucketS3(t, map[string]map[string]int64{
		"test-bucket":        {key: 100, "0/0/logs/" + tbPart + "/a.parquet": 100},
		"bucket-tenant-1002": {key: 120},
	})
	client := coverageS3Client(t, srv.URL)

	m := tenantBucketManifest()
	m.SetTenantBuckets([]TenantBucket{{Bucket: "bucket-tenant-1002", Prefix: "1002/0/"}})
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}
	files := m.GetFilesForRangeTenant(0, 1<<62, "1002", "0")
	if len(files) != 1 {
		t.Fatalf("tenant 1002:0 has %d entries for one key, want 1: %+v", len(files), files)
	}
	if files[0].Bucket != "bucket-tenant-1002" || files[0].Size != 120 {
		t.Errorf("kept %+v, want the dedicated-bucket copy (size 120)", files[0])
	}
	if got := m.TotalFiles(); got != 2 {
		t.Errorf("TotalFiles = %d, want 2", got)
	}
	if got := m.TotalBytes(); got != 220 {
		t.Errorf("TotalBytes = %d, want 220", got)
	}
}

// TestRefresh_DedicatedBucketListFailureKeepsPreviousState: if a dedicated
// bucket cannot be listed the refresh fails and the previous manifest stays,
// instead of swapping in a file set that lost the tenant.
func TestRefresh_DedicatedBucketListFailureKeepsPreviousState(t *testing.T) {
	s3m, srv := newMultiBucketS3(t, map[string]map[string]int64{
		"test-bucket":        {"0/0/logs/" + tbPart + "/a.parquet": 100},
		"bucket-tenant-1002": {"1002/0/logs/" + tbPart + "/c.parquet": 300},
	})
	client := coverageS3Client(t, srv.URL)

	m := tenantBucketManifest()
	m.SetTenantBuckets([]TenantBucket{{Bucket: "bucket-tenant-1002", Prefix: "1002/0/"}})
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if m.TotalFiles() != 2 {
		t.Fatalf("precondition: TotalFiles = %d, want 2", m.TotalFiles())
	}

	s3m.mu.Lock()
	s3m.fail["bucket-tenant-1002"] = true
	s3m.mu.Unlock()

	if err := m.RefreshFromS3(context.Background(), client); err == nil {
		t.Fatal("refresh must fail when a dedicated tenant bucket cannot be listed")
	}
	if got := len(m.GetFilesForRangeTenant(0, 1<<62, "1002", "0")); got != 1 {
		t.Errorf("after the failed refresh tenant 1002:0 has %d objects, want its 1 object kept", got)
	}
}

// TestRefresh_NoTenantBucketsListsOnlyDefault: without registered dedicated
// buckets the refresh behaves exactly as before — one bucket listed.
func TestRefresh_NoTenantBucketsListsOnlyDefault(t *testing.T) {
	s3m, srv := newMultiBucketS3(t, map[string]map[string]int64{
		"test-bucket":        {"0/0/logs/" + tbPart + "/a.parquet": 100},
		"bucket-tenant-1002": {"1002/0/logs/" + tbPart + "/c.parquet": 300},
	})
	client := coverageS3Client(t, srv.URL)

	m := tenantBucketManifest()
	// Entries that must be ignored: empty bucket, the default bucket itself,
	// and an empty prefix.
	m.SetTenantBuckets([]TenantBucket{{Bucket: "", Prefix: "9/9/"}, {Bucket: "test-bucket", Prefix: "0/0/"}, {Bucket: "bucket-tenant-1002", Prefix: ""}})
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("TotalFiles = %d, want 1", got)
	}
	s3m.mu.Lock()
	defer s3m.mu.Unlock()
	if s3m.listed["bucket-tenant-1002"] != 0 {
		t.Error("an ignored dedicated-bucket entry was listed")
	}
}

// TestRefresh_DedicatedBucketObjectRegisteredByFlushSurvives: the writer
// registers a flushed object locally before any refresh has listed it; the
// refresh must keep it (with its enrichment) and learn its bucket.
func TestRefresh_DedicatedBucketObjectRegisteredByFlushSurvives(t *testing.T) {
	key := "1002/0/logs/" + tbPart + "/c.parquet"
	_, srv := newMultiBucketS3(t, map[string]map[string]int64{
		"test-bucket":        {"0/0/logs/" + tbPart + "/a.parquet": 100},
		"bucket-tenant-1002": {key: 300},
	})
	client := coverageS3Client(t, srv.URL)

	m := tenantBucketManifest()
	m.SetTenantBuckets([]TenantBucket{{Bucket: "bucket-tenant-1002", Prefix: "1002/0/"}})
	m.AddFile(tbPart, FileInfo{Key: key, Size: 300, RowCount: 42, MinTimeNs: 1, MaxTimeNs: 2})

	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}
	files := m.GetFilesForRangeTenant(0, 1<<62, "1002", "0")
	if len(files) != 1 {
		t.Fatalf("tenant 1002:0 has %d objects after refresh, want 1: %+v", len(files), files)
	}
	if files[0].RowCount != 42 || files[0].Bucket != "bucket-tenant-1002" {
		t.Errorf("kept %+v, want the flush-time enrichment (RowCount 42) plus the listed bucket", files[0])
	}
}

// TestRefresh_TenantBucketListFailuresAreCounted: every dedicated bucket that
// fails to list is counted under its own label, so an operator can see which
// bucket is freezing the fleet's manifest, and the refresh error names the
// first failure in registered order (not whichever LIST answered first).
func TestRefresh_TenantBucketListFailuresAreCounted(t *testing.T) {
	const first, second = "bucket-count-first", "bucket-count-second"
	s3m, srv := newMultiBucketS3(t, map[string]map[string]int64{
		"test-bucket": {"0/0/logs/" + tbPart + "/a.parquet": 100},
		first:         {"1002/0/logs/" + tbPart + "/c.parquet": 300},
		second:        {"1003/0/logs/" + tbPart + "/d.parquet": 300},
	})
	client := coverageS3Client(t, srv.URL)

	m := tenantBucketManifest()
	// Registering a bucket exports its series at zero, so an alert can fire on
	// the first failure instead of waiting for the series to appear.
	m.SetTenantBuckets([]TenantBucket{
		{Bucket: first, Prefix: "1002/0/"},
		{Bucket: second, Prefix: "1003/0/"},
	})
	before := map[string]uint64{}
	for _, bucket := range []string{first, second} {
		before[bucket] = metrics.ManifestTenantBucketListErrors.Get(bucket)
	}

	s3m.mu.Lock()
	s3m.fail[first] = true
	s3m.fail[second] = true
	// The first bucket in registered order answers last.
	s3m.delay[first] = 40 * time.Millisecond
	s3m.mu.Unlock()

	err := m.RefreshFromS3(context.Background(), client)
	if err == nil {
		t.Fatal("refresh must fail when a dedicated tenant bucket cannot be listed")
	}
	if !strings.Contains(err.Error(), first) {
		t.Errorf("refresh error = %v, want the first failing bucket in registered order (%s)", err, first)
	}
	for _, bucket := range []string{first, second} {
		if got := metrics.ManifestTenantBucketListErrors.Get(bucket) - before[bucket]; got != 1 {
			t.Errorf("%s: LIST errors rose by %d, want 1", bucket, got)
		}
	}
}

// TestRefresh_TenantBucketListsAreBoundedAndOrdered: the dedicated-bucket LISTs
// run concurrently but bounded, and the merge follows the registered order —
// with the same key in two dedicated buckets, the last registered bucket wins
// whichever LIST answers first.
func TestRefresh_TenantBucketListsAreBoundedAndOrdered(t *testing.T) {
	dupKey := "1002/0/logs/" + tbPart + "/dup.parquet"
	buckets := map[string]map[string]int64{
		"test-bucket": {"0/0/logs/" + tbPart + "/a.parquet": 100},
		// Both dedicated buckets hold the same key with different sizes.
		"bucket-tenant-A": {dupKey: 111},
		"bucket-tenant-B": {dupKey: 222},
	}
	var registered []TenantBucket
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("bucket-filler-%d", i)
		buckets[name] = map[string]int64{fmt.Sprintf("90%d/0/logs/%s/f.parquet", i, tbPart): 10}
		registered = append(registered, TenantBucket{Bucket: name, Prefix: fmt.Sprintf("90%d/0/", i)})
	}
	registered = append(registered,
		TenantBucket{Bucket: "bucket-tenant-A", Prefix: "1002/0/"},
		TenantBucket{Bucket: "bucket-tenant-B", Prefix: "1002/0/"})

	s3m, srv := newMultiBucketS3(t, buckets)
	client := coverageS3Client(t, srv.URL)
	s3m.mu.Lock()
	// A (registered before B) answers last; a merge that followed completion
	// order would keep A's copy.
	s3m.delay["bucket-tenant-A"] = 60 * time.Millisecond
	// Hold the first two LISTs together so they are provably concurrent. Two,
	// not more: tenantBucketListMaxParallel caps how many the refresh runs at
	// once, and a rendezvous wider than that cap would deadlock on a correct
	// implementation.
	s3m.rendezvousN = 2
	s3m.rendezvousCh = make(chan struct{})
	s3m.defaultBucket = "test-bucket"
	s3m.mu.Unlock()

	m := tenantBucketManifest()
	m.SetTenantBuckets(registered)
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}

	files := m.GetFilesForRangeTenant(0, 1<<62, "1002", "0")
	if len(files) != 1 {
		t.Fatalf("duplicate key kept %d times: %+v", len(files), files)
	}
	if files[0].Bucket != "bucket-tenant-B" || files[0].Size != 222 {
		t.Errorf("merged %+v, want the last registered bucket's copy (bucket-tenant-B, size 222)", files[0])
	}
	if got := m.TotalFiles(); got != 8 {
		t.Errorf("TotalFiles = %d, want 8 (1 default + 6 fillers + 1 deduped)", got)
	}

	s3m.mu.Lock()
	defer s3m.mu.Unlock()
	if s3m.maxInflight < 2 {
		t.Errorf("dedicated-bucket LISTs ran with max %d in flight; they must overlap", s3m.maxInflight)
	}
	if s3m.maxInflight > tenantBucketListMaxParallel+1 { // +1: the default bucket's own LIST may overlap
		t.Errorf("dedicated-bucket LISTs ran %d in flight, want at most %d", s3m.maxInflight, tenantBucketListMaxParallel+1)
	}
}
