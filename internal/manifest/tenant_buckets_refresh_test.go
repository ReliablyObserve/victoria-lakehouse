package manifest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// multiBucketS3 serves ListObjectsV2 for several buckets (path-style
// /<bucket>?list-type=2&prefix=...), records which buckets were listed, and can
// fail a bucket to exercise the refresh's error handling.
type multiBucketS3 struct {
	mu      sync.Mutex
	buckets map[string]*mockS3Bucket
	fail    map[string]bool
	listed  map[string]int
}

func newMultiBucketS3(t *testing.T, buckets map[string]map[string]int64) (*multiBucketS3, *httptest.Server) {
	t.Helper()
	m := &multiBucketS3{buckets: map[string]*mockS3Bucket{}, fail: map[string]bool{}, listed: map[string]int{}}
	for name, keys := range buckets {
		m.buckets[name] = newMockBucket(keys)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)[0]
		m.mu.Lock()
		m.listed[bucket]++
		b, fail := m.buckets[bucket], m.fail[bucket]
		m.mu.Unlock()
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
