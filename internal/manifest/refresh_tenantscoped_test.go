package manifest

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
)

// Mock S3 ListObjectsV2 response. Subset of the real shape — only the
// fields RefreshFromS3 / discoverTenantPrefixes look at.
type listResultMock struct {
	XMLName        xml.Name `xml:"ListBucketResult"`
	Contents       []object `xml:"Contents"`
	CommonPrefixes []common `xml:"CommonPrefixes"`
	IsTruncated    bool     `xml:"IsTruncated"`
}
type object struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}
type common struct {
	Prefix string `xml:"Prefix"`
}

// mockS3Bucket holds the keys an httptest server pretends to serve via
// ListObjectsV2. Supports both flat LIST and delimited LIST (the latter
// is what discoverTenantPrefixes uses to find tenant prefixes).
type mockS3Bucket struct {
	keys map[string]int64 // key → size
}

func newMockBucket(keys map[string]int64) *mockS3Bucket {
	return &mockS3Bucket{keys: keys}
}

// handler implements just enough of S3 ListObjectsV2 for the manifest
// refresh path. The real semantics are documented at
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html;
// here we only need: prefix scoping, optional delimiter, CommonPrefixes
// rollup.
func (b *mockS3Bucket) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delim := q.Get("delimiter")

	res := listResultMock{}
	commonPrefixes := map[string]bool{}

	for k, size := range b.keys {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		if delim == "" {
			res.Contents = append(res.Contents, object{Key: k, Size: size})
			continue
		}
		rest := k[len(prefix):]
		if idx := strings.Index(rest, delim); idx >= 0 {
			cp := prefix + rest[:idx+1]
			commonPrefixes[cp] = true
		} else {
			res.Contents = append(res.Contents, object{Key: k, Size: size})
		}
	}

	for cp := range commonPrefixes {
		res.CommonPrefixes = append(res.CommonPrefixes, common{Prefix: cp})
	}
	sort.Slice(res.CommonPrefixes, func(i, j int) bool {
		return res.CommonPrefixes[i].Prefix < res.CommonPrefixes[j].Prefix
	})
	sort.Slice(res.Contents, func(i, j int) bool {
		return res.Contents[i].Key < res.Contents[j].Key
	})

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	if err := xml.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

// startMockBucket spins up the httptest server + returns the S3 client
// pointed at it. Helper for the table tests below.
func startMockBucket(t *testing.T, keys map[string]int64) (*httptest.Server, *http.Client) {
	t.Helper()
	b := newMockBucket(keys)
	srv := httptest.NewServer(http.HandlerFunc(b.handler))
	t.Cleanup(srv.Close)
	return srv, srv.Client()
}

// TestRefreshTenantScoped_DiscoversAllTenants pins the delimited LIST
// discovery: when given a flat bucket with N tenants × M files, the
// refresh must find all tenants and aggregate their files correctly.
func TestRefreshTenantScoped_DiscoversAllTenants(t *testing.T) {
	keys := map[string]int64{}
	// 3 tenants, 4 files each across 2 partitions.
	for _, tenant := range []string{"0/0", "1/1", "1001/0"} {
		for _, part := range []string{"dt=2026-06-04/hour=00", "dt=2026-06-04/hour=12"} {
			for i := 0; i < 2; i++ {
				keys[fmt.Sprintf("%s/traces/%s/f%d.parquet", tenant, part, i)] = 100
			}
		}
	}

	srv, _ := startMockBucket(t, keys)
	client := coverageS3Client(t, srv.URL)

	m := New("test-bucket", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	got, totalFiles, totalBytes, err := m.refreshTenantScoped(t.Context(), client)
	if err != nil {
		t.Fatalf("refreshTenantScoped: %v", err)
	}
	if totalFiles != 12 {
		t.Errorf("totalFiles=%d, want 12", totalFiles)
	}
	if totalBytes != 1200 {
		t.Errorf("totalBytes=%d, want 1200", totalBytes)
	}
	if len(got) != 2 { // two partitions
		t.Errorf("partitions=%d, want 2", len(got))
	}
	// Each partition holds 6 files (3 tenants × 2 files each).
	for partition, files := range got {
		if len(files) != 6 {
			t.Errorf("partition %s: files=%d, want 6", partition, len(files))
		}
	}
}

// TestRefreshTenantScoped_ParityWithFullBucket pins the most important
// invariant: tenant-scoped refresh must produce the SAME (file count,
// bytes, per-partition layout) as the full-bucket LIST on the same
// corpus. Any drift is a correctness bug — RefreshFromS3 is supposed
// to be a faithful snapshot of S3 state regardless of strategy.
func TestRefreshTenantScoped_ParityWithFullBucket(t *testing.T) {
	// Realistic mixed corpus: multiple tenants, partitions, file sizes.
	keys := map[string]int64{
		"0/0/traces/dt=2026-06-04/hour=00/a.parquet":   1000,
		"0/0/traces/dt=2026-06-04/hour=12/b.parquet":   2000,
		"0/0/traces/dt=2026-06-04/hour=12/c.parquet":   3000,
		"1/1/traces/dt=2026-06-04/hour=00/d.parquet":   4000,
		"1/1/traces/dt=2026-06-05/hour=00/e.parquet":   5000,
		"1001/0/traces/dt=2026-06-04/hour=00/f.parquet": 6000,
		// Non-parquet keys must be skipped on both paths.
		"0/0/traces/dt=2026-06-04/hour=12/_metadata.json": 99,
		"_bloom_index.bin":                                999,
	}

	srv, _ := startMockBucket(t, keys)
	client := coverageS3Client(t, srv.URL)

	m1 := New("test-bucket", "")
	m1.prefixTemplate = "{AccountID}/{ProjectID}/"
	full, fullFiles, fullBytes, err := m1.refreshFullBucket(t.Context(), client, "")
	if err != nil {
		t.Fatalf("refreshFullBucket: %v", err)
	}

	m2 := New("test-bucket", "")
	m2.prefixTemplate = "{AccountID}/{ProjectID}/"
	scoped, scopedFiles, scopedBytes, err := m2.refreshTenantScoped(t.Context(), client)
	if err != nil {
		t.Fatalf("refreshTenantScoped: %v", err)
	}

	if fullFiles != scopedFiles {
		t.Errorf("file count drift: full=%d scoped=%d", fullFiles, scopedFiles)
	}
	if fullBytes != scopedBytes {
		t.Errorf("byte count drift: full=%d scoped=%d", fullBytes, scopedBytes)
	}
	if len(full) != len(scoped) {
		t.Errorf("partition count drift: full=%d scoped=%d", len(full), len(scoped))
	}
	for partition, fullFiles := range full {
		scopedFiles, ok := scoped[partition]
		if !ok {
			t.Errorf("partition %q in full but not scoped", partition)
			continue
		}
		if len(fullFiles) != len(scopedFiles) {
			t.Errorf("partition %q: full=%d files, scoped=%d", partition, len(fullFiles), len(scopedFiles))
		}
	}
}

// TestRefreshTenantScoped_EmptyBucket guards the degenerate case —
// fresh deployment with no parquet files yet. Discovery returns no
// tenants; the file map is empty; no spurious errors.
func TestRefreshTenantScoped_EmptyBucket(t *testing.T) {
	srv, _ := startMockBucket(t, map[string]int64{})
	client := coverageS3Client(t, srv.URL)

	m := New("test-bucket", "")
	m.prefixTemplate = "{AccountID}/{ProjectID}/"

	got, files, bytes, err := m.refreshTenantScoped(t.Context(), client)
	if err != nil {
		t.Fatalf("refreshTenantScoped on empty bucket: %v", err)
	}
	if len(got) != 0 || files != 0 || bytes != 0 {
		t.Errorf("expected empty result, got: files=%d bytes=%d partitions=%d", files, bytes, len(got))
	}
}

// TestDiscoverTenantPrefixes_OrgIDTemplate covers the single-segment
// {OrgID}/ template path: the discovery must NOT descend into a
// second level (no project ID exists).
func TestDiscoverTenantPrefixes_OrgIDTemplate(t *testing.T) {
	keys := map[string]int64{
		"acme/data/a.parquet":   1,
		"globex/data/b.parquet": 1,
	}
	srv, _ := startMockBucket(t, keys)
	client := coverageS3Client(t, srv.URL)

	m := New("test-bucket", "")
	m.prefixTemplate = "{OrgID}/"

	got, err := m.discoverTenantPrefixes(t.Context(), client)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	sort.Strings(got)
	if len(got) != 2 || got[0] != "acme/" || got[1] != "globex/" {
		t.Errorf("OrgID discovery wrong: %v", got)
	}
}

var _ = url.Parse // keep url import for future expansion
