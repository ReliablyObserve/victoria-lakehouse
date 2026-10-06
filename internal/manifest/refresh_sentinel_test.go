package manifest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// RefreshFromS3 tells a caller whether the manifest now holds the whole bucket
// (nil), or only serves: the cliff guard kept the old set, or a tenant could
// not be listed. The insert buffer releases its restored segments only on nil
// (#379).

func fillManifest(m *Manifest, n int) {
	for i := 0; i < n; i++ {
		m.AddFile(refreshPartition, enriched(refreshKey("f"+strconv.Itoa(i)), 1))
	}
}

func TestRefreshFromS3_CliffGuardRejectionIsAnError(t *testing.T) {
	m := New("b", "")
	fillManifest(m, 10)
	keys := make([]string, 10)
	for i := range keys {
		keys[i] = refreshKey("f" + strconv.Itoa(i))
	}
	client := coverageS3Client(t, listingServer(t, keys...))
	// A cold start has nothing tracked to guard.
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if !m.Listed() {
		t.Fatal("fixture: the first refresh must mark the manifest listed")
	}
	sparse := coverageS3Client(t, listingServer(t, keys[:2]...))
	err := m.RefreshFromS3(context.Background(), sparse)
	if !errors.Is(err, ErrRefreshRejected) {
		t.Fatalf("a refresh the cliff guard rejects returned %v, want ErrRefreshRejected", err)
	}
	if m.TotalFiles() != 10 {
		t.Errorf("the rejected refresh changed the manifest: %d files, want 10", m.TotalFiles())
	}
}

// A project listing that fails for one account leaves that account out of the
// listing: what was listed is applied, the caller is told it is partial.
func TestRefreshFromS3_PartialTenantListingIsAnError(t *testing.T) {
	keys := map[string]int64{}
	for _, tenant := range []string{"1/0", "2/0"} {
		keys[fmt.Sprintf("%s/logs/%s/f.parquet", tenant, refreshPartition)] = 100
	}
	b := newMockBucket(keys)
	var failing bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failing && r.URL.Query().Get("delimiter") != "" && r.URL.Query().Get("prefix") == "2/" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code></Error>`))
			return
		}
		b.handler(w, r)
	}))
	t.Cleanup(srv.Close)
	client := coverageS3Client(t, srv.URL)

	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	failing = true
	err := m.RefreshFromS3(context.Background(), client)
	if !errors.Is(err, ErrRefreshPartial) {
		t.Fatalf("a refresh that could not list an account returned %v, want ErrRefreshPartial", err)
	}
	if m.TotalFiles() != 1 {
		t.Errorf("the listable account must be applied: %d files, want 1", m.TotalFiles())
	}
	failing = false
	if err := m.RefreshFromS3(context.Background(), client); err != nil {
		t.Fatalf("a complete refresh: %v", err)
	}
	if m.TotalFiles() != 2 {
		t.Errorf("%d files after a complete listing, want 2", m.TotalFiles())
	}
}
