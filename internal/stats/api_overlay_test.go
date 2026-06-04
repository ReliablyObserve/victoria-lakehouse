package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestTenantsEndpoint_PrefersManifestForStorageFacts pins the fix
// for the global-vs-per-tenant mismatch: the registry cumulative
// counters drift higher than the manifest after compactions, so
// /tenants overlay manifest truth onto each entry's file/byte/row
// fields. Without this, /stats/overview (manifest-derived) and
// /tenants (registry-derived) report different totals to the UI.
func TestTenantsEndpoint_PrefersManifestForStorageFacts(t *testing.T) {
	registry := NewTenantRegistry("test")
	// Registry thinks tenant 1:1 wrote 1000 files / 10 GB (cumulative;
	// includes pre-compaction history).
	for i := 0; i < 1000; i++ {
		registry.RecordWrite("1:1", 10*1024*1024, 30*1024*1024, 100, "STANDARD")
	}

	// Manifest reports the post-compaction truth: 50 files / 5 GB.
	mf := manifest.New("test-bucket", "1/1/")
	for i := 0; i < 50; i++ {
		mf.AddFile("dt=2026-06-04/hour=10", manifest.FileInfo{
			Key:       "1/1/logs/dt=2026-06-04/hour=10/file" + tenantTestSeq(i) + ".parquet",
			Size:      100 * 1024 * 1024, // 100 MiB each → 5 GiB total
			RawBytes:  300 * 1024 * 1024,
			RowCount:  1000,
			MinTimeNs: time.Date(2026, 6, 4, 10, 0, 0, 0, time.UTC).UnixNano(),
			MaxTimeNs: time.Date(2026, 6, 4, 11, 0, 0, 0, time.UTC).UnixNano(),
		})
	}

	api := NewAPI(APIConfig{
		Registry: registry,
		Manifest: mf,
		Mode:     "logs",
		Bucket:   "test-bucket",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	var resp TenantsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(resp.Tenants) != 1 {
		t.Fatalf("got %d tenants, want 1", len(resp.Tenants))
	}
	got := resp.Tenants[0]
	if got.TotalFiles != 50 {
		t.Errorf("total_files = %d, want 50 (manifest truth, not 1000 registry)", got.TotalFiles)
	}
	if got.TotalBytes != 50*100*1024*1024 {
		t.Errorf("total_bytes = %d, want manifest truth not registry cumulative", got.TotalBytes)
	}
}

// TestTenantsEndpoint_SurfacesManifestOnlyTenants pins the path where
// data exists on S3 but no live registry entry (fresh process before
// the first write completes; stats-snapshot reset; etc.).
func TestTenantsEndpoint_SurfacesManifestOnlyTenants(t *testing.T) {
	registry := NewTenantRegistry("test")
	// No registry entries — only manifest data.

	mf := manifest.New("test-bucket", "")
	mf.AddFile("dt=2026-06-04/hour=10", manifest.FileInfo{
		Key:      "42/3/logs/dt=2026-06-04/hour=10/x.parquet",
		Size:     1024,
		RawBytes: 4096,
		RowCount: 10,
	})

	api := NewAPI(APIConfig{Registry: registry, Manifest: mf, Mode: "logs", Bucket: "b"})
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	var resp TenantsResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if len(resp.Tenants) != 1 {
		t.Fatalf("got %d tenants, want 1 manifest-only entry", len(resp.Tenants))
	}
	if resp.Tenants[0].AccountID != "42" || resp.Tenants[0].ProjectID != "3" {
		t.Errorf("got %s:%s, want 42:3",
			resp.Tenants[0].AccountID, resp.Tenants[0].ProjectID)
	}
}

func tenantTestSeq(i int) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 4)
	for j := 3; j >= 0; j-- {
		out[j] = hex[i&0xf]
		i >>= 4
	}
	return string(out)
}
