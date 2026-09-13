package parquets3

import (
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// BenchmarkTenantScope_FileSelection measures the per-query cost of the scoped
// object selection (manifest per-tenant lookup + legacy lookup for 0:0 + the
// key re-check) against the previous unscoped walk, on a manifest with 50
// tenants × 168 hourly partitions × 4 objects per tenant-partition (33,600
// objects), for a 7-day query by one tenant.
func BenchmarkTenantScope_FileSelection(b *testing.B) {
	const tenants, partitions, perTP = 50, 168, 4
	s := testStorage()
	s.manifest = manifest.New("test-bucket", "")
	s.manifest.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	for p := 0; p < partitions; p++ {
		t := base.Add(time.Duration(p) * time.Hour)
		part := fmt.Sprintf("dt=%s/hour=%02d", t.Format("2006-01-02"), t.Hour())
		for tn := 0; tn < tenants; tn++ {
			for i := 0; i < perTP; i++ {
				s.manifest.AddFile(part, manifest.FileInfo{
					Key:       fmt.Sprintf("%d/0/logs/%s/f%d.parquet", tn, part, i),
					Size:      1 << 20,
					RowCount:  1000,
					MinTimeNs: t.UnixNano(),
					MaxTimeNs: t.Add(59 * time.Minute).UnixNano(),
				})
			}
		}
	}
	start, end := base.UnixNano(), base.Add(partitions*time.Hour).UnixNano()

	b.Run("unscoped_walk_previous", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if n := len(s.manifest.GetFilesForRange(start, end)); n != tenants*partitions*perTP {
				b.Fatalf("got %d", n)
			}
		}
	})
	b.Run("scoped_tenant_7", func(b *testing.B) {
		b.ReportAllocs()
		scope := tenantScope{account: "7", project: "0"}
		for i := 0; i < b.N; i++ {
			if n := len(s.filesForScope("bench", start, end, scope)); n != partitions*perTP {
				b.Fatalf("got %d", n)
			}
		}
	})
	b.Run("scoped_default_tenant_with_legacy_lookup", func(b *testing.B) {
		b.ReportAllocs()
		scope := tenantScope{account: "0", project: "0"}
		for i := 0; i < b.N; i++ {
			if n := len(s.filesForScope("bench", start, end, scope)); n != partitions*perTP {
				b.Fatalf("got %d", n)
			}
		}
	})
	b.Run("global_read", func(b *testing.B) {
		b.ReportAllocs()
		scope := tenantScope{all: true}
		for i := 0; i < b.N; i++ {
			if n := len(s.filesForScope("bench", start, end, scope)); n != tenants*partitions*perTP {
				b.Fatalf("got %d", n)
			}
		}
	})
}
