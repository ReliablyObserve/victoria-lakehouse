package compaction

import (
	"reflect"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestTenantPrefixFromKey(t *testing.T) {
	cases := []struct {
		key    string
		expect string
	}{
		{"1002/0/logs/dt=2026-06-04/hour=10/a.parquet", "1002/0/logs/"},
		{"888/2/traces/dt=2026-06-04/hour=10/b.parquet", "888/2/traces/"},
		// Legacy: no tenant prefix → empty so all legacy files share a group.
		{"obs-archive/logs/dt=2026-06-04/hour=10/c.parquet", ""},
		{"dt=2026-06-04/hour=10/d.parquet", ""},
		// Too few segments → empty.
		{"1002/0/foo.parquet", ""},
	}
	for _, tc := range cases {
		got := tenantPrefixFromKey(tc.key)
		if got != tc.expect {
			t.Errorf("%q: got %q, want %q", tc.key, got, tc.expect)
		}
	}
}

func TestGroupFilesByTenant_SplitsByPrefixAndBucket(t *testing.T) {
	files := []manifest.FileInfo{
		// Tenant 1002:0, default bucket.
		{Key: "1002/0/logs/dt=2026-06-04/hour=10/a1.parquet", Size: 100},
		{Key: "1002/0/logs/dt=2026-06-04/hour=10/a2.parquet", Size: 100},
		// Tenant 1002:0, but routed to a different bucket — must
		// stay in its own group so output inherits the right bucket.
		{Key: "1002/0/logs/dt=2026-06-04/hour=10/a3.parquet", Bucket: "isolated-bucket", Size: 50},
		// Tenant 888:2.
		{Key: "888/2/logs/dt=2026-06-04/hour=10/b.parquet", Size: 200},
		// Legacy (no tenant prefix).
		{Key: "obs-archive/logs/dt=2026-06-04/hour=10/c.parquet", Size: 300},
	}
	groups := groupFilesByTenant(files)
	if len(groups) != 4 {
		t.Fatalf("got %d groups, want 4", len(groups))
	}
	// Deterministic order: empty prefix first ("" < "1002/0/…"), then by bucket.
	want := []struct {
		prefix string
		bucket string
		nFiles int
	}{
		{"", "", 1},                            // legacy
		{"1002/0/logs/", "", 2},                 // tenant 1002, default bucket
		{"1002/0/logs/", "isolated-bucket", 1}, // tenant 1002, isolated bucket
		{"888/2/logs/", "", 1},                  // tenant 888
	}
	for i, w := range want {
		g := groups[i]
		if g.TenantPrefix != w.prefix || g.Bucket != w.bucket || len(g.Files) != w.nFiles {
			t.Errorf("group[%d] = {prefix:%q bucket:%q files:%d}, want {%q %q %d}",
				i, g.TenantPrefix, g.Bucket, len(g.Files), w.prefix, w.bucket, w.nFiles)
		}
	}
}

func TestGroupFilesByTenant_DeterministicOrder(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "999/9/logs/dt=x/foo.parquet"},
		{Key: "1/1/logs/dt=x/foo.parquet"},
		{Key: "500/0/logs/dt=x/foo.parquet"},
	}
	g1 := groupFilesByTenant(files)
	g2 := groupFilesByTenant(files)
	prefixes1 := make([]string, len(g1))
	prefixes2 := make([]string, len(g2))
	for i := range g1 {
		prefixes1[i] = g1[i].TenantPrefix
		prefixes2[i] = g2[i].TenantPrefix
	}
	if !reflect.DeepEqual(prefixes1, prefixes2) {
		t.Errorf("group order not deterministic: %v vs %v", prefixes1, prefixes2)
	}
	// Sanity: lexicographic on prefix string.
	wantOrder := []string{"1/1/logs/", "500/0/logs/", "999/9/logs/"}
	if !reflect.DeepEqual(prefixes1, wantOrder) {
		t.Errorf("group order = %v, want %v", prefixes1, wantOrder)
	}
}
