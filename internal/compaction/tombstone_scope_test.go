package compaction

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Deletes only hide rows: compaction merges every row forward and a tombstone
// stays in force until its retention passes. A tenant-scoped tombstone must hide
// the rows of its own tenants' objects only, and compaction must never mix
// tenants into one output, so the hiding follows the output key.

// tenantCompactionFixture holds one tenant-prefixed source pair per tenant in
// the same partition and window, each with an "error" row a tombstone matches.
type tenantCompactionFixture struct {
	pool     *mockPool
	manifest *manifest.Manifest
	files    []manifest.FileInfo
	byTenant map[string][]string
}

const tenantCompactionPartition = "dt=2026-07-01/hour=03"

func newTenantCompactionFixture(t *testing.T) *tenantCompactionFixture {
	t.Helper()
	f := &tenantCompactionFixture{pool: newMockPool(), manifest: manifest.New("test-bucket", ""), byTenant: map[string][]string{}}
	f.manifest.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	var keys []string
	for _, tenant := range []string{"1001/0", "2002/0"} {
		for i := 0; i < 2; i++ {
			batch := []schema.LogRow{
				{TimestampUnixNano: int64(1000 + 10*i), Body: tenant + "-keep", SeverityText: "info", ServiceName: "web"},
				{TimestampUnixNano: int64(1001 + 10*i), Body: tenant + "-drop", SeverityText: "error", ServiceName: "web"},
			}
			key := tenant + "/logs/" + tenantCompactionPartition + "/src-" + string(rune('a'+i)) + ".parquet"
			data, err := writeCompactedLogs(batch, 100, 1, false)
			if err != nil {
				t.Fatalf("write source parquet: %v", err)
			}
			f.pool.put(key, data)
			fi := manifest.FileInfo{
				Key: key, Size: int64(len(data)), RowCount: int64(len(batch)),
				MinTimeNs: batch[0].TimestampUnixNano, MaxTimeNs: batch[1].TimestampUnixNano,
				RawBytes: schema.EstimateRawBytesLogs(batch), Labels: schema.ExtractLogLabels(batch),
			}
			f.manifest.AddFile(tenantCompactionPartition, fi)
			f.files = append(f.files, fi)
			f.byTenant[tenant] = append(f.byTenant[tenant], key)
			keys = append(keys, key)
		}
	}
	markListed(t, f.manifest, keys)
	return f
}

func TestCompaction_TenantScopedTombstoneHidesOnlyItsTenantsRows(t *testing.T) {
	f := newTenantCompactionFixture(t)
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID: "ts-2002", Query: `severity_text:="error"`, StartNs: 0, EndNs: 1 << 40,
		CreatedAt: time.Now().Add(-time.Hour), Mode: "permanent",
		Tenants: []delete.TenantRef{{AccountID: 2002}},
	})
	c := NewCompactor(CompactorConfig{Pool: f.pool, Manifest: f.manifest, Mode: config.ModeLogs, RowGroupSize: 100})

	res, err := c.Compact(context.Background(), tenantCompactionPartition, f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if len(res.OutputFiles) != 2 {
		t.Fatalf("want one output per tenant, got %v", res.OutputFiles)
	}
	for _, out := range res.OutputFiles {
		rows, err := readLogRows(f.pool.get(out))
		if err != nil {
			t.Fatalf("read %s: %v", out, err)
		}
		var nErr int
		for _, r := range rows {
			if r.SeverityText == "error" {
				nErr++
			}
		}
		// Nothing is physically removed: both tenants' outputs keep every row.
		if len(rows) != 4 || nErr != 2 {
			t.Errorf("%s: %d rows, %d error rows; want all 4 and 2 (hide-only: compaction copies rows forward)", out, len(rows), nErr)
		}
		// The read path hides the error rows of tenant 2002's output only.
		covered := len(delete.ForKey(store.Active(), nil, out)) == 1
		switch {
		case strings.HasPrefix(out, "2002/") && !covered:
			t.Errorf("tenant 2002's output %s is not covered by its tenant's tombstone", out)
		case strings.HasPrefix(out, "1001/") && covered:
			t.Errorf("tenant 1001's output %s is covered by tenant 2002's delete", out)
		case !strings.HasPrefix(out, "2002/") && !strings.HasPrefix(out, "1001/"):
			t.Errorf("unexpected output key %s", out)
		}
	}
	// Compaction does not retire a tombstone: it lives until retention.
	if _, still := store.Get("ts-2002"); !still {
		t.Error("compaction must not retire a tombstone")
	}
}

func TestKeyScope_AppliesOnlyWhenEveryInputBelongsToTheTombstonesTenants(t *testing.T) {
	scoped := delete.Tombstone{ID: "s", Tenants: []delete.TenantRef{{AccountID: 1001}}}
	unscoped := delete.Tombstone{ID: "n"} // names no tenant; must never apply
	cases := []struct {
		name string
		keys []string
		want bool // applies to every key
	}{
		{"own tenant's inputs", []string{"1001/0/logs/a", "1001/0/logs/b"}, true},
		{"another tenant's inputs", []string{"2002/0/logs/a"}, false},
		// Compaction never mixes tenants into one output; were a group to, the
		// tombstone is withheld from the foreign key.
		{"a group mixing tenants", []string{"1001/0/logs/a", "2002/0/logs/b"}, false},
	}
	for _, tc := range cases {
		every := true
		for _, key := range tc.keys {
			got := delete.ForKey([]delete.Tombstone{scoped, unscoped}, nil, key)
			for _, ts := range got {
				if ts.ID == "n" {
					t.Errorf("%s: a tombstone naming no tenant applies to %s", tc.name, key)
				}
			}
			if len(got) == 0 {
				every = false
			}
		}
		if every != tc.want {
			t.Errorf("%s: applies to every input = %v, want %v", tc.name, every, tc.want)
		}
	}
}

func TestCompaction_ScopedTombstoneCoversOnlyItsTenantsOutput(t *testing.T) {
	f := newTenantCompactionFixture(t)
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID: "t", Query: "*", StartNs: 0, EndNs: 1 << 40,
		CreatedAt: time.Now().Add(-time.Hour), Mode: "permanent",
		Tenants: []delete.TenantRef{{AccountID: 1001}},
	})
	c := NewCompactor(CompactorConfig{Pool: f.pool, Manifest: f.manifest, Mode: config.ModeLogs, RowGroupSize: 100})
	res, err := c.Compact(context.Background(), tenantCompactionPartition, f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for _, out := range res.OutputFiles {
		covered := len(delete.ForKey(store.Active(), nil, out)) == 1
		if strings.HasPrefix(out, "2002/") && covered {
			t.Errorf("tenant 1001's tombstone covers tenant 2002's output %s", out)
		}
		if strings.HasPrefix(out, "1001/") && !covered {
			t.Errorf("tenant 1001's tombstone does not cover its own output %s (the hiding must follow the rows)", out)
		}
	}
}
