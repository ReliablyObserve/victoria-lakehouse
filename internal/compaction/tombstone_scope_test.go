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

// Compaction is the second reaper: it drops rows of eligible tombstones while
// merging. A tenant-scoped tombstone must only ever drop rows of its own
// tenants' objects, and must follow its rows only onto its own tenant's output.

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
			data, err := writeCompactedLogs(batch, 100, 1)
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

func TestCompaction_TenantScopedTombstoneDropsOnlyItsTenantsRows(t *testing.T) {
	f := newTenantCompactionFixture(t)
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID: "ts-2002", Query: `severity_text:="error"`, StartNs: 0, EndNs: 1 << 40,
		AffectedKeys: f.byTenant["2002/0"], CreatedAt: time.Now().Add(-time.Hour),
		Mode: "permanent", Reaped: map[string]bool{},
		Tenants: []delete.TenantRef{{AccountID: 2002}},
	})
	c := NewCompactor(CompactorConfig{Pool: f.pool, Manifest: f.manifest, Mode: config.ModeLogs, RowGroupSize: 100, Tombstones: store})

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
		switch {
		case len(out) > 5 && out[:5] == "2002/":
			if nErr != 0 || len(rows) != 2 {
				t.Errorf("tenant 2002's output %s: %d rows, %d deleted rows carried; want 2 kept, 0 deleted", out, len(rows), nErr)
			}
		case len(out) > 5 && out[:5] == "1001/":
			if nErr != 2 || len(rows) != 4 {
				t.Errorf("tenant 1001's output %s lost rows to tenant 2002's delete: %d rows, %d error rows; want 4 and 2", out, len(rows), nErr)
			}
		default:
			t.Errorf("unexpected output key %s", out)
		}
	}
	// Tenant 1001's group merges first (groups run in key order) while the
	// tombstone is still active — the order in which a leak would show. With
	// its own tenant's output clean, the tombstone then has nothing left.
	if ts, still := store.Get("ts-2002"); still {
		t.Errorf("the tombstone must retire once its tenant's output is clean; still active with keys %v", ts.AffectedKeys)
	}
}

func TestKeyScope_AppliesOnlyWhenEveryInputBelongsToTheTombstonesTenants(t *testing.T) {
	scoped := delete.Tombstone{ID: "s", Tenants: []delete.TenantRef{{AccountID: 1001}}}
	unscoped := delete.Tombstone{ID: "n"} // invalid; must never apply
	cases := []struct {
		name string
		ks   keyScope
		want []string
	}{
		{"own tenant's inputs", keyScope{keys: []string{"1001/0/logs/a", "1001/0/logs/b"}}, []string{"s"}},
		{"another tenant's inputs", keyScope{keys: []string{"2002/0/logs/a"}}, nil},
		{"a group mixing tenants withholds the tombstone", keyScope{keys: []string{"1001/0/logs/a", "2002/0/logs/b"}}, nil},
		{"no inputs named", keyScope{}, nil},
	}
	for _, tc := range cases {
		got := tc.ks.tombstones([]delete.Tombstone{scoped, unscoped})
		var ids []string
		for _, ts := range got {
			ids = append(ids, ts.ID)
		}
		if strings.Join(ids, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s: applied %v, want %v", tc.name, ids, tc.want)
		}
	}
}

func TestReconcileTombstones_ScopedTombstoneNeverFollowsOntoAnotherTenantsOutput(t *testing.T) {
	src := "1001/0/logs/dt=2026-07-09/hour=11/src.parquet"
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID: "t", Query: "*", StartNs: 0, EndNs: 1 << 40, AffectedKeys: []string{src},
		CreatedAt: time.Now().Add(-time.Hour), Mode: "permanent", Reaped: map[string]bool{},
		Tenants: []delete.TenantRef{{AccountID: 1001}},
	})
	foreignOut := "2002/0/logs/dt=2026-07-09/hour=11/out.parquet"
	reconcileTombstones(store, []string{src}, foreignOut, nil, nil, false, nil)
	ts, ok := store.Get("t")
	if !ok {
		t.Fatal("tombstone vanished")
	}
	if containsKey(ts.AffectedKeys, foreignOut) {
		t.Errorf("tenant 1001's tombstone was transferred onto tenant 2002's output: %v", ts.AffectedKeys)
	}
	if !ts.Reaped[src] {
		t.Error("the merged-away source must still be marked reaped")
	}
}
