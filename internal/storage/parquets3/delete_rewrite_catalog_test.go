package parquets3

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestDeleteRewrite_CatalogForgetsTheDeletedValue drives the whole permanent
// delete end to end against a real storage: a writer flush with the pmeta
// catalog on, a tombstone, a real rewrite scheduler run until the tombstone is
// retired, then field_values.
//
// Once the tombstone is gone nothing hides the deleted value any more — the
// only thing standing between the user and a resurrected dropdown entry is the
// catalog no longer listing it. The catalog is a union that only grows, so
// dropping the superseded file's per-file facets (what compaction's hook does)
// is not enough; the rewrite hook in both binaries, PmetaOnRewritten, rebuilds
// the value sets from the files that survive. The negative controls prove this
// test notices when that rebuild is missing.
func TestDeleteRewrite_CatalogForgetsTheDeletedValue(t *testing.T) {
	for _, tc := range []struct {
		name      string
		hook      string
		wantValue []string
	}{
		{name: "with the rewrite hook", hook: "rewritten", wantValue: []string{"api-gateway"}},
		// Negative control: compaction's hook drops per-file facets but the
		// catalog union keeps the deleted value.
		{name: "negative control: compaction hook only", hook: "compacted", wantValue: []string{"api-gateway", "order-service"}},
		// Negative control: no hook at all.
		{name: "negative control: no hook", hook: "", wantValue: []string{"api-gateway", "order-service"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMockS3Server()
			defer mock.close()
			s := testStorageWithS3(t, mock.url())

			catalog := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
			s.catalog = catalog
			bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
			bw.catalogObserver = &catalogObserver{store: catalog}

			now := time.Now()
			bw.AddLogRows([]schema.LogRow{
				{TimestampUnixNano: now.UnixNano(), Body: "a", ServiceName: "api-gateway"},
				{TimestampUnixNano: now.UnixNano(), Body: "b", ServiceName: "order-service"},
				{TimestampUnixNano: now.UnixNano(), Body: "c", ServiceName: "api-gateway"},
			})
			bw.triggerFlush()

			startNs := now.Add(-time.Hour).UnixNano()
			endNs := now.Add(time.Hour).UnixNano()
			q := mustParseQueryWithTime(t, "*", startNs, endNs)

			before, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
			if err != nil {
				t.Fatalf("GetFieldValues before: %v", err)
			}
			if got := valueStrings(before); !reflect.DeepEqual(got, []string{"api-gateway", "order-service"}) {
				t.Fatalf("fixture is wrong: values before the delete = %v", got)
			}

			files := s.manifest.GetFilesForRange(startNs, endNs)
			if len(files) != 1 {
				t.Fatalf("fixture is wrong: expected one flushed file, got %d", len(files))
			}
			oldKey := files[0].Key
			part := manifest.ExtractTenantPartition(oldKey)
			// A tombstone retires only on a manifest that has listed the
			// bucket in this process — before that, a key missing from the
			// manifest says nothing about the object (see Manifest.Listed).
			// A running node reaches that state on its first refresh.
			if !s.manifest.ApplyListing([]manifest.ListedObject{{Key: oldKey, Size: files[0].Size}}, time.Now()) {
				t.Fatal("fixture: the listing was rejected")
			}

			store := delete.NewTombstoneStore()
			store.Add(delete.Tombstone{
				Tenants:      []delete.TenantRef{{}},
				ID:           "ts-e2e",
				Query:        `service.name:="order-service"`,
				StartNs:      startNs,
				EndNs:        endNs,
				AffectedKeys: []string{oldKey},
				CreatedAt:    now.Add(-2 * time.Hour),
				Mode:         "permanent",
				Reaped:       map[string]bool{},
			})
			s.SetTombstoneStore(store)

			cfg := delete.RewriteSchedulerConfig{
				Store:          store,
				Rewriter:       delete.NewRewriter(s.pool, "logs/", 100, "logs"),
				Detector:       delete.NewStorageClassDetector(nil),
				RewriteDelay:   time.Hour,
				AllowedClasses: []string{"STANDARD"},
				Manifest:       s.manifest,
			}
			switch tc.hook {
			case "rewritten":
				cfg.OnPublished = s.PmetaOnRewritten
			case "compacted":
				cfg.OnPublished = s.PmetaOnCompacted
			}
			results := delete.NewRewriteScheduler(cfg).RunOnce(context.Background())
			if len(results) != 1 || results[0].RowsKept != 2 {
				t.Fatalf("expected one rewrite keeping 2 rows, got %+v", results)
			}
			if store.Count() != 0 {
				t.Fatalf("the tombstone should be retired once its only file is rewritten, %d remain", store.Count())
			}
			if s.manifest.HasKey(oldKey) || !s.manifest.HasKey(results[0].NewKey) {
				t.Fatal("the manifest must have swapped the superseded key for the replacement")
			}

			after, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
			if err != nil {
				t.Fatalf("GetFieldValues after: %v", err)
			}
			if got := valueStrings(after); !reflect.DeepEqual(got, tc.wantValue) {
				t.Fatalf("values after the delete completed = %v, want %v", got, tc.wantValue)
			}

			if tc.hook == "rewritten" {
				if _, ok := catalog.FileMeta(part, oldKey); ok {
					t.Error("the catalog still has file-meta for the superseded key")
				}
				if _, ok := catalog.FileMeta(manifest.ExtractTenantPartition(results[0].NewKey), results[0].NewKey); !ok {
					t.Error("the catalog has no file-meta for the replacement")
				}
			}
		})
	}
}
