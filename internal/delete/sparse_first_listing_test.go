package delete

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// REVIEW #404: the first listing after a snapshot load is exempt from the cliff
// guard. A sparse-but-successful first LIST (truncated pagination, throttled
// listing) is then applied AND marks the manifest Listed(), which is the only
// gate the delete scheduler uses before reading "absent from the manifest" as
// "the object is gone". The tombstone retires over objects that still hold its
// rows; the next full listing re-adopts them and the deleted rows are visible.
func TestReview404Regression_SparseFirstListingAfterRestartResurrectsDeletedRows(t *testing.T) {
	pool := newMockRewriterPool()
	const n = 10
	keys := make([]string, n)
	rowsByKey := map[string]int64{}
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("logs/dt=2026-03-01/hour=07/src-%04d.parquet", i)
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(1000 + i), Body: fmt.Sprintf("keep-%d", i), SeverityText: "info", ServiceName: "web"},
			{TimestampUnixNano: int64(2000 + i), Body: fmt.Sprintf("drop-%d", i), SeverityText: "error", ServiceName: "web"},
		}
		pool.Put(keys[i], buildTestParquet(t, rows))
		rowsByKey[keys[i]] = 2
	}

	// The previous process: listed, persisted its snapshot, then restarted.
	prev := newTestManifest(t, rowsByKey)
	snap := filepath.Join(t.TempDir(), "manifest.snapshot")
	if err := prev.SaveTo(snap); err != nil {
		t.Fatal(err)
	}
	m := manifest.New("test-bucket", "")
	if err := m.LoadFrom(snap); err != nil {
		t.Fatal(err)
	}

	store := NewTombstoneStore()
	store.Add(Tombstone{
		Tenants:      []TenantRef{{}},
		ID:           "ts-review",
		Query:        `severity_text:="error"`,
		StartNs:      0,
		EndNs:        10000,
		AffectedKeys: append([]string(nil), keys...),
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "permanent",
		Reaped:       map[string]bool{},
	})
	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       NewRewriter(pool, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       m,
	})

	// First refresh after the restart: a truncated LIST returns 1 of 10 objects.
	sparse := []manifest.ListedObject{{Key: keys[0], Size: 1024}}
	accepted := m.ApplyListing(sparse, time.Now())
	t.Logf("sparse first listing accepted=%v listed=%v files=%d", accepted, m.Listed(), m.TotalFiles())

	sched.RunOnce(context.Background())
	sched.RunOnce(context.Background())

	// The next refresh lists the bucket completely.
	full := make([]manifest.ListedObject, 0)
	for _, k := range pool.Keys() {
		data, _ := pool.Get(k)
		full = append(full, manifest.ListedObject{Key: k, Size: int64(len(data))})
	}
	if !m.ApplyListing(full, time.Now()) {
		t.Fatal("full listing rejected")
	}

	got := visibleBodies(t, m, pool, store)
	var resurrected []string
	for i := 0; i < n; i++ {
		if b := fmt.Sprintf("drop-%d", i); got[b] > 0 {
			resurrected = append(resurrected, b)
		}
	}
	if len(resurrected) > 0 {
		t.Fatalf("deleted rows visible after a sparse first listing (tombstones active=%d): %v", store.Count(), resurrected)
	}
}
