package delete

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Every claim that a crash or a failed step "leaves an unmanifested object the
// orphan sweep reclaims" is only true if the periodic manifest refresh — which
// rebuilds the manifest from the bucket listing every few minutes — does not
// adopt that object first. These tests run the refresh in the windows the
// rewrite leaves behind.

// simulateManifestRefresh runs the periodic refresh against the in-memory
// bucket: the manifest is rebuilt from a listing of every object, merged with
// what it tracks, exactly as Manifest.RefreshFromS3 does.
func simulateManifestRefresh(t *testing.T, m *lhmanifest.Manifest, pool interface {
	Keys() []string
	Get(string) ([]byte, bool)
}) {
	t.Helper()
	listStart := time.Now()
	objects := make([]lhmanifest.ListedObject, 0)
	for _, k := range pool.Keys() {
		data, _ := pool.Get(k)
		objects = append(objects, lhmanifest.ListedObject{Key: k, Size: int64(len(data))})
	}
	if !m.ApplyListing(objects, listStart) {
		t.Fatal("the refresh was rejected by the cliff guard")
	}
}

// visibleBodies is what a query sees: every row of every MANIFESTED object,
// minus the rows an active tombstone hides, counted per body. A manifested key
// with no object behind it is reported, not skipped.
func visibleBodies(t *testing.T, m *lhmanifest.Manifest, pool interface {
	Get(string) ([]byte, bool)
}, store *TombstoneStore) map[string]int {
	t.Helper()
	counts := map[string]int{}
	var keys []string
	for _, files := range m.AllFiles() {
		for _, fi := range files {
			keys = append(keys, fi.Key)
		}
	}
	sort.Strings(keys)
	active := store.Active()
	for _, k := range keys {
		data, ok := pool.Get(k)
		if !ok {
			t.Fatalf("manifested key %s has no object behind it", k)
		}
		r := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
		rows := make([]schema.LogRow, r.NumRows())
		n, _ := r.Read(rows)
		_ = r.Close()
		for i := range rows[:n] {
			hidden := false
			for j := range active {
				if active[j].MatchesFields(LogRowFields(&rows[i]), rows[i].TimestampUnixNano) {
					hidden = true
					break
				}
			}
			if !hidden {
				counts[rows[i].Body]++
			}
		}
	}
	return counts
}

// assertQueriesCorrect is the user-facing invariant after every refresh: each
// kept row is visible exactly once and no deleted row is visible.
func (f *rewriteFixture) assertQueriesCorrect(t *testing.T, stage string) {
	t.Helper()
	got := visibleBodies(t, f.manifest, f.pool, f.store)
	for body := range f.keptBodies {
		switch got[body] {
		case 1:
		case 0:
			t.Fatalf("%s: kept row %q is not visible", stage, body)
		default:
			t.Fatalf("%s: kept row %q is visible %d times", stage, body, got[body])
		}
	}
	for body := range f.deletedBodies {
		if got[body] > 0 {
			t.Fatalf("%s: deleted row %q is visible", stage, body)
		}
	}
}

// TestRewriteRefresh_SupersededObjectWhoseDeleteFailedIsNotReadopted: the
// publish landed, the delete of the superseded object failed, and the next
// refresh lists both objects.
func TestRewriteRefresh_SupersededObjectWhoseDeleteFailedIsNotReadopted(t *testing.T) {
	f := newRewriteFixture(t)
	f.fault.failDeleteOn = f.key

	f.sched.RunOnce(context.Background())
	if !f.pool.Has(f.key) {
		t.Fatal("fixture: the superseded object must survive its failed delete")
	}

	simulateManifestRefresh(t, f.manifest, f.pool)
	if f.manifest.HasKey(f.key) {
		t.Fatalf("the refresh re-adopted %s, which the rewrite replaced", f.key)
	}
	f.assertQueriesCorrect(t, "refresh after the failed delete")

	// The delete is retried and everything converges, refresh included.
	f.sched.RunOnce(context.Background())
	simulateManifestRefresh(t, f.manifest, f.pool)
	f.assertQueriesCorrect(t, "after the retry")
	f.assertConverged(t, "after the retry")
	if f.store.Count() != 0 {
		t.Fatalf("the tombstone must retire once the superseded object is gone, %d remain", f.store.Count())
	}
}

// uploadHookPool runs a callback after every successful upload, so a test can
// act — run a refresh — in the window between a rewrite's upload and its
// publish.
type uploadHookPool struct {
	*faultPool
	mu          sync.Mutex
	afterUpload func(key string)
}

func (p *uploadHookPool) Upload(ctx context.Context, key string, data []byte) error {
	if err := p.faultPool.Upload(ctx, key, data); err != nil {
		return err
	}
	p.mu.Lock()
	hook := p.afterUpload
	p.afterUpload = nil
	p.mu.Unlock()
	if hook != nil {
		hook(key)
	}
	return nil
}

// TestRewriteRefresh_RefreshBetweenUploadAndPublish: the refresh runs while the
// replacement exists but is not yet published.
func TestRewriteRefresh_RefreshBetweenUploadAndPublish(t *testing.T) {
	f := newRewriteFixture(t)
	hp := &uploadHookPool{faultPool: f.fault}
	f.sched.rewriter = NewRewriter(hp, "logs/", 1000, "logs")

	var uploaded string
	hp.afterUpload = func(key string) {
		uploaded = key
		simulateManifestRefresh(t, f.manifest, f.pool)
		if f.manifest.HasKey(key) {
			t.Errorf("the refresh adopted the unpublished replacement %s next to its source", key)
		}
		f.assertQueriesCorrect(t, "refresh between upload and publish")
	}

	results := f.sched.RunOnce(context.Background())
	if uploaded == "" || len(results) != 1 {
		t.Fatalf("fixture: the rewrite must upload and complete, uploaded=%q results=%d", uploaded, len(results))
	}
	fi, ok := f.manifest.GetFileByKey(uploaded)
	if !ok || fi.RowCount != int64(len(f.keptBodies)) || len(fi.Labels) == 0 {
		t.Fatalf("the published entry must be the rewrite's own (rows %d, labels), got %+v ok=%v", len(f.keptBodies), fi, ok)
	}
	simulateManifestRefresh(t, f.manifest, f.pool)
	f.assertQueriesCorrect(t, "after the publish")
	f.assertConverged(t, "after the publish")
}
