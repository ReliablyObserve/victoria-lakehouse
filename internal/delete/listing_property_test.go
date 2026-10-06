package delete

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Property (#418, #404 round 4): over random sequences of complete listings,
// partial listings, sparse listings that miss live objects, complete-looking
// listings that silently drop one live object, a peer publishing an object just
// before a second tombstone is created (so no listing that began after the
// tombstone has seen it yet), and scheduler passes, no tombstone set ever
// completes while an object that still holds deleted rows is in the bucket (the
// next full listing would show them again), and no kept row is lost.
func TestProperty_SchedulerNeverCompletesOverUnlistedObjects(t *testing.T) {
	seeds := 40
	if testing.Short() {
		seeds = 10
	}
	for seed := 0; seed < seeds; seed++ {
		runSchedulerListingProperty(t, int64(seed))
	}
}

func bodiesIn(t *testing.T, pool *mockRewriterPool) (drop, keep int) {
	t.Helper()
	for _, k := range pool.Keys() {
		data, _ := pool.Get(k)
		r := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
		rows := make([]schema.LogRow, r.NumRows())
		n, _ := r.Read(rows)
		_ = r.Close()
		for _, row := range rows[:n] {
			if strings.HasPrefix(row.Body, "drop-") {
				drop++
			} else {
				keep++
			}
		}
	}
	return drop, keep
}

// inEveryWorkList reports whether every active tombstone lists key.
func inEveryWorkList(store *TombstoneStore, key string) bool {
	for _, ts := range store.Active() {
		found := false
		for _, k := range ts.AffectedKeys {
			if k == key {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func runSchedulerListingProperty(t *testing.T, seed int64) {
	rnd := rand.New(rand.NewSource(seed))
	pool := newMockRewriterPool()
	n := 8 + rnd.Intn(8)
	keys := make([]string, n)
	rowsByKey := map[string]int64{}
	for i := range keys {
		keys[i] = fmt.Sprintf("logs/dt=2026-03-01/hour=07/b%d-%04d.parquet", i%2, i)
		pool.Put(keys[i], buildTestParquet(t, []schema.LogRow{
			{TimestampUnixNano: int64(1000 + i), Body: fmt.Sprintf("keep-%d", i), SeverityText: "info", ServiceName: "web"},
			{TimestampUnixNano: int64(2000 + i), Body: fmt.Sprintf("drop-%d", i), SeverityText: "error", ServiceName: "web"},
		}))
		rowsByKey[keys[i]] = 2
	}
	prev := newTestManifest(t, rowsByKey)
	snap := filepath.Join(t.TempDir(), "manifest.snapshot")
	if err := prev.SaveTo(snap); err != nil {
		t.Fatal(err)
	}
	m := manifest.New("test-bucket", "")
	if err := m.LoadFrom(snap); err != nil {
		t.Fatal(err)
	}
	m.SetObjectProber(func(_ context.Context, _, k string) (bool, error) { return pool.Has(k), nil })

	store := NewTombstoneStore()
	store.Add(Tombstone{
		Tenants: []TenantRef{{}}, ID: "ts-prop", Query: `severity_text:="error"`,
		StartNs: 0, EndNs: 10000, AffectedKeys: append([]string(nil), keys...),
		CreatedAt: time.Now().Add(-2 * time.Hour), Mode: "permanent", Reaped: map[string]bool{},
	})
	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store: store, Rewriter: NewRewriter(pool, "logs/", 1000, "logs"),
		Detector: NewStorageClassDetector(nil), RewriteDelay: time.Hour,
		AllowedClasses: []string{"STANDARD"}, Manifest: m,
	})
	// Tombstones are eligible as soon as they exist, so one created mid-run
	// (ts-prop-2) is worked on while the listing that predates it is current.
	sched.rewriteDelay = 0
	listing := func(skipPrefix string) []manifest.ListedObject {
		var out []manifest.ListedObject
		for _, k := range pool.Keys() {
			if skipPrefix != "" && strings.Contains(k, "/"+skipPrefix) {
				continue
			}
			data, _ := pool.Get(k)
			out = append(out, manifest.ListedObject{Key: k, Size: int64(len(data))})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
		return out
	}

	peerPublished := false
	for step := 0; step < 12; step++ {
		label := fmt.Sprintf("seed=%d step=%d", seed, step)
		switch rnd.Intn(6) {
		case 4: // a complete-looking listing silently drops one tracked object
			// Only an object every active tombstone already has in its work
			// list: one dropped before any pass discovered it is the residual
			// TestReview404R4_Residual_SilentDropBeforeDiscovery documents.
			all := listing("")
			var tracked []int
			for i, o := range all {
				if m.HasKey(o.Key) && inEveryWorkList(store, o.Key) {
					tracked = append(tracked, i)
				}
			}
			if len(tracked) > 0 {
				i := tracked[rnd.Intn(len(tracked))]
				m.ApplyListing(append(all[:i:i], all[i+1:]...), time.Now())
			}
		case 5: // a peer publishes, then a new tombstone is created over what the manifest holds
			if peerPublished {
				continue
			}
			peerPublished = true
			pk := "logs/dt=2026-03-01/hour=07/peer-0001.parquet"
			pool.Put(pk, buildTestParquet(t, []schema.LogRow{
				{TimestampUnixNano: 5000, Body: "keep-peer", SeverityText: "info", ServiceName: "web"},
				{TimestampUnixNano: 5001, Body: "drop-peer", SeverityText: "error", ServiceName: "web"},
			}))
			var affected []string
			for _, fi := range m.GetFilesForRange(0, 10000) {
				affected = append(affected, fi.Key)
			}
			store.Add(Tombstone{
				Tenants: []TenantRef{{}}, ID: "ts-prop-2", Query: `severity_text:="error"`,
				StartNs: 0, EndNs: 1 << 62, AffectedKeys: affected,
				CreatedAt: time.Now(), Mode: "permanent", Reaped: map[string]bool{},
			})
		case 0:
			m.ApplyListing(listing(""), time.Now())
		case 1:
			m.ApplyPartialListing(listing("b1-"), time.Now(), []string{"logs/dt=2026-03-01/hour=07/b1-"})
		case 2: // sparse listing missing live objects
			if m.TotalFiles() >= 4 {
				all := listing("")
				rnd.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
				if k := m.TotalFiles()/2 - 1; len(all) > k {
					all = all[:k]
				}
				m.ApplyListing(all, time.Now())
			}
		default:
			sched.RunOnce(context.Background())
		}
		if store.Count() == 0 {
			if drop, _ := bodiesIn(t, pool); drop > 0 {
				t.Fatalf("%s: the tombstone completed while %d deleted rows are still in objects of the bucket (listed=%v, complete=%+v)", label, drop, m.Listed(), m.LastCompleteRefresh())
			}
		}
	}
	// A healthy tail converges: a complete listing, then passes until done.
	for i := 0; i < 6; i++ {
		m.ApplyListing(listing(""), time.Now())
		sched.RunOnce(context.Background())
	}
	wantKeep := n
	if peerPublished {
		wantKeep++
	}
	if drop, keep := bodiesIn(t, pool); drop > 0 || keep != wantKeep {
		t.Fatalf("seed=%d: after convergence deleted rows=%d (want 0), kept rows=%d (want %d); tombstones active=%d", seed, drop, keep, wantKeep, store.Count())
	}
}
