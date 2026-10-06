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

// Property (#418): over random sequences of complete listings, partial
// listings, sparse listings that miss live objects and scheduler passes, a
// tombstone never completes while an object that still holds its rows is in the
// bucket (the next full listing would show the deleted rows again), and no kept
// row is lost.
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

	for step := 0; step < 12; step++ {
		label := fmt.Sprintf("seed=%d step=%d", seed, step)
		switch rnd.Intn(4) {
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
	if drop, keep := bodiesIn(t, pool); drop > 0 || keep != n {
		t.Fatalf("seed=%d: after convergence deleted rows=%d (want 0), kept rows=%d (want %d); tombstones active=%d", seed, drop, keep, n, store.Count())
	}
}
