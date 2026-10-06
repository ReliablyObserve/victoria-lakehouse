package compaction

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func spinNow() time.Time {
	a := time.Now()
	for {
		if b := time.Now(); b.After(a) {
			return b
		}
	}
}

// Property (#418, #404 round 4): over random sequences of complete listings,
// partial listings, sparse listings that miss live objects, legitimate peer
// shrinks, complete-looking listings that silently drop a live object, shrinks
// whose HEAD sample can miss the live objects among the dropped ones, peer
// flushes, peer pushes that retire an object (its node owes the delete), stray
// unrecorded objects and Tier B passes, Tier B never deletes a live object, and
// deletes only objects the manifest holds a retirement record for.
func TestProperty_TierBNeverDeletesALiveObject(t *testing.T) {
	seeds := 80
	if testing.Short() {
		seeds = 20
	}
	reclaimed := 0
	for seed := 0; seed < seeds; seed++ {
		reclaimed += runTierBProperty(t, int64(seed))
	}
	// Liveness: retired objects do get reclaimed (the property is not vacuous).
	if reclaimed == 0 {
		t.Fatal("no Tier B pass deleted a retired object in any sequence")
	}
}

func runTierBProperty(t *testing.T, seed int64) (reclaimed int) {
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(seed))
	pool := newListingPool()
	const part = "dt=2026-01-01/hour=00"
	live := map[string]bool{}    // objects the data lives in
	orphans := map[string]bool{} // stray objects nothing references
	retired := map[string]bool{} // retired by a peer's push: may be deleted
	seq := 0
	newKey := func(acct int) string {
		seq++
		return fmt.Sprintf("logs/%s/a%d-o%04d.parquet", part, acct, seq)
	}
	add := func(k string, m map[string]bool) {
		_ = pool.UploadWithMtime(ctx, k, []byte("x"), spinNow())
		m[k] = true
	}
	prev := manifest.New("bkt", "logs/")
	for i := 0; i < 10+rnd.Intn(20); i++ {
		k := newKey(1 + rnd.Intn(2))
		add(k, live)
		prev.AddFile(part, manifest.FileInfo{Key: k, Size: 1, RowCount: 1})
	}
	snap := filepath.Join(t.TempDir(), "m.snap")
	if err := prev.SaveTo(snap); err != nil {
		t.Fatal(err)
	}
	m := manifest.New("bkt", "logs/")
	if err := m.LoadFrom(snap); err != nil {
		t.Fatal(err)
	}
	exists := func(k string) bool { pool.mu.Lock(); defer pool.mu.Unlock(); _, ok := pool.mtimes[k]; return ok }
	m.SetObjectProber(func(_ context.Context, _, k string) (bool, error) { return exists(k), nil })

	listing := func(skip string) []manifest.ListedObject {
		var out []manifest.ListedObject
		pool.mu.Lock()
		defer pool.mu.Unlock()
		for k := range pool.mtimes {
			if skip != "" && len(k) > len("logs/"+part+"/")+len(skip) && k[len("logs/"+part+"/"):len("logs/"+part+"/")+len(skip)] == skip {
				continue
			}
			out = append(out, manifest.ListedObject{Key: k, Size: 1})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
		return out
	}
	tracked := func() map[string]bool {
		out := map[string]bool{}
		for _, fs := range m.AllFiles() {
			for _, fi := range fs {
				out[fi.Key] = true
			}
		}
		return out
	}
	r := NewOwnershipResolver("self", staticPeers("self"))
	sweep := NewOrphanSweep(OrphanSweepConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: NewLevelPolicy(10, 20, 0),
		Lister: pool, Prefix: "logs/", Mode: config.ModeLogs,
		Interval: time.Minute, OrphanTTL: time.Nanosecond,
	})

	for step := 0; step < 30; step++ {
		label := fmt.Sprintf("seed=%d step=%d", seed, step)
		switch op := rnd.Intn(11); op {
		case 0:
			m.ApplyListing(listing(""), spinNow())
		case 1:
			m.ApplyPartialListing(listing("a2-"), spinNow(), []string{"logs/" + part + "/a2-"})
		case 2: // sparse listing; only when every tracked key is live so the sample is certain
			tr := tracked()
			if len(tr) < 4 {
				continue
			}
			stale := false
			for k := range tr {
				if !exists(k) {
					stale = true
				}
			}
			if stale {
				continue
			}
			all := listing("")
			rnd.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
			if n := len(tr)/2 - 1; len(all) > n {
				all = all[:n]
			}
			if m.ApplyListing(all, spinNow()) {
				t.Fatalf("%s: a sparse listing with live dropped keys was applied", label)
			}
		case 3: // peer compaction shrinks the bucket
			var ks []string
			for k := range live {
				if exists(k) {
					ks = append(ks, k)
				}
			}
			sort.Strings(ks)
			rnd.Shuffle(len(ks), func(i, j int) { ks[i], ks[j] = ks[j], ks[i] })
			for _, k := range ks[:len(ks)*7/10] {
				_ = pool.Delete(ctx, k)
				delete(live, k)
			}
			m.ApplyListing(listing(""), spinNow())
		case 4: // a peer flushes a live object
			add(newKey(1+rnd.Intn(2)), live)
		case 5: // a stray orphan appears
			add(newKey(1+rnd.Intn(2)), orphans)
		case 6: // a complete-looking listing silently drops one live tracked object
			all := listing("")
			tr := tracked()
			var drop []int
			for i, o := range all {
				if live[o.Key] && tr[o.Key] {
					drop = append(drop, i)
				}
			}
			if len(drop) == 0 {
				continue
			}
			i := drop[rnd.Intn(len(drop))]
			all = append(all[:i], all[i+1:]...)
			m.ApplyListing(all, spinNow()) // < half dropped: no HEAD sample, believed
		case 7: // a peer compacts: most objects go, and the listing also misses a live one
			var ks []string
			for k := range live {
				if exists(k) {
					ks = append(ks, k)
				}
			}
			if len(ks) < 4 {
				continue
			}
			sort.Strings(ks)
			rnd.Shuffle(len(ks), func(i, j int) { ks[i], ks[j] = ks[j], ks[i] })
			for _, k := range ks[:len(ks)*8/10] {
				_ = pool.Delete(ctx, k)
				delete(live, k)
			}
			missed := ks[len(ks)-1]
			var objs []manifest.ListedObject
			for _, o := range listing("") {
				if o.Key != missed {
					objs = append(objs, o)
				}
			}
			m.ApplyListing(objs, spinNow()) // the sample may or may not find `missed`
		case 8: // a peer's push retires a tracked live object (its node owes the delete)
			var ks []string
			for k := range tracked() {
				if live[k] && exists(k) {
					ks = append(ks, k)
				}
			}
			if len(ks) == 0 {
				continue
			}
			sort.Strings(ks)
			k := ks[rnd.Intn(len(ks))]
			m.RemoveFile(part, k)
			delete(live, k)
			retired[k] = true
		default: // Tier B pass
			before := map[string]bool{}
			pool.mu.Lock()
			for k := range pool.mtimes {
				before[k] = true
			}
			pool.mu.Unlock()
			if _, err := sweep.RunTierB(ctx); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			for k := range before {
				if exists(k) {
					continue
				}
				if live[k] {
					t.Fatalf("%s: Tier B deleted the live object %s (manifest listed=%v, complete refresh=%+v)", label, k, m.Listed(), m.LastCompleteRefresh())
				}
				if !retired[k] {
					t.Fatalf("%s: Tier B deleted %s, which no retirement record named", label, k)
				}
				delete(retired, k)
				reclaimed++
			}
		}
	}
	return reclaimed
}
