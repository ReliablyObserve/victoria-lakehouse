package compaction

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil/storageinvariants"
)

// TestDeleteLifecycleProperties drives random sequences of every operation that
// touches a deleted row — deletes in all three modes and on both sides of the
// un-delete window, un-deletes, the window passing, rewrite passes, compactions,
// and restarts that rebuild the tombstone store from its persisted copy — and
// checks, after every step, the properties that must hold no matter the order:
//
//	P1  every manifested key has its object, every other object is one the
//	    manifest is waiting to delete, and Σ RowCount == a scan of what the
//	    manifest serves
//	P2  no row is ever served twice
//	P3  a row no delete ever matched is always served
//	P4  un-delete contract: a row matched only by hide-mode tombstones, or by
//	    permanent/auto tombstones still inside their window, is still served
//	P5  a retired tombstone's rows are not served — retirement never precedes
//	    removal
//
// Deletes of superseded objects fail at random, the periodic manifest refresh
// runs at random (including in the middle of a restart, before anything else),
// and restarts rebuild the manifest from its snapshot as well as the tombstone
// store from its persisted copy. After the sequence the faults stop and the
// system must converge to the strict set: nothing left awaiting deletion.
//
// The hand-written tests pick one interleaving each; the bugs this suite exists
// for lived in the ones nobody picked.
func TestDeleteLifecycleProperties(t *testing.T) {
	seeds := 40
	if testing.Short() {
		seeds = 8
	}
	for seed := int64(0); seed < int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runDeleteLifecycle(t, rand.New(rand.NewSource(seed)))
		})
	}
}

const propertyDelay = time.Hour

var propertyHour = time.Date(2026, 7, 9, 11, 0, 0, 0, time.UTC)

type lifecycleWorld struct {
	t        *testing.T
	rng      *rand.Rand
	dir      string
	pool     *gatedPool
	manifest *manifest.Manifest
	store    *delete.TombstoneStore

	services []string
	// rowService is the oracle's view of every row ever written: body -> service.
	rowService map[string]string
	// tombstones the oracle knows about, by id.
	tombstones map[string]*oracleTombstone
	nextID     int
}

type oracleTombstone struct {
	service   string
	mode      string
	undeleted bool
}

func runDeleteLifecycle(t *testing.T, rng *rand.Rand) {
	w := &lifecycleWorld{
		t:          t,
		rng:        rng,
		dir:        t.TempDir(),
		pool:       &gatedPool{mockPool: newMockPool()},
		manifest:   manifest.New("test-bucket", ""),
		services:   []string{"alpha", "beta", "gamma"},
		rowService: map[string]string{},
		tombstones: map[string]*oracleTombstone{},
	}
	w.restart()
	for i := 0; i < 3; i++ {
		w.flushFile(i)
	}
	w.check("initial")

	for step := 0; step < 18; step++ {
		var label string
		// Superseded-object deletes fail for this step one time in four.
		failing := w.rng.Intn(4) == 0
		if failing {
			w.pool.setFailDelete(func(k string) bool { return strings.HasSuffix(k, ".parquet") })
		}
		switch w.rng.Intn(12) {
		case 0:
			label = w.deleteRows("permanent", true)
		case 1:
			label = w.deleteRows("permanent", false)
		case 2:
			label = w.deleteRows("hide", false)
		case 3:
			label = w.undelete()
		case 4:
			label = w.windowPasses()
		case 5, 6:
			w.scheduler().RunOnce(context.Background())
			label = "rewrite pass"
		case 7:
			label = w.compact()
		case 8:
			w.restart()
			label = "restart"
		case 9, 10:
			w.refresh()
			label = "manifest refresh"
		case 11:
			w.manifest.ReclaimRetired(context.Background(), w.pool.Delete, 0)
			label = "reclaim retired objects"
		}
		w.pool.setFailDelete(nil)
		w.check(fmt.Sprintf("step %d (%s, deletes failing=%v)", step, label, failing))
	}

	// The faults stop: every leftover must be settled.
	for i := 0; i < 6; i++ {
		w.scheduler().RunOnce(context.Background())
		w.manifest.ReclaimRetired(context.Background(), w.pool.Delete, 0)
		w.refresh()
		w.check(fmt.Sprintf("settling pass %d", i))
	}
	w.checkSettled("settled")
}

func (w *lifecycleWorld) restart() {
	// A fresh process: the store comes back only from what was persisted, and
	// the manifest from its snapshot, with interrupted rewrites resolved and a
	// refresh run before anything else.
	snapshot := filepath.Join(w.dir, "manifest.bin")
	if err := w.manifest.SaveTo(snapshot); err != nil {
		w.t.Fatalf("snapshot: %v", err)
	}
	w.store = delete.NewTombstoneStore()
	cfg := delete.PersistenceConfig{Dir: w.dir}
	if _, err := w.store.Restore(context.Background(), cfg); err != nil {
		w.t.Fatalf("restore: %v", err)
	}
	w.store.EnablePersistence(cfg)
	m := manifest.New("test-bucket", "")
	if err := m.LoadFrom(snapshot); err != nil {
		w.t.Fatalf("load snapshot: %v", err)
	}
	w.manifest = m
	delete.ResolveInterruptedRewrites(w.store, m)
	w.refresh()
}

// refresh runs the periodic manifest refresh against the bucket.
func (w *lifecycleWorld) refresh() {
	listStart := time.Now()
	var objects []manifest.ListedObject
	for _, k := range w.pool.Keys() {
		objects = append(objects, manifest.ListedObject{Key: k, Size: int64(len(w.pool.get(k)))})
	}
	w.manifest.ApplyListing(objects, listStart)
}

func (w *lifecycleWorld) flushFile(i int) {
	var rows []schema.LogRow
	for j := 0; j < 3+w.rng.Intn(3); j++ {
		svc := w.services[w.rng.Intn(len(w.services))]
		body := fmt.Sprintf("file%d-row%d", i, j)
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: propertyHour.Add(time.Duration(i*60+j) * time.Second).UnixNano(),
			Body:              body,
			ServiceName:       svc,
		})
		w.rowService[body] = svc
	}
	data, err := WriteLogs(rows, 100, 1)
	if err != nil {
		w.t.Fatalf("write: %v", err)
	}
	key := fmt.Sprintf("logs/dt=2026-07-09/hour=11/flush-%d.parquet", i)
	w.pool.put(key, data)
	minNs, maxNs := schema.LogRowTimeBounds(rows)
	w.manifest.AddFile("dt=2026-07-09/hour=11", manifest.FileInfo{
		Key: key, Size: int64(len(data)), RowCount: int64(len(rows)), MinTimeNs: minNs, MaxTimeNs: maxNs,
		Labels: schema.ExtractLogLabels(rows), RawBytes: schema.EstimateRawBytesLogs(rows),
	})
}

func (w *lifecycleWorld) deleteRows(mode string, pastWindow bool) string {
	svc := w.services[w.rng.Intn(len(w.services))]
	id := fmt.Sprintf("ts-%d", w.nextID)
	w.nextID++
	created := time.Now()
	if pastWindow {
		created = created.Add(-2 * propertyDelay)
	}
	var keys []string
	for _, files := range w.manifest.AllFiles() {
		for _, fi := range files {
			keys = append(keys, fi.Key)
		}
	}
	sort.Strings(keys)
	w.store.Add(delete.Tombstone{
		ID:           id,
		Query:        fmt.Sprintf(`service.name:=%q`, svc),
		StartNs:      propertyHour.UnixNano(),
		EndNs:        propertyHour.Add(time.Hour).UnixNano(),
		AffectedKeys: keys,
		CreatedAt:    created,
		Mode:         mode,
		Reaped:       map[string]bool{},
	})
	w.tombstones[id] = &oracleTombstone{service: svc, mode: mode}
	return fmt.Sprintf("delete %s %s past-window=%v", mode, svc, pastWindow)
}

func (w *lifecycleWorld) undelete() string {
	active := w.store.Active()
	if len(active) == 0 {
		return "undelete (none active)"
	}
	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	ts := active[w.rng.Intn(len(active))]
	if err := w.store.TryRemove(ts.ID); err != nil {
		return "undelete " + ts.ID + " refused: " + err.Error()
	}
	if o, ok := w.tombstones[ts.ID]; ok {
		o.undeleted = true
	}
	return "undelete " + ts.ID
}

func (w *lifecycleWorld) windowPasses() string {
	active := w.store.Active()
	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	for _, ts := range active {
		if ts.Mode != "hide" && !ts.EligibleForPhysicalRemoval(time.Now(), propertyDelay) {
			w.store.Update(ts.ID, func(cur *delete.Tombstone) bool {
				cur.CreatedAt = time.Now().Add(-2 * propertyDelay)
				return true
			})
			return "window passes for " + ts.ID
		}
	}
	return "window passes (nothing pending)"
}

func (w *lifecycleWorld) compact() string {
	files := w.manifest.FilesForPartition("dt=2026-07-09/hour=11")
	if len(files) < 2 {
		return "compact (too few files)"
	}
	c := NewCompactor(CompactorConfig{
		Pool: w.pool, Manifest: w.manifest, Prefix: "logs/", Mode: config.ModeLogs,
		RowGroupSize: 100, Tombstones: w.store, TombstoneRewriteDelay: propertyDelay,
	})
	if _, err := c.Compact(context.Background(), "dt=2026-07-09/hour=11", files, 0); err != nil {
		// Only a lost publish race may abandon a merge, and nothing races here.
		w.t.Fatalf("compact: %v", err)
	}
	return fmt.Sprintf("compact %d files", len(files))
}

func (w *lifecycleWorld) scheduler() *delete.RewriteScheduler {
	return delete.NewRewriteScheduler(delete.RewriteSchedulerConfig{
		Store:          w.store,
		Rewriter:       delete.NewRewriter(w.pool, "logs/", 100, "logs", delete.WithParquetWriters(delete.ParquetWriters{Logs: WriteLogs, Traces: WriteTraces, CompressionLevel: 1})),
		Detector:       delete.NewStorageClassDetector(nil),
		RewriteDelay:   propertyDelay,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       w.manifest,
	})
}

func (w *lifecycleWorld) check(stage string) {
	t := w.t
	t.Helper()

	storageinvariants.Assert(t, stage, storageinvariants.State{
		Manifest: w.manifest, Bucket: w.pool.mockPool, Tombstones: tombstoneViewsOf(w.store),
		AwaitingDeletion: storageinvariants.AwaitingDeletionIn(w.manifest),
	})

	// "stored" is what the manifest serves: objects awaiting deletion are not
	// readable by any query.
	stored := map[string]int{}
	var scanned int64
	for _, k := range storageinvariants.ManifestKeys(w.manifest) {
		rows, err := readLogRows(w.pool.get(k))
		if err != nil {
			t.Fatalf("%s: read %s: %v", stage, k, err)
		}
		for i := range rows {
			stored[rows[i].Body]++
		}
		scanned += int64(len(rows))
	}
	if got := storageinvariants.ManifestRows(w.manifest); got != scanned {
		t.Fatalf("%s: P1 manifest claims %d rows, scan finds %d", stage, got, scanned)
	}

	active := map[string]delete.Tombstone{}
	for _, ts := range w.store.Active() {
		active[ts.ID] = ts
	}
	now := time.Now()

	for body, svc := range w.rowService {
		if stored[body] > 1 {
			t.Fatalf("%s: P2 row %q is stored %d times", stage, body, stored[body])
		}

		matchedByAny := false
		mayBeRemoved := false // some tombstone was allowed to physically remove it
		for id, o := range w.tombstones {
			if o.service != svc {
				continue
			}
			matchedByAny = true
			ts, isActive := active[id]
			switch {
			case o.undeleted:
				// Removed while active. Whether it was already eligible when
				// removed is not tracked by the oracle; be permissive.
				mayBeRemoved = true
			case isActive:
				if ts.EligibleForPhysicalRemoval(now, propertyDelay) {
					mayBeRemoved = true
				}
			default:
				// Retired: it must have removed its rows first.
				if stored[body] > 0 {
					t.Fatalf("%s: P5 tombstone %s retired but its row %q is still stored", stage, id, body)
				}
				mayBeRemoved = true
			}
		}

		if !matchedByAny && stored[body] != 1 {
			t.Fatalf("%s: P3 row %q matched no delete but is stored %d times", stage, body, stored[body])
		}
		if matchedByAny && !mayBeRemoved && stored[body] != 1 {
			t.Fatalf("%s: P4 row %q is protected by the un-delete contract but is stored %d times", stage, body, stored[body])
		}
	}
}

// checkSettled is the strict end state: nothing awaits deletion, every object
// is manifested, and no rewrite is unfinished.
func (w *lifecycleWorld) checkSettled(stage string) {
	w.t.Helper()
	storageinvariants.Assert(w.t, stage, storageinvariants.State{
		Manifest: w.manifest, Bucket: w.pool.mockPool, Tombstones: tombstoneViewsOf(w.store),
	})
	if rk := w.manifest.RetiredKeys(); len(rk) != 0 {
		for _, r := range rk {
			if w.pool.get(r.Key) != nil {
				w.t.Fatalf("%s: retired object %s still in the bucket", stage, r.Key)
			}
		}
	}
	for _, ts := range w.store.Active() {
		if len(ts.Superseded) != 0 {
			w.t.Fatalf("%s: tombstone %s still has unfinished rewrites: %+v", stage, ts.ID, ts.Superseded)
		}
	}
}
