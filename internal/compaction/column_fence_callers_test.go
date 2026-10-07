package compaction

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// What the callers of Compact do with the forward fence (schema.UnknownColumns):
// what they announce as removed, whether an all-fenced plan takes the tenant's
// merge slot, and that a fenced object is not downloaded again on every tick.

// makeFuture replaces the objects behind keys with objects that carry a column
// the running code does not model, as a newer version would write them.
func (w *planWorld) makeFuture(keys []string) {
	w.t.Helper()
	for i, k := range keys {
		var data []byte
		if w.mode == config.ModeTraces {
			data = writeParquetT(w.t, []futureTrace{{int64(9000 + i), fmt.Sprintf("ft-%d", i), fmt.Sprintf("fs-%d", i), "svc", "kept"}})
		} else {
			data = writeParquetT(w.t, []futureLog{{int64(9000 + i), fmt.Sprintf("future-%d", i), "svc", "kept"}})
		}
		w.pool.put(k, data)
	}
}

func sortedJoin(s []string) string {
	c := append([]string(nil), s...)
	sort.Strings(c)
	return strings.Join(c, ",")
}

// downloadCounter counts Download calls per key over a pool.
type downloadCounter struct {
	*mockPool
	mu sync.Mutex
	n  map[string]int
}

func (d *downloadCounter) Download(ctx context.Context, key string) ([]byte, error) {
	d.mu.Lock()
	if d.n == nil {
		d.n = map[string]int{}
	}
	d.n[key]++
	d.mu.Unlock()
	return d.mockPool.Download(ctx, key)
}

func (d *downloadCounter) count(key string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n[key]
}

// The scheduler announces as removed (pmeta purge + peer push) exactly the keys
// it merged, never a fenced key that stays live. Mutation: announcing every
// selected key again makes this fail.
func TestFence_Scheduler_AnnouncesOnlyMergedInputs(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		known := w.add("1001/0", p, 0, 10, 2, nil)
		fenced := w.add("1001/0", p, 0, 2, 2, nil)
		w.makeFuture(fenced)
		var removed []string
		calls := 0
		sched := w.shippedScheduler()
		sched.onCompacted = func(_ []manifest.FileInfo, r []string, _ map[string]map[string][]string) {
			calls++
			removed = append(removed, r...)
		}
		if _, err := sched.Scan(context.Background()); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("onCompacted calls=%d, want 1", calls)
		}
		if sortedJoin(removed) != sortedJoin(known) {
			t.Fatalf("removed %v\nwant exactly the merged inputs %v", removed, known)
		}
		live := map[string]bool{}
		for _, k := range w.keys(p) {
			live[k] = true
		}
		for _, k := range fenced {
			if !live[k] || w.pool.get(k) == nil {
				t.Fatalf("fenced object %s left the manifest or the bucket", k)
			}
		}
	})
}

// Same contract for a Tier A steal.
func TestFence_TierA_AnnouncesOnlyMergedInputs(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-72 * time.Hour))
		known := w.add("1003/0", p, 0, 10, 2, nil)
		fenced := w.add("1003/0", p, 0, 2, 2, nil)
		w.makeFuture(fenced)
		w.m.MarkAttempt(p, time.Now().Add(-10*time.Minute))
		var removed []string
		got, err := tierAWorld(w, nil, func(_ []manifest.FileInfo, r []string, _ map[string]map[string][]string) {
			removed = append(removed, r...)
		}).RunTierA(context.Background())
		if err != nil || got != 1 {
			t.Fatalf("stolen=%d err=%v, want 1", got, err)
		}
		if sortedJoin(removed) != sortedJoin(known) {
			t.Fatalf("removed %v\nwant exactly the merged inputs %v", removed, known)
		}
	})
}

// A plan whose inputs are all fenced must not take the tenant's merge slot on
// every scan: the tenant's other partitions still get compacted, and nothing is
// counted as a compaction run (#343 class).
func TestFence_AllFencedPlanDoesNotStarveTenant(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p1 := partitionAt(time.Now().Add(-5 * time.Hour))
		p2 := partitionAt(time.Now().Add(-3 * time.Hour))
		w.makeFuture(w.add("1001/0", p1, 0, 12, 2, nil))
		w.add("1001/0", p2, 0, 10, 2, nil)
		sched := w.shippedScheduler()
		runsBefore := metrics.CompactionRunsTotal.Get()
		for i := 0; i < 5; i++ {
			if _, err := sched.Scan(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if n := len(w.keys(p2)); n != 1 {
			t.Fatalf("after 5 scans partition %s still has %d objects: the all-fenced plan in %s took the tenant's slot every scan", p2, n, p1)
		}
		// Exactly one run: the merge of p2. The all-fenced attempt on p1 is not a run.
		if got := metrics.CompactionRunsTotal.Get() - runsBefore; got != 1 {
			t.Errorf("CompactionRunsTotal grew by %d, want 1 (only the real merge)", got)
		}
		if n := len(w.keys(p1)); n != 12 {
			t.Errorf("fenced partition %s has %d objects, want its 12 untouched", p1, n)
		}
	})
}

// A fenced object is downloaded once; later ticks neither plan it nor fetch it.
// Mutation: dropping the fenceLog.Has / withoutHeld filter makes this fail.
func TestFence_FencedObjectsAreNotDownloadedAgain(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		w.add("1001/0", p, 0, 10, 2, nil)
		fenced := w.add("1001/0", p, 0, 2, 2, nil)
		w.makeFuture(fenced)
		// A second tenant partition that is fully fenced.
		p2 := partitionAt(time.Now().Add(-5 * time.Hour))
		all := w.add("1001/0", p2, 0, 12, 2, nil)
		w.makeFuture(all)
		pool := &downloadCounter{mockPool: w.pool}
		sched := w.schedulerOn(pool)
		for i := 0; i < 6; i++ {
			if _, err := sched.Scan(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		for _, k := range append(append([]string(nil), fenced...), all...) {
			if n := pool.count(k); n > 1 {
				t.Errorf("fenced object %s downloaded %d times over 6 scans, want at most 1", k, n)
			}
		}
	})
}

// The counter counts OBJECTS, once each per process, however many ticks offer
// them again.
func TestFence_CounterCountsObjectsOnce(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		w.add("1001/0", p, 0, 10, 2, nil)
		w.makeFuture(w.add("1001/0", p, 0, 3, 2, nil))
		c := metrics.SkippedUnknownColumns(string(mode), "compact")
		before := c.Get()
		sched := w.shippedScheduler()
		for i := 0; i < 4; i++ {
			if _, err := sched.Scan(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if got := c.Get() - before; got != 3 {
			t.Errorf("counter grew by %d, want 3 (one per fenced object)", got)
		}
	})
}

// A forced recompaction that only meets fenced objects says so and reports
// nothing merged; one that merges the rest reports the fenced ones.
func TestFence_ForceCompactReportsFencedNoOp(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		w.makeFuture(w.add("1001/0", p, 0, 4, 2, nil))
		sched := w.shippedScheduler()
		res, err := sched.ForceCompactPartition(context.Background(), p, 0)
		if err == nil || res != nil {
			t.Fatalf("a forced merge of only fenced objects returned res=%v err=%v, want an error", res, err)
		}
		if !strings.Contains(err.Error(), "columns this version does not know") && !strings.Contains(err.Error(), "fewer than 2 compactable") {
			t.Errorf("unhelpful answer: %v", err)
		}

		freshFence(t) // the second world reuses the first one's object keys
		w2 := newPlanWorld(t, mode)
		w2.add("1001/0", p, 0, 4, 2, nil)
		fenced := w2.add("1001/0", p, 0, 2, 2, nil)
		w2.makeFuture(fenced)
		res, err = w2.shippedScheduler().ForceCompactPartition(context.Background(), p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.InputFiles) != 4 || len(res.FencedFiles) != 2 || len(res.OutputFiles) != 1 {
			t.Fatalf("forced merge reported inputs=%d fenced=%d outputs=%d, want 4, 2, 1", len(res.InputFiles), len(res.FencedFiles), len(res.OutputFiles))
		}
	})
}

// The same key in two buckets is two objects for the fence.
func TestFence_KeyedByBucketAndKey(t *testing.T) {
	a := manifest.FileInfo{Key: "k", Bucket: "b1"}
	b := manifest.FileInfo{Key: "k", Bucket: "b2"}
	if fenceKey(a) == fenceKey(b) || fenceKey(a) != fenceKey(manifest.FileInfo{Key: "k", Bucket: "b1"}) {
		t.Fatal("fenceKey must tell buckets apart")
	}
}

// A scan whose only plan is all fenced merges nothing: Scan reports 0 merges
// (the fenced plan is not a success), and counts no input files.
func TestFence_ScanReportsNoMergeForAnAllFencedPlan(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		w.makeFuture(w.add("1001/0", p, 0, 12, 2, nil))
		inputsBefore := metrics.CompactionFilesInputTotal.Get()
		compacted, err := w.shippedScheduler().Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if compacted != 0 {
			t.Errorf("Scan reported %d merges for a plan whose inputs are all fenced", compacted)
		}
		if got := metrics.CompactionFilesInputTotal.Get() - inputsBefore; got != 0 {
			t.Errorf("CompactionFilesInputTotal grew by %d for a merge that merged nothing", got)
		}
	})
}

// The compactor itself does not fetch an object it already refused (the planner
// filter is a second line; this is the first). Mutation: dropping the Has check
// in compactGroup fails this.
func TestFence_CompactorDoesNotRefetchARefusedObject(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		keys := w.add("1001/0", p, 0, 3, 2, nil)
		w.makeFuture(keys)
		pool := &downloadCounter{mockPool: w.pool}
		c := NewCompactor(CompactorConfig{Pool: pool, Manifest: w.m, Prefix: string(w.mode) + "/", Mode: w.mode, RowGroupSize: 1000, CompressionLevel: 1})
		files := w.m.FilesForPartition(p)
		for i := 0; i < 3; i++ {
			if _, err := c.Compact(context.Background(), p, files, 0); err != nil {
				t.Fatal(err)
			}
		}
		for _, k := range keys {
			if n := pool.count(k); n > 1 {
				t.Errorf("%s downloaded %d times over 3 Compact calls, want 1", k, n)
			}
		}
	})
}

// Repeated forced recompactions of a fenced partition fetch the fenced objects
// at most once (the first call discovers the fence; the later ones do not plan
// them). Mutation: dropping the fence filter in withoutHeld fails this.
func TestFence_RepeatedForceCompactDoesNotRefetch(t *testing.T) {
	freshFence(t)
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		keys := w.add("1001/0", p, 0, 4, 2, nil)
		w.makeFuture(keys)
		pool := &downloadCounter{mockPool: w.pool}
		sched := w.schedulerOn(pool)
		for i := 0; i < 4; i++ {
			if _, err := sched.ForceCompactPartition(context.Background(), p, 0); err == nil {
				t.Fatal("a forced merge of only fenced objects must report nothing merged")
			}
		}
		for _, k := range keys {
			if n := pool.count(k); n > 1 {
				t.Errorf("%s downloaded %d times over 4 forced recompactions, want at most 1", k, n)
			}
		}
	})
}

// withoutHeld drops the objects the fence refused.
func TestFence_WithoutHeldDropsRefusedObjects(t *testing.T) {
	freshFence(t)
	w := newPlanWorld(t, config.ModeLogs)
	p := partitionAt(time.Now().Add(-3 * time.Hour))
	w.add("1001/0", p, 0, 3, 2, nil)
	files := w.m.FilesForPartition(p)
	fenceLog.Mark(fenceKey(files[0]))
	got := withoutHeld(w.m, files)
	if len(got) != 2 {
		t.Fatalf("withoutHeld kept %d of 3 files, want 2", len(got))
	}
	for _, f := range got {
		if f.Key == files[0].Key {
			t.Fatal("the refused object is still planned")
		}
	}
}
