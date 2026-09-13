package compaction

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil/storageinvariants"
)

// These tests run the REAL delete rewrite scheduler and the REAL compactor
// against the same manifest, bucket and tombstone store. Each publishes into
// the manifest; before both publishes were conditional, interleaving them on
// one source file either duplicated that file's rows (both outputs registered)
// or retired a tombstone while a compacted file still held its rows.

const racePartition = "dt=2026-07-03/hour=05"

var raceHour = time.Date(2026, 7, 3, 5, 0, 0, 0, time.UTC)

// raceWorld is one partition, two source files, one tombstone, shared by a
// rewrite scheduler and a compactor.
type raceWorld struct {
	pool     *gatedPool
	manifest *manifest.Manifest
	store    *delete.TombstoneStore
	files    []manifest.FileInfo
	// kept / deleted are the oracle: bodies that must survive exactly once,
	// and bodies that must be gone once the tombstone retires.
	kept    map[string]bool
	deleted map[string]bool
}

func newRaceWorld(t *testing.T) *raceWorld {
	t.Helper()
	w := &raceWorld{
		pool:     &gatedPool{mockPool: newMockPool()},
		manifest: manifest.New("test-bucket", ""),
		store:    delete.NewTombstoneStore(),
		kept:     map[string]bool{},
		deleted:  map[string]bool{},
	}
	for i := 0; i < 2; i++ {
		var rows []schema.LogRow
		for j := 0; j < 4; j++ {
			svc := "web"
			if j%2 == 1 {
				svc = "leaky"
			}
			body := fmt.Sprintf("f%d-r%d-%s", i, j, svc)
			rows = append(rows, schema.LogRow{
				TimestampUnixNano: raceHour.Add(time.Duration(i*10+j) * time.Second).UnixNano(),
				Body:              body,
				ServiceName:       svc,
			})
			if svc == "leaky" {
				w.deleted[body] = true
			} else {
				w.kept[body] = true
			}
		}
		key := fmt.Sprintf("logs/%s/src-%d.parquet", racePartition, i)
		data, err := WriteLogs(rows, 100, 1)
		if err != nil {
			t.Fatalf("write source: %v", err)
		}
		w.pool.put(key, data)
		minNs, maxNs := schema.LogRowTimeBounds(rows)
		fi := manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: int64(len(rows)),
			MinTimeNs: minNs, MaxTimeNs: maxNs,
			Labels: schema.ExtractLogLabels(rows), RawBytes: schema.EstimateRawBytesLogs(rows),
		}
		w.manifest.AddFile(racePartition, fi)
		w.files = append(w.files, fi)
	}
	keys := []string{w.files[0].Key, w.files[1].Key}
	w.store.Add(delete.Tombstone{
		ID:           "ts-race",
		Query:        `service.name:="leaky"`,
		StartNs:      raceHour.UnixNano(),
		EndNs:        raceHour.Add(time.Hour).UnixNano(),
		AffectedKeys: keys,
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "permanent",
		Reaped:       map[string]bool{},
	})
	return w
}

func (w *raceWorld) scheduler() *delete.RewriteScheduler {
	return delete.NewRewriteScheduler(delete.RewriteSchedulerConfig{
		Store:          w.store,
		Rewriter:       delete.NewRewriter(w.pool, "logs/", 100, "logs", delete.WithParquetWriters(delete.ParquetWriters{Logs: WriteLogs, Traces: WriteTraces, CompressionLevel: 1})),
		Detector:       delete.NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       w.manifest,
	})
}

func (w *raceWorld) compactor(delay time.Duration) *Compactor {
	return NewCompactor(CompactorConfig{
		Pool: w.pool, Manifest: w.manifest, Prefix: "logs/", Mode: config.ModeLogs,
		RowGroupSize: 100, Tombstones: w.store, TombstoneRewriteDelay: delay,
	})
}

// converge drives the system to rest: scheduler passes until no tombstone has
// work, then a sweep of any object nothing manifests (what the orphan sweep
// does after its age gate).
func (w *raceWorld) converge(t *testing.T) {
	t.Helper()
	for i := 0; i < 5 && w.store.Count() > 0; i++ {
		w.scheduler().RunOnce(context.Background())
	}
	for _, k := range w.pool.Keys() {
		if !w.manifest.HasKey(k) {
			_ = w.pool.Delete(context.Background(), k)
		}
	}
}

// assert checks the full invariant set plus the row oracle.
func (w *raceWorld) assert(t *testing.T, stage string) {
	t.Helper()
	storageinvariants.Assert(t, stage, storageinvariants.State{
		Manifest: w.manifest, Bucket: w.pool.mockPool, Tombstones: tombstoneViewsOf(w.store),
	})

	counts := map[string]int{}
	var scanned int64
	for _, k := range w.pool.Keys() {
		rows, err := readLogRows(w.pool.get(k))
		if err != nil {
			t.Fatalf("%s: read %s: %v", stage, k, err)
		}
		for i := range rows {
			counts[rows[i].Body]++
		}
		scanned += int64(len(rows))
	}
	if got := storageinvariants.ManifestRows(w.manifest); got != scanned {
		t.Fatalf("%s: manifest claims %d rows, a full scan finds %d", stage, got, scanned)
	}
	for body := range w.kept {
		switch counts[body] {
		case 1:
		case 0:
			t.Fatalf("%s: kept row %q is gone", stage, body)
		default:
			t.Fatalf("%s: kept row %q is stored %d times — a duplicated publish", stage, body, counts[body])
		}
	}
	if w.store.Count() == 0 {
		for body := range w.deleted {
			if counts[body] > 0 {
				t.Fatalf("%s: the tombstone retired but deleted row %q is still stored (%d copies)", stage, body, counts[body])
			}
		}
	}
}

func tombstoneViewsOf(store *delete.TombstoneStore) []storageinvariants.TombstoneView {
	var out []storageinvariants.TombstoneView
	for _, ts := range store.Active() {
		out = append(out, storageinvariants.TombstoneView{
			ID: ts.ID, Mode: ts.Mode, AffectedKeys: ts.AffectedKeys, Reaped: ts.Reaped,
		})
	}
	return out
}

// gatedPool blocks the first Upload whose key matches a predicate until
// released, which lets a test freeze one actor between its read and its
// publish while the other actor runs to completion.
type gatedPool struct {
	*mockPool

	mu       sync.Mutex
	match    func(key string) bool
	reached  chan struct{}
	release  chan struct{}
	consumed bool
}

func (g *gatedPool) gate(match func(string) bool) (reached <-chan struct{}, release func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.match = match
	g.reached = make(chan struct{})
	g.release = make(chan struct{})
	g.consumed = false
	rel := g.release
	return g.reached, func() { close(rel) }
}

func (g *gatedPool) Upload(ctx context.Context, key string, data []byte) error {
	g.mu.Lock()
	hit := g.match != nil && !g.consumed && g.match(key)
	var reached, release chan struct{}
	if hit {
		g.consumed = true
		reached, release = g.reached, g.release
	}
	g.mu.Unlock()
	if hit {
		close(reached)
		<-release
	}
	return g.mockPool.Upload(ctx, key, data)
}

// TestDeleteRace_CompactionPublishesWhileTheRewriteIsInFlight freezes the
// rewrite after it has read its source and written its replacement, lets
// compaction merge that same source and publish, then lets the rewrite try to
// publish. The rewrite must discard its replacement (its source is gone), and
// the tombstone must follow the rows into the compacted output.
func TestDeleteRace_CompactionPublishesWhileTheRewriteIsInFlight(t *testing.T) {
	w := newRaceWorld(t)
	reached, release := w.pool.gate(func(key string) bool {
		return !strings.Contains(key, "compacted-") // the rewrite's replacement upload
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.scheduler().RunOnce(context.Background())
	}()

	<-reached
	if _, err := w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0); err != nil {
		t.Fatalf("compaction should win the race and publish: %v", err)
	}
	release()
	wg.Wait()

	w.converge(t)
	w.assert(t, "compaction published first")
	if w.store.Count() != 0 {
		t.Fatalf("the tombstone should have followed the rows and retired, %d remain", w.store.Count())
	}
}

// TestDeleteRace_RewritePublishesWhileTheCompactionIsInFlight is the opposite
// interleaving: compaction has read both sources and written its output when
// the rewrite replaces one of them. Compaction's publish must be abandoned —
// registering its output would duplicate every row of the source the rewrite
// replaced.
func TestDeleteRace_RewritePublishesWhileTheCompactionIsInFlight(t *testing.T) {
	w := newRaceWorld(t)
	// Make the compaction eligible-blind (delay far in the future) so its
	// output carries the tombstoned rows: the worst case to register.
	reached, release := w.pool.gate(func(key string) bool {
		return strings.Contains(key, "compacted-")
	})

	var compactErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, compactErr = w.compactor(1000*time.Hour).Compact(context.Background(), racePartition, w.files, 0)
	}()

	<-reached
	w.scheduler().RunOnce(context.Background())
	release()
	wg.Wait()

	if compactErr == nil {
		t.Fatal("compaction must abandon its publish once a source it merged has been replaced")
	}

	w.converge(t)
	w.assert(t, "rewrite published first")
	if w.store.Count() != 0 {
		t.Fatalf("the tombstone should retire once both sources are rewritten, %d remain", w.store.Count())
	}
}

// TestDeleteRace_CompactionInsideTheUndeleteWindowThenRetire covers the
// resurrection path directly: a compaction while the tombstone is still inside
// its rewrite_delay carries the rows forward into a new file. When the
// tombstone later becomes eligible, the scheduler must find and rewrite that
// file before retiring — not retire on the strength of the original keys being
// gone.
func TestDeleteRace_CompactionInsideTheUndeleteWindowThenRetire(t *testing.T) {
	w := newRaceWorld(t)
	w.store.Update("ts-race", func(ts *delete.Tombstone) bool {
		ts.CreatedAt = time.Now()
		return true
	})

	res, err := w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	w.assert(t, "after compaction inside the window")
	if w.store.Count() != 1 {
		t.Fatal("the tombstone must stay active: its rows were carried into the compacted output")
	}

	// The window passes.
	w.store.Update("ts-race", func(ts *delete.Tombstone) bool {
		ts.CreatedAt = time.Now().Add(-2 * time.Hour)
		return true
	})
	results := w.scheduler().RunOnce(context.Background())
	var rewroteOutput bool
	for _, r := range results {
		if r.OldKey == res.OutputFile && r.RowsRemoved > 0 {
			rewroteOutput = true
		}
	}
	if !rewroteOutput {
		t.Fatalf("the compacted output %s holding the tombstone's rows was not rewritten: %+v", res.OutputFile, results)
	}

	w.converge(t)
	w.assert(t, "after the window passed")
	if w.store.Count() != 0 {
		t.Fatalf("the tombstone should retire once the output is rewritten, %d remain", w.store.Count())
	}
}

// TestDeleteRace_Randomized runs the two actors truly concurrently many times
// under -race; whichever interleaving the scheduler picks, the converged state
// must satisfy every invariant and the row oracle.
func TestDeleteRace_Randomized(t *testing.T) {
	iterations := 40
	if testing.Short() {
		iterations = 8
	}
	for i := 0; i < iterations; i++ {
		w := newRaceWorld(t)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			w.scheduler().RunOnce(context.Background())
		}()
		go func() {
			defer wg.Done()
			_, _ = w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0)
		}()
		go func() {
			defer wg.Done()
			// A reader: the query path's manifest and tombstone lookups.
			for j := 0; j < 50; j++ {
				_ = w.manifest.GetFilesForRange(raceHour.UnixNano(), raceHour.Add(time.Hour).UnixNano())
				_ = w.store.ForRange(raceHour.UnixNano(), raceHour.Add(time.Hour).UnixNano())
			}
		}()
		wg.Wait()

		w.converge(t)
		w.assert(t, fmt.Sprintf("iteration %d", i))
		if w.store.Count() != 0 {
			t.Fatalf("iteration %d: the tombstone did not retire after convergence", i)
		}
	}
}

// hookManifest runs a callback the first time the rewriter looks up a source's
// partition — i.e. after its non-atomic "is the source still there" check and
// before its swap. That is the narrowest window a concurrent compaction can hit,
// and the only one the atomic condition inside Manifest.ReplaceFile protects.
type hookManifest struct {
	*manifest.Manifest
	mu   sync.Mutex
	hook func()
}

func (h *hookManifest) PartitionForKey(key string) (string, bool) {
	p, ok := h.Manifest.PartitionForKey(key)
	h.mu.Lock()
	hook := h.hook
	h.hook = nil
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	return p, ok
}

// TestDeleteRace_CompactionPublishesBetweenTheRewriteCheckAndItsSwap lands a
// full compaction publish between the rewrite resolving its source's partition
// and swapping it. Only the swap's own condition can catch this; an
// unconditional swap would register the rewrite next to the compacted output
// and store every kept row of that source twice.
func TestDeleteRace_CompactionPublishesBetweenTheRewriteCheckAndItsSwap(t *testing.T) {
	w := newRaceWorld(t)
	hm := &hookManifest{Manifest: w.manifest}
	hm.hook = func() {
		if _, err := w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0); err != nil {
			t.Errorf("compaction inside the window should publish: %v", err)
		}
	}

	sched := delete.NewRewriteScheduler(delete.RewriteSchedulerConfig{
		Store:          w.store,
		Rewriter:       delete.NewRewriter(w.pool, "logs/", 100, "logs", delete.WithParquetWriters(delete.ParquetWriters{Logs: WriteLogs, Traces: WriteTraces, CompressionLevel: 1})),
		Detector:       delete.NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       hm,
	})
	sched.RunOnce(context.Background())

	w.converge(t)
	w.assert(t, "compaction published inside the rewrite's check-to-swap window")
}

// TestDeleteRace_CompactionCrashBeforeTombstoneBookkeeping simulates a
// compaction that published its merged output and removed the sources, then
// died before transferring the tombstone to the output. The output holds the
// tombstone's rows (it ran inside the un-delete window) but nothing on the
// tombstone names it. The only thing standing between those rows and
// resurrection is the scheduler re-reading the files that exist before it
// retires the tombstone.
func TestDeleteRace_CompactionCrashBeforeTombstoneBookkeeping(t *testing.T) {
	w := newRaceWorld(t)

	// The compaction, minus its tombstone bookkeeping: merge everything
	// (nothing dropped — it ran inside the window), publish, delete sources.
	var merged []schema.LogRow
	for _, fi := range w.files {
		rows, err := readLogRows(w.pool.get(fi.Key))
		if err != nil {
			t.Fatalf("read source: %v", err)
		}
		merged = append(merged, rows...)
	}
	data, err := WriteLogs(merged, 100, 1)
	if err != nil {
		t.Fatalf("write output: %v", err)
	}
	outKey := "logs/" + racePartition + "/compacted-L1-crash.parquet"
	w.pool.put(outKey, data)
	minNs, maxNs := schema.LogRowTimeBounds(merged)
	if !w.manifest.ReplaceFiles(racePartition, []string{w.files[0].Key, w.files[1].Key}, manifest.FileInfo{
		Key: outKey, Size: int64(len(data)), RowCount: int64(len(merged)),
		MinTimeNs: minNs, MaxTimeNs: maxNs, Labels: schema.ExtractLogLabels(merged),
	}) {
		t.Fatal("fixture: the publish should succeed")
	}
	for _, fi := range w.files {
		_ = w.pool.Delete(context.Background(), fi.Key)
	}
	// ...crash: reconcileTombstones never runs.

	results := w.scheduler().RunOnce(context.Background())

	var rewroteOutput bool
	for _, r := range results {
		if r.OldKey == outKey && r.RowsRemoved > 0 {
			rewroteOutput = true
		}
	}
	if !rewroteOutput {
		t.Fatalf("the scheduler did not discover and rewrite %s, which still holds the tombstone's rows: %+v", outKey, results)
	}
	w.converge(t)
	w.assert(t, "after a compaction crashed before its tombstone bookkeeping")
	if w.store.Count() != 0 {
		t.Fatalf("the tombstone should retire once the discovered output is rewritten, %d remain", w.store.Count())
	}
}

// TestDelete_LateFileInTheRangeBlocksRetirement covers the other file the
// original AffectedKeys snapshot cannot know about: one written into the
// tombstone's range after the delete was issued. While it holds a matching row
// the tombstone must stay, and the scheduler must rewrite it.
func TestDelete_LateFileInTheRangeBlocksRetirement(t *testing.T) {
	w := newRaceWorld(t)

	late := []schema.LogRow{
		{TimestampUnixNano: raceHour.Add(30 * time.Minute).UnixNano(), Body: "late-leaky", ServiceName: "leaky"},
		{TimestampUnixNano: raceHour.Add(31 * time.Minute).UnixNano(), Body: "late-web", ServiceName: "web"},
	}
	w.deleted["late-leaky"] = true
	w.kept["late-web"] = true
	data, err := WriteLogs(late, 100, 1)
	if err != nil {
		t.Fatalf("write late file: %v", err)
	}
	lateKey := "logs/" + racePartition + "/late.parquet"
	w.pool.put(lateKey, data)
	minNs, maxNs := schema.LogRowTimeBounds(late)
	w.manifest.AddFile(racePartition, manifest.FileInfo{
		Key: lateKey, Size: int64(len(data)), RowCount: int64(len(late)),
		MinTimeNs: minNs, MaxTimeNs: maxNs, Labels: schema.ExtractLogLabels(late),
	})

	w.scheduler().RunOnce(context.Background())
	w.converge(t)
	w.assert(t, "after a late file landed in the tombstone's range")
	if w.store.Count() != 0 {
		t.Fatalf("the tombstone should retire once the late file is rewritten too, %d remain", w.store.Count())
	}
}
