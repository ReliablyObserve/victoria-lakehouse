package manifest

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Review regression (#404 round 5, #424 item 4): SaveTo copies the partition
// slice headers under the read lock and encodes them after releasing it. The
// writers used to modify the same backing arrays in place (removeFileLocked
// shifted the tail down, the per-key mutators wrote the element), so the
// encoder could read a half-shifted partition: one key twice, another missing,
// or key A carrying key B's time bounds. Loaded after a restart, that torn
// entry kept its wrong bounds through every refresh (a refresh keeps the
// enrichment it already knows) and pruned the file out of queries whose rows
// it holds. The writers now copy a partition before modifying it when a
// snapshot may hold it. Run with -race.

const cowPartitions = 4

func cowPartition(p int) string { return fmt.Sprintf("dt=2026-06-%02d/hour=00", 1+p) }

func cowKey(p, i int) string {
	return fmt.Sprintf("logs/%s/f-%05d.parquet", cowPartition(p), i)
}

// cowBounds are the exact bounds of file i: unique per key, never hour-shaped.
func cowBounds(p, i int) (int64, int64) {
	start, _ := parsePartitionTime(cowPartition(p))
	lo := start.UnixNano() + int64(i+1)*int64(time.Millisecond)
	return lo, lo + int64(time.Microsecond)
}

// cowManifest holds perPart files in each of cowPartitions partitions. Exact
// bounds (cowBounds) when exact, else the inferred partition hour.
func cowManifest(perPart int, exact bool) *Manifest {
	m := New("b", "")
	for p := 0; p < cowPartitions; p++ {
		start, _ := parsePartitionTime(cowPartition(p))
		for i := 0; i < perPart; i++ {
			fi := FileInfo{Key: cowKey(p, i), Size: 100, RowCount: 1}
			if exact {
				fi.MinTimeNs, fi.MaxTimeNs = cowBounds(p, i)
			} else {
				fi.RowCount = 0
				fi.MinTimeNs, fi.MaxTimeNs = start.UnixNano(), start.Add(time.Hour).UnixNano()-1
				fi.BoundsInferred = true
			}
			m.AddFile(cowPartition(p), fi)
		}
	}
	return m
}

// checkCowSnapshot loads the snapshot at path and fails when a key appears more
// than once or carries bounds that are neither its own exact bounds nor (when
// allowed) the inferred partition hour.
func checkCowSnapshot(t *testing.T, path string, allowInferred bool) int {
	t.Helper()
	re := New("b", "")
	if err := re.LoadFrom(path); err != nil {
		t.Fatalf("load: %v", err)
	}
	seen := make(map[string]bool)
	for partition, files := range re.AllFiles() {
		for _, fi := range files {
			if seen[fi.Key] {
				t.Fatalf("snapshot tracks %s twice (a half-shifted partition was encoded)", fi.Key)
			}
			seen[fi.Key] = true
			var p, i int
			if _, err := fmt.Sscanf(fi.Key, "logs/dt=2026-06-%02d/hour=00/f-%05d.parquet", &p, &i); err != nil {
				t.Fatalf("unexpected key %q: %v", fi.Key, err)
			}
			p--
			if cowPartition(p) != partition {
				t.Fatalf("%s in partition %s", fi.Key, partition)
			}
			lo, hi := cowBounds(p, i)
			if fi.MinTimeNs == lo && fi.MaxTimeNs == hi && !fi.BoundsInferred {
				continue
			}
			if allowInferred && fi.BoundsInferred && hourShaped(fi.Key, fi.MinTimeNs, fi.MaxTimeNs) {
				continue
			}
			t.Fatalf("snapshot entry %s has bounds [%d,%d] inferred=%v; its own are [%d,%d]: a torn entry",
				fi.Key, fi.MinTimeNs, fi.MaxTimeNs, fi.BoundsInferred, lo, hi)
		}
	}
	return len(seen)
}

func TestReview424_SaveToConcurrentWithRemoveFile(t *testing.T) {
	const perPart = 1000
	m := cowManifest(perPart, true)
	path := filepath.Join(t.TempDir(), "m.snap")
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Remove from the front of every partition: each removal shifts the
		// whole partition down by one.
		for i := 0; i < perPart/2; i++ {
			for p := 0; p < cowPartitions; p++ {
				m.RemoveFile(cowPartition(p), cowKey(p, i))
			}
		}
	}()
	saves := 0
	for {
		select {
		case <-done:
		default:
			if err := m.SaveTo(path); err != nil {
				t.Fatal(err)
			}
			checkCowSnapshot(t, path, false)
			saves++
			continue
		}
		break
	}
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	if n := checkCowSnapshot(t, path, false); n != cowPartitions*perPart/2 {
		t.Fatalf("final snapshot holds %d files, want %d", n, cowPartitions*perPart/2)
	}
	t.Logf("%d snapshots checked while removing", saves)
}

func TestReview424_SaveToConcurrentWithElementMutators(t *testing.T) {
	const perPart = 1000
	m := cowManifest(perPart, false)
	path := filepath.Join(t.TempDir(), "m.snap")
	var wg sync.WaitGroup
	stop := make(chan struct{})
	mutate := func(fn func(p, i int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPart; i++ {
				for p := 0; p < cowPartitions; p++ {
					select {
					case <-stop:
						return
					default:
					}
					fn(p, i)
				}
			}
		}()
	}
	mutate(func(p, i int) {
		lo, hi := cowBounds(p, i)
		m.EnrichFileMetadata(cowKey(p, i), 10, lo, hi)
	})
	mutate(func(p, i int) { m.SetFileBucket(cowKey(p, i), "moved") })
	mutate(func(p, i int) {
		m.UpdateFileColumnStats(cowKey(p, i), map[string]ColumnMinMax{"level": {Min: "a", Max: "z"}})
	})
	mutate(func(p, i int) { m.MarkTraceIDHex(cowKey(p, i)) })
	// Removals at the tail interleave shifts with the element writes.
	mutate(func(p, i int) {
		if i%10 == 9 {
			m.RemoveFile(cowPartition(p), cowKey(p, perPart-1-i))
		}
	})
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	saves := 0
loop:
	for {
		select {
		case <-finished:
			break loop
		default:
		}
		if err := m.SaveTo(path); err != nil {
			close(stop)
			t.Fatal(err)
		}
		checkCowSnapshot(t, path, true)
		saves++
	}
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	checkCowSnapshot(t, path, true)
	t.Logf("%d snapshots checked while mutating", saves)
}

// The deterministic form: the mutations run after SaveTo captured the
// partitions and before it encodes them. The snapshot must be the manifest as
// it was at the capture, and the live manifest must have every mutation.
func TestReview424_SnapshotIsTheStateAtCapture(t *testing.T) {
	const perPart = 10
	m := cowManifest(perPart, false)
	p := 0
	victim, enrichedKey := cowKey(p, 3), cowKey(p, 5)
	lo, hi := cowBounds(p, 5)
	lo9, hi9 := cowBounds(p, 9)
	path := filepath.Join(t.TempDir(), "m.snap")
	saveCapturedTestHook = func() {
		m.RemoveFile(cowPartition(p), victim)
		m.EnrichFileMetadata(enrichedKey, 10, lo, hi)
		m.SetFileBucket(cowKey(p, 6), "moved")
		m.UpdateFileColumnStats(cowKey(p, 7), map[string]ColumnMinMax{"level": {Min: "a", Max: "z"}})
		m.MarkTraceIDHex(cowKey(p, 8))
		m.EnrichFromProvider(cowMetaProvider{key: cowKey(p, 9), lo: lo9, hi: hi9})
	}
	defer func() { saveCapturedTestHook = nil }()
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	saveCapturedTestHook = nil

	if n := checkCowSnapshot(t, path, true); n != cowPartitions*perPart {
		t.Fatalf("snapshot holds %d files, want %d (the state at capture)", n, cowPartitions*perPart)
	}
	re := New("b", "")
	if err := re.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	for _, fi := range re.FilesForPartition(cowPartition(p)) {
		if fi.Bucket != "" || fi.ColumnStats != nil || fi.TraceIDHex || !fi.BoundsInferred || fi.RowCount != 0 {
			t.Fatalf("snapshot entry %s shows a write made after the capture: %+v", fi.Key, fi)
		}
	}

	if m.HasKey(victim) {
		t.Fatal("the live manifest kept the removed key")
	}
	check := func(key string, ok func(FileInfo) bool) {
		t.Helper()
		fi, found := m.GetFileByKey(key)
		if !found || !ok(fi) {
			t.Fatalf("live manifest lost the write to %s: %+v", key, fi)
		}
	}
	check(enrichedKey, func(fi FileInfo) bool { return fi.MinTimeNs == lo && fi.MaxTimeNs == hi && !fi.BoundsInferred })
	check(cowKey(p, 6), func(fi FileInfo) bool { return fi.Bucket == "moved" })
	check(cowKey(p, 7), func(fi FileInfo) bool { return fi.ColumnStats["level"].Max == "z" })
	check(cowKey(p, 8), func(fi FileInfo) bool { return fi.TraceIDHex })
	check(cowKey(p, 9), func(fi FileInfo) bool { return fi.MinTimeNs == lo9 && fi.MaxTimeNs == hi9 && fi.RowCount == 10 })

	// The next snapshot has every mutation.
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	if n := checkCowSnapshot(t, path, true); n != cowPartitions*perPart-1 {
		t.Fatalf("second snapshot holds %d files, want %d", n, cowPartitions*perPart-1)
	}
}

type cowMetaProvider struct {
	key    string
	lo, hi int64
}

func (c cowMetaProvider) FileMeta(_ string, key string) (FileMeta, bool) {
	if key != c.key {
		return FileMeta{}, false
	}
	return FileMeta{RowCount: 10, MinTimeNs: c.lo, MaxTimeNs: c.hi}, true
}

// A partition is copied at most once per snapshot, however many writes it
// takes, and not at all before the first snapshot or after a wholesale
// replacement (a refresh, a load): the copy-on-write cost is bounded by the
// partitions written between two persists, not by the writes.
func TestReview424_CopyOncePerPartitionPerSnapshot(t *testing.T) {
	m := cowManifest(50, false)
	arr := func(p int) *FileInfo {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return &m.files[cowPartition(p)][0]
	}
	before := arr(0)
	m.MarkTraceIDHex(cowKey(0, 1))
	if arr(0) != before {
		t.Fatal("a partition was copied although no snapshot holds it")
	}
	if err := m.SaveTo(filepath.Join(t.TempDir(), "m.snap")); err != nil {
		t.Fatal(err)
	}
	m.MarkTraceIDHex(cowKey(0, 2))
	first := arr(0)
	if first == before {
		t.Fatal("a partition the snapshot holds was written in place")
	}
	for i := 3; i < 50; i++ {
		m.SetFileBucket(cowKey(0, i), "x")
	}
	m.RemoveFile(cowPartition(0), cowKey(0, 49))
	if arr(0) != first {
		t.Fatal("a partition was copied twice between two snapshots")
	}
	if untouched := arr(1); untouched == nil {
		t.Fatal("fixture")
	}
}
