package manifest

import (
	"sync"
	"testing"
)

const (
	replacePartition = "dt=2026-08-01/hour=04"
	replaceOldKey    = "logs/dt=2026-08-01/hour=04/old.parquet"
	replaceNewKey    = "logs/dt=2026-08-01/hour=04/new.parquet"
)

func manifestWithOneFile(t *testing.T) *Manifest {
	t.Helper()
	m := New("bucket", "")
	m.AddFile(replacePartition, FileInfo{
		Key:       replaceOldKey,
		Size:      1000,
		RowCount:  10,
		MinTimeNs: 100,
		MaxTimeNs: 900,
		Labels:    map[string][]string{"service.name": {"web"}},
	})
	return m
}

// TestReplaceFile_NoObservableIntermediateState is the reason ReplaceFile exists
// rather than a RemoveFile followed by an AddFile.
//
// The delete rewriter publishes a rewritten object by swapping one manifest
// entry for another. Done as two calls, a concurrent reader can land in the gap
// and see the partition with NEITHER entry — the rewritten rows briefly
// invisible — or, with the other ordering, with BOTH — the rows briefly counted
// twice. One critical section removes the window entirely.
func TestReplaceFile_NoObservableIntermediateState(t *testing.T) {
	m := manifestWithOneFile(t)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	bad := make(chan string, 4)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			files := m.FilesForPartition(replacePartition)
			if len(files) != 1 {
				select {
				case bad <- "partition observed with a number of files other than 1":
				default:
				}
				return
			}
		}
	}()

	for i := 0; i < 200; i++ {
		m.ReplaceFile(replacePartition, replaceOldKey, FileInfo{Key: replaceNewKey, Size: 500, RowCount: 6, MinTimeNs: 100, MaxTimeNs: 900})
		m.ReplaceFile(replacePartition, replaceNewKey, FileInfo{Key: replaceOldKey, Size: 1000, RowCount: 10, MinTimeNs: 100, MaxTimeNs: 900})
	}
	close(stop)
	wg.Wait()

	select {
	case msg := <-bad:
		t.Fatal(msg)
	default:
	}
}

func TestReplaceFile_SwapsTheEntryAndItsAggregates(t *testing.T) {
	m := manifestWithOneFile(t)

	if !m.ReplaceFile(replacePartition, replaceOldKey, FileInfo{
		Key: replaceNewKey, Size: 400, RowCount: 6, MinTimeNs: 100, MaxTimeNs: 900,
	}) {
		t.Fatal("ReplaceFile should report that it replaced an existing entry")
	}

	if m.HasKey(replaceOldKey) {
		t.Error("the superseded key is still in the manifest")
	}
	if !m.HasKey(replaceNewKey) {
		t.Error("the replacement is not in the manifest")
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("TotalFiles = %d, want 1", got)
	}
	if got := m.TotalBytes(); got != 400 {
		t.Errorf("TotalBytes = %d, want the replacement's 400", got)
	}
	if got := m.TotalRows(); got != 6 {
		t.Errorf("TotalRows = %d, want the replacement's 6", got)
	}
	if p, ok := m.PartitionForKey(replaceNewKey); !ok || p != replacePartition {
		t.Errorf("replacement filed under %q, want %q", p, replacePartition)
	}
}

// TestReplaceFile_RefusesWhenTheSourceIsGone is the optimistic-concurrency
// guard. A rewrite reads its source, then publishes; compaction may have merged
// that source in between. Registering the rewrite anyway would put the kept rows
// in the manifest twice (the rewrite and the compacted output) and bring the
// deleted rows back through the compacted copy.
func TestReplaceFile_RefusesWhenTheSourceIsGone(t *testing.T) {
	m := manifestWithOneFile(t)

	if m.ReplaceFile(replacePartition, "logs/dt=2026-08-01/hour=04/never-existed.parquet",
		FileInfo{Key: replaceNewKey, Size: 1, RowCount: 1}) {
		t.Fatal("ReplaceFile must refuse when the source is not in the manifest")
	}
	if m.HasKey(replaceNewKey) {
		t.Error("a refused swap must not register the replacement")
	}
	if !m.HasKey(replaceOldKey) {
		t.Error("an unrelated entry must not be disturbed")
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("TotalFiles = %d, want 1", got)
	}
}

// TestReplaceFile_RefusesWhenTheSourceIsInAnotherPartition guards against a
// caller passing the wrong partition: the swap is keyed on (partition, key), so
// a mismatch is treated as "not present" rather than removing nothing and
// adding the replacement to the wrong partition.
func TestReplaceFile_RefusesWhenTheSourceIsInAnotherPartition(t *testing.T) {
	m := manifestWithOneFile(t)
	if m.ReplaceFile("dt=2026-08-01/hour=05", replaceOldKey, FileInfo{Key: replaceNewKey, RowCount: 1}) {
		t.Fatal("ReplaceFile must refuse a source registered under a different partition")
	}
	if m.HasKey(replaceNewKey) || !m.HasKey(replaceOldKey) {
		t.Fatal("a refused swap must leave the manifest unchanged")
	}
}

func TestReplaceFiles_IsAllOrNothing(t *testing.T) {
	m := manifestWithOneFile(t)
	second := "logs/dt=2026-08-01/hour=04/second.parquet"
	m.AddFile(replacePartition, FileInfo{Key: second, Size: 500, RowCount: 5, MinTimeNs: 100, MaxTimeNs: 900})
	merged := FileInfo{Key: replaceNewKey, Size: 1200, RowCount: 15, MinTimeNs: 100, MaxTimeNs: 900}

	// One source already gone: nothing may change. This is compaction racing a
	// rewrite (or another compaction) that took one of its inputs.
	if m.ReplaceFiles(replacePartition, []string{replaceOldKey, second, "logs/dt=2026-08-01/hour=04/gone.parquet"}, merged) {
		t.Fatal("ReplaceFiles must refuse when any source is gone")
	}
	if m.HasKey(replaceNewKey) || !m.HasKey(replaceOldKey) || !m.HasKey(second) {
		t.Fatal("a refused merge must leave every entry exactly as it was")
	}
	if got := m.TotalRows(); got != 15 {
		t.Fatalf("TotalRows = %d after a refused merge, want the untouched 15", got)
	}

	// All present: the merge lands in one step.
	if !m.ReplaceFiles(replacePartition, []string{replaceOldKey, second}, merged) {
		t.Fatal("ReplaceFiles must succeed when every source is present")
	}
	if m.HasKey(replaceOldKey) || m.HasKey(second) || !m.HasKey(replaceNewKey) {
		t.Fatal("the merge did not replace the sources with the output")
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("TotalFiles = %d, want 1", got)
	}
	if got := m.TotalRows(); got != 15 {
		t.Errorf("TotalRows = %d, want the output's 15", got)
	}
}

// TestReplaceFiles_ConcurrentConflictingPublishesNeverBothLand races the two
// publishes a delete rewrite and a compaction of the same source perform. Under
// the old unconditional add, both could land and duplicate the source's rows.
// Exactly one must win, every time.
func TestReplaceFiles_ConcurrentConflictingPublishesNeverBothLand(t *testing.T) {
	for i := 0; i < 200; i++ {
		m := manifestWithOneFile(t)
		rewrite := FileInfo{Key: "logs/dt=2026-08-01/hour=04/rewrite.parquet", RowCount: 7}
		compacted := FileInfo{Key: "logs/dt=2026-08-01/hour=04/compacted.parquet", RowCount: 10}

		var wg sync.WaitGroup
		var rewroteOK, compactedOK bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			rewroteOK = m.ReplaceFile(replacePartition, replaceOldKey, rewrite)
		}()
		go func() {
			defer wg.Done()
			compactedOK = m.ReplaceFiles(replacePartition, []string{replaceOldKey}, compacted)
		}()
		wg.Wait()

		if rewroteOK == compactedOK {
			t.Fatalf("iteration %d: rewrite=%v compaction=%v — exactly one publish must win", i, rewroteOK, compactedOK)
		}
		if m.HasKey(rewrite.Key) && m.HasKey(compacted.Key) {
			t.Fatalf("iteration %d: both publishes landed; the source's rows are duplicated", i)
		}
		if got := m.TotalFiles(); got != 1 {
			t.Fatalf("iteration %d: TotalFiles = %d, want 1", i, got)
		}
	}
}

func TestReplaceFile_FiresBothChangeObservers(t *testing.T) {
	m := manifestWithOneFile(t)

	var added, removed []string
	m.SetChangeObserver(
		func(_ string, fi FileInfo) { added = append(added, fi.Key) },
		func(_ string, fi FileInfo) { removed = append(removed, fi.Key) },
	)

	m.ReplaceFile(replacePartition, replaceOldKey, FileInfo{Key: replaceNewKey, Size: 1, RowCount: 1})

	// The pmeta facet feed rides these callbacks. A swap that fired only one of
	// them would leave the catalog describing a file that no longer exists, or
	// missing the one that does.
	if len(removed) != 1 || removed[0] != replaceOldKey {
		t.Errorf("onRemove saw %v, want [%s]", removed, replaceOldKey)
	}
	if len(added) != 1 || added[0] != replaceNewKey {
		t.Errorf("onAdd saw %v, want [%s]", added, replaceNewKey)
	}
}

func TestReplaceFile_RefusesAKeyTheManifestAlreadyServes(t *testing.T) {
	m := manifestWithOneFile(t)
	m.AddFile(replacePartition, FileInfo{Key: replaceNewKey, Size: 5, RowCount: 1})

	// Output keys carry a short random id. A swap onto a key the manifest
	// already serves would drop the source (AddFile's idempotency guard makes
	// the insert a no-op) and take its rows out of the manifest with it, so the
	// swap is refused and nothing changes.
	if m.ReplaceFile(replacePartition, replaceOldKey, FileInfo{Key: replaceNewKey, Size: 5, RowCount: 1}) {
		t.Fatal("a swap onto an already-registered key must be refused")
	}
	if n := len(m.FilesForPartition(replacePartition)); n != 2 {
		t.Fatalf("partition holds %d entries, want both files untouched", n)
	}
	if !m.HasKey(replaceOldKey) || m.IsRetired(replaceOldKey) {
		t.Fatal("the refused swap must leave the source registered")
	}

	// The same guard for a merge publish.
	if m.ReplaceFiles(replacePartition, []string{replaceOldKey}, FileInfo{Key: replaceNewKey, Size: 5, RowCount: 1}) {
		t.Fatal("a merge onto an already-registered key must be refused")
	}
	if n := len(m.FilesForPartition(replacePartition)); n != 2 {
		t.Fatalf("partition holds %d entries after the refused merge", n)
	}
}

func TestReplaceFile_RefusesAHeldKey(t *testing.T) {
	m := manifestWithOneFile(t)
	m.Hold(replaceOldKey)
	if m.ReplaceFile(replacePartition, replaceOldKey, FileInfo{Key: replaceNewKey, Size: 1, RowCount: 1}) {
		t.Fatal("a held key may not be superseded while its own publish is unsettled")
	}
	if m.ReplaceFiles(replacePartition, []string{replaceOldKey}, FileInfo{Key: replaceNewKey, Size: 1, RowCount: 1}) {
		t.Fatal("a merge may not take a held source")
	}
	if m.RemoveFileIfPresent(replacePartition, replaceOldKey) {
		t.Fatal("a held key may not be removed by a rewrite that emptied it")
	}
	m.Release(replaceOldKey)
	if !m.ReplaceFile(replacePartition, replaceOldKey, FileInfo{Key: replaceNewKey, Size: 1, RowCount: 1}) {
		t.Fatal("a released key is swappable again")
	}
}

func TestPartitionForKey(t *testing.T) {
	m := manifestWithOneFile(t)

	p, ok := m.PartitionForKey(replaceOldKey)
	if !ok {
		t.Fatal("a known key must resolve to its partition")
	}
	if p != replacePartition {
		t.Fatalf("PartitionForKey = %q, want %q", p, replacePartition)
	}

	// Deriving the partition from the key string instead would guess; a guess
	// that disagrees with byKey silently orphans the entry, so an unknown key
	// must say so rather than return a plausible-looking answer.
	if _, ok := m.PartitionForKey("logs/dt=2026-08-01/hour=04/unknown.parquet"); ok {
		t.Error("an unknown key must report false")
	}
}

func TestRemoveFile_StillWorksAfterTheLockedSplit(t *testing.T) {
	m := manifestWithOneFile(t)

	// Removing a key that is not there is a no-op, not a panic or a counter
	// drift — the rewriter reaches this when a peer already removed the entry.
	m.RemoveFile(replacePartition, "logs/dt=2026-08-01/hour=04/absent.parquet")
	if got := m.TotalFiles(); got != 1 {
		t.Fatalf("TotalFiles = %d after removing an absent key, want 1", got)
	}

	m.RemoveFile(replacePartition, replaceOldKey)
	if m.HasKey(replaceOldKey) {
		t.Error("the key was not removed")
	}
	if got := m.TotalFiles(); got != 0 {
		t.Errorf("TotalFiles = %d, want 0", got)
	}
	if got := len(m.FilesForPartition(replacePartition)); got != 0 {
		t.Errorf("the emptied partition still holds %d entries", got)
	}
}
