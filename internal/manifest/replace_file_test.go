package manifest

import (
	"errors"
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

// UpsertFile is the writer's commit for an object it may write more than once
// under the same key (a retried buffer-flush window). A new key is an add.
func TestUpsertFile_NewKeyIsAnAdd(t *testing.T) {
	m := New("bucket", "")
	old, replaced, err := m.UpsertFile(replacePartition, FileInfo{Key: replaceNewKey, Size: 300, RowCount: 7, MinTimeNs: 1, MaxTimeNs: 2})
	if err != nil {
		t.Fatal(err)
	}
	if replaced {
		t.Fatal("a key the manifest has never seen must not report a replacement")
	}
	if old.Key != "" || old.RowCount != 0 {
		t.Errorf("no previous entry: got %+v", old)
	}
	if !m.HasKey(replaceNewKey) || m.TotalFiles() != 1 || m.TotalRows() != 7 || m.TotalBytes() != 300 {
		t.Errorf("after the add: files=%d rows=%d bytes=%d", m.TotalFiles(), m.TotalRows(), m.TotalBytes())
	}
}

// Writing the same key again replaces the entry: totals describe only the new
// object, the previous entry is returned, the key is still live (not retired)
// and observers see one remove and one add.
func TestUpsertFile_SameKeyReplacesTheEntry(t *testing.T) {
	m := manifestWithOneFile(t)
	var adds, removes []FileInfo
	m.SetChangeObserver(
		func(_ string, fi FileInfo) { adds = append(adds, fi) },
		func(_ string, fi FileInfo) { removes = append(removes, fi) },
	)

	old, replaced, err := m.UpsertFile(replacePartition, FileInfo{Key: replaceOldKey, Size: 1500, RowCount: 15, MinTimeNs: 100, MaxTimeNs: 950})
	if err != nil {
		t.Fatal(err)
	}
	if !replaced {
		t.Fatal("same key must report a replacement")
	}
	if old.Key != replaceOldKey || old.RowCount != 10 || old.Size != 1000 {
		t.Errorf("previous entry = %+v, want the original (10 rows, 1000 bytes)", old)
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("TotalFiles = %d, want 1 (a rewrite is not a new file)", got)
	}
	if got := m.TotalRows(); got != 15 {
		t.Errorf("TotalRows = %d, want only the new entry's 15", got)
	}
	if got := m.TotalBytes(); got != 1500 {
		t.Errorf("TotalBytes = %d, want only the new entry's 1500", got)
	}
	if !m.HasKey(replaceOldKey) {
		t.Error("the key is the live object and must stay in the manifest")
	}
	files := m.FilesForPartition(replacePartition)
	if len(files) != 1 || files[0].RowCount != 15 || files[0].MaxTimeNs != 950 {
		t.Errorf("partition entry = %+v, want the replacement", files)
	}
	if len(removes) != 1 || removes[0].RowCount != 10 || len(adds) != 1 || adds[0].RowCount != 15 {
		t.Errorf("observers: removes=%+v adds=%+v, want the old entry removed and the new one added", removes, adds)
	}

	// Repeating it with identical content is stable.
	if _, _, err := m.UpsertFile(replacePartition, FileInfo{Key: replaceOldKey, Size: 1500, RowCount: 15, MinTimeNs: 100, MaxTimeNs: 950}); err != nil {
		t.Fatal(err)
	}
	if m.TotalFiles() != 1 || m.TotalRows() != 15 || m.TotalBytes() != 1500 {
		t.Errorf("repeat: files=%d rows=%d bytes=%d", m.TotalFiles(), m.TotalRows(), m.TotalBytes())
	}
}

// UpsertFile does not change ReplaceFile: that one still swaps a different key
// in and refuses when the source is absent.
func TestUpsertFile_LeavesReplaceFileSemanticsAlone(t *testing.T) {
	m := manifestWithOneFile(t)
	if _, _, err := m.UpsertFile(replacePartition, FileInfo{Key: replaceOldKey, Size: 1200, RowCount: 12}); err != nil {
		t.Fatal(err)
	}
	if !m.ReplaceFile(replacePartition, replaceOldKey, FileInfo{Key: replaceNewKey, Size: 400, RowCount: 6}) {
		t.Fatal("ReplaceFile must still swap an upserted entry")
	}
	if m.HasKey(replaceOldKey) || !m.HasKey(replaceNewKey) || m.TotalFiles() != 1 || m.TotalRows() != 6 {
		t.Errorf("after ReplaceFile: files=%d rows=%d", m.TotalFiles(), m.TotalRows())
	}
	if m.ReplaceFile(replacePartition, "logs/dt=2026-08-01/hour=04/absent.parquet", FileInfo{Key: "x", RowCount: 1}) {
		t.Error("ReplaceFile must still refuse an absent source")
	}
}

// A key the manifest has retired (compacted, rewritten or removed) is refused:
// nothing changes and no observer fires, so a retried flush cannot bring back
// an object that was superseded.
func TestUpsertFile_RetiredKeyIsRefused(t *testing.T) {
	m := manifestWithOneFile(t)
	// Compaction publishes the merge output; the source is retired.
	if !m.ReplaceFiles(replacePartition, []string{replaceOldKey}, FileInfo{Key: replaceNewKey, Size: 900, RowCount: 10, MinTimeNs: 100, MaxTimeNs: 900}) {
		t.Fatal("setup: ReplaceFiles refused")
	}
	if !m.IsRetired(replaceOldKey) {
		t.Fatal("setup: the source is not retired")
	}
	var events int
	m.SetChangeObserver(func(string, FileInfo) { events++ }, func(string, FileInfo) { events++ })
	files, rows, bytes := m.TotalFiles(), m.TotalRows(), m.TotalBytes()

	old, replaced, err := m.UpsertFile(replacePartition, FileInfo{Key: replaceOldKey, Size: 1000, RowCount: 10, MinTimeNs: 100, MaxTimeNs: 900})
	if !errors.Is(err, ErrKeyRetired) {
		t.Fatalf("err = %v, want ErrKeyRetired", err)
	}
	if replaced || old.Key != "" {
		t.Errorf("a refused upsert reported replaced=%v old=%+v", replaced, old)
	}
	if m.HasKey(replaceOldKey) {
		t.Error("the retired key is live again")
	}
	if m.TotalFiles() != files || m.TotalRows() != rows || m.TotalBytes() != bytes {
		t.Errorf("totals changed: files %d->%d rows %d->%d bytes %d->%d", files, m.TotalFiles(), rows, m.TotalRows(), bytes, m.TotalBytes())
	}
	if events != 0 {
		t.Errorf("%d observer events for a refused upsert", events)
	}
	if !m.IsRetired(replaceOldKey) {
		t.Error("the key must stay retired")
	}
}
