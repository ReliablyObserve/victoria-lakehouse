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

func TestReplaceFile_AddsWhenTheOldKeyIsAbsent(t *testing.T) {
	m := manifestWithOneFile(t)

	// The rewriter can reach this when the source object was never manifested.
	// The replacement must still be registered — an unmanaged .parquet is
	// exactly what the orphan sweep deletes — and the caller must be told, so
	// it can count the inconsistency.
	if m.ReplaceFile(replacePartition, "logs/dt=2026-08-01/hour=04/never-existed.parquet",
		FileInfo{Key: replaceNewKey, Size: 1, RowCount: 1}) {
		t.Error("ReplaceFile must report false when there was nothing to replace")
	}
	if !m.HasKey(replaceNewKey) {
		t.Error("the replacement must be added even when the old key was absent")
	}
	if !m.HasKey(replaceOldKey) {
		t.Error("an unrelated entry must not be disturbed")
	}
	if got := m.TotalFiles(); got != 2 {
		t.Errorf("TotalFiles = %d, want 2", got)
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

func TestReplaceFile_RejectsADuplicateReplacementKey(t *testing.T) {
	m := manifestWithOneFile(t)
	m.AddFile(replacePartition, FileInfo{Key: replaceNewKey, Size: 5, RowCount: 1})

	// AddFile's idempotency guard still applies inside the swap: the old entry
	// goes, the already-present replacement is not duplicated.
	m.ReplaceFile(replacePartition, replaceOldKey, FileInfo{Key: replaceNewKey, Size: 5, RowCount: 1})

	if n := len(m.FilesForPartition(replacePartition)); n != 1 {
		t.Fatalf("partition holds %d entries, want 1", n)
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("TotalFiles = %d, want 1", got)
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
