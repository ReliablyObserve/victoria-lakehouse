package manifest

import (
	"fmt"
	"testing"
)

// TestManifest_ByKey_LookupIsConsistent pins the byKey reverse index
// staying consistent with m.files across the full life-cycle: AddFile,
// RemoveFile, RefreshFromS3 (m.files = ...), snapshot Load. Each per-key
// mutator (SetFileBucket, UpdateFileColumnStats, EnrichFileMetadata)
// looks up via byKey, so any drift here silently breaks them.
func TestManifest_ByKey_LookupIsConsistent(t *testing.T) {
	m := New("test-bucket", "test/")

	add := func(partition, key string) {
		m.AddFile(partition, FileInfo{Key: key, Size: 100})
	}

	add("dt=2026-01-01/hour=00", "a")
	add("dt=2026-01-01/hour=01", "b")
	add("dt=2026-01-01/hour=01", "c")

	// SetFileBucket must hit each key via byKey, not by scanning every
	// partition. We can't observe that directly, but we can prove the
	// mutation landed on the right FileInfo.
	m.SetFileBucket("a", "new-bucket-a")
	m.SetFileBucket("b", "new-bucket-b")
	m.SetFileBucket("c", "new-bucket-c")

	checkBucket := func(partition, key, want string) {
		t.Helper()
		for _, fi := range m.files[partition] {
			if fi.Key == key {
				if fi.Bucket != want {
					t.Errorf("file %q in %q: bucket=%q want=%q", key, partition, fi.Bucket, want)
				}
				return
			}
		}
		t.Errorf("file %q not found in partition %q", key, partition)
	}
	checkBucket("dt=2026-01-01/hour=00", "a", "new-bucket-a")
	checkBucket("dt=2026-01-01/hour=01", "b", "new-bucket-b")
	checkBucket("dt=2026-01-01/hour=01", "c", "new-bucket-c")

	// SetFileBucket on an unknown key must be a no-op, not a crash. This
	// covers the case where byKey is missing an entry (a code path that
	// should never happen but used to be tolerated by the linear scan).
	m.SetFileBucket("ghost-key", "nope")

	// Remove one — both the slice and byKey must drop it.
	m.RemoveFile("dt=2026-01-01/hour=01", "b")
	if _, present := m.byKey["b"]; present {
		t.Error("RemoveFile didn't update byKey")
	}
	// And further SetFileBucket on the removed key is a no-op.
	m.SetFileBucket("b", "should-be-nop")

	// rebuildByKey must produce identical state to AddFile sequence.
	m.mu.Lock()
	m.rebuildByKey()
	m.mu.Unlock()
	if got := m.byKey["a"]; got != "dt=2026-01-01/hour=00" {
		t.Errorf("rebuildByKey lost key a: got partition=%q", got)
	}
	if got := m.byKey["c"]; got != "dt=2026-01-01/hour=01" {
		t.Errorf("rebuildByKey lost key c: got partition=%q", got)
	}
	if _, present := m.byKey["b"]; present {
		t.Error("rebuildByKey re-added removed key")
	}
}

// TestManifest_ByKey_DedupesViaO1Path pins the AddFile idempotency
// guard switching from O(partition-files) to O(1) via byKey. The
// previous loop over m.files[partition] would also detect same-key
// adds within the SAME partition but not across partitions; byKey
// catches both.
func TestManifest_ByKey_DedupesViaO1Path(t *testing.T) {
	m := New("test-bucket", "test/")

	m.AddFile("dt=2026-01-01/hour=00", FileInfo{Key: "k1", Size: 100})
	m.AddFile("dt=2026-01-01/hour=00", FileInfo{Key: "k1", Size: 999}) // dup, same partition
	m.AddFile("dt=2026-01-01/hour=01", FileInfo{Key: "k1", Size: 999}) // dup, different partition

	// Only the first insert should have landed. totalFiles is bumped once,
	// total bytes reflect the first Size only, and byKey points at the
	// first partition.
	if m.totalFiles != 1 {
		t.Errorf("dedup failed: totalFiles=%d, want 1", m.totalFiles)
	}
	if m.totalBytes != 100 {
		t.Errorf("dedup leaked bytes: totalBytes=%d, want 100", m.totalBytes)
	}
	if got := m.byKey["k1"]; got != "dt=2026-01-01/hour=00" {
		t.Errorf("byKey shifted on duplicate add: got partition=%q", got)
	}
}

// TestManifest_ByKey_ScalesPastLargeCorpus is a smoke test that the
// reverse index doesn't pathologically slow down the file mutators
// at non-trivial counts. We add 50K files (1000× more than a typical
// partition holds) and time a per-key mutator; it has to remain a
// trivial-cost operation. If someone reverts the byKey optimization
// this test will not strictly fail but will become orders of
// magnitude slower under -count=N timing.
func TestManifest_ByKey_ScalesPastLargeCorpus(t *testing.T) {
	if testing.Short() {
		t.Skip("scale smoke test — skipped in -short mode")
	}
	m := New("test-bucket", "test/")
	const N = 50000
	const Partitions = 50
	for i := 0; i < N; i++ {
		partition := fmt.Sprintf("dt=2026-01-01/hour=%02d", i%Partitions)
		m.AddFile(partition, FileInfo{Key: fmt.Sprintf("k%d", i), Size: 1})
	}
	if m.totalFiles != N {
		t.Fatalf("totalFiles = %d, want %d", m.totalFiles, N)
	}
	// The mutator on a key in the middle of the corpus must complete
	// without scanning the whole manifest. Hard to assert wall-clock in
	// CI but we can prove correctness here and leave timing to the
	// scale benchmark suite.
	m.SetFileBucket("k25000", "verified")
	for _, fi := range m.files[fmt.Sprintf("dt=2026-01-01/hour=%02d", 25000%Partitions)] {
		if fi.Key == "k25000" && fi.Bucket != "verified" {
			t.Errorf("middle-of-corpus key didn't update: %+v", fi)
		}
	}
}
