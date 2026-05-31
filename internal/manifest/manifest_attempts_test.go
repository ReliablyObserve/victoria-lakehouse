package manifest

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// TestManifest_MarkAttempt_Records pins down the basic round-trip:
// MarkAttempt(p, t) then LastAttempt(p) must return t exactly.
//
// Negative-control proof: deleting the assignment in MarkAttempt
// causes LastAttempt to return the zero Time and this test to fail
// with "got zero time, want <ts>".
func TestManifest_MarkAttempt_Records(t *testing.T) {
	m := New("bkt", "logs/")
	ts := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	m.MarkAttempt("dt=2026-05-31/hour=12", ts)

	got := m.LastAttempt("dt=2026-05-31/hour=12")
	if !got.Equal(ts) {
		t.Fatalf("LastAttempt: got %v, want %v", got, ts)
	}
}

// TestManifest_LastAttempt_UnseenPartition_ZeroTime asserts the cold
// partition path: never-MarkAttempted partitions return zero time.
//
// Negative-control proof: if LastAttempt returned time.Now() on lookup
// miss, OrphanSweep Tier A would always see a "fresh" attempt and
// never steal from a stalled primary — this test would fail with a
// non-zero time.
func TestManifest_LastAttempt_UnseenPartition_ZeroTime(t *testing.T) {
	m := New("bkt", "logs/")
	got := m.LastAttempt("dt=2026-05-31/hour=09")
	if !got.IsZero() {
		t.Fatalf("LastAttempt for unseen partition: got %v, want zero", got)
	}
}

// TestManifest_AttemptsView_IncludesAllPartitions asserts the sweep
// data contract: cold partitions (no MarkAttempt) must appear in the
// view with zero time so Tier A can treat them as stale.
//
// Negative-control proof: if AttemptsView only returned partitions
// present in partitionAttempts, Tier A would never reclaim cold
// partitions abandoned by a crashed primary — this test would see
// len(view)==1 instead of 2.
func TestManifest_AttemptsView_IncludesAllPartitions(t *testing.T) {
	m := New("bkt", "logs/")
	m.AddFile("dt=2026-05-31/hour=00", FileInfo{Key: "logs/dt=2026-05-31/hour=00/a.parquet", Size: 1})
	m.AddFile("dt=2026-05-31/hour=01", FileInfo{Key: "logs/dt=2026-05-31/hour=01/b.parquet", Size: 1})

	m.MarkAttempt("dt=2026-05-31/hour=00", time.Now())

	view := m.AttemptsView()
	if len(view) != 2 {
		t.Fatalf("AttemptsView len: got %d, want 2", len(view))
	}
	if _, ok := view["dt=2026-05-31/hour=01"]; !ok {
		t.Fatalf("AttemptsView missing cold partition")
	}
	if !view["dt=2026-05-31/hour=01"].IsZero() {
		t.Fatalf("cold partition: want zero, got %v", view["dt=2026-05-31/hour=01"])
	}
}

// TestManifest_AttemptsView_IsSnapshot asserts mutating the returned
// map does not affect the manifest's internal state.
func TestManifest_AttemptsView_IsSnapshot(t *testing.T) {
	m := New("bkt", "logs/")
	m.AddFile("dt=2026-05-31/hour=00", FileInfo{Key: "logs/dt=2026-05-31/hour=00/a.parquet", Size: 1})
	m.MarkAttempt("dt=2026-05-31/hour=00", time.Date(2026, 5, 31, 1, 0, 0, 0, time.UTC))

	view := m.AttemptsView()
	view["dt=2026-05-31/hour=00"] = time.Time{}

	got := m.LastAttempt("dt=2026-05-31/hour=00")
	if got.IsZero() {
		t.Fatalf("AttemptsView returned a live reference; mutation leaked into manifest state")
	}
}

// TestManifest_AddFile_IdempotentOnKey is the load-bearing test for
// the safety backstop in §2.2.4. Two AddFile calls with identical key
// must yield exactly one entry.
//
// Negative-control proof: removing the for-loop scanning m.files[partition]
// for existing.Key == fi.Key causes this test to count 2 entries and
// fail with "got 2, want 1".
func TestManifest_AddFile_IdempotentOnKey(t *testing.T) {
	m := New("bkt", "logs/")
	fi := FileInfo{
		Key:             "logs/dt=2026-05-31/hour=00/compacted-L1-abc12345.parquet",
		Size:            1024,
		CompactionLevel: 1,
	}
	m.AddFile("dt=2026-05-31/hour=00", fi)
	m.AddFile("dt=2026-05-31/hour=00", fi)

	files := m.FilesForPartition("dt=2026-05-31/hour=00")
	if len(files) != 1 {
		t.Fatalf("AddFile idempotency: got %d entries, want 1", len(files))
	}
	if m.TotalFiles() != 1 {
		t.Fatalf("TotalFiles after duplicate AddFile: got %d, want 1", m.TotalFiles())
	}
	if m.TotalBytes() != 1024 {
		t.Fatalf("TotalBytes after duplicate AddFile: got %d, want 1024", m.TotalBytes())
	}
}

// TestManifest_AddFile_ConcurrentSameKeyOnlyOneEntry is the concurrent
// version of the idempotency contract. 100 goroutines call AddFile
// with the same key; the final count must be 1 and race detector must
// be clean.
//
// Negative-control proof: removing the mutex would surface a data race
// under -race; removing the dedupe loop would yield >1 final entries.
func TestManifest_AddFile_ConcurrentSameKeyOnlyOneEntry(t *testing.T) {
	m := New("bkt", "logs/")
	fi := FileInfo{
		Key:             "logs/dt=2026-05-31/hour=00/compacted-L1-deadbeef.parquet",
		Size:            42,
		CompactionLevel: 1,
	}

	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			m.AddFile("dt=2026-05-31/hour=00", fi)
		}()
	}
	wg.Wait()

	files := m.FilesForPartition("dt=2026-05-31/hour=00")
	if len(files) != 1 {
		t.Fatalf("concurrent AddFile idempotency: got %d entries, want 1", len(files))
	}
}

// TestManifest_KeysUnderPrefix_ReturnsMatching asserts OrphanSweep
// Tier B's input data: only keys with the given prefix come back.
//
// Negative-control proof: removing the strings.HasPrefix check would
// return every manifest key, and Tier B would consider unrelated keys
// as "in manifest" while scanning a date prefix — likely safe (no
// false delete) but defeats hash-bucket prefix ownership.
func TestManifest_KeysUnderPrefix_ReturnsMatching(t *testing.T) {
	m := New("bkt", "logs/")
	m.AddFile("dt=2026-05-31/hour=00", FileInfo{Key: "logs/acc/proj/logs/dt=2026-05-31/hour=00/a.parquet", Size: 1})
	m.AddFile("dt=2026-05-31/hour=00", FileInfo{Key: "logs/acc/proj/logs/dt=2026-05-31/hour=00/b.parquet", Size: 1})
	m.AddFile("dt=2026-06-01/hour=00", FileInfo{Key: "logs/acc/proj/logs/dt=2026-06-01/hour=00/c.parquet", Size: 1})

	got := m.KeysUnderPrefix("logs/acc/proj/logs/dt=2026-05-31/")
	if len(got) != 2 {
		t.Fatalf("KeysUnderPrefix: got %d keys, want 2; keys=%v", len(got), got)
	}
	for _, k := range got {
		if !strings.HasPrefix(k, "logs/acc/proj/logs/dt=2026-05-31/") {
			t.Errorf("KeysUnderPrefix returned %q outside requested prefix", k)
		}
	}
}

// TestManifest_KeysUnderPrefix_EmptyPrefix_ReturnsAll asserts the
// degenerate-prefix path returns every key.
//
// Negative-control proof: an incorrect emptyness check that treated
// "" as "match nothing" would fail this test with len(got)==0.
func TestManifest_KeysUnderPrefix_EmptyPrefix_ReturnsAll(t *testing.T) {
	m := New("bkt", "logs/")
	m.AddFile("dt=2026-05-31/hour=00", FileInfo{Key: "logs/a.parquet", Size: 1})
	m.AddFile("dt=2026-05-31/hour=00", FileInfo{Key: "logs/b.parquet", Size: 1})

	got := m.KeysUnderPrefix("")
	if len(got) != 2 {
		t.Fatalf("KeysUnderPrefix(\"\"): got %d, want 2", len(got))
	}
}

// TestManifest_PartitionAttempts_RaceFree is the concurrency contract
// for the watermark API. Reads via LastAttempt + AttemptsView and
// writes via MarkAttempt must be safe under -race.
//
// Negative-control proof: removing the m.mu Lock/Unlock around
// partitionAttempts surfaces a data race when run under
// `go test -race`.
func TestManifest_PartitionAttempts_RaceFree(t *testing.T) {
	m := New("bkt", "logs/")
	for i := 0; i < 10; i++ {
		m.AddFile("dt=2026-05-31/hour=00", FileInfo{
			Key:  "logs/dt=2026-05-31/hour=00/file-" + string(rune('a'+i)) + ".parquet",
			Size: 1,
		})
	}

	const N = 50
	var wg sync.WaitGroup
	wg.Add(3 * N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			m.MarkAttempt("dt=2026-05-31/hour=00", time.Now())
		}()
		go func() {
			defer wg.Done()
			_ = m.LastAttempt("dt=2026-05-31/hour=00")
		}()
		go func() {
			defer wg.Done()
			_ = m.AttemptsView()
		}()
	}
	wg.Wait()
}

// TestManifest_AddFile_DistinctKeysCoexist negative control: distinct
// UUID-suffixed compacted outputs must NOT be deduped — that's the
// whole point of compaction.go:165-166's uuid suffix. This protects
// against a too-aggressive idempotency check (e.g. matching on
// partition + level instead of full key).
func TestManifest_AddFile_DistinctKeysCoexist(t *testing.T) {
	m := New("bkt", "logs/")
	m.AddFile("dt=2026-05-31/hour=00", FileInfo{
		Key:             "logs/dt=2026-05-31/hour=00/compacted-L1-aaaa1111.parquet",
		Size:            10,
		CompactionLevel: 1,
	})
	m.AddFile("dt=2026-05-31/hour=00", FileInfo{
		Key:             "logs/dt=2026-05-31/hour=00/compacted-L1-bbbb2222.parquet",
		Size:            10,
		CompactionLevel: 1,
	})

	files := m.FilesForPartition("dt=2026-05-31/hour=00")
	if len(files) != 2 {
		t.Fatalf("distinct keys must coexist: got %d, want 2", len(files))
	}
}
