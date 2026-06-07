package wal

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Concurrency contract note for these tests:
//
// The current WAL surface (Open / AppendLog / AppendTrace / Replay / Close /
// Truncate / Size / IsFull) carries NO internal mutex and exposes no Sync()
// method. It is a single-writer-at-a-time structure; the lifecycle layer
// that owns the WAL is expected to serialise Append calls externally. These
// race tests therefore exercise the realistic call pattern: many caller
// goroutines funnelling through a caller-side sync.Mutex (mirroring what
// the embedding code does), plus a separate observer goroutine driving Size
// queries (which read w.size — also documented as caller-serialised today).
//
// Where the test description in the spec referenced a "Sync periodically"
// goroutine, we use (*os.File).Sync() on the underlying fd via the
// same-package field access (read-only of w.file). This preserves the rule
// "do not modify WAL code". Sync is invoked under the same caller-side
// mutex used for Append, exactly as a real embedder would do.

// TestRace_AppendSyncReplay_Concurrent spins a writer goroutine and a syncer
// goroutine, both serialised by a caller-side mutex (the realistic embedding
// pattern), runs them for ~10 ms, then asserts that every entry the writer
// counted as successfully appended is recoverable via Replay after Close +
// reopen.
//
// The race detector must stay clean (no concurrent reads/writes of w.size,
// w.file, or the underlying fd). Durability contract: a successfully
// returned Append must survive crash recovery.
func TestRace_AppendSyncReplay_Concurrent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "race.wal")
	w, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var mu sync.Mutex
	var appended atomic.Int64
	done := make(chan struct{})

	// Writer goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := int64(0)
		for {
			select {
			case <-done:
				return
			default:
			}
			row := &schema.LogRow{
				TimestampUnixNano: i,
				Body:              "race-msg",
				ServiceName:       "svc",
			}
			mu.Lock()
			err := w.AppendLog(row)
			mu.Unlock()
			if err == nil {
				appended.Add(1)
			}
			i++
		}
	}()

	// Syncer goroutine — periodically fsync the underlying fd. Sync is
	// taken under the caller-side mutex so we never race with Append's
	// file.Write.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(500 * time.Microsecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				mu.Lock()
				_ = w.file.Sync()
				mu.Unlock()
			}
		}
	}()

	time.Sleep(10 * time.Millisecond)
	close(done)
	wg.Wait()

	// Final sync + close before reopen.
	mu.Lock()
	_ = w.file.Sync()
	mu.Unlock()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wantAppended := appended.Load()
	if wantAppended == 0 {
		t.Fatal("writer made zero progress in 10 ms — test is meaningless")
	}

	// Reopen and Replay from a separate goroutine to exercise the
	// "Replay from another goroutine" path requested in the spec. The
	// original writer goroutines have already returned (wg.Wait above),
	// so there is no live race; what we are validating is that the data
	// reaches durability.
	type replayResult struct {
		logs   int
		traces int
		err    error
	}
	out := make(chan replayResult, 1)
	go func() {
		w2, err := Open(path, 512*1024*1024)
		if err != nil {
			out <- replayResult{err: err}
			return
		}
		defer func() { _ = w2.Close() }()
		l, tr, err := w2.Replay()
		out <- replayResult{logs: len(l), traces: len(tr), err: err}
	}()

	res := <-out
	if res.err != nil {
		t.Fatalf("Replay: %v", res.err)
	}
	if int64(res.logs) != wantAppended {
		t.Fatalf("replayed %d logs, writer reported %d successfully appended (durability contract violated)",
			res.logs, wantAppended)
	}
	if res.traces != 0 {
		t.Errorf("replayed %d traces, want 0", res.traces)
	}
}

// TestRace_ManyWriters_NoSync runs 32 writer goroutines, each appending 1000
// entries, all funnelled through the caller-side mutex (the realistic
// embedding pattern; the WAL itself is not lock-free). One final Sync, one
// Replay. Total replayed count must equal cumulative writer successes.
//
// Race detector validation: the mutex around Append + the deferred reopen
// for Replay means no concurrent access to WAL internals. If a future WAL
// version grows an internal lock and exposes thread-safe Append, this test
// would still pass (the external mutex becomes redundant but not harmful).
func TestRace_ManyWriters_NoSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "many.wal")
	w, err := Open(path, 0) // unlimited
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const (
		writers       = 32
		entriesEach   = 1000
		totalExpected = writers * entriesEach
	)

	var mu sync.Mutex
	var successes atomic.Int64
	var wg sync.WaitGroup
	wg.Add(writers)
	for g := 0; g < writers; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < entriesEach; i++ {
				row := &schema.LogRow{
					TimestampUnixNano: int64(gid*entriesEach + i),
					Body:              "concurrent",
					ServiceName:       "svc",
				}
				mu.Lock()
				err := w.AppendLog(row)
				mu.Unlock()
				if err == nil {
					successes.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	mu.Lock()
	_ = w.file.Sync()
	mu.Unlock()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := successes.Load()
	if got != int64(totalExpected) {
		t.Fatalf("successes = %d, want %d", got, totalExpected)
	}

	w2, err := Open(path, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()
	logs, traces, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(logs) != totalExpected {
		t.Fatalf("replayed %d logs, want %d (durability contract violated)", len(logs), totalExpected)
	}
	if len(traces) != 0 {
		t.Errorf("replayed %d traces, want 0", len(traces))
	}
}

// TestRace_ReadDuringWrite is intentionally not implemented.
//
// The WAL package has no concurrent reader path: Write([]byte) and
// Reader() io.Reader are no-op stubs (see wal.go), and Replay performs a
// Seek(0)+sequential-read that mutates the file offset — it is not
// reentrant with concurrent Append on the same handle. The realistic
// reader path is "Close → reopen → Replay", which is already covered by
// TestRace_AppendSyncReplay_Concurrent and TestRace_ManyWriters_NoSync.
//
// If a future WAL revision adds a streaming Reader() or a snapshot-style
// Replay that does not mutate the file offset, this test should be filled
// in with a writer goroutine + reader goroutine racing on shared state.
func TestRace_ReadDuringWrite(t *testing.T) {
	t.Skip("no concurrent reader path on the WAL surface today; see file comment")
}
