package parquets3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
)

// closePmetaPrefix is the signal directory of this module's keys.
const closePmetaPrefix = "logs/"

const (
	closePartA = "t/dt=2026-10-14/hour=10"
	closePartB = "t/dt=2026-10-14/hour=11"
)

// closeEnv is a viewEnv whose Storage has a pmeta catalog wired the way the binary wires it.
func closeEnv(t *testing.T) *viewEnv {
	t.Helper()
	e := newViewEnv(t)
	cs := newCatalogStore(config.PmetaConfig{Enabled: true}, closePmetaPrefix)
	e.s.catalog = cs
	e.s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	e.s.writer.catalogObserver = &catalogObserver{store: cs, pool: e.s.pool}
	e.s.localBuffer = nil // the env closes its segments itself
	return e
}

func (e *viewEnv) bundleKeys() []string {
	e.srv.mu.RLock()
	defer e.srv.mu.RUnlock()
	var keys []string
	for k := range e.srv.files {
		if strings.Contains(k, "_pmeta") {
			keys = append(keys, k)
		}
	}
	return keys
}

func closeContribution(partition, name string) pmeta.FileContribution {
	return pmeta.FileContribution{Partition: partition, FileKey: partition + "/" + name, RowCount: 5, MinTimeNs: 1, MaxTimeNs: 2}
}

// A bundle a compaction left dirty reaches the bucket when the process is closed: the flusher
// persists it on a tick, and a restart within one tick of a compaction used to leave the stored
// bundle as it was before (the merged-away files listed, the compacted file missing, so the
// compacted file came back without its metadata). Twin of the traces module's test.
func TestStorageClose_PersistsTheDirtyPmetaBundles(t *testing.T) {
	e := closeEnv(t)
	cs := e.s.catalog
	cs.OnFileFlush(closeContribution(closePartA, "compacted-L1-x.parquet"))
	cs.RemoveFiles(closePartA, []string{closePartA + "/a.parquet"})
	if len(e.bundleKeys()) != 0 {
		t.Fatal("a bundle reached the bucket before anything persisted it")
	}
	if err := e.s.Close(); err != nil {
		t.Fatal(err)
	}
	if len(e.bundleKeys()) == 0 {
		t.Fatal("Close left the dirty pmeta bundle unwritten: a restart right after a compaction loses it")
	}
}

// The scenario end to end: two files flushed and persisted, a compaction replaces them with one
// file (through the same hook the compactor calls), the process is closed before the flusher's next
// tick, and the next start loads the bundle from the bucket. The compacted file's entry is there and
// the merged-away files are not.
func TestStorageClose_RestartAfterCompactionKeepsTheCompactedFile(t *testing.T) {
	e := closeEnv(t)
	ctx := context.Background()
	a, b := closePartA+"/a.parquet", closePartA+"/b.parquet"
	e.s.catalog.OnFileFlush(closeContribution(closePartA, "a.parquet"))
	e.s.catalog.OnFileFlush(closeContribution(closePartA, "b.parquet"))
	e.s.writer.persistCatalog(ctx) // the flusher's tick after the flushes
	if len(e.bundleKeys()) != 1 {
		t.Fatalf("bundles after the flushes = %v, want one", e.bundleKeys())
	}

	c := manifest.FileInfo{Key: closePartA + "/compacted-L1-x.parquet", RowCount: 10, MinTimeNs: 1, MaxTimeNs: 2}
	e.s.PmetaOnCompacted([]manifest.FileInfo{c}, []string{a, b}, nil)
	// no tick before the restart
	if err := e.s.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := newCatalogStore(config.PmetaConfig{Enabled: true}, closePmetaPrefix)
	res := restarted.WarmPartitions(ctx, poolObjectStore{e.s.pool}, []string{closePartA}, 1)
	if len(res.NeedsRebuild) != 0 {
		t.Fatalf("the stored bundle needs a rebuild: %v", res.NeedsRebuild)
	}
	if _, ok := restarted.FileMeta(closePartA, c.Key); !ok {
		t.Error("the compacted file has no entry after the restart: the bundle stored before the compaction was loaded")
	}
	for _, k := range []string{a, b} {
		if _, ok := restarted.FileMeta(closePartA, k); ok {
			t.Errorf("merged-away file %s still has an entry after the restart", k)
		}
	}
}

// Close writes a bundle once: a second Close (or a Close with nothing dirty) puts nothing.
func TestStorageClose_PmetaWriteIsIdempotent(t *testing.T) {
	e := closeEnv(t)
	var puts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		e.srv.handler(w, r)
	}))
	t.Cleanup(stub.Close)
	e.s.pool = testPool(t, stub.URL)
	e.s.catalog.OnFileFlush(closeContribution(closePartA, "a.parquet"))
	e.s.writer.catalogObserver.pool = e.s.pool

	if err := e.s.Close(); err != nil {
		t.Fatal(err)
	}
	first := puts.Load()
	if first != 1 {
		t.Fatalf("PUTs by the first Close = %d, want 1 (one dirty partition)", first)
	}
	if err := e.s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := puts.Load(); got != first {
		t.Errorf("the second Close put %d more objects, want none: nothing was dirty", got-first)
	}
}

// With the object store unreachable (a PUT that never answers) Close gives up at
// shutdown.persist_timeout: it returns, does not panic, and the bundle stays dirty.
func TestStorageClose_PmetaWriteIsBoundedWhenTheObjectStoreHangs(t *testing.T) {
	e := closeEnv(t)
	release := make(chan struct{})
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); stub.Close() })
	e.s.pool = testPool(t, stub.URL)
	e.s.writer.catalogObserver.pool = e.s.pool
	e.s.cfg.Shutdown.PersistTimeout = 300 * time.Millisecond
	e.s.catalog.OnFileFlush(closeContribution(closePartA, "a.parquet"))

	start := time.Now()
	if err := e.s.Close(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Close took %v with the object store hanging, want about the 300ms persist timeout", took)
	}
	if got := e.s.catalog.DirtyPartitions(); len(got) != 1 {
		t.Errorf("dirty partitions after the failed write = %v, want the one that was not written", got)
	}
}

// One bundle that cannot be written does not keep the others from being written.
func TestStorageClose_AFailingBundleDoesNotLoseTheOthers(t *testing.T) {
	e := closeEnv(t)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "hour=10") {
			w.WriteHeader(http.StatusForbidden) // not retryable: fails at once
			return
		}
		e.srv.handler(w, r)
	}))
	t.Cleanup(stub.Close)
	e.s.pool = testPool(t, stub.URL)
	e.s.writer.catalogObserver.pool = e.s.pool
	e.s.catalog.OnFileFlush(closeContribution(closePartA, "a.parquet")) // hour=10: its PUT fails
	e.s.catalog.OnFileFlush(closeContribution(closePartB, "b.parquet")) // hour=11

	if err := e.s.Close(); err != nil {
		t.Fatal(err)
	}
	keys := e.bundleKeys()
	if len(keys) != 1 || !strings.Contains(keys[0], "hour=11") {
		t.Errorf("bundles in the bucket = %v, want only the hour=11 one", keys)
	}
	if got := e.s.catalog.DirtyPartitions(); len(got) != 1 || got[0] != closePartA {
		t.Errorf("dirty partitions = %v, want only %s (the one that failed)", got, closePartA)
	}
}

// A contribution that arrives while the bundle is being written (a compaction finishing during the
// shutdown) is not in the bytes written, so its partition stays dirty and the write does not report
// it as stored: the dirty flag is cleared only up to the generation the encode saw.
func TestStorageClose_ContributionDuringThePutStaysDirty(t *testing.T) {
	e := closeEnv(t)
	var once atomic.Bool
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "_pmeta") && once.CompareAndSwap(false, true) {
			e.s.catalog.OnFileFlush(closeContribution(closePartA, "arrives-during-the-put.parquet"))
		}
		e.srv.handler(w, r)
	}))
	t.Cleanup(stub.Close)
	e.s.pool = testPool(t, stub.URL)
	e.s.writer.catalogObserver.pool = e.s.pool
	e.s.catalog.OnFileFlush(closeContribution(closePartA, "a.parquet"))

	if err := e.s.Close(); err != nil {
		t.Fatal(err)
	}
	if !once.Load() {
		t.Fatal("the bundle was not written")
	}
	if got := e.s.catalog.DirtyPartitions(); len(got) != 1 || got[0] != closePartA {
		t.Errorf("dirty partitions after a Close with a contribution in flight = %v, want [%s]", got, closePartA)
	}
}

// A Storage with no catalog (pmeta off) or no writer closes without touching the object store.
func TestStorageClose_WithoutCatalogOrWriter(t *testing.T) {
	e := newViewEnv(t)
	e.s.localBuffer = nil
	if err := e.s.Close(); err != nil { // pmeta off: the writer has no observer
		t.Fatal(err)
	}
	e.s.writer = nil
	if err := e.s.Close(); err != nil {
		t.Fatal(err)
	}
}
