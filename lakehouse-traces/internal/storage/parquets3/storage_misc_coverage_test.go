package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// ---------------------------------------------------------------------------
// SetLocalBuffer / FooterCache accessor / cache adapters
// ---------------------------------------------------------------------------

type fakeLocalBuffer struct {
	closed   bool
	queryErr error
}

func (f *fakeLocalBuffer) RunQuery(_ *logstorage.QueryContext, _ logstorage.WriteDataBlockFunc) error {
	return f.queryErr
}
func (f *fakeLocalBuffer) Close() { f.closed = true }

// TestSetLocalBuffer_ClosedOnShutdown: the Option B buffer wired via
// SetLocalBuffer must be flushed+closed by Storage.Close so the last
// sub-FlushInterval window survives a graceful restart.
func TestSetLocalBuffer_ClosedOnShutdown(t *testing.T) {
	s := testStorage()
	lb := &fakeLocalBuffer{}
	s.SetLocalBuffer(lb)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !lb.closed {
		t.Fatal("Storage.Close must close the local buffer (recent-window data would be lost)")
	}
}

func TestFooterCacheAccessor(t *testing.T) {
	s := testStorage()
	if got := s.FooterCache(); got != nil {
		t.Errorf("expected nil footer cache when not configured, got %v", got)
	}
	fc := NewFooterCache(8)
	s.footerCache = fc
	if got := s.FooterCache(); got != fc {
		t.Error("FooterCache accessor must return the configured cache")
	}
}

// TestL1Adapter_PutNoCopy: the no-copy fast path must still serve Gets
// (smartcache relies on it for large file bodies).
func TestL1Adapter_PutNoCopy(t *testing.T) {
	a := &l1Adapter{lru: cache.NewLRU(1 << 20)}
	val := []byte("parquet-bytes")
	a.PutNoCopy("k1", val)
	got, ok := a.Get("k1")
	if !ok || !bytes.Equal(got, val) {
		t.Fatalf("Get after PutNoCopy = (%q, %v), want (%q, true)", got, ok, val)
	}
	a.Put("k2", []byte("copied"))
	if _, ok := a.Get("k2"); !ok {
		t.Fatal("Get after Put failed")
	}
}

// ---------------------------------------------------------------------------
// BufferBridge self-endpoint fallback
// ---------------------------------------------------------------------------

// TestBufferBridge_SelfEndpointFallback: single-node topology=all — with
// zero discovered peers, the bridge must query its OWN buffer endpoint;
// once real peers appear the self fallback is suppressed.
func TestBufferBridge_SelfEndpointFallback(t *testing.T) {
	b := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: time.Second,
	}, config.ModeLogs)

	if eps := b.getQueryEndpoints(); len(eps) != 0 {
		t.Fatalf("no endpoints and no self: expected none, got %v", eps)
	}

	b.SetSelfEndpoint("http://127.0.0.1:9428/internal/buffer/query")
	eps := b.getQueryEndpoints()
	if len(eps) != 1 || eps[0] != "http://127.0.0.1:9428/internal/buffer/query" {
		t.Fatalf("expected self-endpoint fallback, got %v", eps)
	}

	// Real peers override the self fallback (discovery is the source of truth).
	b.SetEndpoints([]string{"http://peer-1:9428", "http://peer-2:9428"})
	eps = b.getQueryEndpoints()
	if len(eps) != 2 || eps[0] == "http://127.0.0.1:9428/internal/buffer/query" {
		t.Fatalf("peers must suppress the self fallback, got %v", eps)
	}
}

// ---------------------------------------------------------------------------
// Label index S3 persist / load
// ---------------------------------------------------------------------------

// TestLabelIndexS3RoundTrip: persist on one storage, load+merge on a
// fresh one — the recovery path for pods that lost their local volume.
func TestLabelIndexS3RoundTrip(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	src := testStorageWithS3(t, mock.url())
	src.labelIndex.Add("service.name", []string{"api", "worker"})
	src.labelIndex.Add("env", []string{"prod"})
	if err := src.PersistLabelIndexToS3(context.Background()); err != nil {
		t.Fatalf("PersistLabelIndexToS3: %v", err)
	}

	dst := testStorageWithS3(t, mock.url())
	if dst.labelIndex.Len() != 0 {
		t.Fatal("precondition: fresh label index must be empty")
	}
	dst.loadLabelIndexFromS3(context.Background())
	if dst.labelIndex.Len() == 0 {
		t.Fatal("label index not recovered from S3")
	}
	vals := dst.labelIndex.GetFieldValues("service.name", 10)
	got := map[string]bool{}
	for _, v := range vals {
		got[v] = true
	}
	if !got["api"] || !got["worker"] {
		t.Errorf("service.name values = %v, want api+worker", vals)
	}
}

func TestPersistLabelIndexToS3_EmptyIndexIsNoOp(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	if err := s.PersistLabelIndexToS3(context.Background()); err != nil {
		t.Fatalf("empty index persist must be a nil no-op, got %v", err)
	}
	if _, ok := mock.files[s.labelIndexKey()]; ok {
		t.Error("no object must be written for an empty index")
	}
}

func TestLoadLabelIndexFromS3_MissingAndCorrupt(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	// Missing object: silent no-op (cold start).
	s.loadLabelIndexFromS3(context.Background())
	if s.labelIndex.Len() != 0 {
		t.Fatal("missing index object must leave the index empty")
	}

	// Corrupt payload: warn + no-op, never a panic or partial merge.
	mock.putFile(s.labelIndexKey(), []byte("{not-json"))
	s.loadLabelIndexFromS3(context.Background())
	if s.labelIndex.Len() != 0 {
		t.Fatal("corrupt index payload must not populate the index")
	}
}

// ---------------------------------------------------------------------------
// Footer prefetch
// ---------------------------------------------------------------------------

// bigParquetBytes builds a low-compressibility parquet file of at least
// minFileSizeForPrefetch bytes.
func bigParquetBytes(t *testing.T, seed int64) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	rows := make([]logRow, 2500)
	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC).UnixNano()
	for i := range rows {
		pad := make([]byte, 64)
		for j := range pad {
			pad[j] = byte('a' + rng.Intn(26))
		}
		rows[i] = logRow{
			TimestampUnixNano: base + int64(i),
			Body:              fmt.Sprintf("%s-%d", pad, rng.Int63()),
			SeverityText:      "INFO",
			ServiceName:       "svc-present",
		}
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[logRow](&buf, parquet.Compression(&parquet.Uncompressed))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	if int64(len(data)) < minFileSizeForPrefetch {
		t.Fatalf("fixture too small: %d < %d", len(data), minFileSizeForPrefetch)
	}
	return data
}

// TestPrefetchFootersByKeys_HydratesCache: snapshot keys that the
// manifest still knows get their footers range-read into the cache;
// keys the manifest no longer carries are skipped silently.
func TestPrefetchFootersByKeys_HydratesCache(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	key := "logs/dt=2026-06-01/hour=10/big.parquet"
	registerFileInMockS3(t, s, mock, key, bigParquetBytes(t, 1), base)

	s.PrefetchFootersByKeys(context.Background(),
		[]string{key, "logs/dt=2020-01-01/hour=00/expired.parquet"}, 2)

	if _, ok := s.footerCache.Get(key); !ok {
		t.Fatal("snapshot prefetch did not hydrate the footer cache")
	}

	// Empty key list and nil pool are safe no-ops.
	s.PrefetchFootersByKeys(context.Background(), nil, 2)
	s2 := testStorage()
	s2.footerCache = NewFooterCache(4)
	s2.PrefetchFootersByKeys(context.Background(), []string{"k"}, 2)
}

// TestPrefetchFooters_ErrorPaths: download failures (missing object)
// and parse failures (non-parquet bytes) must be counted and skipped —
// never fatal, never cached.
func TestPrefetchFooters_ErrorPaths(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	junk := make([]byte, minFileSizeForPrefetch+1024)
	for i := range junk {
		junk[i] = byte(i % 251)
	}
	mock.putFile("logs/dt=2026-06-01/hour=10/junk.parquet", junk)

	files := []manifest.FileInfo{
		{Key: "logs/dt=2026-06-01/hour=10/missing.parquet", Size: minFileSizeForPrefetch + 4096},
		{Key: "logs/dt=2026-06-01/hour=10/junk.parquet", Size: int64(len(junk))},
		{Key: "logs/dt=2026-06-01/hour=10/small.parquet", Size: 10}, // below threshold — skipped
	}
	fetched := prefetchFooters(context.Background(), s.pool, files, s.footerCache, 4, 0)
	if fetched != 0 {
		t.Errorf("no footer should be fetched from broken inputs, got %d", fetched)
	}
	if s.footerCache.Len() != 0 {
		t.Errorf("broken footers must not be cached, cache has %d entries", s.footerCache.Len())
	}

	// Guards: nil pool / nil cache / no files.
	if got := prefetchFooters(context.Background(), nil, files, s.footerCache, 4, 0); got != 0 {
		t.Errorf("nil pool must fetch nothing, got %d", got)
	}
	if got := prefetchFooters(context.Background(), s.pool, files, nil, 4, 0); got != 0 {
		t.Errorf("nil cache must fetch nothing, got %d", got)
	}
	if got := prefetchFooters(context.Background(), s.pool, nil, s.footerCache, 4, 0); got != 0 {
		t.Errorf("empty file list must fetch nothing, got %d", got)
	}
}

// TestPrefetchFooters_CancelledContext: cancellation mid-prefetch must
// abort workers promptly without caching partial state.
func TestPrefetchFooters_CancelledContext(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	data := bigParquetBytes(t, 2)
	files := make([]manifest.FileInfo, 8)
	for i := range files {
		key := fmt.Sprintf("logs/dt=2026-06-01/hour=10/c%02d.parquet", i)
		mock.putFile(key, data)
		files[i] = manifest.FileInfo{Key: key, Size: int64(len(data))}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fetched := prefetchFooters(ctx, s.pool, files, s.footerCache, 2, 0)
	if fetched != 0 {
		t.Errorf("cancelled prefetch must hydrate nothing, got %d", fetched)
	}
}

// ---------------------------------------------------------------------------
// shouldSkipByFooter
// ---------------------------------------------------------------------------

// TestShouldSkipByFooter walks the skip decision tree: only a parsed
// footer whose row-group stats EXCLUDE the pushdown filter may skip a
// file; every failure mode must fall back to "download and scan".
func TestShouldSkipByFooter(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	data := bigParquetBytes(t, 3) // every row has service.name = "svc-present"
	key := "logs/dt=2026-06-01/hour=10/skip.parquet"
	mock.putFile(key, data)
	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	ctx := context.Background()
	absentQ := `service.name:="zzz-absent"`

	t.Run("nil pool falls back to scan", func(t *testing.T) {
		skip, err := shouldSkipByFooter(ctx, nil, fi, absentQ, s.registry, s.footerCache, 0)
		if err != nil || skip {
			t.Errorf("got (skip=%v, err=%v), want (false, nil)", skip, err)
		}
	})

	t.Run("wildcard query cannot skip", func(t *testing.T) {
		skip, err := shouldSkipByFooter(ctx, s.pool, fi, "", s.registry, s.footerCache, 0)
		if err != nil || skip {
			t.Errorf("got (skip=%v, err=%v), want (false, nil)", skip, err)
		}
	})

	t.Run("small file is downloaded fully instead", func(t *testing.T) {
		small := manifest.FileInfo{Key: key, Size: 100}
		skip, err := shouldSkipByFooter(ctx, s.pool, small, absentQ, s.registry, s.footerCache, 0)
		if err != nil || skip {
			t.Errorf("got (skip=%v, err=%v), want (false, nil)", skip, err)
		}
	})

	t.Run("absent value never errors (conservative keep allowed)", func(t *testing.T) {
		// A footer-only parse (ParseFooterFromBytes with a synthetic
		// ReaderAt) cannot read the column-index sections that
		// rowGroupMatchesFilter consults — every check degrades to
		// "might match" and the file is conservatively kept. The
		// contract pinned here: never an error, never a WRONG skip.
		// (Same expectation as TestInteg_shouldSkipByFooter_NoMatch.)
		skip, err := shouldSkipByFooter(ctx, s.pool, fi, absentQ, s.registry, NewFooterCache(8), 0)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		_ = skip // false (conservative) today; true would also be sound for an absent value
	})

	t.Run("present value never skips and caches the footer", func(t *testing.T) {
		fc := NewFooterCache(8)
		skip, err := shouldSkipByFooter(ctx, s.pool, fi, `service.name:="svc-present"`, s.registry, fc, 0)
		if err != nil || skip {
			t.Fatalf("got (skip=%v, err=%v), want (false, nil)", skip, err)
		}
		if _, ok := fc.Get(key); !ok {
			t.Error("matching file's footer must be cached for the query path to reuse")
		}
		// Second call with a warm cache short-circuits (no benefit re-fetching).
		skip, err = shouldSkipByFooter(ctx, s.pool, fi, absentQ, s.registry, fc, 0)
		if err != nil || skip {
			t.Errorf("cached footer path: got (skip=%v, err=%v), want (false, nil)", skip, err)
		}
	})

	t.Run("download error falls back to scan", func(t *testing.T) {
		ghost := manifest.FileInfo{Key: "logs/dt=2026-06-01/hour=10/none.parquet", Size: int64(len(data))}
		skip, err := shouldSkipByFooter(ctx, s.pool, ghost, absentQ, s.registry, NewFooterCache(8), 0)
		if err != nil || skip {
			t.Errorf("got (skip=%v, err=%v), want (false, nil)", skip, err)
		}
	})

	t.Run("corrupt tail falls back to scan", func(t *testing.T) {
		junk := make([]byte, minFileSizeForPrefetch+512)
		mock.putFile("logs/dt=2026-06-01/hour=10/garbage.parquet", junk)
		bad := manifest.FileInfo{Key: "logs/dt=2026-06-01/hour=10/garbage.parquet", Size: int64(len(junk))}
		skip, err := shouldSkipByFooter(ctx, s.pool, bad, absentQ, s.registry, NewFooterCache(8), 0)
		if err != nil || skip {
			t.Errorf("got (skip=%v, err=%v), want (false, nil)", skip, err)
		}
	})
}

// ---------------------------------------------------------------------------
// ShadowExporter.Run / discovery + manifest refresh
// ---------------------------------------------------------------------------

// TestShadowExporter_RunStopsOnCancel: the shadow loop must exit
// promptly on context cancellation (shutdown path) without exporting.
func TestShadowExporter_RunStopsOnCancel(t *testing.T) {
	se := NewShadowExporter(&fakeLocalBuffer{}, nil, "shadow/", 1000, 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		// interval <= 0 exercises the default-interval guard; the
		// cancelled ctx wins before the first tick.
		se.Run(ctx, 0, func(_, _ int64) []logstorage.TenantID {
			t.Error("tenantsFn must not be called after cancellation")
			return nil
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ShadowExporter.Run did not stop on context cancellation")
	}
}

// TestRefreshDiscovery_NoPeersConfigured: with no peer cache or bridge,
// the refresh only polls storage-node discovery; empty DNS answers are
// not errors (single-node deployments).
func TestRefreshDiscovery_NoPeersConfigured(t *testing.T) {
	s := testStorage()
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatalf("RefreshDiscovery without peers: %v", err)
	}

	// With a bridge wired and no AZ awareness, discovered peers (none
	// here) are pushed to the bridge endpoints without error.
	s.bufferBridge = NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true}, config.ModeLogs)
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatalf("RefreshDiscovery with bridge: %v", err)
	}
}

// TestRefreshManifest_EmptyBucket: a refresh against an empty bucket
// must succeed (no files yet) and leave the manifest empty.
func TestRefreshManifest_EmptyBucket(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	if err := s.RefreshManifest(context.Background()); err != nil {
		t.Fatalf("RefreshManifest: %v", err)
	}
	if got := s.manifest.GetFilesForRange(0, 1<<62); len(got) != 0 {
		t.Errorf("empty bucket must yield an empty manifest, got %d files", len(got))
	}
}

func TestSetSelfAZ(t *testing.T) {
	s := testStorage()
	s.SetSelfAZ("us-east-1a")
	if got := s.SelfAZ(); got != "us-east-1a" {
		t.Errorf("SelfAZ = %q, want us-east-1a", got)
	}
}
