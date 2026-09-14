package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// The footer cache is bounded by item count (10,000 by default), not by bytes,
// so an entry must never reference the object it was parsed from: N whole-object
// reads would otherwise pin N object bodies in RAM. Whole-object reads happen on
// every query that needs all columns, on cache warm-up and on the small-file
// metadata enrichment.
//
// Twin of internal/storage/parquets3/footer_cache_retention_test.go.

const retentionObjects = 12

// retentionFixture writes retentionObjects objects of ~1.5 MiB each.
func retentionFixture(t *testing.T) (*Storage, []manifest.FileInfo, int64) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	rng := rand.New(rand.NewSource(20260914))
	const partition = "dt=2026-06-01/hour=10"
	var files []manifest.FileInfo
	var objectBytes int64
	for i := 0; i < retentionObjects; i++ {
		rows := make([]logRow, 20000)
		for r := range rows {
			rows[r] = logRow{
				TimestampUnixNano: base.Add(time.Duration(r) * time.Millisecond).UnixNano(),
				// Incompressible bodies: the retention difference must be
				// visible in object bytes, not hidden by zstd.
				Body:         fmt.Sprintf("%x%x%x", rng.Uint64(), rng.Uint64(), rng.Uint64()),
				SeverityText: []string{"INFO", "WARN", "ERROR"}[r%3],
				ServiceName:  fmt.Sprintf("svc-%d", r%8),
			}
		}
		data := writeParquetToBytes(t, rows)
		key := fmt.Sprintf("logs/%s/obj-%d.parquet", partition, i)
		mock.putFile(key, data)
		fi := manifest.FileInfo{
			Key:       key,
			Size:      int64(len(data)),
			RowCount:  int64(len(rows)),
			MinTimeNs: rows[0].TimestampUnixNano,
			MaxTimeNs: rows[len(rows)-1].TimestampUnixNano,
		}
		s.manifest.AddFile(partition, fi)
		files = append(files, fi)
		objectBytes += fi.Size
	}
	if objectBytes < 8<<20 {
		t.Fatalf("fixture objects total %d bytes; the retention difference must be visible above heap noise", objectBytes)
	}
	return s, files, objectBytes
}

// heapInUse is the live heap after collection. Two cycles: the first frees the
// objects, the second the memory their finalizers released.
func heapInUse() int64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}

// TestFooterCache_WholeObjectReadKeepsOnlyTheFooter: after reading every object
// in full, the cache holds a footer-only entry per object and the objects
// themselves are collectable.
func TestFooterCache_WholeObjectReadKeepsOnlyTheFooter(t *testing.T) {
	s, files, objectBytes := retentionFixture(t)
	ctx := context.Background()

	for _, fi := range files {
		f, planned, err := s.openParquetFileWithPlan(ctx, fi, nil)
		if err != nil {
			t.Fatalf("open %s: %v", fi.Key, err)
		}
		if planned != nil {
			t.Fatalf("a whole-object open returned a planned range view for %s", fi.Key)
		}
		if n := f.NumRows(); n == 0 {
			t.Fatalf("%s decoded no rows", fi.Key)
		}
	}
	if s.footerCache.Len() != len(files) {
		t.Fatalf("footer cache holds %d entries, want %d", s.footerCache.Len(), len(files))
	}
	for _, fi := range files {
		cached, ok := s.footerCache.Get(fi.Key)
		if !ok {
			t.Fatalf("no cache entry for %s", fi.Key)
		}
		if cached.footerSize <= 0 {
			t.Errorf("%s: cache entry keeps no footer copy (footerSize=%d) — it references the object instead", fi.Key, cached.footerSize)
		}
		if int64(cached.footerSize) > fi.Size/8 {
			t.Errorf("%s: cache entry keeps %d bytes of a %d-byte object", fi.Key, cached.footerSize, fi.Size)
		}
	}

	t.Logf("objects=%d bytes=%d cache_entries=%d", len(files), objectBytes, s.footerCache.Len())
}

// TestParseFooterFromData_EntryDoesNotRetainTheObject measures the retention
// directly: build a cache entry per object, drop the objects and the per-read
// handles, and compare the live heap. An entry that references the object body
// keeps every byte of it alive.
func TestParseFooterFromData_EntryDoesNotRetainTheObject(t *testing.T) {
	rng := rand.New(rand.NewSource(20260914))
	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	newObject := func(i int) []byte {
		rows := make([]logRow, 20000)
		for r := range rows {
			rows[r] = logRow{
				TimestampUnixNano: base.Add(time.Duration(r) * time.Millisecond).UnixNano(),
				Body:              fmt.Sprintf("%x%x%x", rng.Uint64(), rng.Uint64(), rng.Uint64()),
				SeverityText:      "INFO",
				ServiceName:       fmt.Sprintf("svc-%d", r%8),
			}
		}
		return writeParquetToBytes(t, rows)
	}

	before := heapInUse()
	entries := make([]*CachedFooter, retentionObjects)
	var objectBytes int64
	for i := range entries {
		data := newObject(i)
		objectBytes += int64(len(data))
		cached, f, err := ParseFooterFromData(fmt.Sprintf("logs/obj-%d.parquet", i), data)
		if err != nil {
			t.Fatalf("ParseFooterFromData: %v", err)
		}
		if f.NumRows() != 20000 {
			t.Fatalf("handle over the object decoded %d rows", f.NumRows())
		}
		entries[i] = cached // the object bytes and the handle go out of scope here
	}
	retained := heapInUse() - before
	if retained < 0 {
		retained = 0
	}
	runtime.KeepAlive(entries)

	if objectBytes < 8<<20 {
		t.Fatalf("objects total %d bytes; too small to tell retention from heap noise", objectBytes)
	}
	if limit := objectBytes / 4; retained > limit {
		t.Errorf("%d cache entries built from %d bytes of objects retained %d bytes after GC, want less than %d",
			len(entries), objectBytes, retained, limit)
	}
	for i, cached := range entries {
		if cached.footerSize <= 0 || int64(cached.footerSize) > objectBytes/int64(len(entries))/8 {
			t.Errorf("entry %d keeps %d bytes, want a footer copy", i, cached.footerSize)
		}
	}
	t.Logf("objects=%d bytes=%d retained_after_gc=%d", len(entries), objectBytes, retained)
}

// TestFooterCache_FooterOnlyEntryStillServesEveryRow: a whole-object query that
// runs against a warm (footer-only) cache entry must still return every row —
// the entry is metadata, the rows come from the object.
func TestFooterCache_FooterOnlyEntryStillServesEveryRow(t *testing.T) {
	s, files, _ := retentionFixture(t)
	ctx := context.Background()
	fi := files[0]

	// Warm the cache the way a query does.
	if _, _, err := s.openParquetFileWithPlan(ctx, fi, nil); err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, ok := s.footerCache.Get(fi.Key); !ok {
		t.Fatal("first open left no cache entry")
	}

	q := mustParseQueryWithTime(t, "*", fi.MinTimeNs-1, fi.MaxTimeNs+1)
	rows := 0
	if err := s.RunQuery(ctx, []logstorage.TenantID{{}}, q, func(_ uint, db *logstorage.DataBlock) {
		rows += db.RowsCount()
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	var want int
	for _, f := range files {
		want += int(f.RowCount)
	}
	if rows != want {
		t.Fatalf("query over a warm footer cache returned %d rows, want %d", rows, want)
	}
}
