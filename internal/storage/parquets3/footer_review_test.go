package parquets3

// Review-fix tests for the zero-GET open: the footer reader no longer invents
// bytes, the cache helpers (PutIfFits, parseObjectFor), the one-lookup overlay
// open, the caller-chosen stripe downloader, and the enrichment bounds read from
// a real page index.

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// A footer-only entry fails a page-index read with errOutsideCachedRange and
// memoizes nothing, so the same file answers correctly once the stripe is
// available. The zero-filling reader decoded an EMPTY index and parquet-go kept
// it on the shared file for the rest of the entry's life.
func TestFooterOnlyEntry_PageIndexReadFailsAndIsNotMemoized(t *testing.T) {
	data := logsObject(t, 9000, 3000)
	size := int64(len(data))
	ft := int64(footerTotal(data))
	cf, f, err := cacheFooterFromTail(context.Background(), nil, "k", append([]byte(nil), data[size-ft:]...), size-ft, size)
	if err != nil {
		t.Fatal(err)
	}
	if cf.HasPageIndex() || !cf.needsPageIndex() {
		t.Fatalf("footer-only entry: HasPageIndex=%v needsPageIndex=%v", cf.HasPageIndex(), cf.needsPageIndex())
	}
	col := f.RowGroups()[0].ColumnChunks()[0]
	for i := 0; i < 2; i++ { // twice: a failed read is not cached as an answer
		if idx, err := col.ColumnIndex(); !errors.Is(err, errOutsideCachedRange) || idx != nil {
			t.Fatalf("ColumnIndex on a footer-only entry = (%v, %v), want errOutsideCachedRange", idx, err)
		}
		if _, err := col.OffsetIndex(); !errors.Is(err, errOutsideCachedRange) {
			t.Fatalf("OffsetIndex on a footer-only entry: %v, want errOutsideCachedRange", err)
		}
	}
	// With the stripe cached the same calls succeed.
	ps := objectStripeStart(t, data)
	full, ff, err := cacheFooterFromTail(context.Background(), nil, "k", append([]byte(nil), data[ps:]...), ps, size)
	if err != nil {
		t.Fatal(err)
	}
	if !full.HasPageIndex() || full.needsPageIndex() {
		t.Fatal("entry with the stripe must report HasPageIndex")
	}
	idx, err := ff.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
	if err != nil || idx == nil || idx.NumPages() == 0 {
		t.Fatalf("ColumnIndex with the stripe cached = (%v, %v)", idx, err)
	}
}

func TestFooterCache_PutIfFits_NeverEvicts(t *testing.T) {
	data := logsObject(t, 3000, 1000)
	size := int64(len(data))
	ft := int64(footerTotal(data))
	mk := func(key string) *CachedFooter {
		cf, _, err := cacheFooterFromTail(context.Background(), nil, key, append([]byte(nil), data[size-ft:]...), size-ft, size)
		if err != nil {
			t.Fatal(err)
		}
		return cf
	}
	w := mk("a").Weight()
	fc := NewFooterCache(2*w + w/2)
	if !fc.PutIfFits("a", mk("a")) || !fc.PutIfFits("b", mk("b")) {
		t.Fatal("two entries must fit a 2.5-entry budget")
	}
	if fc.PutIfFits("c", mk("c")) {
		t.Error("a third entry must not fit")
	}
	if !fc.Has("a") || !fc.Has("b") || fc.Has("c") || fc.Len() != 2 {
		t.Errorf("PutIfFits evicted or inserted wrongly: keys=%v", fc.Keys())
	}
	if fc.Bytes() > fc.MaxBytes() {
		t.Errorf("cache over budget: %d > %d", fc.Bytes(), fc.MaxBytes())
	}
	// Replacing an existing key counts the old entry out first.
	if !fc.PutIfFits("a", mk("a")) || fc.Len() != 2 {
		t.Errorf("re-putting a key must replace it: len=%d", fc.Len())
	}
}

func TestParseObjectFor(t *testing.T) {
	data := logsObject(t, 3000, 1000)
	size := int64(len(data))
	ft := int64(footerTotal(data))
	ps := objectStripeStart(t, data)
	fc := NewFooterCache(0)

	cached, f, fresh, err := parseObjectFor(fc, "k", data)
	if err != nil || !fresh || cached == nil || f == nil {
		t.Fatalf("miss: fresh=%v err=%v", fresh, err)
	}
	if !cached.HasPageIndex() {
		t.Fatal("an entry built from the whole object must keep the page-index stripe")
	}
	fc.Put("k", cached)

	// Hit: the entry in the cache is returned as is, no replacement to Put.
	hitsBefore := metrics.FooterCacheHits.Get()
	again, f2, fresh, err := parseObjectFor(fc, "k", data)
	if err != nil || fresh || again != cached || f2 == nil {
		t.Fatalf("hit: fresh=%v same=%v err=%v", fresh, again == cached, err)
	}
	if metrics.FooterCacheHits.Get() != hitsBefore+1 {
		t.Error("a cache hit must count once")
	}
	// The returned file reads the object's bytes, not the entry's.
	if _, err := f2.RowGroups()[0].ColumnChunks()[0].ColumnIndex(); err != nil {
		t.Fatalf("handle over the object: %v", err)
	}

	// An entry without the stripe is replaced by the one built from the object.
	fo, _, err := cacheFooterFromTail(context.Background(), nil, "k", append([]byte(nil), data[size-ft:]...), size-ft, size)
	if err != nil {
		t.Fatal(err)
	}
	fc.Put("k", fo)
	if up, _, fresh, err := parseObjectFor(fc, "k", data); err != nil || !fresh || !up.HasPageIndex() {
		t.Fatalf("footer-only entry must be upgraded: fresh=%v hasPI=%v err=%v", fresh, up != nil && up.HasPageIndex(), err)
	}
	// A different size under the same key is another version: never reused.
	stale, _, err := cacheFooterFromTail(context.Background(), nil, "k", append([]byte(nil), data[ps:]...), ps, size)
	if err != nil {
		t.Fatal(err)
	}
	stale.FileSize++
	fc.Put("k", stale)
	if _, _, fresh, err := parseObjectFor(fc, "k", data); err != nil || !fresh {
		t.Fatalf("stale entry reused: fresh=%v err=%v", fresh, err)
	}
	// No cache: plain parse.
	if _, _, fresh, err := parseObjectFor(nil, "k", data); err != nil || !fresh {
		t.Fatalf("nil cache: fresh=%v err=%v", fresh, err)
	}
	if _, _, _, err := parseObjectFor(fc, "k", []byte("not parquet")); err == nil {
		t.Fatal("garbage must be rejected")
	}
}

// own() fetches a stripe that lies before the fetched region through the
// caller's downloader (the bounds resolution passes the context-bound one), not
// a fixed deduplicated pool read.
func TestOwn_StripeFetchUsesTheCallersDownloader(t *testing.T) {
	data := logsObject(t, 9000, 3000)
	size := int64(len(data))
	ps := objectStripeStart(t, data)
	ft := int64(footerTotal(data))

	var calls atomic.Int32
	var gotOff, gotLen int64
	dl := func(_ context.Context, _, key string, off, length int64) ([]byte, error) {
		calls.Add(1)
		gotOff, gotLen = off, length
		return append([]byte(nil), data[off:off+length]...), nil
	}
	cf, _, err := cacheFooterFromTail(context.Background(), dl, "k", append([]byte(nil), data[size-ft:]...), size-ft, size)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || gotOff != ps || gotLen != size-ft-ps {
		t.Fatalf("downloader calls=%d range=[%d,+%d), want one call for [%d,+%d)", calls.Load(), gotOff, gotLen, ps, size-ft-ps)
	}
	if !cf.HasPageIndex() {
		t.Fatal("stripe fetched through dl must be kept")
	}
	// A failing downloader leaves a footer-only entry, not an error.
	bad := func(context.Context, string, string, int64, int64) ([]byte, error) { return nil, errors.New("boom") }
	cf, _, err = cacheFooterFromTail(context.Background(), bad, "k", append([]byte(nil), data[size-ft:]...), size-ft, size)
	if err != nil || cf.HasPageIndex() {
		t.Fatalf("failed stripe fetch: err=%v hasPI=%v", err, cf != nil && cf.HasPageIndex())
	}
}

// One query-time open of a cached file looks the entry up once: the overlay used
// to Get it again after GetFor (a second FooterCacheHits, and a window for the
// entry to be evicted in between).
func TestOverlayOpen_CountsOneFooterCacheHit(t *testing.T) {
	fx := newColdFixture(t, 2, 3000, 1000, config.ProjectedFetchModePlanned)
	fx.prefetch(t)
	fi := fx.files[0]
	before := metrics.FooterCacheHits.Get()
	f, view, err := fx.s.openParquetFileWithPlan(context.Background(), fi, map[string]bool{"_msg": true, "_time": true})
	if err != nil {
		t.Fatal(err)
	}
	if view != nil {
		defer func() { _ = view.Close() }()
	}
	if f == nil {
		t.Fatal("nil file")
	}
	if got := metrics.FooterCacheHits.Get() - before; got != 1 {
		t.Errorf("one open counted %d footer-cache hits, want 1", got)
	}

	// withFooterOverlay given the entry does no lookup at all.
	cached, _ := fx.s.footerCache.Get(fi.Key)
	before = metrics.FooterCacheHits.Get()
	raw := fx.s.pool.NewReaderAt(context.Background(), fi.Key, fi.Size)
	got, _ := fx.s.withFooterOverlay(fi, raw, nil, cached)
	if got == raw {
		t.Error("an entry for this object must build an overlay")
	}
	if d := metrics.FooterCacheHits.Get() - before; d != 0 {
		t.Errorf("withFooterOverlay(entry) counted %d hits, want 0", d)
	}
	// An entry for another size builds none.
	other := *cached
	other.FileSize++
	if got, _ := fx.s.withFooterOverlay(fi, raw, nil, &other); got != raw {
		t.Error("an entry of another size must not build an overlay")
	}
}

// The manifest enrichment reads the page index of a cached footer now that the
// entries keep the stripe. The bounds it derives are the exact min/max of the
// timestamp column: equal to the footer's column statistics, tighter than the
// partition-hour inference they replace, and never excluding a row. A footer-only
// entry yields the row count and NO bounds (never a made-up range).
func TestEnrichFromCachedFooter_RealPageIndexBounds(t *testing.T) {
	data := logsObject(t, 9000, 3000)
	size := int64(len(data))
	ps := objectStripeStart(t, data)
	ft := int64(footerTotal(data))
	s := testStorage()
	tsCol := s.registry.TimestampColumn()

	whole, err := parquet.OpenFile(bytes.NewReader(data), size)
	if err != nil {
		t.Fatal(err)
	}
	wantRows, wantMin, wantMax, err := footerTimeBounds(whole, findColumnIndex(whole.Root(), tsCol))
	if err != nil {
		t.Fatal(err)
	}

	withStripe, _, err := cacheFooterFromTail(context.Background(), nil, "k", append([]byte(nil), data[ps:]...), ps, size)
	if err != nil {
		t.Fatal(err)
	}
	rows, minNs, maxNs := pageIndexTimeBounds(withStripe.File, tsCol)
	if rows != wantRows || minNs != wantMin || maxNs != wantMax {
		t.Fatalf("page-index bounds (%d rows, %d..%d) differ from the footer statistics (%d rows, %d..%d)", rows, minNs, maxNs, wantRows, wantMin, wantMax)
	}

	footerOnly, _, err := cacheFooterFromTail(context.Background(), nil, "k", append([]byte(nil), data[size-ft:]...), size-ft, size)
	if err != nil {
		t.Fatal(err)
	}
	rows, minNs, maxNs = pageIndexTimeBounds(footerOnly.File, tsCol)
	if rows != wantRows || minNs != 0 || maxNs != 0 {
		t.Fatalf("footer-only entry: (%d rows, %d..%d), want the row count and no bounds", rows, minNs, maxNs)
	}

	// Through the manifest: a file known only by its partition hour.
	key := "logs/dt=2026-06-01/hour=11/kf.parquet"
	hourStart := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	hourMin, hourMax := hourStart.UnixNano(), hourStart.Add(time.Hour).UnixNano()-1
	reset := func() {
		s.manifest = manifest.New("test", "logs/")
		s.manifest.AddFile(keyPartition(key), manifest.FileInfo{Key: key, Size: size, MinTimeNs: hourMin, MaxTimeNs: hourMax, BoundsInferred: true})
	}
	get := func() manifest.FileInfo {
		fi, ok := s.manifest.GetFileByKey(key)
		if !ok {
			t.Fatal("file missing from the manifest")
		}
		return fi
	}
	reset()
	if !s.enrichFromCachedFooter(manifest.FileInfo{Key: key, Size: size}, footerOnly) {
		t.Fatal("a footer-only entry still supplies the row count")
	}
	if fi := get(); fi.RowCount != wantRows || fi.MinTimeNs != hourMin || fi.MaxTimeNs != hourMax {
		t.Errorf("footer-only enrichment changed the bounds: %+v", fi)
	}
	reset()
	if !s.enrichFromCachedFooter(manifest.FileInfo{Key: key, Size: size}, withStripe) {
		t.Fatal("enrichment with the stripe failed")
	}
	fi := get()
	if fi.MinTimeNs != wantMin || fi.MaxTimeNs != wantMax {
		t.Errorf("manifest bounds %d..%d, want the exact %d..%d", fi.MinTimeNs, fi.MaxTimeNs, wantMin, wantMax)
	}
	if fi.MinTimeNs < hourMin || fi.MaxTimeNs > hourMax {
		t.Errorf("exact bounds %d..%d fall outside the partition-hour inference %d..%d: they must be tighter or equal", fi.MinTimeNs, fi.MaxTimeNs, hourMin, hourMax)
	}
	if fi.BoundsInferred {
		t.Error("exact bounds must clear the inferred flag")
	}
}
