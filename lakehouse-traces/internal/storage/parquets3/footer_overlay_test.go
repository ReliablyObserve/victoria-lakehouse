package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// logsObject writes one production-shaped traces object (the name is kept
// identical to the logs twin of this test) and returns its bytes.
func logsObject(t testing.TB, rows, rgSize int) []byte {
	t.Helper()
	res, err := writeTracesParquet(traceRows(rows, time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)), rgSize, 3)
	if err != nil {
		t.Fatal(err)
	}
	return res.Data
}

func objectStripeStart(t testing.TB, data []byte) int64 {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return pageIndexStripeStart(f)
}

func footerTotal(data []byte) int {
	n, _ := FooterLength(data[len(data)-8:])
	return n + 8
}

func TestCacheFooterFromTail_OwnsFooterAndStripe(t *testing.T) {
	data := logsObject(t, 9000, 3000)
	size := int64(len(data))
	ps := objectStripeStart(t, data)
	if ps <= 0 {
		t.Fatal("fixture has no page index")
	}
	// A prefetch-sized region that covers footer + stripe + some data.
	regionOff := ps - 4096
	region := append([]byte(nil), data[regionOff:]...)

	cf, f, err := cacheFooterFromTail(context.Background(), nil, "k", region, regionOff, size)
	if err != nil {
		t.Fatal(err)
	}
	tail, tailOff := cf.Tail()
	if tailOff != ps || !bytes.Equal(tail, data[ps:]) {
		t.Fatalf("entry keeps [%d,+%d), want exactly the stripe+footer [%d,+%d)", tailOff, len(tail), ps, size-ps)
	}
	if cap(tail) != len(tail) {
		t.Fatalf("tail cap %d > len %d: the entry retains slack from the prefetch buffer", cap(tail), len(tail))
	}
	if !cf.HasPageIndex() {
		t.Fatal("HasPageIndex = false for an entry that holds the whole stripe")
	}
	// The entry owns its bytes: scribbling over the caller's buffer must not
	// change what the cache serves.
	for i := range region {
		region[i] = 0xEE
	}
	if !bytes.Equal(tail, data[ps:]) {
		t.Fatal("cache entry aliases the caller's buffer")
	}
	// The parsed handle reads its lazy page index through those same bytes,
	// not zeros: equal to the object's own index.
	ref, _ := parquet.OpenFile(bytes.NewReader(data), size)
	for rgi, rg := range f.RowGroups() {
		for ci, cc := range rg.ColumnChunks() {
			idx, ierr := cc.ColumnIndex()
			ridx, _ := ref.RowGroups()[rgi].ColumnChunks()[ci].ColumnIndex()
			if ierr != nil || idx.NumPages() != ridx.NumPages() {
				t.Fatalf("rg %d col %d: cached handle ColumnIndex mismatch (%v)", rgi, ci, ierr)
			}
			for p := 0; p < idx.NumPages(); p++ {
				if !bytes.Equal(idx.MinValue(p).Bytes(), ridx.MinValue(p).Bytes()) {
					t.Fatalf("rg %d col %d page %d: cached handle serves a different column index", rgi, ci, p)
				}
			}
		}
	}
}

func TestCacheFooterFromTail_StripeOutsideRegionFetchedOnce(t *testing.T) {
	data := logsObject(t, 9000, 3000)
	size := int64(len(data))
	ps := objectStripeStart(t, data)
	mock := newCountS3()
	defer mock.Close()
	mock.Put("test-bucket", "k", data)
	pool := testPool(t, mock.URL())

	// Region = footer only: the stripe lies before it.
	ft := int64(footerTotal(data))
	regionOff := size - ft
	cf, _, err := cacheFooterFromTail(context.Background(), pool, "k", append([]byte(nil), data[regionOff:]...), regionOff, size)
	if err != nil {
		t.Fatal(err)
	}
	reqs := mock.Requests()
	if len(reqs) != 1 || reqs[0].Off != ps || reqs[0].Len != regionOff-ps {
		t.Fatalf("want exactly one GET for the missing stripe [%d,+%d), got %+v", ps, regionOff-ps, reqs)
	}
	tail, tailOff := cf.Tail()
	if tailOff != ps || !bytes.Equal(tail, data[ps:]) || !cf.HasPageIndex() {
		t.Fatalf("stripe not kept after the extra GET: off=%d len=%d hasPI=%v", tailOff, len(tail), cf.HasPageIndex())
	}
}

// Cache-miss fallback: with no pool (or a failing extra GET) the entry still
// caches the footer; the page index is then read lazily as it always was.
func TestCacheFooterFromTail_FallsBackToFooterOnly(t *testing.T) {
	data := logsObject(t, 9000, 3000)
	size := int64(len(data))
	ft := int64(footerTotal(data))
	regionOff := size - ft

	check := func(name string, cf *CachedFooter, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		tail, tailOff := cf.Tail()
		if tailOff != regionOff || int64(len(tail)) != ft {
			t.Fatalf("%s: tail [%d,+%d), want footer only [%d,+%d)", name, tailOff, len(tail), regionOff, ft)
		}
		if cf.HasPageIndex() {
			t.Fatalf("%s: HasPageIndex = true although the stripe is not cached", name)
		}
	}
	cf, _, err := cacheFooterFromTail(context.Background(), nil, "k", append([]byte(nil), data[regionOff:]...), regionOff, size)
	check("no pool", cf, err)

	mock := newCountS3() // object missing -> the stripe GET fails
	defer mock.Close()
	cf, _, err = cacheFooterFromTail(context.Background(), testPool(t, mock.URL()), "k", append([]byte(nil), data[regionOff:]...), regionOff, size)
	check("failing stripe GET", cf, err)
}

func TestCacheFooterFromTail_RejectsInconsistentInput(t *testing.T) {
	data := logsObject(t, 3000, 1000)
	size := int64(len(data))
	ft := footerTotal(data)
	if _, _, err := cacheFooterFromTail(context.Background(), nil, "k", data[size-int64(ft):], 0, size); err == nil {
		t.Error("tail that does not end at the object size must be rejected")
	}
	if _, _, err := cacheFooterFromTail(context.Background(), nil, "k", data[size-int64(ft)+10:], size-int64(ft)+10, size); err == nil {
		t.Error("tail shorter than the footer must be rejected")
	}
	if _, _, err := cacheFooterFromTail(context.Background(), nil, "k", data[:100], size-100, size); err == nil {
		t.Error("tail without a parquet trailer must be rejected")
	}
}

// All writers keep the page-index stripe: the whole-file path
// (ParseFooterFromData), the direct parser, and the footer fetches.
func TestFooterWriters_AllKeepPageIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy fixture: runs in the heavy (non -short) job")
	}
	data := logsObject(t, 9000, 3000)
	size := int64(len(data))
	ps := objectStripeStart(t, data)

	t.Run("ParseFooterFromData", func(t *testing.T) {
		cf, _, err := ParseFooterFromData("k", data)
		if err != nil {
			t.Fatal(err)
		}
		tail, off := cf.Tail()
		if off != ps || !bytes.Equal(tail, data[ps:]) || !cf.HasPageIndex() {
			t.Fatalf("whole-file writer: tail off=%d len=%d hasPI=%v, want stripe at %d", off, len(tail), cf.HasPageIndex(), ps)
		}
	})
	t.Run("ParseFooterFromBytes", func(t *testing.T) {
		cf, _, err := ParseFooterFromBytes("k", data[ps:], size)
		if err != nil {
			t.Fatal(err)
		}
		if _, off := cf.Tail(); off != ps || !cf.HasPageIndex() {
			t.Fatalf("direct parse: off=%d hasPI=%v", off, cf.HasPageIndex())
		}
	})
	t.Run("fetchFooterFile", func(t *testing.T) {
		fx := newColdFixture(t, 1, 9000, 3000, config.ProjectedFetchModePlanned)
		if _, err := fx.s.fetchFooterFile(context.Background(), fx.files[0]); err != nil {
			t.Fatal(err)
		}
		cf, ok := fx.s.footerCache.Get(fx.files[0].Key)
		if !ok || !cf.HasPageIndex() {
			t.Fatal("fetchFooterFile entry lacks the page index")
		}
	})
	// A prefetch range too small for the footer forces the two-phase fetch. The
	// exact-footer read looks pageIndexLookBehind bytes behind the footer, so the
	// stripe arrives with it: two GETs. With no look-behind the stripe needs its
	// own GET: three. Either way the entry keeps the page index.
	for _, tc := range []struct {
		name   string
		look   int64
		wantGE int
	}{{"two-phase default look-behind", -1, 2}, {"two-phase look-behind", 32 << 10, 2}, {"two-phase no look-behind", 0, 3}} {
		t.Run("fetchFooterFile "+tc.name, func(t *testing.T) {
			if tc.look >= 0 { // -1 runs with the production default
				old := pageIndexLookBehind
				pageIndexLookBehind = tc.look
				defer func() { pageIndexLookBehind = old }()
			}
			fx := newColdFixture(t, 1, 9000, 3000, config.ProjectedFetchModePlanned)
			fx.s.cfg.S3.FooterPrefetchBytes = 4096
			before := len(fx.mock.Requests())
			if _, err := fx.s.fetchFooterFile(context.Background(), fx.files[0]); err != nil {
				t.Fatal(err)
			}
			cf, ok := fx.s.footerCache.Get(fx.files[0].Key)
			if !ok || !cf.HasPageIndex() {
				t.Fatal("two-phase entry lacks the page index")
			}
			tail, off := cf.Tail()
			if !bytes.Equal(tail, fx.datas[fx.files[0].Key][off:]) {
				t.Fatal("two-phase entry holds bytes that differ from the object")
			}
			if n := len(fx.mock.Requests()) - before; n != tc.wantGE {
				t.Fatalf("cold two-phase footer fetch made %d GETs, want %d", n, tc.wantGE)
			}
			// The stripe is cached with the footer: the next open, and every
			// page-index read after it, costs no request at all.
			after := len(fx.mock.Requests())
			if _, err := fx.s.fetchFooterFile(context.Background(), fx.files[0]); err != nil {
				t.Fatal(err)
			}
			for _, rg := range cf.File.RowGroups() {
				for _, cc := range rg.ColumnChunks() {
					if _, e := cc.ColumnIndex(); e != nil {
						t.Fatal(e)
					}
					if _, e := cc.OffsetIndex(); e != nil {
						t.Fatal(e)
					}
				}
			}
			if n := len(fx.mock.Requests()) - after; n != 0 {
				t.Fatalf("a cached oversize-footer entry made %d GETs for its page index, want 0", n)
			}
		})
	}
}

// A cache entry for another version of the key (different size) must never
// feed the overlay.
func TestWithFooterOverlay_RequiresKeyAndSizeMatch(t *testing.T) {
	fx := newColdFixture(t, 1, 6000, 2000, config.ProjectedFetchModePlanned)
	fx.prefetch(t)
	fi := fx.files[0]
	raw := fx.s.pool.NewReaderAt(context.Background(), fi.Key, fi.Size)
	opts := []parquet.FileOption{parquet.OptimisticRead(true)}

	miss0 := metrics.FooterOverlayOpens.Get("miss")
	hit0 := metrics.FooterOverlayOpens.Get("hit")
	got, gotOpts := fx.s.withFooterOverlay(fi, raw, opts)
	if _, ok := got.(interface{ TailOffset() int64 }); !ok || len(gotOpts) != len(opts)+2 {
		t.Fatalf("matching entry must overlay (%T, %d opts)", got, len(gotOpts))
	}
	if metrics.FooterOverlayOpens.Get("hit") != hit0+1 {
		t.Fatal("overlay hit not counted")
	}

	stale := fi
	stale.Size++ // same key, different object version
	got, gotOpts = fx.s.withFooterOverlay(stale, raw, opts)
	if got != raw || len(gotOpts) != len(opts) {
		t.Fatalf("size mismatch must not overlay (%T, %d opts)", got, len(gotOpts))
	}
	unknown := fi
	unknown.Key = "logs/other.parquet"
	if got, _ = fx.s.withFooterOverlay(unknown, raw, opts); got != raw {
		t.Fatal("uncached key must not overlay")
	}
	if metrics.FooterOverlayOpens.Get("miss") != miss0+2 {
		t.Fatal("overlay misses not counted")
	}
	fx.s.footerCache = nil
	if got, _ = fx.s.withFooterOverlay(fi, raw, opts); got != raw {
		t.Fatal("no footer cache must not overlay")
	}
}

// A footer-only entry (no stripe) still opens with zero GETs, but the page
// index is then read lazily: correct bytes, extra requests (the fallback).
func TestCachedFooter_FooterOnlyEntryFallsBackToLazyPageIndex(t *testing.T) {
	fx := newColdFixture(t, 1, 9000, 3000, config.ProjectedFetchModePlanned)
	fi := fx.files[0]
	data := fx.datas[fi.Key]
	cf, _, err := ParseFooterFromBytes(fi.Key, append([]byte(nil), data[len(data)-footerTotal(data):]...), fi.Size)
	if err != nil {
		t.Fatal(err)
	}
	fx.s.footerCache.Put(fi.Key, cf)
	if cf.HasPageIndex() {
		t.Fatal("footer-only entry claims a page index")
	}
	before := len(fx.mock.Requests())
	f, err := fx.s.openRangedParquet(context.Background(), fi, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(fx.mock.Requests()) - before; n != 0 {
		t.Fatalf("open with a cached footer made %d requests, want 0", n)
	}
	if _, err := f.RowGroups()[0].ColumnChunks()[0].ColumnIndex(); err != nil {
		t.Fatalf("lazy ColumnIndex: %v", err)
	}
	if n := len(fx.mock.Requests()) - before; n == 0 {
		t.Fatal("expected a lazy page-index GET for a footer-only entry")
	}
}

// Many goroutines opening and reading the same cached file at once share the
// read-only tail safely (run with -race) and all see zero open GETs.
func TestCachedFooter_ConcurrentOpensShareTail(t *testing.T) {
	fx := newColdFixture(t, 1, 9000, 3000, config.ProjectedFetchModePlanned)
	fx.prefetch(t)
	fi := fx.files[0]
	before := len(fx.mock.Requests())
	var wg sync.WaitGroup
	for g := 0; g < 24; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			var f *parquet.File
			var err error
			if g%2 == 0 {
				var view interface{ Close() error }
				var pf *parquet.File
				pf, v, oerr := fx.s.openPlannedParquet(context.Background(), fi, nil)
				view, f, err = v, pf, oerr
				if err == nil {
					defer func() { _ = view.Close() }()
				}
			} else {
				f, err = fx.s.openRangedParquet(context.Background(), fi, nil)
			}
			if err != nil {
				t.Errorf("open: %v", err)
				return
			}
			for _, rg := range f.RowGroups() {
				for _, cc := range rg.ColumnChunks() {
					if _, e := cc.ColumnIndex(); e != nil {
						t.Errorf("ColumnIndex: %v", e)
						return
					}
					if _, e := cc.OffsetIndex(); e != nil {
						t.Errorf("OffsetIndex: %v", e)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	if n := len(fx.mock.Requests()) - before; n != 0 {
		t.Fatalf("%d concurrent cached opens made %d S3 requests, want 0", 24, n)
	}
}

// The resident-size model must track the heap: it is what the byte budget
// charges, so a drift here silently turns the bound into fiction. Entries are
// measured with 1, 8 and 30+ row groups, fresh and after every ColumnIndex()
// and OffsetIndex() was decoded (parquet-go memoizes them on the *File, so a
// cached entry grows after Put). The model must never under-charge the heap
// and must stay within 1.5x of it.
func TestCachedFooterWeightCalibration(t *testing.T) {
	cases := []struct {
		name         string
		rows, rgSize int
	}{
		{"rg1", 3000, 10000},
		{"rg8", 16000, 2000},
		{"rg40", 60000, 1500},
	}
	for _, c := range cases {
		for _, decoded := range []bool{false, true} {
			state := "fresh"
			if decoded {
				state = "index-decoded"
			}
			t.Run(c.name+"/"+state, func(t *testing.T) {
				data := logsObject(t, c.rows, c.rgSize)
				size := int64(len(data))
				// The tail covers the whole page-index stripe, as a cache entry
				// built by the footer read does.
				tailStart := objectStripeStart(t, data)
				if tailStart < 0 {
					tailStart = size - int64(footerTotal(data))
				}
				region := data[tailStart:]
				// Heap deltas are noisy (leftover goroutines of earlier tests allocate;
				// the previous trial's entries may still be collected mid-trial), so take
				// the median of the positive trials of a helper whose frame is gone, and
				// with it the entries, before the next one starts.
				var trials []int64
				var sample *CachedFooter
				for trial := 0; trial < 5; trial++ {
					d, cf := measureFooterEntries(t, region, tailStart, size, 24, decoded)
					sample = cf
					if d > 0 {
						trials = append(trials, d)
					}
				}
				if len(trials) < 3 {
					t.Fatalf("only %d usable heap trials of 5", len(trials))
				}
				sort.Slice(trials, func(i, j int) bool { return trials[i] < trials[j] })
				per := trials[len(trials)/2]
				rgs := len(sample.File.RowGroups())
				w := sample.Weight()
				t.Logf("rowgroups=%d footer=%dB tail=%dB measured heap/entry=%dB model weight=%dB (ratio %.2f)", rgs, sample.footerSize, len(sample.tail), per, w, float64(w)/float64(per))
				if w < per || float64(w) > 1.5*float64(per) {
					t.Errorf("weight model %d B vs measured %d B per entry (%d row groups, %s): want measured <= model <= 1.5x measured", w, per, rgs, state)
				}
				runtime.KeepAlive(data)
			})
		}
	}
}

// measureFooterEntries builds n cache entries from the same tail and returns the
// heap growth per entry (after GC) and one entry. With decode set, every entry
// also decodes (and so memoizes) all its column and offset indexes. It is a
// function of its own so that all n entries are garbage when it returns.
func measureFooterEntries(t *testing.T, region []byte, tailStart, size int64, n int, decode bool) (int64, *CachedFooter) {
	runtime.GC()
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	keep := make([]*CachedFooter, 0, n)
	for i := 0; i < n; i++ {
		cf, _, err := cacheFooterFromTail(context.Background(), nil, "k", append([]byte(nil), region...), tailStart, size)
		if err != nil {
			t.Fatal(err)
		}
		if decode {
			for _, rg := range cf.File.RowGroups() {
				for _, cc := range rg.ColumnChunks() {
					if _, err := cc.ColumnIndex(); err != nil {
						t.Fatal(err)
					}
					if _, err := cc.OffsetIndex(); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		keep = append(keep, cf)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m1)
	runtime.KeepAlive(keep)
	return (int64(m1.HeapAlloc) - int64(m0.HeapAlloc)) / int64(n), keep[0]
}

// A trace_id point lookup (Jaeger and Tempo trace-by-ID, the log-to-trace
// click) takes the planned reader like every other projected query, and it
// needs fewer S3 requests than the window reader on the same warm footers:
// the page index of the cached footer prunes the row groups, so the plan
// fetches only the surviving chunks while the window reader reads ahead.
func TestPlannedDefault_TraceIDLookupUsesPlannedReader(t *testing.T) {
	rows := traceRows(2*6000, time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	tid := rows[1234].TraceID
	q := fmt.Sprintf(`trace_id:=%s | stats count() n`, tid)

	gets := func(mode string) (string, int, int64) {
		fx := newColdFixture(t, 2, 6000, 2000, mode)
		fx.prefetch(t)
		before := len(fx.mock.Requests())
		ans := fx.answer(t, q)
		var bytes int64
		reqs := fx.mock.Requests()[before:]
		for _, r := range reqs {
			bytes += r.Out
		}
		return ans, len(reqs), bytes
	}
	armed0 := metrics.S3PlannedStrategy.Get("plan-warm-footer")
	planAns, planGets, planBytes := gets(config.ProjectedFetchModePlanned)
	if metrics.S3PlannedStrategy.Get("plan-warm-footer") <= armed0 {
		t.Fatal("the trace_id lookup did not take the planned reader (no plan was armed over the warm footer)")
	}
	winAns, winGets, winBytes := gets(config.ProjectedFetchModeWindow)
	if planAns != "n=6" || winAns != planAns {
		t.Fatalf("trace_id lookup answers: planned %q window %q, want %q", planAns, winAns, "n=6")
	}
	t.Logf("trace_id lookup over warm footers: planned %d GETs / %d B, window %d GETs / %d B", planGets, planBytes, winGets, winBytes)
	if planGets > winGets || planBytes >= winBytes {
		t.Fatalf("planned trace_id lookup read %d GETs / %d B, the window reader %d GETs / %d B: it must read fewer bytes and no more GETs",
			planGets, planBytes, winGets, winBytes)
	}

	// The open itself arms a plan view for a trace_id projection.
	fx := newColdFixture(t, 2, 6000, 2000, config.ProjectedFetchModePlanned)
	fx.prefetch(t)
	f, view, err := fx.s.openParquetFileWithPlan(context.Background(), fx.files[0], map[string]bool{"trace_id": true})
	if err != nil || f == nil {
		t.Fatalf("planned open: %v", err)
	}
	if view == nil {
		t.Fatal("a trace_id projection must open on the planned reader")
	}
	_ = view.Close()
}

// A footer entry cached for another size of the same key is a miss: it is
// removed and the footer fetched again, so a stale schema never reaches the
// file open.
func TestOpenParquetFile_StaleSizeEntryIsAMiss(t *testing.T) {
	fx := newColdFixture(t, 2, 6000, 2000, config.ProjectedFetchModePlanned)
	fx.prefetch(t)
	a, b := fx.files[0], fx.files[1]
	if a.Size == b.Size {
		t.Skip("fixture objects have equal sizes")
	}
	stale, ok := fx.s.footerCache.Get(b.Key)
	if !ok {
		t.Fatal("no cached footer for the second object")
	}
	fx.s.footerCache.Put(a.Key, stale) // an entry of another object's size under a's key

	if _, ok := fx.s.footerCache.GetFor(a.Key, a.Size); ok {
		t.Fatal("GetFor returned an entry cached for another size")
	}
	if fx.s.footerCache.Has(a.Key) {
		t.Fatal("GetFor must remove the stale entry")
	}

	// Through the open path: re-insert the stale entry, open, and expect a
	// fresh footer for the right size to replace it (and GETs to have been made).
	fx.s.footerCache.Put(a.Key, stale)
	before := len(fx.mock.Requests())
	f, view, err := fx.s.openParquetFileWithPlan(context.Background(), a, map[string]bool{"level": true})
	if err != nil || f == nil {
		t.Fatalf("open: %v", err)
	}
	if view != nil {
		_ = view.Close()
	}
	if len(fx.mock.Requests()) == before {
		t.Fatal("a stale-size entry was served: the open made no request")
	}
	cur, ok := fx.s.footerCache.Get(a.Key)
	if !ok || cur.FileSize != a.Size {
		t.Fatalf("after the open the cache holds size %d (present=%v), want the current %d", func() int64 {
			if cur != nil {
				return cur.FileSize
			}
			return -1
		}(), ok, a.Size)
	}
	if f.Size() != a.Size {
		t.Fatalf("opened file size %d, want %d", f.Size(), a.Size)
	}
}
