package parquets3

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetfooter"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
)

// The footer read of an object whose manifest entry records FooterTail (the
// bytes from the page-index stripe to the end of the object) is one exact ranged
// request; without it the read uses the prefetch window and, for a footer larger
// than the window, a second request for only the head the window missed.

func tailGETs(fx *coldFixture) (n int, bytes int64) {
	for _, r := range fx.mock.Requests() {
		if strings.HasPrefix(r.Op, "GET") && r.Class == "data" {
			n++
			bytes += r.Len
		}
	}
	return n, bytes
}

func withTail(fx *coldFixture, i int) manifest.FileInfo {
	fi := fx.files[i]
	fi.FooterTail = parquetfooter.Inspect(fx.datas[fi.Key]).Tail
	return fi
}

func TestFooterTail_WritersRecordIt(t *testing.T) {
	rows := traceRows(6000, time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	res, err := writeTracesParquet(rows, 2000, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := parquetfooter.Inspect(res.Data)
	if res.FooterTail <= 0 || res.FooterTail != want.Tail || res.BloomBytes != want.BloomBytes {
		t.Fatalf("flush result: FooterTail=%d BloomBytes=%d, the footer says %d and %d", res.FooterTail, res.BloomBytes, want.Tail, want.BloomBytes)
	}
	// It reaches exactly back to the first page-index section.
	if got, start := int64(len(res.Data))-int64(res.FooterTail), stripeStart(t, res.Data); start < 0 || got != start {
		t.Fatalf("tail starts at %d, the page-index stripe at %d", got, start)
	}
}

// The production drain publishes it in the manifest entry.
func TestSegments_DrainRecordsFooterTail(t *testing.T) {
	e := newSegEnv(t)
	e.ingestIDs(segTenantA, hourAgo.Add(10*time.Minute), "0af7651916cd43dd8448eb211c80319c", "4bf92f3577b34da6a3ce929d0e0bf736")
	e.seal()
	e.drainAll(e.flusher(1000))
	keys := e.storedDataKeys()
	if len(keys) == 0 {
		t.Fatal("nothing stored")
	}
	for _, k := range keys {
		e.u.mu.Lock()
		data := e.u.data[k]
		e.u.mu.Unlock()
		fi, ok := e.m.GetFileByKey(k)
		if !ok || fi.FooterTail <= 0 || fi.FooterTail != parquetfooter.Inspect(data).Tail {
			t.Errorf("%s: manifest FooterTail=%d (found=%v), the footer says %d", k, fi.FooterTail, ok, parquetfooter.Inspect(data).Tail)
		}
	}
}

func TestFooterTail_ExactRecordedReadIsOneRequest(t *testing.T) {
	fx := newColdFixture(t, 1, 9000, 3000, config.ProjectedFetchModePlanned)
	fi := withTail(fx, 0)
	window := footerPrefetchTail(fx.s.footerPrefetchBytes(), fi.Size)
	if int64(fi.FooterTail) <= window {
		t.Fatalf("fixture: the span objects' footer tail (%d, the trace index makes it large) must exceed the window (%d)", fi.FooterTail, window)
	}
	fx.mock.Reset()
	cached, err := fx.s.fetchFooterEntry(context.Background(), fi)
	if err != nil {
		t.Fatal(err)
	}
	reqs := fx.mock.Requests()
	if len(reqs) != 1 || reqs[0].Off != fi.Size-int64(fi.FooterTail) || reqs[0].Len != int64(fi.FooterTail) {
		t.Fatalf("requests = %+v, want one of the last %d bytes", reqs, fi.FooterTail)
	}
	if !cached.HasPageIndex() {
		t.Fatal("the exact read must bring the page-index stripe with the footer")
	}
	if tail, off := cached.Tail(); string(tail) != string(fx.datas[fi.Key][off:]) || cached.TailLen() != fi.FooterTail {
		t.Fatalf("cached tail [%d,+%d) / TailLen %d differs from the recorded %d", off, len(tail), cached.TailLen(), fi.FooterTail)
	}
	// The page index is served from memory: no further request.
	for _, rg := range cached.File.RowGroups() {
		for _, cc := range rg.ColumnChunks() {
			if _, e := cc.ColumnIndex(); e != nil {
				t.Fatal(e)
			}
		}
	}
	if n := len(fx.mock.Requests()); n != 1 {
		t.Fatalf("reading the page index made %d requests, want none", n-1)
	}

	// The same object without the record: the window, then the head it missed.
	// Two requests, and the second does not re-read the first's bytes.
	fx.mock.Reset()
	fx.s.footerCache.Remove(fi.Key)
	if _, err := fx.s.fetchFooterEntry(context.Background(), fx.files[0]); err != nil {
		t.Fatal(err)
	}
	reqs = fx.mock.Requests()
	if len(reqs) != 2 || reqs[0].Len != window || reqs[1].Off+reqs[1].Len != reqs[0].Off {
		t.Fatalf("unrecorded read = %+v, want the %d-byte window then only the head before it", reqs, window)
	}
	if reqs[0].Len+reqs[1].Len > int64(fi.FooterTail)+parquetfooter.LookBehind {
		t.Fatalf("unrecorded read fetched %d bytes for a %d-byte tail", reqs[0].Len+reqs[1].Len, fi.FooterTail)
	}
}

// The record is only a hint: whatever it holds, the footer that comes back is the
// object's own, the answer is unchanged, and the manifest ends up with the truth.
func TestFooterTail_AnyRecordedValueIsHarmless(t *testing.T) {
	base := newColdFixture(t, 2, 9000, 3000, config.ProjectedFetchModePlanned)
	q := `duration:>800ms | stats count() n`
	baseline := base.answer(t, q)
	truth := make([]int32, len(base.files))
	for i, fi := range base.files {
		truth[i] = parquetfooter.Inspect(base.datas[fi.Key]).Tail
	}

	for _, tc := range []struct {
		name string
		val  func(size int64, truth int32) int32
	}{
		{"one byte", func(int64, int32) int32 { return 1 }},
		{"the trailer alone", func(int64, int32) int32 { return 8 }},
		{"one short of the truth", func(_ int64, tr int32) int32 { return tr - 1 }},
		{"half the truth", func(_ int64, tr int32) int32 { return tr / 2 }},
		{"longer than the truth", func(_ int64, tr int32) int32 { return tr + 50_000 }},
		{"the whole object", func(size int64, _ int32) int32 { return int32(size) }},
		{"larger than the object", func(size int64, _ int32) int32 { return int32(size + 1) }},
		{"max int32", func(int64, int32) int32 { return math.MaxInt32 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newColdFixture(t, 2, 9000, 3000, config.ProjectedFetchModePlanned)
			for i, fi := range fx.files {
				fx.s.manifest.SetFooterTail(fi.Key, tc.val(fi.Size, truth[i]))
			}
			if got := fx.answer(t, q); got != baseline {
				t.Fatalf("answer differs with a recorded tail of %q:\n got %s\nwant %s", tc.name, got, baseline)
			}
			for i, fi := range fx.files {
				if got, _ := fx.s.manifest.GetFileByKey(fi.Key); got.FooterTail != truth[i] {
					t.Errorf("%s: manifest FooterTail=%d after the query, want the learned %d", fi.Key, got.FooterTail, truth[i])
				}
			}
		})
	}
}

// An object the manifest knows without the length (adopted from a listing, an
// older snapshot) gets it from its first footer read.
func TestFooterTail_LearnedFromTheFirstRead(t *testing.T) {
	fx := newColdFixture(t, 3, 9000, 3000, config.ProjectedFetchModePlanned)
	for _, fi := range fx.files {
		if got, _ := fx.s.manifest.GetFileByKey(fi.Key); got.FooterTail != 0 {
			t.Fatalf("fixture entry already has FooterTail %d", got.FooterTail)
		}
	}
	// 0: the single-footer read.
	if _, err := fx.s.fetchFooterFile(context.Background(), fx.files[0]); err != nil {
		t.Fatal(err)
	}
	// 1: the footer fetch again, for another object. (These footers, the trace
	// index in them, are larger than the prefetch budget, so the batch prefetch
	// skips them; the logs twin covers its learn hook.)
	if _, err := fx.s.fetchFooterFile(context.Background(), fx.files[1]); err != nil {
		t.Fatal(err)
	}
	// 2: a footer that is already cached is still learned by the prefetch visit.
	if _, err := fx.s.fetchFooterFile(context.Background(), fx.files[2]); err != nil { // cached now
		t.Fatal(err)
	}
	fx.s.manifest.SetFooterTail(fx.files[2].Key, 12345) // wrong
	prefetchFootersOpts(context.Background(), fx.s.pool, fx.files[2:3], fx.s.footerCache, 1, fx.s.footerPrefetchBytes(), prefetchOpts{learn: fx.s.learnFooterTail})
	for _, fi := range fx.files {
		want := parquetfooter.Inspect(fx.datas[fi.Key]).Tail
		if got, _ := fx.s.manifest.GetFileByKey(fi.Key); got.FooterTail != want {
			t.Errorf("%s: FooterTail=%d, want %d", fi.Key, got.FooterTail, want)
		}
	}
}

// End to end: a cold query over objects whose manifest entries record the tail
// reads the same rows with no more footer requests and no more bytes than one
// without, and fewer bytes when the footer is smaller than the window.
func TestFooterTail_ColdQueryReadsFewerFooterBytes(t *testing.T) {
	q := `duration:>800ms | stats count() n`
	run := func(record bool) (string, int, int64) {
		fx := newColdFixture(t, 3, 9000, 3000, config.ProjectedFetchModePlanned)
		if record {
			for i, fi := range fx.files {
				fx.s.manifest.SetFooterTail(fi.Key, withTail(fx, i).FooterTail)
			}
		}
		fx.mock.Reset()
		ans := fx.answer(t, q)
		n, b := tailGETs(fx)
		return ans, n, b
	}
	ansPlain, nPlain, bPlain := run(false)
	ansRec, nRec, bRec := run(true)
	if !strings.HasPrefix(ansPlain, "n=") || ansPlain == "n=0" {
		t.Fatalf("unexpected answer %q (the fixture must match the query)", ansPlain)
	}
	if ansPlain != ansRec {
		t.Fatalf("answers differ:\n%s\n%s", ansPlain, ansRec)
	}
	t.Logf("cold query footer-phase reads: unrecorded %d GETs %d bytes; recorded %d GETs %d bytes", nPlain, bPlain, nRec, bRec)
	if nRec > nPlain || bRec >= bPlain {
		t.Fatalf("recorded: %d GETs %d bytes; unrecorded: %d GETs %d bytes; want no more requests and fewer bytes", nRec, bRec, nPlain, bPlain)
	}
}

// Every place that carries FileInfo.TraceIDHex between processes carries the tail.
func TestFooterTail_CarriedByTheMetadataStores(t *testing.T) {
	const part, key = "dt=2026-05-10/hour=14", "logs/dt=2026-05-10/hour=14/meta.parquet"
	t.Run("file metadata on disk", func(t *testing.T) {
		s := testStorage()
		p, err := cache.NewPersister(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		s.persister = p
		now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
		s.manifest.AddFile(part, manifest.FileInfo{Key: key, Size: 1000, RowCount: 100, MinTimeNs: now.UnixNano(), MaxTimeNs: now.Add(time.Hour).UnixNano(), FooterTail: 65_000})
		s.saveFileMetadataToDisk()

		s2 := testStorage()
		s2.persister = p
		s2.manifest.AddFile(part, manifest.FileInfo{Key: key, Size: 1000})
		s2.loadFileMetadataFromDisk()
		if got, _ := s2.manifest.GetFileByKey(key); got.FooterTail != 65_000 {
			t.Fatalf("FooterTail after the disk round trip = %d, want 65000", got.FooterTail)
		}
	})
	t.Run("pmeta file-meta facet", func(t *testing.T) {
		store := pmeta.NewStore()
		store.Register(pmeta.FacetFileMeta, pmeta.NewFileMetaFactory())
		obs := &catalogObserver{store: store}
		obs.OnFileFlush(manifest.ExtractTenantPartition(key), manifest.FileInfo{Key: key, RowCount: 1, FooterTail: 77_000}, nil, nil)
		fm, ok := catalogFileMetaProvider{store: store}.FileMeta("", key)
		if !ok || fm.FooterTail != 77_000 {
			t.Fatalf("provider = %+v ok=%v, want FooterTail 77000", fm, ok)
		}
		var fi manifest.FileInfo
		fm.ApplyTo(&fi)
		if fi.FooterTail != 77_000 {
			t.Fatalf("ApplyTo: %d", fi.FooterTail)
		}
	})
}

// An object that is mostly footer is not probed ahead of time: past
// prefetchBudget (max(64 KiB, size/8)) the batch prefetch reads nothing, and
// whatever query opens the object reads its footer exactly then. (Before, the
// prefetch spent a window read on every such object and then dropped it.)
func TestFooterTail_PrefetchSkipsFootersOverTheBudget(t *testing.T) {
	fx := newColdFixture(t, 2, 9000, 3000, config.ProjectedFetchModePlanned)
	files := append([]manifest.FileInfo(nil), fx.files...)
	for i := range files {
		files[i].FooterTail = int32(prefetchBudget(files[i].Size) + 1) // a recorded tail over the budget
	}
	// The trace index makes these footers larger than max(64 KiB, size/8) for real:
	// without the record, the window read finds out and stops there.
	{
		fx2 := newColdFixture(t, 1, 9000, 3000, config.ProjectedFetchModePlanned)
		fx2.mock.Reset()
		if n := prefetchFooters(context.Background(), fx2.s.pool, fx2.files, fx2.s.footerCache, 1, fx2.s.footerPrefetchBytes()); n != 0 {
			t.Fatalf("unrecorded: prefetch cached %d footers over the budget", n)
		}
		if n, b := tailGETs(fx2); n != 1 || b != footerPrefetchTail(fx2.s.footerPrefetchBytes(), fx2.files[0].Size) {
			t.Fatalf("unrecorded: %d requests of %d bytes, want one window read", n, b)
		}
	}
	fx.mock.Reset()
	if n := prefetchFooters(context.Background(), fx.s.pool, files, fx.s.footerCache, 2, fx.s.footerPrefetchBytes()); n != 0 {
		t.Fatalf("prefetch cached %d footers over the budget", n)
	}
	if n, _ := tailGETs(fx); n != 0 {
		t.Fatalf("%d requests for footers over the budget, want none: %+v", n, fx.mock.Requests())
	}
	for _, fi := range files {
		if _, ok := fx.s.footerCache.Get(fi.Key); ok {
			t.Fatalf("%s cached", fi.Key)
		}
	}
	// The open path has no budget: it reads the footer when a query needs it.
	fx.mock.Reset()
	if _, err := fx.s.fetchFooterEntry(context.Background(), fx.files[0]); err != nil {
		t.Fatal(err)
	}
	if n, _ := tailGETs(fx); n == 0 {
		t.Fatal("the open path read nothing")
	}
}
