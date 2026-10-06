package parquets3

// GET-count and answer-equivalence guards for the cold read path: a file whose
// footer is cached opens with ZERO S3 round trips, and a query over cached
// footers sends only data ranges to S3, in one wave per file.
//
// These tests use only behaviour (requests seen by the mock S3), so they also
// compile and FAIL against the tree before the footer-overlay change; that is
// the fail-before / pass-after proof.

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

type coldFixture struct {
	s      *Storage
	mock   *countS3
	files  []manifest.FileInfo
	datas  map[string][]byte
	anchor time.Time
}

// newColdFixture writes nFiles Parquet objects with the production writer
// (zstd, SBBF blooms, footer-KV token blooms, page index) and serves them from
// the request-logging mock S3 under a storage that uses production-shaped
// window knobs. rowsPerFile rows go to each file, rgSize rows per row group.
func newColdFixture(t *testing.T, nFiles, rowsPerFile, rgSize int, mode string) *coldFixture {
	t.Helper()
	mock := newCountS3()
	t.Cleanup(mock.Close)
	s := testStorageWithS3(t, mock.URL())
	s.cfg.S3.ReadAheadBytes = 2 << 20
	s.cfg.S3.ReadAheadMaxBytes = 8 << 20
	s.cfg.S3.CoalesceGapBytes = 1 << 20
	s.cfg.S3.ProjectedFetchMode = mode
	// Pin the ranged path: the whole-file warmup for small cold objects has
	// its own routing test.
	s.cfg.S3.WholeFileThresholdBytes = 1
	s.labelIndex.Add("service.name", nil)

	fx := &coldFixture{s: s, mock: mock, datas: map[string][]byte{}, anchor: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	rows := bigmarkRows(nFiles*rowsPerFile, fx.anchor)
	for i := 0; i < nFiles; i++ {
		lo, hi := i*len(rows)/nFiles, (i+1)*len(rows)/nFiles
		chunk := rows[lo:hi]
		res, err := writeLogsParquet(chunk, rgSize, 3)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("logs/dt=2026-06-01/hour=%02d/f%03d.parquet", 10+i%3, i)
		mock.Put("test-bucket", key, res.Data)
		fx.datas[key] = res.Data
		minT, maxT := schema.LogRowTimeBounds(chunk)
		fi := manifest.FileInfo{
			Key: key, Size: int64(len(res.Data)), RowCount: int64(len(chunk)), MinTimeNs: minT, MaxTimeNs: maxT,
			RawBytes: res.RawBytes, Labels: extractLogLabels(chunk), LabelAggregates: schema.ExtractLogLabelAggregates(chunk),
			ColumnBytes: res.ColumnBytes,
		}
		s.manifest.AddFile(partitionFromKey(key), fi)
		fx.files = append(fx.files, fi)
	}
	return fx
}

func (fx *coldFixture) window() (int64, int64) {
	return fx.anchor.Add(-6 * time.Hour).UnixNano(), fx.anchor.Add(time.Minute).UnixNano()
}

// answer runs q and returns a canonical fingerprint of every emitted row.
func (fx *coldFixture) answer(t *testing.T, q string) string {
	t.Helper()
	start, end := fx.window()
	query := mustParseQueryWithTime(t, q, start, end)
	// The same entry point as /select/logsql/query: VictoriaLogs' own pipeline
	// on top of the external storage.
	internalvlstorage.SetStorage(fx.s, nil)
	defer vlapp.SetExternalStorage(nil)
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, query, false, nil)
	var mu sync.Mutex
	var lines []string
	if err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		cols := db.GetColumns(false)
		for i := 0; i < db.RowsCount(); i++ {
			var parts []string
			for _, c := range cols {
				parts = append(parts, c.Name+"="+c.Values[i])
			}
			sort.Strings(parts)
			lines = append(lines, strings.Join(parts, ","))
		}
	}); err != nil {
		t.Fatalf("RunQuery(%q): %v", q, err)
	}
	sort.Strings(lines)
	return strings.Join(lines, ";")
}

func (fx *coldFixture) prefetch(t *testing.T) {
	t.Helper()
	if n := prefetchFooters(context.Background(), fx.s.pool, fx.files, fx.s.footerCache, 4, fx.s.footerPrefetchBytes()); n != len(fx.files) {
		t.Fatalf("footer prefetch cached %d of %d footers", n, len(fx.files))
	}
}

// stripeStart is the offset of the first ColumnIndex/OffsetIndex section of an
// object (-1 when it has none), read from the object itself.
func stripeStart(t *testing.T, data []byte) int64 {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	start := int64(-1)
	for _, rg := range f.Metadata().RowGroups {
		for _, c := range rg.Columns {
			for _, o := range []int64{c.ColumnIndexOffset, c.OffsetIndexOffset} {
				if o > 0 && (start < 0 || o < start) {
					start = o
				}
			}
		}
	}
	return start
}

// A file whose footer is cached opens with no S3 round trip, in both read
// modes, and every ColumnIndex()/OffsetIndex() read afterwards (what row-group
// pruning does) is served from memory and equals the object's own.
func TestCachedFooter_OpenAndPageIndexMakeZeroGETs(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy fixture: runs in the heavy (non -short) job")
	}
	// Both fetch modes, and both parquet page read modes (the zero-GET open must
	// not depend on whether pages are read ahead by goroutines or inline).
	for _, rm := range []string{"sync", "async"} {
		for _, mode := range []string{config.ProjectedFetchModePlanned, config.ProjectedFetchModeWindow} {
			t.Run(rm+"/"+mode, func(t *testing.T) { testCachedFooterZeroGETs(t, mode, rm) })
		}
	}
}

func testCachedFooterZeroGETs(t *testing.T, mode, readMode string) {
	{
		{
			fx := newColdFixture(t, 2, 9000, 3000, mode)
			fx.s.cfg.S3.ParquetReadMode = readMode
			fx.prefetch(t)
			fi := fx.files[0]
			ref, err := parquet.OpenFile(bytes.NewReader(fx.datas[fi.Key]), fi.Size)
			if err != nil {
				t.Fatal(err)
			}

			before := len(fx.mock.Requests())
			var f *parquet.File
			if mode == config.ProjectedFetchModePlanned {
				pf, view, oerr := fx.s.openPlannedParquet(context.Background(), fi, nil, nil)
				if oerr != nil {
					t.Fatal(oerr)
				}
				defer func() { _ = view.Close() }()
				f = pf
			} else {
				f, err = fx.s.openRangedParquet(context.Background(), fi, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			if got := len(fx.mock.Requests()) - before; got != 0 {
				t.Fatalf("opening a cached file made %d S3 requests, want 0: %+v", got, fx.mock.Requests()[before:])
			}

			checked := 0
			for rgi, rg := range f.RowGroups() {
				for ci, cc := range rg.ColumnChunks() {
					idx, ierr := cc.ColumnIndex()
					if ierr != nil {
						t.Fatalf("rg %d col %d ColumnIndex: %v", rgi, ci, ierr)
					}
					oidx, oerr := cc.OffsetIndex()
					if oerr != nil {
						t.Fatalf("rg %d col %d OffsetIndex: %v", rgi, ci, oerr)
					}
					rcc := ref.RowGroups()[rgi].ColumnChunks()[ci]
					ridx, _ := rcc.ColumnIndex()
					roidx, _ := rcc.OffsetIndex()
					if idx.NumPages() != ridx.NumPages() || oidx.NumPages() != roidx.NumPages() {
						t.Fatalf("rg %d col %d: page counts differ from the object's own index", rgi, ci)
					}
					for p := 0; p < idx.NumPages(); p++ {
						if !bytes.Equal(idx.MinValue(p).Bytes(), ridx.MinValue(p).Bytes()) ||
							!bytes.Equal(idx.MaxValue(p).Bytes(), ridx.MaxValue(p).Bytes()) ||
							idx.NullCount(p) != ridx.NullCount(p) {
							t.Fatalf("rg %d col %d page %d: column index differs from the object's own", rgi, ci, p)
						}
						if oidx.Offset(p) != roidx.Offset(p) || oidx.CompressedPageSize(p) != roidx.CompressedPageSize(p) {
							t.Fatalf("rg %d col %d page %d: offset index differs from the object's own", rgi, ci, p)
						}
						checked++
					}
				}
			}
			if checked == 0 {
				t.Fatal("fixture has no page index; the test would prove nothing")
			}
			if got := len(fx.mock.Requests()) - before; got != 0 {
				t.Fatalf("page-index reads made %d S3 requests, want 0 (%d pages checked)", got, checked)
			}
		}
	}
}

// With cached footers a query sends only data ranges to S3: nothing at or after
// the page-index stripe (footer and indexes are in memory), no whole-object
// GET, at most one span per (row group, projected column) per file, and the
// answer equals window mode's.
func TestColdQuery_CachedFooters_OnlyDataRangesReachS3(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy fixture: runs in the heavy (non -short) job")
	}
	const nFiles, rows, rg = 3, 9000, 3000
	planned := newColdFixture(t, nFiles, rows, rg, config.ProjectedFetchModePlanned)
	planned.prefetch(t)
	window := newColdFixture(t, nFiles, rows, rg, config.ProjectedFetchModeWindow)
	window.prefetch(t)

	const q = `BIGMARK | stats count() n`
	// Inject a round trip of latency so concurrent requests overlap in time and
	// the sequential chain (the critical path in round trips) is measurable.
	planned.mock.SetLatency(15 * time.Millisecond)
	window.mock.SetLatency(15 * time.Millisecond)
	pBefore, wBefore := len(planned.mock.Requests()), len(window.mock.Requests())
	pAns, wAns := planned.answer(t, q), window.answer(t, q)
	if pAns != wAns {
		t.Fatalf("answers differ: planned %q window %q", pAns, wAns)
	}
	if !strings.HasPrefix(pAns, "n=") || pAns == "n=0" {
		t.Fatalf("unexpected answer %q (fixture must contain BIGMARK rows)", pAns)
	}
	pAll, wAll := planned.mock.Requests()[pBefore:], window.mock.Requests()[wBefore:]
	// Parquet object reads only: a per-file bloom sidecar probe (a 404 on files
	// without one) is a different request class, counted in the round-trip chain
	// below but not in the data-range assertions.
	dataGets := func(rs []countReq) (out []countReq) {
		for _, r := range rs {
			if r.Class == "data" && strings.HasPrefix(r.Op, "GET") {
				out = append(out, r)
			}
		}
		return out
	}
	pReqs, wReqs := dataGets(pAll), dataGets(wAll)

	for _, r := range pReqs {
		if r.Op == "GET" {
			t.Fatalf("whole-object GET on a cached-footer query: %+v", r)
		}
		ss := stripeStart(t, planned.datas[r.Key])
		if ss > 0 && r.Off+r.Len > ss {
			t.Errorf("planned query read the footer/page-index region [%d,%d) of %s (stripe starts at %d): it must come from memory", r.Off, r.Off+r.Len, r.Key, ss)
		}
	}
	// At most one span per (row group, projected column) per file; the filter
	// reads at most three columns (the filtered column, the time column and a
	// dictionary or bloom neighbour).
	maxGets := nFiles * (rows/rg + 1) * 3
	if len(pReqs) == 0 || len(pReqs) > maxGets {
		t.Fatalf("planned query made %d GETs, want 1..%d", len(pReqs), maxGets)
	}
	sum := func(rs []countReq) (n int64) {
		for _, r := range rs {
			n += r.Len
		}
		return n
	}
	pBytes, wBytes := sum(pReqs), sum(wReqs)
	t.Logf("GETs / bytes: planned=%d / %d window=%d / %d", len(pReqs), pBytes, len(wReqs), wBytes)
	pRep, wRep := planned.mock.Report(pAll), window.mock.Report(wAll)
	t.Logf("sequential round trips: planned=%d window=%d; S3 request cost per query (AWS list price): planned=$%.7f window=$%.7f",
		pRep.Chain, wRep.Chain, pRep.Dollars(), wRep.Dollars())
	// One planned wave per file, files admitted 8 at a time: with 3 files a cached-footer
	// query is a single round trip.
	if pRep.Chain > 2 {
		t.Errorf("planned query took %d sequential round trips over cached footers, want <= 2", pRep.Chain)
	}
	if pRep.Chain > wRep.Chain {
		t.Errorf("planned chain %d exceeds window's %d", pRep.Chain, wRep.Chain)
	}
	// The re-entry gate of the planned fetch: bytes no higher than the window
	// reader's and GETs within 4x of it (planned issues one exact span per
	// row-group column chunk where the window reader's read-ahead merges
	// neighbours, so on small files it can make more, smaller requests).
	if pBytes > wBytes || len(pReqs) > 4*len(wReqs) {
		t.Errorf("planned (%d GETs, %d B) outside the gate against window (%d GETs, %d B): bytes <= window and GETs <= 4x", len(pReqs), pBytes, len(wReqs), wBytes)
	}
}

// Every query shape returns the identical answer whichever way the file is
// opened: planned + overlay (the default), window + overlay, planned with a
// footer-only cache entry (page index read lazily), and with no footer cache.
func TestColdQuery_AnswersIdenticalAcrossOpenPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy fixture: runs in the heavy (non -short) job")
	}
	shapes := []string{
		`BIGMARK | stats count() n`,
		`level:=error | stats count() n`,
		`BIGMARK level:=error | stats by (service.name) count() n`,
		`status:=500 | stats count() n`,
		`* | stats count() n`,
		`* | stats by (service.name) count() n`,
		`_msg:=needle273 | stats count() n`,
	}
	type variant struct {
		name string
		mk   func(t *testing.T) *coldFixture
	}
	variants := []variant{
		{"planned+overlay", func(t *testing.T) *coldFixture {
			fx := newColdFixture(t, 3, 6000, 2000, config.ProjectedFetchModePlanned)
			fx.prefetch(t)
			return fx
		}},
		{"window+overlay", func(t *testing.T) *coldFixture {
			fx := newColdFixture(t, 3, 6000, 2000, config.ProjectedFetchModeWindow)
			fx.prefetch(t)
			return fx
		}},
		{"planned+footer-only", func(t *testing.T) *coldFixture {
			fx := newColdFixture(t, 3, 6000, 2000, config.ProjectedFetchModePlanned)
			for _, fi := range fx.files {
				data := fx.datas[fi.Key]
				fl, _ := FooterLength(data[len(data)-8:])
				cf, _, err := ParseFooterFromBytes(fi.Key, append([]byte(nil), data[len(data)-fl-8:]...), fi.Size)
				if err != nil {
					t.Fatal(err)
				}
				fx.s.footerCache.Put(fi.Key, cf)
			}
			return fx
		}},
		{"planned+cold-footers", func(t *testing.T) *coldFixture {
			return newColdFixture(t, 3, 6000, 2000, config.ProjectedFetchModePlanned)
		}},
		{"planned+overlay+sync", func(t *testing.T) *coldFixture {
			fx := newColdFixture(t, 3, 6000, 2000, config.ProjectedFetchModePlanned)
			fx.s.cfg.S3.ParquetReadMode = "sync"
			fx.prefetch(t)
			return fx
		}},
		{"window+overlay+sync", func(t *testing.T) *coldFixture {
			fx := newColdFixture(t, 3, 6000, 2000, config.ProjectedFetchModeWindow)
			fx.s.cfg.S3.ParquetReadMode = "sync"
			fx.prefetch(t)
			return fx
		}},
		{"window+no-cache", func(t *testing.T) *coldFixture {
			fx := newColdFixture(t, 3, 6000, 2000, config.ProjectedFetchModeWindow)
			fx.s.footerCache = nil
			return fx
		}},
	}
	want := map[string]string{}
	for vi, v := range variants {
		fx := v.mk(t)
		for _, q := range shapes {
			got := fx.answer(t, q)
			if vi == 0 {
				if got == "" {
					t.Fatalf("%s: empty answer for %q", v.name, q)
				}
				want[q] = got
				continue
			}
			if got != want[q] {
				t.Errorf("%s: %q answered %q, planned+overlay answered %q", v.name, q, got, want[q])
			}
		}
	}
}
