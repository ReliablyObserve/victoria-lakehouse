package parquets3

// Review fixes S2-2 / S2-3 for the zero-GET open: the footer prefetchers must
// respect the footer-cache budget, and field_names must not depend on what the
// cache holds. Behavioural tests only (requests seen by the mock S3, answers),
// so they also compile against the tree before the fix: fail-before / pass-after.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// keyPartition is the "dt=YYYY-MM-DD/hour=HH" partition of a fixture object key.
func keyPartition(key string) string {
	parts := strings.Split(key, "/")
	return parts[len(parts)-3] + "/" + parts[len(parts)-2]
}

const (
	budgetFiles   = 40
	budgetRows    = 330 // the traces fixture scales rows x8 to clear the 128 KB floor
	budgetRGRows  = 165
	budgetEntries = 5 // the "small" footer budget, in cache entries
)

// resetCaches puts the storage into the cold steady state a run starts from: a
// fresh footer cache of the given budget (0 = auto), empty data caches, an
// empty request log.
func (fx *coldFixture) resetCaches(budget int64) {
	fx.s.footerCache = NewFooterCache(budget)
	fx.s.memCache = cache.NewLRU(64 * 1024 * 1024)
	fx.mock.Reset()
}

// objectGets splits the parquet requests the mock saw into whole-object GETs and
// ranged GETs.
func objectGets(rs []countReq) (whole, ranged int) {
	for _, r := range rs {
		if r.Class != "data" || !strings.HasPrefix(r.Op, "GET") {
			continue
		}
		if r.Op == "GET" {
			whole++
		} else {
			ranged++
		}
	}
	return whole, ranged
}

func requireFooterSizedFiles(t *testing.T, fx *coldFixture) {
	t.Helper()
	for _, fi := range fx.files {
		if fi.Size < minFileSizeForPrefetch {
			t.Fatalf("fixture file %s is %d bytes, below the %d-byte footer-prefetch floor: raise budgetRows", fi.Key, fi.Size, minFileSizeForPrefetch)
		}
	}
}

// smallBudget measures one footer-cache entry of the fixture and returns the
// byte budget of budgetEntries of them.
func smallBudget(t *testing.T, fx *coldFixture) int64 {
	t.Helper()
	fx.resetCaches(0)
	if n := prefetchFooters(context.Background(), fx.s.pool, fx.files, fx.s.footerCache, 4, fx.s.footerPrefetchBytes()); n != len(fx.files) {
		t.Fatalf("auto-budget prefetch cached %d of %d footers", n, len(fx.files))
	}
	avg := fx.s.footerCache.AvgEntryBytes()
	if avg <= 0 {
		t.Fatal("no footer entry weight measured")
	}
	return avg*budgetEntries + avg/2
}

// A query over more files than the footer cache holds must not pay for footers
// it cannot keep: the prefetch used to fetch all 40, the worker opens evicted the
// earliest before use, and every evicted file fetched its footer a second time
// (40-file BIGMARK count: 120 GETs with a 5-entry budget against 80 with a large
// one). The answer must be identical either way.
func TestQuery_FooterPrefetchRespectsBudget_SameGETsAsLargeBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("40-file fixture: runs in the heavy (non -short) job")
	}
	fx := newColdFixture(t, budgetFiles, budgetRows, budgetRGRows, config.ProjectedFetchModePlanned)
	requireFooterSizedFiles(t, fx)
	small := smallBudget(t, fx)
	const q = `duration:>800ms | stats count() n`

	fx.resetCaches(0)
	big := fx.answer(t, q)
	bigWhole, bigRanged := objectGets(fx.mock.Reset())

	fx.resetCaches(small)
	got := fx.answer(t, q)
	smallWhole, smallRanged := objectGets(fx.mock.Reset())

	if !strings.HasPrefix(big, "n=") || big == "n=0" {
		t.Fatalf("unexpected answer %q: the fixture must contain rows matching the query", big)
	}
	if got != big {
		t.Fatalf("answer changed with the footer budget: small %q, large %q", got, big)
	}
	t.Logf("object GETs: large budget %d whole + %d ranged, small budget (%d entries) %d whole + %d ranged",
		bigWhole, bigRanged, budgetEntries, smallWhole, smallRanged)
	if smallWhole != 0 || bigWhole != 0 {
		t.Errorf("whole-object GETs: small=%d large=%d, want 0 (cold footers are range reads over %d-byte files)", smallWhole, bigWhole, fx.files[0].Size)
	}
	// Allow the few extra footer reads of the files processed while the prefetched
	// entries are consumed; the unbudgeted prefetch cost +40 (one per file).
	if limit := bigRanged + bigRanged/10; smallRanged > limit {
		t.Errorf("a %d-entry footer budget made %d ranged GETs, large budget %d (limit %d): the prefetch fetched footers the cache could not keep", budgetEntries, smallRanged, bigRanged, limit)
	}
}

// The prefetch itself fetches no more footers than the budget holds.
func TestPrefetchFooters_StopsAtTheBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("40-file fixture: runs in the heavy (non -short) job")
	}
	fx := newColdFixture(t, budgetFiles, budgetRows, budgetRGRows, config.ProjectedFetchModePlanned)
	requireFooterSizedFiles(t, fx)
	small := smallBudget(t, fx)

	fx.resetCaches(small)
	n := prefetchFooters(context.Background(), fx.s.pool, fx.files, fx.s.footerCache, 16, fx.s.footerPrefetchBytes())
	_, ranged := objectGets(fx.mock.Reset())
	t.Logf("budget %d entries: fetched %d footers with %d ranged GETs, cache holds %d", budgetEntries, n, ranged, fx.s.footerCache.Len())
	if n > budgetEntries+1 {
		t.Errorf("prefetch fetched %d footers for a %d-entry budget", n, budgetEntries)
	}
	if n < 1 {
		t.Errorf("prefetch fetched %d footers, want at least 1", n)
	}
	if ranged > 2*budgetEntries {
		t.Errorf("prefetch made %d ranged GETs for a %d-entry budget", ranged, budgetEntries)
	}
	if fx.s.footerCache.Bytes() > small {
		t.Errorf("cache holds %d bytes over its %d-byte budget", fx.s.footerCache.Bytes(), small)
	}
}

// Startup enrichment: with a footer budget smaller than the file set the
// prefetched footers used to be read back through the LRU, the evicted ones fell
// into enrichSmallFiles, and that downloaded every one of those objects whole
// (40 files, 5-entry budget: 35 whole-object GETs; a large budget: 0). Each
// footer is now enriched from as it is parsed, so every file gets its row count
// and bounds from one ranged GET whatever the budget.
func TestWarmMetadata_SmallFooterBudget_NoWholeObjectDownloads(t *testing.T) {
	if testing.Short() {
		t.Skip("40-file fixture: runs in the heavy (non -short) job")
	}
	fx := newColdFixture(t, budgetFiles, budgetRows, budgetRGRows, config.ProjectedFetchModePlanned)
	requireFooterSizedFiles(t, fx)
	small := smallBudget(t, fx)

	run := func(budget int64) (whole, ranged, enriched int, bounds map[string][2]int64) {
		fx.resetCaches(budget)
		// A manifest that lists the objects without row counts or exact bounds, as
		// after a restart with no persisted metadata.
		fx.s.manifest = manifest.New("test-bucket", "logs/")
		for _, fi := range fx.files {
			bare := manifest.FileInfo{Key: fi.Key, Size: fi.Size}
			fx.s.manifest.AddFile(keyPartition(fi.Key), bare)
		}
		fx.s.WarmMetadata(context.Background())
		whole, ranged = objectGets(fx.mock.Reset())
		bounds = map[string][2]int64{}
		for _, fi := range fx.s.manifest.GetFilesForRange(0, 1<<62) {
			if fi.RowCount > 0 {
				enriched++
			}
			bounds[fi.Key] = [2]int64{fi.MinTimeNs, fi.MaxTimeNs}
		}
		return whole, ranged, enriched, bounds
	}

	bigWhole, bigRanged, bigEnriched, bigBounds := run(0)
	smallWhole, smallRanged, smallEnriched, smallBounds := run(small)
	t.Logf("large budget: %d whole + %d ranged GETs, %d/%d enriched; %d-entry budget: %d whole + %d ranged GETs, %d/%d enriched",
		bigWhole, bigRanged, bigEnriched, budgetFiles, budgetEntries, smallWhole, smallRanged, smallEnriched, budgetFiles)
	if bigEnriched != budgetFiles {
		t.Fatalf("large budget enriched %d of %d files", bigEnriched, budgetFiles)
	}
	if smallEnriched != budgetFiles {
		t.Errorf("small budget enriched %d of %d files", smallEnriched, budgetFiles)
	}
	if bigWhole != 0 || smallWhole != 0 {
		t.Errorf("whole-object GETs: large=%d small=%d, want 0 for both (every file is above the %d-byte prefetch floor)", bigWhole, smallWhole, minFileSizeForPrefetch)
	}
	if smallRanged > bigRanged+bigRanged/10 {
		t.Errorf("ranged GETs: small budget %d, large %d", smallRanged, bigRanged)
	}
	keys := make([]string, 0, len(bigBounds))
	for k := range bigBounds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if bigBounds[k] != smallBounds[k] {
			t.Errorf("bounds of %s depend on the footer budget: large %v, small %v", k, bigBounds[k], smallBounds[k])
		}
	}
}

// The files below the footer-prefetch floor are the only ones enrichSmallFiles
// downloads whole.
func TestEnrichSmallFiles_OnlyBelowTheFooterPrefetchFloor(t *testing.T) {
	fx := newColdFixture(t, 3, 40, 40, config.ProjectedFetchModePlanned)
	for _, fi := range fx.files {
		if fi.Size >= minFileSizeForPrefetch {
			t.Fatalf("fixture file %s is %d bytes: this test needs files below the floor", fi.Key, fi.Size)
		}
	}
	fx.resetCaches(0)
	fx.s.manifest = manifest.New("test-bucket", "logs/")
	for _, fi := range fx.files {
		fx.s.manifest.AddFile(keyPartition(fi.Key), manifest.FileInfo{Key: fi.Key, Size: fi.Size})
	}
	fx.s.WarmMetadata(context.Background())
	whole, _ := objectGets(fx.mock.Reset())
	if whole != len(fx.files) {
		t.Errorf("whole-object GETs = %d, want %d (one per small file)", whole, len(fx.files))
	}
	for _, fi := range fx.s.manifest.GetFilesForRange(0, 1<<62) {
		if fi.RowCount == 0 {
			t.Errorf("%s was not enriched", fi.Key)
		}
	}
}

// fieldNameHits runs field_names over the fixture window.
func fieldNameHits(t *testing.T, fx *coldFixture) map[string]uint64 {
	t.Helper()
	start, end := fx.window()
	q := mustParseQueryWithTime(t, "*", start, end)
	vals, err := fx.s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]uint64, len(vals))
	for _, v := range vals {
		out[v.Value] = v.Hits
	}
	return out
}

func footerOnlyEntry(t *testing.T, fx *coldFixture, key string) *CachedFooter {
	t.Helper()
	data := fx.datas[key]
	fl, err := FooterLength(data[len(data)-8:])
	if err != nil {
		t.Fatal(err)
	}
	cf, _, err := ParseFooterFromBytes(key, append([]byte(nil), data[len(data)-fl-8:]...), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return cf
}

// The logs module derives field_names hit counts from the page index null
// counts (and its test pins the cache-state independence). The traces answer
// does not read the page index at all, so it was never affected: names with the
// documented Hits=1, identical whatever the footer cache holds.
func TestFieldNames_TracesAnswerUnaffectedByFooterCacheState(t *testing.T) {
	fx := newColdFixture(t, 1, budgetRows, budgetRGRows, config.ProjectedFetchModePlanned)
	key := fx.files[0].Key

	fx.resetCaches(0)
	noEntry := fieldNameHits(t, fx)

	fx.resetCaches(0)
	fx.prefetch(t)
	if cf, ok := fx.s.footerCache.Get(key); !ok || !cf.HasPageIndex() {
		t.Fatal("fixture: the prefetched entry must hold the page-index stripe")
	}
	withStripe := fieldNameHits(t, fx)

	fx.resetCaches(0)
	fx.s.footerCache.Put(key, footerOnlyEntry(t, fx, key))
	footerOnly := fieldNameHits(t, fx)

	if len(withStripe) == 0 {
		t.Fatal("no field names")
	}
	for name, states := range map[string]map[string]uint64{"no entry": noEntry, "footer-only entry": footerOnly} {
		if len(states) != len(withStripe) {
			t.Errorf("%s: %d fields, with the stripe cached %d", name, len(states), len(withStripe))
		}
		for f, h := range withStripe {
			if states[f] != h {
				t.Errorf("%s: field %q has %d hits, with the stripe cached %d", name, f, states[f], h)
			}
		}
	}
	for f, h := range withStripe {
		if h != 1 {
			t.Errorf("traces field %q has Hits=%d, want the documented 1", f, h)
		}
	}
}

var _ = fmt.Sprintf
