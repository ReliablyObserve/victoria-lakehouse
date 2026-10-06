package parquets3

import (
	"context"
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The footer prefetch SIZE is per-signal config now — s3.footer_prefetch_bytes,
// resolved by (*Storage).footerPrefetchBytes() in signal_defaults.go (logs
// 128KB / traces 640KB defaults; the shared 64KB constant it replaces could
// never hold a traces L2 footer, whose embedded trace index runs 467-519KB,
// so those reads always fell back to full downloads). Free functions in this
// file take the resolved size as a parameter; <= 0 means the per-signal
// default.
const (
	// minFileSizeForPrefetch is the minimum file size to attempt footer pre-fetch.
	// Files smaller than this are downloaded fully, which is faster than two round-trips.
	minFileSizeForPrefetch = 128 * 1024 // 128KB
)

// footerPrefetchTail bounds the footer tail read by file size: small files
// keep the 64KB floor (a 400KB file must not spend a third of its body on
// a footer probe — pinned by TestGetFieldValues_UsesColumnProjectedRead),
// while the large compacted files that actually carry oversized footers
// (traces L2: 467-519KB of trace-index KV on ~24MB objects → clamp 3MB)
// get the full per-signal prefetch size. Same max(64KB, size/8) clamp
// family as the coalescing-gap clamps. An under-fetch self-heals:
// fetchFooterFile issues an exact second range read once the trailer
// reveals the true footer length.
func footerPrefetchTail(prefetchBytes, fileSize int64) int64 {
	if sizeCap := max64(64<<10, fileSize/8); prefetchBytes > sizeCap {
		return sizeCap
	}
	return prefetchBytes
}

// pendingFooter is a parsed footer whose cache entry still references the
// caller's buffer; own() turns it into an entry that owns exactly what is worth
// keeping. Splitting the two lets a caller that may discard the footer (the
// row-group skip probe) parse without paying for the copy.
type pendingFooter struct {
	cached     *CachedFooter
	file       *parquet.File
	rd         *footerReaderAt
	key        string
	tail       []byte
	tailOff    int64
	fileSize   int64
	footerSize int64
}

func parseFooterRegion(key string, tail []byte, tailOff, fileSize int64) (*pendingFooter, error) {
	if tailOff < 0 || tailOff+int64(len(tail)) != fileSize {
		return nil, fmt.Errorf("cache footer %s: tail [%d,+%d) does not end at object size %d", key, tailOff, len(tail), fileSize)
	}
	cached, f, rd, err := parseFooterBytes(key, tail, fileSize)
	if err != nil {
		return nil, err
	}
	return &pendingFooter{cached: cached, file: f, rd: rd, key: key, tail: tail, tailOff: tailOff, fileSize: fileSize, footerSize: int64(cached.footerSize)}, nil
}

// own finalizes the entry: it keeps a copy of the footer and the page-index
// stripe (ColumnIndex + OffsetIndex sections) that sits right before it, so a
// later open serves both from memory (see s3reader.OverlayReaderAt). The
// caller's buffer is not retained, so a 128 KB prefetch tail does not pin
// 128 KB per entry.
//
// When the stripe starts before the fetched region (a footer large enough to
// push it out of the range) one extra range GET, through dl (the caller's own
// ranged read: deduplicated for the shared prefetch, context-bound for the
// bounds resolution), fetches the missing bytes; with dl nil, or if that fails,
// the entry caches the footer alone. A footer-only entry never answers a
// page-index read with made-up bytes: footerReaderAt fails it with
// errOutsideCachedRange and the caller fetches the stripe or reports the value
// unknown (accumulateFieldHits).
func (p *pendingFooter) own(ctx context.Context, dl rangeDownloader) (*CachedFooter, *parquet.File) {
	footerStart := p.fileSize - p.footerSize
	keepFrom := footerStart
	var front []byte
	if ps := pageIndexStripeStart(p.file); ps > 0 && ps < footerStart {
		if ps >= p.tailOff {
			keepFrom = ps
		} else if dl != nil {
			metrics.S3GetsByPhase.Inc("footer")
			if b, gerr := dl(ctx, "footer", p.key, ps, p.tailOff-ps); gerr == nil && int64(len(b)) == p.tailOff-ps {
				front, keepFrom = b, ps
			}
		}
	}
	owned := make([]byte, p.fileSize-keepFrom) // exact size: no append slack
	if front != nil {
		copy(owned, front)
		copy(owned[len(front):], p.tail)
	} else {
		copy(owned, p.tail[keepFrom-p.tailOff:])
	}
	p.rd.footer = owned
	p.cached.tail, p.cached.tailOff = owned, keepFrom
	return p.cached, p.file
}

// cacheFooterFromTail parses the footer out of tail — the object's bytes from
// tailOff to EOF, as returned by a footer range read — and builds the cache
// entry for it (see pendingFooter.own).
func cacheFooterFromTail(ctx context.Context, dl rangeDownloader, key string, tail []byte, tailOff, fileSize int64) (*CachedFooter, *parquet.File, error) {
	p, err := parseFooterRegion(key, tail, tailOff, fileSize)
	if err != nil {
		return nil, nil, err
	}
	cached, f := p.own(ctx, dl)
	return cached, f, nil
}

// shouldSkipByFooter performs an S3 range read to fetch only the parquet footer,
// parses row group metadata, and checks the pushdown filter against each row group.
// Returns (true, nil) if the file can be safely skipped (no row group matches the filter).
// Returns (false, nil) on any failure, allowing full download to proceed.
//
// Decision to skip prefetch:
//   - pool is nil (no S3 access)
//   - no pushdown filter (wildcard query)
//   - file is too small (< 32KB — faster to download fully)
//   - footer already cached (queryFile will use the cache, no benefit)
func shouldSkipByFooter(
	ctx context.Context,
	pool *s3reader.ClientPool,
	fi manifest.FileInfo,
	queryStr string,
	registry *schema.Registry,
	footerCache *FooterCache,
	prefetchBytes int64,
) (bool, error) {
	// Skip when pool is nil — no S3 access possible.
	if pool == nil {
		return false, nil
	}
	if prefetchBytes <= 0 {
		prefetchBytes = defaultFooterPrefetchBytes
	}

	// Skip when there's no pushdown filter — wildcard queries must scan everything.
	pdf := buildPushDownFilter(queryStr, registry)
	if pdf == nil {
		return false, nil
	}

	// Skip prefetch for small files; downloading the full file is faster than two round-trips.
	if fi.Size < minFileSizeForPrefetch {
		return false, nil
	}

	// Skip if footer is already cached — queryFile will use it; no benefit from pre-fetch.
	if footerCache != nil {
		if _, ok := footerCache.Get(fi.Key); ok {
			return false, nil
		}
	}

	// Range-read the configured tail to get the parquet footer.
	offset := fi.Size - footerPrefetchTail(prefetchBytes, fi.Size)
	if offset < 0 {
		offset = 0
	}
	length := fi.Size - offset

	metrics.S3GetsByPhase.Inc("footer")
	tail, err := pool.DownloadRangeDedup(ctx, "footer", fi.Key, offset, length)
	if err != nil {
		// Fall back to full download — don't fail the query.
		return false, nil
	}

	// The tail must contain at least the 8-byte parquet suffix (footer length + magic).
	if len(tail) < 8 {
		return false, nil
	}

	// Read the footer length from the last 8 bytes.
	footerLen, err := FooterLength(tail[len(tail)-8:])
	if err != nil {
		// Not a valid parquet file or bad magic — fall back.
		return false, nil
	}

	// Determine how many bytes of footer we have in the tail.
	// The full footer region is: footerLen bytes + 8 bytes suffix = footerLen+8 bytes from end.
	totalFooterBytes := footerLen + 8
	if totalFooterBytes > len(tail) {
		// Footer is larger than what we fetched — fall back to full download.
		return false, nil
	}

	// Parse the footer metadata.
	pending, err := parseFooterRegion(fi.Key, tail, offset, fi.Size)
	if err != nil {
		// Parse error — fall back.
		return false, nil
	}
	pf := pending.file

	// Resolve column indices now that we have the schema.
	resolvedPdf := resolvePushDownIndices(pf, pdf)

	// Check each row group against the filter.
	rowGroups := pf.RowGroups()
	anyMatch := false
	for _, rg := range rowGroups {
		if rowGroupMatchesFilter(pf, rg, resolvedPdf) {
			anyMatch = true
			break
		}
	}

	if !anyMatch {
		// No row group matches — safe to skip.
		metrics.ParquetRowGroupsSkipped.Inc("footer_prefetch")
		return true, nil
	}

	// At least one row group might match — cache the footer (with its
	// page-index stripe) for queryFile to reuse.
	if footerCache != nil {
		cached, _ := pending.own(ctx, dedupDownloader(pool))
		footerCache.Put(fi.Key, cached)
	}

	return false, nil
}

// dedupDownloader is the pool's deduplicated ranged read (nil without a pool),
// the form the shared footer prefetch uses.
func dedupDownloader(pool *s3reader.ClientPool) rangeDownloader {
	if pool == nil {
		return nil
	}
	return pool.DownloadRangeDedup
}

// prefetchOpts tunes prefetchFootersOpts.
type prefetchOpts struct {
	// reserve is the number of cache entries to leave for opens that run
	// concurrently with the consumers of the prefetched footers (the query's
	// file workers): their own footer Puts evict from the LRU, so prefetching
	// the whole budget would see its oldest entries evicted before they are
	// used. The prefetch still caches at least one entry.
	reserve int
	// visit, when set, is handed every footer as soon as it is parsed,
	// whether or not the cache keeps it, so a consumer that needs each footer
	// once (the startup enrichment) never reads it back through the LRU.
	visit func(fi manifest.FileInfo, cached *CachedFooter)
	// fetchAll fetches every footer even past what the cache can hold; only
	// what fits is kept (PutIfFits, no eviction). Without visit it has no use.
	fetchAll bool
}

// estimatedEntryBytes is the charge assumed for a footer that has not been
// measured yet: a deliberately high figure (the tail it would read, the decoded
// metadata, the fixed overhead), so the first wave over a small budget is small
// and the following waves are sized from measured weights.
func estimatedEntryBytes(prefetchBytes int64) int64 {
	return prefetchBytes + int64(retainedMetadataFactor*float64(prefetchBytes)) + cachedFooterOverhead
}

// prefetchFooters fetches parquet footers for the given files in parallel
// using prefetchBytes-sized tail range reads (<= 0 = the per-signal default)
// and populates the footer cache, so subsequent file processing can use range
// reads instead of full file downloads. It fetches only as many footers as the
// cache budget holds (see prefetchFootersOpts).
func prefetchFooters(ctx context.Context, pool *s3reader.ClientPool, files []manifest.FileInfo, footerCache *FooterCache, concurrency int, prefetchBytes int64) int {
	return prefetchFootersOpts(ctx, pool, files, footerCache, concurrency, prefetchBytes, prefetchOpts{})
}

// prefetchFootersOpts is prefetchFooters with options. Unless fetchAll is set
// it is BUDGET-AWARE: a footer fetched beyond what the cache can hold would be
// evicted before its file is opened and fetched a second time, so the batch
// stops at the number of entries the byte budget holds (budget / measured mean
// entry weight, minus the entries of this batch already cached and the
// reserve, at least 1) and runs in waves so the mean is measured on the entries
// just fetched. The files left out open (and cache) their own footer on demand.
func prefetchFootersOpts(ctx context.Context, pool *s3reader.ClientPool, files []manifest.FileInfo, footerCache *FooterCache, concurrency int, prefetchBytes int64, o prefetchOpts) int {
	if pool == nil || footerCache == nil || len(files) == 0 {
		return 0
	}
	if prefetchBytes <= 0 {
		prefetchBytes = defaultFooterPrefetchBytes
	}
	if concurrency <= 0 {
		concurrency = 16
	}

	var uncached []manifest.FileInfo
	cachedInBatch := 0
	for _, fi := range files {
		if fi.Size < minFileSizeForPrefetch {
			continue
		}
		if cf, ok := footerCache.Get(fi.Key); ok {
			cachedInBatch++
			if o.visit != nil {
				o.visit(fi, cf)
			}
		} else {
			uncached = append(uncached, fi)
		}
	}
	if len(uncached) == 0 {
		return 0
	}

	var st prefetchStats
	if o.fetchAll {
		fetchFooterBatch(ctx, pool, uncached, footerCache, concurrency, prefetchBytes, o.visit, true, &st)
	} else {
		budget := footerCache.MaxBytes()
		rest := uncached
		for len(rest) > 0 && ctx.Err() == nil {
			avg := estimatedEntryBytes(prefetchBytes)
			if st.n > 0 {
				avg = max64(1, st.weight/int64(st.n))
			} else if a := footerCache.AvgEntryBytes(); a > 0 {
				avg = a
			}
			limit := max64(1, budget/avg-int64(o.reserve)) - int64(cachedInBatch) - int64(st.n)
			if limit <= 0 {
				break
			}
			wave := min(int64(len(rest)), limit, int64(concurrency))
			fetchFooterBatch(ctx, pool, rest[:wave], footerCache, concurrency, prefetchBytes, o.visit, false, &st)
			rest = rest[wave:]
		}
		if len(rest) > 0 {
			metrics.PrefetchTasksTotal.Add("footer_prefetch_skipped_budget", len(rest))
		}
	}

	if st.dlErrors > 0 || st.parseErrors > 0 || st.tooBig > 0 {
		logger.Infof("footer prefetch: errors: dl=%d parse=%d too_big=%d", st.dlErrors, st.parseErrors, st.tooBig)
	}
	if st.n > 0 {
		metrics.PrefetchTasksTotal.Add("footer_prefetch", st.n)
		logger.Infof("footer prefetch: cached %d/%d footers", st.n, len(uncached))
	}
	return st.n
}

// prefetchStats accumulates over the waves of one prefetch call.
type prefetchStats struct {
	n                             int   // footers fetched and parsed
	weight                        int64 // their summed cache charge
	dlErrors, parseErrors, tooBig int
}

// fetchFooterBatch fetches and parses the footers of batch with up to
// concurrency workers, hands each to visit (called from the workers: it must be
// safe for concurrent use), and caches it (Put, or PutIfFits for a bulk fetch
// that must not evict).
func fetchFooterBatch(ctx context.Context, pool *s3reader.ClientPool, batch []manifest.FileInfo, footerCache *FooterCache, concurrency int, prefetchBytes int64, visit func(manifest.FileInfo, *CachedFooter), ifFits bool, st *prefetchStats) {
	if concurrency > len(batch) {
		concurrency = len(batch)
	}
	taskCh := make(chan manifest.FileInfo, len(batch))
	for _, fi := range batch {
		taskCh <- fi
	}
	close(taskCh)

	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range taskCh {
				if ctx.Err() != nil {
					return
				}
				offset := fi.Size - footerPrefetchTail(prefetchBytes, fi.Size)
				if offset < 0 {
					offset = 0
				}
				length := fi.Size - offset
				metrics.S3GetsByPhase.Inc("footer")
				tail, err := pool.DownloadRangeDedup(ctx, "footer", fi.Key, offset, length)
				if err != nil || len(tail) < 8 {
					mu.Lock()
					st.dlErrors++
					mu.Unlock()
					continue
				}
				footerLen, err := FooterLength(tail[len(tail)-8:])
				if err != nil {
					mu.Lock()
					st.parseErrors++
					mu.Unlock()
					continue
				}
				totalFooterBytes := footerLen + 8
				if totalFooterBytes > len(tail) {
					mu.Lock()
					st.tooBig++
					mu.Unlock()
					continue
				}
				cached, _, err := cacheFooterFromTail(ctx, pool.DownloadRangeDedup, fi.Key, tail, offset, fi.Size)
				if err != nil {
					mu.Lock()
					st.parseErrors++
					if st.parseErrors == 1 {
						logger.Warnf("footer prefetch: first parse error: key=%s size=%d tail=%d err=%v", fi.Key, fi.Size, len(tail), err)
					}
					mu.Unlock()
					continue
				}
				if visit != nil {
					visit(fi, cached)
				}
				if ifFits {
					footerCache.PutIfFits(fi.Key, cached)
				} else {
					footerCache.Put(fi.Key, cached)
				}
				mu.Lock()
				st.n++
				st.weight += cached.Weight()
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}
