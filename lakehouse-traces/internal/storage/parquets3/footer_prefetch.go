package parquets3

import (
	"context"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

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

	// Extract the footer slice from the end of tail.
	footerSlice := tail[len(tail)-totalFooterBytes:]

	// Parse the footer metadata.
	cached, pf, err := ParseFooterFromBytes(fi.Key, footerSlice, fi.Size)
	if err != nil {
		// Parse error — fall back.
		return false, nil
	}

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

	// At least one row group might match — cache the footer for queryFile to reuse.
	if footerCache != nil {
		footerCache.Put(fi.Key, cached)
	}

	return false, nil
}

// prefetchFooters fetches parquet footers for all given files in parallel
// using prefetchBytes-sized tail range reads (<= 0 = the per-signal default)
// and populates the footer cache. This ensures subsequent file processing
// can use range reads instead of full file downloads.
func prefetchFooters(ctx context.Context, pool *s3reader.ClientPool, files []manifest.FileInfo, footerCache *FooterCache, concurrency int, prefetchBytes int64) int {
	if pool == nil || footerCache == nil || len(files) == 0 {
		return 0
	}
	if prefetchBytes <= 0 {
		prefetchBytes = defaultFooterPrefetchBytes
	}
	if concurrency <= 0 {
		concurrency = 16
	}
	if concurrency > len(files) {
		concurrency = len(files)
	}

	var uncached []manifest.FileInfo
	for _, fi := range files {
		if fi.Size < minFileSizeForPrefetch {
			continue
		}
		if _, ok := footerCache.Get(fi.Key); !ok {
			uncached = append(uncached, fi)
		}
	}
	if len(uncached) == 0 {
		return 0
	}

	taskCh := make(chan manifest.FileInfo, len(uncached))
	for _, fi := range uncached {
		taskCh <- fi
	}
	close(taskCh)

	var fetched int
	var dlErrors, parseErrors, tooBig int
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
					dlErrors++
					mu.Unlock()
					continue
				}
				footerLen, err := FooterLength(tail[len(tail)-8:])
				if err != nil {
					mu.Lock()
					parseErrors++
					mu.Unlock()
					continue
				}
				totalFooterBytes := footerLen + 8
				if totalFooterBytes > len(tail) {
					mu.Lock()
					tooBig++
					mu.Unlock()
					continue
				}
				footerSlice := tail[len(tail)-totalFooterBytes:]
				cached, _, err := ParseFooterFromBytes(fi.Key, footerSlice, fi.Size)
				if err != nil {
					mu.Lock()
					parseErrors++
					if parseErrors == 1 {
						logger.Warnf("footer prefetch: first parse error: key=%s size=%d footer_slice=%d err=%v", fi.Key, fi.Size, len(footerSlice), err)
					}
					mu.Unlock()
					continue
				}
				footerCache.Put(fi.Key, cached)
				mu.Lock()
				fetched++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if dlErrors > 0 || parseErrors > 0 || tooBig > 0 {
		logger.Infof("footer prefetch: errors: dl=%d parse=%d too_big=%d", dlErrors, parseErrors, tooBig)
	}

	if fetched > 0 {
		metrics.PrefetchTasksTotal.Add("footer_prefetch", fetched)
		logger.Infof("footer prefetch: cached %d/%d footers", fetched, len(uncached))
	}
	return fetched
}
