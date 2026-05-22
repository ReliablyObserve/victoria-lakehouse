package parquets3

import (
	"context"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const (
	// footerPrefetchSize is the number of bytes to range-read from the end of a file
	// to extract the parquet footer metadata.
	footerPrefetchSize = 16 * 1024 // 16KB

	// minFileSizeForPrefetch is the minimum file size to attempt footer pre-fetch.
	// Files smaller than this are downloaded fully, which is faster than two round-trips.
	minFileSizeForPrefetch = 32 * 1024 // 32KB
)

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
) (bool, error) {
	// Skip when pool is nil — no S3 access possible.
	if pool == nil {
		return false, nil
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

	// Range-read the last 16KB to get the parquet footer.
	offset := fi.Size - footerPrefetchSize
	if offset < 0 {
		offset = 0
	}
	length := fi.Size - offset

	tail, err := pool.DownloadRange(ctx, fi.Key, offset, length)
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
