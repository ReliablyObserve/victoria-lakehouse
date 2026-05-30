package parquets3

const minFileSizeForRangeRead = 64 * 1024 // 64KB minimum
const rangeReadThreshold = 0.5            // use range reads when reading < 50% of columns

// minFileSizeForWildcardRangeRead is the cutoff above which a
// wildcard (all-columns) query switches from buffered full download
// to S3 range-read via a lazy ReaderAt. The wildcard path used to
// always full-download (queryColumns returns nil for `*`, so
// projectedCols was empty and shouldUseRangeRead returned false),
// which pins fi.Size bytes resident for the entire open-decode-emit
// window per worker. At 16 workers × ~30 MiB average file size that
// was the dominant retention path under 7-day wildcard load.
//
// Range-read for wildcards still issues range requests for column
// chunks, but parquet-go streams row groups one at a time — once
// row group N is consumed, its column pages can be GC'd while
// row group N+1's pages are fetched. Peak resident memory drops
// from cumulative-file-bytes to working-set-row-group-bytes
// (typically <10 MiB per file even on wide schemas).
//
// 4 MiB is the cutoff: files below that are dominated by per-request
// HTTP overhead rather than data transfer, and the full download
// from L1 cache (or coalesced range) is cheaper. Above 4 MiB the
// memory saving dominates.
const minFileSizeForWildcardRangeRead = 4 * 1024 * 1024

// shouldUseRangeRead returns true when S3 range reads would be more
// efficient than downloading the full file FOR PROJECTED QUERIES
// (queries that read fewer than half the columns). This is the
// original path; wildcard queries (which need all columns) use
// shouldUseWildcardRangeRead instead.
func shouldUseRangeRead(fileSize int64, projectedCols, totalCols int) bool {
	if fileSize <= minFileSizeForRangeRead {
		return false
	}
	if totalCols == 0 || projectedCols == 0 {
		return false
	}
	ratio := float64(projectedCols) / float64(totalCols)
	return ratio < rangeReadThreshold
}

// shouldUseWildcardRangeRead returns true when a wildcard (all-columns)
// query against fi.Size should open the parquet file via a lazy
// S3 ReaderAt instead of a buffered full-download bytes.Reader. This
// is the Goal B switch — it bounds peak heap on wildcard queries to
// the working-set row-group bytes (typically <10 MiB per file) rather
// than the cumulative-file-bytes (16 workers × 30 MiB ≈ 480 MiB).
//
// The cutoff is intentionally higher than minFileSizeForRangeRead
// (4 MiB vs 64 KiB) because the wildcard path issues at least one
// range request per row group rather than per projected column, so
// per-request overhead dominates on small files where the data
// transfer is negligible.
//
// fileSize 0 or negative is treated as "unknown" and returns false
// (fall back to full download).
func shouldUseWildcardRangeRead(fileSize int64) bool {
	return fileSize >= minFileSizeForWildcardRangeRead
}

// estimateColumnChunkBytes estimates how many bytes would be fetched
// with range reads for the projected columns.
func estimateColumnChunkBytes(fileSize int64, totalCols, projectedCols int, footerSize int) int64 {
	if totalCols == 0 {
		return fileSize
	}
	dataBytes := fileSize - int64(footerSize)
	if dataBytes < 0 {
		dataBytes = fileSize
	}
	estimated := (dataBytes * int64(projectedCols)) / int64(totalCols)
	return estimated + int64(footerSize)
}
