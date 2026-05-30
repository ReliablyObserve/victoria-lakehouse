package parquets3

const minFileSizeForRangeRead = 64 * 1024 // 64KB minimum
const rangeReadThreshold = 0.5            // use range reads when reading < 50% of columns

// minFileSizeForWildcardRangeRead is the cutoff above which a
// wildcard (all-columns) query switches from buffered full download
// to S3 range-read via a lazy ReaderAt. Mirror of the same constant
// in internal/storage/parquets3/range_reader.go — see that file for
// the heap-diff rationale.
const minFileSizeForWildcardRangeRead = 4 * 1024 * 1024

// shouldUseRangeRead returns true when S3 range reads would be more
// efficient than downloading the full file FOR PROJECTED QUERIES.
// Wildcard queries use shouldUseWildcardRangeRead instead.
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
// S3 ReaderAt instead of a buffered full-download bytes.Reader.
// Bounds peak heap on wildcard queries to working-set row-group bytes
// (typically <10 MiB per file) rather than cumulative-file-bytes
// (16 workers × ~30 MiB ≈ 480 MiB).
func shouldUseWildcardRangeRead(fileSize int64) bool {
	return fileSize >= minFileSizeForWildcardRangeRead
}
