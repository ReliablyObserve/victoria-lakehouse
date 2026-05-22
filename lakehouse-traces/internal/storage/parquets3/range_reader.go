package parquets3

const minFileSizeForRangeRead = 64 * 1024 // 64KB minimum
const rangeReadThreshold = 0.5            // use range reads when reading < 50% of columns

// shouldUseRangeRead returns true when S3 range reads would be more
// efficient than downloading the full file.
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
