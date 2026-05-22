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
