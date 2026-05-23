package parquets3

// canSkipByColumnStats returns true if a row group can be skipped because the
// exact-match value falls outside the [minVal, maxVal] range of a sorted column.
func canSkipByColumnStats(value, minVal, maxVal string) bool {
	if minVal == "" || maxVal == "" {
		return false
	}
	return value < minVal || value > maxVal
}
