package parquets3

import "testing"

func TestShouldUseRangeRead(t *testing.T) {
	// Small file — not worth range reads
	if shouldUseRangeRead(10*1024, 2, 10) {
		t.Error("small files should use full download")
	}

	// Large file with few projected columns — use range reads
	if !shouldUseRangeRead(1024*1024, 1, 10) {
		t.Error("large file with 1/10 columns should use range reads")
	}

	// All columns projected — not worth it
	if shouldUseRangeRead(1024*1024, 10, 10) {
		t.Error("all columns projected, should use full download")
	}

	// More than half projected — not worth it
	if shouldUseRangeRead(1024*1024, 6, 10) {
		t.Error("6/10 columns projected, should use full download")
	}

	// Zero columns projected — not worth it
	if shouldUseRangeRead(1024*1024, 0, 10) {
		t.Error("zero projected columns should use full download")
	}

	// Zero total columns — not worth it
	if shouldUseRangeRead(1024*1024, 5, 0) {
		t.Error("zero total columns should use full download")
	}

	// Exactly at threshold (50%) — not worth it (ratio must be strictly < 0.5)
	if shouldUseRangeRead(1024*1024, 5, 10) {
		t.Error("exactly 50% of columns projected, should use full download")
	}

	// Just below threshold — use range reads
	if !shouldUseRangeRead(1024*1024, 4, 10) {
		t.Error("4/10 columns projected, should use range reads")
	}

	// Exactly at minFileSizeForRangeRead — not worth it (must be strictly >)
	if shouldUseRangeRead(64*1024, 1, 10) {
		t.Error("file at minimum size boundary should use full download")
	}

	// Just above minFileSizeForRangeRead — eligible for range reads
	if !shouldUseRangeRead(64*1024+1, 1, 10) {
		t.Error("file just above minimum size with 1/10 columns should use range reads")
	}
}

// NOTE: TestEstimateColumnChunkBytes is skipped because the
// estimateColumnChunkBytes function does not exist in the traces module.
// The traces range_reader.go only contains shouldUseRangeRead.
