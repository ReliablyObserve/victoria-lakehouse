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

func TestEstimateColumnChunkBytes(t *testing.T) {
	// With a 100KB file, 10 columns, and requesting 2 columns:
	est := estimateColumnChunkBytes(100*1024, 10, 2, 4096)
	if est >= 100*1024 {
		t.Errorf("estimate %d should be less than full file %d", est, 100*1024)
	}
	// Should be roughly 20% of data + footer
	dataBytes := float64(100*1024 - 4096)
	expectedApprox := int64(dataBytes*0.2) + int64(4096)
	if est < expectedApprox/2 || est > expectedApprox*2 {
		t.Errorf("estimate %d seems wrong, expected around %d", est, expectedApprox)
	}

	// Zero total columns — returns full fileSize
	est2 := estimateColumnChunkBytes(100*1024, 0, 5, 4096)
	if est2 != 100*1024 {
		t.Errorf("zero total cols: expected full file size %d, got %d", 100*1024, est2)
	}

	// Footer larger than file — dataBytes clamped to fileSize
	est3 := estimateColumnChunkBytes(1000, 10, 2, 5000)
	if est3 < 0 {
		t.Errorf("negative footer size case returned negative estimate %d", est3)
	}

	// Projecting all columns — returns close to full file (minus footerSize rounding)
	est4 := estimateColumnChunkBytes(100*1024, 10, 10, 4096)
	if est4 != int64(100*1024) {
		t.Errorf("all columns: expected full file %d, got %d", 100*1024, est4)
	}
}
