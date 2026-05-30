package delete

import (
	"testing"
)

// TestAffectsFile_InvertedRange verifies that a tombstone with StartNs > EndNs
// (an invalid/inverted range) does not incorrectly affect files. The current
// AffectsFile implementation has no validation for StartNs < EndNs.
func TestAffectsFile_InvertedRange(t *testing.T) {
	ts := Tombstone{
		ID:      "inverted",
		StartNs: 1000,
		EndNs:   500, // EndNs < StartNs = inverted
	}

	// An inverted range should not affect any file, since it represents
	// an impossible time interval.
	if ts.AffectsFile(0, 2000) {
		t.Error("tombstone with inverted range (start > end) should not affect any file; " +
			"BUG: AffectsFile has no StartNs <= EndNs validation")
	}
	if ts.AffectsFile(600, 900) {
		t.Error("tombstone with inverted range should not affect file within 'gap'")
	}
	if ts.AffectsFile(0, 10000) {
		t.Error("tombstone with inverted range should not affect any file regardless of bounds")
	}
}

// TestAffectsFile_ZeroRange verifies that a point tombstone (StartNs == EndNs)
// correctly affects files containing that exact timestamp.
func TestAffectsFile_ZeroRange(t *testing.T) {
	ts := Tombstone{
		ID:      "point",
		StartNs: 500,
		EndNs:   500,
	}

	// File range [0, 1000] contains timestamp 500
	if !ts.AffectsFile(0, 1000) {
		t.Error("point tombstone should affect file containing that timestamp")
	}

	// File range [501, 1000] does NOT contain timestamp 500
	if ts.AffectsFile(501, 1000) {
		t.Error("point tombstone should not affect file starting after the point timestamp")
	}

	// File range [0, 499] does NOT contain timestamp 500
	if ts.AffectsFile(0, 499) {
		t.Error("point tombstone should not affect file ending before the point timestamp")
	}

	// Exact match: file is [500, 500]
	if !ts.AffectsFile(500, 500) {
		t.Error("point tombstone should affect single-nanosecond file at same timestamp")
	}
}

// TestAffectsFile_EpochZero verifies tombstones at epoch 0 work correctly.
func TestAffectsFile_EpochZero(t *testing.T) {
	ts := Tombstone{
		ID:      "epoch-zero",
		StartNs: 0,
		EndNs:   100,
	}

	if !ts.AffectsFile(0, 200) {
		t.Error("epoch-zero tombstone should affect overlapping file")
	}
	if !ts.AffectsFile(0, 50) {
		t.Error("epoch-zero tombstone should affect file fully within tombstone range")
	}
	if !ts.AffectsFile(50, 150) {
		t.Error("epoch-zero tombstone should affect partially overlapping file")
	}
	if ts.AffectsFile(101, 200) {
		t.Error("epoch-zero tombstone should not affect file starting after tombstone ends")
	}
}

// TestAffectsFile_MaxInt64 verifies tombstones at maximum timestamp work correctly.
func TestAffectsFile_MaxInt64(t *testing.T) {
	maxNs := int64(1<<63 - 1) // math.MaxInt64
	ts := Tombstone{
		ID:      "max-ts",
		StartNs: maxNs - 100,
		EndNs:   maxNs,
	}

	if !ts.AffectsFile(maxNs-200, maxNs) {
		t.Error("tombstone at max int64 should affect overlapping file")
	}
	if ts.AffectsFile(0, maxNs-101) {
		t.Error("tombstone at max int64 should not affect file ending before its start")
	}
}

// TestMatchesRow_InvertedRange verifies that MatchesRow with an inverted
// tombstone range does not match any row. The timestamp check in MatchesRow
// (timestampNs < t.StartNs || timestampNs > t.EndNs) should reject all
// timestamps when StartNs > EndNs, since no value can be both >= 1000 and <= 500.
func TestMatchesRow_InvertedRange(t *testing.T) {
	ts := Tombstone{
		ID:      "inverted-match",
		Query:   "*",
		StartNs: 1000,
		EndNs:   500, // inverted
	}

	// Test timestamps within, before, and after the inverted range
	testTimestamps := []int64{0, 250, 500, 750, 1000, 1500}
	for _, tsNs := range testTimestamps {
		row := map[string]string{"_msg": "test"}
		if ts.MatchesRow(row, tsNs) {
			t.Errorf("tombstone with inverted range should not match any row; matched at timestamp %d", tsNs)
		}
	}
}

// TestMatchesRow_EpochZero verifies that rows at epoch-zero timestamp are
// correctly matched by a tombstone covering epoch zero.
func TestMatchesRow_EpochZero(t *testing.T) {
	ts := Tombstone{
		ID:      "epoch-zero-match",
		Query:   "*",
		StartNs: 0,
		EndNs:   100,
	}

	row := map[string]string{"_msg": "epoch data"}
	if !ts.MatchesRow(row, 0) {
		t.Error("tombstone at epoch zero should match row at timestamp 0")
	}
	if !ts.MatchesRow(row, 50) {
		t.Error("tombstone at epoch zero should match row at timestamp 50")
	}
	if !ts.MatchesRow(row, 100) {
		t.Error("tombstone at epoch zero should match row at timestamp 100 (inclusive end)")
	}
	if ts.MatchesRow(row, 101) {
		t.Error("tombstone at epoch zero should not match row at timestamp 101")
	}
}

// TestAffectsFile_BothZero verifies that a tombstone with both StartNs=0 and
// EndNs=0 (point tombstone at epoch zero) correctly handles epoch-zero files.
func TestAffectsFile_BothZero(t *testing.T) {
	ts := Tombstone{
		ID:      "both-zero",
		StartNs: 0,
		EndNs:   0,
	}

	if !ts.AffectsFile(0, 100) {
		t.Error("tombstone [0,0] should affect file starting at 0")
	}
	if ts.AffectsFile(1, 100) {
		t.Error("tombstone [0,0] should not affect file starting at 1")
	}
}

// TestForRange_InvertedRange verifies that ForRange handles inverted tombstone
// ranges without panicking or returning incorrect results.
func TestForRange_InvertedRange(t *testing.T) {
	store := NewTombstoneStore()

	// Add a tombstone with inverted range
	store.Add(Tombstone{ID: "inverted", StartNs: 1000, EndNs: 500})
	// Add a normal tombstone for comparison
	store.Add(Tombstone{ID: "normal", StartNs: 200, EndNs: 800})

	// The inverted tombstone's AffectsFile check (1000 <= fileMax && 500 >= fileMin)
	// may accidentally match when it shouldn't. Test this.
	result := store.ForRange(600, 900)

	// Only the normal tombstone should match
	for _, ts := range result {
		if ts.ID == "inverted" {
			t.Error("inverted tombstone should not be returned by ForRange; " +
				"BUG: AffectsFile has no range validation")
		}
	}
}
