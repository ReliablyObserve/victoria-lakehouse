package schema

import "testing"

// The shuffled fixtures deliberately place the true MIN and MAX away from the
// first/last positions so a positional rows[0]/rows[len-1] derivation would
// return the WRONG values — the regression these helpers exist to prevent
// (parquet-compression-research.md, trap 1).

func TestLogRowTimeBounds_Shuffled(t *testing.T) {
	rows := []LogRow{
		{TimestampUnixNano: 3000},
		{TimestampUnixNano: 5000}, // true max, middle position
		{TimestampUnixNano: 1000}, // true min, middle position
		{TimestampUnixNano: 4000},
		{TimestampUnixNano: 2000},
	}
	minNs, maxNs := LogRowTimeBounds(rows)
	if minNs != 1000 || maxNs != 5000 {
		t.Fatalf("LogRowTimeBounds = (%d, %d), want (1000, 5000)", minNs, maxNs)
	}
	// Absent-value guards: the positional derivation would have produced these.
	if minNs == rows[0].TimestampUnixNano {
		t.Errorf("min %d equals rows[0] — fixture must keep the true min off the first position", minNs)
	}
	if maxNs == rows[len(rows)-1].TimestampUnixNano {
		t.Errorf("max %d equals rows[len-1] — fixture must keep the true max off the last position", maxNs)
	}
}

func TestTraceRowTimeBounds_Shuffled(t *testing.T) {
	rows := []TraceRow{
		{TimestampUnixNano: 700},
		{TimestampUnixNano: 100}, // true min, middle position
		{TimestampUnixNano: 900}, // true max, middle position
		{TimestampUnixNano: 300},
	}
	minNs, maxNs := TraceRowTimeBounds(rows)
	if minNs != 100 || maxNs != 900 {
		t.Fatalf("TraceRowTimeBounds = (%d, %d), want (100, 900)", minNs, maxNs)
	}
	if minNs == rows[0].TimestampUnixNano || maxNs == rows[len(rows)-1].TimestampUnixNano {
		t.Errorf("bounds (%d, %d) match positional first/last — fixture regressed", minNs, maxNs)
	}
}

func TestLogRowTimeBounds_EmptyAndSingle(t *testing.T) {
	if minNs, maxNs := LogRowTimeBounds(nil); minNs != 0 || maxNs != 0 {
		t.Errorf("empty LogRowTimeBounds = (%d, %d), want (0, 0)", minNs, maxNs)
	}
	if minNs, maxNs := LogRowTimeBounds([]LogRow{{TimestampUnixNano: 42}}); minNs != 42 || maxNs != 42 {
		t.Errorf("single LogRowTimeBounds = (%d, %d), want (42, 42)", minNs, maxNs)
	}
}

func TestTraceRowTimeBounds_EmptyAndSingle(t *testing.T) {
	if minNs, maxNs := TraceRowTimeBounds(nil); minNs != 0 || maxNs != 0 {
		t.Errorf("empty TraceRowTimeBounds = (%d, %d), want (0, 0)", minNs, maxNs)
	}
	if minNs, maxNs := TraceRowTimeBounds([]TraceRow{{TimestampUnixNano: 7}}); minNs != 7 || maxNs != 7 {
		t.Errorf("single TraceRowTimeBounds = (%d, %d), want (7, 7)", minNs, maxNs)
	}
}
