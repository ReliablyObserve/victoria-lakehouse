package schema

// LogRowTimeBounds returns the true minimum and maximum TimestampUnixNano
// across rows in a single O(n) scan. Callers MUST NOT derive file time bounds
// positionally (rows[0] / rows[len-1]): once rows are ordered by
// (stream_id, timestamp) for compression, the first/last positions no longer
// hold the extremes — an understated MaxTimeNs (or overstated MinTimeNs) in
// manifest.FileInfo lets time-range pruning silently skip files containing
// matches, and re-opens the buffer↔Parquet double-count the bufferWatermark
// closed. Returns (0, 0) for an empty slice.
//
// Used by both Go modules (lakehouse-traces imports this package) — keep the
// flush/compaction call sites in sync across the twins.
func LogRowTimeBounds(rows []LogRow) (minNs, maxNs int64) {
	if len(rows) == 0 {
		return 0, 0
	}
	minNs = rows[0].TimestampUnixNano
	maxNs = minNs
	for i := 1; i < len(rows); i++ {
		ts := rows[i].TimestampUnixNano
		if ts < minNs {
			minNs = ts
		}
		if ts > maxNs {
			maxNs = ts
		}
	}
	return minNs, maxNs
}

// TraceRowTimeBounds is the TraceRow twin of LogRowTimeBounds — same O(n)
// scan, same sort-order-robustness contract. Returns (0, 0) for an empty
// slice.
func TraceRowTimeBounds(rows []TraceRow) (minNs, maxNs int64) {
	if len(rows) == 0 {
		return 0, 0
	}
	minNs = rows[0].TimestampUnixNano
	maxNs = minNs
	for i := 1; i < len(rows); i++ {
		ts := rows[i].TimestampUnixNano
		if ts < minNs {
			minNs = ts
		}
		if ts > maxNs {
			maxNs = ts
		}
	}
	return minNs, maxNs
}
