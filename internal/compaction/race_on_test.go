//go:build race

package compaction

// raceEnabled scales the default size of the heavier randomized tests: under
// the race detector Parquet encode/decode is about 25x slower.
const raceEnabled = true
