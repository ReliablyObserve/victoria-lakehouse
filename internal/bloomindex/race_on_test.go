//go:build race

package bloomindex

// raceEnabled scales down the heavier randomized tests: the race detector makes
// the decoder-heavy differential test roughly 20x slower.
const raceEnabled = true
