package parquets3

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// TestCanSkipByColumnStats_LargeVolume runs canSkipByColumnStats 100000 times
// with random values and verifies it completes in < 100ms and returns correct results.
func TestCanSkipByColumnStats_LargeVolume(t *testing.T) {
	const iterations = 100_000

	// Use a fixed seed for reproducibility.
	rng := rand.New(rand.NewSource(42))

	// Pre-generate random test cases so generation time doesn't count.
	type testCase struct {
		value, minVal, maxVal string
	}
	cases := make([]testCase, iterations)
	chars := "abcdefghijklmnopqrstuvwxyz"
	for i := range cases {
		v := string(chars[rng.Intn(len(chars))]) + string(chars[rng.Intn(len(chars))])
		mn := string(chars[rng.Intn(len(chars))]) + string(chars[rng.Intn(len(chars))])
		mx := string(chars[rng.Intn(len(chars))]) + string(chars[rng.Intn(len(chars))])
		// Ensure mn <= mx (swap if needed).
		if mn > mx {
			mn, mx = mx, mn
		}
		cases[i] = testCase{value: v, minVal: mn, maxVal: mx}
	}

	start := time.Now()
	for _, tc := range cases {
		got := canSkipByColumnStats(tc.value, tc.minVal, tc.maxVal)
		// Verify correctness: must skip iff value is strictly outside [min, max].
		want := tc.value < tc.minVal || tc.value > tc.maxVal
		if got != want {
			t.Errorf("canSkipByColumnStats(%q, %q, %q) = %v, want %v",
				tc.value, tc.minVal, tc.maxVal, got, want)
		}
	}
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("100000 iterations took %v, want < 100ms", elapsed)
	}
}

// TestCanSkipByColumnStats_EdgeCases verifies correct behavior with empty strings,
// very long strings, unicode, and null bytes.
func TestCanSkipByColumnStats_EdgeCases(t *testing.T) {
	longStr := strings.Repeat("a", 1000)
	longStrB := strings.Repeat("b", 1000)
	unicodeMin := "αβγ"
	unicodeMax := "ωψφ"
	unicodeMid := "μνξ"

	tests := []struct {
		name        string
		value       string
		minVal      string
		maxVal      string
		wantSkip    bool
	}{
		// Empty stats — never skip.
		{
			name:     "both empty",
			value:    "anything",
			minVal:   "",
			maxVal:   "",
			wantSkip: false,
		},
		{
			name:     "empty minVal only",
			value:    "zeta",
			minVal:   "",
			maxVal:   "omega",
			wantSkip: false,
		},
		{
			name:     "empty maxVal only",
			value:    "alpha",
			minVal:   "alpha",
			maxVal:   "",
			wantSkip: false,
		},
		// Very long strings — value equals min.
		{
			name:     "long string at min boundary",
			value:    longStr,
			minVal:   longStr,
			maxVal:   longStrB,
			wantSkip: false,
		},
		// Very long strings — value below min.
		{
			name:     "long string below min",
			value:    longStr,
			minVal:   longStrB,
			maxVal:   longStrB,
			wantSkip: true,
		},
		// Very long strings — value above max.
		{
			name:     "long string above max",
			value:    longStrB,
			minVal:   longStr,
			maxVal:   longStr,
			wantSkip: true,
		},
		// Unicode values.
		{
			name:     "unicode value inside range",
			value:    unicodeMid,
			minVal:   unicodeMin,
			maxVal:   unicodeMax,
			wantSkip: false,
		},
		{
			name:     "unicode value below range",
			value:    unicodeMin,
			minVal:   unicodeMid,
			maxVal:   unicodeMax,
			wantSkip: true,
		},
		{
			name:     "unicode value above range",
			value:    unicodeMax,
			minVal:   unicodeMin,
			maxVal:   unicodeMid,
			wantSkip: true,
		},
		{
			name:     "unicode value at min",
			value:    unicodeMin,
			minVal:   unicodeMin,
			maxVal:   unicodeMax,
			wantSkip: false,
		},
		{
			name:     "unicode value at max",
			value:    unicodeMax,
			minVal:   unicodeMin,
			maxVal:   unicodeMax,
			wantSkip: false,
		},
		// Null bytes (valid Go strings).
		{
			name:     "null byte value inside range",
			value:    "\x00b",
			minVal:   "\x00a",
			maxVal:   "\x00c",
			wantSkip: false,
		},
		{
			name:     "null byte value below range",
			value:    "\x00",
			minVal:   "\x01",
			maxVal:   "\x02",
			wantSkip: true,
		},
		// Empty value.
		{
			name:     "empty value inside range",
			value:    "",
			minVal:   "",
			maxVal:   "z",
			wantSkip: false, // minVal is empty so function returns false
		},
		{
			name:     "empty value with valid range",
			value:    "",
			minVal:   "a",
			maxVal:   "z",
			wantSkip: true, // "" < "a"
		},
		// Single character.
		{
			name:     "single char exact match",
			value:    "m",
			minVal:   "m",
			maxVal:   "m",
			wantSkip: false,
		},
		{
			name:     "single char above single-value range",
			value:    "n",
			minVal:   "m",
			maxVal:   "m",
			wantSkip: true,
		},
		// Generated: 100 values, verify no panics.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Must not panic.
			got := canSkipByColumnStats(tt.value, tt.minVal, tt.maxVal)
			if got != tt.wantSkip {
				t.Errorf("canSkipByColumnStats(%q, %q, %q) = %v, want %v",
					tt.value, tt.minVal, tt.maxVal, got, tt.wantSkip)
			}
		})
	}

	// Smoke test: run through 100 generated edge-case strings to verify no panics.
	specialValues := []string{
		"", "\x00", "\xff", "\n", "\t", "\r",
		"\x00\x00\x00", "\xff\xff\xff",
		strings.Repeat("\x00", 100),
		strings.Repeat("\xff", 100),
	}
	for i, sv := range specialValues {
		t.Run(fmt.Sprintf("special_%d", i), func(t *testing.T) {
			// Should not panic regardless of input.
			_ = canSkipByColumnStats(sv, sv, sv)
			_ = canSkipByColumnStats(sv, "", sv)
			_ = canSkipByColumnStats(sv, sv, "")
		})
	}
}
