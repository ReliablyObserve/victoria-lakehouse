package manifest

import (
	"fmt"
	"sort"
	"testing"
)

// TestKeysUnderPrefix_DateBucketFastPath pins the partition-scoped
// fast path. A prefix containing "dt=YYYY-MM-DD" must only iterate the
// matching hourly partitions — at PB-scale this is the difference
// between O(50M) and O(2K) file scans.
func TestKeysUnderPrefix_DateBucketFastPath(t *testing.T) {
	m := New("b", "")

	// Spread keys across multiple dates. Only one matches the prefix.
	m.AddFile("dt=2026-06-04/hour=00", FileInfo{Key: "0/0/traces/dt=2026-06-04/hour=00/a.parquet"})
	m.AddFile("dt=2026-06-04/hour=12", FileInfo{Key: "0/0/traces/dt=2026-06-04/hour=12/b.parquet"})
	m.AddFile("dt=2026-06-04/hour=23", FileInfo{Key: "0/0/traces/dt=2026-06-04/hour=23/c.parquet"})
	m.AddFile("dt=2026-06-05/hour=00", FileInfo{Key: "0/0/traces/dt=2026-06-05/hour=00/d.parquet"})
	m.AddFile("dt=2026-06-03/hour=15", FileInfo{Key: "0/0/traces/dt=2026-06-03/hour=15/e.parquet"})

	got := m.KeysUnderPrefix("0/0/traces/dt=2026-06-04/")
	sort.Strings(got)
	want := []string{
		"0/0/traces/dt=2026-06-04/hour=00/a.parquet",
		"0/0/traces/dt=2026-06-04/hour=12/b.parquet",
		"0/0/traces/dt=2026-06-04/hour=23/c.parquet",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d:\n  got: %v\n  want: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q want %q", i, got[i], want[i])
		}
	}
}

// TestKeysUnderPrefix_AgreesWithLegacyOnNonDatePrefix verifies the
// fallback (non-date prefix) path returns the same keys the legacy
// full-scan would. Critical for admin tooling and any caller that
// passes a non-bucketed prefix.
func TestKeysUnderPrefix_AgreesWithLegacyOnNonDatePrefix(t *testing.T) {
	m := New("b", "")
	m.AddFile("dt=2026-06-04/hour=00", FileInfo{Key: "0/0/traces/dt=2026-06-04/hour=00/a.parquet"})
	m.AddFile("dt=2026-06-04/hour=12", FileInfo{Key: "1/1/traces/dt=2026-06-04/hour=12/b.parquet"})
	m.AddFile("dt=2026-06-04/hour=23", FileInfo{Key: "1001/0/logs/dt=2026-06-04/hour=23/c.parquet"})

	// Prefix with no dt= segment. Must still find the matching keys.
	got := m.KeysUnderPrefix("1/1/")
	if len(got) != 1 || got[0] != "1/1/traces/dt=2026-06-04/hour=12/b.parquet" {
		t.Errorf("non-date prefix scan wrong: %v", got)
	}

	// Empty prefix returns every key.
	all := m.KeysUnderPrefix("")
	if len(all) != 3 {
		t.Errorf("empty prefix should return all 3 keys, got %d", len(all))
	}
}

// TestHasKey_O1Lookup pins the byKey-backed point lookup. Used by the
// orphan sweep's third safety gate; must be cheap so the gate doesn't
// dominate the sweep's wall clock at PB-scale.
func TestHasKey_O1Lookup(t *testing.T) {
	m := New("b", "")
	m.AddFile("dt=2026-06-04/hour=00", FileInfo{Key: "a"})
	m.AddFile("dt=2026-06-04/hour=12", FileInfo{Key: "b"})

	if !m.HasKey("a") {
		t.Error("HasKey(a) returned false on present key")
	}
	if !m.HasKey("b") {
		t.Error("HasKey(b) returned false on present key")
	}
	if m.HasKey("ghost") {
		t.Error("HasKey(ghost) returned true on absent key")
	}

	m.RemoveFile("dt=2026-06-04/hour=00", "a")
	if m.HasKey("a") {
		t.Error("HasKey(a) returned true after RemoveFile")
	}
}

// TestExtractDateFromPrefix_HandlesMalformedInput guards the
// fast-path predicate against accidental matches that would route a
// non-date prefix down the partition-scoped path and miss keys.
func TestExtractDateFromPrefix_HandlesMalformedInput(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"0/0/traces/dt=2026-06-04/", "2026-06-04"},
		{"0/0/traces/dt=2026-06-04/hour=12/", "2026-06-04"},
		{"dt=2026-06-04", "2026-06-04"},
		{"0/0/dt=20260604/", ""},        // missing hyphens — must NOT match
		{"0/0/dt=2026-06/", ""},         // too short
		{"0/0/traces/", ""},             // no dt= segment
		{"", ""},                        // empty
		{"dt=abcd-ef-gh", ""},           // non-digit positions → reject (tightened post-security review)
		{"dt=2026-13-99", "2026-13-99"}, // digits-only — passes the cheap check; month/day range is the caller's problem
		{"dt=2026/06/04", ""},           // wrong separator
	}
	for _, tc := range cases {
		got := extractDateFromPrefix(tc.in)
		if got != tc.want {
			t.Errorf("extractDateFromPrefix(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestKeysUnderPrefix_DateFastPathSubsetOfFallback pins the
// equivalence invariant: for any date prefix, the fast path must
// return EXACTLY the same set the legacy full-scan would. Any drift
// here silently breaks orphan-sweep correctness — a false-positive
// orphan deletion is a data-loss bug.
func TestKeysUnderPrefix_DateFastPathSubsetOfFallback(t *testing.T) {
	m := New("b", "")

	// Build a non-trivial corpus mixing dates and tenants.
	dates := []string{"2026-06-03", "2026-06-04", "2026-06-05"}
	tenants := []string{"0/0", "1/1", "1001/0"}
	idx := 0
	for _, d := range dates {
		for h := 0; h < 4; h++ {
			for _, tk := range tenants {
				m.AddFile(
					fmt.Sprintf("dt=%s/hour=%02d", d, h),
					FileInfo{Key: fmt.Sprintf("%s/traces/dt=%s/hour=%02d/f%d.parquet", tk, d, h, idx)},
				)
				idx++
			}
		}
	}

	for _, d := range dates {
		prefix := fmt.Sprintf("0/0/traces/dt=%s/", d)

		// Fast path (with date detection).
		fast := m.KeysUnderPrefix(prefix)

		// Legacy-equivalent: full scan with HasPrefix filter.
		var legacy []string
		for _, files := range m.files {
			for _, fi := range files {
				if len(prefix) == 0 || (len(fi.Key) >= len(prefix) && fi.Key[:len(prefix)] == prefix) {
					legacy = append(legacy, fi.Key)
				}
			}
		}

		sort.Strings(fast)
		sort.Strings(legacy)
		if len(fast) != len(legacy) {
			t.Errorf("date=%s: fast=%d, legacy=%d", d, len(fast), len(legacy))
			continue
		}
		for i := range fast {
			if fast[i] != legacy[i] {
				t.Errorf("date=%s [%d]: fast=%q legacy=%q", d, i, fast[i], legacy[i])
			}
		}
	}
}
