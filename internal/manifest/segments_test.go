package manifest

import (
	"fmt"
	"testing"
	"time"
)

func TestSegmentNonceOfKey(t *testing.T) {
	const n = "65000000aaaabbbb"
	for _, tc := range []struct {
		key  string
		want string
	}{
		{"0/0/logs/dt=2026-10-02/hour=06/" + n + "-0.parquet", n},
		{"logs/dt=2026-10-02/hour=06/" + n + "-1a.parquet", n},
		{n + "-3.parquet", n},
		{"logs/dt=2026-10-02/hour=06/0123456789abcdef.parquet", ""},    // a compaction output or an earlier release
		{"logs/dt=2026-10-02/hour=06/" + n + ".parquet", ""},           // no slice
		{"logs/dt=2026-10-02/hour=06/65000000AAAABBBB-0.parquet", ""},  // not lower-case hex
		{"logs/dt=2026-10-02/hour=06/65000000aaaabbb-0.parquet", ""},   // 15 characters
		{"logs/dt=2026-10-02/hour=06/65000000aaaabbbbc-0.parquet", ""}, // 17 characters
		{"logs/dt=2026-10-02/hour=06/6500000zaaaabbbb-0.parquet", ""},  // not hex
		{"logs/dt=2026-10-02/hour=06/x/" + n + "-0.parquet", n},        // only the file name counts
		{"logs/" + n + "-0/file.parquet", ""},                          // a directory is not a nonce
		{"", ""},
	} {
		if got := SegmentNonceOfKey(tc.key); got != tc.want {
			t.Errorf("SegmentNonceOfKey(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestSegmentNonceTime(t *testing.T) {
	want := time.Unix(0x65000000, 0)
	if got := SegmentNonceTime("65000000aaaabbbb"); !got.Equal(want) {
		t.Errorf("SegmentNonceTime = %v, want %v", got, want)
	}
	for _, bad := range []string{"", "65000000", "65000000aaaabbbbcc", "zz000000aaaabbbb"} {
		if got := SegmentNonceTime(bad); !got.IsZero() {
			t.Errorf("SegmentNonceTime(%q) = %v, want zero", bad, got)
		}
	}
}

func TestSegmentMarkerKey(t *testing.T) {
	if got := SegmentMarkerKey("0/0/logs/", "65000000aaaabbbb"); got != "0/0/logs/_segments/65000000aaaabbbb" {
		t.Errorf("SegmentMarkerKey = %q", got)
	}
}

// An object of a buffer segment is released for merging or rewriting only once
// its segment is committed AND the protection (the insert pod's grace, with
// margin) has passed, or — whatever its marker says — once the segment is old
// enough that its owner is presumed gone. Every other object is always free.
func TestSegmentGuard_Released(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	nonceAt := func(age time.Duration) string {
		return fmt.Sprintf("%08x%08x", uint32(now.Add(-age).Unix()), uint32(0xdeadbeef))
	}
	keyOf := func(nonce string) string { return "logs/dt=2026-10-02/hour=10/" + nonce + "-0.parquet" }
	protect := 5 * time.Minute

	recent := nonceAt(10 * time.Minute)
	old := nonceAt(SegmentReleaseAfter + time.Hour)

	type tc struct {
		name  string
		guard *SegmentGuard
		key   string
		want  bool
	}
	cases := []tc{
		{"no nonce is always free, with no guard", nil, "logs/dt=2026-10-02/hour=10/0123456789abcdef.parquet", true},
		{"no nonce is always free, with a failed listing", &SegmentGuard{Protect: protect}, "logs/dt=2026-10-02/hour=10/0123456789abcdef.parquet", true},
		{"a segment object with no guard waits", nil, keyOf(recent), false},
		{"a failed marker listing releases nothing", &SegmentGuard{Protect: protect, Listed: false}, keyOf(recent), false},
		{"no marker yet: the segment is not committed", &SegmentGuard{Protect: protect, Listed: true, Markers: map[string]time.Time{}}, keyOf(recent), false},
		{"committed a moment ago: the pod still serves it", &SegmentGuard{Protect: protect, Listed: true, Markers: map[string]time.Time{recent: now.Add(-time.Minute)}}, keyOf(recent), false},
		{"committed exactly the protection ago", &SegmentGuard{Protect: protect, Listed: true, Markers: map[string]time.Time{recent: now.Add(-protect)}}, keyOf(recent), true},
		{"committed long ago", &SegmentGuard{Protect: protect, Listed: true, Markers: map[string]time.Time{recent: now.Add(-time.Hour)}}, keyOf(recent), true},
		{"another segment's marker does not release it", &SegmentGuard{Protect: protect, Listed: true, Markers: map[string]time.Time{nonceAt(time.Hour): now.Add(-time.Hour)}}, keyOf(recent), false},
		{"older than the release age: freed without a marker", nil, keyOf(old), true},
		{"older than the release age: freed with a failed listing", &SegmentGuard{Protect: protect}, keyOf(old), true},
		{"older than the release age: freed with a fresh marker", &SegmentGuard{Protect: protect, Listed: true, Markers: map[string]time.Time{old: now.Add(-time.Second)}}, keyOf(old), true},
	}
	for _, c := range cases {
		if got := c.guard.Released(c.key, now); got != c.want {
			t.Errorf("%s: Released = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestSegmentGuard_ReleasedFilesKeepsOnlyTheFreeOnes(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	live := fmt.Sprintf("%08x%08x", uint32(now.Add(-time.Hour).Unix()), uint32(1))
	done := fmt.Sprintf("%08x%08x", uint32(now.Add(-2*time.Hour).Unix()), uint32(2))
	g := &SegmentGuard{Protect: time.Minute, Listed: true, Markers: map[string]time.Time{done: now.Add(-time.Hour)}}
	files := []FileInfo{
		{Key: "logs/dt=2026-10-02/hour=10/" + live + "-0.parquet"},
		{Key: "logs/dt=2026-10-02/hour=10/" + done + "-0.parquet"},
		{Key: "logs/dt=2026-10-02/hour=10/0123456789abcdef.parquet"},
	}
	got := g.ReleasedFiles(files, now)
	if len(got) != 2 || got[0].Key != files[1].Key || got[1].Key != files[2].Key {
		t.Errorf("ReleasedFiles = %v", got)
	}
	if len(files) != 3 || files[0].Key == "" {
		t.Error("the input slice was modified")
	}
}

// When the guard releases every file it hands back the caller's slice: the
// compaction planner passes the manifest's own slices (RangePartitions) and
// must not pay a copy of every partition on every scan. When it drops one,
// the input is left as it was.
func TestSegmentGuard_ReleasedFilesCopiesOnlyWhenItDrops(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	g := &SegmentGuard{Protect: time.Minute, Listed: true}
	free := []FileInfo{
		{Key: "logs/dt=2026-10-02/hour=10/0123456789abcdef.parquet"},
		{Key: "logs/dt=2026-10-02/hour=10/compacted-L1.parquet"},
	}
	got := g.ReleasedFiles(free, now)
	if len(got) != 2 || &got[0] != &free[0] {
		t.Fatalf("all released: got %v, want the input slice itself", got)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = g.ReleasedFiles(free, now) }); allocs != 0 {
		t.Errorf("all released: %v allocations per call, want 0", allocs)
	}
	live := fmt.Sprintf("%08x%08x", uint32(now.Add(-time.Hour).Unix()), uint32(1))
	mixed := []FileInfo{free[0], {Key: "logs/dt=2026-10-02/hour=10/" + live + "-0.parquet"}, free[1]}
	got = g.ReleasedFiles(mixed, now)
	if len(got) != 2 || got[0].Key != free[0].Key || got[1].Key != free[1].Key {
		t.Fatalf("one held: got %v", got)
	}
	if mixed[1].Key == free[1].Key || len(mixed) != 3 {
		t.Error("the input slice was modified")
	}
	var nilGuard *SegmentGuard
	if got := nilGuard.ReleasedFiles(mixed, now); len(got) != 2 {
		t.Errorf("nil guard: got %d files, want the 2 without a live nonce", len(got))
	}
}
