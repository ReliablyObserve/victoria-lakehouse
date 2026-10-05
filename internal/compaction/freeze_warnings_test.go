package compaction

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestRollupWindowConflict guards the predicate: the rollup runs from the
// (one-hour floored) rollup age up to the freeze age, so a freeze age at or
// below the rollup age leaves it no window; 0 disables the rollup and cannot
// conflict.
func TestRollupWindowConflict(t *testing.T) {
	const day = 24 * time.Hour
	for _, c := range []struct {
		freeze, rollup time.Duration
		want           bool
	}{
		{28 * day, 24 * time.Hour, false},
		{25 * time.Hour, 24 * time.Hour, false},
		{24 * time.Hour, 24 * time.Hour, true},
		{12 * time.Hour, 24 * time.Hour, true}, // 1-day rule: freeze at 12h
		{5 * day, 24 * time.Hour, false},
		{2 * time.Hour, 24 * time.Hour, true},
		{30 * time.Minute, 10 * time.Minute, true},  // rollup floored to 1h
		{90 * time.Minute, 10 * time.Minute, false}, // 90m > floored 1h
		{time.Hour, 10 * time.Minute, true},
		{time.Hour, 0, false},
		{0, 0, false},
	} {
		if got := RollupWindowConflict(c.freeze, c.rollup); got != c.want {
			t.Errorf("RollupWindowConflict(%v, %v) = %v, want %v", c.freeze, c.rollup, got, c.want)
		}
	}
}

// TestRollupConflictWarnings guards the startup lines: one per conflicting
// scope (global rules, each tenant override), none for a healthy freeze, a nil
// freeze or INTELLIGENT_TIERING-only rules.
func TestRollupConflictWarnings(t *testing.T) {
	d := delete.NewStorageClassDetector(rules(30, delete.ClassStandardIA)) // freezes at 28d: fine
	d.SetTenantRules(map[uint32]map[uint32][]delete.LifecycleRule{
		1001: {0: rules(1, delete.ClassGlacier)},            // freeze at 12h: conflicts
		1002: {0: rules(10, delete.ClassGlacier)},           // 8d: fine
		1003: {0: rules(2, delete.ClassIntelligentTiering)}, // no frozen class
	})
	f := &LifecycleFreeze{Detector: d}
	got := f.RollupConflictWarnings(24*time.Hour, []string{"1001/0/", "1002/0/", "1003/0/"})
	if len(got) != 1 || !strings.Contains(got[0], "tenant 1001/0/") || !strings.Contains(got[0], "12h0m0s") {
		t.Fatalf("warnings = %q, want one for tenant 1001/0/ at 12h", got)
	}

	global := &LifecycleFreeze{ExtraRules: rules(2, delete.ClassGlacier)} // 2d rule: margin capped to 1d -> freeze 1d
	got = global.RollupConflictWarnings(24*time.Hour, nil)
	if len(got) != 1 || !strings.Contains(got[0], "global lifecycle rules") {
		t.Fatalf("global warning = %q", got)
	}
	if len(global.RollupConflictWarnings(0, nil)) != 0 {
		t.Fatal("rollup disabled must not warn")
	}
	var nilFreeze *LifecycleFreeze
	if nilFreeze.RollupConflictWarnings(24*time.Hour, []string{"1/0/"}) != nil {
		t.Fatal("nil freeze warned")
	}
	if w := (&LifecycleFreeze{ExtraRules: rules(1, delete.ClassIntelligentTiering)}).RollupConflictWarnings(24*time.Hour, nil); len(w) != 0 {
		t.Fatalf("INTELLIGENT_TIERING-only rules warned: %q", w)
	}
}

// TestFreezeVsRollupWindow documents the behaviour the warning exists for (R2):
// with a Glacier transition at or below the rollup age (here 1 and 2 days with
// the 24 h rollup) a quiet tenant's three small L0 files, below min_files_l0,
// are never merged, because the partition is frozen before it is old enough
// to roll up; with a transition far enough out they merge at the rollup age.
func TestFreezeVsRollupWindow(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		for _, tc := range []struct {
			days       int
			wantMerged bool
		}{{1, false}, {2, false}, {3, true}, {7, true}, {30, true}} {
			t.Run(fmt.Sprintf("glacier_at_%dd", tc.days), func(t *testing.T) {
				now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
				old := planClock
				planClock = func() time.Time { return now }
				t.Cleanup(func() { planClock = old })
				w := newPlanWorld(t, mode)
				p := partitionAt(now.Add(-2 * time.Hour))
				w.add("1001/0", p, 0, 3, 2, nil)
				s := w.shippedScheduler()
				s.freeze = &LifecycleFreeze{Detector: delete.NewStorageClassDetector(rules(tc.days, delete.ClassGlacier))}
				warns := s.freeze.RollupConflictWarnings(24*time.Hour, nil)
				if (len(warns) > 0) == tc.wantMerged {
					t.Fatalf("warning=%v but rollup window open=%v", warns, tc.wantMerged)
				}
				merged := false
				for step := 0; step < 24*35; step++ {
					now = now.Add(time.Hour)
					n, err := s.Scan(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					merged = merged || n > 0
				}
				if merged != tc.wantMerged {
					pt, _ := manifest.ParsePartitionTime(p)
					t.Fatalf("transition %dd: merged=%v, want %v (partition %v)", tc.days, merged, tc.wantMerged, pt)
				}
			})
		}
	})
}
