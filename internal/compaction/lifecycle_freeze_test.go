package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

const day = 24 * time.Hour

func tenantFile(acct int, name string) manifest.FileInfo {
	return manifest.FileInfo{Key: fmt.Sprintf("%d/0/logs/dt=2026-01-01/hour=00/%s.parquet", acct, name)}
}

func TestFreeze_RecordedStorageClass(t *testing.T) {
	// Guards the recorded-class check: STANDARD and INTELLIGENT_TIERING stay
	// rewritable; every other class is frozen, even with no freeze configured
	// (nil receiver). Without it tiered objects are rewritten at a retrieval
	// fee plus early-deletion charge, and fail outright in Glacier/Deep Archive.
	cases := map[string]bool{
		"": false, "STANDARD": false, "INTELLIGENT_TIERING": false, "standard": false,
		"STANDARD_IA": true, "ONEZONE_IA": true, "GLACIER_IR": true, "GLACIER": true, "DEEP_ARCHIVE": true,
		"standard_ia": true,
	}
	var nilFreeze *LifecycleFreeze
	withDetector := &LifecycleFreeze{Detector: delete.NewStorageClassDetector(nil)}
	for class, want := range cases {
		fi := tenantFile(1, "a")
		fi.StorageClass = class
		for name, f := range map[string]*LifecycleFreeze{"nil": nilFreeze, "detector": withDetector} {
			got, reason := f.frozen(fi, unitNow, unitNow)
			if got != want {
				t.Errorf("%s freeze, class %q: frozen=%v, want %v", name, class, got, want)
			}
			if got && reason != frozenStorageClass {
				t.Errorf("class %q: reason %q", class, reason)
			}
		}
	}
}

func TestFreeze_DetectorCachedClass(t *testing.T) {
	// Guards the cached-class check: a class the delete path has already
	// learned for a key freezes it even in a brand-new partition.
	d := delete.NewStorageClassDetector(nil)
	f := &LifecycleFreeze{Detector: d}
	fi := tenantFile(1, "a")
	if ok, _ := f.frozen(fi, unitNow, unitNow); ok {
		t.Fatal("frozen with nothing cached")
	}
	d.SetCache(fi.Key, delete.ClassGlacier)
	if ok, reason := f.frozen(fi, unitNow, unitNow); !ok || reason != frozenStorageClass {
		t.Fatalf("cached GLACIER: frozen=%v reason=%q", ok, reason)
	}
	d.SetCache(fi.Key, delete.ClassStandard)
	if ok, _ := f.frozen(fi, unitNow, unitNow); ok {
		t.Fatal("cached STANDARD must not freeze")
	}
}

func rules(pairs ...any) []delete.LifecycleRule {
	var out []delete.LifecycleRule
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, delete.LifecycleRule{TransitionDays: pairs[i].(int), Class: pairs[i+1].(delete.StorageClass)})
	}
	return out
}

func TestFreeze_AgeAgainstFirstTransition(t *testing.T) {
	// Guards the age freeze: a partition past (first non-rewritable transition
	// - margin) is frozen. 30-day IA rule, default 48 h margin -> frozen from day 28.
	f := &LifecycleFreeze{Detector: delete.NewStorageClassDetector(rules(30, delete.ClassStandardIA))}
	fi := tenantFile(1, "a")
	for _, tc := range []struct {
		age  time.Duration
		want bool
	}{
		{20 * day, false}, {28*day - time.Minute, false}, {28 * day, true}, {40 * day, true},
	} {
		got, reason := f.frozen(fi, unitNow.Add(-tc.age), unitNow)
		if got != tc.want || (got && reason != frozenAge) {
			t.Errorf("age %s: frozen=%v reason=%q, want %v", tc.age, got, reason, tc.want)
		}
	}
}

func TestFreeze_Margin(t *testing.T) {
	// Guards the margin arithmetic: explicit margin, the 48 h default, the
	// half-transition clamp (a 1-day rule freezes at 12 h, not at 0), and a
	// 0-day rule (freezes at once).
	fi := tenantFile(1, "a")
	at := func(f *LifecycleFreeze, age time.Duration) bool {
		ok, _ := f.frozen(fi, unitNow.Add(-age), unitNow)
		return ok
	}
	ia := func(days int) *delete.StorageClassDetector {
		return delete.NewStorageClassDetector(rules(days, delete.ClassStandardIA))
	}
	custom := &LifecycleFreeze{Detector: ia(30), Margin: 24 * time.Hour}
	if at(custom, 29*day-time.Minute) || !at(custom, 29*day) {
		t.Error("custom 24h margin must freeze from day 29")
	}
	oneDay := &LifecycleFreeze{Detector: ia(1)}
	if at(oneDay, 12*time.Hour-time.Minute) || !at(oneDay, 12*time.Hour) {
		t.Error("1-day rule must clamp the margin to half and freeze from 12h")
	}
	zero := &LifecycleFreeze{Detector: ia(0)}
	if !at(zero, 0) {
		t.Error("a 0-day rule freezes immediately")
	}
	negative := &LifecycleFreeze{Detector: ia(30), Margin: -time.Hour}
	if at(negative, 28*day-time.Minute) || !at(negative, 28*day) {
		t.Error("a non-positive margin falls back to the 48h default")
	}
}

func TestFreeze_NoNonRewritableRuleNoAgeFreeze(t *testing.T) {
	// Guards CanRewrite in FirstNonRewritableTransition: INTELLIGENT_TIERING
	// and STANDARD rules never freeze (those objects stay rewritable), and no
	// rules at all means no age freeze.
	fi := tenantFile(1, "a")
	old := unitNow.Add(-400 * day)
	for name, f := range map[string]*LifecycleFreeze{
		"nil":      nil,
		"empty":    {},
		"no rules": {Detector: delete.NewStorageClassDetector(nil)},
		"IT only":  {Detector: delete.NewStorageClassDetector(rules(1, delete.ClassIntelligentTiering, 5, delete.ClassStandard))},
		"IT extra": {ExtraRules: rules(1, delete.ClassIntelligentTiering)},
	} {
		if ok, _ := f.frozen(fi, old, unitNow); ok {
			t.Errorf("%s: froze a 400-day-old partition", name)
		}
	}
}

func TestFreeze_PerTenantOverrideAndExtraRules(t *testing.T) {
	// Guards firstTransition: the tenant's own rules replace the global ones
	// for that tenant only; ExtraRules (stats.s3_lifecycle_rules) are global
	// and the EARLIEST transition of either set wins.
	d := delete.NewStorageClassDetector(rules(60, delete.ClassStandardIA))
	d.SetTenantRules(map[uint32]map[uint32][]delete.LifecycleRule{
		1001: {0: rules(10, delete.ClassGlacier)},
		1003: {0: rules(5, delete.ClassIntelligentTiering)}, // override with no frozen class
	})
	f := &LifecycleFreeze{Detector: d}
	at := func(f *LifecycleFreeze, acct int, age time.Duration) bool {
		ok, _ := f.frozen(tenantFile(acct, "a"), unitNow.Add(-age), unitNow)
		return ok
	}
	if !at(f, 1001, 8*day) || at(f, 1001, 8*day-time.Minute) {
		t.Error("tenant 1001 override (10 d) must freeze from day 8")
	}
	if at(f, 1002, 20*day) || !at(f, 1002, 58*day) {
		t.Error("tenant 1002 has no override: the global 60 d rule applies")
	}
	if at(f, 1003, 300*day) {
		t.Error("tenant 1003 overrides with INTELLIGENT_TIERING only: never age-frozen")
	}
	// Legacy key without tenant prefix uses the global rules.
	if ok, _ := f.frozen(manifest.FileInfo{Key: "logs/dt=2026-01-01/hour=00/x.parquet"}, unitNow.Add(-58*day), unitNow); !ok {
		t.Error("legacy key must use the global rules")
	}

	both := &LifecycleFreeze{Detector: delete.NewStorageClassDetector(rules(60, delete.ClassStandardIA)), ExtraRules: rules(20, delete.ClassGlacierIR)}
	if !at(both, 1002, 18*day) || at(both, 1002, 18*day-time.Minute) {
		t.Error("extra rule 20 d (earlier than detector 60 d) must win")
	}
	both = &LifecycleFreeze{Detector: delete.NewStorageClassDetector(rules(10, delete.ClassStandardIA)), ExtraRules: rules(60, delete.ClassGlacierIR)}
	if !at(both, 1002, 8*day) {
		t.Error("detector rule 10 d (earlier than extra 60 d) must win")
	}
	extraOnly := &LifecycleFreeze{ExtraRules: rules(30, delete.ClassGlacierIR)}
	if !at(extraOnly, 1002, 28*day) {
		t.Error("ExtraRules alone must freeze with no detector")
	}
}

// TestScan_LifecycleFreeze runs the scheduler with a 30-day STANDARD_IA rule
// over three partitions that each hold a mergeable pair: the 29-day-old one
// (past day 28) is left alone, the 27- and 20-day-old ones are merged, and the
// frozen-files gauge reports what was skipped. Without the freeze wired into
// planPartition the oldest partition would be rewritten (a retrieval fee plus
// the early-deletion charge in real S3).
func TestScan_LifecycleFreeze(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		now := time.Now()
		frozenP := partitionAt(now.Add(-29 * day))
		p27 := partitionAt(now.Add(-27 * day))
		p20 := partitionAt(now.Add(-20 * day))
		// The tenant keys 1001/0 and 1002/0 share each partition.
		for _, p := range []string{frozenP, p27, p20} {
			w.add("1001/0", p, 1, 2, 3, nil)
		}
		w.add("1002/0", frozenP, 1, 3, 3, func(fi *manifest.FileInfo) { fi.StorageClass = "GLACIER" })
		sched := w.shippedScheduler()
		sched.freeze = &LifecycleFreeze{Detector: delete.NewStorageClassDetector(rules(30, delete.ClassStandardIA))}
		before := len(w.keys(frozenP))
		rows := w.rows()
		// MaxConcurrent is merges per tenant per scan, so tenant 1001/0's two
		// mergeable partitions take two scans.
		n := 0
		for scan := 0; scan < 5; scan++ {
			c, err := sched.Scan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			n += c
		}
		if n != 2 {
			t.Fatalf("compactions = %d, want 2 (27 d and 20 d partitions)", n)
		}
		if got := len(w.keys(frozenP)); got != before {
			t.Fatalf("29-day partition changed: %d -> %d files", before, got)
		}
		if got := len(w.keys(p27)); got != 1 {
			t.Fatalf("27-day partition has %d files, want 1", got)
		}
		if got := len(w.keys(p20)); got != 1 {
			t.Fatalf("20-day partition has %d files, want 1", got)
		}
		if w.rows() != rows {
			t.Fatalf("rows %d -> %d", rows, w.rows())
		}
		// 1001/0: two files frozen by age; 1002/0: three recorded GLACIER files.
		if got := metrics.CompactionFrozenFiles.Get(frozenAge); got != 2 {
			t.Errorf("frozen_files{reason=age} = %d, want 2", got)
		}
		if got := metrics.CompactionFrozenFiles.Get(frozenStorageClass); got != 3 {
			t.Errorf("frozen_files{reason=storage_class} = %d, want 3", got)
		}
	})
}

func TestScan_FrozenGaugeResetsWhenNothingFrozen(t *testing.T) {
	// Guards the per-scan gauge Set: a scan that freezes nothing must report 0
	// rather than keep the last non-zero value.
	bothModes(t, func(t *testing.T, mode config.Mode) {
		metrics.CompactionFrozenFiles.Set(frozenAge, 99)
		metrics.CompactionFrozenFiles.Set(frozenStorageClass, 99)
		w := newPlanWorld(t, mode)
		w.add("1001/0", partitionAt(time.Now().Add(-3*time.Hour)), 0, 2, 2, nil)
		if _, err := w.shippedScheduler().Scan(context.Background()); err != nil {
			t.Fatal(err)
		}
		if a, s := metrics.CompactionFrozenFiles.Get(frozenAge), metrics.CompactionFrozenFiles.Get(frozenStorageClass); a != 0 || s != 0 {
			t.Fatalf("gauges = %d/%d, want 0/0", a, s)
		}
	})
}
