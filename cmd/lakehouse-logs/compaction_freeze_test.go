package main

import (
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// TestCompactionFreeze_WiredFromConfig proves the compaction freeze is built
// from both lifecycle rule sets of the config and shares the detector pointer
// the delete path uses. Without it the scheduler would never see an
// operator's lifecycle rules and would rewrite objects S3 has tiered.
func TestCompactionFreeze_WiredFromConfig(t *testing.T) {
	cfg := config.Default()
	cfg.Delete.LifecycleRules = []config.LifecycleRuleConfig{{TransitionDays: 30, StorageClass: "STANDARD_IA"}}
	cfg.Stats.S3LifecycleRules = []config.LifecycleRuleConfig{
		{TransitionDays: 90, StorageClass: "GLACIER"},
		{TransitionDays: 7, StorageClass: "INTELLIGENT_TIERING"},
	}
	detector := newStorageClassDetector(cfg)
	freeze := compactionFreeze(cfg, detector)
	if freeze == nil || freeze.Detector != detector {
		t.Fatalf("freeze must share the detector: %+v", freeze)
	}
	if got, ok := detector.FirstNonRewritableTransition("1/0/x/dt=2026-01-01/hour=00/a.parquet"); !ok || got != 30 {
		t.Fatalf("detector transition = %d,%v; want 30,true", got, ok)
	}
	if len(freeze.ExtraRules) != 2 {
		t.Fatalf("ExtraRules = %v; want both stats rules", freeze.ExtraRules)
	}
	if got, ok := delete.FirstNonRewritableTransition(freeze.ExtraRules); !ok || got != 90 {
		t.Fatalf("extra transition = %d,%v; want 90,true (INTELLIGENT_TIERING is rewritable)", got, ok)
	}
	if freeze.SizeMergeMaxAge != cfg.Compaction.SizeMergeMaxAge || freeze.SizeMergeMaxAge != 168*time.Hour {
		t.Fatalf("SizeMergeMaxAge = %v, want the configured compaction.size_merge_max_age (168h default)", freeze.SizeMergeMaxAge)
	}
	cfg.Compaction.SizeMergeMaxAge = -time.Hour
	if got := compactionFreeze(cfg, detector).SizeMergeMaxAge; got != -time.Hour {
		t.Fatalf("a negative cap must pass through to disable it, got %v", got)
	}
}

// TestCompactionFreezeWarnings_AtStartup guards the startup check: a global or
// per-tenant lifecycle rule whose freeze age is not later than
// compaction.daily_rollup_age yields a warning naming its scope (the rollup can
// then never run for that data); a healthy rule, a disabled compaction and no
// rules give none.
func TestCompactionFreezeWarnings_AtStartup(t *testing.T) {
	cfg := config.Default()
	cfg.Compaction.DailyRollupAge = 24 * time.Hour
	cfg.Delete.LifecycleRules = []config.LifecycleRuleConfig{{TransitionDays: 30, StorageClass: "STANDARD_IA"}}
	detector := newStorageClassDetector(cfg)
	if w := compactionFreezeWarnings(cfg, detector, nil); len(w) != 0 {
		t.Fatalf("healthy 30-day rule warned: %q", w)
	}

	cfg.Stats.S3LifecycleRules = []config.LifecycleRuleConfig{{TransitionDays: 1, StorageClass: "GLACIER"}}
	w := compactionFreezeWarnings(cfg, detector, nil)
	if len(w) != 1 || !strings.Contains(w[0], "global lifecycle rules") {
		t.Fatalf("1-day global rule: %q", w)
	}

	cfg.Stats.S3LifecycleRules = nil
	detector.SetTenantRules(map[uint32]map[uint32][]delete.LifecycleRule{1001: {0: {{TransitionDays: 2, Class: delete.ClassGlacier}}}})
	w = compactionFreezeWarnings(cfg, detector, []tenant.LifecycleEntry{{AccountID: 1001, ProjectID: 0}})
	if len(w) != 1 || !strings.Contains(w[0], "tenant 1001/0/") {
		t.Fatalf("2-day tenant override: %q", w)
	}

	cfg.Compaction.Enabled = false
	if w := compactionFreezeWarnings(cfg, detector, []tenant.LifecycleEntry{{AccountID: 1001}}); len(w) != 0 {
		t.Fatalf("disabled compaction warned: %q", w)
	}
}
