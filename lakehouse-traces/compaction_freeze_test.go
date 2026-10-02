package main

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
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
}
