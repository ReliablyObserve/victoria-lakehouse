package main

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
)

// newStorageClassDetector builds the detector from delete.lifecycle_rules.
// Per-tenant overrides are installed later on the same pointer.
func newStorageClassDetector(cfg *config.Config) *delete.StorageClassDetector {
	rules := make([]delete.LifecycleRule, len(cfg.Delete.LifecycleRules))
	for i, r := range cfg.Delete.LifecycleRules {
		rules[i] = delete.LifecycleRule{
			TransitionDays: r.TransitionDays,
			Class:          delete.ParseStorageClass(r.StorageClass),
		}
	}
	return delete.NewStorageClassDetector(rules)
}

// compactionFreeze is the lifecycle freeze shared by the compaction scheduler
// and the orphan sweep: the detector (delete.lifecycle_rules, per-tenant
// overrides, cached classes) plus stats.s3_lifecycle_rules, which describe the
// same bucket. Compaction never rewrites what either says S3 has moved out of
// STANDARD, or is about to.
func compactionFreeze(cfg *config.Config, detector *delete.StorageClassDetector) *compaction.LifecycleFreeze {
	extra := make([]delete.LifecycleRule, len(cfg.Stats.S3LifecycleRules))
	for i, r := range cfg.Stats.S3LifecycleRules {
		extra[i] = delete.LifecycleRule{
			TransitionDays: r.TransitionDays,
			Class:          delete.ParseStorageClass(r.StorageClass),
		}
	}
	return &compaction.LifecycleFreeze{Detector: detector, ExtraRules: extra}
}
