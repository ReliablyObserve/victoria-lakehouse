package bloomindex

import (
	"context"
	"testing"
	"time"
)

func TestBloomController_DefaultConfig(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	cfg := bc.Config()

	if cfg.Tier1MaxAge != 7*24*time.Hour {
		t.Errorf("tier1_max_age = %v, want 7d", cfg.Tier1MaxAge)
	}
	if cfg.Tier2MaxAge != 30*24*time.Hour {
		t.Errorf("tier2_max_age = %v, want 30d", cfg.Tier2MaxAge)
	}
	if cfg.TargetFileSize != 128*1024*1024 {
		t.Errorf("target_file_size = %d, want 128MB", cfg.TargetFileSize)
	}
}

func TestBloomController_TierConfig(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	tc := bc.TierConfig()

	if tc.Tier1MaxAge != 7*24*time.Hour {
		t.Errorf("tier1_max_age = %v", tc.Tier1MaxAge)
	}
}

func TestBloomController_LeaderOnly(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	bc.Observe(context.Background(), Observation{FilesPerHour: 5000})

	if len(bc.Adjustments()) != 0 {
		t.Error("non-leader should not make adjustments")
	}

	bc.SetLeader(true)
	bc.Observe(context.Background(), Observation{FilesPerHour: 5000})

	if len(bc.Adjustments()) == 0 {
		t.Error("leader should make adjustments for high volume")
	}
}

func TestBloomController_HighVolume_IncreasesFileSize(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	bc.SetLeader(true)

	bc.Observe(context.Background(), Observation{FilesPerHour: 5000})

	cfg := bc.Config()
	if cfg.TargetFileSize != 512*1024*1024 {
		t.Errorf("target_file_size = %d, want 512MB", cfg.TargetFileSize)
	}

	adjs := bc.Adjustments()
	if len(adjs) != 1 || adjs[0].Parameter != "target_file_size" {
		t.Errorf("unexpected adjustments: %+v", adjs)
	}
}

func TestBloomController_SSDPressure_ShrinksTier1(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	bc.SetLeader(true)

	bc.Observe(context.Background(), Observation{SSDUsageRatio: 0.95})

	cfg := bc.Config()
	if cfg.Tier1MaxAge != 6*24*time.Hour {
		t.Errorf("tier1_max_age = %v, want 6d", cfg.Tier1MaxAge)
	}
}

func TestBloomController_SSDLow_ExpandsTier1(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	bc.SetLeader(true)

	bc.Observe(context.Background(), Observation{SSDUsageRatio: 0.3})

	cfg := bc.Config()
	if cfg.Tier1MaxAge != 8*24*time.Hour {
		t.Errorf("tier1_max_age = %v, want 8d", cfg.Tier1MaxAge)
	}
}

func TestBloomController_Tier1MaxExpand(t *testing.T) {
	cfg := DefaultBloomControllerConfig()
	cfg.Tier1MaxAge = 14 * 24 * time.Hour
	bc := NewBloomController(cfg)
	bc.SetLeader(true)

	bc.Observe(context.Background(), Observation{SSDUsageRatio: 0.3})

	if bc.Config().Tier1MaxAge != 14*24*time.Hour {
		t.Error("tier1 should not expand beyond 14d")
	}
}

func TestBloomController_Tier1MinShrink(t *testing.T) {
	cfg := DefaultBloomControllerConfig()
	cfg.Tier1MaxAge = 24 * time.Hour
	bc := NewBloomController(cfg)
	bc.SetLeader(true)

	bc.Observe(context.Background(), Observation{SSDUsageRatio: 0.95})

	if bc.Config().Tier1MaxAge != 24*time.Hour {
		t.Error("tier1 should not shrink below 1d")
	}
}

func TestBloomController_PinnedOverride(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	bc.SetLeader(true)
	bc.PinOverride("target_file_size")

	bc.Observe(context.Background(), Observation{FilesPerHour: 5000})

	cfg := bc.Config()
	if cfg.TargetFileSize != 128*1024*1024 {
		t.Error("pinned parameter should not be auto-tuned")
	}
}

func TestBloomController_LowVolume_SwitchesToDaily(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	bc.SetLeader(true)

	bc.Observe(context.Background(), Observation{FilesPerHour: 10})

	cfg := bc.Config()
	if cfg.PartitionGranularity != GranularityDay {
		t.Error("low volume should switch to daily granularity")
	}
}

func TestBloomController_ApplyConfig(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())

	newCfg := BloomControllerConfig{
		Enabled:     true,
		Tier1MaxAge: 5 * 24 * time.Hour,
		Tier2MaxAge: 20 * 24 * time.Hour,
		Tier3MaxAge: 60 * 24 * time.Hour,
	}
	bc.ApplyConfig(newCfg)

	got := bc.Config()
	if got.Tier1MaxAge != 5*24*time.Hour {
		t.Errorf("tier1_max_age = %v, want 5d", got.Tier1MaxAge)
	}
}

func TestBloomController_NoDoubleAdjust(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	bc.SetLeader(true)

	bc.Observe(context.Background(), Observation{FilesPerHour: 5000})
	bc.Observe(context.Background(), Observation{FilesPerHour: 5000})

	if len(bc.Adjustments()) != 1 {
		t.Errorf("same value should not produce duplicate adjustments, got %d", len(bc.Adjustments()))
	}
}
