package config

import (
	"testing"
	"time"
)

func TestValidProfiles(t *testing.T) {
	profiles := ValidProfiles()
	if len(profiles) != 5 {
		t.Fatalf("expected 5 profiles, got %d", len(profiles))
	}
	expected := []Profile{ProfileBalanced, ProfileMaxPerformance, ProfileMaxDurability, ProfileMaxCostSavings, ProfileDev}
	for i, p := range expected {
		if profiles[i] != p {
			t.Errorf("profiles[%d] = %q, want %q", i, profiles[i], p)
		}
	}
}

func TestProfileConfig_AllProfiles(t *testing.T) {
	for _, p := range ValidProfiles() {
		cfg := ProfileConfig(p)
		if cfg == nil {
			t.Errorf("ProfileConfig(%q) returned nil", p)
			continue
		}
		cfg.Mode = ModeLogs
		cfg.S3.Bucket = "test"
		if err := cfg.Validate(); err != nil {
			t.Errorf("ProfileConfig(%q).Validate() = %v", p, err)
		}
	}
}

func TestProfileConfig_EmptyIsBalanced(t *testing.T) {
	balanced := ProfileConfig(ProfileBalanced)
	empty := ProfileConfig("")
	if empty == nil {
		t.Fatal("ProfileConfig(\"\") returned nil")
	}
	if empty.Insert.FlushInterval != balanced.Insert.FlushInterval {
		t.Errorf("empty profile flush_interval = %v, want balanced %v",
			empty.Insert.FlushInterval, balanced.Insert.FlushInterval)
	}
	if empty.Cache.MemoryLimit != balanced.Cache.MemoryLimit {
		t.Errorf("empty profile cache memory = %q, want balanced %q",
			empty.Cache.MemoryLimit, balanced.Cache.MemoryLimit)
	}
}

func TestProfileConfig_InvalidProfile(t *testing.T) {
	cfg := ProfileConfig("nonexistent")
	if cfg == nil {
		t.Fatal("ProfileConfig(\"nonexistent\") returned nil — should return balanced fallback")
	}
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Profile = "nonexistent"
	if err := cfg.Validate(); err == nil {
		t.Error("expected Validate() to reject invalid profile name")
	}
}

func TestProfileConfig_BalancedSettings(t *testing.T) {
	cfg := ProfileConfig(ProfileBalanced)

	if cfg.Insert.FlushInterval != 10*time.Second {
		t.Errorf("balanced flush_interval = %v, want 10s", cfg.Insert.FlushInterval)
	}
	if !cfg.Insert.WALEnabled {
		t.Error("balanced WAL should be enabled")
	}
	if cfg.Insert.CompressionLevel != 7 {
		t.Errorf("balanced compression = %d, want 7", cfg.Insert.CompressionLevel)
	}
	if cfg.Insert.AckMode != "buffer" {
		t.Errorf("balanced ack_mode = %q, want buffer", cfg.Insert.AckMode)
	}
	if cfg.Cache.MemoryLimit != "512MB" {
		t.Errorf("balanced cache memory = %q, want 512MB", cfg.Cache.MemoryLimit)
	}
	if cfg.Query.FileWorkers != 8 {
		t.Errorf("balanced file_workers = %d, want 8", cfg.Query.FileWorkers)
	}
	if cfg.Prefetch.Correlated != true {
		t.Error("balanced prefetch.correlated should be true")
	}
}

func TestProfileConfig_MaxPerformanceSettings(t *testing.T) {
	cfg := ProfileConfig(ProfileMaxPerformance)

	if cfg.Insert.FlushInterval != 5*time.Second {
		t.Errorf("max-perf flush_interval = %v, want 5s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.WALEnabled {
		t.Error("max-perf WAL should be disabled")
	}
	if cfg.Insert.CompressionLevel != 3 {
		t.Errorf("max-perf compression = %d, want 3", cfg.Insert.CompressionLevel)
	}
	if cfg.Insert.MaxBufferRows != 100000 {
		t.Errorf("max-perf max_buffer_rows = %d, want 100000", cfg.Insert.MaxBufferRows)
	}
	if cfg.Cache.MemoryLimit != "2GB" {
		t.Errorf("max-perf cache memory = %q, want 2GB", cfg.Cache.MemoryLimit)
	}
	if cfg.Query.FileWorkers != 16 {
		t.Errorf("max-perf file_workers = %d, want 16", cfg.Query.FileWorkers)
	}
	if cfg.Query.MaxConcurrent != 64 {
		t.Errorf("max-perf max_concurrent = %d, want 64", cfg.Query.MaxConcurrent)
	}
	if !cfg.CrossSignal.Enabled {
		t.Error("max-perf cross_signal should be enabled")
	}
	if !cfg.Startup.ServeStale {
		t.Error("max-perf serve_stale should be true")
	}
}

func TestProfileConfig_MaxDurabilitySettings(t *testing.T) {
	cfg := ProfileConfig(ProfileMaxDurability)

	if cfg.Insert.AckMode != "flush-sync" {
		t.Errorf("max-durability ack_mode = %q, want flush-sync", cfg.Insert.AckMode)
	}
	if !cfg.Insert.WALEnabled {
		t.Error("max-durability WAL should be enabled")
	}
	if cfg.Insert.WALMaxBytes != "1GB" {
		t.Errorf("max-durability wal_max_bytes = %q, want 1GB", cfg.Insert.WALMaxBytes)
	}
	if cfg.Insert.PeerReplicate {
		t.Error("max-durability peer_replicate should be false (flush-sync covers AZ)")
	}
	if !cfg.Compaction.Enabled {
		t.Error("max-durability compaction should be enabled")
	}
	if cfg.Delete.DefaultMode != "permanent" {
		t.Errorf("max-durability delete mode = %q, want permanent", cfg.Delete.DefaultMode)
	}
	if cfg.Delete.VerifyInterval != 1*time.Hour {
		t.Errorf("max-durability verify_interval = %v, want 1h", cfg.Delete.VerifyInterval)
	}
	if cfg.S3.RetryMax != 5 {
		t.Errorf("max-durability retry_max = %d, want 5", cfg.S3.RetryMax)
	}
}

func TestProfileConfig_MaxCostSavingsSettings(t *testing.T) {
	cfg := ProfileConfig(ProfileMaxCostSavings)

	if cfg.Insert.FlushInterval != 30*time.Second {
		t.Errorf("max-cost flush_interval = %v, want 30s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.WALEnabled {
		t.Error("max-cost WAL should be disabled")
	}
	if cfg.Insert.CompressionLevel != 11 {
		t.Errorf("max-cost compression = %d, want 11", cfg.Insert.CompressionLevel)
	}
	if cfg.Cache.MemoryLimit != "128MB" {
		t.Errorf("max-cost cache memory = %q, want 128MB", cfg.Cache.MemoryLimit)
	}
	if cfg.Select.BufferQueryEnabled {
		t.Error("max-cost buffer query should be disabled")
	}
	if cfg.Prefetch.Correlated {
		t.Error("max-cost prefetch.correlated should be false")
	}
	if cfg.Delete.DefaultMode != "hide" {
		t.Errorf("max-cost delete mode = %q, want hide", cfg.Delete.DefaultMode)
	}
}

func TestProfileConfig_DevSettings(t *testing.T) {
	cfg := ProfileConfig(ProfileDev)

	if cfg.Insert.FlushInterval != 1*time.Second {
		t.Errorf("dev flush_interval = %v, want 1s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.WALEnabled {
		t.Error("dev WAL should be disabled")
	}
	if cfg.Insert.CompressionLevel != 1 {
		t.Errorf("dev compression = %d, want 1", cfg.Insert.CompressionLevel)
	}
	if cfg.Insert.MaxBufferRows != 1000 {
		t.Errorf("dev max_buffer_rows = %d, want 1000", cfg.Insert.MaxBufferRows)
	}
	if !cfg.S3.ForcePathStyle {
		t.Error("dev force_path_style should be true for MinIO")
	}
	if cfg.Cache.MemoryLimit != "64MB" {
		t.Errorf("dev cache memory = %q, want 64MB", cfg.Cache.MemoryLimit)
	}
	if cfg.S3.RetryMax != 1 {
		t.Errorf("dev retry_max = %d, want 1", cfg.S3.RetryMax)
	}
	if cfg.Peer.AZAware {
		t.Error("dev az_aware should be false")
	}
	if cfg.Startup.WarmupWindow != 1*time.Hour {
		t.Errorf("dev warmup_window = %v, want 1h", cfg.Startup.WarmupWindow)
	}
}
