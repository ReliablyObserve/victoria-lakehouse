package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProfilePrecedence_ProfileOverridesDefault(t *testing.T) {
	cfg := Default()
	cfg.Profile = ProfileDev

	profileCfg := ProfileConfig(cfg.Profile)
	merged := mergeConfig(profileCfg, &Config{
		Mode: ModeLogs,
		S3:   S3Config{Bucket: "test"},
	})

	if merged.Insert.FlushInterval != 1*time.Second {
		t.Errorf("dev profile flush_interval = %v, want 1s", merged.Insert.FlushInterval)
	}
	if !merged.S3.ForcePathStyle {
		t.Error("dev profile force_path_style should be true")
	}
}

func TestProfilePrecedence_ConfigFileOverridesProfile(t *testing.T) {
	content := `
lakehouse:
  profile: dev
  insert:
    flush_interval: 5s
    compression_level: 7
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Insert.FlushInterval != 5*time.Second {
		t.Errorf("config file should override dev profile flush_interval: got %v, want 5s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.CompressionLevel != 7 {
		t.Errorf("config file should override dev profile compression: got %d, want 7", cfg.Insert.CompressionLevel)
	}
	if !cfg.S3.ForcePathStyle {
		t.Error("dev profile force_path_style should still be true (not overridden by file)")
	}
	if cfg.Insert.MaxBufferRows != 1000 {
		t.Errorf("dev profile max_buffer_rows should still be 1000 (not overridden): got %d", cfg.Insert.MaxBufferRows)
	}
}

func TestProfilePrecedence_BalancedIsDefault(t *testing.T) {
	content := `
lakehouse:
  s3:
    bucket: test
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Insert.FlushInterval != 60*time.Second {
		t.Errorf("default (balanced) flush_interval = %v, want 60s", cfg.Insert.FlushInterval)
	}
}

func TestProfilePrecedence_MaxDurabilityFromFile(t *testing.T) {
	content := `
lakehouse:
  profile: max-durability
  s3:
    bucket: test
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Insert.AckMode != "flush-sync" {
		t.Errorf("max-durability ack_mode = %q, want flush-sync", cfg.Insert.AckMode)
	}
	if cfg.S3.RetryMax != 5 {
		t.Errorf("max-durability retry_max = %d, want 5", cfg.S3.RetryMax)
	}
}

func TestProfilePrecedence_MaxPerformanceWithOverride(t *testing.T) {
	content := `
lakehouse:
  profile: max-performance
  s3:
    bucket: test
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadWithMode(path, ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode: %v", err)
	}

	if cfg.Insert.CompressionLevel != 3 {
		t.Errorf("compression should be 3 (from profile): got %d", cfg.Insert.CompressionLevel)
	}
	if cfg.Query.FileWorkers != 16 {
		t.Errorf("file_workers should be 16 (from profile): got %d", cfg.Query.FileWorkers)
	}
}

func TestProfilePrecedence_PerSignalFromFile(t *testing.T) {
	content := `
lakehouse:
  profile: balanced
  s3:
    bucket: test
  logs:
    profile: max-durability
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadWithMode(path, ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode: %v", err)
	}

	if cfg.Insert.AckMode != "flush-sync" {
		t.Errorf("per-signal max-durability should set ack_mode: got %q, want flush-sync", cfg.Insert.AckMode)
	}
	if cfg.S3.RetryMax != 5 {
		t.Errorf("per-signal max-durability retry_max = %d, want 5", cfg.S3.RetryMax)
	}
}

func TestProfilePrecedence_PerRoleFromFile(t *testing.T) {
	content := `
lakehouse:
  profile: balanced
  s3:
    bucket: test
  logs:
    profile: max-durability
    insert:
      profile: max-performance
    select:
      profile: max-cost-savings
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	insertCfg, err := LoadWithMode(path, ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode insert: %v", err)
	}

	selectCfg, err := LoadWithMode(path, ModeLogs, RoleSelect)
	if err != nil {
		t.Fatalf("LoadWithMode select: %v", err)
	}

	if insertCfg.Insert.FlushInterval != 5*time.Second {
		t.Errorf("insert pod flush_interval = %v, want 5s (max-performance)", insertCfg.Insert.FlushInterval)
	}

	if selectCfg.Query.FileWorkers != 4 {
		t.Errorf("select pod file_workers = %d, want 4 (max-cost-savings)", selectCfg.Query.FileWorkers)
	}
	if selectCfg.Select.BufferQueryEnabled {
		t.Error("select pod buffer_query should be off (max-cost-savings)")
	}
}

func TestProfilePrecedence_PerRoleWithExplicitOverride(t *testing.T) {
	content := `
lakehouse:
  profile: balanced
  s3:
    bucket: test
  logs:
    insert:
      profile: max-performance
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadWithMode(path, ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode: %v", err)
	}

	if cfg.Insert.CompressionLevel != 3 {
		t.Errorf("compression should be 3 (from max-performance profile): got %d", cfg.Insert.CompressionLevel)
	}
}

func TestProfilePrecedence_TracesPerSignal(t *testing.T) {
	content := `
lakehouse:
  profile: balanced
  s3:
    bucket: test
  traces:
    profile: dev
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadWithMode(path, ModeTraces, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode: %v", err)
	}

	if cfg.Insert.FlushInterval != 1*time.Second {
		t.Errorf("traces dev profile flush_interval = %v, want 1s", cfg.Insert.FlushInterval)
	}
	if !cfg.S3.ForcePathStyle {
		t.Error("traces dev profile force_path_style should be true")
	}
}
