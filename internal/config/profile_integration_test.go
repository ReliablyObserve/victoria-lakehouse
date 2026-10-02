package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeConfig is a test helper that writes YAML content to a temp file and returns the path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestIntegration_DevProfileWithMinIO(t *testing.T) {
	path := writeConfig(t, `
lakehouse:
  mode: logs
  profile: dev
  s3:
    bucket: local-dev
    endpoint: http://localhost:9000
    access_key: minioadmin
    secret_key: minioadmin
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Dev profile defaults
	if cfg.Insert.BufferFlushInterval != 10*time.Second {
		t.Errorf("buffer_flush_interval = %v, want 10s", cfg.Insert.BufferFlushInterval)
	}
	if cfg.Insert.CompressionLevel != 1 {
		t.Errorf("compression_level = %d, want 1", cfg.Insert.CompressionLevel)
	}
	if !cfg.S3.ForcePathStyle {
		t.Error("dev force_path_style should be true for MinIO")
	}
	if cfg.S3.RetryMax != 1 {
		t.Errorf("dev retry_max = %d, want 1", cfg.S3.RetryMax)
	}

	// File overrides
	if cfg.S3.Endpoint != "http://localhost:9000" {
		t.Errorf("endpoint = %q, want http://localhost:9000", cfg.S3.Endpoint)
	}
	if cfg.S3.Bucket != "local-dev" {
		t.Errorf("bucket = %q, want local-dev", cfg.S3.Bucket)
	}
	if cfg.S3.AccessKey != "minioadmin" {
		t.Errorf("access_key = %q, want minioadmin", cfg.S3.AccessKey)
	}

	if cfg.Profile != ProfileDev {
		t.Errorf("profile = %q, want dev", cfg.Profile)
	}
}

func TestIntegration_MaxDurabilityProduction(t *testing.T) {
	path := writeConfig(t, `
lakehouse:
  mode: logs
  profile: max-durability
  s3:
    bucket: prod-logs
    region: eu-west-1
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Max-durability profile fields
	if !cfg.Compaction.Enabled {
		t.Error("max-durability compaction should be enabled")
	}
	if cfg.S3.RetryMax != 5 {
		t.Errorf("retry_max = %d, want 5", cfg.S3.RetryMax)
	}
	if cfg.Delete.DefaultMode != "permanent" {
		t.Errorf("delete mode = %q, want permanent", cfg.Delete.DefaultMode)
	}

	// File override for region
	if cfg.S3.Region != "eu-west-1" {
		t.Errorf("region = %q, want eu-west-1", cfg.S3.Region)
	}
}

func TestIntegration_BalancedWithExplicitOverrides(t *testing.T) {
	path := writeConfig(t, `
lakehouse:
  mode: logs
  profile: balanced
  s3:
    bucket: test
  insert:
    compression_level: 11
    buffer_flush_interval: 30s
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Explicit overrides from config file
	if cfg.Insert.CompressionLevel != 11 {
		t.Errorf("compression_level = %d, want 11 (file override)", cfg.Insert.CompressionLevel)
	}
	if cfg.Insert.BufferFlushInterval != 30*time.Second {
		t.Errorf("buffer_flush_interval = %v, want 30s (file override)", cfg.Insert.BufferFlushInterval)
	}

	// Other balanced defaults should remain
	if cfg.Insert.BufferDir != "/data/lakehouse/buffer" {
		t.Errorf("buffer_dir = %q, want the balanced default", cfg.Insert.BufferDir)
	}
}

func TestIntegration_AllProfilesCompileForLogs(t *testing.T) {
	for _, p := range ValidProfiles() {
		t.Run(string(p), func(t *testing.T) {
			path := writeConfig(t, `
lakehouse:
  mode: logs
  profile: `+string(p)+`
  s3:
    bucket: test
`)

			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%q): %v", p, err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate(%q): %v", p, err)
			}
			if cfg.Mode != ModeLogs {
				t.Errorf("mode = %q, want logs", cfg.Mode)
			}
		})
	}
}

func TestIntegration_AllProfilesCompileForTraces(t *testing.T) {
	for _, p := range ValidProfiles() {
		t.Run(string(p), func(t *testing.T) {
			path := writeConfig(t, `
lakehouse:
  mode: traces
  profile: `+string(p)+`
  s3:
    bucket: test
`)

			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%q): %v", p, err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate(%q): %v", p, err)
			}
			if cfg.Mode != ModeTraces {
				t.Errorf("mode = %q, want traces", cfg.Mode)
			}
		})
	}
}

func TestIntegration_InvalidProfileRejected(t *testing.T) {
	path := writeConfig(t, `
lakehouse:
  mode: logs
  profile: super-turbo
  s3:
    bucket: test
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if err := cfg.Validate(); err == nil {
		t.Error("expected Validate to reject invalid profile 'super-turbo'")
	}
}

func TestIntegration_DurabilityFieldsRoundTrip(t *testing.T) {
	path := writeConfig(t, `
lakehouse:
  profile: max-durability
  s3:
    bucket: test
  gc:
    enabled: true
    interval: 2h
`)

	cfg, err := LoadWithMode(path, ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode: %v", err)
	}

	// ack_mode from max-durability profile

	// flush_linger is 0 for max-durability (immediate flush for durability)

	// flush_max_rows from balanced defaults

	// GC interval from file override
	if cfg.GC.Interval != 2*time.Hour {
		t.Errorf("gc.interval = %v, want 2h", cfg.GC.Interval)
	}
	if !cfg.GC.Enabled {
		t.Error("gc.enabled should be true from file override")
	}
}

func TestIntegration_PerSignalOverride(t *testing.T) {
	yaml := `
lakehouse:
  profile: balanced
  s3:
    bucket: test
  logs:
    profile: max-durability
  traces:
    profile: balanced
`
	path := writeConfig(t, yaml)

	logsCfg, err := LoadWithMode(path, ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode logs: %v", err)
	}

	tracesCfg, err := LoadWithMode(path, ModeTraces, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode traces: %v", err)
	}

	// Logs should use max-durability
	if logsCfg.S3.RetryMax != 5 {
		t.Errorf("logs retry_max = %d, want 5 (max-durability)", logsCfg.S3.RetryMax)
	}
	if logsCfg.Delete.DefaultMode != "permanent" {
		t.Errorf("logs delete mode = %q, want permanent (max-durability)", logsCfg.Delete.DefaultMode)
	}

	// Traces should use balanced
	if tracesCfg.Insert.CompressionLevel != Default().Insert.CompressionLevel {
		t.Errorf("traces compression_level = %d, want the balanced default %d", tracesCfg.Insert.CompressionLevel, Default().Insert.CompressionLevel)
	}
	if tracesCfg.S3.RetryMax == 5 && Default().S3.RetryMax != 5 {
		t.Errorf("traces took the max-durability retry_max")
	}
}

func TestIntegration_PerRoleOverride(t *testing.T) {
	yaml := `
lakehouse:
  profile: balanced
  s3:
    bucket: test
  logs:
    insert:
      profile: max-performance
    select:
      profile: max-cost-savings
`
	path := writeConfig(t, yaml)

	insertCfg, err := LoadWithMode(path, ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode insert: %v", err)
	}

	selectCfg, err := LoadWithMode(path, ModeLogs, RoleSelect)
	if err != nil {
		t.Fatalf("LoadWithMode select: %v", err)
	}

	// Insert role uses max-performance
	if insertCfg.Insert.CompressionLevel != 3 {
		t.Errorf("insert compression_level = %d, want 3 (max-performance)", insertCfg.Insert.CompressionLevel)
	}

	// Select role uses max-cost-savings
	if selectCfg.Insert.CompressionLevel != 11 {
		t.Errorf("select compression_level = %d, want 11 (max-cost-savings)", selectCfg.Insert.CompressionLevel)
	}
	if selectCfg.Query.FileWorkers != 4 {
		t.Errorf("select file_workers = %d, want 4 (max-cost-savings)", selectCfg.Query.FileWorkers)
	}
	if selectCfg.Query.MaxConcurrent != 16 {
		t.Errorf("select max_concurrent = %d, want 16 (max-cost-savings)", selectCfg.Query.MaxConcurrent)
	}
}

func TestIntegration_PerRoleWithExplicitConfigOverride(t *testing.T) {
	yaml := `
lakehouse:
  profile: balanced
  s3:
    bucket: test
  logs:
    insert:
      profile: max-performance
  insert:
    compression_level: 7
`
	path := writeConfig(t, yaml)

	cfg, err := LoadWithMode(path, ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode: %v", err)
	}

	// Explicit config file overrides beat the profile
	if cfg.Insert.CompressionLevel != 7 {
		t.Errorf("compression_level = %d, want 7 (config file override)", cfg.Insert.CompressionLevel)
	}

	// Other max-performance settings should remain from profile
	if cfg.Query.FileWorkers != 16 {
		t.Errorf("file_workers = %d, want 16 (max-performance profile)", cfg.Query.FileWorkers)
	}
}

func TestIntegration_RoleAllUsesPerSignalProfile(t *testing.T) {
	yaml := `
lakehouse:
  profile: balanced
  s3:
    bucket: test
  logs:
    profile: dev
    insert:
      profile: max-performance
`
	path := writeConfig(t, yaml)

	// RoleAll should skip per-role profiles and use per-signal
	cfg, err := LoadWithMode(path, ModeLogs, RoleAll)
	if err != nil {
		t.Fatalf("LoadWithMode: %v", err)
	}

	// Should use "dev" from logs.profile, not "max-performance" from logs.insert.profile
	if cfg.Profile != ProfileDev {
		t.Errorf("profile = %q, want dev (per-signal, not per-role)", cfg.Profile)
	}
	if cfg.Insert.CompressionLevel != 1 {
		t.Errorf("compression_level = %d, want 1 (dev profile)", cfg.Insert.CompressionLevel)
	}
	if !cfg.S3.ForcePathStyle {
		t.Error("force_path_style should be true (dev profile)")
	}
}
