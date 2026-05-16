package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidProfiles(t *testing.T) {
	profiles := ValidProfiles()
	if len(profiles) != 5 {
		t.Fatalf("expected 5 profiles, got %d", len(profiles))
	}

	expected := []Profile{
		ProfileBalanced,
		ProfileMaxPerformance,
		ProfileMaxDurability,
		ProfileMaxCostSavings,
		ProfileDev,
	}
	for i, p := range expected {
		if profiles[i] != p {
			t.Errorf("profile[%d] = %q, want %q", i, profiles[i], p)
		}
	}
}

func TestIsValidProfile(t *testing.T) {
	for _, p := range ValidProfiles() {
		if !IsValidProfile(string(p)) {
			t.Errorf("IsValidProfile(%q) = false, want true", p)
		}
	}

	invalid := []string{"", "unknown", "BALANCED", "Max-Performance", "fast"}
	for _, p := range invalid {
		if IsValidProfile(p) {
			t.Errorf("IsValidProfile(%q) = true, want false", p)
		}
	}
}

func TestProfileConfig_ReturnsNonNil(t *testing.T) {
	for _, p := range ValidProfiles() {
		cfg := ProfileConfig(p)
		if cfg == nil {
			t.Errorf("ProfileConfig(%q) returned nil", p)
		}
	}
}

func TestProfileConfig_BalancedMatchesDefault(t *testing.T) {
	balanced := ProfileConfig(ProfileBalanced)
	def := Default()

	if balanced.Insert.FlushInterval != def.Insert.FlushInterval {
		t.Errorf("balanced flush interval = %v, want %v", balanced.Insert.FlushInterval, def.Insert.FlushInterval)
	}
	if balanced.Insert.WALEnabled != def.Insert.WALEnabled {
		t.Errorf("balanced WAL enabled = %v, want %v", balanced.Insert.WALEnabled, def.Insert.WALEnabled)
	}
	if balanced.Insert.CompressionLevel != def.Insert.CompressionLevel {
		t.Errorf("balanced compression level = %d, want %d", balanced.Insert.CompressionLevel, def.Insert.CompressionLevel)
	}
	if balanced.Insert.MaxBufferRows != def.Insert.MaxBufferRows {
		t.Errorf("balanced max buffer rows = %d, want %d", balanced.Insert.MaxBufferRows, def.Insert.MaxBufferRows)
	}
}

func TestProfileConfig_MaxPerformance(t *testing.T) {
	cfg := ProfileConfig(ProfileMaxPerformance)

	if cfg.Insert.FlushInterval != 5*time.Second {
		t.Errorf("flush interval = %v, want 5s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.WALEnabled != false {
		t.Errorf("WAL enabled = %v, want false", cfg.Insert.WALEnabled)
	}
	if cfg.Insert.CompressionLevel != 3 {
		t.Errorf("compression level = %d, want 3", cfg.Insert.CompressionLevel)
	}
	if cfg.Insert.MaxBufferRows != 100000 {
		t.Errorf("max buffer rows = %d, want 100000", cfg.Insert.MaxBufferRows)
	}
	if cfg.Insert.MaxBufferBytes != "512MB" {
		t.Errorf("max buffer bytes = %q, want 512MB", cfg.Insert.MaxBufferBytes)
	}
	if cfg.Insert.TargetFileSize != "64MB" {
		t.Errorf("target file size = %q, want 64MB", cfg.Insert.TargetFileSize)
	}
}

func TestProfileConfig_MaxDurability(t *testing.T) {
	cfg := ProfileConfig(ProfileMaxDurability)

	if cfg.Insert.WALEnabled != true {
		t.Errorf("WAL enabled = %v, want true", cfg.Insert.WALEnabled)
	}
	if cfg.Insert.WALMaxBytes != "1GB" {
		t.Errorf("WAL max bytes = %q, want 1GB", cfg.Insert.WALMaxBytes)
	}
	if cfg.Insert.FlushInterval != 10*time.Second {
		t.Errorf("flush interval = %v, want 10s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.CompressionLevel != 7 {
		t.Errorf("compression level = %d, want 7", cfg.Insert.CompressionLevel)
	}
}

func TestProfileConfig_MaxCostSavings(t *testing.T) {
	cfg := ProfileConfig(ProfileMaxCostSavings)

	if cfg.Insert.FlushInterval != 30*time.Second {
		t.Errorf("flush interval = %v, want 30s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.WALEnabled != false {
		t.Errorf("WAL enabled = %v, want false", cfg.Insert.WALEnabled)
	}
	if cfg.Insert.CompressionLevel != 11 {
		t.Errorf("compression level = %d, want 11", cfg.Insert.CompressionLevel)
	}
	if cfg.Insert.MaxBufferRows != 25000 {
		t.Errorf("max buffer rows = %d, want 25000", cfg.Insert.MaxBufferRows)
	}
	if cfg.Insert.MaxBufferBytes != "128MB" {
		t.Errorf("max buffer bytes = %q, want 128MB", cfg.Insert.MaxBufferBytes)
	}
	if cfg.Insert.TargetFileSize != "256MB" {
		t.Errorf("target file size = %q, want 256MB", cfg.Insert.TargetFileSize)
	}
}

func TestProfileConfig_Dev(t *testing.T) {
	cfg := ProfileConfig(ProfileDev)

	if cfg.Insert.FlushInterval != 1*time.Second {
		t.Errorf("flush interval = %v, want 1s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.WALEnabled != false {
		t.Errorf("WAL enabled = %v, want false", cfg.Insert.WALEnabled)
	}
	if cfg.Insert.CompressionLevel != 1 {
		t.Errorf("compression level = %d, want 1", cfg.Insert.CompressionLevel)
	}
	if cfg.Insert.MaxBufferRows != 1000 {
		t.Errorf("max buffer rows = %d, want 1000", cfg.Insert.MaxBufferRows)
	}
	if cfg.Insert.MaxBufferBytes != "32MB" {
		t.Errorf("max buffer bytes = %q, want 32MB", cfg.Insert.MaxBufferBytes)
	}
	if cfg.Insert.TargetFileSize != "8MB" {
		t.Errorf("target file size = %q, want 8MB", cfg.Insert.TargetFileSize)
	}
	if cfg.S3.ForcePathStyle != true {
		t.Errorf("force path style = %v, want true", cfg.S3.ForcePathStyle)
	}
}

func TestProfileConfig_InvalidProfile_Validate(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Profile = "nonexistent"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for invalid profile, got nil")
	}
}

func TestProfileConfig_EmptyProfile_UsesDefaults(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	yaml := `lakehouse:
  mode: logs
  s3:
    bucket: test-bucket
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatal(err)
	}

	def := Default()
	if cfg.Insert.FlushInterval != def.Insert.FlushInterval {
		t.Errorf("flush interval = %v, want default %v", cfg.Insert.FlushInterval, def.Insert.FlushInterval)
	}
	if cfg.Insert.WALEnabled != def.Insert.WALEnabled {
		t.Errorf("WAL enabled = %v, want default %v", cfg.Insert.WALEnabled, def.Insert.WALEnabled)
	}
}

func TestProfileConfig_FileOverridesProfile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	// Use max-performance profile (WAL disabled, compression 3)
	// but override compression to 9 in the config file.
	yaml := `lakehouse:
  mode: logs
  profile: max-performance
  s3:
    bucket: test-bucket
  insert:
    compression_level: 9
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatal(err)
	}

	// Profile setting should apply: WAL disabled
	if cfg.Insert.WALEnabled != false {
		t.Errorf("WAL enabled = %v, want false (from profile)", cfg.Insert.WALEnabled)
	}

	// File override should win: compression = 9 (not profile's 3)
	if cfg.Insert.CompressionLevel != 9 {
		t.Errorf("compression level = %d, want 9 (file override)", cfg.Insert.CompressionLevel)
	}

	// Profile setting should apply: flush interval 5s
	if cfg.Insert.FlushInterval != 5*time.Second {
		t.Errorf("flush interval = %v, want 5s (from profile)", cfg.Insert.FlushInterval)
	}
}

func TestProfileConfig_DevProfileSetsForcePathStyle(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	yaml := `lakehouse:
  mode: logs
  profile: dev
  s3:
    bucket: test-bucket
    endpoint: http://minio:9000
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatal(err)
	}

	if !cfg.S3.ForcePathStyle {
		t.Error("dev profile should set force_path_style = true")
	}
	if cfg.Insert.FlushInterval != 1*time.Second {
		t.Errorf("dev flush interval = %v, want 1s", cfg.Insert.FlushInterval)
	}
}

func TestProfileConfig_InvalidProfile_LoadError(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	yaml := `lakehouse:
  mode: logs
  profile: nonexistent
  s3:
    bucket: test-bucket
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(cfgFile)
	if err == nil {
		t.Fatal("expected error for invalid profile name, got nil")
	}
}

func TestProfileConfig_MaxPerformance_WALDisabled(t *testing.T) {
	// This test specifically verifies that the profile system can set
	// boolean fields to false (WALEnabled), which is a zero value.
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	yaml := `lakehouse:
  mode: logs
  profile: max-performance
  s3:
    bucket: test-bucket
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatal(err)
	}

	// Default has WAL enabled; max-performance disables it.
	if cfg.Insert.WALEnabled {
		t.Error("max-performance profile should disable WAL, but WALEnabled is true")
	}
}

func TestProfileConfig_FileCanEnableWALOverProfile(t *testing.T) {
	// Profile disables WAL, but explicit config file re-enables it.
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.yaml")

	yaml := `lakehouse:
  mode: logs
  profile: max-performance
  s3:
    bucket: test-bucket
  insert:
    wal_enabled: true
`
	if err := os.WriteFile(cfgFile, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgFile)
	if err != nil {
		t.Fatal(err)
	}

	if !cfg.Insert.WALEnabled {
		t.Error("explicit wal_enabled: true should override profile's false")
	}
}
