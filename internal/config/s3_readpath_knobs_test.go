package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The Tier-1 S3 read-path knobs: defaults, yaml overlay merge, and the
// parquet_read_mode enum validation — added with PR 2a batch 1; these branches
// in mergeConfig/validate* are gated by the 90% coverage check.
func TestS3ReadPathKnobs_DefaultsMergeValidate(t *testing.T) {
	d := Default()
	if d.S3.ReadAheadMaxBytes <= 0 || d.S3.ReadBufferSize <= 0 {
		t.Fatalf("defaults: ReadAheadMaxBytes=%d ReadBufferSize=%d, want >0",
			d.S3.ReadAheadMaxBytes, d.S3.ReadBufferSize)
	}
	if d.S3.ParquetReadMode != "async" {
		t.Fatalf("default ParquetReadMode = %q, want async", d.S3.ParquetReadMode)
	}
	d.Mode = ModeLogs
	d.S3.Bucket = "b"
	if err := d.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}

	// yaml overlay merges all three knobs.
	yml := `
lakehouse:
  mode: logs
  s3:
    bucket: b
    read_ahead_max_bytes: 4194304
    read_buffer_size: 262144
    parquet_read_mode: sync
`
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(path, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.S3.ReadAheadMaxBytes != 4194304 || cfg.S3.ReadBufferSize != 262144 || cfg.S3.ParquetReadMode != "sync" {
		t.Fatalf("overlay merge lost knobs: %+v", cfg.S3)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("sync mode must validate: %v", err)
	}

	// enum validation: invalid mode is rejected with a helpful error.
	bad := Default()
	bad.Mode = ModeLogs
	bad.S3.Bucket = "b"
	bad.S3.ParquetReadMode = "warp-speed"
	err = bad.Validate()
	if err == nil || !strings.Contains(err.Error(), "parquet") {
		t.Fatalf("invalid parquet_read_mode must fail validation, got %v", err)
	}
}

// TestS3ReadAheadWasteThreshold_DefaultAndMerge pins the S3-batch-2 waste
// feedback knob: default 0.5, yaml overlay merge, and the absent-value
// contract — a yaml that does NOT set the key keeps the default (the
// overlay's zero value must not clobber it).
func TestS3ReadAheadWasteThreshold_DefaultAndMerge(t *testing.T) {
	if got := Default().S3.ReadAheadWasteThreshold; got != 0.5 {
		t.Fatalf("default ReadAheadWasteThreshold = %v, want 0.5", got)
	}

	// Overlay sets the key → merged.
	yml := `
lakehouse:
  mode: logs
  s3:
    bucket: b
    read_ahead_waste_threshold: 0.8
`
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(path, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.S3.ReadAheadWasteThreshold != 0.8 {
		t.Fatalf("overlay merge lost read_ahead_waste_threshold: %v", cfg.S3.ReadAheadWasteThreshold)
	}

	// Overlay WITHOUT the key → the 0.5 default survives (absent ≠ zero).
	ymlAbsent := `
lakehouse:
  mode: logs
  s3:
    bucket: b
`
	pathAbsent := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(pathAbsent, []byte(ymlAbsent), 0o600); err != nil {
		t.Fatal(err)
	}
	cfgAbsent, err := Load(pathAbsent)
	if err != nil {
		t.Fatal(err)
	}
	if cfgAbsent.S3.ReadAheadWasteThreshold != 0.5 {
		t.Fatalf("absent key must keep the 0.5 default, got %v", cfgAbsent.S3.ReadAheadWasteThreshold)
	}

	// >= 1 is a valid "disable" value, not a validation error.
	d := Default()
	d.Mode = ModeLogs
	d.S3.Bucket = "b"
	d.S3.ReadAheadWasteThreshold = 1.5
	if err := d.Validate(); err != nil {
		t.Fatalf(">=1 (disable) must validate: %v", err)
	}
}

// TestS3ProjectedFetchKnobs_DefaultsMergeValidate pins the Tier-2
// plan-then-fetch knobs: planned is the default mode, 16MB the default
// per-file plan cap, the yaml overlay merges both (and absence keeps the
// defaults), and the enum/sign validation rejects bad values.
func TestS3ProjectedFetchKnobs_DefaultsMergeValidate(t *testing.T) {
	d := Default()
	if d.S3.ProjectedFetchMode != ProjectedFetchModeWindow {
		t.Fatalf("default ProjectedFetchMode = %q, want %q (planned demoted to opt-in: live bench showed GET-count explosion at 100ms RTT — see s3-scan-optimization-plan.md)", d.S3.ProjectedFetchMode, ProjectedFetchModeWindow)
	}
	if d.S3.ProjectedFetchMaxBytes != 16*1024*1024 {
		t.Fatalf("default ProjectedFetchMaxBytes = %d, want 16MB", d.S3.ProjectedFetchMaxBytes)
	}
	d.Mode = ModeLogs
	d.S3.Bucket = "b"
	if err := d.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}

	// yaml overlay merges both knobs (window is the rollback switch).
	yml := `
lakehouse:
  mode: logs
  s3:
    bucket: b
    projected_fetch_mode: window
    projected_fetch_max_bytes: 4194304
`
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(path, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.S3.ProjectedFetchMode != ProjectedFetchModeWindow || cfg.S3.ProjectedFetchMaxBytes != 4194304 {
		t.Fatalf("overlay merge lost projected-fetch knobs: mode=%q max=%d",
			cfg.S3.ProjectedFetchMode, cfg.S3.ProjectedFetchMaxBytes)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("window mode must validate: %v", err)
	}

	// Overlay WITHOUT the keys → defaults survive (absent != zero).
	ymlAbsent := `
lakehouse:
  mode: logs
  s3:
    bucket: b
`
	pathAbsent := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(pathAbsent, []byte(ymlAbsent), 0o600); err != nil {
		t.Fatal(err)
	}
	cfgAbsent, err := Load(pathAbsent)
	if err != nil {
		t.Fatal(err)
	}
	if cfgAbsent.S3.ProjectedFetchMode != ProjectedFetchModeWindow || cfgAbsent.S3.ProjectedFetchMaxBytes != 16*1024*1024 {
		t.Fatalf("absent keys must keep defaults, got mode=%q max=%d",
			cfgAbsent.S3.ProjectedFetchMode, cfgAbsent.S3.ProjectedFetchMaxBytes)
	}

	// Enum validation: an unknown mode is rejected with a helpful error.
	bad := Default()
	bad.Mode = ModeLogs
	bad.S3.Bucket = "b"
	bad.S3.ProjectedFetchMode = "yolo"
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "projected-fetch-mode") {
		t.Fatalf("invalid projected_fetch_mode must fail validation, got %v", err)
	}

	// Sign validation: a negative cap is rejected.
	neg := Default()
	neg.Mode = ModeLogs
	neg.S3.Bucket = "b"
	neg.S3.ProjectedFetchMaxBytes = -1
	if err := neg.Validate(); err == nil || !strings.Contains(err.Error(), "projected-fetch-max-bytes") {
		t.Fatalf("negative projected_fetch_max_bytes must fail validation, got %v", err)
	}
}
