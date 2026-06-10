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
