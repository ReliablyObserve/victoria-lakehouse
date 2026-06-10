// Tests for the per-output-level Parquet row-group size schedule
// (Compaction.RowGroupSizeByOutputLevel) — the compression step-4 knob.
// Mirrors the CompressionLevelByOutputLevel test surface: default
// schedule, per-level resolution + saturation, empty-slice fall-through
// (the absent-value contract), validation, YAML round-trip, and overlay
// merge (including the merge regression that previously dropped the
// compression schedule from YAML).

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig_CompactionRowGroupSchedule(t *testing.T) {
	cfg := Default()
	// L0/L1 outputs keep the historical write-path row-group size
	// (Insert.RowGroupSize default 10000); L2+ rollups double it.
	want := []int{10000, 10000, 20000}
	got := cfg.Compaction.RowGroupSizeByOutputLevel
	if len(got) != len(want) {
		t.Fatalf("default RowGroupSizeByOutputLevel = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("default RowGroupSizeByOutputLevel[%d] = %d, want %d", i, got[i], want[i])
		}
	}
	if got[2] != 2*cfg.Insert.RowGroupSize {
		t.Errorf("L2 slot = %d, want 2× Insert.RowGroupSize (%d)", got[2], 2*cfg.Insert.RowGroupSize)
	}
}

func TestRowGroupSizeForOutput_GlobalSchedule(t *testing.T) {
	cfg := CompactionConfig{RowGroupSizeByOutputLevel: []int{10000, 10000, 20000}}
	cases := []struct {
		outputLevel int
		want        int
	}{
		{0, 10000},
		{1, 10000},
		{2, 20000},
		{3, 20000}, // saturates to last slot
		{99, 20000},
		{-1, 10000}, // clamped to first slot
	}
	for _, tc := range cases {
		if got := cfg.RowGroupSizeForOutput(tc.outputLevel); got != tc.want {
			t.Errorf("RowGroupSizeForOutput(%d) = %d, want %d", tc.outputLevel, got, tc.want)
		}
	}
}

func TestRowGroupSizeForOutput_EmptySliceFallsThrough(t *testing.T) {
	// Empty schedule means "no progressive override"; the compactor
	// must fall back to its static Insert.RowGroupSize. The helper
	// signals that by returning 0 so the caller can branch — pinning
	// the absent-value contract.
	cfg := CompactionConfig{}
	if got := cfg.RowGroupSizeForOutput(2); got != 0 {
		t.Errorf("empty schedule = %d, want 0 (signal to caller to fall back)", got)
	}
}

func TestValidate_RowGroupScheduleInvalid(t *testing.T) {
	tests := []struct {
		name     string
		schedule []int
	}{
		{"zero_slot", []int{10000, 0, 20000}},
		{"negative_slot", []int{-1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test"
			cfg.Compaction.Enabled = true
			cfg.Compaction.RowGroupSizeByOutputLevel = tt.schedule
			if err := cfg.Validate(); err == nil {
				t.Error("expected error for invalid row-group schedule")
			}
		})
	}
}

func TestValidate_RowGroupScheduleValid(t *testing.T) {
	for _, schedule := range [][]int{nil, {}, {5000}, {10000, 10000, 20000}} {
		cfg := Default()
		cfg.Mode = ModeLogs
		cfg.S3.Bucket = "test"
		cfg.Compaction.Enabled = true
		cfg.Compaction.RowGroupSizeByOutputLevel = schedule
		if err := cfg.Validate(); err != nil {
			t.Errorf("schedule %v should be valid: %v", schedule, err)
		}
	}
}

func TestLoad_YAMLRowGroupSchedule_RoundTrip(t *testing.T) {
	content := `
lakehouse:
  mode: logs
  s3:
    bucket: test-bucket
  compaction:
    row_group_size_by_output_level: [5000, 5000, 40000]
    compression_level_by_output_level: [1, 7, 22]
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	wantRG := []int{5000, 5000, 40000}
	if got := cfg.Compaction.RowGroupSizeByOutputLevel; len(got) != len(wantRG) {
		t.Fatalf("RowGroupSizeByOutputLevel = %v, want %v", got, wantRG)
	}
	for i := range wantRG {
		if cfg.Compaction.RowGroupSizeByOutputLevel[i] != wantRG[i] {
			t.Errorf("RowGroupSizeByOutputLevel[%d] = %d, want %d", i, cfg.Compaction.RowGroupSizeByOutputLevel[i], wantRG[i])
		}
	}

	// Regression: the YAML compression schedule used to be silently
	// dropped by mergeConfig (no overlay branch) — the file value must
	// win over the profile default [3, 7, 11].
	wantCL := []int{1, 7, 22}
	if got := cfg.Compaction.CompressionLevelByOutputLevel; len(got) != len(wantCL) {
		t.Fatalf("CompressionLevelByOutputLevel = %v, want %v (YAML value dropped by merge)", got, wantCL)
	}
	for i := range wantCL {
		if cfg.Compaction.CompressionLevelByOutputLevel[i] != wantCL[i] {
			t.Errorf("CompressionLevelByOutputLevel[%d] = %d, want %d", i, cfg.Compaction.CompressionLevelByOutputLevel[i], wantCL[i])
		}
	}
}

func TestMergeConfig_EmptyCompactionSchedulesPreserveBase(t *testing.T) {
	base := Default()
	overlay := &Config{}

	result := mergeConfig(base, overlay)

	if got, want := result.Compaction.RowGroupSizeByOutputLevel, []int{10000, 10000, 20000}; len(got) != len(want) {
		t.Fatalf("RowGroupSizeByOutputLevel = %v, want preserved default %v", got, want)
	}
	if got, want := result.Compaction.CompressionLevelByOutputLevel, []int{3, 7, 11}; len(got) != len(want) {
		t.Fatalf("CompressionLevelByOutputLevel = %v, want preserved default %v", got, want)
	}
}
