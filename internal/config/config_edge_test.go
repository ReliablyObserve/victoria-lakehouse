package config

import (
	"testing"
)

func TestParseSizeBytes_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{"zero MB", "0MB", 0, false},
		{"zero GB", "0GB", 0, false},
		{"zero KB", "0KB", 0, false},
		{"zero TB", "0TB", 0, false},
		{"zero B", "0B", 0, false},
		{"zero bare", "0", 0, false},
		{"leading spaces", "  512MB", 512 * 1024 * 1024, false},
		{"trailing spaces", "512MB  ", 512 * 1024 * 1024, false},
		{"both spaces", " 512MB ", 512 * 1024 * 1024, false},
		{"lowercase mb", "512mb", 512 * 1024 * 1024, false},
		{"mixed case Mb", "512Mb", 512 * 1024 * 1024, false},
		{"just suffix", "MB", 0, true},
		{"just B", "B", 0, true},
		{"negative", "-1GB", -1 * 1024 * 1024 * 1024, false},
		{"float value", "1.5GB", 3 * 1024 * 1024 * 1024 / 2, false},
		{"garbage", "xyz", 0, true},
		{"empty", "", 0, false},
		{"only spaces", "   ", 0, true},
		{"large number", "999TB", 999 * 1024 * 1024 * 1024 * 1024, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSizeBytes(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseSizeBytes(%q) expected error, got %d", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseSizeBytes(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got != tt.want {
				t.Errorf("ParseSizeBytes(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestValidate_AllTopologies(t *testing.T) {
	topologies := []Topology{TopologyAuto, TopologyStorageNode, TopologyDirect, TopologyLokiProxy}
	for _, topo := range topologies {
		cfg := Default()
		cfg.Mode = ModeLogs
		cfg.S3.Bucket = "test"
		cfg.Topology = topo
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() with topology %q: %v", topo, err)
		}
	}
}

func TestValidate_ZeroMaxConnections(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.S3.MaxConnections = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero max connections")
	}
}

func TestValidate_NegativeMaxConnections(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.S3.MaxConnections = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative max connections")
	}
}

func TestValidate_ZeroQueryMaxConcurrent(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Query.MaxConcurrent = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero query max concurrent")
	}
}

func TestValidate_ZeroQueryMaxRows(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Query.MaxRows = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero query max rows")
	}
}

func TestValidate_ZeroCBThreshold(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.CircuitBreaker.Threshold = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero circuit breaker threshold")
	}
}

func TestValidate_EvictionWatermarkBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		wm      float64
		wantErr bool
	}{
		{"zero", 0, true},
		{"negative", -0.1, true},
		{"tiny positive", 0.01, false},
		{"half", 0.5, false},
		{"one", 1.0, false},
		{"over one", 1.01, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test"
			cfg.Cache.EvictionWatermark = tt.wm
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Errorf("expected error for watermark=%f", tt.wm)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error for watermark=%f: %v", tt.wm, err)
			}
		})
	}
}

func TestCacheMemoryBytes_ZeroValue(t *testing.T) {
	cfg := Default()
	cfg.Cache.MemoryLimit = "0MB"
	got := cfg.CacheMemoryBytes()
	if got != 512*1024*1024 {
		t.Errorf("CacheMemoryBytes(0MB) = %d, want default", got)
	}
}

func TestCacheDiskBytes_ZeroValue(t *testing.T) {
	cfg := Default()
	cfg.Cache.DiskLimit = "0GB"
	got := cfg.CacheDiskBytes()
	if got != 50*1024*1024*1024 {
		t.Errorf("CacheDiskBytes(0GB) = %d, want default", got)
	}
}
