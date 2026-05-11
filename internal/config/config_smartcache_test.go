package config

import (
	"testing"
	"time"
)

func TestSmartCacheConfig_Defaults(t *testing.T) {
	cfg := Default()

	if cfg.SmartCache.MaxAge != 24*time.Hour {
		t.Errorf("max_age = %v, want 24h", cfg.SmartCache.MaxAge)
	}
	if cfg.SmartCache.SnapshotInterval != 60*time.Second {
		t.Errorf("snapshot_interval = %v, want 60s", cfg.SmartCache.SnapshotInterval)
	}
	if cfg.SmartCache.QueryGracePeriod != 5*time.Minute {
		t.Errorf("query_grace_period = %v, want 5m", cfg.SmartCache.QueryGracePeriod)
	}
	if cfg.SmartCache.HotAccessThreshold != 3 {
		t.Errorf("hot_access_threshold = %d, want 3", cfg.SmartCache.HotAccessThreshold)
	}
	if cfg.SmartCache.HotWindow != 10*time.Minute {
		t.Errorf("hot_window = %v, want 10m", cfg.SmartCache.HotWindow)
	}
	if cfg.SmartCache.TargetHours != 24 {
		t.Errorf("target_hours = %d, want 24", cfg.SmartCache.TargetHours)
	}
	if cfg.SmartCache.DiskLimitMax != "100GB" {
		t.Errorf("disk_limit_max = %q, want %q", cfg.SmartCache.DiskLimitMax, "100GB")
	}
}

func TestCrossSignalConfig_Defaults(t *testing.T) {
	cfg := Default()

	if cfg.CrossSignal.Enabled != false {
		t.Errorf("cross_signal.enabled = %v, want false", cfg.CrossSignal.Enabled)
	}
	if cfg.CrossSignal.Timeout != 2*time.Second {
		t.Errorf("timeout = %v, want 2s", cfg.CrossSignal.Timeout)
	}
	if cfg.CrossSignal.MaxBatch != 100 {
		t.Errorf("max_batch = %d, want 100", cfg.CrossSignal.MaxBatch)
	}
	if cfg.CrossSignal.BatchInterval != 500*time.Millisecond {
		t.Errorf("batch_interval = %v, want 500ms", cfg.CrossSignal.BatchInterval)
	}
}

func TestQueryConfig_FileWorkers(t *testing.T) {
	cfg := Default()
	if cfg.Query.FileWorkers != 8 {
		t.Errorf("file_workers = %d, want 8", cfg.Query.FileWorkers)
	}
}

func TestS3Config_MaxConcurrentDownloads(t *testing.T) {
	cfg := Default()
	if cfg.S3.MaxConcurrentDownloads != 16 {
		t.Errorf("max_concurrent_downloads = %d, want 16", cfg.S3.MaxConcurrentDownloads)
	}
}

func TestPrefetchConfig_UpdatedDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Prefetch.MaxConcurrent != 8 {
		t.Errorf("prefetch.max_concurrent = %d, want 8", cfg.Prefetch.MaxConcurrent)
	}
	if cfg.Prefetch.MaxQueue != 128 {
		t.Errorf("prefetch.max_queue = %d, want 128", cfg.Prefetch.MaxQueue)
	}
}

func TestSmartCacheConfig_MergeOverlay(t *testing.T) {
	base := Default()
	overlay := &Config{
		SmartCache: SmartCacheConfig{
			MaxAge:             12 * time.Hour,
			HotAccessThreshold: 5,
		},
		CrossSignal: CrossSignalConfig{
			Enabled:  true,
			Endpoint: "http://traces:10428",
			Timeout:  3 * time.Second,
		},
	}

	merged := mergeConfig(base, overlay)

	if merged.SmartCache.MaxAge != 12*time.Hour {
		t.Errorf("merged max_age = %v, want 12h", merged.SmartCache.MaxAge)
	}
	if merged.SmartCache.HotAccessThreshold != 5 {
		t.Errorf("merged hot_access_threshold = %d, want 5", merged.SmartCache.HotAccessThreshold)
	}
	// Non-overridden fields should keep defaults
	if merged.SmartCache.SnapshotInterval != 60*time.Second {
		t.Errorf("merged snapshot_interval = %v, want 60s (default)", merged.SmartCache.SnapshotInterval)
	}
	if !merged.CrossSignal.Enabled {
		t.Error("merged cross_signal.enabled should be true")
	}
	if merged.CrossSignal.Endpoint != "http://traces:10428" {
		t.Errorf("merged endpoint = %q, want %q", merged.CrossSignal.Endpoint, "http://traces:10428")
	}
}

func TestQueryConfig_FileWorkers_MergeOverlay(t *testing.T) {
	base := Default()
	overlay := &Config{
		Query: QueryConfig{
			FileWorkers: 16,
		},
	}
	merged := mergeConfig(base, overlay)
	if merged.Query.FileWorkers != 16 {
		t.Errorf("merged file_workers = %d, want 16", merged.Query.FileWorkers)
	}
}

func TestS3Config_MaxConcurrentDownloads_MergeOverlay(t *testing.T) {
	base := Default()
	overlay := &Config{
		S3: S3Config{
			MaxConcurrentDownloads: 32,
		},
	}
	merged := mergeConfig(base, overlay)
	if merged.S3.MaxConcurrentDownloads != 32 {
		t.Errorf("merged max_concurrent_downloads = %d, want 32", merged.S3.MaxConcurrentDownloads)
	}
}
