package config

import (
	"testing"
	"time"
)

func TestProfileRegression_InsertPath(t *testing.T) {
	type insertExpect struct {
		flushInterval    time.Duration
		walEnabled       bool
		walMaxBytes      string
		compressionLevel int
		maxBufferRows    int
		maxBufferBytes   string
		targetFileSize   string
		rowGroupSize     int
		ackMode          string
	}

	tests := map[Profile]insertExpect{
		ProfileBalanced:       {10 * time.Second, true, "512MB", 7, 50000, "256MB", "128MB", 10000, "buffer"},
		ProfileMaxPerformance: {5 * time.Second, false, "512MB", 3, 100000, "512MB", "64MB", 5000, "buffer"},
		ProfileMaxDurability:  {10 * time.Second, true, "1GB", 7, 50000, "256MB", "128MB", 10000, "flush-sync"},
		ProfileMaxCostSavings: {30 * time.Second, false, "512MB", 11, 25000, "128MB", "256MB", 50000, "buffer"},
		ProfileDev:            {1 * time.Second, false, "32MB", 1, 1000, "32MB", "8MB", 1000, "buffer"},
	}

	for profile, expect := range tests {
		t.Run(string(profile), func(t *testing.T) {
			cfg := ProfileConfig(profile)

			if cfg.Insert.FlushInterval != expect.flushInterval {
				t.Errorf("flush_interval = %v, want %v", cfg.Insert.FlushInterval, expect.flushInterval)
			}
			if cfg.Insert.WALEnabled != expect.walEnabled {
				t.Errorf("wal_enabled = %v, want %v", cfg.Insert.WALEnabled, expect.walEnabled)
			}
			if cfg.Insert.WALMaxBytes != expect.walMaxBytes {
				t.Errorf("wal_max_bytes = %q, want %q", cfg.Insert.WALMaxBytes, expect.walMaxBytes)
			}
			if cfg.Insert.CompressionLevel != expect.compressionLevel {
				t.Errorf("compression_level = %d, want %d", cfg.Insert.CompressionLevel, expect.compressionLevel)
			}
			if cfg.Insert.MaxBufferRows != expect.maxBufferRows {
				t.Errorf("max_buffer_rows = %d, want %d", cfg.Insert.MaxBufferRows, expect.maxBufferRows)
			}
			if cfg.Insert.MaxBufferBytes != expect.maxBufferBytes {
				t.Errorf("max_buffer_bytes = %q, want %q", cfg.Insert.MaxBufferBytes, expect.maxBufferBytes)
			}
			if cfg.Insert.TargetFileSize != expect.targetFileSize {
				t.Errorf("target_file_size = %q, want %q", cfg.Insert.TargetFileSize, expect.targetFileSize)
			}
			if cfg.Insert.RowGroupSize != expect.rowGroupSize {
				t.Errorf("row_group_size = %d, want %d", cfg.Insert.RowGroupSize, expect.rowGroupSize)
			}
			if cfg.Insert.AckMode != expect.ackMode {
				t.Errorf("ack_mode = %q, want %q", cfg.Insert.AckMode, expect.ackMode)
			}
		})
	}
}

func TestProfileRegression_SelectQuery(t *testing.T) {
	type queryExpect struct {
		bufferQueryEnabled bool
		fileWorkers        int
		maxConcurrent      int
		timeout            time.Duration
		maxRows            int64
	}

	tests := map[Profile]queryExpect{
		ProfileBalanced:       {true, 8, 32, 60 * time.Second, 10_000_000},
		ProfileMaxPerformance: {true, 16, 64, 120 * time.Second, 50_000_000},
		ProfileMaxDurability:  {true, 8, 32, 60 * time.Second, 10_000_000},
		ProfileMaxCostSavings: {false, 4, 16, 30 * time.Second, 1_000_000},
		ProfileDev:            {true, 2, 4, 60 * time.Second, 100_000},
	}

	for profile, expect := range tests {
		t.Run(string(profile), func(t *testing.T) {
			cfg := ProfileConfig(profile)

			if cfg.Select.BufferQueryEnabled != expect.bufferQueryEnabled {
				t.Errorf("buffer_query_enabled = %v, want %v", cfg.Select.BufferQueryEnabled, expect.bufferQueryEnabled)
			}
			if cfg.Query.FileWorkers != expect.fileWorkers {
				t.Errorf("file_workers = %d, want %d", cfg.Query.FileWorkers, expect.fileWorkers)
			}
			if cfg.Query.MaxConcurrent != expect.maxConcurrent {
				t.Errorf("max_concurrent = %d, want %d", cfg.Query.MaxConcurrent, expect.maxConcurrent)
			}
			if cfg.Query.Timeout != expect.timeout {
				t.Errorf("timeout = %v, want %v", cfg.Query.Timeout, expect.timeout)
			}
			if cfg.Query.MaxRows != expect.maxRows {
				t.Errorf("max_rows = %d, want %d", cfg.Query.MaxRows, expect.maxRows)
			}
		})
	}
}

func TestProfileRegression_Cache(t *testing.T) {
	type cacheExpect struct {
		memoryLimit string
		diskLimit   string
		footerTTL   time.Duration
		bloomTTL    time.Duration
		pageTTL     time.Duration
	}

	tests := map[Profile]cacheExpect{
		ProfileBalanced:       {"512MB", "50GB", 1 * time.Hour, 1 * time.Hour, 10 * time.Minute},
		ProfileMaxPerformance: {"2GB", "100GB", 4 * time.Hour, 4 * time.Hour, 1 * time.Hour},
		ProfileMaxDurability:  {"512MB", "50GB", 1 * time.Hour, 1 * time.Hour, 10 * time.Minute},
		ProfileMaxCostSavings: {"128MB", "10GB", 30 * time.Minute, 30 * time.Minute, 5 * time.Minute},
		ProfileDev:            {"64MB", "1GB", 1 * time.Minute, 1 * time.Minute, 1 * time.Minute},
	}

	for profile, expect := range tests {
		t.Run(string(profile), func(t *testing.T) {
			cfg := ProfileConfig(profile)

			if cfg.Cache.MemoryLimit != expect.memoryLimit {
				t.Errorf("memory_limit = %q, want %q", cfg.Cache.MemoryLimit, expect.memoryLimit)
			}
			if cfg.Cache.DiskLimit != expect.diskLimit {
				t.Errorf("disk_limit = %q, want %q", cfg.Cache.DiskLimit, expect.diskLimit)
			}
			if cfg.Cache.FooterTTL != expect.footerTTL {
				t.Errorf("footer_ttl = %v, want %v", cfg.Cache.FooterTTL, expect.footerTTL)
			}
			if cfg.Cache.BloomTTL != expect.bloomTTL {
				t.Errorf("bloom_ttl = %v, want %v", cfg.Cache.BloomTTL, expect.bloomTTL)
			}
			if cfg.Cache.PageTTL != expect.pageTTL {
				t.Errorf("page_ttl = %v, want %v", cfg.Cache.PageTTL, expect.pageTTL)
			}
		})
	}
}

func TestProfileRegression_S3(t *testing.T) {
	type s3Expect struct {
		maxConns    int
		timeout     time.Duration
		retryMax    int
		pathStyle   bool
		maxDownload int
	}

	tests := map[Profile]s3Expect{
		ProfileBalanced:       {128, 30 * time.Second, 3, false, 16},
		ProfileMaxPerformance: {256, 15 * time.Second, 5, false, 32},
		ProfileMaxDurability:  {128, 30 * time.Second, 5, false, 16},
		ProfileMaxCostSavings: {64, 60 * time.Second, 3, false, 8},
		ProfileDev:            {16, 30 * time.Second, 1, true, 4},
	}

	for profile, expect := range tests {
		t.Run(string(profile), func(t *testing.T) {
			cfg := ProfileConfig(profile)

			if cfg.S3.MaxConnections != expect.maxConns {
				t.Errorf("max_connections = %d, want %d", cfg.S3.MaxConnections, expect.maxConns)
			}
			if cfg.S3.Timeout != expect.timeout {
				t.Errorf("timeout = %v, want %v", cfg.S3.Timeout, expect.timeout)
			}
			if cfg.S3.RetryMax != expect.retryMax {
				t.Errorf("retry_max = %d, want %d", cfg.S3.RetryMax, expect.retryMax)
			}
			if cfg.S3.ForcePathStyle != expect.pathStyle {
				t.Errorf("force_path_style = %v, want %v", cfg.S3.ForcePathStyle, expect.pathStyle)
			}
			if cfg.S3.MaxConcurrentDownloads != expect.maxDownload {
				t.Errorf("max_concurrent_downloads = %d, want %d", cfg.S3.MaxConcurrentDownloads, expect.maxDownload)
			}
		})
	}
}

func TestProfileRegression_Compaction(t *testing.T) {
	type compactExpect struct {
		enabled       bool
		interval      time.Duration
		maxConcurrent int
		minFilesL0    int
	}

	tests := map[Profile]compactExpect{
		ProfileBalanced:       {false, 5 * time.Minute, 1, 10},
		ProfileMaxPerformance: {true, 2 * time.Minute, 2, 5},
		ProfileMaxDurability:  {true, 5 * time.Minute, 1, 10},
		ProfileMaxCostSavings: {false, 5 * time.Minute, 1, 10},
		ProfileDev:            {false, 5 * time.Minute, 1, 10},
	}

	for profile, expect := range tests {
		t.Run(string(profile), func(t *testing.T) {
			cfg := ProfileConfig(profile)

			if cfg.Compaction.Enabled != expect.enabled {
				t.Errorf("enabled = %v, want %v", cfg.Compaction.Enabled, expect.enabled)
			}
			if cfg.Compaction.Interval != expect.interval {
				t.Errorf("interval = %v, want %v", cfg.Compaction.Interval, expect.interval)
			}
			if cfg.Compaction.MaxConcurrent != expect.maxConcurrent {
				t.Errorf("max_concurrent = %d, want %d", cfg.Compaction.MaxConcurrent, expect.maxConcurrent)
			}
			if cfg.Compaction.MinFilesL0 != expect.minFilesL0 {
				t.Errorf("min_files_l0 = %d, want %d", cfg.Compaction.MinFilesL0, expect.minFilesL0)
			}
		})
	}
}

func TestProfileRegression_Delete(t *testing.T) {
	type deleteExpect struct {
		defaultMode    string
		verifyInterval time.Duration
	}

	tests := map[Profile]deleteExpect{
		ProfileBalanced:       {"auto", 6 * time.Hour},
		ProfileMaxPerformance: {"auto", 6 * time.Hour},
		ProfileMaxDurability:  {"permanent", 1 * time.Hour},
		ProfileMaxCostSavings: {"hide", 24 * time.Hour},
		ProfileDev:            {"auto", 6 * time.Hour},
	}

	for profile, expect := range tests {
		t.Run(string(profile), func(t *testing.T) {
			cfg := ProfileConfig(profile)

			if cfg.Delete.DefaultMode != expect.defaultMode {
				t.Errorf("default_mode = %q, want %q", cfg.Delete.DefaultMode, expect.defaultMode)
			}
			if cfg.Delete.VerifyInterval != expect.verifyInterval {
				t.Errorf("verify_interval = %v, want %v", cfg.Delete.VerifyInterval, expect.verifyInterval)
			}
		})
	}
}

func TestProfileRegression_Prefetch(t *testing.T) {
	type prefetchExpect struct {
		correlated     bool
		readAheadDepth int
		crossSignal    bool
	}

	tests := map[Profile]prefetchExpect{
		ProfileBalanced:       {true, 2, false},
		ProfileMaxPerformance: {true, 4, true},
		ProfileMaxDurability:  {true, 2, false},
		ProfileMaxCostSavings: {false, 0, false},
		ProfileDev:            {false, 0, false},
	}

	for profile, expect := range tests {
		t.Run(string(profile), func(t *testing.T) {
			cfg := ProfileConfig(profile)

			if cfg.Prefetch.Correlated != expect.correlated {
				t.Errorf("correlated = %v, want %v", cfg.Prefetch.Correlated, expect.correlated)
			}
			if cfg.Prefetch.ReadAheadDepth != expect.readAheadDepth {
				t.Errorf("read_ahead_depth = %d, want %d", cfg.Prefetch.ReadAheadDepth, expect.readAheadDepth)
			}
			if cfg.CrossSignal.Enabled != expect.crossSignal {
				t.Errorf("cross_signal.enabled = %v, want %v", cfg.CrossSignal.Enabled, expect.crossSignal)
			}
		})
	}
}

func TestProfileRegression_Peer(t *testing.T) {
	type peerExpect struct {
		maxConns int
		timeout  time.Duration
		azAware  bool
	}

	tests := map[Profile]peerExpect{
		ProfileBalanced:       {32, 5 * time.Second, true},
		ProfileMaxPerformance: {64, 2 * time.Second, true},
		ProfileMaxDurability:  {32, 5 * time.Second, true},
		ProfileMaxCostSavings: {16, 10 * time.Second, true},
		ProfileDev:            {8, 5 * time.Second, false},
	}

	for profile, expect := range tests {
		t.Run(string(profile), func(t *testing.T) {
			cfg := ProfileConfig(profile)

			if cfg.Peer.MaxConnections != expect.maxConns {
				t.Errorf("max_connections = %d, want %d", cfg.Peer.MaxConnections, expect.maxConns)
			}
			if cfg.Peer.Timeout != expect.timeout {
				t.Errorf("timeout = %v, want %v", cfg.Peer.Timeout, expect.timeout)
			}
			if cfg.Peer.AZAware != expect.azAware {
				t.Errorf("az_aware = %v, want %v", cfg.Peer.AZAware, expect.azAware)
			}
		})
	}
}
