package config

import (
	"strings"
	"time"
)

type Profile string

const (
	ProfileBalanced       Profile = "balanced"
	ProfileMaxPerformance Profile = "max-performance"
	ProfileMaxDurability  Profile = "max-durability"
	ProfileMaxCostSavings Profile = "max-cost-savings"
	ProfileDev            Profile = "dev"
)

func ValidProfiles() []Profile {
	return []Profile{
		ProfileBalanced,
		ProfileMaxPerformance,
		ProfileMaxDurability,
		ProfileMaxCostSavings,
		ProfileDev,
	}
}

func IsValidProfile(name string) bool {
	for _, p := range ValidProfiles() {
		if string(p) == name {
			return true
		}
	}
	return false
}

func ValidProfileNames() string {
	names := make([]string, len(ValidProfiles()))
	for i, p := range ValidProfiles() {
		names[i] = string(p)
	}
	return strings.Join(names, ", ")
}

func ProfileConfig(p Profile) *Config {
	switch p {
	case ProfileMaxPerformance:
		return maxPerformanceConfig()
	case ProfileMaxDurability:
		return maxDurabilityConfig()
	case ProfileMaxCostSavings:
		return maxCostSavingsConfig()
	case ProfileDev:
		return devConfig()
	default:
		return balancedConfig()
	}
}

func (c *Config) ResolveEffectiveProfile() Profile {
	if c.Mode == ModeLogs {
		if c.Role == RoleInsert && c.Logs.Insert.Profile != "" {
			return c.Logs.Insert.Profile
		}
		if c.Role == RoleSelect && c.Logs.Select.Profile != "" {
			return c.Logs.Select.Profile
		}
		if c.Logs.Profile != "" {
			return c.Logs.Profile
		}
	}
	if c.Mode == ModeTraces {
		if c.Role == RoleInsert && c.Traces.Insert.Profile != "" {
			return c.Traces.Insert.Profile
		}
		if c.Role == RoleSelect && c.Traces.Select.Profile != "" {
			return c.Traces.Select.Profile
		}
		if c.Traces.Profile != "" {
			return c.Traces.Profile
		}
	}
	if c.Profile != "" {
		return c.Profile
	}
	return ProfileBalanced
}

func balancedConfig() *Config {
	cfg := Default()
	cfg.Profile = ProfileBalanced
	return cfg
}

func maxPerformanceConfig() *Config {
	cfg := Default()
	cfg.Profile = ProfileMaxPerformance

	cfg.Insert.FlushInterval = 5 * time.Second
	cfg.Insert.FlushLinger = 100 * time.Millisecond
	cfg.Insert.AckMode = "buffer"
	cfg.Insert.CompressionLevel = 3
	cfg.Insert.MaxBufferRows = 100000
	cfg.Insert.MaxBufferBytes = "512MB"
	cfg.Insert.TargetFileSize = "64MB"
	cfg.Insert.RowGroupSize = 5000

	cfg.Select.BufferQueryTimeout = 1 * time.Second
	cfg.Query.FileWorkers = 16
	cfg.Query.MaxConcurrent = 64
	cfg.Query.Timeout = 120 * time.Second
	cfg.Query.MaxRows = 50_000_000
	cfg.Query.SlowThreshold = 10 * time.Second

	cfg.Cache.MemoryLimit = "2GB"
	cfg.Cache.DiskLimit = "100GB"
	cfg.Cache.FooterTTL = 4 * time.Hour
	cfg.Cache.BloomTTL = 4 * time.Hour
	cfg.Cache.PageTTL = 1 * time.Hour

	cfg.SmartCache.TargetHours = 72
	cfg.SmartCache.MaxAge = 72 * time.Hour
	cfg.SmartCache.SnapshotInterval = 30 * time.Second
	cfg.SmartCache.HotAccessThreshold = 2
	cfg.SmartCache.HotWindow = 15 * time.Minute
	cfg.SmartCache.DiskLimitMax = "200GB"

	cfg.Prefetch.ReadAheadDepth = 4
	cfg.Prefetch.MaxConcurrent = 16
	cfg.Prefetch.MaxQueue = 256
	cfg.CrossSignal.Enabled = true

	cfg.S3.MaxConnections = 256
	cfg.S3.MaxConcurrentDownloads = 32
	cfg.S3.Timeout = 15 * time.Second
	cfg.S3.RetryMax = 5

	cfg.Compaction.Enabled = true
	cfg.Compaction.Interval = 2 * time.Minute
	cfg.Compaction.MaxConcurrent = 2
	cfg.Compaction.MinFilesL0 = 5

	cfg.GC.Enabled = true
	cfg.GC.Interval = 3 * time.Hour

	cfg.Manifest.PersistInterval = 1 * time.Minute
	cfg.Manifest.RefreshInterval = 1 * time.Minute
	cfg.Startup.ServeStale = true
	cfg.Startup.WarmupWindow = 72 * time.Hour
	cfg.Startup.MaxWarmupTime = 10 * time.Minute

	cfg.Delete.RewriteDelay = 30 * time.Minute
	cfg.Delete.RewriteBatchSize = 100

	cfg.Stats.PushInterval = 15 * time.Second

	cfg.Peer.MaxConnections = 64
	cfg.Peer.Timeout = 2 * time.Second
	cfg.Discovery.PeerRefreshInterval = 10 * time.Second

	return cfg
}

func maxDurabilityConfig() *Config {
	cfg := Default()
	cfg.Profile = ProfileMaxDurability

	cfg.Insert.AckMode = "flush-sync"
	cfg.Insert.FlushLinger = 0
	cfg.Insert.CompressionLevel = 7

	cfg.Compaction.Enabled = true

	cfg.GC.Enabled = true
	cfg.GC.Interval = 1 * time.Hour

	cfg.Retention.Enabled = true

	cfg.S3.RetryMax = 5
	cfg.S3.RetryBaseDelay = 500 * time.Millisecond

	cfg.Delete.DefaultMode = "permanent"
	cfg.Delete.VerifyInterval = 1 * time.Hour

	cfg.Manifest.PersistInterval = 1 * time.Minute
	cfg.SmartCache.SnapshotInterval = 30 * time.Second

	cfg.Stats.Enabled = true
	cfg.Stats.SnapshotInterval = 1 * time.Minute
	cfg.Stats.PushCompression = true

	return cfg
}

func maxCostSavingsConfig() *Config {
	cfg := Default()
	cfg.Profile = ProfileMaxCostSavings

	cfg.Insert.FlushInterval = 30 * time.Second
	cfg.Insert.FlushLinger = 1 * time.Second
	cfg.Insert.CompressionLevel = 11
	cfg.Insert.MaxBufferRows = 25000
	cfg.Insert.MaxBufferBytes = "128MB"
	cfg.Insert.TargetFileSize = "256MB"
	cfg.Insert.RowGroupSize = 50000
	cfg.Insert.AckMode = "buffer"
	cfg.Insert.PeerReplicate = false

	cfg.Select.BufferQueryEnabled = false
	cfg.Query.FileWorkers = 4
	cfg.Query.MaxConcurrent = 16
	cfg.Query.Timeout = 30 * time.Second
	cfg.Query.MaxRows = 1_000_000
	cfg.Query.SlowThreshold = 3 * time.Second

	cfg.Cache.MemoryLimit = "128MB"
	cfg.Cache.DiskLimit = "10GB"
	cfg.Cache.FooterTTL = 30 * time.Minute
	cfg.Cache.BloomTTL = 30 * time.Minute
	cfg.Cache.PageTTL = 5 * time.Minute

	cfg.SmartCache.TargetHours = 6
	cfg.SmartCache.MaxAge = 6 * time.Hour
	cfg.SmartCache.SnapshotInterval = 5 * time.Minute
	cfg.SmartCache.HotAccessThreshold = 5
	cfg.SmartCache.HotWindow = 5 * time.Minute
	cfg.SmartCache.DiskLimitMax = "20GB"
	cfg.SmartCache.QueryGracePeriod = 1 * time.Minute

	cfg.Prefetch.Correlated = false
	cfg.Prefetch.ReadAheadDepth = 0
	cfg.Prefetch.MaxConcurrent = 2
	cfg.Prefetch.MaxQueue = 32
	cfg.CrossSignal.Enabled = false

	cfg.S3.MaxConnections = 64
	cfg.S3.MaxConcurrentDownloads = 8
	cfg.S3.Timeout = 60 * time.Second

	cfg.Compaction.Enabled = false

	cfg.GC.Enabled = false

	cfg.Retention.Enabled = true
	cfg.Retention.Default = "90d"

	cfg.Manifest.PersistInterval = 15 * time.Minute
	cfg.Manifest.RefreshInterval = 15 * time.Minute
	cfg.Startup.WarmupWindow = 6 * time.Hour
	cfg.Startup.MaxWarmupTime = 2 * time.Minute

	cfg.Delete.DefaultMode = "hide"
	cfg.Delete.VerifyInterval = 24 * time.Hour
	cfg.Delete.RewriteDelay = 6 * time.Hour
	cfg.Delete.RewriteBatchSize = 25

	cfg.Stats.Enabled = false
	cfg.Stats.PushInterval = 5 * time.Minute
	cfg.Stats.SnapshotInterval = 30 * time.Minute

	cfg.UI.Enabled = false

	cfg.Peer.MaxConnections = 16
	cfg.Peer.Timeout = 10 * time.Second
	cfg.Discovery.PeerRefreshInterval = 60 * time.Second

	return cfg
}

func devConfig() *Config {
	cfg := Default()
	cfg.Profile = ProfileDev

	cfg.Insert.FlushInterval = 1 * time.Second
	cfg.Insert.FlushLinger = 0
	cfg.Insert.AckMode = "buffer"
	cfg.Insert.CompressionLevel = 1
	cfg.Insert.MaxBufferRows = 1000
	cfg.Insert.MaxBufferBytes = "32MB"
	cfg.Insert.TargetFileSize = "8MB"
	cfg.Insert.RowGroupSize = 1000
	cfg.Insert.PeerReplicate = false

	cfg.Select.BufferQueryTimeout = 2 * time.Second
	cfg.Query.FileWorkers = 2
	cfg.Query.MaxConcurrent = 4
	cfg.Query.MaxRows = 100_000
	cfg.Query.SlowThreshold = 1 * time.Second

	cfg.Cache.MemoryLimit = "64MB"
	cfg.Cache.DiskLimit = "1GB"
	cfg.Cache.FooterTTL = 1 * time.Minute
	cfg.Cache.BloomTTL = 1 * time.Minute
	cfg.Cache.PageTTL = 1 * time.Minute

	cfg.SmartCache.TargetHours = 1
	cfg.SmartCache.MaxAge = 1 * time.Hour
	cfg.SmartCache.DiskLimitMax = "2GB"

	cfg.Prefetch.Correlated = false
	cfg.Prefetch.ReadAheadDepth = 0
	cfg.Prefetch.MaxConcurrent = 1
	cfg.Prefetch.MaxQueue = 8
	cfg.CrossSignal.Enabled = false

	cfg.S3.ForcePathStyle = true
	cfg.S3.MaxConnections = 16
	cfg.S3.MaxConcurrentDownloads = 4
	cfg.S3.RetryMax = 1

	cfg.Compaction.Enabled = false

	cfg.GC.Enabled = false

	cfg.Retention.Enabled = false

	cfg.Stats.Enabled = false

	cfg.Manifest.PersistInterval = 5 * time.Second
	cfg.Manifest.RefreshInterval = 5 * time.Second
	cfg.Startup.ServeStale = true
	cfg.Startup.WarmupWindow = 1 * time.Hour
	cfg.Startup.MaxWarmupTime = 10 * time.Second

	cfg.Delete.RewriteDelay = 10 * time.Second
	cfg.Delete.RewriteBatchSize = 5

	cfg.Peer.AZAware = false
	cfg.Peer.MaxConnections = 8

	return cfg
}
