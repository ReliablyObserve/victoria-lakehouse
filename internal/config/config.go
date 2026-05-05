package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Mode string

const (
	ModeLogs   Mode = "logs"
	ModeTraces Mode = "traces"
)

type Role string

const (
	RoleAll    Role = "all"
	RoleInsert Role = "insert"
	RoleSelect Role = "select"
)

type Topology string

const (
	TopologyAuto        Topology = "auto"
	TopologyStorageNode Topology = "storage-node"
	TopologyDirect      Topology = "direct"
	TopologyLokiProxy   Topology = "loki-proxy"
)

type Config struct {
	Mode     Mode     `yaml:"mode"`
	Role     Role     `yaml:"role"`
	Topology Topology `yaml:"topology"`

	S3             S3Config             `yaml:"s3"`
	Cache          CacheConfig          `yaml:"cache"`
	Discovery      DiscoveryConfig      `yaml:"discovery"`
	HotBoundary    string               `yaml:"hot_boundary"`
	Manifest       ManifestConfig       `yaml:"manifest"`
	Prefetch       PrefetchConfig       `yaml:"prefetch"`
	Peer           PeerConfig           `yaml:"peer"`
	Startup        StartupConfig        `yaml:"startup"`
	Query          QueryConfig          `yaml:"query"`
	Insert         InsertConfig         `yaml:"insert"`
	Select         SelectConfig         `yaml:"select"`
	Schema         SchemaConfig         `yaml:"schema"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
	Tenant         TenantConfig         `yaml:"tenant"`
	Compaction     CompactionConfig     `yaml:"compaction"`
	Delete         DeleteConfig         `yaml:"delete"`
}

type InsertConfig struct {
	FlushInterval    time.Duration `yaml:"flush_interval"`
	MaxBufferRows    int           `yaml:"max_buffer_rows"`
	MaxBufferBytes   string        `yaml:"max_buffer_bytes"`
	TargetFileSize   string        `yaml:"target_file_size"`
	RowGroupSize     int           `yaml:"row_group_size"`
	BloomColumns     []string      `yaml:"bloom_columns"`
	CompressionLevel int           `yaml:"compression_level"`
	WALEnabled       bool          `yaml:"wal_enabled"`
	WALDir           string        `yaml:"wal_dir"`
	WALMaxBytes      string        `yaml:"wal_max_bytes"`
}

func (c *InsertConfig) MaxBufferBytesN() int64 {
	n, _ := ParseSizeBytes(c.MaxBufferBytes)
	if n <= 0 {
		return 256 * 1024 * 1024
	}
	return n
}

func (c *InsertConfig) TargetFileSizeN() int64 {
	n, _ := ParseSizeBytes(c.TargetFileSize)
	if n <= 0 {
		return 128 * 1024 * 1024
	}
	return n
}

func (c *InsertConfig) WALMaxBytesN() int64 {
	n, _ := ParseSizeBytes(c.WALMaxBytes)
	if n <= 0 {
		return 512 * 1024 * 1024
	}
	return n
}

func (c *Config) InsertEnabled() bool {
	return c.Role == RoleAll || c.Role == RoleInsert
}

func (c *Config) SelectEnabled() bool {
	return c.Role == RoleAll || c.Role == RoleSelect
}

type SelectConfig struct {
	BufferQueryEnabled    bool          `yaml:"buffer_query_enabled"`
	InsertHeadlessService string        `yaml:"insert_headless_service"`
	BufferQueryTimeout    time.Duration `yaml:"buffer_query_timeout"`
}

type S3Config struct {
	Bucket         string        `yaml:"bucket"`
	Region         string        `yaml:"region"`
	Prefix         string        `yaml:"prefix"`
	Endpoint       string        `yaml:"endpoint"`
	AccessKey      string        `yaml:"access_key"`
	SecretKey      string        `yaml:"secret_key"`
	ForcePathStyle bool          `yaml:"force_path_style"`
	MaxConnections int           `yaml:"max_connections"`
	Timeout        time.Duration `yaml:"timeout"`
	RetryMax       int           `yaml:"retry_max"`
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
}

type CacheConfig struct {
	MemoryLimit       string        `yaml:"memory_limit"`
	DiskPath          string        `yaml:"disk_path"`
	DiskLimit         string        `yaml:"disk_limit"`
	EvictionWatermark float64       `yaml:"eviction_watermark"`
	FooterTTL         time.Duration `yaml:"footer_ttl"`
	BloomTTL          time.Duration `yaml:"bloom_ttl"`
	PageTTL           time.Duration `yaml:"page_ttl"`
}

type DiscoveryConfig struct {
	HeadlessService      string        `yaml:"headless_service"`
	StorageNodes         []string      `yaml:"storage_nodes"`
	PartitionAuthKey     string        `yaml:"partition_auth_key"`
	RefreshInterval      time.Duration `yaml:"refresh_interval"`
	Timeout              time.Duration `yaml:"timeout"`
	PeerHeadlessService  string        `yaml:"peer_headless_service"`
	PeerRefreshInterval  time.Duration `yaml:"peer_refresh_interval"`
}

type ManifestConfig struct {
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	SQSQueueURL     string        `yaml:"sqs_queue_url"`
	SQSRegion       string        `yaml:"sqs_region"`
	SQSWaitTime     time.Duration `yaml:"sqs_wait_time"`
	PersistPath     string        `yaml:"persist_path"`
	PersistInterval time.Duration `yaml:"persist_interval"`
}

type PrefetchConfig struct {
	Correlated    bool `yaml:"correlated"`
	ReadAheadDepth int  `yaml:"read_ahead_depth"`
	MaxConcurrent int  `yaml:"max_concurrent"`
	MaxQueue      int  `yaml:"max_queue"`
}

type PeerConfig struct {
	AuthKey        string        `yaml:"auth_key"`
	Timeout        time.Duration `yaml:"timeout"`
	MaxConnections int           `yaml:"max_connections"`
}

type StartupConfig struct {
	ServeStale    bool          `yaml:"serve_stale"`
	WarmupWindow  time.Duration `yaml:"warmup_window"`
	MaxWarmupTime time.Duration `yaml:"max_warmup_time"`
}

type QueryConfig struct {
	MaxConcurrent int           `yaml:"max_concurrent"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxRows       int64         `yaml:"max_rows"`
	SlowThreshold time.Duration `yaml:"slow_threshold"`
}

type CircuitBreakerConfig struct {
	Threshold        int           `yaml:"threshold"`
	Timeout          time.Duration `yaml:"timeout"`
	SuccessThreshold int           `yaml:"success_threshold"`
}

type TenantConfig struct {
	DefaultPrefix  string `yaml:"default_prefix"`
	PrefixTemplate string `yaml:"prefix_template"`
}

type CompactionConfig struct {
	Enabled        bool          `yaml:"enabled"`
	Interval       time.Duration `yaml:"interval"`
	MaxConcurrent  int           `yaml:"max_concurrent"`
	MinFilesL0     int           `yaml:"min_files_l0"`
	MinFilesL1     int           `yaml:"min_files_l1"`
	MinAge         time.Duration `yaml:"min_age"`
	LeaderElection string        `yaml:"leader_election"`
	LeaseDuration  time.Duration `yaml:"lease_duration"`
	S3LockTTL      time.Duration `yaml:"s3_lock_ttl"`
	S3Heartbeat    time.Duration `yaml:"s3_heartbeat"`
}

type DeleteConfig struct {
	Enabled              bool                  `yaml:"enabled"`
	DefaultMode          string                `yaml:"default_mode"`
	AutoRewriteClasses   []string              `yaml:"auto_rewrite_classes"`
	RewriteDelay         time.Duration         `yaml:"rewrite_delay"`
	RewriteBatchSize     int                   `yaml:"rewrite_batch_size"`
	RewriteMaxConcurrent int                   `yaml:"rewrite_max_concurrent"`
	PersistPath          string                `yaml:"persist_path"`
	CostWarningThreshold float64               `yaml:"cost_warning_threshold"`
	ForceGlacierHeader   string                `yaml:"force_glacier_header"`
	VerifyInterval       time.Duration         `yaml:"verify_interval"`
	LifecycleRules       []LifecycleRuleConfig `yaml:"lifecycle_rules"`
}

type LifecycleRuleConfig struct {
	TransitionDays int    `yaml:"transition_days"`
	StorageClass   string `yaml:"storage_class"`
}

type ExtraPromotedColumn struct {
	Name  string `yaml:"name"`
	Type  string `yaml:"type"`
	Bloom bool   `yaml:"bloom"`
}

type SchemaConfig struct {
	ExtraPromoted []ExtraPromotedColumn `yaml:"extra_promoted"`
}

func Default() *Config {
	return &Config{
		Role:     RoleAll,
		Topology: TopologyAuto,

		S3: S3Config{
			Region:         "us-east-1",
			MaxConnections: 128,
			Timeout:        30 * time.Second,
			RetryMax:       3,
			RetryBaseDelay: 200 * time.Millisecond,
		},

		Cache: CacheConfig{
			MemoryLimit:       "512MB",
			DiskPath:          "/data/lakehouse/cache",
			DiskLimit:         "50GB",
			EvictionWatermark: 0.8,
			FooterTTL:         1 * time.Hour,
			BloomTTL:          1 * time.Hour,
			PageTTL:           10 * time.Minute,
		},

		Discovery: DiscoveryConfig{
			RefreshInterval:     5 * time.Minute,
			Timeout:             10 * time.Second,
			PeerRefreshInterval: 30 * time.Second,
		},

		Manifest: ManifestConfig{
			RefreshInterval: 5 * time.Minute,
			SQSWaitTime:     20 * time.Second,
			PersistPath:     "/data/lakehouse",
			PersistInterval: 5 * time.Minute,
		},

		Prefetch: PrefetchConfig{
			Correlated:     true,
			ReadAheadDepth: 2,
			MaxConcurrent:  4,
			MaxQueue:       64,
		},

		Peer: PeerConfig{
			Timeout:        5 * time.Second,
			MaxConnections: 32,
		},

		Startup: StartupConfig{
			ServeStale:    false,
			WarmupWindow:  24 * time.Hour,
			MaxWarmupTime: 5 * time.Minute,
		},

		Query: QueryConfig{
			MaxConcurrent: 32,
			Timeout:       60 * time.Second,
			MaxRows:       10_000_000,
			SlowThreshold: 5 * time.Second,
		},

		CircuitBreaker: CircuitBreakerConfig{
			Threshold:        5,
			Timeout:          30 * time.Second,
			SuccessThreshold: 2,
		},

		Insert: InsertConfig{
			FlushInterval:    10 * time.Second,
			MaxBufferRows:    50000,
			MaxBufferBytes:   "256MB",
			TargetFileSize:   "128MB",
			RowGroupSize:     10000,
			BloomColumns:     []string{"service.name", "trace_id"},
			CompressionLevel: 3,
			WALEnabled:       true,
			WALDir:           "/data/lakehouse/wal",
			WALMaxBytes:      "512MB",
		},

		Select: SelectConfig{
			BufferQueryEnabled: true,
			BufferQueryTimeout: 2 * time.Second,
		},

		Tenant: TenantConfig{
			PrefixTemplate: "{AccountID}/{ProjectID}/",
		},

		Compaction: CompactionConfig{
			Enabled:        false,
			Interval:       5 * time.Minute,
			MaxConcurrent:  1,
			MinFilesL0:     10,
			MinFilesL1:     10,
			MinAge:         1 * time.Hour,
			LeaderElection: "auto",
			LeaseDuration:  15 * time.Second,
			S3LockTTL:      60 * time.Second,
			S3Heartbeat:    15 * time.Second,
		},

		Delete: DeleteConfig{
			Enabled:              true,
			DefaultMode:          "auto",
			AutoRewriteClasses:   []string{"STANDARD"},
			RewriteDelay:         time.Hour,
			RewriteBatchSize:     50,
			RewriteMaxConcurrent: 2,
			PersistPath:          "/data/lakehouse/tombstones",
			CostWarningThreshold: 10.0,
			ForceGlacierHeader:   "X-Force-Glacier-Delete",
			VerifyInterval:       6 * time.Hour,
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	var wrapper struct {
		Lakehouse Config `yaml:"lakehouse"`
	}
	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}

	merged := mergeConfig(cfg, &wrapper.Lakehouse)
	return merged, nil
}

func (c *Config) Validate() error {
	if c.Mode == "" {
		return fmt.Errorf("--lakehouse.mode is required (logs or traces)")
	}
	if c.Mode != ModeLogs && c.Mode != ModeTraces {
		return fmt.Errorf("--lakehouse.mode must be 'logs' or 'traces', got %q", c.Mode)
	}
	if c.S3.Bucket == "" {
		return fmt.Errorf("--lakehouse.s3.bucket is required")
	}

	switch c.Role {
	case RoleAll, RoleInsert, RoleSelect, "":
	default:
		return fmt.Errorf("--lakehouse.role must be one of: all, insert, select; got %q", c.Role)
	}
	if c.Role == "" {
		c.Role = RoleAll
	}

	if c.InsertEnabled() {
		if c.Insert.TargetFileSize == "" {
			return fmt.Errorf("--lakehouse.insert.target-file-size is required when insert enabled")
		}
		if c.Insert.FlushInterval <= 0 {
			return fmt.Errorf("--lakehouse.insert.flush-interval must be positive")
		}
		if c.Insert.MaxBufferRows <= 0 {
			return fmt.Errorf("--lakehouse.insert.max-buffer-rows must be positive")
		}
		if c.Insert.RowGroupSize <= 0 {
			return fmt.Errorf("--lakehouse.insert.row-group-size must be positive")
		}
		if c.Insert.CompressionLevel < 1 || c.Insert.CompressionLevel > 22 {
			return fmt.Errorf("--lakehouse.insert.compression-level must be 1-22, got %d", c.Insert.CompressionLevel)
		}
	}

	for _, ep := range c.Schema.ExtraPromoted {
		if ep.Name == "" {
			return fmt.Errorf("--lakehouse.schema.extra-promoted: name is required")
		}
		switch ep.Type {
		case "string", "int32", "int64", "float64":
		default:
			return fmt.Errorf("--lakehouse.schema.extra-promoted %q: type must be string, int32, int64, or float64; got %q", ep.Name, ep.Type)
		}
	}

	switch c.Topology {
	case TopologyAuto, TopologyStorageNode, TopologyDirect, TopologyLokiProxy:
	default:
		return fmt.Errorf("--lakehouse.topology must be one of: auto, storage-node, direct, loki-proxy; got %q", c.Topology)
	}

	if c.Cache.EvictionWatermark <= 0 || c.Cache.EvictionWatermark > 1 {
		return fmt.Errorf("--lakehouse.cache.eviction-watermark must be in (0, 1], got %f", c.Cache.EvictionWatermark)
	}
	if c.S3.MaxConnections <= 0 {
		return fmt.Errorf("--lakehouse.s3.max-connections must be positive, got %d", c.S3.MaxConnections)
	}
	if c.Query.MaxConcurrent <= 0 {
		return fmt.Errorf("--lakehouse.query.max-concurrent must be positive, got %d", c.Query.MaxConcurrent)
	}
	if c.Query.MaxRows <= 0 {
		return fmt.Errorf("--lakehouse.query.max-rows must be positive, got %d", c.Query.MaxRows)
	}
	if c.CircuitBreaker.Threshold <= 0 {
		return fmt.Errorf("--lakehouse.circuit-breaker.threshold must be positive, got %d", c.CircuitBreaker.Threshold)
	}

	switch c.Compaction.LeaderElection {
	case "auto", "k8s", "s3", "none", "":
	default:
		return fmt.Errorf("--lakehouse.compaction.leader-election must be one of: auto, k8s, s3, none; got %q", c.Compaction.LeaderElection)
	}
	if c.Compaction.Enabled {
		if c.Compaction.Interval <= 0 {
			return fmt.Errorf("--lakehouse.compaction.interval must be positive")
		}
		if c.Compaction.MaxConcurrent <= 0 {
			return fmt.Errorf("--lakehouse.compaction.max-concurrent must be positive")
		}
		if c.Compaction.MinFilesL0 < 2 {
			return fmt.Errorf("--lakehouse.compaction.min-files-l0 must be >= 2")
		}
		if c.Compaction.MinFilesL1 < 2 {
			return fmt.Errorf("--lakehouse.compaction.min-files-l1 must be >= 2")
		}
	}

	return nil
}

func ParseSizeBytes(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	s = strings.TrimSpace(s)
	multiplier := int64(1)
	upper := strings.ToUpper(s)
	switch {
	case strings.HasSuffix(upper, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		s = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(upper, "GB"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(upper, "MB"):
		multiplier = 1024 * 1024
		s = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(upper, "KB"):
		multiplier = 1024
		s = strings.TrimSpace(s[:len(s)-2])
	case strings.HasSuffix(upper, "B"):
		s = strings.TrimSpace(s[:len(s)-1])
	}
	var val int64
	_, err := fmt.Sscanf(s, "%d", &val)
	if err != nil {
		return 0, fmt.Errorf("parse size %q: %w", s, err)
	}
	return val * multiplier, nil
}

func (c *Config) CacheMemoryBytes() int64 {
	n, _ := ParseSizeBytes(c.Cache.MemoryLimit)
	if n <= 0 {
		return 512 * 1024 * 1024
	}
	return n
}

func (c *Config) CacheDiskBytes() int64 {
	n, _ := ParseSizeBytes(c.Cache.DiskLimit)
	if n <= 0 {
		return 50 * 1024 * 1024 * 1024
	}
	return n
}

func (c *Config) ListenAddr() string {
	if c.Mode == ModeTraces {
		return ":10428"
	}
	return ":9428"
}

func (c *Config) AutoPrefix() string {
	if c.S3.Prefix != "" {
		return c.S3.Prefix
	}
	if c.Mode == ModeTraces {
		return "traces/"
	}
	return "logs/"
}

func mergeConfig(base, overlay *Config) *Config {
	if overlay.Mode != "" {
		base.Mode = overlay.Mode
	}
	if overlay.Topology != "" {
		base.Topology = overlay.Topology
	}

	// S3
	if overlay.S3.Bucket != "" {
		base.S3.Bucket = overlay.S3.Bucket
	}
	if overlay.S3.Region != "" {
		base.S3.Region = overlay.S3.Region
	}
	if overlay.S3.Prefix != "" {
		base.S3.Prefix = overlay.S3.Prefix
	}
	if overlay.S3.Endpoint != "" {
		base.S3.Endpoint = overlay.S3.Endpoint
	}
	if overlay.S3.AccessKey != "" {
		base.S3.AccessKey = overlay.S3.AccessKey
	}
	if overlay.S3.SecretKey != "" {
		base.S3.SecretKey = overlay.S3.SecretKey
	}
	if overlay.S3.ForcePathStyle {
		base.S3.ForcePathStyle = true
	}
	if overlay.S3.MaxConnections > 0 {
		base.S3.MaxConnections = overlay.S3.MaxConnections
	}
	if overlay.S3.Timeout > 0 {
		base.S3.Timeout = overlay.S3.Timeout
	}
	if overlay.S3.RetryMax > 0 {
		base.S3.RetryMax = overlay.S3.RetryMax
	}
	if overlay.S3.RetryBaseDelay > 0 {
		base.S3.RetryBaseDelay = overlay.S3.RetryBaseDelay
	}

	// Cache
	if overlay.Cache.MemoryLimit != "" {
		base.Cache.MemoryLimit = overlay.Cache.MemoryLimit
	}
	if overlay.Cache.DiskPath != "" {
		base.Cache.DiskPath = overlay.Cache.DiskPath
	}
	if overlay.Cache.DiskLimit != "" {
		base.Cache.DiskLimit = overlay.Cache.DiskLimit
	}
	if overlay.Cache.EvictionWatermark > 0 {
		base.Cache.EvictionWatermark = overlay.Cache.EvictionWatermark
	}
	if overlay.Cache.FooterTTL > 0 {
		base.Cache.FooterTTL = overlay.Cache.FooterTTL
	}
	if overlay.Cache.BloomTTL > 0 {
		base.Cache.BloomTTL = overlay.Cache.BloomTTL
	}
	if overlay.Cache.PageTTL > 0 {
		base.Cache.PageTTL = overlay.Cache.PageTTL
	}

	// Discovery
	if overlay.Discovery.HeadlessService != "" {
		base.Discovery.HeadlessService = overlay.Discovery.HeadlessService
	}
	if len(overlay.Discovery.StorageNodes) > 0 {
		base.Discovery.StorageNodes = overlay.Discovery.StorageNodes
	}
	if overlay.Discovery.PartitionAuthKey != "" {
		base.Discovery.PartitionAuthKey = overlay.Discovery.PartitionAuthKey
	}
	if overlay.Discovery.RefreshInterval > 0 {
		base.Discovery.RefreshInterval = overlay.Discovery.RefreshInterval
	}
	if overlay.Discovery.Timeout > 0 {
		base.Discovery.Timeout = overlay.Discovery.Timeout
	}
	if overlay.Discovery.PeerHeadlessService != "" {
		base.Discovery.PeerHeadlessService = overlay.Discovery.PeerHeadlessService
	}
	if overlay.Discovery.PeerRefreshInterval > 0 {
		base.Discovery.PeerRefreshInterval = overlay.Discovery.PeerRefreshInterval
	}

	// Manifest
	if overlay.Manifest.RefreshInterval > 0 {
		base.Manifest.RefreshInterval = overlay.Manifest.RefreshInterval
	}
	if overlay.Manifest.SQSQueueURL != "" {
		base.Manifest.SQSQueueURL = overlay.Manifest.SQSQueueURL
	}
	if overlay.Manifest.SQSRegion != "" {
		base.Manifest.SQSRegion = overlay.Manifest.SQSRegion
	}
	if overlay.Manifest.SQSWaitTime > 0 {
		base.Manifest.SQSWaitTime = overlay.Manifest.SQSWaitTime
	}
	if overlay.Manifest.PersistPath != "" {
		base.Manifest.PersistPath = overlay.Manifest.PersistPath
	}
	if overlay.Manifest.PersistInterval > 0 {
		base.Manifest.PersistInterval = overlay.Manifest.PersistInterval
	}

	// Prefetch
	if overlay.Prefetch.Correlated {
		base.Prefetch.Correlated = true
	}
	if overlay.Prefetch.ReadAheadDepth > 0 {
		base.Prefetch.ReadAheadDepth = overlay.Prefetch.ReadAheadDepth
	}
	if overlay.Prefetch.MaxConcurrent > 0 {
		base.Prefetch.MaxConcurrent = overlay.Prefetch.MaxConcurrent
	}
	if overlay.Prefetch.MaxQueue > 0 {
		base.Prefetch.MaxQueue = overlay.Prefetch.MaxQueue
	}

	// Peer
	if overlay.Peer.AuthKey != "" {
		base.Peer.AuthKey = overlay.Peer.AuthKey
	}
	if overlay.Peer.Timeout > 0 {
		base.Peer.Timeout = overlay.Peer.Timeout
	}
	if overlay.Peer.MaxConnections > 0 {
		base.Peer.MaxConnections = overlay.Peer.MaxConnections
	}

	// Startup
	if overlay.Startup.ServeStale {
		base.Startup.ServeStale = true
	}
	if overlay.Startup.WarmupWindow > 0 {
		base.Startup.WarmupWindow = overlay.Startup.WarmupWindow
	}
	if overlay.Startup.MaxWarmupTime > 0 {
		base.Startup.MaxWarmupTime = overlay.Startup.MaxWarmupTime
	}

	// Query
	if overlay.Query.MaxConcurrent > 0 {
		base.Query.MaxConcurrent = overlay.Query.MaxConcurrent
	}
	if overlay.Query.Timeout > 0 {
		base.Query.Timeout = overlay.Query.Timeout
	}
	if overlay.Query.MaxRows > 0 {
		base.Query.MaxRows = overlay.Query.MaxRows
	}
	if overlay.Query.SlowThreshold > 0 {
		base.Query.SlowThreshold = overlay.Query.SlowThreshold
	}

	// Circuit Breaker
	if overlay.CircuitBreaker.Threshold > 0 {
		base.CircuitBreaker.Threshold = overlay.CircuitBreaker.Threshold
	}
	if overlay.CircuitBreaker.Timeout > 0 {
		base.CircuitBreaker.Timeout = overlay.CircuitBreaker.Timeout
	}
	if overlay.CircuitBreaker.SuccessThreshold > 0 {
		base.CircuitBreaker.SuccessThreshold = overlay.CircuitBreaker.SuccessThreshold
	}

	// Tenant
	if overlay.Tenant.DefaultPrefix != "" {
		base.Tenant.DefaultPrefix = overlay.Tenant.DefaultPrefix
	}
	if overlay.Tenant.PrefixTemplate != "" {
		base.Tenant.PrefixTemplate = overlay.Tenant.PrefixTemplate
	}

	// HotBoundary
	if overlay.HotBoundary != "" {
		base.HotBoundary = overlay.HotBoundary
	}

	// Role
	if overlay.Role != "" {
		base.Role = overlay.Role
	}

	// Insert
	if overlay.Insert.FlushInterval > 0 {
		base.Insert.FlushInterval = overlay.Insert.FlushInterval
	}
	if overlay.Insert.MaxBufferRows > 0 {
		base.Insert.MaxBufferRows = overlay.Insert.MaxBufferRows
	}
	if overlay.Insert.MaxBufferBytes != "" {
		base.Insert.MaxBufferBytes = overlay.Insert.MaxBufferBytes
	}
	if overlay.Insert.RowGroupSize > 0 {
		base.Insert.RowGroupSize = overlay.Insert.RowGroupSize
	}
	if len(overlay.Insert.BloomColumns) > 0 {
		base.Insert.BloomColumns = overlay.Insert.BloomColumns
	}
	if overlay.Insert.CompressionLevel > 0 {
		base.Insert.CompressionLevel = overlay.Insert.CompressionLevel
	}
	if overlay.Insert.TargetFileSize != "" {
		base.Insert.TargetFileSize = overlay.Insert.TargetFileSize
	}
	if overlay.Insert.WALEnabled {
		base.Insert.WALEnabled = true
	}
	if overlay.Insert.WALDir != "" {
		base.Insert.WALDir = overlay.Insert.WALDir
	}
	if overlay.Insert.WALMaxBytes != "" {
		base.Insert.WALMaxBytes = overlay.Insert.WALMaxBytes
	}

	// Select
	if overlay.Select.BufferQueryEnabled {
		base.Select.BufferQueryEnabled = true
	}
	if overlay.Select.InsertHeadlessService != "" {
		base.Select.InsertHeadlessService = overlay.Select.InsertHeadlessService
	}
	if overlay.Select.BufferQueryTimeout > 0 {
		base.Select.BufferQueryTimeout = overlay.Select.BufferQueryTimeout
	}

	// Schema
	if len(overlay.Schema.ExtraPromoted) > 0 {
		base.Schema.ExtraPromoted = overlay.Schema.ExtraPromoted
	}

	// Compaction
	if overlay.Compaction.Enabled {
		base.Compaction.Enabled = true
	}
	if overlay.Compaction.Interval > 0 {
		base.Compaction.Interval = overlay.Compaction.Interval
	}
	if overlay.Compaction.MaxConcurrent > 0 {
		base.Compaction.MaxConcurrent = overlay.Compaction.MaxConcurrent
	}
	if overlay.Compaction.MinFilesL0 > 0 {
		base.Compaction.MinFilesL0 = overlay.Compaction.MinFilesL0
	}
	if overlay.Compaction.MinFilesL1 > 0 {
		base.Compaction.MinFilesL1 = overlay.Compaction.MinFilesL1
	}
	if overlay.Compaction.MinAge > 0 {
		base.Compaction.MinAge = overlay.Compaction.MinAge
	}
	if overlay.Compaction.LeaderElection != "" {
		base.Compaction.LeaderElection = overlay.Compaction.LeaderElection
	}
	if overlay.Compaction.LeaseDuration > 0 {
		base.Compaction.LeaseDuration = overlay.Compaction.LeaseDuration
	}
	if overlay.Compaction.S3LockTTL > 0 {
		base.Compaction.S3LockTTL = overlay.Compaction.S3LockTTL
	}
	if overlay.Compaction.S3Heartbeat > 0 {
		base.Compaction.S3Heartbeat = overlay.Compaction.S3Heartbeat
	}

	// Delete
	if overlay.Delete.Enabled {
		base.Delete.Enabled = true
	}
	if overlay.Delete.DefaultMode != "" {
		base.Delete.DefaultMode = overlay.Delete.DefaultMode
	}
	if len(overlay.Delete.AutoRewriteClasses) > 0 {
		base.Delete.AutoRewriteClasses = overlay.Delete.AutoRewriteClasses
	}
	if overlay.Delete.RewriteDelay > 0 {
		base.Delete.RewriteDelay = overlay.Delete.RewriteDelay
	}
	if overlay.Delete.RewriteBatchSize > 0 {
		base.Delete.RewriteBatchSize = overlay.Delete.RewriteBatchSize
	}
	if overlay.Delete.RewriteMaxConcurrent > 0 {
		base.Delete.RewriteMaxConcurrent = overlay.Delete.RewriteMaxConcurrent
	}
	if overlay.Delete.PersistPath != "" {
		base.Delete.PersistPath = overlay.Delete.PersistPath
	}
	if overlay.Delete.CostWarningThreshold > 0 {
		base.Delete.CostWarningThreshold = overlay.Delete.CostWarningThreshold
	}
	if overlay.Delete.ForceGlacierHeader != "" {
		base.Delete.ForceGlacierHeader = overlay.Delete.ForceGlacierHeader
	}
	if overlay.Delete.VerifyInterval > 0 {
		base.Delete.VerifyInterval = overlay.Delete.VerifyInterval
	}
	if len(overlay.Delete.LifecycleRules) > 0 {
		base.Delete.LifecycleRules = overlay.Delete.LifecycleRules
	}

	return base
}
