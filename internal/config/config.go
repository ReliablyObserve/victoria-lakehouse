package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
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
	Profile  Profile  `yaml:"profile"`

	S3          S3Config          `yaml:"s3"`
	Cache       CacheConfig       `yaml:"cache"`
	Discovery   DiscoveryConfig   `yaml:"discovery"`
	HotBoundary string            `yaml:"hot_boundary"`
	Manifest    ManifestConfig    `yaml:"manifest"`
	Prefetch    PrefetchConfig    `yaml:"prefetch"`
	Peer        PeerConfig        `yaml:"peer"`
	Startup     StartupConfig     `yaml:"startup"`
	Query       QueryConfig       `yaml:"query"`
	Insert      InsertConfig      `yaml:"insert"`
	Select      SelectConfig      `yaml:"select"`
	Schema      SchemaConfig      `yaml:"schema"`
	Tenant      TenantConfig      `yaml:"tenant"`
	Compaction  CompactionConfig  `yaml:"compaction"`
	Delete      DeleteConfig      `yaml:"delete"`
	GC          GCConfig          `yaml:"gc"`
	SmartCache  SmartCacheConfig  `yaml:"smart_cache"`
	CrossSignal CrossSignalConfig `yaml:"cross_signal"`
	Retention   RetentionConfig   `yaml:"retention"`
	Stats       StatsConfig       `yaml:"stats"`
	UI          UIConfig          `yaml:"ui"`
	Telemetry   TelemetryConfig   `yaml:"telemetry"`

	Logs   LogsModeConfig   `yaml:"logs"`
	Traces TracesModeConfig `yaml:"traces"`
}

type LogsModeConfig struct {
	BloomColumns  []string       `yaml:"bloom_columns"`
	DeletePrefix  string         `yaml:"delete_prefix"`
	CompatVersion string         `yaml:"compat_version"`
	Profile       Profile        `yaml:"profile"`
	Insert        RoleProfileRef `yaml:"insert"`
	Select        RoleProfileRef `yaml:"select"`
}

type TracesModeConfig struct {
	BloomColumns   []string       `yaml:"bloom_columns"`
	DeletePrefix   string         `yaml:"delete_prefix"`
	CompatVersion  string         `yaml:"compat_version"`
	JaegerEnabled  bool           `yaml:"jaeger_enabled"`
	JaegerGRPCAddr string         `yaml:"jaeger_grpc_addr"`
	Profile        Profile        `yaml:"profile"`
	Insert         RoleProfileRef `yaml:"insert"`
	Select         RoleProfileRef `yaml:"select"`
}

func (c *Config) ActiveBloomColumns() []string {
	if c.Mode == ModeTraces && len(c.Traces.BloomColumns) > 0 {
		return c.Traces.BloomColumns
	}
	if c.Mode == ModeLogs && len(c.Logs.BloomColumns) > 0 {
		return c.Logs.BloomColumns
	}
	return c.Insert.BloomColumns
}

func (c *Config) ActiveDeletePrefix() string {
	if c.Mode == ModeTraces && c.Traces.DeletePrefix != "" {
		return c.Traces.DeletePrefix
	}
	if c.Mode == ModeLogs && c.Logs.DeletePrefix != "" {
		return c.Logs.DeletePrefix
	}
	if c.Mode == ModeTraces {
		return "/delete/tracessql"
	}
	return "/delete/logsql"
}

func (c *Config) ActiveCompatVersion() string {
	if c.Mode == ModeTraces && c.Traces.CompatVersion != "" {
		return c.Traces.CompatVersion
	}
	if c.Mode == ModeLogs && c.Logs.CompatVersion != "" {
		return c.Logs.CompatVersion
	}
	return ""
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

	AckMode              string        `yaml:"ack_mode"`
	FlushLinger          time.Duration `yaml:"flush_linger"`
	FlushMaxRows         int           `yaml:"flush_max_rows"`
	PeerReplicate        bool          `yaml:"peer_replicate"`
	PeerReplicateTimeout time.Duration `yaml:"peer_replicate_timeout"`
	PeerReplicateTTL     time.Duration `yaml:"peer_replicate_ttl"`
	AsyncWALEnabled      bool          `yaml:"async_wal_enabled"`
	AsyncWALBatchLinger  time.Duration `yaml:"async_wal_batch_linger"`
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

type GCConfig struct {
	Enabled           bool          `yaml:"enabled"`
	Interval          time.Duration `yaml:"interval"`
	OrphanGracePeriod time.Duration `yaml:"orphan_grace_period"`
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
	AZAware               bool          `yaml:"az_aware"`
	CrossAZFallback       bool          `yaml:"cross_az_fallback"`
}

type S3Config struct {
	Bucket                 string        `yaml:"bucket"`
	Region                 string        `yaml:"region"`
	Prefix                 string        `yaml:"prefix"`
	Endpoint               string        `yaml:"endpoint"`
	AccessKey              string        `yaml:"access_key"`
	SecretKey              string        `yaml:"secret_key"`
	ForcePathStyle         bool          `yaml:"force_path_style"`
	MaxConnections         int           `yaml:"max_connections"`
	Timeout                time.Duration `yaml:"timeout"`
	RetryMax               int           `yaml:"retry_max"`
	RetryBaseDelay         time.Duration `yaml:"retry_base_delay"`
	MaxConcurrentDownloads int           `yaml:"max_concurrent_downloads"`
	ReadAheadBytes         int           `yaml:"read_ahead_bytes"`
	CoalesceGapBytes       int           `yaml:"coalesce_gap_bytes"`
}

type CacheConfig struct {
	MemoryLimit       string        `yaml:"memory_limit"`
	DiskPath          string        `yaml:"disk_path"`
	DiskLimit         string        `yaml:"disk_limit"`
	EvictionWatermark float64       `yaml:"eviction_watermark"`
	FooterTTL         time.Duration `yaml:"footer_ttl"`
	BloomTTL          time.Duration `yaml:"bloom_ttl"`
	PageTTL           time.Duration `yaml:"page_ttl"`
	WarmupPartitions  int           `yaml:"warmup_partitions"`
	WarmupMaxFiles    int           `yaml:"warmup_max_files"`
	WarmupConcurrency int           `yaml:"warmup_concurrency"`
	PartitionMode     string        `yaml:"partition_mode"` // "az-local" (default), "global", "distributed"
}

type DiscoveryConfig struct {
	HeadlessService     string        `yaml:"headless_service"`
	StorageNodes        []string      `yaml:"storage_nodes"`
	PartitionAuthKey    string        `yaml:"partition_auth_key"`
	RefreshInterval     time.Duration `yaml:"refresh_interval"`
	Timeout             time.Duration `yaml:"timeout"`
	PeerHeadlessService string        `yaml:"peer_headless_service"`
	PeerRefreshInterval time.Duration `yaml:"peer_refresh_interval"`
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
	Correlated     bool `yaml:"correlated"`
	ReadAheadDepth int  `yaml:"read_ahead_depth"`
	MaxConcurrent  int  `yaml:"max_concurrent"`
	MaxQueue       int  `yaml:"max_queue"`
}

type PeerConfig struct {
	AuthKey         string        `yaml:"auth_key"`
	Timeout         time.Duration `yaml:"timeout"`
	MaxConnections  int           `yaml:"max_connections"`
	AZAware         bool          `yaml:"az_aware"`
	AZMode          string        `yaml:"az_mode"`
	CrossAZFallback bool          `yaml:"cross_az_fallback"`
	AZEnvVar        string        `yaml:"az_env_var"`
	AZMinPeersPerAZ int           `yaml:"az_min_peers_per_az"`
}

type StartupConfig struct {
	ServeStale    bool          `yaml:"serve_stale"`
	WarmupWindow  time.Duration `yaml:"warmup_window"`
	MaxWarmupTime time.Duration `yaml:"max_warmup_time"`
}

type QueryConfig struct {
	MaxConcurrent    int           `yaml:"max_concurrent"`
	FileWorkers      int           `yaml:"file_workers"`
	Timeout          time.Duration `yaml:"timeout"`
	MaxRows          int64         `yaml:"max_rows"`
	MaxFilesPerQuery int           `yaml:"max_files_per_query"`
	SlowThreshold    time.Duration `yaml:"slow_threshold"`
}

type TenantConfig struct {
	DefaultPrefix     string                 `yaml:"default_prefix"`
	PrefixTemplate    string                 `yaml:"prefix_template"`
	Isolation         string                 `yaml:"isolation"`
	BucketTemplate    string                 `yaml:"bucket_template"`
	DefaultAccount    string                 `yaml:"default_account"`
	DefaultProject    string                 `yaml:"default_project"`
	HeaderAccount     string                 `yaml:"header_account"`
	HeaderProject     string                 `yaml:"header_project"`
	GlobalReadHeader  string                 `yaml:"global_read_header"`
	GlobalReadValue   string                 `yaml:"global_read_value"`
	GlobalReadToken   string                 `yaml:"global_read_token"`
	KnownTenants      []KnownTenant          `yaml:"known_tenants"`
	OrgIDHeader       string                 `yaml:"orgid_header"`
	MetricsFormat     string                 `yaml:"metrics_format"`
	AutoRegister      bool                   `yaml:"auto_register"`
	AliasSyncInterval time.Duration          `yaml:"alias_sync_interval"`
	Aliases           map[string]AliasTarget `yaml:"aliases"`
}

type AliasTarget struct {
	AccountID uint32 `yaml:"account_id"`
	ProjectID uint32 `yaml:"project_id"`
}

type KnownTenant struct {
	AccountID      string                `yaml:"account_id"`
	ProjectID      string                `yaml:"project_id"`
	LifecycleRules []LifecycleRuleConfig `yaml:"lifecycle_rules"`
	PricePerGB     map[string]float64    `yaml:"price_per_gb"`
}

type StatsConfig struct {
	Enabled                     bool                  `yaml:"enabled"`
	PushInterval                time.Duration         `yaml:"push_interval"`
	PushCompression             bool                  `yaml:"push_compression"`
	SnapshotInterval            time.Duration         `yaml:"snapshot_interval"`
	SnapshotPrefix              string                `yaml:"snapshot_prefix"`
	MetaBucket                  string                `yaml:"meta_bucket"`
	MaxDeltaCount               int                   `yaml:"max_delta_count"`
	MetricsCardinalityLimit     int                   `yaml:"metrics_cardinality_limit"`
	CardinalityWarningThreshold int                   `yaml:"cardinality_warning_threshold"`
	BreakdownLabels             []string              `yaml:"breakdown_labels"`
	S3LifecycleRules            []LifecycleRuleConfig `yaml:"s3_lifecycle_rules"`
	S3PricePerGB                map[string]float64    `yaml:"s3_price_per_gb"`
	S3RequestPrices             map[string]float64    `yaml:"s3_request_prices"`
	S3InventoryBucket           string                `yaml:"s3_inventory_bucket"`
	HeadObjectSampleInterval    time.Duration         `yaml:"headobject_sample_interval"`
	HeadObjectMaxPerRefresh     int                   `yaml:"headobject_max_per_refresh"`
}

type UIConfig struct {
	Enabled        bool   `yaml:"enabled"`
	VMUITab        bool   `yaml:"vmui_tab"`
	RefreshDefault int    `yaml:"refresh_default"`
	Theme          string `yaml:"theme"`
}

func (t TenantConfig) ResolvedPrefix() string {
	if t.DefaultPrefix != "" {
		return t.DefaultPrefix
	}
	if t.PrefixTemplate == "" || (t.DefaultAccount == "" && t.DefaultProject == "") {
		return ""
	}
	r := strings.NewReplacer("{AccountID}", t.DefaultAccount, "{ProjectID}", t.DefaultProject)
	return r.Replace(t.PrefixTemplate)
}

type CompactionConfig struct {
	Enabled        bool          `yaml:"enabled"`
	Interval       time.Duration `yaml:"interval"`
	MaxConcurrent  int           `yaml:"max_concurrent"`
	MinFilesL0     int           `yaml:"min_files_l0"`
	MinFilesL1     int           `yaml:"min_files_l1"`
	MinAge         time.Duration `yaml:"min_age"`
	DailyRollupAge time.Duration `yaml:"daily_rollup_age"`
	LeaderElection string        `yaml:"leader_election"`
	LeaseDuration  time.Duration `yaml:"lease_duration"`
	S3LockTTL      time.Duration `yaml:"s3_lock_ttl"`
	S3Heartbeat    time.Duration `yaml:"s3_heartbeat"`
	ShardID        int           `yaml:"shard_id"`
	ShardCount     int           `yaml:"shard_count"`
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

type SmartCacheConfig struct {
	MaxAge             time.Duration `yaml:"max_age"`
	SnapshotInterval   time.Duration `yaml:"snapshot_interval"`
	QueryGracePeriod   time.Duration `yaml:"query_grace_period"`
	HotAccessThreshold int           `yaml:"hot_access_threshold"`
	HotWindow          time.Duration `yaml:"hot_window"`
	TargetHours        int           `yaml:"target_hours"`
	DiskLimitMax       string        `yaml:"disk_limit_max"`
	IngestionRateHint  string        `yaml:"ingestion_rate_hint"`
}

type CrossSignalConfig struct {
	Enabled         bool          `yaml:"enabled"`
	Endpoint        string        `yaml:"endpoint"`
	HeadlessService string        `yaml:"headless_service"`
	AuthKey         string        `yaml:"auth_key"`
	Timeout         time.Duration `yaml:"timeout"`
	MaxBatch        int           `yaml:"max_batch"`
	BatchInterval   time.Duration `yaml:"batch_interval"`
}

type RetentionConfig struct {
	Enabled       bool            `yaml:"enabled"`
	Default       string          `yaml:"default"`
	CheckInterval string          `yaml:"check_interval"`
	Rules         []RetentionRule `yaml:"rules"`
}

type RetentionRule struct {
	Match map[string]string `yaml:"match"`
	Keep  string            `yaml:"keep"`
}

type ExtraPromotedColumn struct {
	Name  string `yaml:"name"`
	Type  string `yaml:"type"`
	Bloom bool   `yaml:"bloom"`
}

type SchemaConfig struct {
	ExtraPromoted []ExtraPromotedColumn `yaml:"extra_promoted"`
}

type RoleProfileRef struct {
	Profile Profile `yaml:"profile"`
}

func Default() *Config {
	return &Config{
		Role:     RoleAll,
		Topology: TopologyAuto,

		S3: S3Config{
			Region:                 "us-east-1",
			MaxConnections:         128,
			Timeout:                30 * time.Second,
			RetryMax:               3,
			RetryBaseDelay:         200 * time.Millisecond,
			MaxConcurrentDownloads: 16,
			ReadAheadBytes:         2 * 1024 * 1024, // 2MB
			CoalesceGapBytes:       64 * 1024,       // 64KB
		},

		Cache: CacheConfig{
			MemoryLimit:       "512MB",
			DiskPath:          "/data/lakehouse/cache",
			DiskLimit:         "50GB",
			EvictionWatermark: 0.8,
			FooterTTL:         1 * time.Hour,
			BloomTTL:          1 * time.Hour,
			PageTTL:           10 * time.Minute,
			PartitionMode:     "az-local",
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
			MaxConcurrent:  8,
			MaxQueue:       128,
		},

		Peer: PeerConfig{
			Timeout:         5 * time.Second,
			MaxConnections:  32,
			AZAware:         true,
			AZMode:          "preferred",
			CrossAZFallback: true,
			AZEnvVar:        "LAKEHOUSE_AZ",
			AZMinPeersPerAZ: 2,
		},

		Startup: StartupConfig{
			ServeStale:    false,
			WarmupWindow:  24 * time.Hour,
			MaxWarmupTime: 5 * time.Minute,
		},

		Query: QueryConfig{
			MaxConcurrent:    32,
			FileWorkers:      64,
			Timeout:          60 * time.Second,
			MaxRows:          10_000_000,
			MaxFilesPerQuery: 500,
			SlowThreshold:    5 * time.Second,
		},

		Insert: InsertConfig{
			FlushInterval:    60 * time.Second,
			MaxBufferRows:    50000,
			MaxBufferBytes:   "256MB",
			TargetFileSize:   "128MB",
			RowGroupSize:     10000,
			BloomColumns:     []string{"service.name", "trace_id"},
			CompressionLevel: 7,
			WALEnabled:       true,
			WALDir:           "/data/lakehouse/wal",
			WALMaxBytes:      "512MB",

			AckMode:              "buffer",
			FlushLinger:          200 * time.Millisecond,
			FlushMaxRows:         5000,
			PeerReplicate:        false,
			PeerReplicateTimeout: 5 * time.Millisecond,
			PeerReplicateTTL:     30 * time.Second,
			AsyncWALEnabled:      false,
			AsyncWALBatchLinger:  50 * time.Millisecond,
		},

		Select: SelectConfig{
			BufferQueryEnabled: true,
			BufferQueryTimeout: 2 * time.Second,
			AZAware:            true,
			CrossAZFallback:    true,
		},

		Tenant: TenantConfig{
			PrefixTemplate:    "{AccountID}/{ProjectID}/",
			Isolation:         "prefix",
			DefaultAccount:    "0",
			DefaultProject:    "0",
			HeaderAccount:     "X-Scope-AccountID",
			HeaderProject:     "X-Scope-ProjectID",
			OrgIDHeader:       "X-Scope-OrgID",
			MetricsFormat:     "id",
			AutoRegister:      false,
			AliasSyncInterval: 30 * time.Second,
		},

		Compaction: CompactionConfig{
			Enabled:        true,
			Interval:       5 * time.Minute,
			MaxConcurrent:  1,
			MinFilesL0:     10,
			MinFilesL1:     10,
			MinAge:         1 * time.Hour,
			DailyRollupAge: 24 * time.Hour,
			LeaderElection: "auto",
			LeaseDuration:  15 * time.Second,
			S3LockTTL:      60 * time.Second,
			S3Heartbeat:    15 * time.Second,
			ShardID:        -1,
			ShardCount:     1,
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

		GC: GCConfig{
			Enabled:           true,
			Interval:          6 * time.Hour,
			OrphanGracePeriod: 1 * time.Hour,
		},

		SmartCache: SmartCacheConfig{
			MaxAge:             24 * time.Hour,
			SnapshotInterval:   60 * time.Second,
			QueryGracePeriod:   5 * time.Minute,
			HotAccessThreshold: 3,
			HotWindow:          10 * time.Minute,
			TargetHours:        24,
			DiskLimitMax:       "100GB",
		},

		CrossSignal: CrossSignalConfig{
			Enabled:       false,
			Timeout:       2 * time.Second,
			MaxBatch:      100,
			BatchInterval: 500 * time.Millisecond,
		},

		Retention: RetentionConfig{
			Enabled:       false,
			Default:       "90d",
			CheckInterval: "1h",
		},

		Stats: StatsConfig{
			Enabled:                     true,
			PushInterval:                30 * time.Second,
			PushCompression:             true,
			SnapshotInterval:            5 * time.Minute,
			SnapshotPrefix:              "_meta/tenant-stats",
			MaxDeltaCount:               1000,
			MetricsCardinalityLimit:     100,
			CardinalityWarningThreshold: 10000,
			BreakdownLabels:             []string{"service.name", "deployment.environment", "k8s.namespace.name", "k8s.cluster.name"},
			S3PricePerGB: map[string]float64{
				"STANDARD":     0.023,
				"STANDARD_IA":  0.0125,
				"GLACIER_IR":   0.004,
				"GLACIER":      0.0036,
				"DEEP_ARCHIVE": 0.00099,
			},
			S3RequestPrices: map[string]float64{
				"PUT":  0.005,
				"GET":  0.0004,
				"LIST": 0.005,
			},
			HeadObjectSampleInterval: 6 * time.Hour,
			HeadObjectMaxPerRefresh:  50,
		},

		UI: UIConfig{
			Enabled:        true,
			VMUITab:        true,
			RefreshDefault: 0,
			Theme:          "auto",
		},

		Telemetry: TelemetryConfig{
			Enabled:          false,
			SampleRate:       0.1,
			AlwaysSampleSlow: true,
			BatchTimeout:     5 * time.Second,
		},

		Logs: LogsModeConfig{
			BloomColumns:  []string{"service.name", "trace_id"},
			DeletePrefix:  "/delete/logsql",
			CompatVersion: "",
		},

		Traces: TracesModeConfig{
			BloomColumns:   []string{"trace_id", "service.name"},
			DeletePrefix:   "/delete/tracessql",
			CompatVersion:  "",
			JaegerEnabled:  true,
			JaegerGRPCAddr: ":16685",
		},
	}
}

func Load(path string) (*Config, error) {
	return LoadWithMode(path, "", "")
}

func LoadWithMode(path string, mode Mode, role Role) (*Config, error) {
	if path == "" {
		cfg := Default()
		if mode != "" {
			cfg.Mode = mode
		}
		if role != "" {
			cfg.Role = role
		}
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

	fileConfig := &wrapper.Lakehouse

	if mode != "" {
		fileConfig.Mode = mode
	}
	if role != "" {
		fileConfig.Role = role
	}

	profile := fileConfig.ResolveEffectiveProfile()

	base := ProfileConfig(profile)
	merged := mergeConfig(base, fileConfig)
	merged.Profile = profile

	return merged, nil
}

func MergeConfigs(base, overlay *Config) *Config {
	return mergeConfig(base, overlay)
}

func (c *Config) Validate() error {
	if c.Profile != "" && !IsValidProfile(string(c.Profile)) {
		return fmt.Errorf("--lakehouse.profile must be one of: %s; got %q", ValidProfileNames(), c.Profile)
	}
	if c.Mode == "" {
		return fmt.Errorf("mode is required (logs or traces)")
	}
	if c.Mode != ModeLogs && c.Mode != ModeTraces {
		return fmt.Errorf("mode must be 'logs' or 'traces', got %q", c.Mode)
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

	switch c.Profile {
	case ProfileBalanced, ProfileMaxPerformance, ProfileMaxDurability,
		ProfileMaxCostSavings, ProfileDev, "":
	default:
		return fmt.Errorf("--lakehouse.profile must be one of: balanced, max-performance, max-durability, max-cost-savings, dev; got %q", c.Profile)
	}

	if c.InsertEnabled() {
		if err := c.validateInsert(); err != nil {
			return err
		}
	}

	if err := c.validateEnums(); err != nil {
		return err
	}

	if err := c.validateSubsystems(); err != nil {
		return err
	}

	return nil
}

func (c *Config) validateInsert() error {
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
	if c.Insert.MaxBufferBytes != "" {
		if _, err := ParseSizeBytes(c.Insert.MaxBufferBytes); err != nil {
			return fmt.Errorf("--lakehouse.insert.max-buffer-bytes: invalid size %q: %w", c.Insert.MaxBufferBytes, err)
		}
	}
	if _, err := ParseSizeBytes(c.Insert.TargetFileSize); err != nil {
		return fmt.Errorf("--lakehouse.insert.target-file-size: invalid size %q: %w", c.Insert.TargetFileSize, err)
	}
	if c.Insert.WALMaxBytes != "" {
		if _, err := ParseSizeBytes(c.Insert.WALMaxBytes); err != nil {
			return fmt.Errorf("--lakehouse.insert.wal-max-bytes: invalid size %q: %w", c.Insert.WALMaxBytes, err)
		}
	}
	switch c.Insert.AckMode {
	case "buffer", "wal", "flush-sync":
	default:
		return fmt.Errorf("--lakehouse.insert.ack-mode must be one of: buffer, wal, flush-sync; got %q", c.Insert.AckMode)
	}
	if c.Insert.FlushLinger < 0 {
		return fmt.Errorf("--lakehouse.insert.flush-linger must be non-negative")
	}
	if c.Insert.FlushMaxRows < 0 {
		return fmt.Errorf("--lakehouse.insert.flush-max-rows must be non-negative")
	}
	if c.Insert.PeerReplicateTTL < 0 {
		return fmt.Errorf("--lakehouse.insert.peer-replicate-ttl must be non-negative")
	}
	return nil
}

func (c *Config) validateEnums() error {
	switch c.Peer.AZMode {
	case "preferred", "strict", "":
	default:
		return fmt.Errorf("--lakehouse.peer.az-mode must be preferred or strict, got %q", c.Peer.AZMode)
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

	switch c.Compaction.LeaderElection {
	case "auto", "k8s", "s3", "none", "":
	default:
		return fmt.Errorf("--lakehouse.compaction.leader-election must be one of: auto, k8s, s3, none; got %q", c.Compaction.LeaderElection)
	}

	switch c.UI.Theme {
	case "auto", "dark", "light", "":
	default:
		return fmt.Errorf("--lakehouse.ui.theme must be auto, dark, or light; got %q", c.UI.Theme)
	}

	return nil
}

func (c *Config) validateSubsystems() error {
	if c.Cache.EvictionWatermark <= 0 || c.Cache.EvictionWatermark > 1 {
		return fmt.Errorf("--lakehouse.cache.eviction-watermark must be in (0, 1], got %f", c.Cache.EvictionWatermark)
	}
	switch c.Cache.PartitionMode {
	case "az-local", "global", "distributed", "":
	default:
		return fmt.Errorf("--lakehouse.cache.partition-mode must be one of: az-local, global, distributed; got %q", c.Cache.PartitionMode)
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

	if c.GC.Enabled {
		if c.GC.Interval <= 0 {
			return fmt.Errorf("--lakehouse.gc.interval must be positive")
		}
		if c.GC.OrphanGracePeriod <= 0 {
			return fmt.Errorf("--lakehouse.gc.orphan-grace-period must be positive")
		}
	}

	if c.Stats.Enabled {
		if c.Stats.PushInterval <= 0 {
			return fmt.Errorf("--lakehouse.stats.push-interval must be positive")
		}
		if c.Stats.MetricsCardinalityLimit < 0 {
			return fmt.Errorf("--lakehouse.stats.metrics-cardinality-limit must be >= 0")
		}
		if c.Stats.HeadObjectMaxPerRefresh < 0 {
			return fmt.Errorf("--lakehouse.stats.headobject-max-per-refresh must be >= 0")
		}
	}
	if c.Tenant.Isolation == "bucket" && c.Tenant.BucketTemplate == "" {
		return fmt.Errorf("--lakehouse.tenant.bucket-template is required when isolation=bucket")
	}

	if c.HotBoundary != "" {
		if err := validateDuration(c.HotBoundary); err != nil {
			return fmt.Errorf("--lakehouse.hot-boundary: invalid duration %q: %w", c.HotBoundary, err)
		}
	}

	if c.Compaction.Enabled && c.Compaction.LeaderElection == "none" {
		return fmt.Errorf("--lakehouse.compaction.leader-election must not be \"none\" when compaction is enabled; concurrent compactors may corrupt data")
	}

	return nil
}

func ParseSizeBytes(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	s = strings.TrimSpace(s)
	// flagutil.ParseBytes treats KB/MB/GB/TB as SI (decimal) units.
	// Lakehouse historically uses binary (1024-based) semantics for those
	// suffixes, so rewrite them to KiB/MiB/GiB/TiB before delegating.
	upper := strings.ToUpper(s)
	switch {
	case strings.HasSuffix(upper, "TB"):
		s = s[:len(s)-2] + "TiB"
	case strings.HasSuffix(upper, "GB"):
		s = s[:len(s)-2] + "GiB"
	case strings.HasSuffix(upper, "MB"):
		s = s[:len(s)-2] + "MiB"
	case strings.HasSuffix(upper, "KB"):
		s = s[:len(s)-2] + "KiB"
	case strings.HasSuffix(upper, "B"):
		// Plain "B" suffix (e.g. "100B") — strip it so flagutil
		// parses the bare number as bytes.
		s = s[:len(s)-1]
	}
	return flagutil.ParseBytes(s)
}

// validateDuration checks that s parses as a Go duration or as a
// VictoriaMetrics-style extended duration (e.g. "7d", "14d", "30d").
func validateDuration(s string) error {
	if s == "" {
		return fmt.Errorf("empty duration")
	}
	// Try standard Go duration first (supports ns, us, ms, s, m, h).
	if _, err := time.ParseDuration(s); err == nil {
		return nil
	}
	// Support day suffix: a positive integer followed by "d".
	trimmed := strings.TrimSpace(s)
	if strings.HasSuffix(trimmed, "d") {
		numPart := trimmed[:len(trimmed)-1]
		if numPart == "" {
			return fmt.Errorf("missing numeric value before 'd'")
		}
		for _, ch := range numPart {
			if ch < '0' || ch > '9' {
				return fmt.Errorf("invalid duration %q: not a valid number before 'd'", s)
			}
		}
		return nil
	}
	return fmt.Errorf("cannot parse %q as duration (supported: Go durations or Nd for days)", s)
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

func (c *Config) DefaultPort() string {
	if c.Mode == ModeTraces {
		return "10428"
	}
	return "9428"
}

func (c *Config) AutoPrefix() string {
	if c.S3.Prefix != "" {
		return c.S3.Prefix
	}

	signal := "logs/"
	if c.Mode == ModeTraces {
		signal = "traces/"
	}

	tp := c.Tenant.ResolvedPrefix()
	if tp != "" {
		return tp + signal
	}
	return signal
}

func mergeConfig(base, overlay *Config) *Config { //nolint:gocyclo // field-by-field merge is inherently high complexity
	if overlay.Mode != "" {
		base.Mode = overlay.Mode
	}
	if overlay.Topology != "" {
		base.Topology = overlay.Topology
	}
	if overlay.Profile != "" {
		base.Profile = overlay.Profile
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
	if overlay.S3.MaxConcurrentDownloads > 0 {
		base.S3.MaxConcurrentDownloads = overlay.S3.MaxConcurrentDownloads
	}
	if overlay.S3.ReadAheadBytes > 0 {
		base.S3.ReadAheadBytes = overlay.S3.ReadAheadBytes
	}
	if overlay.S3.CoalesceGapBytes > 0 {
		base.S3.CoalesceGapBytes = overlay.S3.CoalesceGapBytes
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
	if overlay.Cache.PartitionMode != "" {
		base.Cache.PartitionMode = overlay.Cache.PartitionMode
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
	if overlay.Peer.AZEnvVar != "" {
		base.Peer.AZEnvVar = overlay.Peer.AZEnvVar
	}
	if overlay.Peer.AZMode != "" {
		base.Peer.AZMode = overlay.Peer.AZMode
	}
	if overlay.Peer.AZMinPeersPerAZ > 0 {
		base.Peer.AZMinPeersPerAZ = overlay.Peer.AZMinPeersPerAZ
	}
	if overlay.Peer.AZAware {
		base.Peer.AZAware = true
	}
	if overlay.Peer.CrossAZFallback {
		base.Peer.CrossAZFallback = true
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
	if overlay.Query.FileWorkers > 0 {
		base.Query.FileWorkers = overlay.Query.FileWorkers
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

	// Tenant
	if overlay.Tenant.DefaultPrefix != "" {
		base.Tenant.DefaultPrefix = overlay.Tenant.DefaultPrefix
	}
	if overlay.Tenant.PrefixTemplate != "" {
		base.Tenant.PrefixTemplate = overlay.Tenant.PrefixTemplate
	}
	if overlay.Tenant.Isolation != "" {
		base.Tenant.Isolation = overlay.Tenant.Isolation
	}
	if overlay.Tenant.BucketTemplate != "" {
		base.Tenant.BucketTemplate = overlay.Tenant.BucketTemplate
	}
	if overlay.Tenant.DefaultAccount != "" {
		base.Tenant.DefaultAccount = overlay.Tenant.DefaultAccount
	}
	if overlay.Tenant.DefaultProject != "" {
		base.Tenant.DefaultProject = overlay.Tenant.DefaultProject
	}
	if overlay.Tenant.HeaderAccount != "" {
		base.Tenant.HeaderAccount = overlay.Tenant.HeaderAccount
	}
	if overlay.Tenant.HeaderProject != "" {
		base.Tenant.HeaderProject = overlay.Tenant.HeaderProject
	}
	if overlay.Tenant.GlobalReadHeader != "" {
		base.Tenant.GlobalReadHeader = overlay.Tenant.GlobalReadHeader
	}
	if overlay.Tenant.GlobalReadValue != "" {
		base.Tenant.GlobalReadValue = overlay.Tenant.GlobalReadValue
	}
	if overlay.Tenant.GlobalReadToken != "" {
		base.Tenant.GlobalReadToken = overlay.Tenant.GlobalReadToken
	}
	if len(overlay.Tenant.KnownTenants) > 0 {
		base.Tenant.KnownTenants = overlay.Tenant.KnownTenants
	}

	// Stats
	if overlay.Stats.Enabled {
		base.Stats.Enabled = true
	}
	if overlay.Stats.PushInterval > 0 {
		base.Stats.PushInterval = overlay.Stats.PushInterval
	}
	if overlay.Stats.PushCompression {
		base.Stats.PushCompression = true
	}
	if overlay.Stats.SnapshotInterval > 0 {
		base.Stats.SnapshotInterval = overlay.Stats.SnapshotInterval
	}
	if overlay.Stats.SnapshotPrefix != "" {
		base.Stats.SnapshotPrefix = overlay.Stats.SnapshotPrefix
	}
	if overlay.Stats.MetaBucket != "" {
		base.Stats.MetaBucket = overlay.Stats.MetaBucket
	}
	if overlay.Stats.MaxDeltaCount > 0 {
		base.Stats.MaxDeltaCount = overlay.Stats.MaxDeltaCount
	}
	if overlay.Stats.MetricsCardinalityLimit > 0 {
		base.Stats.MetricsCardinalityLimit = overlay.Stats.MetricsCardinalityLimit
	}
	if overlay.Stats.CardinalityWarningThreshold > 0 {
		base.Stats.CardinalityWarningThreshold = overlay.Stats.CardinalityWarningThreshold
	}
	if len(overlay.Stats.S3LifecycleRules) > 0 {
		base.Stats.S3LifecycleRules = overlay.Stats.S3LifecycleRules
	}
	if len(overlay.Stats.S3PricePerGB) > 0 {
		base.Stats.S3PricePerGB = overlay.Stats.S3PricePerGB
	}
	if len(overlay.Stats.S3RequestPrices) > 0 {
		base.Stats.S3RequestPrices = overlay.Stats.S3RequestPrices
	}
	if overlay.Stats.S3InventoryBucket != "" {
		base.Stats.S3InventoryBucket = overlay.Stats.S3InventoryBucket
	}
	if overlay.Stats.HeadObjectSampleInterval > 0 {
		base.Stats.HeadObjectSampleInterval = overlay.Stats.HeadObjectSampleInterval
	}
	if overlay.Stats.HeadObjectMaxPerRefresh > 0 {
		base.Stats.HeadObjectMaxPerRefresh = overlay.Stats.HeadObjectMaxPerRefresh
	}

	// UI
	if overlay.UI.Enabled {
		base.UI.Enabled = true
	}
	if overlay.UI.VMUITab {
		base.UI.VMUITab = true
	}
	if overlay.UI.RefreshDefault > 0 {
		base.UI.RefreshDefault = overlay.UI.RefreshDefault
	}
	if overlay.UI.Theme != "" {
		base.UI.Theme = overlay.UI.Theme
	}

	// HotBoundary
	if overlay.HotBoundary != "" {
		base.HotBoundary = overlay.HotBoundary
	}

	// Role
	if overlay.Role != "" {
		base.Role = overlay.Role
	}

	// Profile
	if overlay.Profile != "" {
		base.Profile = overlay.Profile
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
	if overlay.Insert.AckMode != "" {
		base.Insert.AckMode = overlay.Insert.AckMode
	}
	if overlay.Insert.FlushLinger > 0 {
		base.Insert.FlushLinger = overlay.Insert.FlushLinger
	}
	if overlay.Insert.FlushMaxRows > 0 {
		base.Insert.FlushMaxRows = overlay.Insert.FlushMaxRows
	}
	if overlay.Insert.PeerReplicate {
		base.Insert.PeerReplicate = true
	}
	if overlay.Insert.PeerReplicateTimeout > 0 {
		base.Insert.PeerReplicateTimeout = overlay.Insert.PeerReplicateTimeout
	}
	if overlay.Insert.PeerReplicateTTL > 0 {
		base.Insert.PeerReplicateTTL = overlay.Insert.PeerReplicateTTL
	}
	if overlay.Insert.AsyncWALEnabled {
		base.Insert.AsyncWALEnabled = true
	}
	if overlay.Insert.AsyncWALBatchLinger > 0 {
		base.Insert.AsyncWALBatchLinger = overlay.Insert.AsyncWALBatchLinger
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
	if overlay.Select.AZAware {
		base.Select.AZAware = true
	}
	if overlay.Select.CrossAZFallback {
		base.Select.CrossAZFallback = true
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
	if overlay.Compaction.DailyRollupAge > 0 {
		base.Compaction.DailyRollupAge = overlay.Compaction.DailyRollupAge
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

	// SmartCache
	if overlay.SmartCache.MaxAge > 0 {
		base.SmartCache.MaxAge = overlay.SmartCache.MaxAge
	}
	if overlay.SmartCache.SnapshotInterval > 0 {
		base.SmartCache.SnapshotInterval = overlay.SmartCache.SnapshotInterval
	}
	if overlay.SmartCache.QueryGracePeriod > 0 {
		base.SmartCache.QueryGracePeriod = overlay.SmartCache.QueryGracePeriod
	}
	if overlay.SmartCache.HotAccessThreshold > 0 {
		base.SmartCache.HotAccessThreshold = overlay.SmartCache.HotAccessThreshold
	}
	if overlay.SmartCache.HotWindow > 0 {
		base.SmartCache.HotWindow = overlay.SmartCache.HotWindow
	}
	if overlay.SmartCache.TargetHours > 0 {
		base.SmartCache.TargetHours = overlay.SmartCache.TargetHours
	}
	if overlay.SmartCache.DiskLimitMax != "" {
		base.SmartCache.DiskLimitMax = overlay.SmartCache.DiskLimitMax
	}
	if overlay.SmartCache.IngestionRateHint != "" {
		base.SmartCache.IngestionRateHint = overlay.SmartCache.IngestionRateHint
	}

	// CrossSignal
	if overlay.CrossSignal.Enabled {
		base.CrossSignal.Enabled = true
	}
	if overlay.CrossSignal.Endpoint != "" {
		base.CrossSignal.Endpoint = overlay.CrossSignal.Endpoint
	}
	if overlay.CrossSignal.HeadlessService != "" {
		base.CrossSignal.HeadlessService = overlay.CrossSignal.HeadlessService
	}
	if overlay.CrossSignal.AuthKey != "" {
		base.CrossSignal.AuthKey = overlay.CrossSignal.AuthKey
	}
	if overlay.CrossSignal.Timeout > 0 {
		base.CrossSignal.Timeout = overlay.CrossSignal.Timeout
	}
	if overlay.CrossSignal.MaxBatch > 0 {
		base.CrossSignal.MaxBatch = overlay.CrossSignal.MaxBatch
	}
	if overlay.CrossSignal.BatchInterval > 0 {
		base.CrossSignal.BatchInterval = overlay.CrossSignal.BatchInterval
	}

	// Retention
	if overlay.Retention.Enabled {
		base.Retention.Enabled = true
	}
	if overlay.Retention.Default != "" {
		base.Retention.Default = overlay.Retention.Default
	}
	if overlay.Retention.CheckInterval != "" {
		base.Retention.CheckInterval = overlay.Retention.CheckInterval
	}
	if len(overlay.Retention.Rules) > 0 {
		base.Retention.Rules = overlay.Retention.Rules
	}

	// GC
	if overlay.GC.Enabled {
		base.GC.Enabled = true
	}
	if overlay.GC.Interval > 0 {
		base.GC.Interval = overlay.GC.Interval
	}
	if overlay.GC.OrphanGracePeriod > 0 {
		base.GC.OrphanGracePeriod = overlay.GC.OrphanGracePeriod
	}

	// Telemetry
	if overlay.Telemetry.Enabled {
		base.Telemetry.Enabled = true
	}
	if overlay.Telemetry.Endpoint != "" {
		base.Telemetry.Endpoint = overlay.Telemetry.Endpoint
	}
	if overlay.Telemetry.SampleRate > 0 {
		base.Telemetry.SampleRate = overlay.Telemetry.SampleRate
	}
	if !overlay.Telemetry.AlwaysSampleSlow {
		base.Telemetry.AlwaysSampleSlow = false
	}
	if overlay.Telemetry.ServiceName != "" {
		base.Telemetry.ServiceName = overlay.Telemetry.ServiceName
	}
	if overlay.Telemetry.BatchTimeout > 0 {
		base.Telemetry.BatchTimeout = overlay.Telemetry.BatchTimeout
	}

	// Logs mode config
	if len(overlay.Logs.BloomColumns) > 0 {
		base.Logs.BloomColumns = overlay.Logs.BloomColumns
	}
	if overlay.Logs.DeletePrefix != "" {
		base.Logs.DeletePrefix = overlay.Logs.DeletePrefix
	}
	if overlay.Logs.CompatVersion != "" {
		base.Logs.CompatVersion = overlay.Logs.CompatVersion
	}
	if overlay.Logs.Profile != "" {
		base.Logs.Profile = overlay.Logs.Profile
	}
	if overlay.Logs.Insert.Profile != "" {
		base.Logs.Insert.Profile = overlay.Logs.Insert.Profile
	}
	if overlay.Logs.Select.Profile != "" {
		base.Logs.Select.Profile = overlay.Logs.Select.Profile
	}

	// Traces mode config
	if len(overlay.Traces.BloomColumns) > 0 {
		base.Traces.BloomColumns = overlay.Traces.BloomColumns
	}
	if overlay.Traces.DeletePrefix != "" {
		base.Traces.DeletePrefix = overlay.Traces.DeletePrefix
	}
	if overlay.Traces.CompatVersion != "" {
		base.Traces.CompatVersion = overlay.Traces.CompatVersion
	}
	if overlay.Traces.JaegerEnabled {
		base.Traces.JaegerEnabled = true
	}
	if overlay.Traces.JaegerGRPCAddr != "" {
		base.Traces.JaegerGRPCAddr = overlay.Traces.JaegerGRPCAddr
	}
	if overlay.Traces.Profile != "" {
		base.Traces.Profile = overlay.Traces.Profile
	}
	if overlay.Traces.Insert.Profile != "" {
		base.Traces.Insert.Profile = overlay.Traces.Insert.Profile
	}
	if overlay.Traces.Select.Profile != "" {
		base.Traces.Select.Profile = overlay.Traces.Select.Profile
	}

	return base
}
