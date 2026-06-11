package config

import (
	"fmt"
	"net"
	"net/url"
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
	Shutdown    ShutdownConfig    `yaml:"shutdown"`
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
	Pmeta       PmetaConfig       `yaml:"pmeta"`

	Logs   LogsModeConfig   `yaml:"logs"`
	Traces TracesModeConfig `yaml:"traces"`
}

// PmetaConfig gates the unified partition-metadata layer (internal/pmeta). It is
// ON by default; when disabled no catalog store is built and the
// hot flush/query paths are unchanged. See docs/architecture/metadata-consolidation.md.
type PmetaConfig struct {
	// Enabled turns on the field/value catalog facet (dropdown speedups). The
	// catalog is built at flush and self-heals from S3, so it is safe to toggle.
	Enabled bool `yaml:"enabled"`

	// CardinalityThreshold caps how many distinct values the catalog keeps per
	// field. A field that exceeds it is treated as high-cardinality: the catalog
	// stops storing its values (bounding RAM) and the read path falls through to
	// the legacy scan, so the catalog never serves a truncated value list.
	// 0 = unlimited (keep every field exact). Default 50000.
	CardinalityThreshold int `yaml:"cardinality_threshold"`

	// AlwaysSketchFields are forced high-cardinality regardless of the threshold
	// (known unbounded id columns, e.g. trace_id, span_id, request_id).
	AlwaysSketchFields []string `yaml:"always_sketch_fields"`

	// RefuseSketchEnumeration, when true, makes field_values for an
	// AlwaysSketchFields field return EMPTY instead of scanning to enumerate it
	// (matches VL/VT, and avoids a pointless expensive scan on id columns nobody
	// browses — you look them up by exact value, which is unaffected). Opt-in
	// (default false) because it is a behavior change for those fields. Threshold
	// crossers are NOT refused — they still fall through to the scan.
	RefuseSketchEnumeration bool `yaml:"refuse_sketch_enumeration"`
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

	AckMode              string        `yaml:"ack_mode"`
	FlushLinger          time.Duration `yaml:"flush_linger"`
	FlushMaxRows         int           `yaml:"flush_max_rows"`
	PeerReplicate        bool          `yaml:"peer_replicate"`
	PeerReplicateTimeout time.Duration `yaml:"peer_replicate_timeout"`
	PeerReplicateTTL     time.Duration `yaml:"peer_replicate_ttl"`

	// BufferEngine selects how the insert buffer (recently-ingested,
	// not-yet-flushed rows) is held and queried (Option B):
	//   "buffer"   (default) — legacy []schema.{Log,Trace}Row staging +
	//                           struct→DataBlock conversion at query time.
	//   "logstore"           — a per-pod logstorage.Storage, queried via the
	//                           same engine as the S3-Parquet scan (no
	//                           conversion). Rolled out in phases behind this
	//                           flag; during P1 it dual-writes alongside the
	//                           legacy buffer, which stays authoritative.
	BufferEngine string `yaml:"buffer_engine"`
	// BufferDir is the local/tmpfs directory for the logstore buffer's
	// parts (durability is logstorage persistence here + the S3 Parquet flush).
	BufferDir string `yaml:"buffer_dir"`
	// BufferRetention bounds how long rows live in the logstore buffer before
	// VL drops them; once the flush sink is active this is just a ceiling.
	BufferRetention time.Duration `yaml:"buffer_retention"`
	// BufferFlushEnabled makes the logstore buffer the AUTHORITATIVE Parquet
	// producer via the BufferFlusher (the WAL cutover). Default false: the
	// legacy []row path stays authoritative and the buffer is read-only shadow.
	// Requires BufferEngine == "logstore".
	BufferFlushEnabled bool `yaml:"buffer_flush_enabled"`
	// BufferFlushInterval is the BufferFlusher's object-store flush CAP: the max
	// time a sub-target window waits before being flushed to S3 Parquet anyway.
	// The flusher checks more often than this but only flushes a window once it
	// reaches target_file_size OR has lingered this long — so S3 gets
	// ~target-sized objects, not one tiny file per tick. Must be << BufferRetention
	// (validated: retention >= 2*interval). Default 5m.
	BufferFlushInterval time.Duration `yaml:"buffer_flush_interval"`
}

// BufferEngineLogstore reports whether the logstorage-native buffer (Option B)
// is selected. Default ("" or "buffer") keeps the legacy staging buffer.
func (c *InsertConfig) BufferEngineLogstore() bool {
	return c.BufferEngine == "logstore"
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
	// K8s-style request/limit/scaling for S3 download concurrency
	// (request = always-reserved baseline, limit = hard ceiling,
	// scaling = ramp policy). When any of these are non-zero they
	// take precedence over MaxConcurrentDownloads which becomes
	// a deprecated alias (logs a startup warning once). When all
	// three are zero, MaxConcurrentDownloads is the live value
	// (legacy flat behaviour). See internal/resourcebounds.
	ConcurrentDownloadsRequest int    `yaml:"concurrent_downloads_request"`
	ConcurrentDownloadsLimit   int    `yaml:"concurrent_downloads_limit"`
	ConcurrentDownloadsScaling string `yaml:"concurrent_downloads_scaling"`
	ReadAheadBytes             int    `yaml:"read_ahead_bytes"`
	CoalesceGapBytes           int    `yaml:"coalesce_gap_bytes"`

	// ReadAheadMaxBytes is the ceiling for the ADAPTIVE read-ahead window.
	// The window starts at ReadAheadBytes and doubles (up to this max) after
	// 2+ consecutive forward-sequential buffer misses — wide scans get
	// CH-sized I/O units while needle queries stay at the small base window
	// (a random seek resets it). Default 8MB. Tune per signal: scan-heavy
	// logs deployments benefit from 8-16MB; needle-heavy traces deployments
	// can pin it lower (4MB) to bound over-fetch.
	ReadAheadMaxBytes int `yaml:"read_ahead_max_bytes"`

	// ReadAheadWasteThreshold is the waste-feedback knob for the ADAPTIVE
	// read-ahead window: when a window is evicted with MORE than this
	// fraction of its bytes never read (fetched-but-never-served, the same
	// high-water-mark accounting behind
	// lakehouse_s3_buffer_wasted_bytes_total), the next window is HALVED
	// (floored at read_ahead_bytes) instead of kept or grown, and the
	// growth credit resets — the window only grows again after consecutive
	// efficient windows. Catches the sparse-forward-hop pattern that the
	// pure grow/reset state machine misclassifies as a scan (measured:
	// 46 MB/query never-read on filtered counts at a 56% hit rate).
	// Default 0.5. Values >= 1 disable waste feedback (a window's waste
	// ratio is always < 1); 0 means "use the default", not "shrink on any
	// waste".
	ReadAheadWasteThreshold float64 `yaml:"read_ahead_waste_threshold"`

	// ReadBufferSize is parquet-go's per-column page read buffer for ranged
	// S3 opens (the library default of 4KB is sized for local disk; its own
	// docs suggest ~4MiB for network storage). Default 1MB: one buffered
	// page read per underlying GET instead of hundreds.
	ReadBufferSize int `yaml:"read_buffer_size"`

	// ParquetReadMode selects parquet-go's page read mode on ranged S3
	// opens: "async" (default — pages are read ahead by a per-column
	// goroutine, hiding S3 latency behind decode; bounded at one page in
	// flight per column reader) or "sync" (the library default mode, kept
	// as the rollback switch).
	ParquetReadMode string `yaml:"parquet_read_mode"`

	// ProjectedFetchMode selects the read strategy for COLUMN-PROJECTED
	// parquet reads (queries that touch fewer than half the columns):
	//   "planned" — CH-style plan-then-fetch: the exact coalesced
	//     byte ranges of the projected column chunks (dictionary pages and
	//     page-index sections included) are derived from the cached footer
	//     and fetched concurrently up-front; NO speculative read-ahead
	//     window. Fixes the measured ~46 MB/query of never-read window
	//     bytes on filtered counts, where per-file 2MB base windows are
	//     fetched and abandoned (the adaptive shrink never fires because
	//     window state is per-reader-instance).
	//   "window" — the previous behavior (adaptive read-ahead window),
	//     kept as the full rollback switch.
	// Full-scan (non-projected) reads always use the window stack.
	ProjectedFetchMode string `yaml:"projected_fetch_mode"`

	// ProjectedFetchMaxBytes caps the total coalesced bytes a single file's
	// plan-then-fetch may pin in memory (default 16MB). Plans above the cap
	// fall back to the adaptive-window path for that file (counted in
	// lakehouse_s3_projected_fetch_fallback_total{reason="cap"}). Bounds
	// the worst case of a wide projection over a huge row group.
	ProjectedFetchMaxBytes int `yaml:"projected_fetch_max_bytes"`
}

// ProjectedFetchMode values for S3Config.ProjectedFetchMode.
const (
	ProjectedFetchModePlanned = "planned"
	ProjectedFetchModeWindow  = "window"
)

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

	// FooterMaxItems is the upper bound on the parquet footer cache.
	// Each entry is ~5 KB so the default 10K caps the working set at
	// ~50 MB. At PB-scale (50M files, 5 PB at rest) the default leaves
	// a 0.02% hit rate — too low to be useful. Set to a larger value
	// (e.g. 50000–100000) for those deployments. When zero, the
	// storage layer auto-tunes: max(configured, 0.05% of manifest file
	// count) clamped to [10000, 100000]. The auto-tune re-fires after
	// every successful RefreshFromS3 so a growing bucket gradually
	// scales the cache up.
	FooterMaxItems int `yaml:"footer_max_items"`

	// LabelIndexMaxFields caps the number of distinct field names the
	// in-memory label index will track. When the index reaches this
	// limit, the least-recently-touched field is evicted on each new
	// Add. Setting to 0 disables eviction (unbounded growth — the
	// current default behaviour).
	//
	// At PB-scale with k8s-tagged data, distinct label key counts can
	// climb into the hundreds of thousands (k8s.pod.name × every pod
	// restart, container.id × every deploy). Capping prevents OOM in
	// the long-running pod while leaving the most active fields
	// available for tag-enumeration queries.
	LabelIndexMaxFields int `yaml:"label_index_max_fields"`

	// K8s-style request/limit/scaling for the L1 in-memory cache
	// budget. When non-zero, these take precedence over MemoryLimit
	// which becomes a deprecated alias logged once at startup. Sizes
	// are accepted as Go size strings (e.g. "256MB"). See
	// internal/resourcebounds.
	MemoryRequest string `yaml:"memory_request"`
	MemoryLimitV2 string `yaml:"memory_limit_v2"`
	MemoryScaling string `yaml:"memory_scaling"`
}

type DiscoveryConfig struct {
	HeadlessService       string        `yaml:"headless_service"`
	StorageNodes          []string      `yaml:"storage_nodes"`
	PartitionAuthKey      string        `yaml:"partition_auth_key"`
	RefreshInterval       time.Duration `yaml:"refresh_interval"`
	Timeout               time.Duration `yaml:"timeout"`
	PeerHeadlessService   string        `yaml:"peer_headless_service"`
	PeerRefreshInterval   time.Duration `yaml:"peer_refresh_interval"`
	RingStabilizeDuration time.Duration `yaml:"ring_stabilize_duration"`
	RingChangeNotify      bool          `yaml:"ring_change_notify"`
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
	ServeStale          bool          `yaml:"serve_stale"`
	WarmupWindow        time.Duration `yaml:"warmup_window"`
	MaxWarmupTime       time.Duration `yaml:"max_warmup_time"`
	PeerSyncTimeout     time.Duration `yaml:"peer_sync_timeout"`
	RequireManifestSync bool          `yaml:"require_manifest_sync"`
	StaleThreshold      time.Duration `yaml:"stale_threshold"`
	WALReconciliation   bool          `yaml:"wal_reconciliation"`
	CacheRevalidation   bool          `yaml:"cache_revalidation"`
	MaxResyncTime       time.Duration `yaml:"max_resync_time"`

	// MinManifestFiles is the lower-bound the lifecycle manager
	// requires before /ready can return 200. Counters the
	// "first-ever boot, empty PVC" honesty gap: without this gate
	// /ready flipped true ~1s after start while the manifest was
	// still empty, lying about query availability for the 3-5 min
	// it took the background S3 LIST to complete. Set above the
	// smallest healthy partition count (100 for tiny dev clusters,
	// 10000 for PB-scale prod). 0 disables (pre-change behaviour).
	MinManifestFiles int64 `yaml:"min_manifest_files"`

	// ServeWhileWarming, when true, lets /ready return 204 ("ready
	// but warming") immediately after disk recovery — before the
	// background S3 refresh completes. Strict load balancers that
	// only route on 200 will still wait for full warmup; soft
	// routers (vtselect peer fan-out, k8s readinessProbe with
	// successThreshold=1) can route to a 204 pod. Default false
	// keeps strict semantics.
	ServeWhileWarming bool `yaml:"serve_while_warming"`
}

type ShutdownConfig struct {
	Delay          time.Duration `yaml:"delay"`
	MaxGraceful    time.Duration `yaml:"max_graceful_duration"`
	FlushTimeout   time.Duration `yaml:"flush_timeout"`
	PersistTimeout time.Duration `yaml:"persist_timeout"`
	ReleaseTimeout time.Duration `yaml:"release_timeout"`
}

type QueryConfig struct {
	MaxConcurrent    int           `yaml:"max_concurrent"`
	FileWorkers      int           `yaml:"file_workers"`
	Timeout          time.Duration `yaml:"timeout"`
	MaxRows          int64         `yaml:"max_rows"`
	MaxFilesPerQuery int           `yaml:"max_files_per_query"`
	// MaxLiveBytes is a per-query ceiling on the bytes of in-flight
	// DataBlocks currently held by RunQuery before writeBlock has consumed
	// them. When exceeded, the query context is cancelled, returning a
	// partial result instead of OOM-killing the container. 0 means use the
	// default (defaultMaxLiveBytes in storage/parquets3).
	MaxLiveBytes  int64         `yaml:"max_live_bytes"`
	SlowThreshold time.Duration `yaml:"slow_threshold"`

	// K8s-style request/limit/scaling for file workers (process-wide
	// concurrent parquet-file readers). Same semantics as
	// S3.ConcurrentDownloads{Request,Limit,Scaling}. When non-zero
	// these take precedence over FileWorkers which becomes a
	// deprecated alias logged once at startup. See
	// internal/resourcebounds.
	FileWorkersRequest int    `yaml:"file_workers_request"`
	FileWorkersLimit   int    `yaml:"file_workers_limit"`
	FileWorkersScaling string `yaml:"file_workers_scaling"`

	// K8s-style request/limit/scaling for the per-query row ceiling.
	// MaxRows remains the legacy hard limit (also the current Limit
	// fallback when MaxRowsLimit is zero); MaxRowsRequest is an
	// operator-visible baseline. Streaming hits the ceiling at
	// MaxRowsLimit (or MaxRows if not set) regardless of Request.
	MaxRowsRequest int64  `yaml:"max_rows_request"`
	MaxRowsLimit   int64  `yaml:"max_rows_limit"`
	MaxRowsScaling string `yaml:"max_rows_scaling"`
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

	// Overrides keys: either "<account>:<project>" (e.g. "1:1") or a
	// string OrgID alias (e.g. "acme-corp"). String keys are resolved
	// via the alias map at startup; unresolved aliases re-resolve on
	// the alias-sync interval so late-registered tenants pick up
	// their override without a process restart. See
	// docs/multi-tenancy.md "Per-tenant overrides" for the merge rules.
	Overrides map[string]TenantOverride `yaml:"overrides"`
}

// TenantOverride scopes per-tenant policy knobs. Each field is optional;
// nil/zero values fall through to the global default. Designed as a
// thin "what does this tenant get differently" record, not a full
// shadow of the global config — the supported override surface is
// deliberately small and explicit.
type TenantOverride struct {
	// Retention.Keep overrides the default retention duration for files
	// owned by this tenant. Accepted forms: "7d", "30d", "720h", Go
	// duration syntax. Empty string = inherit global default.
	Retention TenantRetentionOverride `yaml:"retention"`

	// Cardinality caps metric label cardinality for this tenant.
	// Zero = inherit global stats.metrics_cardinality_limit.
	Cardinality TenantCardinalityOverride `yaml:"cardinality"`

	// Ingest applies per-tenant rate limits on the insert path.
	// Zero = no per-tenant limit (still subject to global limits).
	Ingest TenantIngestOverride `yaml:"ingest"`

	// Lifecycle replaces the global storage-class transition schedule
	// for files owned by this tenant. Empty = inherit global.
	Lifecycle []LifecycleRuleConfig `yaml:"lifecycle"`

	// S3 selects an alternative bucket for this tenant. When non-empty,
	// every Parquet object the tenant produces lands in that bucket
	// (vs the global s3.bucket); reads route the same way via the
	// pool's BucketRouter. Sidecars/manifests stay in the default
	// bucket so a single fleet-wide manifest still resolves files
	// across many tenant buckets.
	S3 TenantS3Override `yaml:"s3"`

	// Compaction overrides the compaction policy for this tenant.
	// Currently exposes the per-output-level compression schedule;
	// extensible to other compaction knobs without changing the
	// override-resolution flow.
	Compaction TenantCompactionOverride `yaml:"compaction"`
}

// TenantCompactionOverride scopes compaction knobs per tenant. Each
// field is optional; nil/empty falls through to the global
// Compaction.* value. Lets a high-volume / cost-sensitive tenant
// commit more CPU to compression than the global default, or a
// throughput-sensitive tenant relax compression to keep the
// compactor pool wide open.
type TenantCompactionOverride struct {
	// CompressionLevelByOutputLevel mirrors the global field's shape
	// (slot N = compression level for output files at compaction
	// level N). Empty = inherit global schedule.
	CompressionLevelByOutputLevel []int `yaml:"compression_level_by_output_level"`
}

type TenantRetentionOverride struct {
	Keep string `yaml:"keep"`
}

type TenantCardinalityOverride struct {
	MaxFields  int `yaml:"max_fields"`
	MaxStreams int `yaml:"max_streams"`
}

type TenantIngestOverride struct {
	MaxBytesPerSec int64 `yaml:"max_bytes_per_sec"`
	MaxRowsPerSec  int64 `yaml:"max_rows_per_sec"`
}

type TenantS3Override struct {
	Bucket string `yaml:"bucket"`
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

	// CompressionLevelByOutputLevel sets the zstd level used when
	// emitting a compacted file at output level i (index 0 = L0
	// rewrite, 1 = L0→L1, 2 = L1→L2, ...). Default is a progressive
	// schedule [7, 11, 15, 18, 22] — fresh writes optimize for
	// CPU/ingest, while older cold rollups invest more CPU to shrink
	// long-term storage. Out-of-range output levels fall back to the
	// last configured slot, or to Insert.CompressionLevel if the slice
	// is empty.
	CompressionLevelByOutputLevel []int `yaml:"compression_level_by_output_level"`

	// RowGroupSizeByOutputLevel sets the Parquet row-group size (max
	// rows per row group) used when emitting a compacted file at
	// output level i — same slot semantics as
	// CompressionLevelByOutputLevel (index 0 = L0 rewrite, 1 = L0→L1,
	// 2 = L1→L2, ...). Default [10000, 10000, 20000]: L0/L1 outputs
	// keep the historical row-group size (the Insert.RowGroupSize
	// default), L2+ rollups double it — cold rollups are scan-heavy
	// and rarely pruned at row-group granularity, so fewer/larger row
	// groups buy compression (dictionaries amortized over more rows,
	// fewer page + row-group headers) at a modest pruning-granularity
	// cost. Out-of-range output levels fall back to the last
	// configured slot, or to Insert.RowGroupSize if the slice is
	// empty.
	RowGroupSizeByOutputLevel []int `yaml:"row_group_size_by_output_level"`
}

// CompressionLevelForOutput returns the configured zstd level for the
// given compaction output level. Falls back to the last slot if the
// caller asks for a deeper rollup than configured (so an operator who
// listed [7, 11, 15] gets 15 for level 3+ instead of an out-of-bounds
// panic). Returns 0 when the slice is empty — the compactor treats
// that as "use Insert.CompressionLevel" so existing deployments keep
// their pre-progressive behaviour until they opt in.
func (c *CompactionConfig) CompressionLevelForOutput(outputLevel int) int {
	if len(c.CompressionLevelByOutputLevel) == 0 {
		return 0
	}
	if outputLevel >= len(c.CompressionLevelByOutputLevel) {
		outputLevel = len(c.CompressionLevelByOutputLevel) - 1
	}
	if outputLevel < 0 {
		outputLevel = 0
	}
	return c.CompressionLevelByOutputLevel[outputLevel]
}

// RowGroupSizeForOutput returns the configured Parquet row-group size
// for the given compaction output level. Same contract as
// CompressionLevelForOutput: saturates to the last configured slot for
// deeper rollups (so [10000, 20000] yields 20000 for level 5 instead
// of an out-of-bounds panic), and returns 0 when the slice is empty —
// the compactor treats that as "use the static Insert.RowGroupSize"
// so deployments that clear the schedule keep the pre-schedule
// behaviour.
func (c *CompactionConfig) RowGroupSizeForOutput(outputLevel int) int {
	if len(c.RowGroupSizeByOutputLevel) == 0 {
		return 0
	}
	if outputLevel >= len(c.RowGroupSizeByOutputLevel) {
		outputLevel = len(c.RowGroupSizeByOutputLevel) - 1
	}
	if outputLevel < 0 {
		outputLevel = 0
	}
	return c.RowGroupSizeByOutputLevel[outputLevel]
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
	TransitionDays int    `yaml:"transition_days" json:"transition_days"`
	StorageClass   string `yaml:"storage_class" json:"storage_class"`
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

	// K8s-style request/limit/scaling for the smart-cache disk budget.
	// When non-zero, these take precedence over DiskLimitMax which
	// becomes a deprecated alias logged once at startup. Sizes accepted
	// as Go size strings (e.g. "50GB"). See internal/resourcebounds.
	DiskRequest string `yaml:"disk_request"`
	DiskLimit   string `yaml:"disk_limit"`
	DiskScaling string `yaml:"disk_scaling"`
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

		// pmeta is the metadata layer post-consolidation: facets serve reads,
		// bundles persist/warm, and the legacy sidecars are no longer written
		// (their writers are deleted). Disabling it is a degraded mode: no
		// catalog/bloom for new files; cold restarts warm from footers only.
		Pmeta: PmetaConfig{Enabled: true},

		S3: S3Config{
			Region:                  "us-east-1",
			MaxConnections:          128,
			Timeout:                 30 * time.Second,
			RetryMax:                3,
			RetryBaseDelay:          200 * time.Millisecond,
			MaxConcurrentDownloads:  16,
			ReadAheadBytes:          2 * 1024 * 1024, // 2MB base window
			CoalesceGapBytes:        1024 * 1024,     // 1MB (BDP-priced; was 64KB)
			ReadAheadMaxBytes:       8 * 1024 * 1024, // 8MB adaptive ceiling
			ReadAheadWasteThreshold: 0.5,             // shrink window when >50% of it was never read
			ReadBufferSize:          1024 * 1024,     // 1MB parquet page read buffer
			ParquetReadMode:         "async",
			ProjectedFetchMode:      ProjectedFetchModeWindow,
			ProjectedFetchMaxBytes:  16 * 1024 * 1024, // 16MB per-file plan cap
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
			RefreshInterval:       5 * time.Minute,
			Timeout:               10 * time.Second,
			PeerRefreshInterval:   30 * time.Second,
			RingStabilizeDuration: 60 * time.Second,
			RingChangeNotify:      true,
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
			ServeStale:          false,
			WarmupWindow:        24 * time.Hour,
			MaxWarmupTime:       5 * time.Minute,
			PeerSyncTimeout:     30 * time.Second,
			RequireManifestSync: true,
			StaleThreshold:      1 * time.Hour,
			WALReconciliation:   true,
			CacheRevalidation:   true,
			MaxResyncTime:       10 * time.Minute,
		},

		Shutdown: ShutdownConfig{
			Delay:          5 * time.Second,
			MaxGraceful:    7 * time.Second,
			FlushTimeout:   30 * time.Second,
			PersistTimeout: 10 * time.Second,
			ReleaseTimeout: 5 * time.Second,
		},

		Query: QueryConfig{
			MaxConcurrent:    32,
			FileWorkers:      64,
			Timeout:          60 * time.Second,
			MaxRows:          10_000_000,
			MaxFilesPerQuery: 0, // 0 = unlimited (match VL upstream); memory budget is the real safety net
			// 512 MiB live-block budget — about 1/4 of the 2 GiB container
			// limit, leaving room for caches + parquet decode buffers.
			MaxLiveBytes:  512 * 1024 * 1024,
			SlowThreshold: 5 * time.Second,
		},

		Insert: InsertConfig{
			FlushInterval:    60 * time.Second,
			MaxBufferRows:    50000,
			MaxBufferBytes:   "256MB",
			TargetFileSize:   "128MB",
			RowGroupSize:     10000,
			BloomColumns:     []string{"service.name", "trace_id"},
			CompressionLevel: 3,

			AckMode:              "buffer",
			FlushLinger:          200 * time.Millisecond,
			FlushMaxRows:         5000,
			PeerReplicate:        false,
			PeerReplicateTimeout: 5 * time.Millisecond,
			PeerReplicateTTL:     30 * time.Second,

			BufferEngine:        "buffer", // legacy staging buffer; "logstore" opts into Option B
			BufferDir:           "/data/lakehouse/buffer",
			BufferRetention:     time.Hour,
			BufferFlushEnabled:  false, // cutover off by default; legacy path authoritative
			BufferFlushInterval: 5 * time.Minute,
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
			// Progressive compression schedule, indexed by the output
			// file's compaction level (slot N = level for files at
			// compaction-level N). Default [3, 7, 11] maps to the
			// three useful encoder levels the parquet-go zstd codec
			// exposes (Default / Better / Best) — anything above 11
			// collapses to the same Best encoder and is a wasted knob
			// until we switch to klauspost/compress/zstd direct (out
			// of scope for this PR; the schedule shape stays so a
			// later codec swap doesn't need a config migration).
			//
			//   L0 (fresh write):      level 3 = zstd Default  (~100 MB/s, ratio ~2.8×)
			//   L1 (1st compaction):   level 7 = zstd Better   (~30  MB/s, ratio ~3.1×)
			//   L2+ (rollups):         level 11 = zstd Best    (~15  MB/s, ratio ~3.3×)
			//
			// CPU spent escalates fastest at the L2 step (~2× over L1)
			// for the smallest marginal ratio gain (~0.2×). Operators
			// who don't care about cold-tier storage cost can pin the
			// schedule at [7] and run a uniform Better-compression
			// everywhere; operators chasing every byte can override
			// with their own array once we have a finer codec.
			CompressionLevelByOutputLevel: []int{3, 7, 11},
			// Row-group size schedule, same slot semantics. L0/L1
			// outputs keep the historical 10k rows per row group
			// (= the Insert.RowGroupSize default); L2+ rollups double
			// to 20k. Cold rollups are scan-heavy and rarely benefit
			// from row-group-granularity pruning, so fewer/larger
			// groups trade a little pruning resolution for better
			// compression (dictionaries amortized over 2× the rows,
			// half the page/row-group header overhead). Measured on
			// real L2 files.
			RowGroupSizeByOutputLevel: []int{10000, 10000, 20000},
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

	if err := c.validateS3Endpoint(); err != nil {
		return err
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

func (c *Config) validateS3Endpoint() error {
	if c.S3.Endpoint == "" {
		return nil
	}
	u, err := url.Parse(c.S3.Endpoint)
	if err != nil {
		return fmt.Errorf("--lakehouse.s3.endpoint: invalid URL %q: %w", c.S3.Endpoint, err)
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("--lakehouse.s3.endpoint: scheme must be http or https, got %q", u.Scheme)
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("--lakehouse.s3.endpoint: link-local IP %q not allowed (possible SSRF)", host)
		}
	}
	if host == "metadata.google.internal" {
		return fmt.Errorf("--lakehouse.s3.endpoint: cloud metadata endpoint %q not allowed", host)
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
	switch c.Insert.BufferEngine {
	case "", "buffer", "logstore":
	default:
		return fmt.Errorf("--lakehouse.insert.buffer-engine must be \"buffer\" or \"logstore\", got %q", c.Insert.BufferEngine)
	}
	if c.Insert.BufferFlushEnabled {
		if !c.Insert.BufferEngineLogstore() {
			return fmt.Errorf("--lakehouse.insert.buffer-flush-enabled requires buffer-engine=logstore, got %q", c.Insert.BufferEngine)
		}
		if c.Insert.BufferFlushInterval <= 0 {
			return fmt.Errorf("--lakehouse.insert.buffer-flush-interval must be > 0 when flush is enabled")
		}
		// CRASH-SAFETY constraint: un-flushed rows live ONLY in the buffer until
		// the flusher commits them, so the buffer must retain them across (a) a
		// full linger window before they flush AND (b) any restart downtime before
		// recovery re-flushes. Require retention >= 4x the flush cap so there is a
		// generous recovery margin beyond the 2x linger floor — if retention is
		// too tight, a row could age out of the buffer before a crashed flusher
		// recovers, which IS data loss (there is no LH WAL backstop anymore).
		if c.Insert.BufferRetention < 4*c.Insert.BufferFlushInterval {
			return fmt.Errorf("--lakehouse.insert.buffer-retention (%s) must be >= 4x buffer-flush-interval (%s): un-flushed rows must survive a linger window PLUS restart downtime, since the buffer is their only store until flushed",
				c.Insert.BufferRetention, c.Insert.BufferFlushInterval)
		}
	}
	if c.Insert.MaxBufferBytes != "" {
		if _, err := ParseSizeBytes(c.Insert.MaxBufferBytes); err != nil {
			return fmt.Errorf("--lakehouse.insert.max-buffer-bytes: invalid size %q: %w", c.Insert.MaxBufferBytes, err)
		}
	}
	if _, err := ParseSizeBytes(c.Insert.TargetFileSize); err != nil {
		return fmt.Errorf("--lakehouse.insert.target-file-size: invalid size %q: %w", c.Insert.TargetFileSize, err)
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

	switch c.UI.Theme {
	case "auto", "dark", "light", "":
	default:
		return fmt.Errorf("--lakehouse.ui.theme must be auto, dark, or light; got %q", c.UI.Theme)
	}

	switch c.S3.ParquetReadMode {
	case "async", "sync", "":
	default:
		return fmt.Errorf("--lakehouse.s3.parquet-read-mode must be async or sync, got %q", c.S3.ParquetReadMode)
	}

	switch c.S3.ProjectedFetchMode {
	case ProjectedFetchModePlanned, ProjectedFetchModeWindow, "":
	default:
		return fmt.Errorf("--lakehouse.s3.projected-fetch-mode must be planned or window, got %q", c.S3.ProjectedFetchMode)
	}
	if c.S3.ProjectedFetchMaxBytes < 0 {
		return fmt.Errorf("--lakehouse.s3.projected-fetch-max-bytes must be >= 0, got %d", c.S3.ProjectedFetchMaxBytes)
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
		for i, n := range c.Compaction.RowGroupSizeByOutputLevel {
			if n <= 0 {
				return fmt.Errorf("--lakehouse.compaction.row-group-size-by-output-level slot %d must be positive, got %d", i, n)
			}
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

	return nil
}

func (c *Config) ValidateShutdown(terminationGracePeriod time.Duration) error {
	total := c.Shutdown.Delay + c.Shutdown.FlushTimeout + c.Shutdown.PersistTimeout + c.Shutdown.ReleaseTimeout
	margin := 5 * time.Second
	if total > terminationGracePeriod-margin {
		return fmt.Errorf("shutdown phase total (%s) exceeds terminationGracePeriodSeconds (%s) minus %s safety margin", total, terminationGracePeriod, margin)
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
	if overlay.S3.ConcurrentDownloadsRequest > 0 {
		base.S3.ConcurrentDownloadsRequest = overlay.S3.ConcurrentDownloadsRequest
	}
	if overlay.S3.ConcurrentDownloadsLimit > 0 {
		base.S3.ConcurrentDownloadsLimit = overlay.S3.ConcurrentDownloadsLimit
	}
	if overlay.S3.ConcurrentDownloadsScaling != "" {
		base.S3.ConcurrentDownloadsScaling = overlay.S3.ConcurrentDownloadsScaling
	}
	if overlay.S3.ReadAheadBytes > 0 {
		base.S3.ReadAheadBytes = overlay.S3.ReadAheadBytes
	}
	if overlay.S3.CoalesceGapBytes > 0 {
		base.S3.CoalesceGapBytes = overlay.S3.CoalesceGapBytes
	}
	if overlay.S3.ReadAheadMaxBytes > 0 {
		base.S3.ReadAheadMaxBytes = overlay.S3.ReadAheadMaxBytes
	}
	if overlay.S3.ReadAheadWasteThreshold > 0 {
		base.S3.ReadAheadWasteThreshold = overlay.S3.ReadAheadWasteThreshold
	}
	if overlay.S3.ReadBufferSize > 0 {
		base.S3.ReadBufferSize = overlay.S3.ReadBufferSize
	}
	if overlay.S3.ParquetReadMode != "" {
		base.S3.ParquetReadMode = overlay.S3.ParquetReadMode
	}
	if overlay.S3.ProjectedFetchMode != "" {
		base.S3.ProjectedFetchMode = overlay.S3.ProjectedFetchMode
	}
	if overlay.S3.ProjectedFetchMaxBytes > 0 {
		base.S3.ProjectedFetchMaxBytes = overlay.S3.ProjectedFetchMaxBytes
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
	if overlay.Cache.MemoryRequest != "" {
		base.Cache.MemoryRequest = overlay.Cache.MemoryRequest
	}
	if overlay.Cache.MemoryLimitV2 != "" {
		base.Cache.MemoryLimitV2 = overlay.Cache.MemoryLimitV2
	}
	if overlay.Cache.MemoryScaling != "" {
		base.Cache.MemoryScaling = overlay.Cache.MemoryScaling
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
	if overlay.Discovery.RingStabilizeDuration > 0 {
		base.Discovery.RingStabilizeDuration = overlay.Discovery.RingStabilizeDuration
	}
	if overlay.Discovery.RingChangeNotify {
		base.Discovery.RingChangeNotify = true
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
	if overlay.Startup.PeerSyncTimeout > 0 {
		base.Startup.PeerSyncTimeout = overlay.Startup.PeerSyncTimeout
	}
	if overlay.Startup.RequireManifestSync {
		base.Startup.RequireManifestSync = true
	}
	if overlay.Startup.StaleThreshold > 0 {
		base.Startup.StaleThreshold = overlay.Startup.StaleThreshold
	}
	if overlay.Startup.WALReconciliation {
		base.Startup.WALReconciliation = true
	}
	if overlay.Startup.CacheRevalidation {
		base.Startup.CacheRevalidation = true
	}
	if overlay.Startup.MaxResyncTime > 0 {
		base.Startup.MaxResyncTime = overlay.Startup.MaxResyncTime
	}

	// Shutdown
	if overlay.Shutdown.Delay > 0 {
		base.Shutdown.Delay = overlay.Shutdown.Delay
	}
	if overlay.Shutdown.MaxGraceful > 0 {
		base.Shutdown.MaxGraceful = overlay.Shutdown.MaxGraceful
	}
	if overlay.Shutdown.FlushTimeout > 0 {
		base.Shutdown.FlushTimeout = overlay.Shutdown.FlushTimeout
	}
	if overlay.Shutdown.PersistTimeout > 0 {
		base.Shutdown.PersistTimeout = overlay.Shutdown.PersistTimeout
	}
	if overlay.Shutdown.ReleaseTimeout > 0 {
		base.Shutdown.ReleaseTimeout = overlay.Shutdown.ReleaseTimeout
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
	if overlay.Query.MaxLiveBytes > 0 {
		base.Query.MaxLiveBytes = overlay.Query.MaxLiveBytes
	}
	if overlay.Query.SlowThreshold > 0 {
		base.Query.SlowThreshold = overlay.Query.SlowThreshold
	}
	if overlay.Query.FileWorkersRequest > 0 {
		base.Query.FileWorkersRequest = overlay.Query.FileWorkersRequest
	}
	if overlay.Query.FileWorkersLimit > 0 {
		base.Query.FileWorkersLimit = overlay.Query.FileWorkersLimit
	}
	if overlay.Query.FileWorkersScaling != "" {
		base.Query.FileWorkersScaling = overlay.Query.FileWorkersScaling
	}
	if overlay.Query.MaxRowsRequest > 0 {
		base.Query.MaxRowsRequest = overlay.Query.MaxRowsRequest
	}
	if overlay.Query.MaxRowsLimit > 0 {
		base.Query.MaxRowsLimit = overlay.Query.MaxRowsLimit
	}
	if overlay.Query.MaxRowsScaling != "" {
		base.Query.MaxRowsScaling = overlay.Query.MaxRowsScaling
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
	if overlay.Tenant.OrgIDHeader != "" {
		base.Tenant.OrgIDHeader = overlay.Tenant.OrgIDHeader
	}
	if overlay.Tenant.MetricsFormat != "" {
		base.Tenant.MetricsFormat = overlay.Tenant.MetricsFormat
	}
	if overlay.Tenant.AutoRegister {
		base.Tenant.AutoRegister = true
	}
	if overlay.Tenant.AliasSyncInterval > 0 {
		base.Tenant.AliasSyncInterval = overlay.Tenant.AliasSyncInterval
	}
	if len(overlay.Tenant.Aliases) > 0 {
		base.Tenant.Aliases = overlay.Tenant.Aliases
	}
	if len(overlay.Tenant.Overrides) > 0 {
		base.Tenant.Overrides = overlay.Tenant.Overrides
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
	if overlay.Insert.BufferEngine != "" {
		base.Insert.BufferEngine = overlay.Insert.BufferEngine
	}
	if overlay.Insert.BufferDir != "" {
		base.Insert.BufferDir = overlay.Insert.BufferDir
	}
	if overlay.Insert.BufferRetention > 0 {
		base.Insert.BufferRetention = overlay.Insert.BufferRetention
	}
	if overlay.Insert.BufferFlushEnabled {
		base.Insert.BufferFlushEnabled = true
	}
	if overlay.Insert.BufferFlushInterval > 0 {
		base.Insert.BufferFlushInterval = overlay.Insert.BufferFlushInterval
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
	if len(overlay.Compaction.CompressionLevelByOutputLevel) > 0 {
		base.Compaction.CompressionLevelByOutputLevel = overlay.Compaction.CompressionLevelByOutputLevel
	}
	if len(overlay.Compaction.RowGroupSizeByOutputLevel) > 0 {
		base.Compaction.RowGroupSizeByOutputLevel = overlay.Compaction.RowGroupSizeByOutputLevel
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
	if overlay.SmartCache.DiskRequest != "" {
		base.SmartCache.DiskRequest = overlay.SmartCache.DiskRequest
	}
	if overlay.SmartCache.DiskLimit != "" {
		base.SmartCache.DiskLimit = overlay.SmartCache.DiskLimit
	}
	if overlay.SmartCache.DiskScaling != "" {
		base.SmartCache.DiskScaling = overlay.SmartCache.DiskScaling
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
