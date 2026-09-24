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

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
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
	// Mode is the signal this binary serves: logs or traces. Each binary sets
	// its own mode, so a value in the config file is ignored.
	Mode Mode `yaml:"mode"`
	// Role selects the components to run: all, insert or select.
	Role Role `yaml:"role"`
	// Topology selects how the hot tier is found: auto, storage-node, direct
	// or loki-proxy.
	Topology Topology `yaml:"topology"`
	// Profile names the profile the config file is merged over: balanced,
	// max-performance, max-durability, max-cost-savings or dev. A per-signal
	// or per-role profile takes precedence.
	Profile Profile `yaml:"profile"`

	S3        S3Config        `yaml:"s3"`
	Cache     CacheConfig     `yaml:"cache"`
	Discovery DiscoveryConfig `yaml:"discovery"`
	// HotBoundary fixes the hot/cold boundary at an age such as 7d or 168h
	// instead of discovering it from the hot storage nodes.
	HotBoundary string            `yaml:"hot_boundary"`
	Manifest    ManifestConfig    `yaml:"manifest"`
	Prefetch    PrefetchConfig    `yaml:"prefetch"`
	Peer        PeerConfig        `yaml:"peer"`
	Startup     StartupConfig     `yaml:"startup"`
	Shutdown    ShutdownConfig    `yaml:"shutdown"`
	Query       QueryConfig       `yaml:"query"`
	Insert      InsertConfig      `yaml:"insert"`
	Select      SelectConfig      `yaml:"select"`
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

// PromotedAttribute declares a custom (non-OTel-standard) attribute that the
// operator wants promoted out of the attribute map into a dedicated spare
// column (Tier 2). Name is the attribute key as it appears in ingested data;
// Bloom requests a SplitBlockFilter on the slot column (set it only for
// high-cardinality keys queried by equality — a bloom on a low-card key is
// wasted space). Up to schema.DedicatedSlotCount per signal; excess is ignored
// with a startup warning. The name→slot binding is written to each file's
// Parquet footer so the file stays self-describing and portable.
type PromotedAttribute struct {
	Name  string `yaml:"name"`
	Bloom bool   `yaml:"bloom"`
}

// LogsModeConfig holds settings that apply only to lakehouse-logs.
type LogsModeConfig struct {
	// BloomColumns are extra columns to bloom-index for logs; the built-in log
	// bloom columns are always included.
	BloomColumns []string `yaml:"bloom_columns"`
	// PromotedAttributes are custom attributes promoted into dedicated Parquet
	// columns, each {name, bloom}.
	PromotedAttributes []PromotedAttribute `yaml:"promoted_attributes"`
	// DeletePrefix is the path prefix of the logs delete API.
	DeletePrefix string `yaml:"delete_prefix"`
	// CompatVersion is the VictoriaLogs version the API reports; empty reports
	// the built-in one.
	CompatVersion string `yaml:"compat_version"`
	// Profile is the profile for the logs signal; it takes precedence over
	// profile.
	Profile Profile        `yaml:"profile"`
	Insert  RoleProfileRef `yaml:"insert"`
	Select  RoleProfileRef `yaml:"select"`
}

// TracesModeConfig holds settings that apply only to lakehouse-traces.
type TracesModeConfig struct {
	// BloomColumns are extra columns to bloom-index for traces; the built-in
	// trace bloom columns are always included.
	BloomColumns []string `yaml:"bloom_columns"`
	// PromotedAttributes are custom attributes promoted into dedicated Parquet
	// columns, each {name, bloom}.
	PromotedAttributes []PromotedAttribute `yaml:"promoted_attributes"`
	// DeletePrefix is the path prefix of the traces delete API.
	DeletePrefix string `yaml:"delete_prefix"`
	// CompatVersion is the VictoriaTraces version the API reports; empty
	// reports the built-in one.
	CompatVersion string `yaml:"compat_version"`
	// JaegerEnabled serves the Jaeger query API.
	JaegerEnabled bool `yaml:"jaeger_enabled"`
	// JaegerGRPCAddr is the listen address of the Jaeger gRPC API.
	JaegerGRPCAddr string `yaml:"jaeger_grpc_addr"`
	// Profile is the profile for the traces signal; it takes precedence over
	// profile.
	Profile Profile        `yaml:"profile"`
	Insert  RoleProfileRef `yaml:"insert"`
	Select  RoleProfileRef `yaml:"select"`
}

// ActivePromotedAttributes returns the operator-configured Tier-2 custom
// attribute promotions for the active signal.
func (c *Config) ActivePromotedAttributes() []PromotedAttribute {
	if c.Mode == ModeTraces {
		return c.Traces.PromotedAttributes
	}
	return c.Logs.PromotedAttributes
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

// WrittenBloomColumns returns the bloom-filtered column set the Parquet writer
// ACTUALLY emits for the active signal: the strict schema bloom set (Tier-1
// dedicated columns flagged HasBloom + the legacy service.name/trace_id) plus
// the operator's bloom-enabled custom slots, unioned with any extra columns
// named via -lakehouse.<signal>.bloom-columns.
//
// This is the source of truth the stats / cardinality API must report
// `has_bloom` from. ActiveBloomColumns() alone is just the legacy operator list
// ({service.name, trace_id}) and omits every dedicated column — so the
// Cardinality Explorer would show "no bloom" for columns that are in fact
// bloom-indexed on disk (writer.go + compactor.go both bloom this exact set).
func (c *Config) WrittenBloomColumns() []string {
	var slotBlooms []string
	if pa := c.ActivePromotedAttributes(); len(pa) > 0 {
		sa := make([]schema.SlotAttr, len(pa))
		for i, a := range pa {
			sa[i] = schema.SlotAttr{Name: a.Name, Bloom: a.Bloom}
		}
		slotBlooms = schema.NewSlotResolver(sa).BloomSlots()
	}
	var cols []string
	if c.Mode == ModeTraces {
		cols = schema.TraceBloomColumns(slotBlooms...)
	} else {
		cols = schema.LogBloomColumns(slotBlooms...)
	}
	// Union with operator-named extras so a custom -bloom-columns entry is still
	// reported as bloomed.
	seen := make(map[string]bool, len(cols))
	for _, col := range cols {
		seen[col] = true
	}
	for _, col := range c.ActiveBloomColumns() {
		if col != "" && !seen[col] {
			seen[col] = true
			cols = append(cols, col)
		}
	}
	return cols
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

// InsertConfig controls buffering and flushing on the write path.
type InsertConfig struct {
	// FlushInterval is the interval at which buffered rows are flushed to
	// Parquet on S3.
	FlushInterval time.Duration `yaml:"flush_interval"`
	// MaxBufferRows is the number of rows a partition buffer holds before it
	// flushes.
	MaxBufferRows int `yaml:"max_buffer_rows"`
	// MaxBufferBytes bounds the rows not yet written to object storage —
	// buffered, being uploaded, or put back after a failed upload — as a size
	// string. Past it inserts are refused with 429, as VictoriaLogs does when
	// it cannot take writes, until flushes catch up.
	MaxBufferBytes string `yaml:"max_buffer_bytes"`
	// TargetFileSize is the target Parquet file size, as a size string; a
	// buffer reaching it flushes early.
	TargetFileSize string `yaml:"target_file_size"`
	// RowGroupSize is the number of rows per Parquet row group in freshly
	// written files.
	RowGroupSize int `yaml:"row_group_size"`
	// BloomColumns are extra columns to bloom-index on write, in addition to
	// the signal's built-in bloom columns.
	BloomColumns []string `yaml:"bloom_columns"`
	// CompressionLevel is the zstd level (1-22) of freshly written files;
	// compaction recompresses older files per
	// compaction.compression_level_by_output_level.
	CompressionLevel int `yaml:"compression_level"`

	// AckMode selects when an insert is acknowledged: buffer (once buffered),
	// wal or flush-sync (once S3 confirms the write).
	AckMode string `yaml:"ack_mode"`
	// FlushLinger delays a flush to coalesce small writes.
	FlushLinger time.Duration `yaml:"flush_linger"`
	// FlushMaxRows caps the rows of one flush batch.
	FlushMaxRows int `yaml:"flush_max_rows"`
	// PeerReplicate replicates inserts to peer insert pods.
	PeerReplicate bool `yaml:"peer_replicate"`
	// PeerReplicateTimeout bounds one peer replication.
	PeerReplicateTimeout time.Duration `yaml:"peer_replicate_timeout"`
	// PeerReplicateTTL is how long replicated rows are kept on peers.
	PeerReplicateTTL time.Duration `yaml:"peer_replicate_ttl"`

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

// GCConfig controls garbage collection of orphan files.
type GCConfig struct {
	// Enabled runs garbage collection of orphan files.
	Enabled bool `yaml:"enabled"`
	// Interval is the garbage collection scan interval.
	Interval time.Duration `yaml:"interval"`
	// OrphanGracePeriod is the age an unreferenced file must reach before
	// garbage collection deletes it.
	OrphanGracePeriod time.Duration `yaml:"orphan_grace_period"`
}

func (c *Config) InsertEnabled() bool {
	return c.Role == RoleAll || c.Role == RoleInsert
}

func (c *Config) SelectEnabled() bool {
	return c.Role == RoleAll || c.Role == RoleSelect
}

// SelectConfig controls how select pods read rows not yet flushed to S3.
type SelectConfig struct {
	// BufferQueryEnabled queries the insert pods for rows not yet flushed to
	// S3.
	BufferQueryEnabled bool `yaml:"buffer_query_enabled"`
	// InsertHeadlessService is the Kubernetes headless service that resolves
	// the insert pods for buffer queries.
	InsertHeadlessService string `yaml:"insert_headless_service"`
	// BufferQueryTimeout bounds a buffer query to the insert pods.
	BufferQueryTimeout time.Duration `yaml:"buffer_query_timeout"`
	// AZAware prefers insert pods in the select pod's own availability zone
	// for buffer queries.
	AZAware bool `yaml:"az_aware"`
	// CrossAZFallback queries insert pods in other zones when no same-zone pod
	// answers.
	CrossAZFallback bool `yaml:"cross_az_fallback"`
}

// S3Config is the object storage the Parquet files live in.
type S3Config struct {
	// Bucket is the S3 bucket holding the Parquet files. Required.
	Bucket string `yaml:"bucket"`
	// Region is the AWS region of the bucket.
	Region string `yaml:"region"`
	// Prefix is the S3 key prefix. Empty derives it from the tenant prefix and
	// the signal (logs/ or traces/).
	Prefix string `yaml:"prefix"`
	// Endpoint is a custom S3-compatible endpoint URL (MinIO, R2); http or
	// https only.
	Endpoint string `yaml:"endpoint"`
	// AccessKey is a static S3 access key. Prefer IAM roles or IRSA.
	AccessKey string `yaml:"access_key"`
	// SecretKey is the static S3 secret key paired with AccessKey.
	SecretKey string `yaml:"secret_key"`
	// ForcePathStyle uses path-style S3 URLs, which MinIO requires.
	ForcePathStyle bool `yaml:"force_path_style"`
	// MaxConnections caps concurrent HTTP connections to S3.
	MaxConnections int `yaml:"max_connections"`
	// Timeout bounds a single S3 request.
	Timeout time.Duration `yaml:"timeout"`
	// RetryMax caps retries of a failed S3 request.
	RetryMax int `yaml:"retry_max"`
	// RetryBaseDelay is the first retry backoff; it doubles on every retry.
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
	// MaxConcurrentDownloads is the deprecated flat S3 download concurrency,
	// used as request and limit when concurrent_downloads_request and
	// concurrent_downloads_limit are both unset.
	MaxConcurrentDownloads int `yaml:"max_concurrent_downloads"`
	// K8s-style request/limit/scaling for S3 download concurrency
	// (request = always-reserved baseline, limit = hard ceiling,
	// scaling = ramp policy). When any of these are non-zero they
	// take precedence over MaxConcurrentDownloads which becomes
	// a deprecated alias (logs a startup warning once). When all
	// three are zero, MaxConcurrentDownloads is the live value
	// (legacy flat behaviour). See internal/resourcebounds.
	ConcurrentDownloadsRequest int `yaml:"concurrent_downloads_request"`
	// ConcurrentDownloadsLimit is the hard ceiling on concurrent S3 downloads.
	// 0 falls back to the request, or to max_concurrent_downloads when both
	// are unset.
	ConcurrentDownloadsLimit int `yaml:"concurrent_downloads_limit"`
	// ConcurrentDownloadsScaling is the ramp policy from request to limit:
	// fixed, linear or expbackoff. Empty means fixed.
	ConcurrentDownloadsScaling string `yaml:"concurrent_downloads_scaling"`
	// ReadAheadBytes is the base read-ahead window, in bytes, for sequential
	// S3 range reads; the adaptive window grows from it up to
	// read_ahead_max_bytes.
	ReadAheadBytes int `yaml:"read_ahead_bytes"`
	// CoalesceGapBytes merges S3 range reads separated by fewer bytes than
	// this into one GET.
	CoalesceGapBytes int `yaml:"coalesce_gap_bytes"`

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

	// ProjectedFetchMaxBytes — DEPRECATED since the planned-fetch v2
	// slice 1 cap re-scope (kept parsed for config compatibility; no
	// longer consulted). The per-PLAN cap punished exactly the cross-RG
	// coalescing that cuts GETs (merged gap bytes counted into the plan
	// total and tripped fallback{reason="cap"}). The cap is now per-SPAN
	// (PlannedFetchSpanCapBytes — CH bytes_per_read_task scope) and plan
	// admission is the memory ledger's job: span bytes are charged to the
	// same fileBudget ledger as decode admission, and the file worker
	// already holds an fi.Size admission that subsumes any plan (a plan
	// can never exceed fi.Size — spans are file-clamped and disjoint).
	ProjectedFetchMaxBytes int `yaml:"projected_fetch_max_bytes"`

	// PlannedFetchMaxInflight bounds concurrent span GETs per file on the
	// plan-then-fetch projected read path: min(k, spans) in flight.
	// Default 16 (both signals): the live planned-v1 verdict measured
	// 13-15 spans/file on real L2 footer geometry being drained 4-at-a-
	// time into ~4 serial RTT waves per file; k=16 puts a typical
	// per-file plan in flight in ONE wave while 8 file workers x 16 stays
	// at MaxIdleConnsPerHost=128, the true HTTP/1.1 parallelism ceiling.
	// Only the opt-in planned path reads this; window mode is untouched.
	PlannedFetchMaxInflight int `yaml:"planned_fetch_max_inflight"`

	// PlannedFetchSpanCapBytes caps ONE coalesced span of a plan-then-
	// fetch projected read (default 16MB) — ClickHouse's
	// bytes_per_read_task scope (16 MiB PER read task, NOT per plan).
	// Spans above the cap are SPLIT into cap-sized concurrent GETs; the
	// plan as a whole is admitted via the memory ledger (its absolute
	// ceiling is fi.Size), so plans whose TOTAL exceeds the retired
	// per-plan cap no longer fall back to the window path. Only the
	// opt-in planned path reads this.
	PlannedFetchSpanCapBytes int `yaml:"planned_fetch_span_cap_bytes"`

	// WholeFileThresholdBytes is S*: on the opt-in PLANNED projected-read
	// path, a file whose footer is NOT yet cached and whose size is below
	// this threshold is downloaded WHOLE through the smart-cache path
	// (one GET; the download doubles as the footer-cache warmup via
	// ParseFooterFromData) instead of paying footer-fetch + span RTTs.
	// 0 = per-signal default: logs 5MB, traces 8MB — from the cost model
	// over the live file-size distributions (traces files carry the
	// multi-hundred-KB trace-index footer, shifting the whole-file
	// breakeven higher). Files at or above the threshold fetch the footer
	// (single range read) and plan exact spans. Window mode is untouched.
	WholeFileThresholdBytes int `yaml:"whole_file_threshold_bytes"`

	// FooterPrefetchBytes is the tail range-read size used to prefetch
	// parquet footers (footer cache fills: prefetchFooters,
	// shouldSkipByFooter, fetchFooterFile, the inline open-path fetch).
	// 0 = per-signal default: logs 128KB, traces 640KB. The defaults
	// DIFFER because the footer geometry does: every live traces
	// compacted-L2 footer measures 467-519KB (the trace index lives in
	// footer key-value metadata), so the previous shared 64KB constant
	// could never hold one — traces L2 projected reads always fell back
	// to full downloads (fallback{reason="no-footer"}). 640KB fits them
	// with headroom. Logs footers are ~50KB; 128KB covers footer AND the
	// page-index stripe (ends 91-97KB from EOF on measured files), so one
	// tail GET serves footer + all indexes.
	FooterPrefetchBytes int `yaml:"footer_prefetch_bytes"`
}

// ProjectedFetchMode values for S3Config.ProjectedFetchMode.
const (
	ProjectedFetchModePlanned = "planned"
	ProjectedFetchModeWindow  = "window"
)

// CacheConfig sizes the L1 in-memory and L2 disk caches of footers, blooms and pages.
type CacheConfig struct {
	// MemoryLimit is the deprecated L1 in-memory cache budget, as a size
	// string such as 512MB, used when memory_request and memory_limit_v2 are
	// unset.
	MemoryLimit string `yaml:"memory_limit"`
	// DiskPath is the L2 disk cache directory.
	DiskPath string `yaml:"disk_path"`
	// DiskLimit is the L2 disk cache size, as a size string such as 50GB.
	DiskLimit string `yaml:"disk_limit"`
	// EvictionWatermark is the fraction of disk_limit, in (0, 1], at which L2
	// eviction starts.
	EvictionWatermark float64 `yaml:"eviction_watermark"`
	// FooterTTL is how long a cached Parquet footer stays valid.
	FooterTTL time.Duration `yaml:"footer_ttl"`
	// BloomTTL is how long cached bloom filter data stays valid.
	BloomTTL time.Duration `yaml:"bloom_ttl"`
	// PageTTL is how long a cached Parquet data page stays valid.
	PageTTL time.Duration `yaml:"page_ttl"`
	// WarmupPartitions is the number of recent hourly partitions warmed at
	// startup. Warmup runs only when this or warmup_max_files is set; 0 means
	// 6 when it runs.
	WarmupPartitions int `yaml:"warmup_partitions"`
	// WarmupMaxFiles caps the files warmed at startup. Warmup runs only when
	// this or warmup_partitions is set; 0 means 500 when it runs.
	WarmupMaxFiles int `yaml:"warmup_max_files"`
	// WarmupConcurrency is the number of concurrent startup warmup downloads;
	// 0 means 16.
	WarmupConcurrency int `yaml:"warmup_concurrency"`
	// PartitionMode scopes the peer cache ring: az-local (peers in the pod's
	// availability zone), global (every peer) or distributed.
	PartitionMode string `yaml:"partition_mode"`

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
	// MemoryLimitV2 is the hard ceiling of the L1 in-memory cache budget, as a
	// size string.
	MemoryLimitV2 string `yaml:"memory_limit_v2"`
	// MemoryScaling is the ramp policy from memory_request to memory_limit_v2:
	// fixed, linear or expbackoff. Empty means fixed.
	MemoryScaling string `yaml:"memory_scaling"`
}

// DiscoveryConfig finds the hot storage nodes and the peer fleet.
type DiscoveryConfig struct {
	// HeadlessService is the Kubernetes headless service that resolves the hot
	// VictoriaLogs/VictoriaTraces storage nodes.
	HeadlessService string `yaml:"headless_service"`
	// StorageNodes is a static list of hot storage node addresses, used
	// instead of headless_service.
	StorageNodes []string `yaml:"storage_nodes"`
	// PartitionAuthKey is the auth key sent to the storage nodes'
	// /internal/partition/list endpoint.
	PartitionAuthKey string `yaml:"partition_auth_key"`
	// RefreshInterval is how often the storage nodes and their partitions are
	// refreshed.
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	// Timeout bounds a single storage node discovery request.
	Timeout time.Duration `yaml:"timeout"`
	// PeerHeadlessService is the Kubernetes headless service that resolves the
	// peer fleet for the distributed cache and stats gossip.
	PeerHeadlessService string `yaml:"peer_headless_service"`
	// PeerRefreshInterval is how often peer ring membership is refreshed.
	PeerRefreshInterval time.Duration `yaml:"peer_refresh_interval"`
	// RingStabilizeDuration keeps departed peers in the ring as a shadow set
	// during scaling, so both old and new assignments resolve.
	RingStabilizeDuration time.Duration `yaml:"ring_stabilize_duration"`
	// RingChangeNotify notifies subscribers when ring membership changes.
	RingChangeNotify bool `yaml:"ring_change_notify"`
}

// ManifestConfig controls the index of Parquet files and its local snapshot.
type ManifestConfig struct {
	// RefreshInterval is how often the manifest is fully re-listed from S3.
	RefreshInterval time.Duration `yaml:"refresh_interval"`
	// SQSQueueURL is an SQS queue receiving S3 event notifications for
	// near-real-time manifest updates.
	SQSQueueURL string `yaml:"sqs_queue_url"`
	// SQSRegion is the AWS region of the SQS queue.
	SQSRegion string `yaml:"sqs_region"`
	// SQSWaitTime is the long-poll wait of an SQS receive.
	SQSWaitTime time.Duration `yaml:"sqs_wait_time"`
	// PersistPath is the directory holding the manifest snapshot, index and
	// footer-cache snapshot.
	PersistPath string `yaml:"persist_path"`
	// PersistInterval is how often the manifest snapshot is written to
	// persist_path.
	PersistInterval time.Duration `yaml:"persist_interval"`
}

// PrefetchConfig controls proactive cache warming ahead of queries.
type PrefetchConfig struct {
	// Correlated enables correlated logs-to-traces prefetch by trace_id.
	Correlated bool `yaml:"correlated"`
	// ReadAheadDepth is the number of partitions prefetched ahead of a
	// sequential scan.
	ReadAheadDepth int `yaml:"read_ahead_depth"`
	// MaxConcurrent caps concurrent prefetch downloads.
	MaxConcurrent int `yaml:"max_concurrent"`
	// MaxQueue caps pending prefetch tasks.
	MaxQueue int `yaml:"max_queue"`
}

// PeerConfig controls the distributed peer cache.
type PeerConfig struct {
	// AuthKey is the bearer key protecting the peer cache HTTP endpoints.
	AuthKey string `yaml:"auth_key"`
	// Timeout bounds a single peer cache fetch.
	Timeout time.Duration `yaml:"timeout"`
	// MaxConnections caps HTTP connections per peer.
	MaxConnections int `yaml:"max_connections"`
	// AZAware prefers peers in the pod's own availability zone.
	AZAware bool `yaml:"az_aware"`
	// AZMode is preferred (fall back to other zones) or strict (require
	// az_min_peers_per_az peers in the same zone).
	AZMode string `yaml:"az_mode"`
	// CrossAZFallback allows fetching from another zone when no same-zone peer
	// is available.
	CrossAZFallback bool `yaml:"cross_az_fallback"`
	// AZEnvVar names the environment variable that overrides the detected
	// availability zone.
	AZEnvVar string `yaml:"az_env_var"`
	// AZMinPeersPerAZ is the minimum number of same-zone peers strict mode
	// requires.
	AZMinPeersPerAZ int `yaml:"az_min_peers_per_az"`
}

// StartupConfig controls warmup and readiness when a pod starts.
type StartupConfig struct {
	// ServeStale serves persisted local state before the S3 refresh completes.
	ServeStale bool `yaml:"serve_stale"`
	// WarmupWindow is the age of the recent data whose footers and blooms are
	// pre-cached at startup.
	WarmupWindow time.Duration `yaml:"warmup_window"`
	// MaxWarmupTime bounds startup warmup before the pod reports ready.
	MaxWarmupTime time.Duration `yaml:"max_warmup_time"`
	// PeerSyncTimeout bounds syncing state from peers at startup.
	PeerSyncTimeout time.Duration `yaml:"peer_sync_timeout"`
	// RequireManifestSync requires a manifest sync before the pod reports
	// ready.
	RequireManifestSync bool `yaml:"require_manifest_sync"`
	// StaleThreshold is the age after which persisted state, for example after
	// a volume reattach, is treated as stale.
	StaleThreshold time.Duration `yaml:"stale_threshold"`
	// WALReconciliation reconciles persisted write state against the manifest
	// on a stale start.
	WALReconciliation bool `yaml:"wal_reconciliation"`
	// CacheRevalidation revalidates cache entries on a stale start.
	CacheRevalidation bool `yaml:"cache_revalidation"`
	// MaxResyncTime bounds the stale-state resync before the pod reports ready
	// anyway.
	MaxResyncTime time.Duration `yaml:"max_resync_time"`

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

// ShutdownConfig bounds the phases of a graceful shutdown.
type ShutdownConfig struct {
	// Delay is the pause before the HTTP server stops, so load balancers drain
	// the pod. The Helm chart passes it to -http.shutdownDelay.
	Delay time.Duration `yaml:"delay"`
	// MaxGraceful is the time in-flight HTTP requests get to finish. The Helm
	// chart passes it to -http.maxGracefulShutdownDuration.
	MaxGraceful time.Duration `yaml:"max_graceful_duration"`
	// FlushTimeout bounds flushing buffered rows to S3 during shutdown.
	FlushTimeout time.Duration `yaml:"flush_timeout"`
	// PersistTimeout bounds writing the manifest, footer-cache and stats
	// snapshots during shutdown; 0 means 30s.
	PersistTimeout time.Duration `yaml:"persist_timeout"`
	// ReleaseTimeout bounds notifying peers during shutdown.
	ReleaseTimeout time.Duration `yaml:"release_timeout"`
}

// QueryConfig bounds the cost of select queries.
type QueryConfig struct {
	// MaxConcurrent caps concurrent select queries; a query over the cap gets
	// HTTP 429.
	MaxConcurrent int `yaml:"max_concurrent"`
	// FileWorkers is the deprecated per-query pool of parallel Parquet file
	// readers, used as request and limit when file_workers_request and
	// file_workers_limit are unset.
	FileWorkers int `yaml:"file_workers"`
	// Timeout bounds a single query.
	Timeout time.Duration `yaml:"timeout"`
	// MaxRows is the deprecated per-query row ceiling, used when
	// max_rows_limit is unset.
	MaxRows int64 `yaml:"max_rows"`
	// MaxFilesPerQuery rejects a query that matches more S3 files than this; 0
	// means unlimited, leaving the memory budget as the safety net.
	MaxFilesPerQuery int `yaml:"max_files_per_query"`
	// MaxLiveBytes is a per-query ceiling on the bytes of in-flight
	// DataBlocks currently held by RunQuery before writeBlock has consumed
	// them. When exceeded, the query context is cancelled, returning a
	// partial result instead of OOM-killing the container. 0 means use the
	// default (defaultMaxLiveBytes in storage/parquets3).
	MaxLiveBytes int64 `yaml:"max_live_bytes"`
	// SlowThreshold logs queries that run longer than this.
	SlowThreshold time.Duration `yaml:"slow_threshold"`

	// K8s-style request/limit/scaling for file workers (process-wide
	// concurrent parquet-file readers). Same semantics as
	// S3.ConcurrentDownloads{Request,Limit,Scaling}. When non-zero
	// these take precedence over FileWorkers which becomes a
	// deprecated alias logged once at startup. See
	// internal/resourcebounds.
	FileWorkersRequest int `yaml:"file_workers_request"`
	// FileWorkersLimit is the hard ceiling on concurrent Parquet file readers.
	FileWorkersLimit int `yaml:"file_workers_limit"`
	// FileWorkersScaling is the ramp policy from file_workers_request to
	// file_workers_limit: fixed, linear or expbackoff. Empty means fixed.
	FileWorkersScaling string `yaml:"file_workers_scaling"`

	// K8s-style request/limit/scaling for the per-query row ceiling.
	// MaxRows remains the legacy hard limit (also the current Limit
	// fallback when MaxRowsLimit is zero); MaxRowsRequest is an
	// operator-visible baseline. Streaming hits the ceiling at
	// MaxRowsLimit (or MaxRows if not set) regardless of Request.
	MaxRowsRequest int64 `yaml:"max_rows_request"`
	// MaxRowsLimit is the hard per-query row ceiling.
	MaxRowsLimit int64 `yaml:"max_rows_limit"`
	// MaxRowsScaling is the ramp policy from max_rows_request to
	// max_rows_limit: fixed, linear or expbackoff. Empty means fixed.
	MaxRowsScaling string `yaml:"max_rows_scaling"`
}

// TenantConfig controls multi-tenant routing, isolation and per-tenant overrides.
type TenantConfig struct {
	// DefaultPrefix is a static S3 key prefix that replaces prefix_template.
	DefaultPrefix string `yaml:"default_prefix"`
	// PrefixTemplate is the S3 prefix of a tenant; {AccountID} and {ProjectID}
	// are replaced from the request.
	PrefixTemplate string `yaml:"prefix_template"`
	// Isolation is prefix (tenants share the bucket under prefix_template) or
	// bucket (each tenant gets the bucket named by bucket_template).
	Isolation string `yaml:"isolation"`
	// BucketTemplate names a tenant's bucket in bucket isolation; required
	// when isolation is bucket.
	BucketTemplate string `yaml:"bucket_template"`
	// DefaultAccount is the AccountID of a request that carries no tenant
	// header.
	DefaultAccount string `yaml:"default_account"`
	// DefaultProject is the ProjectID of a request that carries no tenant
	// header.
	DefaultProject string `yaml:"default_project"`
	// HeaderAccount is the HTTP header carrying the AccountID.
	HeaderAccount string `yaml:"header_account"`
	// HeaderProject is the HTTP header carrying the ProjectID.
	HeaderProject string `yaml:"header_project"`
	// GlobalReadHeader is the header that, carrying global_read_value, grants
	// a cross-tenant read.
	GlobalReadHeader string `yaml:"global_read_header"`
	// GlobalReadValue is the value global_read_header must carry.
	GlobalReadValue string `yaml:"global_read_value"`
	// GlobalReadToken is a bearer token that grants a cross-tenant read.
	GlobalReadToken string `yaml:"global_read_token"`
	// KnownTenants lists tenants with their own lifecycle rules and prices,
	// for bucket isolation.
	KnownTenants []KnownTenant `yaml:"known_tenants"`
	// OrgIDHeader is the header carrying a string tenant id (the Loki/Tempo
	// X-Scope-OrgID), resolved through the aliases.
	OrgIDHeader string `yaml:"orgid_header"`
	// MetricsFormat is the tenant label of metrics: id, name or both.
	MetricsFormat string `yaml:"metrics_format"`
	// AutoRegister registers an unknown string tenant id as a new alias.
	AutoRegister bool `yaml:"auto_register"`
	// AliasSyncInterval is how often runtime aliases and tenant policies sync
	// across the fleet.
	AliasSyncInterval time.Duration `yaml:"alias_sync_interval"`
	// Aliases maps string tenant ids to an AccountID and ProjectID.
	Aliases map[string]AliasTarget `yaml:"aliases"`

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

// StatsConfig controls tenant statistics, storage class tracking and cost estimates.
type StatsConfig struct {
	// Enabled collects tenant statistics and syncs them across the fleet.
	Enabled bool `yaml:"enabled"`
	// PushInterval is how often stat deltas are broadcast to peers.
	PushInterval time.Duration `yaml:"push_interval"`
	// PushCompression zstd-compresses delta broadcasts.
	PushCompression bool `yaml:"push_compression"`
	// SnapshotInterval is how often the full registry snapshot is written to
	// S3.
	SnapshotInterval time.Duration `yaml:"snapshot_interval"`
	// SnapshotPrefix is the S3 key prefix of the registry snapshot.
	SnapshotPrefix string `yaml:"snapshot_prefix"`
	// MetaBucket is a dedicated bucket for metadata in bucket isolation; empty
	// uses s3.bucket.
	MetaBucket string `yaml:"meta_bucket"`
	// MaxDeltaCount is the number of deltas after which a full sync is forced.
	MaxDeltaCount int `yaml:"max_delta_count"`
	// MetricsCardinalityLimit caps distinct tenant label values in metrics.
	MetricsCardinalityLimit int `yaml:"metrics_cardinality_limit"`
	// CardinalityWarningThreshold is the field cardinality that raises a
	// high-cardinality warning.
	CardinalityWarningThreshold int `yaml:"cardinality_warning_threshold"`
	// BreakdownLabels are the dimensions offered by the stats breakdown.
	BreakdownLabels []string `yaml:"breakdown_labels"`
	// S3LifecycleRules are the bucket lifecycle rules used to predict storage
	// classes without API calls.
	S3LifecycleRules []LifecycleRuleConfig `yaml:"s3_lifecycle_rules"`
	// S3PricePerGB is the storage price per GB-month of each storage class,
	// for cost estimates.
	S3PricePerGB map[string]float64 `yaml:"s3_price_per_gb"`
	// S3RequestPrices is the price per 1000 requests of each request type, for
	// cost estimates.
	S3RequestPrices map[string]float64 `yaml:"s3_request_prices"`
	// S3InventoryBucket is an S3 Inventory bucket used to verify storage
	// classes exactly.
	S3InventoryBucket string `yaml:"s3_inventory_bucket"`
	// HeadObjectSampleInterval is the interval of HeadObject spot checks near
	// lifecycle transitions.
	HeadObjectSampleInterval time.Duration `yaml:"headobject_sample_interval"`
	// HeadObjectMaxPerRefresh caps HeadObject calls per refresh.
	HeadObjectMaxPerRefresh int `yaml:"headobject_max_per_refresh"`
	// NodeMetaTTL bounds how long a peer's gossiped metadata footprint stays in
	// the fleet view (/stats/instances + the cluster-wide Overview sum) without a
	// refresh. The node id is the (ephemeral) container hostname, so without this
	// dead nodes loaded from the shared S3 snapshot would accumulate forever.
	// Defaults to 3× PushInterval (so a single missed gossip never evicts a live
	// peer); set explicitly to override. 0 disables the staleness filter.
	NodeMetaTTL time.Duration `yaml:"node_meta_ttl"`
}

// UIConfig controls the Lakehouse Explorer web UI.
type UIConfig struct {
	// Enabled serves the Lakehouse Explorer at /lakehouse/ui/.
	Enabled bool `yaml:"enabled"`
	// VMUITab adds a Lakehouse tab to the VictoriaLogs/VictoriaTraces VMUI.
	VMUITab bool `yaml:"vmui_tab"`
	// RefreshDefault is the default auto-refresh interval in seconds; 0
	// disables it.
	RefreshDefault int `yaml:"refresh_default"`
	// Theme is the UI theme: auto, dark or light.
	Theme string `yaml:"theme"`
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

// CompactionConfig controls background Parquet compaction.
type CompactionConfig struct {
	// Enabled runs the compaction scheduler. Every pod runs it; HRW ownership
	// assigns each partition to exactly one pod.
	Enabled bool `yaml:"enabled"`
	// Interval is the compaction scan interval.
	Interval time.Duration `yaml:"interval"`
	// MaxConcurrent is the number of partitions a pod compacts concurrently.
	MaxConcurrent int `yaml:"max_concurrent"`
	// MinFilesL0 is the number of L0 files a partition needs before L0 to L1
	// compaction; at least 2.
	MinFilesL0 int `yaml:"min_files_l0"`
	// MinFilesL1 is the number of L1 files a partition needs before L1 to L2
	// compaction; at least 2.
	MinFilesL1 int `yaml:"min_files_l1"`
	// MinAge keeps files younger than this out of compaction.
	MinAge time.Duration `yaml:"min_age"`
	// DailyRollupAge is the partition age after which L1 files roll up into
	// daily files.
	DailyRollupAge time.Duration `yaml:"daily_rollup_age"`

	// CompressionLevelByOutputLevel sets the zstd level used when
	// emitting a compacted file at output level i (index 0 = L0
	// rewrite, 1 = L0→L1, 2 = L1→L2, ...). The default is a progressive
	// schedule (see Default) — fresh writes optimize for CPU/ingest,
	// while older cold rollups invest more CPU to shrink long-term
	// storage. Out-of-range output levels fall back to the last
	// configured slot, or to Insert.CompressionLevel if the slice is
	// empty.
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

// DeleteConfig controls the delete API, tombstones and file rewrites.
type DeleteConfig struct {
	// Enabled serves the delete API.
	Enabled bool `yaml:"enabled"`
	// DefaultMode is the delete mode of a request that names none: hide,
	// permanent or auto.
	DefaultMode string `yaml:"default_mode"`
	// AutoRewriteClasses are the S3 storage classes whose files auto mode
	// rewrites.
	AutoRewriteClasses []string `yaml:"auto_rewrite_classes"`
	// RewriteDelay is the wait after a tombstone before files are rewritten,
	// so tombstones batch.
	RewriteDelay time.Duration `yaml:"rewrite_delay"`
	// RewriteBatchSize is the number of files per rewrite batch.
	RewriteBatchSize int `yaml:"rewrite_batch_size"`
	// RewriteMaxConcurrent is the number of concurrent rewrite workers.
	RewriteMaxConcurrent int `yaml:"rewrite_max_concurrent"`
	// PersistPath is the directory tombstones persist to.
	PersistPath string `yaml:"persist_path"`
	// CostWarningThreshold is the estimated rewrite cost, in dollars, above
	// which a delete warns.
	CostWarningThreshold float64 `yaml:"cost_warning_threshold"`
	// ForceGlacierHeader is the header that forces rewriting files in Glacier
	// storage classes.
	ForceGlacierHeader string `yaml:"force_glacier_header"`
	// VerifyInterval is how often completed deletes are verified again.
	VerifyInterval time.Duration `yaml:"verify_interval"`
	// LifecycleRules are the bucket lifecycle rules used to predict storage
	// classes for delete cost estimates.
	LifecycleRules []LifecycleRuleConfig `yaml:"lifecycle_rules"`
}

type LifecycleRuleConfig struct {
	TransitionDays int    `yaml:"transition_days" json:"transition_days"`
	StorageClass   string `yaml:"storage_class" json:"storage_class"`
}

// SmartCacheConfig controls how cached data is pinned, aged and sized.
type SmartCacheConfig struct {
	// MaxAge is the maximum age of a cached entry.
	MaxAge time.Duration `yaml:"max_age"`
	// SnapshotInterval is how often cache metadata is persisted to disk.
	SnapshotInterval time.Duration `yaml:"snapshot_interval"`
	// QueryGracePeriod is how long entries stay pinned after their query ends.
	QueryGracePeriod time.Duration `yaml:"query_grace_period"`
	// HotAccessThreshold is the number of accesses within hot_window that
	// marks an entry hot.
	HotAccessThreshold int `yaml:"hot_access_threshold"`
	// HotWindow is the window over which hot accesses are counted.
	HotWindow time.Duration `yaml:"hot_window"`
	// TargetHours is the number of hours of recent data the cache sizes itself
	// to hold.
	TargetHours int `yaml:"target_hours"`
	// DiskLimitMax is the deprecated smart-cache disk budget, as a size
	// string, used when disk_request and disk_limit are unset.
	DiskLimitMax string `yaml:"disk_limit_max"`
	// IngestionRateHint is an ingestion rate such as 500MB that seeds cache
	// sizing; empty detects it.
	IngestionRateHint string `yaml:"ingestion_rate_hint"`

	// K8s-style request/limit/scaling for the smart-cache disk budget.
	// When non-zero, these take precedence over DiskLimitMax which
	// becomes a deprecated alias logged once at startup. Sizes accepted
	// as Go size strings (e.g. "50GB"). See internal/resourcebounds.
	DiskRequest string `yaml:"disk_request"`
	// DiskLimit is the hard ceiling of the smart-cache disk budget, as a size
	// string.
	DiskLimit string `yaml:"disk_limit"`
	// DiskScaling is the ramp policy from disk_request to disk_limit: fixed,
	// linear or expbackoff. Empty means fixed.
	DiskScaling string `yaml:"disk_scaling"`
}

// CrossSignalConfig controls prefetch and eviction hints between the logs and traces lakehouses.
type CrossSignalConfig struct {
	// Enabled sends prefetch and eviction hints to the other signal's
	// lakehouse.
	Enabled bool `yaml:"enabled"`
	// Endpoint is the URL of the other signal's lakehouse.
	Endpoint string `yaml:"endpoint"`
	// HeadlessService is the Kubernetes headless service that resolves the
	// other signal's pods, used instead of endpoint.
	HeadlessService string `yaml:"headless_service"`
	// AuthKey is the shared key of cross-signal requests.
	AuthKey string `yaml:"auth_key"`
	// Timeout bounds a cross-signal request.
	Timeout time.Duration `yaml:"timeout"`
	// MaxBatch is the number of trace ids per hint batch.
	MaxBatch int `yaml:"max_batch"`
	// BatchInterval is how often hint batches are sent.
	BatchInterval time.Duration `yaml:"batch_interval"`
}

// RetentionConfig controls automatic deletion of expired data.
type RetentionConfig struct {
	// Enabled deletes data older than its retention period.
	Enabled bool `yaml:"enabled"`
	// Default is the retention period of data no rule matches, as a duration
	// such as 90d.
	Default string `yaml:"default"`
	// CheckInterval is how often expired data is looked for, as a duration
	// such as 1h.
	CheckInterval string `yaml:"check_interval"`
	// Rules are per-stream retention periods: a label match and a keep
	// duration.
	Rules []RetentionRule `yaml:"rules"`
}

type RetentionRule struct {
	Match map[string]string `yaml:"match"`
	Keep  string            `yaml:"keep"`
}

// RoleProfileRef selects a profile for one role of a signal.
type RoleProfileRef struct {
	// Profile is the profile for this role (insert or select) of the signal;
	// it takes precedence over the signal's profile.
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
			Region:                   "us-east-1",
			MaxConnections:           128,
			Timeout:                  30 * time.Second,
			RetryMax:                 3,
			RetryBaseDelay:           200 * time.Millisecond,
			MaxConcurrentDownloads:   16,
			ReadAheadBytes:           2 * 1024 * 1024, // 2MB base window
			CoalesceGapBytes:         1024 * 1024,     // 1MB (BDP-priced; was 64KB)
			ReadAheadMaxBytes:        8 * 1024 * 1024, // 8MB adaptive ceiling
			ReadAheadWasteThreshold:  0.5,             // shrink window when >50% of it was never read
			ReadBufferSize:           1024 * 1024,     // 1MB parquet page read buffer
			ParquetReadMode:          "async",
			ProjectedFetchMode:       ProjectedFetchModeWindow,
			ProjectedFetchMaxBytes:   16 * 1024 * 1024, // DEPRECATED (per-plan cap retired; kept parsed)
			PlannedFetchMaxInflight:  16,               // min(16, spans) concurrent span GETs per file
			PlannedFetchSpanCapBytes: 16 * 1024 * 1024, // 16MB per-SPAN cap (CH bytes_per_read_task)
			// WholeFileThresholdBytes / FooterPrefetchBytes: 0 = per-signal
			// defaults resolved in the storage layer (logs 5MB/128KB,
			// traces 8MB/640KB — see the field comments above).
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
			Enabled:          true,
			PushInterval:     30 * time.Second,
			PushCompression:  true,
			SnapshotInterval: 5 * time.Minute,
			SnapshotPrefix:   "_meta/tenant-stats",
			// 3× PushInterval: a live peer re-gossips every PushInterval, so a
			// node is only aged out after ~3 consecutive missed pushes — long
			// enough to ride out a transient hiccup, short enough that a recreated
			// container's old hostname disappears within a couple of minutes.
			NodeMetaTTL:                 90 * time.Second,
			MaxDeltaCount:               1000,
			MetricsCardinalityLimit:     100,
			CardinalityWarningThreshold: 10000,
			// Mode-neutral dimensions present in both logs and traces (severity_text
			// is logs-only and would render an empty block on traces — add it via the
			// breakdown search box when on logs).
			BreakdownLabels: []string{"deployment.environment", "service.name", "k8s.namespace.name", "k8s.cluster.name", "k8s.deployment.name"},
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

	cfg, err := loadConfigBytes(data, mode, role)
	if err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}
	return cfg, nil
}

// loadConfigBytes is LoadWithMode for an already-read config file: the
// file's `lakehouse:` document is merged over the profile it selects.
func loadConfigBytes(data []byte, mode Mode, role Role) (*Config, error) {
	var wrapper struct {
		Lakehouse Config `yaml:"lakehouse"`
	}
	if err := yaml.Unmarshal(data, &wrapper); err != nil {
		return nil, err
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
	if c.S3.PlannedFetchMaxInflight < 0 {
		return fmt.Errorf("--lakehouse.s3.planned-fetch-max-inflight must be >= 0, got %d", c.S3.PlannedFetchMaxInflight)
	}
	if c.S3.PlannedFetchSpanCapBytes < 0 {
		return fmt.Errorf("--lakehouse.s3.planned-fetch-span-cap-bytes must be >= 0, got %d", c.S3.PlannedFetchSpanCapBytes)
	}
	if c.S3.WholeFileThresholdBytes < 0 {
		return fmt.Errorf("--lakehouse.s3.whole-file-threshold-bytes must be >= 0, got %d", c.S3.WholeFileThresholdBytes)
	}
	if c.S3.FooterPrefetchBytes < 0 {
		return fmt.Errorf("--lakehouse.s3.footer-prefetch-bytes must be >= 0, got %d", c.S3.FooterPrefetchBytes)
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
	if err := validateTenantPrefixTemplate(c.Tenant.PrefixTemplate); err != nil {
		return err
	}

	if c.HotBoundary != "" {
		if err := validateDuration(c.HotBoundary); err != nil {
			return fmt.Errorf("--lakehouse.hot-boundary: invalid duration %q: %w", c.HotBoundary, err)
		}
	}

	return nil
}

// validateTenantPrefixTemplate rejects a tenant prefix template the writer and
// the manifest cannot agree on. Object keys carry the tenant as the two leading
// segments, {AccountID}/{ProjectID}: that is what the writer expands, what the
// manifest parses back, and what the read path checks an object against.
//
// A template that leaves a placeholder unexpanded ({OrgID}, say) writes every
// tenant under one static prefix — objects with no tenant segment, which belong
// to tenant 0:0, so no other tenant could read its own rows back. A template
// with only one of the two segments is just as broken: the signal directory
// ("logs/", "traces/") is then parsed as the missing segment. An empty template
// is the single-tenant (legacy) layout and stays valid.
func validateTenantPrefixTemplate(tmpl string) error {
	if tmpl == "" {
		return nil
	}
	const guidance = "object keys carry the tenant as {AccountID}/{ProjectID}/, which is what the writer expands and the read path checks; " +
		"string OrgIDs are presentation-only and map to an account/project pair (see docs/multi-tenancy.md#tenant-name-mapping-x-scope-orgid)"
	if !strings.Contains(tmpl, "{AccountID}") || !strings.Contains(tmpl, "{ProjectID}") {
		return fmt.Errorf("--lakehouse.tenant.prefix-template %q must contain both {AccountID} and {ProjectID}: %s", tmpl, guidance)
	}
	if rest := strings.NewReplacer("{AccountID}", "", "{ProjectID}", "").Replace(tmpl); strings.ContainsAny(rest, "{}") {
		return fmt.Errorf("--lakehouse.tenant.prefix-template %q carries a placeholder the writer does not expand (only {AccountID} and {ProjectID} are expanded): %s", tmpl, guidance)
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
	if overlay.S3.PlannedFetchMaxInflight > 0 {
		base.S3.PlannedFetchMaxInflight = overlay.S3.PlannedFetchMaxInflight
	}
	if overlay.S3.PlannedFetchSpanCapBytes > 0 {
		base.S3.PlannedFetchSpanCapBytes = overlay.S3.PlannedFetchSpanCapBytes
	}
	if overlay.S3.WholeFileThresholdBytes > 0 {
		base.S3.WholeFileThresholdBytes = overlay.S3.WholeFileThresholdBytes
	}
	if overlay.S3.FooterPrefetchBytes > 0 {
		base.S3.FooterPrefetchBytes = overlay.S3.FooterPrefetchBytes
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
	if overlay.Stats.NodeMetaTTL > 0 {
		base.Stats.NodeMetaTTL = overlay.Stats.NodeMetaTTL
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
