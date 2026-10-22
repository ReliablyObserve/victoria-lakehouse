package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/azdetect"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/crosssignal"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/lifecycle"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/prefetch"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/retention"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selectapi"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/startup"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/telemetry"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/ui"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// vlCompat is the VictoriaLogs release this binary embeds, reported on
// /lakehouse/info and in the startup log so an operator can tell which upstream
// a running node speaks without reading go.mod. It must equal the version the
// root go.mod requires; TestVLCompatMatchesGoMod fails the build otherwise,
// because a stale value here misreports compatibility to every client that asks.
const vlCompat = "1.53.0"

var (
	configPath      = flag.String("lakehouse.config", "", "Path to YAML config file")
	s3Bucket        = flag.String("lakehouse.s3.bucket", "", "S3 bucket name (required)")
	s3Region        = flag.String("lakehouse.s3.region", "", "S3 region")
	s3Prefix        = flag.String("lakehouse.s3.prefix", "", "S3 key prefix")
	s3Endpoint      = flag.String("lakehouse.s3.endpoint", "", "Custom S3 endpoint (MinIO)")
	s3AccessKey     = flag.String("lakehouse.s3.access-key", "", "S3 access key")
	s3SecretKey     = flag.String("lakehouse.s3.secret-key", "", "S3 secret key")
	s3PathStyle     = flag.Bool("lakehouse.s3.force-path-style", false, "Use path-style S3 URLs")
	topology        = flag.String("lakehouse.topology", "", "Deployment topology: auto, storage-node, direct, loki-proxy")
	hotBoundary     = flag.String("lakehouse.hot-boundary", "", "Manual hot boundary override (e.g., 7d)")
	role            = flag.String("lakehouse.role", "", "Role: all, insert, select (default: all)")
	profileFlag     = flag.String("lakehouse.profile", "", "Configuration profile: balanced, max-performance, max-durability, max-cost-savings, dev. Applied under the already-loaded config, so only keys left at zero take the profile value; prefer profile: in the config file")
	listenAddrFlag  = flag.String("httpListenAddr", ":9428", "HTTP listen address")
	manifestRefresh = flag.Duration("lakehouse.manifest.refresh-interval", 0, "Manifest refresh interval (e.g., 30s)")

	cacheMemoryMB         = flag.Int("lakehouse.cache.memory-mb", 0, "L1 memory cache size in MB (default: 512)")
	cacheDiskPath         = flag.String("lakehouse.cache.disk-path", "", "L2 disk cache directory path")
	cacheDiskMB           = flag.Int("lakehouse.cache.disk-max-mb", 0, "L2 disk cache max size in MB (default: 51200)")
	cacheFooterMaxBytes   = flag.Int("lakehouse.cache.footer-max-bytes", 0, "Byte budget of the Parquet footer cache: footers are cached with their page index so a cached file opens with no S3 round trip; least-recently-used entries are evicted to stay within the budget (0 = auto: 10% of the memory the process may use for caches, clamped to 32 MiB..1 GiB)")
	cacheWarmupPartitions = flag.Int("lakehouse.cache.warmup-partitions", 0, "Number of recent hourly partitions to warm on startup; warmup runs only when this or -lakehouse.cache.warmup-max-files is set (default: 0 = 6 when warmup runs)")
	cacheWarmupMaxFiles   = flag.Int("lakehouse.cache.warmup-max-files", 0, "Max files to warm on startup; warmup runs only when this or -lakehouse.cache.warmup-partitions is set (default: 0 = 500 when warmup runs)")
	cachePartitionMode    = flag.String("lakehouse.cache.partition-mode", "", "Cache partition mode: az-local (default), global, distributed")

	compactionEnabled        = flag.Bool("lakehouse.compaction.enabled", false, "Enable the compaction scheduler (on by default; false does not turn it off — select a profile that disables compaction in the config file)")
	compactionInterval       = flag.Duration("lakehouse.compaction.interval", 0, "Compaction scan interval")
	compactionDailyRollupAge = flag.Duration("lakehouse.compaction.daily-rollup-age", 0, "Partition age after which a tenant's files in an hour (each under 32 MiB) merge into one (default: 24h)")
	compactionRowGroupSizes  = flag.String("lakehouse.compaction.row-group-size-by-output-level", "", "Comma-separated Parquet row-group sizes per compaction output level, slot N = output level N (default: 10000,10000,20000)")

	queryFileWorkers      = flag.Int("lakehouse.query.file-workers", 0, "Number of parallel file workers for queries (default: 64)")
	queryMaxFilesPerQuery = flag.Int("lakehouse.query.max-files-per-query", 0, "Max S3 files per query before rejection (default: 0 = unlimited)")
	queryMaxLiveBytes     = flag.Int64("lakehouse.query.max-live-bytes", 0, "Per-query ceiling on in-flight DataBlock bytes before cancellation (default: 512MiB)")

	s3ReadAhead       = flag.Int("lakehouse.s3.read-ahead-bytes", 0, "S3 read-ahead base window in bytes (default: 2MB)")
	s3CoalesceGap     = flag.Int("lakehouse.s3.coalesce-gap-bytes", 0, "Merge S3 range reads with gaps smaller than this (default: 1MB)")
	s3ReadAheadMax    = flag.Int("lakehouse.s3.read-ahead-max-bytes", 0, "Adaptive read-ahead window ceiling in bytes; the window doubles from read-ahead-bytes on sequential scans (default: 8MB)")
	s3ReadAheadWaste  = flag.Float64("lakehouse.s3.read-ahead-waste-threshold", 0, "Waste-feedback threshold for the adaptive read-ahead window: when an evicted window had more than this fraction of its bytes never read, the next window halves toward read-ahead-bytes instead of growing; >=1 disables (default: 0.5)")
	s3ReadBufferSize  = flag.Int("lakehouse.s3.read-buffer-size", 0, "Parquet page read buffer for ranged S3 opens in bytes (default: 1MB)")
	s3ParquetReadMode = flag.String("lakehouse.s3.parquet-read-mode", "", "Parquet page read mode on ranged S3 opens: sync (column readers take turns on the read-ahead window) or async (read-ahead goroutine per column) (default: sync)")

	s3ProjectedFetchMode      = flag.String("lakehouse.s3.projected-fetch-mode", "", "Read strategy for column-projected parquet reads: planned (plan-then-fetch exact coalesced column-chunk ranges, no speculative window) or window (adaptive read-ahead window — deprecated fallback, removed in the next release) (default: planned)")
	s3ProjectedFetchMaxBytes  = flag.Int("lakehouse.s3.projected-fetch-max-bytes", 0, "DEPRECATED: the per-plan cap is retired (kept parsed for compatibility; plans are admitted via the memory ledger and capped per-SPAN by lakehouse.s3.planned-fetch-span-cap-bytes)")
	s3PlannedFetchMaxInflight = flag.Int("lakehouse.s3.planned-fetch-max-inflight", 0, "Concurrent span GETs per file on the planned projected-read path: min(k, spans) in flight (default: 16)")
	s3PlannedFetchSpanCap     = flag.Int("lakehouse.s3.planned-fetch-span-cap-bytes", 0, "Per-SPAN byte cap on the planned projected-read path; coalesced spans above it are split into cap-sized concurrent GETs (CH bytes_per_read_task scope; default: 16MB)")
	s3WholeFileThreshold      = flag.Int("lakehouse.s3.whole-file-threshold-bytes", 0, "S*: on the planned projected-read path, a cold-footer file below this size is downloaded whole (the download warms the footer cache) instead of footer-fetch + spans (default: 5MB logs / 8MB traces)")
	s3FooterPrefetchBytes     = flag.Int("lakehouse.s3.footer-prefetch-bytes", 0, "Tail range-read size for parquet footer prefetch/fetch (default: 128KB logs / 640KB traces — traces L2 footers carry a 467-519KB trace index)")

	// K8s-style request/limit/scaling for S3 download concurrency
	// (see internal/resourcebounds). When any of these are non-zero
	// they take precedence over the deprecated lakehouse.s3.max-concurrent-downloads
	// flag. Triple defaults: request=4, limit=16, scaling=fixed.
	s3ConcurrentDownloadsRequest = flag.Int("lakehouse.s3.concurrent-downloads.request", 0, "S3 download concurrency request (always-reserved baseline; default: 0 = limit/4 when the limit is at least 8, else the limit)")
	s3ConcurrentDownloadsLimit   = flag.Int("lakehouse.s3.concurrent-downloads.limit", 0, "S3 download concurrency limit (hard ceiling; default: 0 = the request, or -lakehouse.s3.max-concurrent-downloads when both are unset)")
	s3ConcurrentDownloadsScaling = flag.String("lakehouse.s3.concurrent-downloads.scaling", "", "S3 download concurrency scaling policy: fixed|linear|expbackoff (empty means fixed)")
	// DEPRECATED: superseded by the request/limit/scaling triple above.
	// Setting this flag alone still works (logged as deprecation warning
	// at startup); the value is taken as both request and limit
	// (flat behaviour). Will be removed in v1.0.
	s3MaxConcurrentDownloads = flag.Int("lakehouse.s3.max-concurrent-downloads", 0, "DEPRECATED: use -lakehouse.s3.concurrent-downloads.{request,limit,scaling}. Flat S3 download concurrency (default: 16).")

	// K8s-style request/limit/scaling for query.file_workers.
	queryFileWorkersRequest = flag.Int("lakehouse.query.file-workers.request", 0, "Query file-workers request (always-reserved baseline)")
	queryFileWorkersLimit   = flag.Int("lakehouse.query.file-workers.limit", 0, "Query file-workers limit (hard ceiling)")
	queryFileWorkersScaling = flag.String("lakehouse.query.file-workers.scaling", "", "Query file-workers scaling policy: fixed|linear|expbackoff")

	// K8s-style request/limit/scaling for cache.memory (in-memory L1).
	cacheMemoryRequest = flag.String("lakehouse.cache.memory.request", "", "Cache memory request (Go size string, e.g. 64MB)")
	cacheMemoryLimit   = flag.String("lakehouse.cache.memory.limit", "", "Cache memory limit (Go size string, e.g. 256MB)")
	cacheMemoryScaling = flag.String("lakehouse.cache.memory.scaling", "", "Cache memory scaling policy: fixed|linear|expbackoff")

	// K8s-style request/limit/scaling for smart_cache.disk.
	smartCacheDiskRequest = flag.String("lakehouse.smart-cache.disk.request", "", "Smart-cache disk request (Go size string)")
	smartCacheDiskLimit   = flag.String("lakehouse.smart-cache.disk.limit", "", "Smart-cache disk limit (Go size string)")
	smartCacheDiskScaling = flag.String("lakehouse.smart-cache.disk.scaling", "", "Smart-cache disk scaling policy: fixed|linear|expbackoff")

	// K8s-style request/limit/scaling for query.max_rows.
	queryMaxRowsRequest = flag.Int64("lakehouse.query.max-rows.request", 0, "Query max-rows request (operator-visible baseline)")
	queryMaxRowsLimit   = flag.Int64("lakehouse.query.max-rows.limit", 0, "Query max-rows limit (hard ceiling)")
	queryMaxRowsScaling = flag.String("lakehouse.query.max-rows.scaling", "", "Query max-rows scaling policy: fixed|linear|expbackoff")

	logsBloomColumns = flag.String("lakehouse.logs.bloom-columns", "", "Comma-separated bloom filter columns for logs (default: service.name,trace_id)")
	logsDeletePrefix = flag.String("lakehouse.logs.delete-prefix", "", "Delete API prefix (default: /delete/logsql)")

	tenantDefaultAccount    = flag.String("lakehouse.tenant.default-account", "", "Default tenant account ID (default: 0)")
	tenantDefaultProject    = flag.String("lakehouse.tenant.default-project", "", "Default tenant project ID (default: 0)")
	tenantHeaderAccount     = flag.String("lakehouse.tenant.header-account", "", "HTTP header for account ID (default: X-Scope-AccountID)")
	tenantHeaderProject     = flag.String("lakehouse.tenant.header-project", "", "HTTP header for project ID (default: X-Scope-ProjectID)")
	tenantGlobalHeader      = flag.String("lakehouse.tenant.global-read-header", "", "Header name for global read access")
	tenantGlobalValue       = flag.String("lakehouse.tenant.global-read-value", "", "Expected header value for global read access")
	tenantGlobalToken       = flag.String("lakehouse.tenant.global-read-token", "", "Bearer token for global read access")
	tenantIsolation         = flag.String("lakehouse.tenant.isolation", "", "Tenant isolation mode: prefix or bucket")
	tenantPrefixTemplate    = flag.String("lakehouse.tenant.prefix-template", "", "S3 prefix template (default: {AccountID}/{ProjectID}/)")
	tenantBucketTemplate    = flag.String("lakehouse.tenant.bucket-template", "", "Bucket name template for bucket isolation")
	tenantDefaultPrefix     = flag.String("lakehouse.tenant.default-prefix", "", "Static S3 key prefix override")
	tenantOrgIDHeader       = flag.String("lakehouse.tenant.orgid-header", "", "HTTP header for string tenant ID (default: X-Scope-OrgID)")
	tenantMetricsFormat     = flag.String("lakehouse.tenant.metrics-format", "", "Prometheus tenant label format: id, name, both (default: id)")
	tenantAutoRegister      = flag.Bool("lakehouse.tenant.auto-register", false, "Auto-register unknown X-Scope-OrgID tenants on write paths (insert/OTLP/native). Reads never register")
	tenantAutoRegisterMin   = flag.Uint("lakehouse.tenant.auto-register-min-id", 0, "First AccountID auto-registration may allocate; configured aliases must stay outside [min,max] (default: 2147483648)")
	tenantAutoRegisterMax   = flag.Uint("lakehouse.tenant.auto-register-max-id", 0, "Last AccountID auto-registration may allocate; must be below 4294967295 (default: 4294967294)")
	tenantAliasSyncInterval = flag.Duration("lakehouse.tenant.alias-sync-interval", 0, "Fleet sync interval for runtime aliases (default: 30s)")
	tenantAliases           = flag.String("lakehouse.tenant.alias", "", "Static tenant aliases: comma-separated orgid:account:project (e.g. acme-corp:1001:0,staging-team:1002:0). Re-applied every startup as the reconstruction baseline; merged with S3-persisted runtime aliases.")

	pmetaEnabled       = flag.Bool("lakehouse.pmeta.enabled", true, "Unified partition-metadata layer (catalog + file-meta + bloom facets). Disabling is a degraded mode: no metadata for new files")
	pmetaAlwaysSketch  = flag.String("lakehouse.pmeta.always-sketch-fields", "", "Comma-separated id columns to sketch instead of enumerate (e.g. trace_id,span_id)")
	pmetaCardThreshold = flag.Int("lakehouse.pmeta.cardinality-threshold", 0, "Per-field distinct-value cap before a field is high-card (0 = default 50000)")
	pmetaRefuseSketch  = flag.Bool("lakehouse.pmeta.refuse-sketch-enumeration", false, "Return empty for always-sketch field_values instead of scanning")
)

func main() {
	// Operator subcommands. Kept ahead of buildinfo/envflag so they don't
	// require full lakehouse config to run inside a container's HEALTHCHECK
	// or supply-chain verifier.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			runHealthcheckSubcommand()
			return
		case "fips-status":
			runFIPSStatusSubcommand()
			return
		case "print-default-config", "-print-default-config", "--print-default-config":
			runPrintDefaultConfigSubcommand()
			return
		}
	}
	buildinfo.Init()
	envflag.Parse()

	logger.InitNoLogFlags()
	if err := checkUpstreamFlagsHonoured(flag.CommandLine); err != nil {
		logger.Fatalf("%s", err)
	}
	vlMemoryAllowed := memory.Allowed()

	// Tell Go's GC the soft memory ceiling. Without this, transient
	// allocation peaks during a wildcard scan can push RSS over the
	// cgroup limit before GC reclaims them — the kernel OOM-kills the
	// container even when inuse_space is well below the cap. Setting
	// GOMEMLIMIT to memory.Allowed() (60% of the cgroup limit by
	// default) makes GC aggressive enough to keep RSS inside the cap,
	// while leaving the remaining 40% as headroom for the kernel page
	// cache, network/disk buffers, parquet-go internal pools, and Go
	// runtime overhead. See https://pkg.go.dev/runtime/debug#SetMemoryLimit
	// and https://tip.golang.org/doc/gc-guide#Memory_limit.
	prevLimit := debug.SetMemoryLimit(int64(vlMemoryAllowed))
	logger.Infof("Go GC memory limit set to %d bytes (was %d); cgroup_memory_limit≈%d/0.6 bytes",
		vlMemoryAllowed, prevLimit, vlMemoryAllowed)

	logger.Infof("lakehouse-logs starting; vl_compat=%s, memory_allowed_bytes=%d", vlCompat, vlMemoryAllowed)

	cfg, err := config.LoadWithMode(*configPath, config.ModeLogs, config.Role(*role))
	if err != nil {
		logger.Errorf("failed to load config: %s", err)
		os.Exit(1)
	}

	applyFlags(cfg)

	if err := cfg.Validate(); err != nil {
		logger.Errorf("invalid config: %s", err)
		os.Exit(1)
	}

	addr := *listenAddrFlag
	if addr == ":9428" && cfg.ListenAddr() != "" && cfg.ListenAddr() != ":9428" {
		addr = cfg.ListenAddr()
	}

	logger.Infof("starting lakehouse-logs; version=%s, role=%s, topology=%s, listen=%s, s3_bucket=%s, s3_prefix=%s",
		buildinfo.Version, cfg.Role, cfg.Topology, addr, cfg.S3.Bucket, cfg.AutoPrefix())

	peerauth.SetMaxBodyBytes(cfg.Peer.MaxRequestBodyBytes)
	peerauth.Report(cfg.Peer.AuthEnabled, cfg.Peer.AuthKey != "", cfg.Discovery.PeerHeadlessService != "" || cfg.Role == config.RoleInsert || cfg.Role == config.RoleSelect)

	run(cfg, addr)
}

func run(cfg *config.Config, addr string) {
	sm := startup.NewManager(cfg.Startup.MinManifestFiles)

	store, err := parquets3.New(cfg)
	if err != nil {
		logger.Errorf("failed to initialize storage: %s", err)
		os.Exit(1)
	}

	shutdownTelemetry, err := telemetry.Init(context.Background(), cfg.Telemetry, "lakehouse-logs")
	if err != nil {
		logger.Errorf("telemetry init failed: %s", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTelemetry(context.Background()) }()

	selfAZ := azdetect.Detect(context.Background(), azdetect.Options{
		EnvVar:  cfg.Peer.AZEnvVar,
		Timeout: 2 * time.Second,
	})
	if selfAZ != "" {
		logger.Infof("detected AZ: %s", selfAZ)
		store.SetSelfAZ(selfAZ)
	}

	// Register self as the buffer-bridge fallback endpoint so single-
	// node deployments (no peer discovery, no other insert pods) still
	// see their own unflushed buffer on cold-tier queries. Once real
	// peers appear via DNS discovery, BufferBridge.getQueryEndpoints
	// silently switches over and the self entry stays out of the way.
	if cfg.Role == config.RoleAll && store.BufferBridge() != nil {
		store.BufferBridge().SetSelfEndpoint("http://localhost" + addr)
	}

	// Wire stats callback after writer is initialized but before heavy use.
	// We derive the tenant key from the configured prefix (e.g. "0/0/logs/" → "0:0").
	writerTenantKey := deriveTenantKey(cfg.AutoPrefix())

	var pusher *manifest.Pusher
	if cfg.Discovery.PeerHeadlessService != "" {
		disc := store.Discovery()
		pusher = manifest.NewPusher(manifest.PusherConfig{
			GetPeers:   func() []string { return disc.GetPeers() },
			AuthSecret: cfg.Peer.AuthKey,
			SelfAddr:   addr,
		})
	}

	// tenantPolicyHolder is set further down (line ~410) after the
	// resolver wiring. setupCompaction's per-tenant compression
	// lookup is a closure that reads this variable at call time —
	// by the first compaction tick (Compaction.Interval default
	// 5 min) the holder is always populated, so the closure
	// returns real overrides instead of nil-no-op.
	tombstoneStore := delete.NewTombstoneStore()
	tombstoneRet := &tombstoneRetention{}
	// The view of the deletes is read from the bucket by every pod: nothing a pod
	// holds locally is a source of truth, and the optional cache directory only
	// saves reading the (immutable) objects again after a restart.
	if err := attachTombstones(cfg, store.Pool(), tombstoneStore, tombstoneRet); err != nil {
		logger.Fatalf("tombstone view: %s", err)
	}
	store.SetTombstoneStore(tombstoneStore)

	// The detector carries the delete lifecycle rules (per-tenant overrides are
	// installed later on the same pointer); compaction reads it to keep away from
	// objects S3 lifecycle has moved out of STANDARD.
	detector := newStorageClassDetector(cfg)

	var tenantPolicyHolder *tenant.PolicyRegistry
	sched, sweep, stopCompaction := setupCompaction(cfg, store, pusher, addr, &tenantPolicyHolder, detector, selfaddr.SharedSystemEnv())
	if stopCompaction != nil {
		defer stopCompaction()
	}
	_ = sweep

	// --- Tenant resolver ---
	resolverCfg := tenant.ResolverConfig{
		MetricsFormat: tenant.ParseMetricsFormat(cfg.Tenant.MetricsFormat),
		AutoRegister:  cfg.Tenant.AutoRegister,
		OrgIDHeader:   cfg.Tenant.OrgIDHeader,
	}
	resolver := tenant.NewResolver(resolverCfg)

	if len(aliasFlagConflicts) > 0 {
		logger.Errorf("invalid tenant aliases: %s", strings.Join(aliasFlagConflicts, "; "))
		os.Exit(1)
	}
	autoRange := tenant.AutoRange{Min: cfg.Tenant.AutoRegisterMinID, Max: cfg.Tenant.AutoRegisterMaxID}
	configured := make([]tenant.AliasEntry, 0, len(cfg.Tenant.Aliases))
	for orgID, target := range cfg.Tenant.Aliases {
		configured = append(configured, tenant.AliasEntry{OrgID: orgID, AccountID: target.AccountID, ProjectID: target.ProjectID, Source: tenant.SourceConfig})
	}
	if err := tenant.ValidateConfiguredAliases(configured, autoRange); err != nil {
		logger.Errorf("invalid tenant aliases: %s", err)
		os.Exit(1)
	}
	for _, ae := range configured {
		if err := resolver.AddAliasFrom(ae.OrgID, tenant.TenantID{AccountID: ae.AccountID, ProjectID: ae.ProjectID}, tenant.SourceConfig); err != nil {
			logger.Errorf("invalid tenant alias %q: %s", ae.OrgID, err)
			os.Exit(1)
		}
	}

	// The alias registry is one S3 object updated only with conditional
	// writes: the single allocator across pods and restarts. Auto-registered
	// tenants get AccountIDs from the reserved range, never one a configured
	// alias, a persisted alias or an int tenant with data holds.
	aliasRegistry, err := tenant.NewRegistry(resolver, tenant.RegistryConfig{
		Pool:        store.Pool(),
		Key:         cfg.AutoPrefix() + "_meta/tenant-aliases.json",
		Range:       autoRange,
		DataTenants: store.DataTenantAccountIDs,
	})
	if err != nil {
		logger.Errorf("invalid tenant auto-register range: %s", err)
		os.Exit(1)
	}
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := aliasRegistry.Sync(syncCtx); err != nil {
		logger.Warnf("failed to load tenant aliases from S3: %s", err)
	}
	syncCancel()

	if resolver.HasAliases() {
		logger.Infof("tenant resolver active; aliases=%d, metrics_format=%s, auto_register=%v",
			len(resolver.AllAliases()), cfg.Tenant.MetricsFormat, cfg.Tenant.AutoRegister)
	}

	store.Manifest().SetPrefixTemplate(cfg.Tenant.PrefixTemplate)
	// "logs/" keeps the tenant-scoped manifest refresh inside the
	// per-tier S3 prefix; without it a logs pod also LISTs
	// `<tenant>/traces/` and the trace parquets' columns leak into
	// `/select/logsql/field_names` and Grafana drilldown.
	store.Manifest().SetSignalSuffix("logs/")

	// --- Tenant stats ---
	registry := stats.NewTenantRegistry(hostname())
	// Age out dead nodes: the node id is the ephemeral container hostname, so
	// stale entries loaded from the shared S3 snapshot must expire from the fleet
	// view (/stats/instances + the cluster-wide Overview sum) once they stop
	// gossiping. Self is always kept fresh by the per-tick SetNodeMeta.
	registry.SetNodeMetaTTL(cfg.Stats.NodeMetaTTL)

	if w := store.Writer(); w != nil {
		fallback := writerTenantKey
		w.SetStatsCallback(func(accountID, projectID uint32, compressedBytes, rawBytes, rows int64, storageClass string) {
			key := tenantStatsKey(accountID, projectID, fallback)
			registry.RecordWrite(key, compressedBytes, rawBytes, rows, storageClass)
		})
		if pf := tenantPrefixResolver(cfg); pf != nil {
			w.SetTenantPrefix(pf)
		}
	}

	// Load snapshot from S3 if configured.
	if cfg.Stats.Enabled && cfg.Stats.SnapshotPrefix != "" {
		snapshotBucket := cfg.Stats.MetaBucket
		if snapshotBucket == "" {
			snapshotBucket = cfg.S3.Bucket
		}
		_ = snapshotBucket // bucket selection for future multi-bucket support
		snapshotKey := cfg.AutoPrefix() + cfg.Stats.SnapshotPrefix + "/snapshot.json"
		s3Pool := store.Pool()
		data, err := s3Pool.Download(context.Background(), snapshotKey)
		if err == nil && len(data) > 0 {
			if err := registry.LoadSnapshot(hostname(), data); err != nil {
				logger.Warnf("failed to load stats snapshot: %s", err)
			} else {
				logger.Infof("loaded stats snapshot from S3; tenants=%d", registry.TenantCount())
			}
		}
	}

	cardLimiter := stats.NewCardinalityLimiter(cfg.Stats.MetricsCardinalityLimit)

	classTracker := stats.NewStorageClassTracker(cfg.Stats.S3LifecycleRules, nil)

	costCalc := stats.NewCostCalculator(cfg.Stats.S3PricePerGB, cfg.Stats.S3RequestPrices)

	stopCh := make(chan struct{})
	defer close(stopCh)

	// Every node keeps its view of the deletes current by listing the tombstone
	// keys created since its last look; reads fail closed when it cannot.
	startTombstoneSync(tombstoneStore, stopCh)

	if cancel := startTenantAliasSync(cfg, store, resolver, addr); cancel != nil {
		defer cancel()
	}

	// Per-tenant policy overrides. Pending alias-keyed entries get
	// re-resolved on the same cadence as alias sync so late-registered
	// tenants pick up their overrides without a restart.
	policy, err := tenant.NewPolicyRegistry(cfg.Tenant.Overrides, resolver)
	if err != nil {
		logger.Fatalf("tenant overrides invalid: %s", err)
	}
	if pending := policy.PendingAliases(); len(pending) > 0 {
		logger.Infof("tenant overrides pending alias resolution: %v", pending)
	}
	startTenantPolicyRefresh(cfg, policy, stopCh)
	startTenantAliasPersist(cfg, aliasRegistry, stopCh)
	startPeerDiscovery(cfg, store, stopCh)
	// Publish to the compaction lookup closure now that the registry
	// is built; from the next compaction tick onward the closure
	// returns per-tenant compression schedules instead of nil-no-op.
	tenantPolicyHolder = policy

	// Retention manager: append synthesized rules from per-tenant
	// overrides to the global rules list, then start the eviction loop.
	// Every role cuts its reads at the retention horizon (a file past its
	// retention is not planned, although the eviction loop, which only the
	// maintenance roles run, may not have deleted it yet).
	if rp, err := installRetention(cfg, policy, store); err != nil {
		logger.Fatalf("retention policy invalid: %s", err)
	} else {
		// The tombstones expire on the same policy: once the data a delete covers
		// is past the longest retention, its record is not needed.
		tombstoneRet.set(rp)
	}

	if stopRetention := startRetention(cfg, store, policy); stopRetention != nil {
		defer stopRetention()
	}

	// Per-tenant cardinality limiter — gates streams at the insert
	// path so a tenant over its MaxStreams cap drops the offending
	// rows rather than letting unbounded stream cardinality leak
	// through to the writer.
	internalvlstorage.SetCardinalityGate(tenant.NewCardinalityLimiter(policy))

	// Tier-2 dedicated columns: build the custom-attribute slot resolver from
	// config and install it on the ingest + writer/read paths. nil-safe when no
	// promoted_attributes are configured.
	if pa := cfg.ActivePromotedAttributes(); len(pa) > 0 {
		sa := make([]schema.SlotAttr, len(pa))
		for i, a := range pa {
			sa[i] = schema.SlotAttr{Name: a.Name, Bloom: a.Bloom}
		}
		sr := schema.NewSlotResolver(sa)
		internalvlstorage.SetSlotResolver(sr)
		parquets3.SetSlotResolver(sr)
		compaction.SetSlotResolver(sr)
		logger.Infof("dedicated columns: %d custom attribute(s) promoted to slots", len(pa))
	}

	applyTenantStorageOverrides(store, policy, detector)
	logCompactionFreezeWarnings(cfg, detector, policy)

	if cfg.Stats.Enabled {
		startStatsLoops(cfg, store, registry, resolver, addr, stopCh)
	}

	if sc := store.SmartCache(); sc != nil {
		sc.StartEvictionLoop(30*time.Second, stopCh)
		snapshotPath := ""
		if cfg.Cache.DiskPath != "" {
			snapshotPath = cfg.Cache.DiskPath + "/smartcache.meta.json"
		}
		sc.StartSnapshotLoop(snapshotPath, cfg.SmartCache.SnapshotInterval, stopCh)
		logger.Infof("smart cache started; max_age=%v, snapshot_interval=%v",
			cfg.SmartCache.MaxAge, cfg.SmartCache.SnapshotInterval)
	}

	// Size-stats aggregate: maintained by manifest add/remove diffs (flush +
	// compaction), seeded by a Recompute after warm-load and reconciled on each
	// manifest refresh (see runStartup). Read in O(1) by the stats API.
	var statsAgg *stats.StatsAggregate
	if cfg.Stats.Enabled {
		statsAgg = stats.NewStatsAggregate()
		store.Manifest().SetChangeObserver(statsAgg.OnAdd, statsAgg.OnRemove)
		// Cold-start accelerator: seed from the S3 sidecar so the stats API can
		// serve sizes before the warm-load Recompute finishes (which then corrects
		// any staleness). Missing object on a brand-new bucket is expected.
		if err := statsAgg.LoadFromS3(context.Background(), store.Pool(), cfg.AutoPrefix()+stats.AggregateSidecarKeySuffix); err == nil {
			logger.Infof("loaded stats-aggregate sidecar from S3 (cold-start cache)")
		}
	}

	mux := newMux(cfg, store, sm, tombstoneStore, detector, registry, cardLimiter, classTracker, costCalc, resolver, aliasRegistry, policy, statsAgg)

	// Wire the compaction drain endpoint (spec §11.1). The preStop
	// hook in the chart POSTs here to initiate a graceful drain
	// before terminationGracePeriodSeconds elapses. The handler
	// returns 200 with X-Lakehouse-Draining=true; returns 503 when
	// compaction is disabled (sched is nil).
	mux.HandleFunc("/lakehouse/drain", compaction.DrainHandler(sched))
	// Manual compaction trigger (spec: compaction hints). POST {partition, level?}
	// forces (re)compaction of one partition past the level policy — picking up the
	// stale-schema / fragmented candidates surfaced by /stats/compaction. HRW ownership
	// gated (403 if not owner) so two pods never both rewrite the same partition.
	mux.HandleFunc("/lakehouse/compaction/recompact", compaction.RecompactHandler(sched))

	var handler http.Handler = mux
	if resolver != nil && (resolver.HasAliases() || cfg.Tenant.AutoRegister) {
		// Chain order: rate-limit AFTER tenant.Middleware so the inner
		// handler runs against already-resolved AccountID/ProjectID
		// headers. The rate limiter is a no-op when policy is empty
		// or the tenant has no Ingest override.
		handler = tenant.RateLimitMiddleware(tenant.NewIngestRateLimiter(policy))(resolver.Middleware(mux))
	}
	if cfg.Telemetry.Enabled {
		handler = otelhttp.NewHandler(handler, "lakehouse")
	}

	requestHandler := newRequestHandler(handler)

	go runStartup(sm, cfg, store, registry, writerTenantKey, statsAgg)

	httpserver.Serve([]string{addr}, requestHandler, httpserver.ServeOptions{})
	logger.Infof("lakehouse-logs listening; addr=%s", addr)

	sig := procutil.WaitForSigterm()
	logger.Infof("shutdown signal received; signal=%v", sig)

	runShutdown(cfg, addr, store, registry, stopCompaction)
}

// startRetention builds the retention manager and starts its eviction loop. It
// returns the function that stops the loop, or nil when retention is disabled.
// Extracted from run() so the wiring can be tested.
func startRetention(cfg *config.Config, store *parquets3.Storage, policy *tenant.PolicyRegistry) func() {
	if !cfg.Retention.Enabled || !maintenanceAllowed(cfg, "retention") {
		return nil
	}
	rp := store.RetentionPolicy()
	if rp == nil {
		var err error
		if rp, err = installRetention(cfg, policy, store); err != nil {
			logger.Fatalf("retention policy invalid: %s", err)
		}
	}
	retentionMgr, err := buildRetentionManager(cfg, store, rp, "logs")
	if err != nil {
		logger.Fatalf("retention manager init failed: %s", err)
	}
	retentionCtx, retentionCancel := context.WithCancel(context.Background())
	go retentionMgr.Start(retentionCtx)
	return retentionCancel
}

// runShutdown drains the lakehouse-logs pod in the order required for
// honest /ready on the next boot. Mirror of lakehouse-traces/main.go's
// runShutdown — per feedback_logs_traces_module_parity these two blocks
// MUST stay line-aligned. Extracted from run() to keep its cyclomatic
// complexity under the gocyclo budget.
func runShutdown(
	cfg *config.Config,
	addr string,
	store *parquets3.Storage,
	registry *stats.TenantRegistry,
	stopCompaction func(),
) {
	if err := httpserver.Stop([]string{addr}); err != nil {
		logger.Errorf("HTTP server shutdown error: %s", err)
	}

	// The compaction starts draining now, in the background, and is waited for
	// just before the storage closes: a compaction that finishes after Close has
	// written the pmeta bundles would leave its bundle dirty with nothing left to
	// write it. The drain overlaps the snapshot, tombstone and stats steps below.
	compStop := startCompactionStop(stopCompaction)

	// Persist the manifest snapshot FIRST, before the long-running
	// .Stop() calls. See lakehouse-traces/main.go for the full
	// rationale — same pattern, same timeout bound, same logging.
	persistTimeout := cfg.Shutdown.EffectivePersistTimeout()
	persistManifestSnapshot(store, manifestSnapshotPath(cfg), persistTimeout, "shutdown")

	// Footer-cache snapshot — small (a few bytes per key) and fast.
	// Persist sequentially with manifest because both feed the next
	// pod's warmup loop and missing either causes a measurably worse
	// first-query latency. Capped by the same PersistTimeout budget;
	// failure here doesn't fail the shutdown.
	if fc := store.FooterCache(); fc != nil {
		fcStart := time.Now()
		if err := parquets3.SaveFooterCacheKeys(fc, footerCacheSnapshotPath(cfg)); err != nil {
			logger.Errorf("footer-cache snapshot on shutdown failed after %v: %s", time.Since(fcStart), err)
		} else {
			logger.Infof("footer-cache snapshot on shutdown persisted in %v (keys=%d)", time.Since(fcStart), fc.Len())
		}
	}

	// Only the roles that started them stop them: vlinsert.Stop closes channels
	// vlinsert.Init creates, and panics on a select pod ("close of nil channel").
	if cfg.InsertEnabled() {
		vlinsert.Stop()
	}
	if cfg.SelectEnabled() {
		internalselect.Stop()
	}

	// Final stats snapshot on shutdown — bounded too.
	if cfg.Stats.Enabled && cfg.Stats.SnapshotPrefix != "" && cfg.RunsMaintenance() {
		snapshotBucket := cfg.Stats.MetaBucket
		if snapshotBucket == "" {
			snapshotBucket = cfg.S3.Bucket
		}
		_ = snapshotBucket // bucket selection for future multi-bucket support
		snapshotKey := cfg.AutoPrefix() + cfg.Stats.SnapshotPrefix + "/snapshot.json"
		if data, err := registry.MarshalSnapshot(); err == nil {
			statsCtx, statsCancel := context.WithTimeout(context.Background(), persistTimeout)
			if err := store.Pool().Upload(statsCtx, snapshotKey, data); err != nil {
				logger.Errorf("failed to persist stats snapshot on shutdown: %s", err)
			}
			statsCancel()
		}
	}

	// Wait for the compaction to stop (bounded), then close.
	compStop.wait(persistTimeout)

	// Close stops the flusher and closes the insert buffer; the snapshot
	// persisted at the top of this function cannot contain what the flusher
	// wrote while the Stop() calls above ran, so persist again.
	closeStoreAndPersistManifest(store, manifestSnapshotPath(cfg), persistTimeout)

	logger.Infof("lakehouse-logs stopped")
}

// compactionStop is the compaction scheduler's drain and stop running in the
// background; wait collects it with a bound. The scheduler's own drain may wait
// much longer (90 s by default) for a compaction in flight, which must not hold
// back the close of the storage past the shutdown budget. On the bound the
// shutdown goes on with the compaction still running; the deferred stop in run()
// is the backstop, and a compaction that finishes after the storage closed leaves
// its pmeta bundle as it was stored. Mirror of the other binary's type.
type compactionStop struct {
	done    chan struct{}
	started time.Time
}

// startCompactionStop runs stop (nil: nothing to stop) in the background.
func startCompactionStop(stop func()) *compactionStop {
	c := &compactionStop{done: make(chan struct{}), started: time.Now()}
	if stop == nil {
		close(c.done)
		return c
	}
	go func() {
		stop()
		close(c.done)
	}()
	return c
}

// wait returns once the stop has finished, or when timeout has passed since it
// started, whichever comes first.
func (c *compactionStop) wait(timeout time.Duration) {
	remaining := timeout - time.Since(c.started)
	if remaining < 0 {
		remaining = 0
	}
	t := time.NewTimer(remaining)
	defer t.Stop()
	select {
	case <-c.done:
	case <-t.C:
		select {
		case <-c.done:
		default:
			logger.Warnf("compaction did not drain within %v; closing the storage with it still running", timeout)
		}
	}
}

// closeStoreAndPersistManifest closes the storage (the flusher stops, the insert
// buffer closes) and then persists the manifest snapshot AGAIN: the objects the
// flusher wrote since the snapshot taken at the top of runShutdown are not in
// it, and a restart would learn them only from the S3 listing, with time bounds
// it can only guess (the partition hour). With this snapshot the next boot
// starts with their exact bounds. The first snapshot stays: it is the one that
// survives a SIGKILL during the long Stop() calls before Close. Mirror of the
// other binary's helper.
func closeStoreAndPersistManifest(store *parquets3.Storage, path string, timeout time.Duration) {
	if err := store.Close(); err != nil {
		logger.Errorf("storage close error: %s", err)
	}
	persistManifestSnapshot(store, path, timeout, "after close")
}

// persistManifestSnapshot saves the manifest snapshot to path, bounded by
// timeout so a misbehaving local disk cannot extend shutdown. when names the
// shutdown phase in the log lines. Mirror of the other binary's helper.
func persistManifestSnapshot(store *parquets3.Storage, path string, timeout time.Duration, when string) {
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- store.Manifest().SaveTo(path)
	}()
	select {
	case err := <-done:
		if err != nil {
			logger.Errorf("manifest snapshot (%s) failed after %v: %s", when, time.Since(start), err)
		} else {
			logger.Infof("manifest snapshot (%s) persisted in %v", when, time.Since(start))
		}
	case <-time.After(timeout):
		logger.Errorf("manifest snapshot (%s) timed out after %v — next pod will boot with the previous snapshot", when, timeout)
	}
}

// maintenanceAllowed reports whether this process may run the named background
// job, and says once, at INFO, when it may not although the config asks for it.
// A select pod only reads: every job that writes to S3 or mutates state shared
// between pods (compaction and its orphan sweep, delete rewrites, retention, the
// stats snapshots and sidecar, the pmeta cleanup) runs on the roles all and
// insert only, so a select replica can never rewrite the objects an insert pod
// compacts (#311). config.RunsMaintenance is the single predicate.
func maintenanceAllowed(cfg *config.Config, job string) bool {
	if cfg.RunsMaintenance() {
		return true
	}
	logger.Infof("%s not started: role=%s only reads, and only the roles all and insert run background maintenance", job, cfg.Role)
	return false
}

// newCompactionOwnership builds the compaction ownership resolver. The ring is
// the writer pods (writers, resolved from select.insert_headless_service) when
// they are known: every pod of the shared peer ring would otherwise be a
// candidate owner, and a select pod that owns a partition never compacts it, so
// that share of the partitions (a third with two selects and one insert) would
// never be compacted. Without a writer list it falls back to the peer ring, or to
// this pod alone. Extracted from setupCompaction so the ring can be tested.
//
// The ring is the list exactly as discovery returned it. addr is the listen
// address (":9428"), discovery lists the pods by IP or hostname, and the resolver
// finds the member that is this pod with env (internal/selfaddr). The listen
// address is never added to a discovered list: a pod that is not found in it
// owns nothing, where adding it made the pod an extra member that left a share
// of the partitions ownerless and another share owned by every writer.
func newCompactionOwnership(addr string, peerCache *peercache.PeerCache, writers func() []string, env selfaddr.Env, discovery bool) *compaction.OwnershipResolver {
	// Single-pod fallback, only when no discovery is configured at all: this pod
	// is alone and owns every partition. With discovery configured, an empty list
	// (DNS failed, the headless service lists nobody yet) means the pod does not
	// know who its peers are, and it owns nothing until it does: falling back to
	// itself would make every pod of a fleet own everything during a DNS outage.
	ownership := compaction.NewOwnershipResolver(addr, func() []string {
		if writers != nil {
			if w := writers(); len(w) > 0 {
				return w
			}
		}
		if peerCache != nil {
			if members := peerCache.Members(); len(members) > 0 {
				return members
			}
		}
		if discovery {
			return nil
		}
		return []string{addr}
	})
	ownership.Env = env
	if peerCache != nil {
		ownership.Stabilizing = peerCache.IsStabilizing
		ownership.IsDraining = peerCache.IsDraining
	}
	return ownership
}

// setupCompaction wires the election-free compaction scheduler + orphan
// sweeper for one module. Returns (scheduler, sweep, stop) where stop is
// nil when compaction is disabled. Extracted from run() to keep the
// dispatch cyclomatic complexity below the gocyclo:50 budget after the
// PR A election removal added drain + ownership + ring-change plumbing.
func setupCompaction(
	cfg *config.Config,
	store *parquets3.Storage,
	pusher *manifest.Pusher,
	addr string,
	tenantPolicyHolder **tenant.PolicyRegistry,
	detector *delete.StorageClassDetector,
	selfEnv selfaddr.Env,
) (*compaction.Scheduler, *compaction.OrphanSweep, func()) {
	if !cfg.Compaction.Enabled || !maintenanceAllowed(cfg, "compaction and the orphan sweep") {
		return nil, nil, nil
	}

	policy := compaction.NewLevelPolicy(
		cfg.Compaction.MinFilesL0,
		cfg.Compaction.MinFilesL1,
		cfg.Compaction.MinAge,
	)
	policy.DailyRollupAge = cfg.Compaction.DailyRollupAge

	// HRW ownership wiring (spec §2.1.2 + §11.2). The pod finds its own entry in
	// the discovered list with internal/selfaddr; a pod that cannot owns nothing
	// and lakehouse_ring_self_resolved{ring="compaction"} says so (alerts
	// LakehouseRingSelfUnresolved and LakehouseNoWriterInCompactionRing). Ownership
	// hashes over the whole writer list; availability zones play no part.
	peerCache := store.PeerCache()
	ownership := newCompactionOwnership(addr, peerCache, store.WriterPeers, selfEnv, discoveryEnabled(cfg))
	if cfg.Discovery.PeerHeadlessService != "" && cfg.Select.InsertHeadlessService == "" {
		logger.Warnf("compaction ownership hashes over the whole peer ring because select.insert_headless_service is not set; " +
			"select pods in that ring own partitions that nothing compacts. Set it to the insert pods' headless service (the chart does)")
	}

	s3Pool := &s3PoolAdapter{pool: store.Pool()}

	notifyPusher := func(added []manifest.FileInfo, removed []string) {
		if pusher != nil {
			pusher.Notify(added, removed)
		}
	}

	freeze := compactionFreeze(cfg, detector)

	sched := compaction.NewScheduler(compaction.SchedulerConfig{
		Manifest:                 store.Manifest(),
		Pool:                     store.Pool(),
		Ownership:                ownership,
		FairShare:                compaction.NewFairShareScheduler(1),
		Policy:                   policy,
		Prefix:                   cfg.AutoPrefix(),
		Mode:                     cfg.Mode,
		Interval:                 cfg.Compaction.Interval,
		MaxConcurrent:            cfg.Compaction.MaxConcurrent,
		RowGroupSize:             cfg.Insert.RowGroupSize,
		CompressionLevel:         cfg.Insert.CompressionLevel,
		CurrentSchemaFingerprint: parquets3.CurrentSchemaFingerprint(cfg.Mode),
		Freeze:                   freeze,
		CompactionConfig:         cfg.Compaction,
		TenantCompressionLookup: func(prefix string) []int {
			if tenantPolicyHolder == nil || *tenantPolicyHolder == nil || prefix == "" {
				return nil
			}
			account, project, ok := parseTenantPrefix(prefix)
			if !ok {
				return nil
			}
			eff := (*tenantPolicyHolder).For(account, project)
			if eff == nil {
				return nil
			}
			return eff.CompactionCompressionLevels
		},
		OnRingChange: func(register func(eventType string)) {
			if peerCache != nil {
				peerCache.OnRingChange(func(ev peercache.RingChangeEvent) {
					register(string(ev.Type))
				})
			}
		},
		OnCompacted: func(added []manifest.FileInfo, removed []string, blooms map[string]map[string][]string) {
			store.PmetaOnCompacted(added, removed, blooms) // facet feed (incl. combined bloom) + dead-key cleanup
			notifyPusher(added, removed)
		},
	})
	sched.SetSegmentGuard(store.Pool(), 2*bufferGrace(cfg))
	sched.Start()

	sweep := compaction.NewOrphanSweep(compaction.OrphanSweepConfig{
		Manifest:  store.Manifest(),
		Pool:      store.Pool(),
		Ownership: ownership,
		Policy:    policy,
		Freeze:    freeze,
		// Tier A steals leave live buffer segments alone like the scheduler.
		SegmentGuard:     sched.SegmentGuard,
		Lister:           s3Pool,
		Prefix:           cfg.AutoPrefix(),
		Mode:             cfg.Mode,
		Interval:         cfg.Compaction.Interval,
		RowGroupSize:     cfg.Insert.RowGroupSize,
		CompressionLevel: cfg.Insert.CompressionLevel,
		// Orphan-sweep (Tier-A steal) feeds pmeta the same as the scheduler path —
		// the stolen partition's catalog + combined bloom go to the facets and the
		// merged-away inputs are purged — then pushes the manifest. (Symmetric with
		// the traces module; previously logs only pushed the manifest, leaving
		// orphan-swept files without facet/bloom entries until the next compaction.)
		OnCompacted: func(added []manifest.FileInfo, removed []string, blooms map[string]map[string][]string) {
			store.PmetaOnCompacted(added, removed, blooms) // facet feed (incl. combined bloom) + dead-key cleanup
			notifyPusher(added, removed)
		},
	})
	sweep.Start()

	logger.Infof("compaction scheduler started (HRW ownership, no leader election); interval=%v",
		cfg.Compaction.Interval)

	stop := func() {
		sched.Drain()
		sched.Stop()
		sweep.Stop()
	}
	return sched, sweep, stop
}

// startTenantAliasSync starts the SyncPusher that propagates tenant
// alias registrations across the peer ring. Returns the cancel func
// (nil when no sync is needed) so the caller can defer it.
// startPeerDiscovery resolves the gossip peer ring on the configured interval so
// the stats + tenant-alias SyncPushers (and Phase D fleet metadata) actually have
// peers to push to. Without this RefreshDiscovery is never called and GetPeers()
// is always empty — single-instance works, but multi-instance silently never
// gossips. No-op unless a peer headless service is configured.
// discoveryEnabled reports whether the discovery loop has anything to resolve:
// the peer ring, or the insert pods a select pod reads unflushed rows from.
func discoveryEnabled(cfg *config.Config) bool {
	return cfg.Discovery.PeerHeadlessService != "" || cfg.Select.InsertHeadlessService != ""
}

func startPeerDiscovery(cfg *config.Config, store *parquets3.Storage, stopCh <-chan struct{}) {
	if !discoveryEnabled(cfg) {
		return
	}
	interval := cfg.Discovery.PeerRefreshInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	refresh := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.RefreshDiscovery(ctx); err != nil {
			logger.Warnf("peer discovery refresh: %s", err)
		}
	}
	go func() {
		refresh() // resolve once promptly so gossip starts without a full interval wait
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				refresh()
			}
		}
	}()
}

func startTenantAliasSync(cfg *config.Config, store *parquets3.Storage, resolver *tenant.TenantResolver, addr string) context.CancelFunc {
	if resolver == nil {
		return nil
	}
	if !resolver.HasAliases() && !cfg.Tenant.AutoRegister {
		return nil
	}
	disc := store.Discovery()
	if disc == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	pusher := tenant.NewSyncPusher(tenant.SyncPusherConfig{
		Resolver: resolver,
		GetPeers: func() []string { return disc.GetPeers() },
		AuthKey:  cfg.Peer.AuthKey,
		SelfAddr: addr,
		Interval: cfg.Tenant.AliasSyncInterval,
	})
	pusher.Start(ctx)
	logger.Infof("tenant alias sync started; interval=%v", cfg.Tenant.AliasSyncInterval)
	return cancel
}

// startTenantPolicyRefresh kicks off the periodic policy refresh that
// re-resolves alias-keyed override entries as new aliases register.
func startTenantPolicyRefresh(cfg *config.Config, policy *tenant.PolicyRegistry, stopCh <-chan struct{}) {
	if cfg.Tenant.AliasSyncInterval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(cfg.Tenant.AliasSyncInterval)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				policy.Refresh()
			}
		}
	}()
}

// startTenantAliasPersist periodically reconciles this pod with the shared
// alias registry in S3: configured aliases are merged in and entries other
// pods registered are adopted, all through conditional writes, so runtime
// (auto-registered, API) tenants survive restarts and pods never overwrite
// each other.
func startTenantAliasPersist(cfg *config.Config, reg *tenant.Registry, stopCh <-chan struct{}) {
	if reg == nil || cfg.Tenant.AliasSyncInterval <= 0 {
		return
	}
	reg.Run(stopCh, cfg.Tenant.AliasSyncInterval)
}

// applyTenantStorageOverrides installs per-tenant bucket isolation and
// lifecycle transition overrides on the storage layer. Extracted from
// run() to keep its cyclomatic complexity below the gocyclo:50 budget.
func applyTenantStorageOverrides(store *parquets3.Storage, policy *tenant.PolicyRegistry, detector *delete.StorageClassDetector) {
	// Bucket isolation: tenants with an S3 bucket override route
	// writes (via TenantPool/TenantBucket on the writer) and reads
	// (via the pool's BucketRouter) to their dedicated bucket.
	// Manifest sidecars stay in the default bucket so a single
	// fleet-wide manifest still resolves files across tenant buckets.
	if entries := policy.BucketEntries(); len(entries) > 0 {
		poolRegistry := s3reader.NewPoolRegistry(store.Pool())
		bucketByTenant := make(map[uint64]string, len(entries))
		for _, e := range entries {
			bucketByTenant[uint64(e.AccountID)<<32|uint64(e.ProjectID)] = e.Bucket
			_ = poolRegistry.PoolFor(e.Bucket) // warm up
		}
		bucketFor := func(a, p uint32) string {
			return bucketByTenant[uint64(a)<<32|uint64(p)]
		}
		// The manifest refresh must list the dedicated buckets too, or their
		// objects drop out of the manifest at the next refresh.
		tenantBuckets := make([]manifest.TenantBucket, 0, len(entries))
		for _, e := range entries {
			tenantBuckets = append(tenantBuckets, manifest.TenantBucket{
				Bucket: e.Bucket,
				Prefix: fmt.Sprintf("%d/%d/", e.AccountID, e.ProjectID),
			})
		}
		store.Manifest().SetTenantBuckets(tenantBuckets)
		store.Pool().SetBucketRouter(func(key string) string {
			acc, proj, ok := parseTenantFromS3Key(key)
			if !ok {
				return ""
			}
			return bucketFor(acc, proj)
		})
		if w := store.Writer(); w != nil {
			w.SetTenantBucket(bucketFor)
			w.SetTenantPool(func(bucket string) parquets3.PoolWriter {
				return poolRegistry.PoolFor(bucket)
			})
		}
		logger.Infof("tenant bucket isolation installed: %d tenants across %d buckets",
			len(entries), len(poolRegistry.Buckets()))
	}

	// Per-tenant lifecycle overrides: tenants with their own transition
	// schedule shadow the global rules; everyone else stays on global.
	if entries := policy.LifecycleEntries(); len(entries) > 0 {
		overrides := make([]delete.TenantLifecycleOverride, 0, len(entries))
		for _, e := range entries {
			classes := make([]delete.StorageClass, len(e.Classes))
			for i, c := range e.Classes {
				classes[i] = delete.ParseStorageClass(c)
			}
			overrides = append(overrides, delete.TenantLifecycleOverride{
				AccountID:      e.AccountID,
				ProjectID:      e.ProjectID,
				TransitionDays: e.TransitionDays,
				Classes:        classes,
			})
		}
		detector.SetTenantRules(delete.BuildTenantRules(overrides))
		logger.Infof("tenant lifecycle overrides installed: %d tenants", len(entries))
	}
}

func startStatsLoops(cfg *config.Config, store *parquets3.Storage, registry *stats.TenantRegistry, resolver *tenant.TenantResolver, addr string, stopCh chan struct{}) {
	var syncPusher *stats.SyncPusher
	if disc := store.Discovery(); disc != nil {
		syncPusher = stats.NewSyncPusher(stats.SyncPusherConfig{
			Registry: registry,
			GetPeers: func() []string {
				peers := disc.GetPeers()
				urls := make([]string, len(peers))
				for i, p := range peers {
					urls[i] = "http://" + p + "/internal/stats/sync"
				}
				return urls
			},
			AuthKey:  cfg.Peer.AuthKey,
			SelfAddr: addr,
			Compress: cfg.Stats.PushCompression,
		})
	}

	go func() {
		if syncPusher == nil {
			return
		}
		ticker := time.NewTicker(cfg.Stats.PushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := syncPusher.PushDelta(context.Background()); err != nil {
					logger.Warnf("stats push failed: %s", err)
				}
			case <-stopCh:
				return
			}
		}
	}()

	go func() {
		if cfg.Stats.SnapshotPrefix == "" || !maintenanceAllowed(cfg, "the stats snapshot writes") {
			return
		}
		snapshotBucket := cfg.Stats.MetaBucket
		if snapshotBucket == "" {
			snapshotBucket = cfg.S3.Bucket
		}
		_ = snapshotBucket
		snapshotKey := cfg.AutoPrefix() + cfg.Stats.SnapshotPrefix + "/snapshot.json"
		ticker := time.NewTicker(cfg.Stats.SnapshotInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				data, err := registry.MarshalSnapshot()
				if err != nil {
					logger.Warnf("stats snapshot marshal failed: %s", err)
					metrics.StatsSnapshotErrors.Inc()
					continue
				}
				if err := store.Pool().Upload(context.Background(), snapshotKey, data); err != nil {
					logger.Warnf("stats snapshot upload failed: %s", err)
					metrics.StatsSnapshotErrors.Inc()
				} else {
					metrics.StatsSnapshotTotal.Inc()
				}
			case <-stopCh:
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, ts := range registry.All() {
					accID, _ := strconv.ParseUint(ts.AccountID, 10, 32)
					projID, _ := strconv.ParseUint(ts.ProjectID, 10, 32)
					key := resolver.MetricLabel(uint32(accID), uint32(projID))
					metrics.TenantFiles.Set(key, ts.TotalFiles)
					metrics.TenantBytes.Set(key, ts.TotalBytes)
					metrics.TenantRawBytes.Set(key, ts.RawBytes)
				}
			case <-stopCh:
				return
			}
		}
	}()
}

// mountBufferQuery serves this insert pod's unflushed rows to the select pods'
// buffer bridge at /internal/buffer/query: off with upstream's
// -internalselect.disable (buffer.Gate), and behind the peer key when
// peer.auth_enabled is set (buffer.NewHandler). all_tenants=true needs the key
// in any case, enforced or only held (buffer.Handler.WithPeerKey).
func mountBufferQuery(mux *http.ServeMux, src buffer.Source, peer config.PeerConfig) {
	if buffer.InternalSelectDisabled() {
		logger.Infof("%s is off (-internalselect.disable): select pods do not see this pod's unflushed rows until they are in object storage", buffer.Path)
	}
	mux.Handle(buffer.Path, buffer.Gate(buffer.Path, buffer.InternalSelectDisabled, buffer.NewHandler(src, peer.EnforcedKey()).WithPeerKey(peer.AuthKey)))
}

// newRequestHandler is the handler the HTTP server serves. -deleteAuthKey
// (VictoriaLogs v1.53.0) guards every /delete/* request first, and overrides
// -httpAuth.* there: the HTTP server skips Basic Auth for the paths upstream
// reports as checking an authKey flag themselves, and DeleteAuth is that check
// for both delete APIs. Must run once, before httpserver.Serve.
func newRequestHandler(handler http.Handler) func(http.ResponseWriter, *http.Request) bool {
	handler = internaldelete.DeleteAuth(handler)
	registerAuthKeyProtectedPaths()
	return func(w http.ResponseWriter, r *http.Request) bool {
		handler.ServeHTTP(w, r)
		return true
	}
}

// registerAuthKeyProtectedPaths tells the HTTP server which requests verify an
// -*AuthKey flag themselves, so it does not demand -httpAuth.* credentials for
// them first: that is how -deleteAuthKey overrides -httpAuth.* on /delete/*
// (VictoriaLogs v1.53.0, issue #1764). It is upstream's own predicate; the
// binary does not serve /internal/force_merge, /internal/force_flush,
// /internal/log_new_streams or /internal/partition/*, the other paths upstream
// protects this way. Must run once, before httpserver.Serve.
func registerAuthKeyProtectedPaths() {
	registerAuthKeyPathsOnce.Do(func() {
		httpserver.RegisterAuthKeyProtectedPathsFunc(vlselect.IsAuthKeyProtectedPath)
	})
}

// registerAuthKeyPathsOnce: the HTTP server accepts the predicate once per
// process and panics on a second registration.
var registerAuthKeyPathsOnce sync.Once

// mountInternalProtocol mounts VL's cluster protocol for /internal/select/* and
// /internal/delete/*. /internal/delete/* goes to upstream's own
// vlselect.RequestHandler, so the -internaldelete.enable gate (default off),
// its help text and its "disabled" answer are VictoriaLogs' code;
// internaldelete.Handler only adds the lakehouse delete.enabled requirement
// after upstream's flag. Both prefixes answer a bare 405 to anything but POST
// from upstream's own internalselect.RequestHandler (VictoriaLogs v1.53.0, issue
// #1635), so no lakehouse wrapper is needed.
//
// -internalselect.disable (upstream's flag, default off) turns /internal/select/*
// off with upstream's own answer, checked first as upstream does; buffer.Gate
// does the same for /internal/buffer/query on insert pods.
func mountInternalProtocol(mux *http.ServeMux, deleteEnabled bool) {
	mux.Handle("/internal/select/", buffer.Gate("/internal/select/*", buffer.InternalSelectDisabled,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			internalselect.RequestHandler(r.Context(), w, r, strings.ReplaceAll(r.URL.Path, "//", "/"))
		})))
	mux.HandleFunc("/internal/delete/", internaldelete.Handler(internaldelete.FlagEnabled, deleteEnabled,
		func(w http.ResponseWriter, r *http.Request) {
			vlselect.RequestHandler(w, r)
		}))
}

// mountPublicDelete serves upstream's public delete API (/delete/run_task,
// /delete/stop_task, /delete/active_tasks) through upstream's own
// vlselect.RequestHandler: the -delete.enable gate (default off), the
// -deleteAuthKey check, its help text, its answers (including the 405 that
// makes /delete/run_task POST-only, VictoriaLogs v1.53.0) and its request
// handling are VictoriaLogs' code, and the storage calls land in the lakehouse
// adapter, which registers each task as a tombstone scoped to the request's
// tenant. internaldelete.PublicHandler only adds the lakehouse delete.enabled
// requirement after upstream's flag, and delete.ScopeTaskRequests the caller, so
// stop_task and active_tasks act only on the caller's own tasks (globalRead: the
// operator view). Any other /delete/* path the lakehouse does not serve itself
// gets upstream's answer too, as on a VictoriaLogs node.
func mountPublicDelete(mux *http.ServeMux, deleteEnabled bool, globalRead func(*http.Request) bool) {
	mux.HandleFunc("/delete/", internaldelete.PublicHandler(internaldelete.PublicFlagEnabled, deleteEnabled,
		delete.ScopeTaskRequests(globalRead, func(w http.ResponseWriter, r *http.Request) {
			vlselect.RequestHandler(w, r)
		})))
}

// globalReadAuthorizer validates the operator credential that widens a delete
// API request to every tenant's tombstones — the credential that widens a
// select to every tenant. Nil-safe: without a configured credential it
// authorizes nothing.
func globalReadAuthorizer(cfg *config.Config) func(*http.Request) bool {
	auth := tenant.NewGlobalReadAuth(cfg.Tenant.GlobalReadHeader, cfg.Tenant.GlobalReadValue, cfg.Tenant.GlobalReadToken)
	return func(r *http.Request) bool { return auth.Enabled() && auth.Authorize(r) }
}

func newMux(cfg *config.Config, store *parquets3.Storage, sm *startup.Manager, tombstoneStore *delete.TombstoneStore, detector *delete.StorageClassDetector, registry *stats.TenantRegistry, cardLimiter *stats.CardinalityLimiter, classTracker *stats.StorageClassTracker, costCalc *stats.CostCalculator, resolver *tenant.TenantResolver, aliasRegistry *tenant.Registry, policy *tenant.PolicyRegistry, statsAgg *stats.StatsAggregate) *http.ServeMux {
	mux := http.NewServeMux()

	metrics.NewInfoGauge("lakehouse_info", map[string]string{
		"version":  buildinfo.Version,
		"mode":     "logs",
		"topology": string(cfg.Topology),
		"role":     string(cfg.Role),
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "OK")
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		// See lakehouse-traces/main.go /ready handler for full
		// three-state semantics. Same contract here.
		switch {
		case !tombstonesReady(cfg, tombstoneStore):
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, "NOT READY (the list of deleted data has not been read yet)")
		case !sm.ServingReady():
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "NOT READY (phase: %s, min_manifest_files: %d)",
				sm.Phase(), sm.MinManifestFiles())
		case !sm.WarmupComplete() && cfg.Startup.ServeWhileWarming:
			w.WriteHeader(http.StatusNoContent)
		case !sm.WarmupComplete():
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "WARMING (phase: %s)", sm.Phase())
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, "READY")
		}
	})

	m := store.Manifest()
	mux.HandleFunc("/manifest/range", m.RangeHandler())
	mux.HandleFunc("/manifest/partitions", m.PartitionsHandler())

	mux.HandleFunc("/lakehouse/info", HandleLakehouseInfo(LakehouseInfoConfig{
		Version:  buildinfo.Version,
		Mode:     "logs",
		Topology: string(cfg.Topology),
		Compat:   vlCompat,
		IsReady:  sm.IsReady,
		Phase:    func() string { return sm.Phase().String() },
		PeerAuth: PeerAuthInfo{Enabled: cfg.Peer.AuthEnabled, KeyConfigured: cfg.Peer.AuthKey != ""},
	}))

	if cfg.InsertEnabled() || cfg.SelectEnabled() {
		if cfg.Telemetry.Enabled {
			internalvlstorage.SetStorage(telemetry.NewTracedStorage(store), tombstoneStore)
		} else {
			internalvlstorage.SetStorage(store, tombstoneStore)
		}
		// Tombstones created by upstream's delete API (/delete/run_task and
		// the cluster protocol) take the configured default delete mode.
		internalvlstorage.SetDeleteTaskMode(cfg.Delete.DefaultMode)
	}

	if cfg.SelectEnabled() {
		internalselect.Init(0)
		mountInternalProtocol(mux, cfg.Delete.Enabled)
		mountPublicDelete(mux, cfg.Delete.Enabled, globalReadAuthorizer(cfg))

		publicHandler := selectapi.NewHandler(store, cfg, selectapi.WithResolver(resolver))
		publicHandler.Register(mux)
	}

	if cfg.InsertEnabled() {
		// The insert buffer: upstream logstorage, cut into segments by ingest
		// time. Every acknowledged row is in it until the flusher has written
		// it to Parquet; the query path reads it, and the buffer bridge serves
		// it to select pods.
		segs, err := membuffer.OpenSegments(membuffer.Config{Path: cfg.Insert.BufferDir})
		if err != nil {
			logger.Fatalf("open the insert buffer: %s", err)
		}
		var bs internalvlstorage.BufferStore = segs
		if cfg.Telemetry.Enabled {
			bs = telemetry.NewTracedBuffer(segs)
		}
		internalvlstorage.SetInsertStorage(bs, cfg.Insert.BufferDir)
		store.SetLocalBuffer(segs)
		w := store.Writer()
		if w == nil {
			logger.Fatalf("insert is enabled but the Parquet writer is nil")
		}
		flusher := parquets3.NewBufferFlusher(w, segs, cfg.Insert.BufferDir, internalvlstorage.FlushRowKeeper(), parquets3.BufferFlusherConfig{
			TargetBytes: cfg.Insert.TargetFileSizeN(),
			MaxAge:      cfg.Insert.BufferFlushInterval,
			Grace:       bufferGrace(cfg),
			// A restored segment waits for one complete manifest refresh: warn
			// when that takes longer than two refresh intervals (5 minutes at
			// least, the warm-up refresh's timeout).
			HoldWarnAfter: max(2*cfg.Manifest.RefreshInterval, 5*time.Minute),
		})
		flusher.Start(time.Second)
		store.SetBufferFlusher(flusher)
		logger.Infof("insert buffer at %s: segments sealed after %s or %s, kept %s after they are written", segs.Path(), cfg.Insert.BufferFlushInterval, cfg.Insert.TargetFileSize, bufferGrace(cfg))
		vlinsert.Init()

		vlinsertHandler := func(w http.ResponseWriter, r *http.Request) {
			if !vlinsert.RequestHandler(w, r) {
				http.NotFound(w, r)
			}
		}
		mux.HandleFunc("/insert/", vlinsertHandler)
		mux.HandleFunc("/internal/insert", vlinsertHandler)
		mux.HandleFunc("/api/v2/logs", vlinsertHandler)
		mux.HandleFunc("/api/v1/validate", vlinsertHandler)
		mux.HandleFunc("/services/collector/", vlinsertHandler)

		mountBufferQuery(mux, parquets3.BridgeSource{Segments: segs}, cfg.Peer)
	}

	if cfg.Delete.Enabled && tombstoneStore != nil {
		mq := &manifestQuerierAdapter{m: store.Manifest()}
		// Tenant callers manage only their own tombstones; the global-read
		// credential (the one that widens a select) is the operator's view.
		dh := delete.NewHandler(tombstoneStore, mq, detector, &cfg.Delete, "logs",
			delete.WithGlobalReadAuthorizer(globalReadAuthorizer(cfg)))
		dh.Register(mux)
	}

	// Cross-signal handlers
	if cfg.CrossSignal.Enabled && cfg.SelectEnabled() {
		var prefetchRouter crosssignal.PrefetchRouter
		var evictionRouter crosssignal.EvictionRouter
		if sc := store.SmartCache(); sc != nil {
			evictionRouter = sc
		}

		engine := prefetch.NewEngine(4, 1000, func(ctx context.Context, key string) error {
			return store.WarmFile(ctx, key)
		})
		prefetchRouter = engine

		csHandler := crosssignal.NewHandler(crosssignal.HandlerConfig{
			AuthKey:         cfg.CrossSignal.AuthKey,
			PrefetchRouter:  prefetchRouter,
			EvictionHandler: evictionRouter,
		})
		csHandler.Register(mux)
	}

	// The pods' internal endpoints: cache, manifest push, stats and tenant sync.
	peerDeps := peerEndpoints{Cache: store, Manifest: store, PeerCache: store.PeerHandler(), Resolver: resolver}
	if cfg.Stats.Enabled {
		peerDeps.Stats = registry
	}
	mountPeerEndpoints(mux, cfg.Peer, peerDeps)

	// Tenant alias handler
	if resolver != nil {
		aliasHandler := tenant.NewHandler(resolver, nil, cfg.Peer.EnforcedKey()).WithRegistry(aliasRegistry)
		aliasHandler.Register(mux)
	}

	// Admin: tenant bucket migration. Gated by the same global-read
	// credential the rest of the privileged surface uses (one auth
	// surface = one operator workflow to reason about).
	{
		mig := tenant.NewMigrator(store.Manifest(), store.Pool(), cfg.S3.Bucket)
		admin := tenant.NewAdminHandler(mig, tenant.AdminAuthConfig{
			HeaderName:  cfg.Tenant.GlobalReadHeader,
			HeaderValue: cfg.Tenant.GlobalReadValue,
			BearerToken: cfg.Tenant.GlobalReadToken,
		})
		admin.Register(mux)
	}

	// Parity panel: surfaces the side-by-side global comparison of
	// the embedded VL view (`* | stats count()`) vs the manifest
	// LiveAggregate. Both queries answer the same question from
	// different code paths so any drift is operationally meaningful.
	if cfg.Stats.Enabled {
		mountParity(mux, cfg, store.Manifest(), store, *listenAddrFlag)
	}

	// Stats API. statsAgg (the materialized size-stats aggregate) is created and
	// observer-wired by the caller and passed in; the API reads it in O(1).
	if cfg.Stats.Enabled {
		statsAPI := stats.NewAPI(stats.APIConfig{
			Registry:       registry,
			Manifest:       store.Manifest(),
			CostCalc:       costCalc,
			ClassTracker:   classTracker,
			LabelIndex:     store.LabelIndex(),
			SchemaRegistry: store.SchemaRegistry(),
			Resolver:       resolver,
			Policy:         policy,
			Mode:           "logs",
			Bucket:         cfg.S3.Bucket,
			// The schema bloom set the writer/compactor actually emit (Tier-1
			// dedicated columns + slots + legacy), NOT the bare operator list —
			// so the Cardinality Explorer's has_bloom reflects what's on disk.
			BloomColumns:         cfg.WrittenBloomColumns(),
			BreakdownLabels:      cfg.Stats.BreakdownLabels,
			AlwaysSketchFields:   cfg.Pmeta.AlwaysSketchFields,
			PmetaCardinality:     store.PmetaCardinality,
			StatsAggregate:       statsAgg,
			MetaResidentBytes:    store.PmetaResidentBytes,
			MetaDiskBytes:        store.DiskCacheBytes,
			MetaS3Bytes:          statsAgg.MetaS3,
			MetaBytesByTenant:    store.PmetaPersistedBytesByTenant,
			MetadataBytesByField: store.PmetaMetadataBytesByField,
			RetentionEnabled:     cfg.Retention.Enabled,
			RetentionDefault:     cfg.Retention.Default,
			RetentionRules:       len(cfg.Retention.Rules),
			// #171: compaction-hints endpoint inputs (stale-schema + zstd schedule).
			CurrentSchemaFingerprint: parquets3.CurrentSchemaFingerprint(cfg.Mode),
			CompactionConfig:         cfg.Compaction,
		})
		statsAPI.Register(mux)
	}

	// Bloom status API
	mux.HandleFunc("/api/v1/bloom/status", bloomindex.HandleBloomStatus(&bloomindex.StatusProvider{
		Cache:          store.BloomCache(),
		Mode:           "logs",
		IndexedColumns: cfg.Logs.BloomColumns,
	}))

	// Lakehouse UI
	uiHandler := ui.NewHandler(ui.HandlerConfig{
		Enabled: cfg.UI.Enabled,
	})
	uiHandler.Register(mux)

	// VMUI with Lakehouse tab injection
	ui.RegisterVMUI(mux, cfg.UI.VMUITab)

	// Lifecycle endpoints for K8s probes and observability
	lcInfo := lifecycle.LifecycleInfo{
		GetPhase:   func() string { return sm.Phase().String() },
		IsReady:    func() bool { return sm.IsReady() && tombstonesReady(cfg, tombstoneStore) },
		IsDraining: func() bool { return false },
	}
	mux.HandleFunc("/internal/lifecycle/drain", lifecycle.HandleDrain(nil))
	mux.HandleFunc("/internal/lifecycle/ready", lifecycle.HandleLifecycleReady(lcInfo))
	mux.HandleFunc("/internal/lifecycle/ring", lifecycle.HandleLifecycleRing(lcInfo))
	mux.HandleFunc("/internal/lifecycle/stale", lifecycle.HandleLifecycleStale(lcInfo))

	return mux
}

func manifestSnapshotPath(cfg *config.Config) string {
	return filepath.Join(cfg.Manifest.PersistPath, "manifest-snapshot.json")
}

func footerCacheSnapshotPath(cfg *config.Config) string {
	return filepath.Join(cfg.Manifest.PersistPath, "footer-cache-snapshot.bin")
}

// parseTenantPrefix splits "<account>/<project>/<mode>/" into its
// numeric account+project pair. Returns ok=false for any prefix shape
// that isn't an integer-keyed tenant prefix (legacy keys, malformed
// strings, the empty string). The tenant policy registry is keyed by
// (account, project) ints, so this is the call-site adapter.
func parseTenantPrefix(prefix string) (uint32, uint32, bool) {
	parts := strings.SplitN(prefix, "/", 4)
	if len(parts) < 3 {
		return 0, 0, false
	}
	acct, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	proj, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	return uint32(acct), uint32(proj), true
}

// saveStatsAggregate writes the size-stats aggregate sidecar that lets the
// next start serve sizes before its own warm-up has finished. Extracted from
// runStartup so the wiring can be tested.
func saveStatsAggregate(ctx context.Context, cfg *config.Config, store *parquets3.Storage, statsAgg *stats.StatsAggregate) {
	if !cfg.RunsMaintenance() {
		return
	}
	if err := statsAgg.SaveToS3(ctx, store.Pool(), cfg.AutoPrefix()+stats.AggregateSidecarKeySuffix); err != nil {
		logger.Warnf("failed to persist stats-aggregate sidecar: %s", err)
	}
}

// cleanupLegacyPmeta runs the one-time cleanup of the orphaned global pmeta
// bundles (idempotent through a marker object). Extracted from runStartup so
// the wiring can be tested.
func cleanupLegacyPmeta(ctx context.Context, cfg *config.Config, store *parquets3.Storage) {
	if cfg.Pmeta.Enabled && cfg.RunsMaintenance() {
		store.CleanupLegacyGlobalBundles(ctx, cfg.AutoPrefix())
	}
}

func runStartup(sm *startup.Manager, cfg *config.Config, store *parquets3.Storage, registry *stats.TenantRegistry, tenantKey string, statsAgg *stats.StatsAggregate) {
	// Set before the first refresh: LakehouseManifestNoCompleteRefresh reads
	// it during warm-up too (a 0 here would shrink its budget to 15 minutes).
	metrics.ManifestRefreshIntervalSeconds.Set(cfg.Manifest.RefreshInterval.Seconds())
	// Phase 1 (foreground): disk recovery + manifest-files gate.
	// See lakehouse-traces/main.go runStartup for full semantics.
	sm.SetPhase(startup.PhaseDiskRecovery)

	mpath := manifestSnapshotPath(cfg)
	if err := store.Manifest().LoadFrom(mpath); err != nil {
		logger.Warnf("manifest disk load failed (will recover from S3): %s", err)
	} else {
		m := store.Manifest()
		if m.TotalFiles() > 0 {
			logger.Infof("manifest loaded from disk; files=%d, bytes=%d", m.TotalFiles(), m.TotalBytes())
		}
	}
	sm.SetManifestFiles(int64(store.Manifest().TotalFiles()))
	logger.Infof("disk recovery complete; entering serve-while-warming mode (manifest_files=%d, min=%d)",
		store.Manifest().TotalFiles(), cfg.Startup.MinManifestFiles)

	sm.SetServingReady()

	// Phase 2 (background): S3 refresh + cache warmup + snapshot.
	// Periodic refresh ticker waits for completion so the manifest
	// mutex isn't contended.
	warmupStart := time.Now()
	var warmupS3RefreshDuration time.Duration
	warmupDone := make(chan struct{})
	go func() {
		defer close(warmupDone)
		sm.SetPhase(startup.PhaseS3Refresh)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		s3RefreshStart := time.Now()
		refreshErr := store.RefreshManifest(ctx)
		warmupS3RefreshDuration = time.Since(s3RefreshStart)
		if refreshErr != nil {
			logger.Errorf("manifest S3 refresh failed: %s", refreshErr)
		} else {
			m := store.Manifest()
			sm.SetManifestFiles(int64(m.TotalFiles()))
			logger.Infof("manifest S3 refresh complete; files=%d, bytes=%d, min_time=%v, max_time=%v",
				m.TotalFiles(), m.TotalBytes(), m.MinTime(), m.MaxTime())
			registry.ReconcileWithManifest(tenantKey,
				int64(m.TotalFiles()), m.TotalBytes(), m.TotalRawBytes(), m.TotalRows(),
				m.MinTime().UnixNano(), m.MaxTime().UnixNano())
			store.WarmLabelIndex(ctx)
			store.WarmCatalogFromS3(ctx) // load pmeta bundles (file-meta + bloom) BEFORE WarmMetadata enriches from them
			store.WarmMetadata(ctx)
			store.WarmCatalog(ctx) // re-derive catalog from the now-enriched manifest (no-op unless --pmeta)

			// Seed the size-stats aggregate from the warm-loaded manifest (its
			// source of truth). Steady-state diffs flow through the manifest
			// change-observer; this one sweep populates it on startup.
			if statsAgg != nil {
				statsAgg.Recompute(store.Manifest().AllFiles())
				saveStatsAggregate(ctx, cfg, store, statsAgg)
				statsAgg.SetMetaS3(store.PmetaPersistedBytes())
			}

			// Record this node's metadata footprint once warm so the gossiped
			// per-instance breakdown has a value before the first refresh tick.
			if registry != nil && store != nil {
				registry.SetNodeMeta(store.PmetaResidentBytes(), store.DiskCacheBytes())
			}

			// One-time cleanup of orphaned OLD-format (global dt=/hour=) pmeta
			// bundles left by the move to tenant-scoped partitions. The new
			// tenant bundles are rebuilt above by the warm self-heal, so the
			// old globals are safe to delete. Idempotent via a marker object.
			cleanupLegacyPmeta(ctx, cfg, store)

			if cfg.Cache.WarmupPartitions > 0 || cfg.Cache.WarmupMaxFiles > 0 {
				warmCtx, warmCancel := context.WithTimeout(context.Background(), 2*time.Minute)
				store.WarmupCache(warmCtx)
				warmCancel()
			}

			// Async footer-cache prefetch from the previous shutdown's
			// snapshot. Doesn't block /ready=200 — by the time the user
			// runs their first trace-by-ID lookup, the footers for files
			// the previous pod had cached are already populated. The
			// snapshot is just a key list (small), so the load cost is
			// dominated by S3 round-trips for the actual footer ranges.
			if fc := store.FooterCache(); fc != nil {
				snapshotPath := footerCacheSnapshotPath(cfg)
				keys, err := parquets3.LoadFooterCacheKeys(snapshotPath)
				if err != nil {
					logger.Warnf("footer-cache snapshot load failed: %s", err)
				} else if len(keys) > 0 {
					logger.Infof("footer-cache snapshot loaded: %d keys; scheduling async prefetch", len(keys))
					go store.PrefetchFootersByKeys(context.Background(), keys, 32)
				}
			}
		}

		// Persisted whatever the refresh's outcome: a partial state carries the
		// skipped tenants' previous entries and a rejected one is unchanged, so
		// neither is sparse, and a loaded snapshot is never trusted as a listing
		// (Listed is false after a load). Skipping the write would lose the keys
		// retired since the last one (#418, #404).
		if err := store.Manifest().SaveTo(mpath); err != nil {
			logger.Errorf("manifest snapshot after S3 refresh failed: %s", err)
		}

		sm.SetWarmupComplete()
		logger.Infof("background warmup complete; /ready will report 200")
	}()

	<-warmupDone

	var fcMaxBytes, fcAvgBytes int64
	if fc := store.FooterCache(); fc != nil {
		fcMaxBytes, fcAvgBytes = fc.MaxBytes(), fc.AvgEntryBytes()
	}
	startup.EmitStartupHints(startup.HintInputs{
		ManifestFiles:       int64(store.Manifest().TotalFiles()),
		MinManifestFiles:    cfg.Startup.MinManifestFiles,
		FooterCacheMaxBytes: fcMaxBytes,
		FooterAvgBytes:      fcAvgBytes,
		BufferBridgePeers:   len(cfg.Discovery.StorageNodes),
		SnapshotAge:         time.Since(store.Manifest().SavedAt()),
		PersistInterval:     cfg.Manifest.PersistInterval,
		S3RefreshDuration:   warmupS3RefreshDuration,
		WarmupDuration:      time.Since(warmupStart),
	})

	refreshTicker := time.NewTicker(cfg.Manifest.RefreshInterval)
	defer refreshTicker.Stop()
	persistTicker := time.NewTicker(cfg.Manifest.PersistInterval)
	defer persistTicker.Stop()
	// Snapshot-age gauge tick. See lakehouse-traces/main.go for
	// the same pattern.
	ageTicker := time.NewTicker(5 * time.Second)
	defer ageTicker.Stop()
	updateSnapshotAge := func() {
		savedAt := store.Manifest().SavedAt()
		if savedAt.IsZero() {
			metrics.ManifestSnapshotAgeSeconds.Set(float64(24 * 365 * 3600))
			return
		}
		metrics.ManifestSnapshotAgeSeconds.Set(time.Since(savedAt).Seconds())
	}
	updateSnapshotAge()

	for {
		select {
		case <-refreshTicker.C:
			rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
			if err := store.RefreshManifest(rctx); err != nil {
				logger.Errorf("periodic manifest refresh failed: %s", err)
			} else {
				m := store.Manifest()
				sm.SetManifestFiles(int64(m.TotalFiles()))
				logger.Infof("manifest refreshed; files=%d, bytes=%d", m.TotalFiles(), m.TotalBytes())
				registry.ReconcileWithManifest(tenantKey,
					int64(m.TotalFiles()), m.TotalBytes(), m.TotalRawBytes(), m.TotalRows(),
					m.MinTime().UnixNano(), m.MaxTime().UnixNano())
				// Reconcile the size-stats aggregate against the refreshed manifest
				// (the source of truth) — corrects any drift from a bulk refresh
				// that bypassed the per-file change observer.
				if statsAgg != nil {
					statsAgg.Recompute(m.AllFiles())
					saveStatsAggregate(rctx, cfg, store, statsAgg)
					statsAgg.SetMetaS3(store.PmetaPersistedBytes())
				}
				// Refresh this node's gossiped metadata footprint each tick
				// (independent of the size-stats aggregate).
				if registry != nil && store != nil {
					registry.SetNodeMeta(store.PmetaResidentBytes(), store.DiskCacheBytes())
				}
			}
			rcancel()
		case <-ageTicker.C:
			updateSnapshotAge()
		case <-persistTicker.C:
			if err := store.Manifest().SaveTo(mpath); err != nil {
				logger.Errorf("periodic manifest persist failed: %s", err)
			}
			updateSnapshotAge()
		}
	}
}

// applyFlags applies CLI flag overrides on top of cfg loaded from the
// config file or profile. Split into per-section helpers because the
// flat form crossed gocyclo:50 when the K8s-style resource-bound triples
// were added in PR #97; per-section keeps each helper's complexity linear
// in flag count per section and the top-level a clean sequence of calls.
// Field-write order within each helper preserves the flat-form order for
// diff readability.
func applyFlags(cfg *config.Config) {
	applyTopLevelFlags(cfg)
	applyS3Flags(&cfg.S3)
	applyResourceBoundFlags(cfg)
	applyTopologyFlags(cfg)
	applyManifestFlags(&cfg.Manifest)
	applyCacheFlags(&cfg.Cache)
	applyCompactionFlags(&cfg.Compaction)
	applyQueryLegacyFlags(&cfg.Query)
	applyLogsFlags(&cfg.Logs)
	applyTenantFlags(&cfg.Tenant)
	applyPmetaFlags(&cfg.Pmeta)
	applyPeerFlags(&cfg.Peer)
}

// peerAuthKey overrides peer.auth_key. Its value stays out of /flags and
// /metrics (VictoriaMetrics' flagutil.IsSecretFlag hides every flag whose name
// contains "key"), and upstream's envflag expands %{ENV_VAR} in it, so a
// Kubernetes Secret reaches it through an environment variable.
var peerAuthKey = flag.String("lakehouse.peer.auth-key", "", "The key the pods present to each other's internal endpoints (/internal/buffer/query, /internal/cache/*, /internal/manifest/update, /internal/stats/sync, /internal/tenant/sync) whenever it is set, and require on their own with -lakehouse.peer.auth-enabled. Overrides peer.auth_key from the config file; pass a Secret as -lakehouse.peer.auth-key=%{ENV_VAR}. Empty: nothing is sent and /internal/buffer/query refuses all_tenants=true. See docs/security.md")

// peerAuthEnabled turns on enforcement of the peer key (peer.auth_enabled).
// Off by default; a pod sends the key on its own peer calls whenever one is
// set, so roll the key out first, then switch this on.
var peerAuthEnabled = flag.Bool("lakehouse.peer.auth-enabled", false, "Require the peer key (-lakehouse.peer.auth-key) on this pod's internal endpoints: /internal/buffer/query, /internal/cache/*, /internal/manifest/update, /internal/stats/sync, /internal/tenant/sync and the tenant alias admin writes. Off (default): they take requests without a credential, as before, and /internal/buffer/query still refuses all_tenants=true to a caller that does not present the key. Needs -lakehouse.peer.auth-key; the process refuses to start without it. Overrides peer.auth_enabled only to true. See docs/security.md")

// peerMaxRequestBodyBytes caps the request bodies of the internal endpoints.
var peerMaxRequestBodyBytes = flag.Int64("lakehouse.peer.max-request-body-bytes", 0, "Cap on the request body of the internal endpoints that read one (/internal/manifest/update, /internal/stats/sync, /internal/tenant/sync), and on the size a compressed stats sync body expands to; a larger one is refused with 413. 0 uses peer.max_request_body_bytes (default 64 MiB). Always on, whatever -lakehouse.peer.auth-enabled says")

// applyPeerFlags applies -lakehouse.peer.auth-key over peer.auth_key,
// -lakehouse.peer.auth-enabled over peer.auth_enabled (an unset flag never
// turns the config file's true off) and -lakehouse.peer.max-request-body-bytes
// over peer.max_request_body_bytes.
func applyPeerFlags(c *config.PeerConfig) {
	if k := *peerAuthKey; k != "" {
		c.AuthKey = k
	}
	if *peerAuthEnabled {
		c.AuthEnabled = true
	}
	if n := *peerMaxRequestBodyBytes; n > 0 {
		c.MaxRequestBodyBytes = n
	}
}

func applyPmetaFlags(c *config.PmetaConfig) {
	// The flag is authoritative (default true): explicit
	// -lakehouse.pmeta.enabled=false is the opt-out into degraded mode.
	c.Enabled = *pmetaEnabled
	if *pmetaAlwaysSketch != "" {
		c.AlwaysSketchFields = strings.Split(*pmetaAlwaysSketch, ",")
	}
	if *pmetaCardThreshold > 0 {
		c.CardinalityThreshold = *pmetaCardThreshold
	}
	if *pmetaRefuseSketch {
		c.RefuseSketchEnumeration = true
	}
}

func applyTopLevelFlags(cfg *config.Config) {
	if p := *profileFlag; p != "" {
		profileCfg := config.ProfileConfig(config.Profile(p))
		*cfg = *config.MergeConfigs(profileCfg, cfg)
		cfg.Profile = config.Profile(p)
	}
	if r := *role; r != "" {
		cfg.Role = config.Role(r)
	}
}

func applyS3Flags(s3 *config.S3Config) {
	if b := *s3Bucket; b != "" {
		s3.Bucket = b
	}
	if r := *s3Region; r != "" {
		s3.Region = r
	}
	if p := *s3Prefix; p != "" {
		s3.Prefix = p
	}
	if e := *s3Endpoint; e != "" {
		s3.Endpoint = e
	}
	if k := *s3AccessKey; k != "" {
		s3.AccessKey = k
	}
	if k := *s3SecretKey; k != "" {
		s3.SecretKey = k
	}
	if *s3PathStyle {
		s3.ForcePathStyle = true
	}
	if *s3ReadAhead > 0 {
		s3.ReadAheadBytes = *s3ReadAhead
	}
	if *s3CoalesceGap > 0 {
		s3.CoalesceGapBytes = *s3CoalesceGap
	}
	if *s3ReadAheadMax > 0 {
		s3.ReadAheadMaxBytes = *s3ReadAheadMax
	}
	if *s3ReadAheadWaste > 0 {
		s3.ReadAheadWasteThreshold = *s3ReadAheadWaste
	}
	if *s3ReadBufferSize > 0 {
		s3.ReadBufferSize = *s3ReadBufferSize
	}
	if *s3ParquetReadMode != "" {
		s3.ParquetReadMode = *s3ParquetReadMode
	}
	if *s3ProjectedFetchMode != "" {
		s3.ProjectedFetchMode = *s3ProjectedFetchMode
	}
	if *s3ProjectedFetchMaxBytes > 0 {
		s3.ProjectedFetchMaxBytes = *s3ProjectedFetchMaxBytes
	}
	if *s3PlannedFetchMaxInflight > 0 {
		s3.PlannedFetchMaxInflight = *s3PlannedFetchMaxInflight
	}
	if *s3PlannedFetchSpanCap > 0 {
		s3.PlannedFetchSpanCapBytes = *s3PlannedFetchSpanCap
	}
	if *s3WholeFileThreshold > 0 {
		s3.WholeFileThresholdBytes = *s3WholeFileThreshold
	}
	if *s3FooterPrefetchBytes > 0 {
		s3.FooterPrefetchBytes = *s3FooterPrefetchBytes
	}
	if *s3MaxConcurrentDownloads > 0 {
		s3.MaxConcurrentDownloads = *s3MaxConcurrentDownloads
	}
	if *s3ConcurrentDownloadsRequest > 0 {
		s3.ConcurrentDownloadsRequest = *s3ConcurrentDownloadsRequest
	}
	if *s3ConcurrentDownloadsLimit > 0 {
		s3.ConcurrentDownloadsLimit = *s3ConcurrentDownloadsLimit
	}
	if s := *s3ConcurrentDownloadsScaling; s != "" {
		s3.ConcurrentDownloadsScaling = s
	}
}

// applyResourceBoundFlags applies the K8s-style request/limit/scaling
// triples for the 4 surfaces that span multiple config sections
// (Query.FileWorkers*, Cache.Memory*, SmartCache.Disk*, Query.MaxRows*).
// Grouped together because they share the same triple shape and were
// added as a single feature in PR #97.
func applyResourceBoundFlags(cfg *config.Config) {
	if *queryFileWorkersRequest > 0 {
		cfg.Query.FileWorkersRequest = *queryFileWorkersRequest
	}
	if *queryFileWorkersLimit > 0 {
		cfg.Query.FileWorkersLimit = *queryFileWorkersLimit
	}
	if s := *queryFileWorkersScaling; s != "" {
		cfg.Query.FileWorkersScaling = s
	}
	if s := *cacheMemoryRequest; s != "" {
		cfg.Cache.MemoryRequest = s
	}
	if s := *cacheMemoryLimit; s != "" {
		cfg.Cache.MemoryLimitV2 = s
	}
	if s := *cacheMemoryScaling; s != "" {
		cfg.Cache.MemoryScaling = s
	}
	if s := *smartCacheDiskRequest; s != "" {
		cfg.SmartCache.DiskRequest = s
	}
	if s := *smartCacheDiskLimit; s != "" {
		cfg.SmartCache.DiskLimit = s
	}
	if s := *smartCacheDiskScaling; s != "" {
		cfg.SmartCache.DiskScaling = s
	}
	if *queryMaxRowsRequest > 0 {
		cfg.Query.MaxRowsRequest = *queryMaxRowsRequest
	}
	if *queryMaxRowsLimit > 0 {
		cfg.Query.MaxRowsLimit = *queryMaxRowsLimit
	}
	if s := *queryMaxRowsScaling; s != "" {
		cfg.Query.MaxRowsScaling = s
	}
}

func applyTopologyFlags(cfg *config.Config) {
	if t := *topology; t != "" {
		cfg.Topology = config.Topology(t)
	}
	if h := *hotBoundary; h != "" {
		cfg.HotBoundary = h
	}
}

func applyManifestFlags(m *config.ManifestConfig) {
	if *manifestRefresh > 0 {
		m.RefreshInterval = *manifestRefresh
	}
}

func applyCacheFlags(c *config.CacheConfig) {
	if *cacheMemoryMB > 0 {
		c.MemoryLimit = fmt.Sprintf("%dMB", *cacheMemoryMB)
	}
	if p := *cacheDiskPath; p != "" {
		c.DiskPath = p
	}
	if *cacheFooterMaxBytes > 0 {
		c.FooterMaxBytes = *cacheFooterMaxBytes
	}
	if *cacheDiskMB > 0 {
		c.DiskLimit = fmt.Sprintf("%dMB", *cacheDiskMB)
	}
	if *cacheWarmupPartitions > 0 {
		c.WarmupPartitions = *cacheWarmupPartitions
	}
	if *cacheWarmupMaxFiles > 0 {
		c.WarmupMaxFiles = *cacheWarmupMaxFiles
	}
	if pm := *cachePartitionMode; pm != "" {
		c.PartitionMode = pm
	}
}

func applyCompactionFlags(c *config.CompactionConfig) {
	if *compactionEnabled {
		c.Enabled = true
	}
	if *compactionInterval > 0 {
		c.Interval = *compactionInterval
	}
	if *compactionDailyRollupAge > 0 {
		c.DailyRollupAge = *compactionDailyRollupAge
	}
	if s := *compactionRowGroupSizes; s != "" {
		sizes, err := parseIntList(s)
		if err != nil {
			logger.Fatalf("invalid -lakehouse.compaction.row-group-size-by-output-level %q: %s", s, err)
		}
		c.RowGroupSizeByOutputLevel = sizes
	}
}

// parseIntList parses a comma-separated list of positive integers
// (the flag form of the per-output-level schedules; config.Validate
// re-checks positivity for the YAML path).
func parseIntList(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("not an integer: %q", p)
		}
		out = append(out, n)
	}
	return out, nil
}

// applyQueryLegacyFlags applies the pre-resourcebound query-related
// flags that haven't been migrated to the request/limit/scaling triple
// shape yet (file-workers legacy, max-files-per-query, max-live-bytes).
func applyQueryLegacyFlags(q *config.QueryConfig) {
	if *queryFileWorkers > 0 {
		q.FileWorkers = *queryFileWorkers
	}
	if *queryMaxFilesPerQuery > 0 {
		q.MaxFilesPerQuery = *queryMaxFilesPerQuery
	}
	if *queryMaxLiveBytes > 0 {
		q.MaxLiveBytes = *queryMaxLiveBytes
	}
}

func applyLogsFlags(l *config.LogsModeConfig) {
	if s := *logsBloomColumns; s != "" {
		l.BloomColumns = strings.Split(s, ",")
	}
	if s := *logsDeletePrefix; s != "" {
		l.DeletePrefix = s
	}
}

func applyTenantFlags(t *config.TenantConfig) {
	if s := *tenantDefaultAccount; s != "" {
		t.DefaultAccount = s
	}
	if s := *tenantDefaultProject; s != "" {
		t.DefaultProject = s
	}
	if s := *tenantHeaderAccount; s != "" {
		t.HeaderAccount = s
	}
	if s := *tenantHeaderProject; s != "" {
		t.HeaderProject = s
	}
	if s := *tenantGlobalHeader; s != "" {
		t.GlobalReadHeader = s
	}
	if s := *tenantGlobalValue; s != "" {
		t.GlobalReadValue = s
	}
	if s := *tenantGlobalToken; s != "" {
		t.GlobalReadToken = s
	}
	if s := *tenantIsolation; s != "" {
		t.Isolation = s
	}
	if s := *tenantPrefixTemplate; s != "" {
		t.PrefixTemplate = s
	}
	if s := *tenantBucketTemplate; s != "" {
		t.BucketTemplate = s
	}
	if s := *tenantDefaultPrefix; s != "" {
		t.DefaultPrefix = s
	}
	if s := *tenantOrgIDHeader; s != "" {
		t.OrgIDHeader = s
	}
	if s := *tenantMetricsFormat; s != "" {
		t.MetricsFormat = s
	}
	if *tenantAutoRegister {
		t.AutoRegister = true
	}
	if *tenantAutoRegisterMin > 0 {
		t.AutoRegisterMinID = uint32(min(*tenantAutoRegisterMin, uint(1<<32-1)))
	}
	if *tenantAutoRegisterMax > 0 {
		t.AutoRegisterMaxID = uint32(min(*tenantAutoRegisterMax, uint(1<<32-1)))
	}
	if *tenantAliasSyncInterval > 0 {
		t.AliasSyncInterval = *tenantAliasSyncInterval
	}
	if s := *tenantAliases; s != "" {
		if t.Aliases == nil {
			t.Aliases = make(map[string]config.AliasTarget)
		}
		for _, entry := range strings.Split(s, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			parts := strings.Split(entry, ":")
			if len(parts) != 3 {
				logger.Warnf("ignoring invalid tenant alias %q (want orgid:account:project)", entry)
				continue
			}
			acct, err1 := strconv.ParseUint(parts[1], 10, 32)
			proj, err2 := strconv.ParseUint(parts[2], 10, 32)
			if err1 != nil || err2 != nil {
				logger.Warnf("ignoring invalid tenant alias %q (account/project must be numeric)", entry)
				continue
			}
			target := config.AliasTarget{AccountID: uint32(acct), ProjectID: uint32(proj)}
			if prev, dup := t.Aliases[parts[0]]; dup && prev != target {
				aliasFlagConflicts = append(aliasFlagConflicts, fmt.Sprintf("tenant alias %q is given two targets: %d:%d and %d:%d",
					parts[0], prev.AccountID, prev.ProjectID, target.AccountID, target.ProjectID))
				continue
			}
			t.Aliases[parts[0]] = target
		}
	}
}

// aliasFlagConflicts collects -lakehouse.tenant.alias entries that name one
// OrgID twice with different targets; startup refuses to continue on them.
var aliasFlagConflicts []string

func hostname() string {
	if h := os.Getenv("POD_NAME"); h != "" {
		return h
	}
	h, _ := os.Hostname()
	return h
}

// tenantStatsKey formats a (accountID, projectID) pair as the registry's
// "account:project" key, falling back to the writer's default key when
// the row carries no tenant (single-tenant deployments).
func tenantStatsKey(accountID, projectID uint32, fallback string) string {
	if accountID == 0 && projectID == 0 {
		return fallback
	}
	return strconv.FormatUint(uint64(accountID), 10) + ":" + strconv.FormatUint(uint64(projectID), 10)
}

// parseTenantFromS3Key extracts (account, project) from a
// tenant-isolated key like "1002/0/logs/dt=.../foo.parquet". Returns
// ok=false for keys outside that layout (manifest sidecars under
// _meta/, single-prefix legacy files, etc.) so the bucket router
// can short-circuit to the default bucket for them.
func parseTenantFromS3Key(key string) (uint32, uint32, bool) {
	parts := strings.SplitN(key, "/", 4)
	if len(parts) < 3 {
		return 0, 0, false
	}
	acc, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	proj, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	return uint32(acc), uint32(proj), true
}

// retentionConfig is the retention config with the tenant overrides' keep
// durations appended as rules (the longest matching rule wins). Tenant overrides
// materialize as match rules on the file's account_id / project_id labels, so the
// rules engine handles per-tenant TTL resolution with no special casing. The
// policy is built from it once, at start. (An alias-keyed override that resolves
// after start applies only after the next restart.)
func retentionConfig(cfg *config.Config, policy *tenant.PolicyRegistry) retention.Config {
	rules := make([]retention.Rule, 0, len(cfg.Retention.Rules))
	for _, r := range cfg.Retention.Rules {
		rules = append(rules, retention.Rule{Match: r.Match, Keep: r.Keep})
	}
	tenantEntries := []retention.TenantRetentionEntry{}
	for _, e := range policy.RetentionEntries() {
		tenantEntries = append(tenantEntries, retention.TenantRetentionEntry{
			AccountID: e.AccountID,
			ProjectID: e.ProjectID,
			Keep:      e.Retention,
		})
	}
	rules = append(rules, retention.SynthesizeRules(tenantEntries)...)

	return retention.Config{
		Enabled:       cfg.Retention.Enabled,
		Default:       cfg.Retention.Default,
		CheckInterval: cfg.Retention.CheckInterval,
		Rules:         rules,
	}
}

// installRetention builds the pod's retention policy once and installs it on the
// storage, which cuts its reads at it. Every role calls it, including the select
// role, which never runs the eviction loop. The eviction manager is then built
// from the same Policy object (buildRetentionManager), so what is deleted and
// what is hidden cannot differ.
func installRetention(cfg *config.Config, policy *tenant.PolicyRegistry, store *parquets3.Storage) (*retention.Policy, error) {
	rp, err := retention.NewPolicy(retentionConfig(cfg, policy))
	if err != nil {
		return nil, err
	}
	store.SetRetentionPolicy(rp)
	return rp, nil
}

// buildRetentionManager wires the retention package against the running storage
// and manifest, enforcing the given policy.
func buildRetentionManager(cfg *config.Config, store *parquets3.Storage, rp *retention.Policy, mode string) (*retention.Manager, error) {
	deleter := &poolDeleter{pool: store.Pool()}
	slogLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mgr, err := retention.NewWithPolicy(rp, cfg.Retention.CheckInterval, store.Manifest(), deleter, cfg.S3.Bucket, slogLogger.With("component", "retention", "mode", mode))
	if err != nil {
		return nil, err
	}
	// pmeta facet cleanup: expired files leave the facets; a fully-expired
	// partition's bundle is evicted from RAM and its S3 object deleted.
	mgr.SetOnFileRemoved(store.PmetaOnFileExpired)
	return mgr, nil
}

// poolDeleter adapts an s3reader.ClientPool to retention.FileDeleter.
// The bucket parameter is ignored — the pool is bound to its own
// bucket at construction. (Phase 2 bucket isolation will replace this
// with a tenant-aware router.)
type poolDeleter struct {
	pool *s3reader.ClientPool
}

func (d *poolDeleter) DeleteObject(ctx context.Context, _ string, key string) error {
	return d.pool.Delete(ctx, key)
}

// tenantPrefixResolver returns a function that expands the configured
// tenant prefix template into a per-tenant S3 key prefix. Returns nil
// when the template doesn't carry per-tenant placeholders — in that
// case the writer uses its single default prefix unchanged.
//
// Per docs/multi-tenancy.md "boundary principle", the template MUST use
// integer-keyed placeholders ({AccountID}, {ProjectID}) so the on-disk
// layout stays VL/VT-compatible. String OrgIDs are presentation-only.
func tenantPrefixResolver(cfg *config.Config) parquets3.TenantPrefixFunc {
	tmpl := cfg.Tenant.PrefixTemplate
	if tmpl == "" {
		return nil
	}
	if !strings.Contains(tmpl, "{AccountID}") && !strings.Contains(tmpl, "{ProjectID}") {
		return nil
	}
	signal := "logs/"
	if cfg.Mode == config.ModeTraces {
		signal = "traces/"
	}
	return func(accountID, projectID uint32) string {
		a := strconv.FormatUint(uint64(accountID), 10)
		p := strconv.FormatUint(uint64(projectID), 10)
		return strings.NewReplacer("{AccountID}", a, "{ProjectID}", p).Replace(tmpl) + signal
	}
}

func deriveTenantKey(prefix string) string {
	parts := strings.SplitN(strings.TrimSuffix(prefix, "/"), "/", 3)
	if len(parts) >= 2 {
		return parts[0] + ":" + parts[1]
	}
	if len(parts) == 1 && parts[0] != "" {
		return parts[0] + ":"
	}
	return "0:0"
}

type s3PoolAdapter struct {
	pool *s3reader.ClientPool
}

func (a *s3PoolAdapter) List(ctx context.Context, prefix string) ([]string, error) {
	client := a.pool.S3Client()
	bucket := a.pool.Bucket()

	var keys []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

// HeadObject implements compaction.S3Lister. Used by Tier B orphan
// sweep to read LastModified for the OrphanTTL gate.
func (a *s3PoolAdapter) HeadObject(ctx context.Context, key string) (int64, time.Time, error) {
	client := a.pool.S3Client()
	bucket := a.pool.Bucket()

	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, time.Time{}, err
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	var mtime time.Time
	if out.LastModified != nil {
		mtime = *out.LastModified
	}
	return size, mtime, nil
}

type manifestQuerierAdapter struct {
	m *manifest.Manifest
}

// TenantKeyParser lends the delete API the manifest's key → tenant attribution,
// so a tenant's delete and estimate cover exactly its own objects.
func (a *manifestQuerierAdapter) TenantKeyParser() func(key string) (account, project string, ok bool) {
	return a.m.TenantKeyParser()
}

// AccountOnlyTenantKeys tells the delete API that keys carry the account alone,
// so it refuses a delete scoped to a ProjectID other than 0.
func (a *manifestQuerierAdapter) AccountOnlyTenantKeys() bool { return a.m.AccountOnlyTenantKeys() }

func (a *manifestQuerierAdapter) GetFilesForRange(startNs, endNs int64) []delete.FileInfo {
	mFiles := a.m.GetFilesForRange(startNs, endNs)
	result := make([]delete.FileInfo, len(mFiles))
	for i, f := range mFiles {
		result[i] = delete.FileInfo{
			Key:       f.Key,
			Size:      f.Size,
			MinTimeNs: f.MinTimeNs,
			MaxTimeNs: f.MaxTimeNs,
		}
	}
	return result
}

// runHealthcheckSubcommand performs a tiny HTTP probe of the running server's
// /health endpoint and exits non-zero on failure. It replaces the previous
// standalone /usr/local/bin/healthcheck binary (~3 MB) which was COPY'd into
// the image purely for HEALTHCHECK lines and Docker compose healthchecks.
//
// Usage: `lakehouse-logs healthcheck [URL]` (default: http://localhost:9428/health)
func runHealthcheckSubcommand() {
	url := "http://localhost:9428/health"
	if len(os.Args) > 2 {
		url = os.Args[2]
	}
	client := &http.Client{Timeout: 5 * time.Second}
	// #nosec G107,G704 -- operator subcommand; URL is hardcoded default or CLI arg.
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
		os.Exit(1)
	}
	// #nosec G107,G704 -- operator subcommand; URL is hardcoded default or CLI arg.
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "health check returned %d\n", resp.StatusCode)
		os.Exit(1)
	}
}

// runFIPSStatusSubcommand prints whether Go's native FIPS 140-3 mode is
// active in this binary and exits 0 (enabled) or 1 (disabled). Driven by
// GOFIPS140 build env var and the GODEBUG=fips140=on runtime knob.
//
// Usage: `lakehouse-logs fips-status`
func runFIPSStatusSubcommand() {
	if fips140Enabled() {
		fmt.Println("fips140: enabled")
		os.Exit(0)
	}
	fmt.Println("fips140: disabled")
	os.Exit(1)
}

// bufferGrace is how long a written buffer segment stays readable: two
// manifest refreshes plus a margin, so a select pod has listed the segment's
// objects before the insert pod stops serving its rows through the bridge.
func bufferGrace(cfg *config.Config) time.Duration {
	refresh := cfg.Manifest.RefreshInterval
	if refresh <= 0 {
		refresh = 30 * time.Second
	}
	return 2*refresh + 30*time.Second
}

// mountParity registers GET /lakehouse/api/v1/admin/parity. Its loopback query
// goes to listenAddr, the address this process serves HTTP on (-httpListenAddr),
// not to the mode's default: another server (a hot VictoriaLogs on :9428) may
// hold that, and the caller's global-read secret must not travel there.
func mountParity(mux *http.ServeMux, cfg *config.Config, mf *manifest.Manifest, buf stats.BufferSource, listenAddr string) {
	parityAPI := stats.NewAPI(stats.APIConfig{
		Manifest: mf, Buffer: buf, Mode: "logs", Bucket: cfg.S3.Bucket,
		ParityForwardHeader: cfg.Tenant.GlobalReadHeader,
	})
	parityAPI.RegisterParity(mux, stats.NewLoopbackVLQuerier(listenAddr, ""), func(r *http.Request) bool {
		// Same validator the select path uses to widen a query to
		// every tenant — one credential, one implementation.
		return tenant.NewGlobalReadAuth(
			cfg.Tenant.GlobalReadHeader,
			cfg.Tenant.GlobalReadValue,
			cfg.Tenant.GlobalReadToken,
		).Authorize(r)
	})
}
