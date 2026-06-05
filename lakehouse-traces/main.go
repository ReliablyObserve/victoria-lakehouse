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
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/azdetect"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/crosssignal"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/lifecycle"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/prefetch"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/retention"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/startup"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/telemetry"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/ui"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/selectapi"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage/parquets3"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/victoria-traces/servicegraph"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const vtCompat = "0.8.2"

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
	profileFlag     = flag.String("lakehouse.profile", "", "Configuration profile: balanced, max-performance, max-durability, max-cost-savings, dev")
	flushInterval   = flag.Duration("lakehouse.insert.flush-interval", 0, "Insert flush interval (e.g., 10s)")
	listenAddrFlag  = flag.String("httpListenAddr", ":10428", "HTTP listen address")
	manifestRefresh = flag.Duration("lakehouse.manifest.refresh-interval", 0, "Manifest refresh interval (e.g., 30s)")

	cacheMemoryMB         = flag.Int("lakehouse.cache.memory-mb", 0, "L1 memory cache size in MB (default: 256)")
	cacheDiskPath         = flag.String("lakehouse.cache.disk-path", "", "L2 disk cache directory path")
	cacheDiskMB           = flag.Int("lakehouse.cache.disk-max-mb", 0, "L2 disk cache max size in MB (default: 1024)")
	cacheWarmupPartitions = flag.Int("lakehouse.cache.warmup-partitions", 0, "Number of recent hourly partitions to warm on startup (0=disabled)")
	cacheWarmupMaxFiles   = flag.Int("lakehouse.cache.warmup-max-files", 0, "Max files to warm on startup (default: 500)")
	cachePartitionMode    = flag.String("lakehouse.cache.partition-mode", "", "Cache partition mode: az-local (default), global, distributed")

	compactionEnabled        = flag.Bool("lakehouse.compaction.enabled", false, "Enable compaction scheduler")
	compactionInterval       = flag.Duration("lakehouse.compaction.interval", 0, "Compaction scan interval")
	compactionDailyRollupAge = flag.Duration("lakehouse.compaction.daily-rollup-age", 0, "Minimum partition age for daily rollup compaction (default: 24h)")

	queryFileWorkers      = flag.Int("lakehouse.query.file-workers", 0, "Number of parallel file workers for queries (default: 8)")
	queryMaxFilesPerQuery = flag.Int("lakehouse.query.max-files-per-query", 0, "Max S3 files per query before rejection (default: 500)")
	queryMaxLiveBytes     = flag.Int64("lakehouse.query.max-live-bytes", 0, "Per-query ceiling on in-flight DataBlock bytes before cancellation (default: 512MiB)")

	s3ReadAhead   = flag.Int("lakehouse.s3.read-ahead-bytes", 0, "S3 read-ahead buffer size in bytes (default: 2MB)")
	s3CoalesceGap = flag.Int("lakehouse.s3.coalesce-gap-bytes", 0, "Merge S3 range reads with gaps smaller than this (default: 64KB)")

	// K8s-style request/limit/scaling for S3 download concurrency
	// (see internal/resourcebounds). When any of these are non-zero
	// they take precedence over the deprecated lakehouse.s3.max-concurrent-downloads
	// flag. Triple defaults: request=4, limit=16, scaling=fixed.
	s3ConcurrentDownloadsRequest = flag.Int("lakehouse.s3.concurrent-downloads.request", 0, "S3 download concurrency request (always-reserved baseline; default: 4)")
	s3ConcurrentDownloadsLimit   = flag.Int("lakehouse.s3.concurrent-downloads.limit", 0, "S3 download concurrency limit (hard ceiling; default: 16)")
	s3ConcurrentDownloadsScaling = flag.String("lakehouse.s3.concurrent-downloads.scaling", "", "S3 download concurrency scaling policy: fixed|linear|expbackoff (default: fixed)")
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

	tracesBloomColumns  = flag.String("lakehouse.traces.bloom-columns", "", "Comma-separated bloom filter columns for traces (default: trace_id,service.name)")
	tracesDeletePrefix  = flag.String("lakehouse.traces.delete-prefix", "", "Delete API prefix (default: /delete/tracessql)")
	tracesJaegerEnabled = flag.Bool("lakehouse.traces.jaeger-enabled", true, "Enable Jaeger query API")
	tracesJaegerGRPC    = flag.String("lakehouse.traces.jaeger-grpc-addr", "", "Jaeger gRPC listen address (default: :16685)")

	tenantDefaultAccount    = flag.String("lakehouse.tenant.default-account", "", "Default tenant account ID (default: 0)")
	tenantDefaultProject    = flag.String("lakehouse.tenant.default-project", "", "Default tenant project ID (default: 0)")
	tenantHeaderAccount     = flag.String("lakehouse.tenant.header-account", "", "HTTP header for account ID (default: AccountID)")
	tenantHeaderProject     = flag.String("lakehouse.tenant.header-project", "", "HTTP header for project ID (default: ProjectID)")
	tenantGlobalHeader      = flag.String("lakehouse.tenant.global-read-header", "", "Header name for global read access")
	tenantGlobalValue       = flag.String("lakehouse.tenant.global-read-value", "", "Expected header value for global read access")
	tenantGlobalToken       = flag.String("lakehouse.tenant.global-read-token", "", "Bearer token for global read access")
	tenantIsolation         = flag.String("lakehouse.tenant.isolation", "", "Tenant isolation mode: prefix or bucket")
	tenantPrefixTemplate    = flag.String("lakehouse.tenant.prefix-template", "", "S3 prefix template (default: {AccountID}/{ProjectID}/)")
	tenantBucketTemplate    = flag.String("lakehouse.tenant.bucket-template", "", "Bucket name template for bucket isolation")
	tenantDefaultPrefix     = flag.String("lakehouse.tenant.default-prefix", "", "Static S3 key prefix override")
	tenantOrgIDHeader       = flag.String("lakehouse.tenant.orgid-header", "", "HTTP header for string tenant ID (default: X-Scope-OrgID)")
	tenantMetricsFormat     = flag.String("lakehouse.tenant.metrics-format", "", "Tenant metrics label format: id, name, both (default: id)")
	tenantAutoRegister      = flag.Bool("lakehouse.tenant.auto-register", false, "Auto-register unknown X-Scope-OrgID tenants")
	tenantAliasSyncInterval = flag.Duration("lakehouse.tenant.alias-sync-interval", 0, "Fleet sync interval for runtime aliases (default: 30s)")
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
		}
	}
	buildinfo.Init()
	envflag.Parse()

	logger.InitNoLogFlags()
	memAllowed := memory.Allowed()

	// Tell Go's GC the soft memory ceiling so transient allocation peaks
	// during a wildcard scan don't push RSS over the cgroup limit before
	// GC reclaims them. Mirror of cmd/lakehouse-logs/main.go — see that
	// file for the GOMEMLIMIT rationale.
	prevLimit := debug.SetMemoryLimit(int64(memAllowed))
	logger.Infof("Go GC memory limit set to %d bytes (was %d); cgroup_memory_limit≈%d/0.6 bytes",
		memAllowed, prevLimit, memAllowed)

	logger.Infof("lakehouse-traces starting; vt_compat=%s, memory_allowed_bytes=%d", vtCompat, memAllowed)

	cfg, err := config.LoadWithMode(*configPath, config.ModeTraces, config.Role(*role))
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
	if addr == ":10428" && cfg.ListenAddr() != "" && cfg.ListenAddr() != ":10428" {
		addr = cfg.ListenAddr()
	}

	logger.Infof("starting lakehouse-traces; version=%s, role=%s, topology=%s, listen=%s, s3_bucket=%s, s3_prefix=%s",
		buildinfo.Version, cfg.Role, cfg.Topology, addr, cfg.S3.Bucket, cfg.AutoPrefix())

	run(cfg, addr)
}

func run(cfg *config.Config, addr string) {
	sm := startup.NewManager(cfg.Startup.MinManifestFiles)

	store, err := parquets3.New(cfg)
	if err != nil {
		logger.Errorf("failed to initialize storage: %s", err)
		os.Exit(1)
	}

	shutdownTelemetry, err := telemetry.Init(context.Background(), cfg.Telemetry, "lakehouse-traces")
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

	// StartWriter replays the on-disk WAL before serving inserts.
	// We mark WAL-replay-needed before calling it so the lifecycle
	// manager gates /ready=200 on completion, then mark done right
	// after. select-only roles skip both calls (writer is nil) so
	// they don't pay the gate.
	if store.Writer() != nil {
		sm.SetWALReplayNeeded()
	}
	store.StartWriter()
	if store.Writer() != nil {
		sm.SetWALReplayDone()
	}

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

	sched, sweep, stopCompaction := setupCompaction(cfg, store, pusher, addr)
	if stopCompaction != nil {
		defer stopCompaction()
	}
	_ = sweep

	tombstoneStore := delete.NewTombstoneStore()
	if err := tombstoneStore.LoadFromDisk(cfg.Delete.PersistPath); err != nil {
		logger.Warnf("failed to load tombstones from disk: %s; path=%s", err, cfg.Delete.PersistPath)
	}
	if tombstoneStore.Count() == 0 {
		s3Pool := &s3PoolAdapter{pool: store.Pool()}
		if err := tombstoneStore.LoadFromS3(context.Background(), s3Pool, cfg.S3.Bucket, cfg.AutoPrefix()); err != nil {
			logger.Warnf("failed to load tombstones from S3: %s", err)
		}
	}
	store.SetTombstoneStore(tombstoneStore)

	// --- Tenant resolver ---
	resolverCfg := tenant.ResolverConfig{
		MetricsFormat: tenant.ParseMetricsFormat(cfg.Tenant.MetricsFormat),
		AutoRegister:  cfg.Tenant.AutoRegister,
		OrgIDHeader:   cfg.Tenant.OrgIDHeader,
	}
	resolver := tenant.NewResolver(resolverCfg)

	for orgID, target := range cfg.Tenant.Aliases {
		if err := resolver.AddAlias(orgID, tenant.TenantID{
			AccountID: target.AccountID,
			ProjectID: target.ProjectID,
		}); err != nil {
			logger.Warnf("invalid tenant alias %q: %s", orgID, err)
		}
	}

	persister := tenant.NewS3Persister(store.Pool(), cfg.AutoPrefix()+"_meta/tenant-aliases.json")
	s3Aliases, err := persister.LoadAliases()
	if err != nil {
		logger.Warnf("failed to load tenant aliases from S3: %s", err)
	} else {
		for _, ae := range s3Aliases {
			if _, exists := resolver.Resolve(ae.OrgID); !exists {
				_ = resolver.AddAlias(ae.OrgID, tenant.TenantID{
					AccountID: ae.AccountID,
					ProjectID: ae.ProjectID,
				})
			}
		}
		if len(s3Aliases) > 0 {
			logger.Infof("loaded %d tenant aliases from S3", len(s3Aliases))
		}
	}

	if resolver.HasAliases() {
		logger.Infof("tenant resolver active; aliases=%d, metrics_format=%s, auto_register=%v",
			len(resolver.AllAliases()), cfg.Tenant.MetricsFormat, cfg.Tenant.AutoRegister)
	}

	store.Manifest().SetPrefixTemplate(cfg.Tenant.PrefixTemplate)

	// --- Tenant stats ---
	registry := stats.NewTenantRegistry(hostname())

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
		_ = snapshotBucket
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

	lifecycleRules := make([]delete.LifecycleRule, len(cfg.Delete.LifecycleRules))
	for i, r := range cfg.Delete.LifecycleRules {
		lifecycleRules[i] = delete.LifecycleRule{
			TransitionDays: r.TransitionDays,
			Class:          delete.ParseStorageClass(r.StorageClass),
		}
	}
	detector := delete.NewStorageClassDetector(lifecycleRules)

	rewriter := delete.NewRewriter(store.Pool(), cfg.AutoPrefix(), cfg.Insert.RowGroupSize, "traces")

	var rewriteSched *delete.RewriteScheduler
	if cfg.Delete.Enabled {
		rewriteSched = delete.NewRewriteScheduler(delete.RewriteSchedulerConfig{
			Store:          tombstoneStore,
			Rewriter:       rewriter,
			Detector:       detector,
			RewriteDelay:   cfg.Delete.RewriteDelay,
			AllowedClasses: cfg.Delete.AutoRewriteClasses,
			MaxConcurrent:  cfg.Delete.RewriteMaxConcurrent,
		})
		rewriteSched.Start(cfg.Delete.VerifyInterval)
		logger.Infof("delete rewrite scheduler started; rewrite_delay=%v, verify_interval=%v",
			cfg.Delete.RewriteDelay, cfg.Delete.VerifyInterval)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

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

	if cancel := startTenantAliasSync(cfg, store, resolver, addr); cancel != nil {
		defer cancel()
	}

	policy, err := tenant.NewPolicyRegistry(cfg.Tenant.Overrides, resolver)
	if err != nil {
		logger.Fatalf("tenant overrides invalid: %s", err)
	}
	if pending := policy.PendingAliases(); len(pending) > 0 {
		logger.Infof("tenant overrides pending alias resolution: %v", pending)
	}
	startTenantPolicyRefresh(cfg, policy, stopCh)

	if cfg.Retention.Enabled {
		retentionMgr, err := buildRetentionManager(cfg, store, policy, "traces")
		if err != nil {
			logger.Fatalf("retention manager init failed: %s", err)
		}
		retentionCtx, retentionCancel := context.WithCancel(context.Background())
		go retentionMgr.Start(retentionCtx)
		defer retentionCancel()
	}

	internalvlstorage.SetCardinalityGate(tenant.NewCardinalityLimiter(policy))

	applyTenantStorageOverrides(store, policy, detector)

	mux := newMux(cfg, store, sm, tombstoneStore, detector, registry, cardLimiter, classTracker, costCalc, resolver, persister, policy)

	// Wire the compaction drain endpoint (spec §11.1). Mirror of
	// cmd/lakehouse-logs/main.go — line-parity with feedback_logs_traces_module_parity.
	mux.HandleFunc("/lakehouse/drain", compaction.DrainHandler(sched))

	var handler http.Handler = mux
	if resolver != nil && (resolver.HasAliases() || cfg.Tenant.AutoRegister) {
		handler = tenant.RateLimitMiddleware(tenant.NewIngestRateLimiter(policy))(resolver.Middleware(mux))
	}
	if cfg.Telemetry.Enabled {
		handler = otelhttp.NewHandler(handler, "lakehouse-traces")
	}

	requestHandler := func(w http.ResponseWriter, r *http.Request) bool {
		handler.ServeHTTP(w, r)
		return true
	}

	go runStartup(sm, cfg, store, registry, writerTenantKey)

	httpserver.Serve([]string{addr}, requestHandler, httpserver.ServeOptions{})
	logger.Infof("lakehouse-traces listening; addr=%s", addr)

	sig := procutil.WaitForSigterm()
	logger.Infof("shutdown signal received; signal=%v", sig)

	if err := httpserver.Stop([]string{addr}); err != nil {
		logger.Errorf("HTTP server shutdown error: %s", err)
	}

	// CRITICAL ORDER: persist the manifest snapshot FIRST, before
	// any of the long-running .Stop() calls that could hold the
	// shutdown grace window. If k8s sends SIGKILL after the grace
	// period, the snapshot is what determines /ready accuracy on
	// the NEXT boot — losing it means the new pod starts with an
	// older snapshot (or none) and lies about readiness during the
	// background S3 LIST that follows. Bounded to PersistTimeout so
	// a misbehaving local disk can't extend shutdown indefinitely.
	persistTimeout := cfg.Shutdown.PersistTimeout
	if persistTimeout <= 0 {
		persistTimeout = 30 * time.Second
	}
	manifestSaveStart := time.Now()
	manifestSaveDone := make(chan error, 1)
	go func() {
		manifestSaveDone <- store.Manifest().SaveTo(manifestSnapshotPath(cfg))
	}()
	select {
	case err := <-manifestSaveDone:
		if err != nil {
			logger.Errorf("manifest snapshot on shutdown failed after %v: %s", time.Since(manifestSaveStart), err)
		} else {
			logger.Infof("manifest snapshot on shutdown persisted in %v", time.Since(manifestSaveStart))
		}
	case <-time.After(persistTimeout):
		logger.Errorf("manifest snapshot on shutdown timed out after %v — next pod will boot with previous snapshot", persistTimeout)
	}

	vtinsert.Stop()
	internalselect.Stop()

	if rewriteSched != nil {
		rewriteSched.Stop()
	}
	if err := tombstoneStore.PersistToDisk(cfg.Delete.PersistPath); err != nil {
		logger.Errorf("failed to persist tombstones to disk: %s", err)
	}

	// Final stats snapshot on shutdown — bounded too so it can't
	// extend shutdown past the grace window.
	if cfg.Stats.Enabled && cfg.Stats.SnapshotPrefix != "" {
		snapshotBucket := cfg.Stats.MetaBucket
		if snapshotBucket == "" {
			snapshotBucket = cfg.S3.Bucket
		}
		_ = snapshotBucket
		snapshotKey := cfg.AutoPrefix() + cfg.Stats.SnapshotPrefix + "/snapshot.json"
		if data, err := registry.MarshalSnapshot(); err == nil {
			statsCtx, statsCancel := context.WithTimeout(context.Background(), persistTimeout)
			if err := store.Pool().Upload(statsCtx, snapshotKey, data); err != nil {
				logger.Errorf("failed to persist stats snapshot on shutdown: %s", err)
			}
			statsCancel()
		}
	}

	if err := store.Close(); err != nil {
		logger.Errorf("storage close error: %s", err)
	}

	logger.Infof("lakehouse-traces stopped")
}

// setupCompaction wires the election-free compaction scheduler + orphan
// sweeper for the traces module. Mirror of cmd/lakehouse-logs/main.go's
// setupCompaction — per feedback_logs_traces_module_parity these two
// blocks MUST stay line-aligned.
func setupCompaction(
	cfg *config.Config,
	store *parquets3.Storage,
	pusher *manifest.Pusher,
	addr string,
) (*compaction.Scheduler, *compaction.OrphanSweep, func()) {
	if !cfg.Compaction.Enabled {
		return nil, nil, nil
	}

	policy := compaction.NewLevelPolicy(
		cfg.Compaction.MinFilesL0,
		cfg.Compaction.MinFilesL1,
		cfg.Compaction.MinAge,
	)
	policy.DailyRollupAge = cfg.Compaction.DailyRollupAge

	peerCache := store.PeerCache()
	// Mirror of cmd/lakehouse-logs/main.go: peercache.Members() is empty
	// in single-pod / pre-discovery scenarios. Include self as a
	// fallback so HRW always has at least one candidate; otherwise
	// compaction never runs (lakehouse_compaction_runs_total = 0
	// forever — confirmed via e2e compose).
	ownership := compaction.NewOwnershipResolver(addr, func() []string {
		if peerCache == nil {
			return []string{addr}
		}
		members := peerCache.Members()
		if len(members) == 0 {
			return []string{addr}
		}
		for _, m := range members {
			if m == addr {
				return members
			}
		}
		out := make([]string, 0, len(members)+1)
		out = append(out, members...)
		out = append(out, addr)
		return out
	})
	if peerCache != nil {
		ownership.SameAZPeers = peerCache.SameAZMembers
		ownership.Stabilizing = peerCache.IsStabilizing
		ownership.IsDraining = peerCache.IsDraining
	}

	s3Pool := &s3PoolAdapter{pool: store.Pool()}

	notifyPusher := func(added []manifest.FileInfo, removed []string) {
		if pusher != nil {
			pusher.Notify(added, removed)
		}
	}

	sched := compaction.NewScheduler(compaction.SchedulerConfig{
		Manifest:         store.Manifest(),
		Pool:             store.Pool(),
		Ownership:        ownership,
		FairShare:        compaction.NewFairShareScheduler(1),
		Policy:           policy,
		Prefix:           cfg.AutoPrefix(),
		Mode:             cfg.Mode,
		Interval:         cfg.Compaction.Interval,
		MaxConcurrent:    cfg.Compaction.MaxConcurrent,
		RowGroupSize:     cfg.Insert.RowGroupSize,
		CompressionLevel: cfg.Insert.CompressionLevel,
		OnRingChange: func(register func(eventType string)) {
			if peerCache != nil {
				peerCache.OnRingChange(func(ev peercache.RingChangeEvent) {
					register(string(ev.Type))
				})
			}
		},
		OnCompacted: notifyPusher,
	})
	sched.Start()

	sweep := compaction.NewOrphanSweep(compaction.OrphanSweepConfig{
		Manifest:         store.Manifest(),
		Pool:             store.Pool(),
		Ownership:        ownership,
		Policy:           policy,
		Lister:           s3Pool,
		Prefix:           cfg.AutoPrefix(),
		Mode:             cfg.Mode,
		Interval:         cfg.Compaction.Interval,
		RowGroupSize:     cfg.Insert.RowGroupSize,
		CompressionLevel: cfg.Insert.CompressionLevel,
		OnCompacted:      notifyPusher,
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
// Mirror of cmd/lakehouse-logs/main.go per feedback_logs_traces_module_parity.
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
// Mirror of cmd/lakehouse-logs/main.go per feedback_logs_traces_module_parity.
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

// applyTenantStorageOverrides installs per-tenant bucket isolation and
// lifecycle transition overrides on the storage layer. Extracted from
// run() to keep its cyclomatic complexity below the gocyclo:50 budget.
// Mirror of cmd/lakehouse-logs/main.go per feedback_logs_traces_module_parity.
func applyTenantStorageOverrides(store *parquets3.Storage, policy *tenant.PolicyRegistry, detector *delete.StorageClassDetector) {
	if entries := policy.BucketEntries(); len(entries) > 0 {
		poolRegistry := s3reader.NewPoolRegistry(store.Pool())
		bucketByTenant := make(map[uint64]string, len(entries))
		for _, e := range entries {
			bucketByTenant[uint64(e.AccountID)<<32|uint64(e.ProjectID)] = e.Bucket
			_ = poolRegistry.PoolFor(e.Bucket)
		}
		bucketFor := func(a, p uint32) string {
			return bucketByTenant[uint64(a)<<32|uint64(p)]
		}
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
		if cfg.Stats.SnapshotPrefix == "" {
			return
		}
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

func newMux(cfg *config.Config, store *parquets3.Storage, sm *startup.Manager, tombstoneStore *delete.TombstoneStore, detector *delete.StorageClassDetector, registry *stats.TenantRegistry, cardLimiter *stats.CardinalityLimiter, classTracker *stats.StorageClassTracker, costCalc *stats.CostCalculator, resolver *tenant.TenantResolver, persister *tenant.S3Persister, policy *tenant.PolicyRegistry) *http.ServeMux {
	mux := http.NewServeMux()

	metrics.NewInfoGauge("lakehouse_info", map[string]string{
		"version":  buildinfo.Version,
		"mode":     "traces",
		"topology": string(cfg.Topology),
		"role":     string(cfg.Role),
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "OK")
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		// Three-state readiness:
		//   503 — not yet serving (disk recovery, WAL replay,
		//         MinManifestFiles gate)
		//   204 — serving but background warmup in progress
		//         (S3 refresh, cache warmup, bloom backfill)
		//   200 — fully ready (warmup complete)
		//
		// Strict readiness (k8s readinessProbe with
		// successThreshold>1, helm chart default) gates routing on
		// 200 only. Soft routing (vtselect peer fan-out, the
		// upstream HTTP check helper) accepts 2xx. ServeWhileWarming
		// in StartupConfig controls whether 204 is even possible
		// for this pod — strict deployments leave it false and
		// /ready returns 503 until 200.
		switch {
		case !sm.ServingReady():
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "NOT READY (phase: %s, manifest_files: %d, min: %d)",
				sm.Phase(), 0, sm.MinManifestFiles())
		case !sm.WarmupComplete() && cfg.Startup.ServeWhileWarming:
			w.WriteHeader(http.StatusNoContent)
		case !sm.WarmupComplete():
			// ServeWhileWarming disabled — wait for 200.
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
		Mode:     "traces",
		Topology: string(cfg.Topology),
		Compat:   vtCompat,
		IsReady:  sm.IsReady,
		Phase:    func() string { return sm.Phase().String() },
	}))

	if cfg.InsertEnabled() || cfg.SelectEnabled() {
		if cfg.Telemetry.Enabled {
			internalvlstorage.SetStorage(internalvlstorage.NewTracedStorage(store), tombstoneStore)
		} else {
			internalvlstorage.SetStorage(store, tombstoneStore)
		}
		// Wire a tenant lister so VT's per-tenant background tasks
		// (notably servicegraph) iterate every tenant the LH process
		// holds in cold storage, not just the legacy {0,0}.
		vtstorageadapter.Init(store, vtstorageadapter.WithTenantLister(
			func(startNs, endNs int64) []logstorage.TenantID {
				summaries := store.Manifest().TenantSummariesInWindow(startNs, endNs)
				out := make([]logstorage.TenantID, 0, len(summaries))
				for _, s := range summaries {
					acc, errA := strconv.ParseUint(s.AccountID, 10, 32)
					proj, errP := strconv.ParseUint(s.ProjectID, 10, 32)
					if errA != nil || errP != nil {
						continue
					}
					out = append(out, logstorage.TenantID{
						AccountID: uint32(acc),
						ProjectID: uint32(proj),
					})
				}
				return out
			},
		))

		// VT's service-graph background task aggregates spans into
		// (caller, callee, count) edges and persists them as regular
		// log rows tagged with {trace_service_graph_stream="-"}. The
		// /select/jaeger/api/dependencies reader queries those rows
		// back. Both ends already route through our adapter, so
		// enabling the upstream task gives the cold tier Service
		// Graph parity with hot VT without any cold-tier-specific
		// service-graph code on our side.
		//
		// Disabled-by-default upstream behind -servicegraph.enableTask
		// = true; the LH operator opts in via the same flag.
		//
		// No defer Stop() here — newMux returns immediately after wiring,
		// and a defer at this scope would tear the goroutine down before
		// its first tick. The task lives for the program's lifetime; the
		// OS reclaims the goroutine on process exit.
		servicegraph.Init()
	}

	if cfg.SelectEnabled() {
		internalselect.Init()

		internalHandler := func(w http.ResponseWriter, r *http.Request) {
			internalselect.RequestHandler(r.Context(), w, r)
		}
		mux.HandleFunc("/internal/select/", internalHandler)
		mux.HandleFunc("/internal/delete/", internalHandler)

		publicHandler := selectapi.NewHandler(store, cfg)
		publicHandler.Register(mux)
	}

	if cfg.InsertEnabled() {
		internalvlstorage.SetInsertStorage(store)
		vtinsert.Init()

		vtinsertHandler := func(w http.ResponseWriter, r *http.Request) {
			if !vtinsert.RequestHandler(w, r) {
				http.NotFound(w, r)
			}
		}
		mux.HandleFunc("/insert/", vtinsertHandler)

		if w := store.Writer(); w != nil {
			bh := buffer.NewHandler(w, cfg.Peer.AuthKey)
			mux.Handle("/internal/buffer/query", bh)
		}
	}

	if cfg.Delete.Enabled && tombstoneStore != nil {
		mq := &manifestQuerierAdapter{m: store.Manifest()}
		dh := delete.NewHandler(tombstoneStore, mq, detector, &cfg.Delete, "traces")
		dh.Register(mux)
	}

	mux.HandleFunc("/internal/cache/clear", HandleCacheClear(store, cfg.Peer.AuthKey))

	mux.HandleFunc("/internal/cache/stats", HandleCacheStats(store, cfg.Peer.AuthKey))

	if ph := store.PeerHandler(); ph != nil {
		mux.Handle("/internal/cache/", ph)
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

	mux.HandleFunc("/internal/manifest/update", HandleManifestUpdate(store, cfg.Peer.AuthKey))

	// Stats sync handler
	if cfg.Stats.Enabled {
		syncHandler := stats.NewSyncHandler(registry, cfg.Peer.AuthKey)
		mux.Handle("/internal/stats/sync", syncHandler)
	}

	// Admin: tenant bucket migration. Same auth surface as
	// cmd/lakehouse-logs — see that copy for the design note.
	{
		mig := tenant.NewMigrator(store.Manifest(), store.Pool(), cfg.S3.Bucket)
		admin := tenant.NewAdminHandler(mig, tenant.AdminAuthConfig{
			HeaderName:  cfg.Tenant.GlobalReadHeader,
			HeaderValue: cfg.Tenant.GlobalReadValue,
			BearerToken: cfg.Tenant.GlobalReadToken,
		})
		admin.Register(mux)
	}

	if cfg.Stats.Enabled {
		parityAPI := stats.NewAPI(stats.APIConfig{Manifest: store.Manifest(), Mode: "traces", Bucket: cfg.S3.Bucket})
		listenAddrLocal := *listenAddrFlag
		if cfg.ListenAddr() != "" {
			listenAddrLocal = cfg.ListenAddr()
		}
		parityAPI.RegisterParityWithInternal(mux, stats.NewLocalVLQuerierWithQuery(
			fmt.Sprintf("http://127.0.0.1%s", listenAddrLocal),
			stats.TracesParityQuery,
		), func(r *http.Request) bool {
			if cfg.Tenant.GlobalReadHeader != "" && cfg.Tenant.GlobalReadValue != "" {
				return r.Header.Get(cfg.Tenant.GlobalReadHeader) == cfg.Tenant.GlobalReadValue
			}
			return cfg.Tenant.GlobalReadToken != "" && r.Header.Get("Authorization") == "Bearer "+cfg.Tenant.GlobalReadToken
		}, vtInternalCounter{}, []string{"trace_id_idx", "service_graph"})
	}

	// Stats API
	if cfg.Stats.Enabled {
		statsAPI := stats.NewAPI(stats.APIConfig{
			Registry:        registry,
			Manifest:        store.Manifest(),
			CostCalc:        costCalc,
			ClassTracker:    classTracker,
			LabelIndex:      store.LabelIndex(),
			SchemaRegistry:  store.SchemaRegistry(),
			Resolver:        resolver,
			Policy:          policy,
			Mode:            "traces",
			Bucket:          cfg.S3.Bucket,
			BloomColumns:    cfg.ActiveBloomColumns(),
			BreakdownLabels: cfg.Stats.BreakdownLabels,
		})
		statsAPI.Register(mux)
	}

	// Bloom status API
	mux.HandleFunc("/api/v1/bloom/status", bloomindex.HandleBloomStatus(&bloomindex.StatusProvider{
		Mode:           "traces",
		IndexedColumns: cfg.Traces.BloomColumns,
	}))

	// Tenant alias management API
	if resolver != nil {
		tenantHandler := tenant.NewHandler(resolver, persister, cfg.Peer.AuthKey)
		tenantHandler.Register(mux)
	}

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
		IsReady:    sm.IsReady,
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

func runStartup(sm *startup.Manager, cfg *config.Config, store *parquets3.Storage, registry *stats.TenantRegistry, tenantKey string) {
	// Phase 1 (foreground): disk recovery + manifest-files gate.
	// Disk recovery loads the most-recent snapshot; the lifecycle
	// manager then checks MinManifestFiles before flipping
	// ServingReady. If the threshold isn't met, /ready stays 503
	// until the background S3 refresh fills the manifest. Avoids
	// the first-ever-boot lying-empty window.
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

	// Flip ServingReady. /ready returns 204 or 503 depending on
	// ServeWhileWarming + MinManifestFiles. Background warmup
	// below flips WarmupComplete when it finishes.
	sm.SetServingReady()

	// Phase 2 (background): S3 manifest refresh, label-index +
	// cache warmup, snapshot save, bloom backfill. The periodic
	// refresh ticker waits for this goroutine to finish so the
	// two can't race on the manifest mutex.
	warmupDone := make(chan struct{})
	go func() {
		defer close(warmupDone)
		sm.SetPhase(startup.PhaseS3Refresh)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		if err := store.RefreshManifest(ctx); err != nil {
			logger.Errorf("manifest S3 refresh failed: %s", err)
		} else {
			m := store.Manifest()
			sm.SetManifestFiles(int64(m.TotalFiles()))
			logger.Infof("manifest S3 refresh complete; files=%d, bytes=%d, min_time=%v, max_time=%v",
				m.TotalFiles(), m.TotalBytes(), m.MinTime(), m.MaxTime())
			registry.ReconcileWithManifest(tenantKey,
				int64(m.TotalFiles()), m.TotalBytes(), m.TotalRawBytes(), m.TotalRows(),
				m.MinTime().UnixNano(), m.MaxTime().UnixNano())
			store.WarmLabelIndex(ctx)

			if cfg.Cache.WarmupPartitions > 0 || cfg.Cache.WarmupMaxFiles > 0 {
				warmCtx, warmCancel := context.WithTimeout(context.Background(), 2*time.Minute)
				store.WarmupCache(warmCtx)
				warmCancel()
			}
		}

		if err := store.Manifest().SaveTo(mpath); err != nil {
			logger.Errorf("manifest snapshot after S3 refresh failed: %s", err)
		}

		bctx, bcancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer bcancel()
		store.BackfillBloomIndex(bctx)

		sm.SetWarmupComplete()
		logger.Infof("background warmup complete; /ready will report 200")
	}()

	<-warmupDone

	refreshTicker := time.NewTicker(cfg.Manifest.RefreshInterval)
	defer refreshTicker.Stop()
	persistTicker := time.NewTicker(cfg.Manifest.PersistInterval)
	defer persistTicker.Stop()
	// 5-second tick to update the snapshot-age gauge so monitoring
	// dashboards see live age without depending on the slower
	// refresh/persist cadence. Cheap — single time math + one
	// gauge Set per tick.
	ageTicker := time.NewTicker(5 * time.Second)
	defer ageTicker.Stop()
	updateSnapshotAge := func() {
		savedAt := store.Manifest().SavedAt()
		if savedAt.IsZero() {
			// No successful persist + no loaded snapshot. Report a
			// very large sentinel so alerts that fire on "snapshot
			// older than X" still trigger.
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
			}
			rcancel()
		case <-persistTicker.C:
			if err := store.Manifest().SaveTo(mpath); err != nil {
				logger.Errorf("periodic manifest persist failed: %s", err)
			}
			updateSnapshotAge()
		case <-ageTicker.C:
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
// diff readability.  Mirrors `cmd/lakehouse-logs/main.go.applyFlags`.
func applyFlags(cfg *config.Config) {
	applyTopLevelFlags(cfg)
	applyS3Flags(&cfg.S3)
	applyResourceBoundFlags(cfg)
	applyTopologyFlags(cfg)
	applyManifestFlags(&cfg.Manifest)
	applyCacheFlags(&cfg.Cache)
	applyCompactionFlags(&cfg.Compaction)
	applyQueryLegacyFlags(&cfg.Query)
	applyTracesFlags(&cfg.Traces)
	applyTenantFlags(&cfg.Tenant)
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
	if *flushInterval > 0 {
		cfg.Insert.FlushInterval = *flushInterval
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

func applyTracesFlags(t *config.TracesModeConfig) {
	if s := *tracesBloomColumns; s != "" {
		t.BloomColumns = strings.Split(s, ",")
	}
	if s := *tracesDeletePrefix; s != "" {
		t.DeletePrefix = s
	}
	if *tracesJaegerEnabled {
		t.JaegerEnabled = true
	}
	if s := *tracesJaegerGRPC; s != "" {
		t.JaegerGRPCAddr = s
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
	if *tenantAliasSyncInterval > 0 {
		t.AliasSyncInterval = *tenantAliasSyncInterval
	}
}

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

// vtInternalCounter adapts metrics.VTInternalRowsDropped to the
// stats.VTInternalCounter interface. Kept tiny so the parity wiring
// in stats package doesn't take a direct dependency on internal/metrics.
type vtInternalCounter struct{}

func (vtInternalCounter) Get(kind string) uint64 {
	return metrics.VTInternalRowsDropped.Get(kind)
}

// parseTenantFromS3Key extracts (account, project) from a
// tenant-isolated key. Mirror of the helper in cmd/lakehouse-logs.
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

// buildRetentionManager wires retention against the running storage +
// per-tenant policy. Mirror of the helper in cmd/lakehouse-logs/main.go;
// see that copy for the design rationale.
func buildRetentionManager(cfg *config.Config, store *parquets3.Storage, policy *tenant.PolicyRegistry, mode string) (*retention.Manager, error) {
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

	retCfg := retention.Config{
		Enabled:       cfg.Retention.Enabled,
		Default:       cfg.Retention.Default,
		CheckInterval: cfg.Retention.CheckInterval,
		Rules:         rules,
	}
	deleter := &poolDeleter{pool: store.Pool()}
	slogLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return retention.New(retCfg, store.Manifest(), deleter, cfg.S3.Bucket, slogLogger.With("component", "retention", "mode", mode))
}

type poolDeleter struct {
	pool *s3reader.ClientPool
}

func (d *poolDeleter) DeleteObject(ctx context.Context, _ string, key string) error {
	return d.pool.Delete(ctx, key)
}

// tenantPrefixResolver returns a function that expands the configured
// tenant prefix template into a per-tenant S3 key prefix. See the
// equivalent comment in cmd/lakehouse-logs/main.go for the contract.
func tenantPrefixResolver(cfg *config.Config) parquets3.TenantPrefixFunc {
	tmpl := cfg.Tenant.PrefixTemplate
	if tmpl == "" {
		return nil
	}
	if !strings.Contains(tmpl, "{AccountID}") && !strings.Contains(tmpl, "{ProjectID}") {
		return nil
	}
	signal := "traces/"
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

func (a *s3PoolAdapter) Upload(ctx context.Context, key string, data []byte) error {
	return a.pool.Upload(ctx, key, data)
}

func (a *s3PoolAdapter) Download(ctx context.Context, key string) ([]byte, error) {
	return a.pool.Download(ctx, key)
}

func (a *s3PoolAdapter) Delete(ctx context.Context, key string) error {
	return a.pool.Delete(ctx, key)
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
// sweep to read LastModified for the OrphanTTL gate. Mirror of
// cmd/lakehouse-logs/main.go's implementation — keep in sync per
// feedback_logs_traces_module_parity.
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
// standalone /usr/local/bin/healthcheck binary (~3 MB).
//
// Usage: `lakehouse-traces healthcheck [URL]` (default: http://localhost:10428/health)
func runHealthcheckSubcommand() {
	url := "http://localhost:10428/health"
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
// Usage: `lakehouse-traces fips-status`
func runFIPSStatusSubcommand() {
	if fips140Enabled() {
		fmt.Println("fips140: enabled")
		os.Exit(0)
	}
	fmt.Println("fips140: disabled")
	os.Exit(1)
}
