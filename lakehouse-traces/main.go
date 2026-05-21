package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
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
	"github.com/ReliablyObserve/victoria-lakehouse/internal/election"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/prefetch"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/startup"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/telemetry"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/ui"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/selectapi"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage/parquets3"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
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
	listenAddr      = flag.String("httpListenAddr", ":10428", "HTTP listen address")
	manifestRefresh = flag.Duration("lakehouse.manifest.refresh-interval", 0, "Manifest refresh interval (e.g., 30s)")

	cacheMemoryMB = flag.Int("lakehouse.cache.memory-mb", 0, "L1 memory cache size in MB (default: 256)")
	cacheDiskPath = flag.String("lakehouse.cache.disk-path", "", "L2 disk cache directory path")
	cacheDiskMB   = flag.Int("lakehouse.cache.disk-max-mb", 0, "L2 disk cache max size in MB (default: 1024)")

	compactionEnabled  = flag.Bool("lakehouse.compaction.enabled", false, "Enable compaction scheduler")
	compactionInterval = flag.Duration("lakehouse.compaction.interval", 0, "Compaction scan interval")
	compactionElection = flag.String("lakehouse.compaction.leader-election", "", "Election mode: auto, k8s, s3, none")

	queryFileWorkers = flag.Int("lakehouse.query.file-workers", 0, "Number of parallel file workers for queries (default: 8)")

	tracesBloomColumns  = flag.String("lakehouse.traces.bloom-columns", "", "Comma-separated bloom filter columns for traces (default: trace_id,service.name)")
	tracesDeletePrefix  = flag.String("lakehouse.traces.delete-prefix", "", "Delete API prefix (default: /delete/tracessql)")
	tracesJaegerEnabled = flag.Bool("lakehouse.traces.jaeger-enabled", true, "Enable Jaeger query API")
	tracesJaegerGRPC    = flag.String("lakehouse.traces.jaeger-grpc-addr", "", "Jaeger gRPC listen address (default: :16685)")

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
	tenantMetricsFormat     = flag.String("lakehouse.tenant.metrics-format", "", "Tenant metrics label format: id, name, both (default: id)")
	tenantAutoRegister      = flag.Bool("lakehouse.tenant.auto-register", false, "Auto-register unknown X-Scope-OrgID tenants")
	tenantAliasSyncInterval = flag.Duration("lakehouse.tenant.alias-sync-interval", 0, "Fleet sync interval for runtime aliases (default: 30s)")
)

func main() {
	buildinfo.Init()
	envflag.Parse()

	logger.InitNoLogFlags()
	memAllowed := memory.Allowed()

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

	addr := *listenAddr
	if addr == ":10428" && cfg.ListenAddr() != "" && cfg.ListenAddr() != ":10428" {
		addr = cfg.ListenAddr()
	}

	logger.Infof("starting lakehouse-traces; version=%s, role=%s, topology=%s, listen=%s, s3_bucket=%s, s3_prefix=%s",
		buildinfo.Version, cfg.Role, cfg.Topology, addr, cfg.S3.Bucket, cfg.AutoPrefix())

	run(cfg, addr)
}

func run(cfg *config.Config, addr string) {
	sm := startup.NewManager()

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

	store.StartWriter()

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

	var sched *compaction.Scheduler
	if cfg.Compaction.Enabled {
		leader := election.NewAutoElector(election.AutoElectorConfig{
			Mode:    cfg.Compaction.LeaderElection,
			S3Store: store.Pool(),
			S3Config: election.S3ElectorConfig{
				LockKey:           cfg.AutoPrefix() + "_compaction_lock.json",
				Identity:          hostname(),
				Address:           addr,
				HeartbeatInterval: cfg.Compaction.S3Heartbeat,
				LockTTL:           cfg.Compaction.S3LockTTL,
			},
			K8sConfig: election.K8sElectorConfig{
				LeaseName:     "lakehouse-compaction-traces",
				LeaseDuration: cfg.Compaction.LeaseDuration,
			},
		})

		elCtx, elCancel := context.WithCancel(context.Background())
		leader.Start(elCtx)

		sentinel := compaction.NewSentinel(store.Pool(), 10*time.Minute)
		policy := compaction.NewLevelPolicy(
			cfg.Compaction.MinFilesL0,
			cfg.Compaction.MinFilesL1,
			cfg.Compaction.MinAge,
		)

		sched = compaction.NewScheduler(compaction.SchedulerConfig{
			Leader:           leader,
			Manifest:         store.Manifest(),
			Pool:             store.Pool(),
			Sentinel:         sentinel,
			Policy:           policy,
			Prefix:           cfg.AutoPrefix(),
			Mode:             cfg.Mode,
			Interval:         cfg.Compaction.Interval,
			MaxConcurrent:    cfg.Compaction.MaxConcurrent,
			RowGroupSize:     cfg.Insert.RowGroupSize,
			CompressionLevel: cfg.Insert.CompressionLevel,
			OnCompacted: func(added []manifest.FileInfo, removed []string) {
				if pusher != nil {
					pusher.Notify(added, removed)
				}
			},
		})
		sched.Start()

		defer func() {
			sched.Stop()
			leader.Stop()
			elCancel()
		}()

		logger.Infof("compaction scheduler started; election=%s, interval=%v",
			cfg.Compaction.LeaderElection, cfg.Compaction.Interval)
	}

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
		tenantKey := writerTenantKey
		w.SetStatsCallback(func(compressedBytes, rawBytes, rows int64, storageClass string) {
			registry.RecordWrite(tenantKey, compressedBytes, rawBytes, rows, storageClass)
		})
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

	// Tenant alias fleet sync
	if resolver != nil && (resolver.HasAliases() || cfg.Tenant.AutoRegister) {
		if disc := store.Discovery(); disc != nil {
			aliasSyncCtx, aliasSyncCancel := context.WithCancel(context.Background())
			aliasSyncPusher := tenant.NewSyncPusher(tenant.SyncPusherConfig{
				Resolver: resolver,
				GetPeers: func() []string { return disc.GetPeers() },
				AuthKey:  cfg.Peer.AuthKey,
				SelfAddr: addr,
				Interval: cfg.Tenant.AliasSyncInterval,
			})
			aliasSyncPusher.Start(aliasSyncCtx)
			defer aliasSyncCancel()
			logger.Infof("tenant alias sync started; interval=%v", cfg.Tenant.AliasSyncInterval)
		}
	}

	mux := newMux(cfg, store, sm, tombstoneStore, detector, registry, cardLimiter, classTracker, costCalc, resolver, persister)

	var handler http.Handler = mux
	if resolver != nil && (resolver.HasAliases() || cfg.Tenant.AutoRegister) {
		handler = resolver.Middleware(mux)
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

	vtinsert.Stop()
	internalselect.Stop()

	if rewriteSched != nil {
		rewriteSched.Stop()
	}
	if err := tombstoneStore.PersistToDisk(cfg.Delete.PersistPath); err != nil {
		logger.Errorf("failed to persist tombstones to disk: %s", err)
	}

	// Final stats snapshot on shutdown
	if cfg.Stats.Enabled && cfg.Stats.SnapshotPrefix != "" {
		snapshotBucket := cfg.Stats.MetaBucket
		if snapshotBucket == "" {
			snapshotBucket = cfg.S3.Bucket
		}
		_ = snapshotBucket
		snapshotKey := cfg.AutoPrefix() + cfg.Stats.SnapshotPrefix + "/snapshot.json"
		if data, err := registry.MarshalSnapshot(); err == nil {
			if err := store.Pool().Upload(context.Background(), snapshotKey, data); err != nil {
				logger.Errorf("failed to persist stats snapshot on shutdown: %s", err)
			}
		}
	}

	if err := store.Close(); err != nil {
		logger.Errorf("storage close error: %s", err)
	}

	logger.Infof("lakehouse-traces stopped")
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

func newMux(cfg *config.Config, store *parquets3.Storage, sm *startup.Manager, tombstoneStore *delete.TombstoneStore, detector *delete.StorageClassDetector, registry *stats.TenantRegistry, cardLimiter *stats.CardinalityLimiter, classTracker *stats.StorageClassTracker, costCalc *stats.CostCalculator, resolver *tenant.TenantResolver, persister *tenant.S3Persister) *http.ServeMux {
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
		if sm.IsReady() {
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprint(w, "READY")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "NOT READY (phase: %s)", sm.Phase())
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

	return mux
}

func runStartup(sm *startup.Manager, cfg *config.Config, store *parquets3.Storage, registry *stats.TenantRegistry, tenantKey string) {
	sm.SetPhase(startup.PhaseDiskRecovery)
	logger.Infof("disk recovery complete")

	sm.SetPhase(startup.PhaseS3Refresh)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := store.RefreshManifest(ctx); err != nil {
		logger.Errorf("manifest S3 refresh failed: %s", err)
	} else {
		m := store.Manifest()
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

	sm.SetPhase(startup.PhaseReady)

	go func() {
		bctx, bcancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer bcancel()
		store.BackfillBloomIndex(bctx)
	}()

	ticker := time.NewTicker(cfg.Manifest.RefreshInterval)
	defer ticker.Stop()
	for range ticker.C {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := store.RefreshManifest(rctx); err != nil {
			logger.Errorf("periodic manifest refresh failed: %s", err)
		} else {
			m := store.Manifest()
			logger.Infof("manifest refreshed; files=%d, bytes=%d", m.TotalFiles(), m.TotalBytes())
			registry.ReconcileWithManifest(tenantKey,
				int64(m.TotalFiles()), m.TotalBytes(), m.TotalRawBytes(), m.TotalRows(),
				m.MinTime().UnixNano(), m.MaxTime().UnixNano())
		}
		rcancel()
	}
}

func applyFlags(cfg *config.Config) {
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
	if b := *s3Bucket; b != "" {
		cfg.S3.Bucket = b
	}
	if r := *s3Region; r != "" {
		cfg.S3.Region = r
	}
	if p := *s3Prefix; p != "" {
		cfg.S3.Prefix = p
	}
	if e := *s3Endpoint; e != "" {
		cfg.S3.Endpoint = e
	}
	if k := *s3AccessKey; k != "" {
		cfg.S3.AccessKey = k
	}
	if k := *s3SecretKey; k != "" {
		cfg.S3.SecretKey = k
	}
	if *s3PathStyle {
		cfg.S3.ForcePathStyle = true
	}
	if t := *topology; t != "" {
		cfg.Topology = config.Topology(t)
	}
	if h := *hotBoundary; h != "" {
		cfg.HotBoundary = h
	}
	if *manifestRefresh > 0 {
		cfg.Manifest.RefreshInterval = *manifestRefresh
	}
	if *cacheMemoryMB > 0 {
		cfg.Cache.MemoryLimit = fmt.Sprintf("%dMB", *cacheMemoryMB)
	}
	if p := *cacheDiskPath; p != "" {
		cfg.Cache.DiskPath = p
	}
	if *cacheDiskMB > 0 {
		cfg.Cache.DiskLimit = fmt.Sprintf("%dMB", *cacheDiskMB)
	}
	if *compactionEnabled {
		cfg.Compaction.Enabled = true
	}
	if *compactionInterval > 0 {
		cfg.Compaction.Interval = *compactionInterval
	}
	if e := *compactionElection; e != "" {
		cfg.Compaction.LeaderElection = e
	}

	if *queryFileWorkers > 0 {
		cfg.Query.FileWorkers = *queryFileWorkers
	}

	if s := *tracesBloomColumns; s != "" {
		cfg.Traces.BloomColumns = strings.Split(s, ",")
	}
	if s := *tracesDeletePrefix; s != "" {
		cfg.Traces.DeletePrefix = s
	}
	if *tracesJaegerEnabled {
		cfg.Traces.JaegerEnabled = true
	}
	if s := *tracesJaegerGRPC; s != "" {
		cfg.Traces.JaegerGRPCAddr = s
	}

	if s := *tenantDefaultAccount; s != "" {
		cfg.Tenant.DefaultAccount = s
	}
	if s := *tenantDefaultProject; s != "" {
		cfg.Tenant.DefaultProject = s
	}
	if s := *tenantHeaderAccount; s != "" {
		cfg.Tenant.HeaderAccount = s
	}
	if s := *tenantHeaderProject; s != "" {
		cfg.Tenant.HeaderProject = s
	}
	if s := *tenantGlobalHeader; s != "" {
		cfg.Tenant.GlobalReadHeader = s
	}
	if s := *tenantGlobalValue; s != "" {
		cfg.Tenant.GlobalReadValue = s
	}
	if s := *tenantGlobalToken; s != "" {
		cfg.Tenant.GlobalReadToken = s
	}
	if s := *tenantIsolation; s != "" {
		cfg.Tenant.Isolation = s
	}
	if s := *tenantPrefixTemplate; s != "" {
		cfg.Tenant.PrefixTemplate = s
	}
	if s := *tenantBucketTemplate; s != "" {
		cfg.Tenant.BucketTemplate = s
	}
	if s := *tenantDefaultPrefix; s != "" {
		cfg.Tenant.DefaultPrefix = s
	}
	if s := *tenantOrgIDHeader; s != "" {
		cfg.Tenant.OrgIDHeader = s
	}
	if s := *tenantMetricsFormat; s != "" {
		cfg.Tenant.MetricsFormat = s
	}
	if *tenantAutoRegister {
		cfg.Tenant.AutoRegister = true
	}
	if *tenantAliasSyncInterval > 0 {
		cfg.Tenant.AliasSyncInterval = *tenantAliasSyncInterval
	}
}

func hostname() string {
	if h := os.Getenv("POD_NAME"); h != "" {
		return h
	}
	h, _ := os.Hostname()
	return h
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
