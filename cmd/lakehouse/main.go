package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/election"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/insertapi"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/internalselect"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selectapi"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/startup"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
)

var (
	version = "dev"
)

func main() {
	var (
		configPath  = flag.String("lakehouse.config", "", "Path to YAML config file")
		mode        = flag.String("lakehouse.mode", "", "Operating mode: logs or traces (required)")
		s3Bucket    = flag.String("lakehouse.s3.bucket", "", "S3 bucket name (required)")
		s3Region    = flag.String("lakehouse.s3.region", "", "S3 region")
		s3Prefix    = flag.String("lakehouse.s3.prefix", "", "S3 key prefix")
		s3Endpoint  = flag.String("lakehouse.s3.endpoint", "", "Custom S3 endpoint (MinIO)")
		s3AccessKey = flag.String("lakehouse.s3.access-key", "", "S3 access key")
		s3SecretKey = flag.String("lakehouse.s3.secret-key", "", "S3 secret key")
		s3PathStyle = flag.Bool("lakehouse.s3.force-path-style", false, "Use path-style S3 URLs")
		topology    = flag.String("lakehouse.topology", "", "Deployment topology: auto, storage-node, direct, loki-proxy")
		hotBoundary = flag.String("lakehouse.hot-boundary", "", "Manual hot boundary override (e.g., 7d)")
		role             = flag.String("lakehouse.role", "", "Role: all, insert, select (default: all)")
		flushInterval    = flag.Duration("lakehouse.insert.flush-interval", 0, "Insert flush interval (e.g., 10s)")
		listenAddr       = flag.String("httpListenAddr", "", "HTTP listen address (auto-set from mode)")
		logLevel         = flag.String("loggerLevel", "INFO", "Log level: DEBUG, INFO, WARN, ERROR")
		manifestRefresh  = flag.Duration("lakehouse.manifest.refresh-interval", 0, "Manifest refresh interval (e.g., 30s)")

		compactionEnabled  = flag.Bool("lakehouse.compaction.enabled", false, "Enable compaction scheduler")
		compactionInterval = flag.Duration("lakehouse.compaction.interval", 0, "Compaction scan interval")
		compactionElection = flag.String("lakehouse.compaction.leader-election", "", "Election mode: auto, k8s, s3, none")
	)
	flag.Parse()

	logger := setupLogger(*logLevel)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	applyFlags(cfg, *mode, *role, *s3Bucket, *s3Region, *s3Prefix, *s3Endpoint,
		*s3AccessKey, *s3SecretKey, *s3PathStyle, *topology, *hotBoundary, *manifestRefresh, *flushInterval,
		*compactionEnabled, *compactionInterval, *compactionElection)

	if err := cfg.Validate(); err != nil {
		logger.Error("invalid config", "error", err)
		os.Exit(1)
	}

	addr := *listenAddr
	if addr == "" {
		addr = cfg.ListenAddr()
	}

	logger.Info("starting victoria-lakehouse",
		"version", version,
		"mode", cfg.Mode,
		"role", cfg.Role,
		"topology", cfg.Topology,
		"listen", addr,
		"s3_bucket", cfg.S3.Bucket,
		"s3_region", cfg.S3.Region,
		"s3_prefix", cfg.AutoPrefix(),
	)

	sm := startup.NewManager(logger)

	store, err := parquets3.New(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize storage", "error", err)
		os.Exit(1)
	}

	store.StartWriter()

	var pusher *manifest.Pusher
	if cfg.Discovery.PeerHeadlessService != "" {
		disc := store.Discovery()
		pusher = manifest.NewPusher(manifest.PusherConfig{
			GetPeers:   func() []string { return disc.GetPeers() },
			AuthSecret: cfg.Peer.AuthKey,
			SelfAddr:   addr,
			Logger:     logger,
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
				Logger:            logger,
			},
			K8sConfig: election.K8sElectorConfig{
				LeaseName:     "lakehouse-compaction-" + string(cfg.Mode),
				LeaseDuration: cfg.Compaction.LeaseDuration,
				Logger:        logger,
			},
			Logger: logger,
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
			Logger:           logger,
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

		logger.Info("compaction scheduler started",
			"election", cfg.Compaction.LeaderElection,
			"interval", cfg.Compaction.Interval,
		)
	}

	// --- Delete subsystem ---
	tombstoneStore := delete.NewTombstoneStore()
	if err := tombstoneStore.LoadFromDisk(cfg.Delete.PersistPath); err != nil {
		logger.Warn("failed to load tombstones from disk", "error", err, "path", cfg.Delete.PersistPath)
	}
	if tombstoneStore.Count() == 0 {
		s3Pool := &s3PoolAdapter{pool: store.Pool()}
		if err := tombstoneStore.LoadFromS3(context.Background(), s3Pool, cfg.S3.Bucket, cfg.AutoPrefix()); err != nil {
			logger.Warn("failed to load tombstones from S3", "error", err)
		}
	}
	store.SetTombstoneStore(tombstoneStore)

	// Build lifecycle rules for storage class detection.
	lifecycleRules := make([]delete.LifecycleRule, len(cfg.Delete.LifecycleRules))
	for i, r := range cfg.Delete.LifecycleRules {
		lifecycleRules[i] = delete.LifecycleRule{
			TransitionDays: r.TransitionDays,
			Class:          delete.ParseStorageClass(r.StorageClass),
		}
	}
	detector := delete.NewStorageClassDetector(lifecycleRules)

	rewriter := delete.NewRewriter(store.Pool(), cfg.AutoPrefix(), cfg.Insert.RowGroupSize)

	var rewriteSched *delete.RewriteScheduler
	if cfg.Delete.Enabled {
		rewriteSched = delete.NewRewriteScheduler(delete.RewriteSchedulerConfig{
			Store:          tombstoneStore,
			Rewriter:       rewriter,
			Detector:       detector,
			RewriteDelay:   cfg.Delete.RewriteDelay,
			AllowedClasses: cfg.Delete.AutoRewriteClasses,
			MaxConcurrent:  cfg.Delete.RewriteMaxConcurrent,
			Logger:         logger,
		})
		rewriteSched.Start(cfg.Delete.VerifyInterval)
		logger.Info("delete rewrite scheduler started",
			"rewrite_delay", cfg.Delete.RewriteDelay,
			"verify_interval", cfg.Delete.VerifyInterval,
		)
	}

	mux := newMux(cfg, store, sm, tombstoneStore, detector)

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: cfg.Query.Timeout + 5*time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go runStartup(sm, cfg, logger, store)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		logger.Info("HTTP server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	if rewriteSched != nil {
		rewriteSched.Stop()
	}
	if err := tombstoneStore.PersistToDisk(cfg.Delete.PersistPath); err != nil {
		logger.Error("failed to persist tombstones to disk", "error", err)
	}

	if err := store.Close(); err != nil {
		logger.Error("storage close error", "error", err)
	}

	logger.Info("victoria-lakehouse stopped")
}

func newMux(cfg *config.Config, store *parquets3.Storage, sm *startup.Manager, tombstoneStore *delete.TombstoneStore, detector *delete.StorageClassDetector) *http.ServeMux {
	mux := http.NewServeMux()

	metrics.NewInfoGauge("lakehouse_info", map[string]string{
		"version":  version,
		"mode":     string(cfg.Mode),
		"topology": string(cfg.Topology),
		"role":     string(cfg.Role),
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		metrics.Default().WritePrometheus(w)
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

	mux.HandleFunc("/lakehouse/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version":       version,
			"mode":          cfg.Mode,
			"topology":      cfg.Topology,
			"ready":         sm.IsReady(),
			"phase":         sm.Phase().String(),
			"vl_compat":     "1.50.0",
			"vt_compat":     "0.8.2",
		})
	})

	if cfg.SelectEnabled() {
		isHandler := internalselect.NewHandler(store, sm.Logger(), cfg.Query.Timeout)
		isHandler.Register(mux)

		publicHandler := selectapi.NewHandler(store, sm.Logger(), cfg)
		publicHandler.Register(mux)
	}

	if cfg.InsertEnabled() {
		var bq insertapi.BufferQuerier
		if w := store.Writer(); w != nil {
			bq = w
		}
		ih := insertapi.NewHandler(store, sm.Logger(), cfg, bq)
		ih.Register(mux)
	}

	if cfg.Delete.Enabled && tombstoneStore != nil {
		mq := &manifestQuerierAdapter{m: store.Manifest()}
		dh := delete.NewHandler(tombstoneStore, mq, detector, &cfg.Delete, sm.Logger())
		dh.Register(mux)
	}

	if ph := store.PeerHandler(); ph != nil {
		mux.Handle("/internal/cache/", ph)
	}

	mux.HandleFunc("/internal/manifest/update", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if cfg.Peer.AuthKey != "" {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer "+cfg.Peer.AuthKey {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}

		var update manifest.ManifestUpdate
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		m := store.Manifest()
		for _, fi := range update.Added {
			partition := manifest.ExtractPartition(fi.Key)
			if partition != "" {
				m.AddFile(partition, fi)
			}
		}
		for _, key := range update.Removed {
			partition := manifest.ExtractPartition(key)
			if partition != "" {
				m.RemoveFile(partition, key)
			}
		}

		metrics.ManifestUpdateReceivedTotal.Inc()
		w.WriteHeader(http.StatusOK)
	})

	return mux
}

func runStartup(sm *startup.Manager, cfg *config.Config, logger *slog.Logger, store *parquets3.Storage) {
	sm.SetPhase(startup.PhaseDiskRecovery)
	logger.Info("disk recovery complete")

	sm.SetPhase(startup.PhaseS3Refresh)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := store.RefreshManifest(ctx); err != nil {
		logger.Error("manifest S3 refresh failed", "error", err)
	} else {
		m := store.Manifest()
		logger.Info("manifest S3 refresh complete",
			"files", m.TotalFiles(),
			"bytes", m.TotalBytes(),
			"min_time", m.MinTime(),
			"max_time", m.MaxTime(),
		)
	}

	sm.SetPhase(startup.PhaseReady)

	ticker := time.NewTicker(cfg.Manifest.RefreshInterval)
	defer ticker.Stop()
	for range ticker.C {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := store.RefreshManifest(rctx); err != nil {
			logger.Error("periodic manifest refresh failed", "error", err)
		} else {
			m := store.Manifest()
			logger.Debug("manifest refreshed",
				"files", m.TotalFiles(),
				"bytes", m.TotalBytes(),
			)
		}
		rcancel()
	}
}

func applyFlags(cfg *config.Config, mode, role, bucket, region, prefix, endpoint,
	accessKey, secretKey string, pathStyle bool, topology, hotBoundary string, manifestRefresh time.Duration, flushInterval time.Duration,
	compactionEnabled bool, compactionInterval time.Duration, compactionElection string) {
	if mode != "" {
		cfg.Mode = config.Mode(mode)
	}
	if role != "" {
		cfg.Role = config.Role(role)
	}
	if flushInterval > 0 {
		cfg.Insert.FlushInterval = flushInterval
	}
	if bucket != "" {
		cfg.S3.Bucket = bucket
	}
	if region != "" {
		cfg.S3.Region = region
	}
	if prefix != "" {
		cfg.S3.Prefix = prefix
	}
	if endpoint != "" {
		cfg.S3.Endpoint = endpoint
	}
	if accessKey != "" {
		cfg.S3.AccessKey = accessKey
	}
	if secretKey != "" {
		cfg.S3.SecretKey = secretKey
	}
	if pathStyle {
		cfg.S3.ForcePathStyle = true
	}
	if topology != "" {
		cfg.Topology = config.Topology(topology)
	}
	if hotBoundary != "" {
		cfg.HotBoundary = hotBoundary
	}
	if manifestRefresh > 0 {
		cfg.Manifest.RefreshInterval = manifestRefresh
	}
	if compactionEnabled {
		cfg.Compaction.Enabled = true
	}
	if compactionInterval > 0 {
		cfg.Compaction.Interval = compactionInterval
	}
	if compactionElection != "" {
		cfg.Compaction.LeaderElection = compactionElection
	}
}

func setupLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func hostname() string {
	if h := os.Getenv("POD_NAME"); h != "" {
		return h
	}
	h, _ := os.Hostname()
	return h
}
