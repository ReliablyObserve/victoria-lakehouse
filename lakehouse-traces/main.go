package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/election"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/insertapi"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/startup"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/internalselect"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/selectapi"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage/parquets3"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	flushInterval   = flag.Duration("lakehouse.insert.flush-interval", 0, "Insert flush interval (e.g., 10s)")
	listenAddr      = flag.String("httpListenAddr", ":10428", "HTTP listen address")
	manifestRefresh = flag.Duration("lakehouse.manifest.refresh-interval", 0, "Manifest refresh interval (e.g., 30s)")

	cacheMemoryMB = flag.Int("lakehouse.cache.memory-mb", 0, "L1 memory cache size in MB (default: 256)")
	cacheDiskPath = flag.String("lakehouse.cache.disk-path", "", "L2 disk cache directory path")
	cacheDiskMB   = flag.Int("lakehouse.cache.disk-max-mb", 0, "L2 disk cache max size in MB (default: 1024)")

	compactionEnabled  = flag.Bool("lakehouse.compaction.enabled", false, "Enable compaction scheduler")
	compactionInterval = flag.Duration("lakehouse.compaction.interval", 0, "Compaction scan interval")
	compactionElection = flag.String("lakehouse.compaction.leader-election", "", "Election mode: auto, k8s, s3, none")

	tracesBloomColumns = flag.String("lakehouse.traces.bloom-columns", "", "Comma-separated bloom filter columns for traces (default: trace_id,service.name)")
	tracesDeletePrefix = flag.String("lakehouse.traces.delete-prefix", "", "Delete API prefix (default: /delete/tracessql)")
	tracesJaegerEnabled = flag.Bool("lakehouse.traces.jaeger-enabled", true, "Enable Jaeger query API")
	tracesJaegerGRPC    = flag.String("lakehouse.traces.jaeger-grpc-addr", "", "Jaeger gRPC listen address (default: :16685)")
)

func main() {
	buildinfo.Init()
	envflag.Parse()

	logger.InitNoLogFlags()
	memAllowed := memory.Allowed()

	logger.Infof("lakehouse-traces starting; vt_compat=%s, memory_allowed_bytes=%d", vtCompat, memAllowed)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Errorf("failed to load config: %s", err)
		os.Exit(1)
	}

	cfg.Mode = config.ModeTraces
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

	store.StartWriter()

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

	mux := newMux(cfg, store, sm, tombstoneStore, detector)

	requestHandler := func(w http.ResponseWriter, r *http.Request) bool {
		mux.ServeHTTP(w, r)
		return true
	}

	go runStartup(sm, cfg, store)

	httpserver.Serve([]string{addr}, requestHandler, httpserver.ServeOptions{})
	logger.Infof("lakehouse-traces listening; addr=%s", addr)

	sig := procutil.WaitForSigterm()
	logger.Infof("shutdown signal received; signal=%v", sig)

	if err := httpserver.Stop([]string{addr}); err != nil {
		logger.Errorf("HTTP server shutdown error: %s", err)
	}

	if rewriteSched != nil {
		rewriteSched.Stop()
	}
	if err := tombstoneStore.PersistToDisk(cfg.Delete.PersistPath); err != nil {
		logger.Errorf("failed to persist tombstones to disk: %s", err)
	}

	if err := store.Close(); err != nil {
		logger.Errorf("storage close error: %s", err)
	}

	logger.Infof("lakehouse-traces stopped")
}

func newMux(cfg *config.Config, store *parquets3.Storage, sm *startup.Manager, tombstoneStore *delete.TombstoneStore, detector *delete.StorageClassDetector) *http.ServeMux {
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

	mux.HandleFunc("/lakehouse/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version":   buildinfo.Version,
			"mode":      "traces",
			"topology":  cfg.Topology,
			"ready":     sm.IsReady(),
			"phase":     sm.Phase().String(),
			"vt_compat": vtCompat,
		})
	})

	if cfg.SelectEnabled() {
		isHandler := internalselect.NewHandler(store, cfg.Query.Timeout, tombstoneStore)
		isHandler.Register(mux)

		publicHandler := selectapi.NewHandler(store, cfg)
		publicHandler.Register(mux)
	}

	if cfg.InsertEnabled() {
		var bq insertapi.BufferQuerier
		if w := store.Writer(); w != nil {
			bq = w
		}
		ih := insertapi.NewHandler(store, cfg, bq)
		ih.Register(mux)
	}

	if cfg.Delete.Enabled && tombstoneStore != nil {
		mq := &manifestQuerierAdapter{m: store.Manifest()}
		dh := delete.NewHandler(tombstoneStore, mq, detector, &cfg.Delete, "traces")
		dh.Register(mux)
	}

	mux.HandleFunc("/internal/cache/clear", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		store.ClearCaches()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"cleared": true})
	})

	mux.HandleFunc("/internal/cache/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		stats := store.MemCacheStats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"l1_entries":   stats.Entries,
			"l1_size":      stats.Size,
			"l1_max_size":  stats.MaxSize,
			"l1_hits":      stats.Hits,
			"l1_misses":    stats.Misses,
			"l1_evictions": stats.Evictions,
		})
	})

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

func runStartup(sm *startup.Manager, cfg *config.Config, store *parquets3.Storage) {
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
		store.WarmLabelIndex(ctx)
	}

	sm.SetPhase(startup.PhaseReady)

	ticker := time.NewTicker(cfg.Manifest.RefreshInterval)
	defer ticker.Stop()
	for range ticker.C {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := store.RefreshManifest(rctx); err != nil {
			logger.Errorf("periodic manifest refresh failed: %s", err)
		} else {
			m := store.Manifest()
			logger.Infof("manifest refreshed; files=%d, bytes=%d", m.TotalFiles(), m.TotalBytes())
		}
		rcancel()
	}
}

func applyFlags(cfg *config.Config) {
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
}

func hostname() string {
	if h := os.Getenv("POD_NAME"); h != "" {
		return h
	}
	h, _ := os.Hostname()
	return h
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
