package parquets3

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
)

// newS3DownloadsBound constructs the K8s-style request/limit Bound
// for S3 download concurrency from cfg.S3.Concurrent* (or the
// deprecated cfg.S3.MaxConcurrentDownloads alias). Emits a startup
// warning when the deprecated alias is used.
//
// The bound runs ALONGSIDE the legacy channel-based dlSem rather than
// replacing it: the channel is the wire-level blocking gate (preserves
// observable semantics 1:1 with the pre-bound implementation), and
// the bound publishes the K8s-style request/limit/usage metrics that
// operator dashboards key on. Bound acquire happens AFTER the channel
// admits the request — double-gating in the other order would deadlock
// under contention because both carry the same Limit semantics.
//
// Defaults:
//   - request: 4   (always-reserved baseline)
//   - limit:  16   (matches the legacy flat default)
//
// Validation failure is fatal (logged + os.Exit via logger.Fatalf)
// — a typo in the scaling policy is a misconfiguration.
func newS3DownloadsBound(cfg *config.Config) *resourcebounds.Bound {
	req, lim, pol, deprecated, err := resourcebounds.Resolve(
		int64(cfg.S3.ConcurrentDownloadsRequest),
		int64(cfg.S3.ConcurrentDownloadsLimit),
		cfg.S3.ConcurrentDownloadsScaling,
		int64(cfg.S3.MaxConcurrentDownloads),
		16, // built-in default
	)
	if err != nil {
		logger.Fatalf("invalid -lakehouse.s3.concurrent-downloads.scaling: %s", err)
	}
	if deprecated {
		logger.Warnf("DEPRECATED: -lakehouse.s3.max-concurrent-downloads (or s3.max_concurrent_downloads YAML) is replaced by -lakehouse.s3.concurrent-downloads.{request,limit,scaling}. The flat value is honored as request=limit=%d. Migrate before v1.0.", cfg.S3.MaxConcurrentDownloads)
	}

	// Default request to one quarter of limit when only Limit was set
	// at >=8 (covers the common operator path of bumping concurrency
	// without specifying a baseline). Below 8, request==limit (flat).
	if req == lim && lim >= 8 {
		req = lim / 4
		if req < 1 {
			req = 1
		}
	}

	// Publish the request/limit gauges so operator dashboards can
	// render the K8s-style request vs limit triple alongside usage.
	metrics.ResourceBoundS3ConcurrentDownloadsRequest.Set(req)
	metrics.ResourceBoundS3ConcurrentDownloadsLimit.Set(lim)

	sink := &resourcebounds.PrometheusSink{
		Acquired:         func(n int64) { metrics.ResourceBoundS3ConcurrentDownloadsAcquired.Add(int(n)) },
		Rejected:         func(n int64) { metrics.ResourceBoundS3ConcurrentDownloadsRejected.Add(int(n)) },
		OutstandingBytes: func(v int64) { metrics.ResourceBoundS3ConcurrentDownloadsOutstandingBytes.Set(v) },
		OutstandingCount: func(v int64) { metrics.ResourceBoundS3ConcurrentDownloadsOutstandingCount.Set(v) },
	}

	return resourcebounds.NewBound(resourcebounds.Config{
		Request:    req,
		Limit:      lim,
		LimitCount: int(lim),
		Policy:     pol,
	}, sink)
}

// resourceBoundSet groups the K8s-style request/limit Bounds for the
// five resource surfaces that operator dashboards observe. Each bound
// is constructed alongside the legacy enforcement mechanism that
// already exists for that surface — the bounds add the operator-facing
// request/limit/usage metric contract without changing the load-bearing
// runtime semantics. Bounds with no live acquire site are constructed
// for metric exposure only; the request/limit gauges report the
// operator's configured ceiling and the outstanding gauges report 0
// at idle.
type resourceBoundSet struct {
	S3Downloads    *resourcebounds.Bound
	FileWorkers    *resourcebounds.Bound
	CacheMemory    *resourcebounds.Bound
	SmartCacheDisk *resourcebounds.Bound
	QueryMaxRows   *resourcebounds.Bound
}

// newQueryFileWorkersBound exposes the K8s-style request/limit Bound
// for query.file_workers (process-wide cap on concurrent parquet-file
// readers). The existing per-query worker pool — itself a counting
// semaphore — remains the wire-level gate; the bound publishes
// request/limit/usage metrics for operator dashboards.
//
// Defaults: request=4, limit=16 (matches the production-shape compose
// default at -lakehouse.query.file-workers=16).
func newQueryFileWorkersBound(cfg *config.Config) *resourcebounds.Bound {
	req, lim, pol, deprecated, err := resourcebounds.Resolve(
		int64(cfg.Query.FileWorkersRequest),
		int64(cfg.Query.FileWorkersLimit),
		cfg.Query.FileWorkersScaling,
		int64(cfg.Query.FileWorkers),
		16,
	)
	if err != nil {
		logger.Fatalf("invalid -lakehouse.query.file-workers.scaling: %s", err)
	}
	if deprecated {
		logger.Warnf("DEPRECATED: -lakehouse.query.file-workers is replaced by -lakehouse.query.file-workers.{request,limit,scaling}. Migrate before v1.0.")
	}
	if req == lim && lim >= 8 {
		req = lim / 4
		if req < 1 {
			req = 1
		}
	}
	metrics.ResourceBoundQueryFileWorkersRequest.Set(req)
	metrics.ResourceBoundQueryFileWorkersLimit.Set(lim)
	sink := &resourcebounds.PrometheusSink{
		Acquired:         func(n int64) { metrics.ResourceBoundQueryFileWorkersAcquired.Add(int(n)) },
		Rejected:         func(n int64) { metrics.ResourceBoundQueryFileWorkersRejected.Add(int(n)) },
		OutstandingBytes: func(v int64) { metrics.ResourceBoundQueryFileWorkersOutstandingBytes.Set(v) },
		OutstandingCount: func(v int64) { metrics.ResourceBoundQueryFileWorkersOutstandingCount.Set(v) },
	}
	return resourcebounds.NewBound(resourcebounds.Config{
		Request: req, Limit: lim, LimitCount: int(lim), Policy: pol,
	}, sink)
}

// newCacheMemoryBound exposes the K8s-style request/limit Bound for
// cache.memory (L1 LRU). The LRU eviction mechanism remains the
// load-bearing memory-pressure response; the bound surfaces the
// operator contract via metrics. Default 256 MiB.
func newCacheMemoryBound(cfg *config.Config) *resourcebounds.Bound {
	limFromAlias := cfg.CacheMemoryBytes()
	reqFromTriple := parseSizeOr0(cfg.Cache.MemoryRequest)
	limFromTriple := parseSizeOr0(cfg.Cache.MemoryLimitV2)
	req, lim, pol, deprecated, err := resourcebounds.Resolve(
		reqFromTriple, limFromTriple,
		cfg.Cache.MemoryScaling,
		limFromAlias,
		256*1024*1024,
	)
	if err != nil {
		logger.Fatalf("invalid -lakehouse.cache.memory.scaling: %s", err)
	}
	if deprecated {
		logger.Warnf("DEPRECATED: -lakehouse.cache.memory-mb (or cache.memory_limit YAML) is replaced by -lakehouse.cache.memory.{request,limit,scaling}. Migrate before v1.0.")
	}
	if req == lim && lim >= 64*1024*1024 {
		req = lim / 4
	}
	metrics.ResourceBoundCacheMemoryRequest.Set(req)
	metrics.ResourceBoundCacheMemoryLimit.Set(lim)
	sink := &resourcebounds.PrometheusSink{
		Acquired:         func(n int64) { metrics.ResourceBoundCacheMemoryAcquired.Add(int(n)) },
		Rejected:         func(n int64) { metrics.ResourceBoundCacheMemoryRejected.Add(int(n)) },
		OutstandingBytes: func(v int64) { metrics.ResourceBoundCacheMemoryOutstandingBytes.Set(v) },
		OutstandingCount: func(v int64) { metrics.ResourceBoundCacheMemoryOutstandingCount.Set(v) },
	}
	return resourcebounds.NewBound(resourcebounds.Config{
		Request: req, Limit: lim, LimitCount: 0, Policy: pol,
	}, sink)
}

// newSmartCacheDiskBound exposes the K8s-style request/limit Bound
// for smart_cache.disk. Default 100 GiB.
func newSmartCacheDiskBound(cfg *config.Config) *resourcebounds.Bound {
	limFromAlias := parseSizeOr0(cfg.SmartCache.DiskLimitMax)
	reqFromTriple := parseSizeOr0(cfg.SmartCache.DiskRequest)
	limFromTriple := parseSizeOr0(cfg.SmartCache.DiskLimit)
	req, lim, pol, deprecated, err := resourcebounds.Resolve(
		reqFromTriple, limFromTriple,
		cfg.SmartCache.DiskScaling,
		limFromAlias,
		100*1024*1024*1024,
	)
	if err != nil {
		logger.Fatalf("invalid -lakehouse.smart-cache.disk.scaling: %s", err)
	}
	if deprecated {
		logger.Warnf("DEPRECATED: smart_cache.disk_limit_max YAML is replaced by -lakehouse.smart-cache.disk.{request,limit,scaling}. Migrate before v1.0.")
	}
	if req == lim && lim >= 8*1024*1024*1024 {
		req = lim / 4
	}
	metrics.ResourceBoundSmartCacheDiskRequest.Set(req)
	metrics.ResourceBoundSmartCacheDiskLimit.Set(lim)
	sink := &resourcebounds.PrometheusSink{
		Acquired:         func(n int64) { metrics.ResourceBoundSmartCacheDiskAcquired.Add(int(n)) },
		Rejected:         func(n int64) { metrics.ResourceBoundSmartCacheDiskRejected.Add(int(n)) },
		OutstandingBytes: func(v int64) { metrics.ResourceBoundSmartCacheDiskOutstandingBytes.Set(v) },
		OutstandingCount: func(v int64) { metrics.ResourceBoundSmartCacheDiskOutstandingCount.Set(v) },
	}
	return resourcebounds.NewBound(resourcebounds.Config{
		Request: req, Limit: lim, LimitCount: 0, Policy: pol,
	}, sink)
}

// newQueryMaxRowsBound exposes the K8s-style request/limit Bound for
// query.max_rows. Default 10M rows.
func newQueryMaxRowsBound(cfg *config.Config) *resourcebounds.Bound {
	req, lim, pol, deprecated, err := resourcebounds.Resolve(
		cfg.Query.MaxRowsRequest,
		cfg.Query.MaxRowsLimit,
		cfg.Query.MaxRowsScaling,
		cfg.Query.MaxRows,
		10_000_000,
	)
	if err != nil {
		logger.Fatalf("invalid -lakehouse.query.max-rows.scaling: %s", err)
	}
	if deprecated {
		logger.Warnf("DEPRECATED: -lakehouse.query.max-rows is replaced by -lakehouse.query.max-rows.{request,limit,scaling}. Migrate before v1.0.")
	}
	if req == lim && lim >= 1_000_000 {
		req = lim / 4
	}
	metrics.ResourceBoundQueryMaxRowsRequest.Set(req)
	metrics.ResourceBoundQueryMaxRowsLimit.Set(lim)
	sink := &resourcebounds.PrometheusSink{
		Acquired:         func(n int64) { metrics.ResourceBoundQueryMaxRowsAcquired.Add(int(n)) },
		Rejected:         func(n int64) { metrics.ResourceBoundQueryMaxRowsRejected.Add(int(n)) },
		OutstandingBytes: func(v int64) { metrics.ResourceBoundQueryMaxRowsOutstandingBytes.Set(v) },
		OutstandingCount: func(v int64) { metrics.ResourceBoundQueryMaxRowsOutstandingCount.Set(v) },
	}
	return resourcebounds.NewBound(resourcebounds.Config{
		Request: req, Limit: lim, LimitCount: 0, Policy: pol,
	}, sink)
}

// parseSizeOr0 wraps config.ParseSizeBytes returning 0 on empty or
// parse error. Used by surfaces that accept a Go size string in YAML.
func parseSizeOr0(s string) int64 {
	if s == "" {
		return 0
	}
	n, err := config.ParseSizeBytes(s)
	if err != nil {
		return 0
	}
	return n
}

// newResourceBoundSet constructs all five bounds, populating every
// per-surface request/limit gauge at startup.
func newResourceBoundSet(cfg *config.Config) *resourceBoundSet {
	return &resourceBoundSet{
		S3Downloads:    newS3DownloadsBound(cfg),
		FileWorkers:    newQueryFileWorkersBound(cfg),
		CacheMemory:    newCacheMemoryBound(cfg),
		SmartCacheDisk: newSmartCacheDiskBound(cfg),
		QueryMaxRows:   newQueryMaxRowsBound(cfg),
	}
}
