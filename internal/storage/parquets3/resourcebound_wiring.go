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
