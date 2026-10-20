package main

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/retention"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// Twin of cmd/lakehouse-logs/tombstones.go — keep the two identical.

// tombstoneRetention hands the retention policy to the tombstone view's expiry
// once the tenant policy registry has built it. Until then nothing expires,
// which is the safe direction.
type tombstoneRetention struct {
	policy atomic.Pointer[retention.Policy]
}

func (r *tombstoneRetention) set(p *retention.Policy) { r.policy.Store(p) }

// expiry is delete.Options.Expiry: a tombstone is no longer needed once the data
// it covers is past the longest retention that can apply.
func (r *tombstoneRetention) expiry(t delete.Tombstone) (time.Time, bool) {
	p := r.policy.Load()
	if !p.Enabled() {
		return time.Time{}, false
	}
	return delete.ExpiryAt(t, p.MaxTTL())
}

// attachTombstones connects the pod's view of the deletes to the bucket. Every
// role does: a select pod reads the view to filter, and takes the delete
// requests it is sent; the roles that run maintenance also remove the keys of
// tombstones whose retention has passed. With delete.enabled false the view is
// still kept and enforced, but reads are never refused (GateOff).
func attachTombstones(cfg *config.Config, pool *s3reader.ClientPool, ts *delete.TombstoneStore, ret *tombstoneRetention, tweak ...func(*delete.Options)) error {
	opts := delete.Options{
		Backend:     delete.NewS3Backend(pool),
		Prefix:      cfg.AutoPrefix(),
		GateOff:     !cfg.Delete.Enabled,
		CacheDir:    cfg.Delete.CacheDir,
		Expiry:      ret.expiry,
		Maintenance: cfg.RunsMaintenance(),
	}
	// The poll interval is a constant (one minute); only tests change it.
	for _, f := range tweak {
		f(&opts)
	}
	return ts.Attach(opts)
}

// startTombstoneSync runs the view's poll loop until stopCh closes. The first
// full listing is retried until it succeeds; reads wait for it (and fail closed
// when it does not come), ingest and flush never do.
func startTombstoneSync(ts *delete.TombstoneStore, stopCh <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stopCh
		cancel()
	}()
	go ts.Start(ctx)
	logger.Infof("tombstone sync started")
}

// tombstonesReady is the tombstone part of readiness. A select pod's reads are
// refused until its view of the deletes has been read once (Gate), so it reports
// not ready until then and the load balancer does not send it queries it must
// refuse. Other roles, and a deployment with deletes disabled, are not held.
func tombstonesReady(cfg *config.Config, ts *delete.TombstoneStore) bool {
	if cfg.Role != config.RoleSelect || !cfg.Delete.Enabled {
		return true
	}
	return ts.Synced()
}
