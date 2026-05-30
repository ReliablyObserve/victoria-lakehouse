// internal/election/auto.go
package election

import (
	"context"
	"os"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// AutoElectorConfig configures the AutoElector mode and the underlying
// elector implementations it may delegate to.
type AutoElectorConfig struct {
	Mode      string // "auto", "k8s", "s3", "none"
	S3Store   S3Store
	S3Config  S3ElectorConfig
	K8sConfig K8sElectorConfig

	// newK8sElector overrides the K8s elector constructor for testing.
	// If nil, NewK8sElector is used.
	newK8sElector func(K8sElectorConfig) (*K8sElector, error)
}

// AutoElector selects and wraps a Leader implementation based on the configured
// Mode, with automatic environment-based fallback when Mode is "auto".
type AutoElector struct {
	inner Leader
}

// NewAutoElector constructs an AutoElector, choosing the inner Leader based on
// cfg.Mode:
//   - "none" or "" → NoopElector (always leader, no coordination)
//   - "s3"         → S3Elector
//   - "k8s"        → K8sElector (falls back to noop on configuration error)
//   - "auto"       → K8s if KUBERNETES_SERVICE_HOST is set, else S3 if S3Store
//     is provided, else noop
func NewAutoElector(cfg AutoElectorConfig) *AutoElector {
	newK8s := cfg.newK8sElector
	if newK8s == nil {
		newK8s = NewK8sElector
	}

	var inner Leader
	switch cfg.Mode {
	case "none", "":
		inner = NewNoopElector()
		logger.Infof("election mode: none")
	case "s3":
		inner = NewS3Elector(cfg.S3Store, cfg.S3Config)
		logger.Infof("election mode: s3")
	case "k8s":
		e, err := newK8s(cfg.K8sConfig)
		if err != nil {
			logger.Errorf("k8s election failed, falling back to noop: %s", err)
			inner = NewNoopElector()
		} else {
			inner = e
		}
		logger.Infof("election mode: k8s")
	case "auto":
		// Only attempt K8s if both (a) we're actually in a cluster and (b) the
		// k8s_election build tag is compiled in. Stub builds advertise no K8s
		// backend, so we fall through to the configured S3/noop fallback
		// instead of silently never electing.
		if os.Getenv("KUBERNETES_SERVICE_HOST") != "" && K8sBackendCompiledIn() {
			e, err := newK8s(cfg.K8sConfig)
			if err != nil {
				logger.Warnf("k8s election failed, falling back to s3: %s", err)
				inner = NewS3Elector(cfg.S3Store, cfg.S3Config)
			} else {
				inner = e
				logger.Infof("election mode: auto -> k8s")
			}
		} else {
			if os.Getenv("KUBERNETES_SERVICE_HOST") != "" && !K8sBackendCompiledIn() {
				logger.Warnf("election mode: auto -> k8s requested but binary built without k8s_election tag; falling back to s3/none")
			}
			if cfg.S3Store != nil {
				inner = NewS3Elector(cfg.S3Store, cfg.S3Config)
				logger.Infof("election mode: auto -> s3")
			} else {
				inner = NewNoopElector()
				logger.Infof("election mode: auto -> none")
			}
		}
	default:
		logger.Warnf("unknown election mode, defaulting to none; mode=%s", cfg.Mode)
		inner = NewNoopElector()
	}
	return &AutoElector{inner: inner}
}

// IsLeader delegates to the underlying Leader implementation.
func (a *AutoElector) IsLeader() bool { return a.inner.IsLeader() }

// Start delegates to the underlying Leader implementation.
func (a *AutoElector) Start(ctx context.Context) { a.inner.Start(ctx) }

// Stop delegates to the underlying Leader implementation.
func (a *AutoElector) Stop() { a.inner.Stop() }
