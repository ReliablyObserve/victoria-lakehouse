// internal/election/auto.go
package election

import (
	"context"
	"log/slog"
	"os"
)

// AutoElectorConfig configures the AutoElector mode and the underlying
// elector implementations it may delegate to.
type AutoElectorConfig struct {
	Mode      string // "auto", "k8s", "s3", "none"
	S3Store   S3Store
	S3Config  S3ElectorConfig
	K8sConfig K8sElectorConfig
	Logger    *slog.Logger
}

// AutoElector selects and wraps a Leader implementation based on the configured
// Mode, with automatic environment-based fallback when Mode is "auto".
type AutoElector struct {
	inner  Leader
	logger *slog.Logger
}

// NewAutoElector constructs an AutoElector, choosing the inner Leader based on
// cfg.Mode:
//   - "none" or "" → NoopElector (always leader, no coordination)
//   - "s3"         → S3Elector
//   - "k8s"        → K8sElector (falls back to noop on configuration error)
//   - "auto"       → K8s if KUBERNETES_SERVICE_HOST is set, else S3 if S3Store
//     is provided, else noop
func NewAutoElector(cfg AutoElectorConfig) *AutoElector {
	lg := cfg.Logger
	if lg == nil {
		lg = slog.Default()
	}
	var inner Leader
	switch cfg.Mode {
	case "none", "":
		inner = NewNoopElector()
		lg.Info("election mode: none")
	case "s3":
		inner = NewS3Elector(cfg.S3Store, cfg.S3Config)
		lg.Info("election mode: s3")
	case "k8s":
		e, err := NewK8sElector(cfg.K8sConfig)
		if err != nil {
			lg.Error("k8s election failed, falling back to noop", "error", err)
			inner = NewNoopElector()
		} else {
			inner = e
		}
		lg.Info("election mode: k8s")
	case "auto":
		if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
			e, err := NewK8sElector(cfg.K8sConfig)
			if err != nil {
				lg.Warn("k8s election failed, falling back to s3", "error", err)
				inner = NewS3Elector(cfg.S3Store, cfg.S3Config)
			} else {
				inner = e
				lg.Info("election mode: auto → k8s")
			}
		} else {
			if cfg.S3Store != nil {
				inner = NewS3Elector(cfg.S3Store, cfg.S3Config)
				lg.Info("election mode: auto → s3")
			} else {
				inner = NewNoopElector()
				lg.Info("election mode: auto → none")
			}
		}
	default:
		lg.Warn("unknown election mode, defaulting to none", "mode", cfg.Mode)
		inner = NewNoopElector()
	}
	return &AutoElector{inner: inner, logger: lg}
}

// IsLeader delegates to the underlying Leader implementation.
func (a *AutoElector) IsLeader() bool { return a.inner.IsLeader() }

// Start delegates to the underlying Leader implementation.
func (a *AutoElector) Start(ctx context.Context) { a.inner.Start(ctx) }

// Stop delegates to the underlying Leader implementation.
func (a *AutoElector) Stop() { a.inner.Stop() }
