// internal/election/k8s.go
package election

import (
	"context"
	"os"
	"sync/atomic"
	"time"
)

// K8sElectorConfig holds configuration for the Kubernetes lease-based elector.
type K8sElectorConfig struct {
	LeaseName      string
	LeaseNamespace string
	Identity       string
	LeaseDuration  time.Duration
	RenewDeadline  time.Duration
	RetryPeriod    time.Duration
}

// K8sElector implements Leader using a Kubernetes Lease object for distributed
// leader election.
//
// The k8s.io/client-go-backed run loop lives in k8s_full.go and is only
// compiled in when the `k8s_election` build tag is set. Default ("slim")
// production binaries link the stub in k8s_stub.go instead, which avoids
// pulling in the ~21 MB of code that k8s.io/client-go and its transitive
// dependencies (k8s.io/api/*, gnostic, json-iterator, cbor, apimachinery,
// kube-openapi, structured-merge-diff) contribute to the binary.
//
// In stub mode Start() logs a warning and the elector never becomes leader,
// matching the existing failure-mode behaviour of a failed in-cluster
// configuration. Callers using AutoElector in "auto" mode should additionally
// consult K8sBackendCompiledIn() to avoid selecting K8s when it would be a
// permanent no-op, or rely on AutoElector's own auto-skip logic for "auto"
// mode (added alongside this split).
type K8sElector struct {
	cfg    K8sElectorConfig
	leader atomic.Bool
	cancel context.CancelFunc
}

// k8sRunFunc is the build-tag-injected run loop. It is set by the init() in
// either k8s_full.go (real client-go implementation) or k8s_stub.go (no-op
// that logs once). It is never nil at runtime.
var k8sRunFunc func(ctx context.Context, e *K8sElector)

// k8sBackendCompiledIn is set to true by k8s_full.go's init() when this binary
// includes the real client-go-backed run loop, and stays false in stub builds.
// AutoElector reads it via K8sBackendCompiledIn().
var k8sBackendCompiledIn bool

// K8sBackendCompiledIn reports whether this binary was built with the
// `k8s_election` build tag and therefore has a functional in-cluster K8s
// leader-election backend.
//
// AutoElector consults this in "auto" mode: when the backend is NOT compiled
// in, "auto" falls through the K8s branch even if KUBERNETES_SERVICE_HOST is
// set, so that the configured S3 or noop fallback takes over instead of
// silently never electing.
func K8sBackendCompiledIn() bool { return k8sBackendCompiledIn }

// NewK8sElector constructs a K8sElector, applying defaults for zero-value durations.
//
// Construction always succeeds in both slim and full builds: the K8s API
// contact happens lazily inside Start() / the run loop. This preserves the
// existing test contract (NewK8sElector never errors) and lets AutoElector's
// "k8s" mode wrap the elector even in stub builds (where it will simply never
// become leader, mirroring the in-cluster-config-failed path).
func NewK8sElector(cfg K8sElectorConfig) (*K8sElector, error) {
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 15 * time.Second
	}
	if cfg.RenewDeadline == 0 {
		cfg.RenewDeadline = 10 * time.Second
	}
	if cfg.RetryPeriod == 0 {
		cfg.RetryPeriod = 2 * time.Second
	}
	if cfg.LeaseNamespace == "" {
		cfg.LeaseNamespace = os.Getenv("POD_NAMESPACE")
		if cfg.LeaseNamespace == "" {
			cfg.LeaseNamespace = "default"
		}
	}
	if cfg.Identity == "" {
		cfg.Identity, _ = os.Hostname()
	}
	return &K8sElector{cfg: cfg}, nil
}

// IsLeader reports whether this instance currently holds the Kubernetes lease.
func (e *K8sElector) IsLeader() bool { return e.leader.Load() }

// Start begins the leader election loop in a goroutine. It returns immediately.
func (e *K8sElector) Start(ctx context.Context) {
	ctx, e.cancel = context.WithCancel(ctx)
	go k8sRunFunc(ctx, e)
}

// Stop cancels the leader election loop and marks this instance as non-leader.
func (e *K8sElector) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.leader.Store(false)
}
