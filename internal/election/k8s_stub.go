//go:build !k8s_election
// +build !k8s_election

// internal/election/k8s_stub.go
//
// Stub Kubernetes-backed leader election. Linked when the `k8s_election`
// build tag is NOT set, which is the default for production images. It
// deliberately avoids importing any k8s.io/* package; that transitive closure
// is approximately 21 MB of code + PC-line tables that the slim build does
// not pay for.
//
// Behaviour mirrors the existing failure path of the real run loop (e.g.
// when in-cluster config is unavailable): the elector logs a warning once
// and never becomes leader. AutoElector callers already cope with this case
// — "auto" mode now consults K8sBackendCompiledIn() and falls through the
// K8s branch when the backend is stubbed, preferring the configured S3 or
// noop fallback instead of silently never electing.
//
// To re-enable the real in-cluster K8s leader election, rebuild with
//
//	make build BUILD_TAGS=k8s_election
//
// or for Docker
//
//	docker build --build-arg BUILD_TAGS=k8s_election ...
package election

import (
	"context"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

var k8sStubWarnOnce sync.Once

func init() {
	k8sRunFunc = runK8sStub
	// k8sBackendCompiledIn stays at its zero value (false).
}

func runK8sStub(_ context.Context, e *K8sElector) {
	k8sStubWarnOnce.Do(func() {
		logger.Warnf("k8s leader election requested but binary built without k8s_election tag; this elector will never become leader (identity=%s, lease=%s/%s). Rebuild with -tags k8s_election to enable, or switch to s3/none election mode.",
			e.cfg.Identity, e.cfg.LeaseNamespace, e.cfg.LeaseName)
	})
}
