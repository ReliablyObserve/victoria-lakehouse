//go:build k8s_election
// +build k8s_election

// internal/election/k8s_full.go
//
// Full in-cluster Kubernetes-backed leader election. This file is compiled
// only when the `k8s_election` build tag is set. It pulls in the entire
// k8s.io/client-go transitive closure (k8s.io/api/*, apimachinery,
// kube-openapi, gnostic, json-iterator, cbor, structured-merge-diff, ...)
// which contributes approximately 21 MB to the linked binary.
//
// Default production builds omit this file and link the no-op stub in
// k8s_stub.go instead; AutoElector consults K8sBackendCompiledIn() to skip
// the K8s branch in "auto" mode when the backend is not available.
package election

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func init() {
	k8sRunFunc = runK8sFull
	k8sBackendCompiledIn = true
}

func runK8sFull(ctx context.Context, e *K8sElector) {
	config, err := rest.InClusterConfig()
	if err != nil {
		logger.Errorf("k8s in-cluster config failed: %s", err)
		return
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Errorf("k8s client creation failed: %s", err)
		return
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      e.cfg.LeaseName,
			Namespace: e.cfg.LeaseNamespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: e.cfg.Identity,
		},
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   e.cfg.LeaseDuration,
		RenewDeadline:   e.cfg.RenewDeadline,
		RetryPeriod:     e.cfg.RetryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(_ context.Context) {
				e.leader.Store(true)
				logger.Infof("compaction leader elected; identity=%s", e.cfg.Identity)
			},
			OnStoppedLeading: func() {
				e.leader.Store(false)
				logger.Infof("compaction leadership lost; identity=%s", e.cfg.Identity)
			},
			OnNewLeader: func(identity string) {
				logger.Infof("new compaction leader; leader=%s", identity)
			},
		},
	})
}
