// internal/election/k8s.go
package election

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// K8sElectorConfig holds configuration for the Kubernetes lease-based elector.
type K8sElectorConfig struct {
	LeaseName      string
	LeaseNamespace string
	Identity       string
	LeaseDuration  time.Duration
	RenewDeadline  time.Duration
	RetryPeriod    time.Duration
	Logger         *slog.Logger
}

// K8sElector implements Leader using a Kubernetes Lease object for distributed
// leader election.
type K8sElector struct {
	cfg    K8sElectorConfig
	leader atomic.Bool
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewK8sElector constructs a K8sElector, applying defaults for zero-value durations.
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
	lg := cfg.Logger
	if lg == nil {
		lg = slog.Default()
	}
	return &K8sElector{cfg: cfg, logger: lg.With("component", "election.k8s")}, nil
}

// IsLeader reports whether this instance currently holds the Kubernetes lease.
func (e *K8sElector) IsLeader() bool { return e.leader.Load() }

// Start begins the leader election loop in a goroutine. It returns immediately.
func (e *K8sElector) Start(ctx context.Context) {
	ctx, e.cancel = context.WithCancel(ctx)
	go e.run(ctx)
}

// Stop cancels the leader election loop and marks this instance as non-leader.
func (e *K8sElector) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.leader.Store(false)
}

func (e *K8sElector) run(ctx context.Context) {
	config, err := rest.InClusterConfig()
	if err != nil {
		e.logger.Error("k8s in-cluster config failed", "error", err)
		return
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		e.logger.Error("k8s client creation failed", "error", err)
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
				e.logger.Info("compaction leader elected", "identity", e.cfg.Identity)
			},
			OnStoppedLeading: func() {
				e.leader.Store(false)
				e.logger.Info("compaction leadership lost", "identity", e.cfg.Identity)
			},
			OnNewLeader: func(identity string) {
				e.logger.Info("new compaction leader", "leader", identity)
			},
		},
	})
}
