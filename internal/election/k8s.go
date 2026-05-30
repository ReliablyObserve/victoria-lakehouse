// internal/election/k8s.go
//
// Kubernetes leader-election backed by a coordination.k8s.io/v1 Lease, talking
// to the API server directly via REST. This is the always-compiled K8s elector
// for lakehouse-{logs,traces}; the previous build-tag split (full client-go
// vs. stub) has been removed. The hand-rolled state machine pulls in only
// k8s.io/client-go/rest + k8s.io/apimachinery/pkg/apis/meta/v1, which keeps
// the binary at ~7 MB for the election subtree (vs. ~21 MB for the full
// k8s.io/client-go closure).
//
// State machine (see internal/election/README.md and RUNBOOK.md):
//
//	Init -> Acquiring -> Held -> Renewing -> Released (Stop) | Lost (renew deadline)
//	                                       \_> Acquiring (re-attempt)
//
// Coordination guarantees:
//   - Lease conflicts use Kubernetes CAS via resourceVersion + 409 Conflict.
//     A stale write returns 409; we re-GET and retry.
//   - 429 Too Many Requests is treated as a retryable backoff (RetryPeriod).
//   - Renewal failures within RenewDeadline force OnStoppedLeading and a
//     re-Acquire attempt (no infinite hold beyond the deadline).
//   - Stop deletes our HolderIdentity from the Lease (best-effort) and exits
//     the loop within 2 * RetryPeriod under normal conditions.
//
// Test surface: clock + httpClient are interfaces so unit tests can drive
// state-machine transitions deterministically against a httptest.Server.
package election

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// K8sElectorConfig holds configuration for the Kubernetes lease-based elector.
//
// LeaseDuration is how long a holder is allowed to keep the lease before a
// fresh candidate may take it. RenewDeadline is how long the current holder
// is allowed to fail renewals before giving up leadership (must be strictly
// less than LeaseDuration). RetryPeriod is the inter-attempt sleep for both
// acquire and renew loops.
type K8sElectorConfig struct {
	LeaseName      string
	LeaseNamespace string
	Identity       string
	LeaseDuration  time.Duration
	RenewDeadline  time.Duration
	RetryPeriod    time.Duration

	// OnNewLeader fires every time the observed lease holder changes, including
	// the first observation. It is called from the elector goroutine, must not
	// block, and may be nil.
	OnNewLeader func(identity string)

	// OnStoppedLeading fires when this candidate loses leadership (renew
	// deadline exceeded, Stop() called while leading, etc.). It runs from the
	// elector goroutine, must not block, and may be nil.
	OnStoppedLeading func()
}

// Clock is the time source used by the K8sElector. Production uses realClock;
// tests inject a fakeClock to make state transitions deterministic.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
	Sleep(d time.Duration)
}

// Ticker abstracts time.Ticker so a fake clock can replace it in tests.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// HTTPDoer is the subset of *http.Client that the elector actually uses. It
// is satisfied by *http.Client and by httptest-backed mocks.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type realClock struct{}

func (realClock) Now() time.Time        { return time.Now() }
func (realClock) Sleep(d time.Duration) { time.Sleep(d) }
func (realClock) NewTicker(d time.Duration) Ticker {
	return &realTicker{t: time.NewTicker(d)}
}

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

// K8sElector implements Leader using a Kubernetes coordination/v1 Lease as a
// distributed lock. Construction is cheap and never fails for any reason that
// would prevent operation; the API-server contact happens lazily inside Run.
type K8sElector struct {
	cfg K8sElectorConfig

	// apiBase is the API server URL up to /apis. Set from rest.InClusterConfig
	// when Start runs; tests inject directly via newK8sElectorForTest.
	apiBase string
	client  HTTPDoer
	bearer  string
	clock   Clock

	leader atomic.Bool

	// observedHolder is the most recently seen Lease.holderIdentity; used to
	// suppress duplicate OnNewLeader callbacks.
	observedHolder atomic.Value // string

	cancel context.CancelFunc
	doneCh chan struct{}

	// stopOnce guards Stop's release attempt and cancel call.
	stopOnce sync.Once
}

// k8sBackendCompiledIn stays for backward compatibility with AutoElector,
// which used to consult it to skip a stub backend. With the always-on K8s
// implementation this is always true.
var k8sBackendCompiledIn = true

// K8sBackendCompiledIn reports whether the in-cluster K8s leader-election
// backend is available. With the removal of the k8s_election build tag this
// always returns true; the predicate is retained so AutoElector and any
// downstream consumers keep compiling unchanged.
func K8sBackendCompiledIn() bool { return k8sBackendCompiledIn }

// NewK8sElector constructs a K8sElector, applying defaults for zero-value
// durations and reading identity / namespace from environment / hostname.
//
// Construction never fails; in-cluster config errors surface inside the
// Start goroutine, matching the previous client-go-backed behaviour.
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
	return &K8sElector{cfg: cfg, clock: realClock{}}, nil
}

// IsLeader reports whether this instance currently holds the Lease.
func (e *K8sElector) IsLeader() bool { return e.leader.Load() }

// Start begins the leader election loop in a goroutine. It returns
// immediately. The goroutine exits when Stop is called or the context is
// cancelled.
func (e *K8sElector) Start(ctx context.Context) {
	ctx, e.cancel = context.WithCancel(ctx)
	e.doneCh = make(chan struct{})
	go e.run(ctx)
}

// Stop attempts to release the lease (so a successor can take it
// immediately) and then cancels the elector goroutine. It is idempotent and
// safe to call after Start has not been invoked.
func (e *K8sElector) Stop() {
	e.stopOnce.Do(func() {
		// Best-effort release: if we hold the lease, clear holderIdentity
		// before tearing down. This is wrapped in a tight deadline so a
		// hung API server can't block Stop.
		if e.leader.Load() && e.client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = e.releaseLease(ctx)
		}
		if e.cancel != nil {
			e.cancel()
		}
	})
	e.leader.Store(false)
	if e.doneCh != nil {
		select {
		case <-e.doneCh:
		case <-time.After(2 * time.Second):
			// loop is wedged; give up rather than block the caller.
		}
	}
}

// run is the elector main loop. It blocks until ctx is cancelled.
func (e *K8sElector) run(ctx context.Context) {
	defer close(e.doneCh)

	if e.client == nil {
		// First start in a real environment: build the in-cluster client.
		config, err := rest.InClusterConfig()
		if err != nil {
			logger.Errorf("k8s in-cluster config failed: %s", err)
			return
		}
		hc, err := rest.HTTPClientFor(config)
		if err != nil {
			logger.Errorf("k8s http client creation failed: %s", err)
			return
		}
		e.client = hc
		e.apiBase = config.Host
		e.bearer = config.BearerToken
	}

	logger.Infof("k8s leader election starting; identity=%s lease=%s/%s lease_duration=%s renew_deadline=%s retry_period=%s",
		e.cfg.Identity, e.cfg.LeaseNamespace, e.cfg.LeaseName,
		e.cfg.LeaseDuration, e.cfg.RenewDeadline, e.cfg.RetryPeriod)

	for {
		if ctx.Err() != nil {
			return
		}
		// Acquire phase: poll until we own the lease or ctx ends.
		acquired := e.acquireLoop(ctx)
		if !acquired {
			return
		}
		// Renew phase: hold the lease, renewing periodically until we lose
		// it (RenewDeadline exceeded) or ctx ends. On loss we drop back to
		// acquireLoop.
		e.renewLoop(ctx)
	}
}

// acquireLoop repeatedly attempts to acquire the lease until ctx ends. It
// returns true when this candidate has become leader.
func (e *K8sElector) acquireLoop(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		got, err := e.tryAcquire(ctx)
		if err != nil {
			logger.Warnf("k8s lease acquire failed; identity=%s lease=%s/%s err=%s",
				e.cfg.Identity, e.cfg.LeaseNamespace, e.cfg.LeaseName, err)
		}
		if got {
			e.leader.Store(true)
			logger.Infof("k8s leader elected; identity=%s", e.cfg.Identity)
			return true
		}
		// Sleep RetryPeriod (cancellable).
		select {
		case <-ctx.Done():
			return false
		case <-time.After(e.cfg.RetryPeriod):
		}
	}
}

// renewLoop renews the lease at RetryPeriod cadence. If renewals fail for
// longer than RenewDeadline the loop exits, OnStoppedLeading fires, and the
// outer loop falls back to acquireLoop.
func (e *K8sElector) renewLoop(ctx context.Context) {
	ticker := e.clock.NewTicker(e.cfg.RetryPeriod)
	defer ticker.Stop()

	lastRenew := e.clock.Now()

	for {
		select {
		case <-ctx.Done():
			e.fireStoppedLeading("ctx-done")
			return
		case <-ticker.C():
			if e.clock.Now().Sub(lastRenew) > e.cfg.RenewDeadline {
				// Hard deadline: we cannot prove we still hold the lease.
				logger.Warnf("k8s lease renewal deadline exceeded; identity=%s lease=%s/%s deadline=%s",
					e.cfg.Identity, e.cfg.LeaseNamespace, e.cfg.LeaseName, e.cfg.RenewDeadline)
				e.fireStoppedLeading("renew-deadline")
				return
			}
			ok, err := e.tryRenew(ctx)
			if err != nil {
				logger.Warnf("k8s lease renew error; identity=%s err=%s", e.cfg.Identity, err)
				continue
			}
			if !ok {
				// Conflict: another holder took it. Step down immediately.
				logger.Infof("k8s lease lost to another holder; identity=%s", e.cfg.Identity)
				e.fireStoppedLeading("conflict")
				return
			}
			lastRenew = e.clock.Now()
		}
	}
}

// fireStoppedLeading clears leader flag and invokes the OnStoppedLeading
// callback once. The reason is logged at info level for operability.
func (e *K8sElector) fireStoppedLeading(reason string) {
	if !e.leader.CompareAndSwap(true, false) {
		return
	}
	logger.Infof("k8s leadership released; identity=%s reason=%s", e.cfg.Identity, reason)
	if cb := e.cfg.OnStoppedLeading; cb != nil {
		cb()
	}
}

// observeLeader fires OnNewLeader iff the observed holder differs from the
// previous one. Initial observation always fires (zero -> first-holder).
func (e *K8sElector) observeLeader(holder string) {
	prev := ""
	if v := e.observedHolder.Load(); v != nil {
		prev = v.(string)
	}
	if prev == holder {
		return
	}
	e.observedHolder.Store(holder)
	if cb := e.cfg.OnNewLeader; cb != nil {
		cb(holder)
	}
}

// leaseURL builds the API path for our Lease object.
func (e *K8sElector) leaseURL() string {
	return fmt.Sprintf("%s/apis/coordination.k8s.io/v1/namespaces/%s/leases/%s",
		e.apiBase, e.cfg.LeaseNamespace, e.cfg.LeaseName)
}

// leaseListURL builds the API path for the create endpoint.
func (e *K8sElector) leaseListURL() string {
	return fmt.Sprintf("%s/apis/coordination.k8s.io/v1/namespaces/%s/leases",
		e.apiBase, e.cfg.LeaseNamespace)
}

// leaseObject is the on-wire shape of a coordination.k8s.io/v1 Lease. We
// declare a local struct rather than importing k8s.io/api/coordination/v1 to
// avoid pulling the runtime/schema transitive closure.
type leaseObject struct {
	Kind       string            `json:"kind"`
	APIVersion string            `json:"apiVersion"`
	Metadata   metav1.ObjectMeta `json:"metadata"`
	Spec       leaseSpec         `json:"spec"`
}

type leaseSpec struct {
	HolderIdentity       *string           `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds *int32            `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          *metav1.MicroTime `json:"acquireTime,omitempty"`
	RenewTime            *metav1.MicroTime `json:"renewTime,omitempty"`
	LeaseTransitions     *int32            `json:"leaseTransitions,omitempty"`
}

// statusError matches the kubernetes apimachinery Status payload returned on
// non-2xx responses. We only need the Reason field for retry classification.
type statusError struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
	Code    int    `json:"code"`
}

// tryAcquire attempts to GET-then-PUT (or POST) the Lease so that we become
// the holder. Returns true if we successfully wrote our identity.
func (e *K8sElector) tryAcquire(ctx context.Context) (bool, error) {
	current, status, err := e.getLease(ctx)
	if err != nil {
		return false, err
	}
	switch status {
	case http.StatusOK:
		// Lease exists. If we already hold it (e.g. recovered after a brief
		// outage) just take it. Otherwise check expiry and compete.
		holder := ""
		if current.Spec.HolderIdentity != nil {
			holder = *current.Spec.HolderIdentity
		}
		e.observeLeader(holder)
		if holder != "" && holder != e.cfg.Identity {
			if !e.leaseExpired(current) {
				return false, nil
			}
		}
		// Take the lease via PUT with resourceVersion.
		now := metav1.NewMicroTime(e.clock.Now())
		updated := *current
		id := e.cfg.Identity
		updated.Spec.HolderIdentity = &id
		updated.Spec.RenewTime = &now
		if holder != e.cfg.Identity {
			updated.Spec.AcquireTime = &now
			t := int32(0)
			if current.Spec.LeaseTransitions != nil {
				t = *current.Spec.LeaseTransitions
			}
			t++
			updated.Spec.LeaseTransitions = &t
		}
		secs := int32(e.cfg.LeaseDuration / time.Second)
		updated.Spec.LeaseDurationSeconds = &secs
		body, err := json.Marshal(updated)
		if err != nil {
			return false, err
		}
		_, putStatus, err := e.doRequest(ctx, http.MethodPut, e.leaseURL(), body)
		switch {
		case err != nil:
			return false, err
		case putStatus == http.StatusOK || putStatus == http.StatusCreated:
			e.observeLeader(e.cfg.Identity)
			return true, nil
		case putStatus == http.StatusConflict || putStatus == http.StatusTooManyRequests:
			// Lost the CAS race; back off, retry next tick.
			return false, nil
		default:
			return false, fmt.Errorf("k8s lease put returned status %d", putStatus)
		}
	case http.StatusNotFound:
		// Create fresh.
		now := metav1.NewMicroTime(e.clock.Now())
		secs := int32(e.cfg.LeaseDuration / time.Second)
		id := e.cfg.Identity
		zero := int32(0)
		lease := leaseObject{
			Kind:       "Lease",
			APIVersion: "coordination.k8s.io/v1",
			Metadata: metav1.ObjectMeta{
				Name:      e.cfg.LeaseName,
				Namespace: e.cfg.LeaseNamespace,
			},
			Spec: leaseSpec{
				HolderIdentity:       &id,
				LeaseDurationSeconds: &secs,
				AcquireTime:          &now,
				RenewTime:            &now,
				LeaseTransitions:     &zero,
			},
		}
		body, err := json.Marshal(lease)
		if err != nil {
			return false, err
		}
		_, createStatus, err := e.doRequest(ctx, http.MethodPost, e.leaseListURL(), body)
		switch {
		case err != nil:
			return false, err
		case createStatus == http.StatusCreated || createStatus == http.StatusOK:
			e.observeLeader(e.cfg.Identity)
			return true, nil
		case createStatus == http.StatusConflict || createStatus == http.StatusTooManyRequests:
			return false, nil
		default:
			return false, fmt.Errorf("k8s lease create returned status %d", createStatus)
		}
	default:
		return false, fmt.Errorf("k8s lease get returned status %d", status)
	}
}

// tryRenew updates RenewTime on a Lease we already hold. Returns false (no
// error) when another holder has the lease. Returns true,nil on success.
func (e *K8sElector) tryRenew(ctx context.Context) (bool, error) {
	current, status, err := e.getLease(ctx)
	if err != nil {
		return false, err
	}
	if status != http.StatusOK {
		return false, fmt.Errorf("k8s lease get during renew returned status %d", status)
	}
	holder := ""
	if current.Spec.HolderIdentity != nil {
		holder = *current.Spec.HolderIdentity
	}
	e.observeLeader(holder)
	if holder != e.cfg.Identity {
		// Lost the lease.
		return false, nil
	}
	now := metav1.NewMicroTime(e.clock.Now())
	updated := *current
	updated.Spec.RenewTime = &now
	body, err := json.Marshal(updated)
	if err != nil {
		return false, err
	}
	_, putStatus, err := e.doRequest(ctx, http.MethodPut, e.leaseURL(), body)
	switch {
	case err != nil:
		return false, err
	case putStatus == http.StatusOK:
		return true, nil
	case putStatus == http.StatusConflict:
		// Someone else updated since our GET; treat as transient and let the
		// outer renew loop retry until RenewDeadline.
		return true, nil
	case putStatus == http.StatusTooManyRequests:
		return true, nil
	default:
		return false, fmt.Errorf("k8s lease renew put returned status %d", putStatus)
	}
}

// releaseLease clears HolderIdentity so a successor takes the lease quickly.
// Best-effort; errors are returned but the caller may safely ignore.
func (e *K8sElector) releaseLease(ctx context.Context) error {
	current, status, err := e.getLease(ctx)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("k8s lease get during release returned status %d", status)
	}
	holder := ""
	if current.Spec.HolderIdentity != nil {
		holder = *current.Spec.HolderIdentity
	}
	if holder != e.cfg.Identity {
		return nil
	}
	updated := *current
	updated.Spec.HolderIdentity = nil
	body, err := json.Marshal(updated)
	if err != nil {
		return err
	}
	_, putStatus, err := e.doRequest(ctx, http.MethodPut, e.leaseURL(), body)
	if err != nil {
		return err
	}
	if putStatus != http.StatusOK && putStatus != http.StatusConflict {
		return fmt.Errorf("k8s lease release put returned status %d", putStatus)
	}
	return nil
}

// getLease fetches the Lease. Returns the parsed body for 200 responses and
// the HTTP status for the caller to dispatch on. 404 returns (nil, 404, nil).
func (e *K8sElector) getLease(ctx context.Context) (*leaseObject, int, error) {
	body, status, err := e.doRequest(ctx, http.MethodGet, e.leaseURL(), nil)
	if err != nil {
		return nil, 0, err
	}
	if status != http.StatusOK {
		return nil, status, nil
	}
	var lease leaseObject
	if err := json.Unmarshal(body, &lease); err != nil {
		return nil, status, fmt.Errorf("decode lease: %w", err)
	}
	return &lease, status, nil
}

// leaseExpired reports whether the given Lease has aged past its
// LeaseDurationSeconds.
func (e *K8sElector) leaseExpired(l *leaseObject) bool {
	if l == nil || l.Spec.RenewTime == nil || l.Spec.LeaseDurationSeconds == nil {
		return true
	}
	dur := time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second
	return e.clock.Now().Sub(l.Spec.RenewTime.Time) > dur
}

// doRequest is the single HTTP roundtrip helper used by all CRUD calls. It
// returns the raw body, the HTTP status, and any transport error.
func (e *K8sElector) doRequest(ctx context.Context, method, url string, body []byte) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if e.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+e.bearer)
	}
	req.Header.Set("User-Agent", "lakehouse-election/1.0")
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	// On non-2xx, attempt to decode k8s Status for richer error context.
	if resp.StatusCode >= 400 && len(respBody) > 0 {
		var s statusError
		if json.Unmarshal(respBody, &s) == nil && s.Reason != "" {
			return respBody, resp.StatusCode, nil
		}
	}
	return respBody, resp.StatusCode, nil
}

// strPtr is a tiny helper for *string literals.
//
//nolint:unused // retained for test injection helpers.
func strPtr(s string) *string { return &s }

// i32Ptr is a tiny helper for *int32 literals.
//
//nolint:unused // retained for test injection helpers.
func i32Ptr(i int32) *int32 { return &i }

// parseResourceVersion lifts the metadata.resourceVersion as a uint64 for
// optimistic-concurrency reasoning. Returns 0 if absent or non-numeric.
//
//nolint:unused // retained for diagnostics / regression-test invariants.
func parseResourceVersion(rv string) uint64 {
	if rv == "" {
		return 0
	}
	v, err := strconv.ParseUint(rv, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
