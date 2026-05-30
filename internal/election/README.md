# internal/election

Leader-election implementations used by the lakehouse-logs and lakehouse-traces
compaction scheduler, plus any other "single-writer" subsystem that needs an
HA-safe coordinator.

## Backends

| Backend       | Constructor          | Storage / coordination       | Use case                              |
|---------------|----------------------|------------------------------|---------------------------------------|
| `NoopElector` | `NewNoopElector()`   | none (always leader)         | single-node deployments, dev          |
| `S3Elector`   | `NewS3Elector(...)`  | S3 lock file + lease TTL     | non-K8s deployments, multi-cloud      |
| `K8sElector`  | `NewK8sElector(...)` | `coordination.k8s.io/v1` Lease | in-cluster K8s deployments           |
| `AutoElector` | `NewAutoElector(...)`| picks based on env + Mode    | default; safe in all environments     |

All four implement the `Leader` interface (`leader.go`):

```go
type Leader interface {
    Start(ctx context.Context)
    Stop()
    IsLeader() bool
}
```

## K8sElector — what's actually imported

The K8sElector is the most-watched file in this subtree because the entire
`k8s.io/client-go` v0.36 closure weighs ~21 MB on disk (text + pclntab +
DATA_CONST). Pulling the full clientset re-introduces ~700 packages of
transitive Kubernetes API typing that we don't need to acquire and renew a
Lease.

PR #96 rewrote `k8s.go` to talk to the API server directly over REST. The
allowed import surface is intentionally small:

- `k8s.io/client-go/rest` — `InClusterConfig`, `HTTPClientFor`
  (gives us a TLS-validated http.Client bound to the ServiceAccount token).
- `k8s.io/apimachinery/pkg/apis/meta/v1` — `ObjectMeta`, `MicroTime`,
  status payload structs. Lightweight; no scheme registration.

What is **forbidden** (locked by `k8s_regression_test.go::TestNoForbiddenImports`):

- `k8s.io/client-go/kubernetes` — the full clientset wrapper.
- `k8s.io/client-go/tools/leaderelection` — the official leader-elector,
  which in turn imports kubernetes.
- `k8s.io/client-go/tools/leaderelection/resourcelock` — the lock
  abstraction the official elector wires through.
- `k8s.io/api/core/v1`, `k8s.io/api/apps/v1`, `k8s.io/api/resource/v1`,
  `k8s.io/api/admissionregistration/v1` — heavy typed API modules.

If you need a feature that requires any of these, please raise an issue
first. A 14-MB regression here is a 16% bump in the production image and a
~40 MB bump on every CI build cache.

## State machine

```
Init -> Acquiring -> Held -> Renewing -> Released  (Stop)
                                   \_>  Lost      (renew deadline exceeded)
                                          \_>  Acquiring (retry)
```

Detailed transitions:

1. **Init**: constructor returns a `*K8sElector` with no API contact.
2. **Acquiring**: Start spawns a goroutine that GETs the Lease.
   - If the Lease doesn't exist (404): POST a fresh one with our identity.
   - If the Lease exists and is held by someone else and not expired: wait,
     retry every `RetryPeriod`.
   - If the Lease exists and is held by us or is expired: PUT with our
     identity. Server may 409 (CAS lost) or 429 (rate limit) → wait, retry.
3. **Held / Renewing**: a ticker fires every `RetryPeriod`. Each tick:
   - GET the Lease.
   - If holder is no longer us: step down (Lost transition).
   - Otherwise PUT with a fresh RenewTime.
   - If `Now - lastRenew > RenewDeadline`: step down (Lost transition).
4. **Released**: Stop best-effort-clears HolderIdentity and exits the loop
   within `2 * RetryPeriod` under normal conditions.

## Coordination guarantees

- **Mutual exclusion** is enforced by Kubernetes via the Lease's resourceVersion.
  Two candidates GET the same lease, both PUT with their own identity, one
  wins (HTTP 200) and the other loses (HTTP 409). The loser re-GETs and either
  observes the new holder (waits) or re-PUTs (rare; only if the winner crashed).
- **Liveness on holder failure** is bounded by `LeaseDuration`. After a crash,
  a candidate sees `Now - RenewTime > LeaseDurationSeconds` and treats the
  Lease as expired, then takes it.
- **Liveness on partition** is bounded by `RenewDeadline`. If a holder cannot
  reach the apiserver for that long, it steps down voluntarily — preventing
  a stale holder from continuing to act after the network heals (which
  Kubernetes would have already let a new holder take over).

The defaults are `LeaseDuration=15s`, `RenewDeadline=10s`, `RetryPeriod=2s`.
LeaseDuration MUST be strictly greater than RenewDeadline for liveness.

## Operator playbook

When the cluster wedges and you suspect leader election, see
[`RUNBOOK.md`](./RUNBOOK.md) in this directory.

## RBAC

The chart's `compaction-rbac.yaml` template wires a ServiceAccount,
Role, and RoleBinding for `coordination.k8s.io/leases` (`get, list, create,
update, patch`). The chart's negative-control kind e2e test (`tests/e2e-k8s/`)
asserts that removing the RoleBinding makes leader election fail loudly with
a 403 in the logs — so the chart's RBAC is load-bearing, not cosmetic.

## ServiceAccount token rotation (PR #98 Item 1)

The elector reads the SA bearer token via `rest.InClusterConfig` and stores
the on-disk path (`config.BearerTokenFile`) for re-reads. Every API request
calls `bearerTokenForRequest` which re-reads the file (kubelet rotates
projected tokens roughly hourly; without re-reading we would 401 after the
first rotation). A missing-file read briefly falls back to the cached
in-memory token so a single FS hiccup doesn't tear down leadership.

## Startup-error surfacing (PR #98 Item 4)

A pod deployed with `automountServiceAccountToken: false` (or any
misconfigured token projection) has `KUBERNETES_SERVICE_HOST` set but no
token file. `rest.InClusterConfig` does NOT check the token file; the
elector's `bootstrap()` does an explicit `os.Stat` and surfaces the
canonical error `"service account token not found at <path>
(automountServiceAccountToken disabled?)"`. The error is stored on the
elector via `StartupError()` so `main.go` can fail loudly at deployment
time instead of silently never electing.

## Observability metrics (PR #98 Item 8)

The K8sElector emits the following Prometheus metric families. These are
the operator contract — names and label shapes are locked by
`k8s_metrics_test.go::TestK8sElector_EmitsExpectedMetrics`.

| Metric | Type | Labels | When emitted |
|---|---|---|---|
| `lakehouse_leader_election_state` | gauge | `role`, `lease`, `module` | Acquire → set role=leader → 1; Stop / step-down → role=follower → 1 |
| `lakehouse_leader_election_acquire_total` | counter | `lease`, `module` | Successful acquire transition |
| `lakehouse_leader_election_renew_total` | counter | `lease`, `module`, `result` | Each renew attempt (result ∈ success/conflict/failure) |
| `lakehouse_leader_election_release_total` | counter | `lease`, `module` | Successful release at Stop |
| `lakehouse_leader_election_acquire_duration_seconds` | histogram | `lease`, `module` | Time from Start to first IsLeader=true |
| `lakehouse_leader_election_lease_holder` | gauge | `lease`, `module`, `identity` | Set to 1 for the current observed holder; previous holder reset to 0 |
| `lakehouse_leader_election_startup_errors_total` | counter | `lease`, `module` | Each `bootstrap()` failure (InClusterConfig, SA token, HTTPClient) |

Wiring: `main.go` calls `election.SetMetricsHook(metrics.NewElectionHook())`
once at startup. The hook is a thin interface in `internal/election` so the
election package does NOT depend on `internal/metrics` (and its ~190-package
vmmetrics closure), keeping the dep count under the 340 ceiling enforced by
`TestElectionDepCount`.

## Benchmarks (PR #98 Item 11)

Acquire-and-release wall time against an httptest fakeAPIServer with
simulated network latency:

| Scenario | ns/op (M5 Pro, Go 1.26) | Notes |
|---|---|---|
| 0 ms latency (LAN-like) | ~1.4 ms | One GET + one POST (fresh lease) |
| 10 ms latency | ~21 ms | GET + POST round trips |
| 50 ms latency | ~101 ms | Dominated by network |

These are sequential reference points, not SLOs. Re-run via:

```bash
GOWORK=off go test -bench=BenchmarkK8sElector -benchmem ./internal/election/
```

`-count=20` gives p50/p95/p99 distribution.

## K8s version matrix (PR #98 Item 10)

CI runs the kind e2e against K8s **v1.29.14** and **v1.32.5**. Both must
pass `test_leader_election.sh` (5 base sections + items 6/7/8),
`test_token_rotation.sh`, `test_no_sa_token.sh`, and
`test_helm_upgrade.sh`. See `.github/workflows/e2e-k8s.yaml` for the
matrix definition.
