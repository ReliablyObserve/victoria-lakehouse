# K8sElector runbook

Operational playbook for diagnosing leader-election issues in
lakehouse-logs and lakehouse-traces deployments.

## Quick triage

Symptom: no compaction running; manifest never gets new daily-rollup files.

```bash
# 1. Who does the cluster THINK is the leader?
kubectl get lease -n <ns> lakehouse-compaction-logs \
  -o jsonpath='{.spec.holderIdentity}{"\n"}{.spec.renewTime}{"\n"}'

# 2. Do the actual pods agree?
kubectl get pods -n <ns> -l app.kubernetes.io/component=insert -o wide
for p in $(kubectl get pods -n <ns> -l app.kubernetes.io/component=insert \
              -o jsonpath='{.items[*].metadata.name}'); do
  kubectl logs -n <ns> "$p" --tail=200 | grep -E "k8s leader|leadership"
done

# 3. Is RBAC working?
kubectl auth can-i create leases.coordination.k8s.io \
  --as=system:serviceaccount:<ns>:<sa-name> -n <ns>
# Expect: yes
```

If any of the above shows confusion (no holder, multiple holders, RBAC
denied), follow one of the detailed sections below.

---

## Case 1: no leader, no logs about election

**Cause**: `leaderElection` flag is set to `none` or the chart is deployed
without RBAC.

```bash
# Check the rendered deployment:
helm get values <release> -n <ns> | grep -E 'leader_election|serviceAccount'

# Check the chart actually rendered RBAC for the SA:
kubectl get role,rolebinding -n <ns> | grep leader
```

**Fix**: set `lakehouseConfig.compaction.leader_election: auto` (or `k8s`)
in values.yaml and `helm upgrade`. Ensure the Role grants
`coordination.k8s.io/leases` verbs `get, list, create, update, patch`.

---

## Case 2: 403 Forbidden on Lease GET/CREATE

**Symptom**: log line `k8s lease acquire failed; err=k8s lease get returned status 403`.

**Cause**: the Pod's ServiceAccount cannot access
`coordination.k8s.io/leases` in the namespace.

```bash
# Confirm the SA mounted is what the chart says:
kubectl get pod -n <ns> <pod-name> -o jsonpath='{.spec.serviceAccountName}'

# Confirm the RoleBinding exists and points at THAT SA:
kubectl get rolebinding -n <ns> -o yaml | grep -B2 -A10 <sa-name>
```

**Fix**: re-apply the chart so the RBAC manifests are re-rendered. If you
disabled the chart's RBAC manifests for any reason, you must supply your
own equivalent — see `charts/victoria-lakehouse/templates/compaction-rbac.yaml`
for the exact role.

---

## Case 3: leader keeps flapping every ~30 seconds

**Symptom**: log lines `k8s leader elected; identity=A` followed by
`k8s leadership released; identity=A reason=renew-deadline` followed by
`k8s leader elected; identity=B` repeating.

**Cause**: the holder cannot reach the apiserver fast enough to renew
within RenewDeadline (10s). Possible underlying causes:

1. CNI network is congested or has high latency to the apiserver.
2. Pod is under throttling (CPU limit too low; check
   `kube_pod_container_status_throttled` metrics).
3. Apiserver is overloaded (check `apiserver_request_duration_seconds`
   p99 for `verb=PUT,resource=leases`).

```bash
# Check pod CPU throttling:
kubectl describe pod -n <ns> <leader-pod> | grep -i throttl

# Time a manual lease PUT from the pod's network namespace:
kubectl exec -n <ns> <leader-pod> -- sh -c \
  'time curl -kI --cert /var/run/secrets/.../ca.crt \
       https://kubernetes.default.svc/healthz'
```

**Fix**: increase `RenewDeadline` if your apiserver p99 is legitimately
slow. Or raise the Pod's CPU limit so renewals don't get scheduled out.

---

## Case 4: two leaders simultaneously

**Symptom**: two pods both report `IsLeader()=true` in their `/metrics`.

This should be impossible (Kubernetes CAS on resourceVersion guarantees
mutual exclusion) but in practice can be observed during a kubelet partition
where the leader's PUT failed to reach the apiserver but the pod itself
hasn't yet hit RenewDeadline.

**Verify it's real:**

```bash
# The Lease can only ever have ONE holderIdentity. Confirm with the
# server, not with pod-local flags:
kubectl get lease -n <ns> lakehouse-compaction-logs \
  -o jsonpath='{.spec.holderIdentity}{"\n"}'
```

If the lease only has one holder, the "two leaders" you saw was a stale
in-pod flag during the RenewDeadline window. Both pods can briefly believe
they're the leader; only the one actually winning CAS will succeed at any
compaction write. This is acceptable for our use case (compaction is
idempotent on the manifest), and the stale pod will step down within
`RenewDeadline = 10s`.

If the lease actually shows two holders or oscillates between holders
faster than `LeaseDuration`, the apiserver is misbehaving. Open a
Kubernetes issue with the lease object dump and apiserver logs.

---

## Case 5: lease never created, no pod ever becomes leader

**Symptom**: `kubectl get lease -n <ns>` returns "No resources found".
Pods show `k8s lease acquire failed; err=...`.

**Likely cause**: `rest.InClusterConfig()` failed because the pod is not
running inside a real Kubernetes cluster (e.g., docker compose, KinD without
proper SA mounting).

```bash
# Verify the ServiceAccount mount inside the pod:
kubectl exec -n <ns> <pod-name> -- ls -la /var/run/secrets/kubernetes.io/serviceaccount/
# Expect: ca.crt, namespace, token
```

**Fix**: set `lakehouseConfig.compaction.leader_election: s3` for non-K8s
deployments (this is the chart default for plain docker compose).

---

## Useful queries

```bash
# Watch leadership transitions in real time:
kubectl get lease -n <ns> lakehouse-compaction-logs -w

# Find which pod IS the leader right now:
HOLDER=$(kubectl get lease -n <ns> lakehouse-compaction-logs \
           -o jsonpath='{.spec.holderIdentity}')
kubectl get pods -n <ns> -o jsonpath='{range .items[?(@.metadata.name=="'"$HOLDER"'")]}{.spec.nodeName}{"\n"}{end}'

# Force a failover (smoke test):
kubectl delete pod -n <ns> "$HOLDER"
# Expect: another pod picks up within LeaseDuration + RenewDeadline = ~25-40s.
```

## Related code

- `internal/election/k8s.go` — the state machine itself.
- `internal/election/k8s_regression_test.go::TestRenewDeadline_LeaderExits`
  — the test that locks the renew-deadline behaviour.
- `tests/verification/probe_k8s_election_failover.sh` — httptest-based
  smoke test of the failover semantics.
- `tests/e2e-k8s/test_leader_election.sh` — full kind-cluster smoke test
  including RBAC negative control and multi-namespace.
- `charts/victoria-lakehouse/templates/compaction-rbac.yaml` — the RBAC
  surface that makes the K8sElector work; rendered when
  `compaction.leader_election` is `auto` or `k8s`.
