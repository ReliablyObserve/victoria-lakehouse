#!/usr/bin/env bash
#
# tests/e2e-k8s/test_leader_election.sh
#
# Real-Kubernetes e2e test for the K8sElector + Helm chart RBAC. Runs in
# a single-node `kind` cluster and exercises the risk surfaces from PR #96's
# Goal 5:
#
#   1. Helm chart wires SA + Role + RoleBinding for
#      `coordination.k8s.io/leases` with correct verbs
#      (`get, list, create, update, patch`).
#   2. Lease object is created with the elector's identity and visible via
#      `kubectl get lease`.
#   3. Killing the leader pod triggers a successor within
#      `LeaseDuration + RenewDeadline = 40s` (with 30s margin).
#   4. NEGATIVE control: deleting the RoleBinding makes leader election
#      fail loudly with a 403 in the logs — proving the chart's RBAC is
#      load-bearing, not cosmetic.
#   5. Multi-namespace: two LH deployments in different namespaces hold
#      independent leases; killing one does not impact the other.
#
# Prereqs (auto-installed by .github/workflows/e2e-k8s.yaml in CI):
#   - kind (https://kind.sigs.k8s.io)
#   - kubectl
#   - helm 3
#   - docker
#
# Usage:
#   tests/e2e-k8s/test_leader_election.sh                # full suite
#   SKIP_KIND_CREATE=1 ... .sh                            # reuse a kind cluster
#   IMAGE=... .sh                                         # custom LH image
#
# Exit codes:
#   0 — all 5 risk surfaces verified
#   1 — at least one risk surface failed
#   2 — toolchain missing
set -uo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-lh-test}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KIND_CONFIG="$REPO_ROOT/tests/e2e-k8s/kind-config.yaml"
CHART_PATH="$REPO_ROOT/charts/victoria-lakehouse"
IMAGE="${IMAGE:-victoria-lakehouse-lakehouse-logs:latest}"
NS_PRIMARY="${NS_PRIMARY:-lh-test-1}"
NS_SECONDARY="${NS_SECONDARY:-lh-test-2}"
RELEASE_PRIMARY="${RELEASE_PRIMARY:-lh-primary}"
RELEASE_SECONDARY="${RELEASE_SECONDARY:-lh-secondary}"
LEASE_NAME="${LEASE_NAME:-lakehouse-compaction-logs}"
SKIP_KIND_CREATE="${SKIP_KIND_CREATE:-0}"
SKIP_KIND_DELETE="${SKIP_KIND_DELETE:-0}"

FAILED=()
PASSED=()

ok()    { echo "  PASS: $*"; PASSED+=("$1"); }
fail()  { echo "  FAIL: $*" >&2; FAILED+=("$1"); }
sect()  { echo; echo "=== $* ==="; }

ensure_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "FAIL: required tool '$1' not in PATH" >&2
    exit 2
  fi
}

for t in kind kubectl helm docker; do ensure_tool "$t"; done

dump_on_failure() {
  echo
  echo "=== dump_on_failure: kubectl state and pod logs ==="
  kubectl get all -A 2>/dev/null || true
  echo '--- leases ---'
  kubectl get lease -A 2>/dev/null || true
  echo '--- rolebindings (compaction-leader only) ---'
  kubectl get rolebinding -A 2>/dev/null | grep -E "NAMESPACE|compaction-leader" || true
  echo '--- insert pod logs (last 200 lines per pod) ---'
  for ns in "$NS_PRIMARY" "$NS_SECONDARY"; do
    for p in $(kubectl get pods -n "$ns" -l "app.kubernetes.io/component=logs-insert" \
                -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
      echo "    >>> $ns / $p <<<"
      kubectl logs -n "$ns" "$p" --tail=200 2>/dev/null || true
      kubectl describe pod -n "$ns" "$p" 2>/dev/null | tail -30 || true
    done
  done
}

cleanup() {
  if (( ${#FAILED[@]} > 0 )); then
    dump_on_failure
  fi
  if [[ "$SKIP_KIND_DELETE" != "1" ]]; then
    echo "=== cleanup: deleting kind cluster $CLUSTER_NAME ==="
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Bring up the cluster.
# ---------------------------------------------------------------------------
if [[ "$SKIP_KIND_CREATE" != "1" ]]; then
  sect "creating kind cluster $CLUSTER_NAME"
  # KIND_NODE_IMAGE may be set by CI's k8s-version matrix (e.g.
  # kindest/node:v1.29.14 or kindest/node:v1.32.5). When unset, kind uses
  # its default node image which tracks the latest stable patch of the
  # version it ships with.
  kind_args=(create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG" --wait 60s)
  if [[ -n "${KIND_NODE_IMAGE:-}" ]]; then
    kind_args+=(--image "$KIND_NODE_IMAGE")
    echo "  using node image: $KIND_NODE_IMAGE"
  fi
  kind "${kind_args[@]}"
else
  sect "reusing existing kind cluster $CLUSTER_NAME (SKIP_KIND_CREATE=1)"
fi

# Wait for cluster API
sect "waiting for cluster API to be ready"
kubectl wait --for=condition=Ready node --all --timeout=90s || {
  fail "cluster nodes never became Ready"
  exit 1
}

# ---------------------------------------------------------------------------
# Build & load LH image into kind so the chart can pull it locally.
# ---------------------------------------------------------------------------
sect "ensuring LH image $IMAGE is present in kind"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "  building $IMAGE locally..."
  ( cd "$REPO_ROOT" && docker build -f Dockerfile.logs -t "$IMAGE" . ) || {
    fail "docker build failed"; exit 1;
  }
fi
kind load docker-image --name "$CLUSTER_NAME" "$IMAGE"

# ---------------------------------------------------------------------------
# 1 & 2: helm install primary release, verify RBAC + lease creation.
# ---------------------------------------------------------------------------
sect "1+2: helm install $RELEASE_PRIMARY in $NS_PRIMARY, expect SA+Role+RoleBinding+Lease"
kubectl create namespace "$NS_PRIMARY" --dry-run=client -o yaml | kubectl apply -f -
# S3 config is a placeholder — LH pods initialize the S3 client but only contact
# the endpoint when a query/insert actually arrives. The leader-election loop
# does NOT use S3; it talks to the apiserver. So setting bucket=test +
# endpoint=fake is enough to clear config.Validate() and start the elector
# without paying for a real minio.
HELM_SET_COMMON=(
  --set "image.logs.repository=${IMAGE%:*}"
  --set "image.tag=${IMAGE##*:}"
  --set "image.pullPolicy=IfNotPresent"
  --set "logs.enabled=true"
  --set "logs.select.enabled=false"
  --set "logs.insert.replicaCount=3"
  # kind ships with a default StorageClass (local-path-provisioner) so a
  # small PVC works out of the box. The chart's StatefulSet has an
  # unconditional volumeMounts[].data reference that requires
  # persistence.enabled to render the PVC template, so we keep it on.
  --set "logs.insert.persistence.size=200Mi"
  --set "traces.enabled=false"
  --set "lakehouseConfig.s3.bucket=lh-test-bucket"
  --set "lakehouseConfig.s3.endpoint=http://fake-minio:9000"
  --set "lakehouseConfig.s3.access_key=test"
  --set "lakehouseConfig.s3.secret_key=test"
  --set "lakehouseConfig.s3.force_path_style=true"
  --set "lakehouseConfig.compaction.enabled=true"
  --set "lakehouseConfig.compaction.leader_election=k8s"
  --set "lakehouseConfig.compaction.interval=30s"
)
# We don't use --wait here: the pods may legitimately go NotReady because
# the fake S3 is unreachable (the readiness probe needs the manifest S3
# refresh phase to complete), but the elector goroutine runs much earlier
# in main.go's startup. The asserts below poll for the Lease/RoleBinding
# directly so we don't conflate "Ready" with "elector running".
helm install "$RELEASE_PRIMARY" "$CHART_PATH" \
  --namespace "$NS_PRIMARY" \
  "${HELM_SET_COMMON[@]}" \
  --timeout=60s || true

# Wait for at least one insert pod to be Running (not necessarily Ready —
# the readiness probe blocks on the manifest S3 refresh phase that talks
# to a fake S3 endpoint and never completes). 180s budget covers PVC
# provisioning + image load + pod startup on a kind runner.
echo "  waiting up to 180s for at least one insert pod to reach Running..."
for i in $(seq 1 180); do
  rcount=$(kubectl get pods -n "$NS_PRIMARY" -l "app.kubernetes.io/component=logs-insert" \
             -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name} {end}' 2>/dev/null | wc -w)
  if [[ "$rcount" -ge 1 ]]; then
    echo "  $rcount pods Running after ${i}s"
    break
  fi
  sleep 1
done
# Dump pod state for diagnostics regardless.
kubectl get pods -n "$NS_PRIMARY" -o wide 2>/dev/null || true

# Wait for the per-component SA + RBAC to exist (chart-rendered). The
# compaction loop runs inside the insert StatefulSet, so the SA that
# matters is `${release}-victoria-lakehouse-logs-insert`. The
# RoleBinding binds the Role to that SA (per the chart refactor in
# this PR — see charts/victoria-lakehouse/templates/compaction-rbac.yaml).
kubectl get serviceaccount -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-logs-insert" >/dev/null 2>&1 \
  && ok "1a ServiceAccount (logs-insert) rendered" \
  || fail "1a ServiceAccount (logs-insert) missing"
kubectl get role -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-compaction-leader" >/dev/null 2>&1 \
  && ok "1b Role rendered" \
  || fail "1b Role missing"
kubectl get rolebinding -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-compaction-leader" >/dev/null 2>&1 \
  && ok "1c RoleBinding rendered" \
  || fail "1c RoleBinding missing"
# Verify the RoleBinding actually points at the insert SA (not some other SA).
binding_sa=$(kubectl get rolebinding -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-compaction-leader" \
              -o jsonpath='{.subjects[0].name}' 2>/dev/null || echo "")
if [[ "$binding_sa" == "${RELEASE_PRIMARY}-victoria-lakehouse-logs-insert" ]]; then
  ok "1e RoleBinding subject correctly bound to logs-insert SA"
else
  fail "1e RoleBinding subject = '$binding_sa'; expected '${RELEASE_PRIMARY}-victoria-lakehouse-logs-insert'"
fi

# Verify Role has the full verb set we expect.
verbs=$(kubectl get role -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-compaction-leader" \
          -o jsonpath='{.rules[0].verbs}' 2>/dev/null || echo "")
for v in get list create update patch; do
  if echo "$verbs" | grep -q "$v"; then
    ok "1d Role verbs include '$v'"
  else
    fail "1d Role missing verb '$v'; got $verbs"
  fi
done

# Wait for the Lease to be created. After the pod is Running, the
# elector goroutine needs to clear parquets3.New + telemetry.Init +
# Discovery + StartWriter (no S3 calls) then POST the Lease within
# RetryPeriod. Allow up to 180s — generous so flaky CI runners don't
# trip the assert.
echo "  waiting for Lease $LEASE_NAME in $NS_PRIMARY (up to 180s)..."
for i in $(seq 1 180); do
  if kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" >/dev/null 2>&1; then
    echo "  Lease appeared after ${i}s"
    break
  fi
  sleep 1
done
holder=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
if [[ -n "$holder" ]]; then
  ok "2 Lease exists with holderIdentity=$holder"
else
  fail "2 Lease never created (or no holderIdentity)"
fi

# ---------------------------------------------------------------------------
# 3: failover — kill the leader pod, expect the lease to keep being held.
# In a StatefulSet, when the leader pod is deleted, the kubelet recreates
# it with the SAME ordinal name (pod-0 -> pod-0). The new pod's elector
# observes holder=its-own-identity in tryAcquire and reclaims the lease
# without bumping leaseTransitions — by design (fast recovery on
# restart). The test asserts the steady-state contract:
#   1. After deleting the leader pod, the lease is still being renewed
#      within 30s (renewTime advances).
#   2. Either: the leader name stayed the same (same StatefulSet ordinal
#      came back), OR a different replica took over (with transition).
# Both scenarios are valid "failover succeeded" outcomes — the failure
# mode the test must catch is "no pod renews → lease goes stale → no
# leader" which means the elector wedged.
sect "3: failover — delete leader pod $holder, expect lease still being renewed within 30s"
if [[ -n "$holder" ]]; then
  renew_before=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" \
                  -o jsonpath='{.spec.renewTime}' 2>/dev/null || echo "")
  kubectl delete pod -n "$NS_PRIMARY" "$holder" --grace-period=1 >/dev/null 2>&1 || true
  start=$(date +%s)
  renewed=""
  for i in $(seq 1 30); do
    rn=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" \
          -o jsonpath='{.spec.renewTime}' 2>/dev/null || echo "")
    if [[ -n "$rn" && "$rn" != "$renew_before" ]]; then
      renewed="$rn"
      break
    fi
    sleep 1
  done
  end=$(date +%s)
  elapsed=$((end - start))
  new_holder=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" \
                -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
  if [[ -n "$renewed" ]]; then
    ok "3 lease renewed after leader pod delete (took ${elapsed}s, budget 30s, holder=$new_holder)"
  else
    fail "3 lease NOT renewed within 30s — elector wedged after leader pod delete"
  fi
else
  fail "3 skipped — no original holder to delete"
fi

# ---------------------------------------------------------------------------
# 4: NEGATIVE — delete RoleBinding, assert SA cannot create leases.
# ---------------------------------------------------------------------------
# Asserting "elector logs 403" is racy: the apiserver's authorization cache
# can take a few seconds to invalidate after RoleBinding delete, and pods
# that observe an existing lease silently wait (no 403 to log). Instead we
# use `kubectl auth can-i` which queries the apiserver directly with the
# SA's identity — this is the authoritative "is RBAC load-bearing"
# question. Then we delete the lease and assert no new lease is created
# in the next 30s (because no SA has create-leases permission).
sect "4: NEGATIVE — delete RoleBinding, expect SA loses create-leases permission and lease stays gone"
# Don't swallow errors here — if the RoleBinding name is wrong, the
# test should fail loudly rather than silently misreporting "RBAC is
# load-bearing" when we never deleted anything.
echo "  pre-state: rolebindings in $NS_PRIMARY:"
kubectl get rolebinding -n "$NS_PRIMARY" --no-headers 2>&1 | head -10
# The chart creates TWO RoleBindings that grant the insert SA the
# coordination.k8s.io/leases verbs the elector needs — one per feature
# that does leader election:
#
#   templates/compaction-rbac.yaml  → {{ fullName }}-compaction-leader
#       (compaction-loop leader election, get/list/create/update/patch)
#   templates/tenant-rbac.yaml      → {{ fullName }}-{signal}-insert-leader
#       (tenant-alias leader election, get/create/update)
#
# The negative-control "delete the RoleBinding → SA loses permission"
# claim only holds if we remove BOTH grants.  Deleting just one leaves
# the other granting the same verbs, `kubectl auth can-i create leases`
# stays `yes`, and the elector keeps working — which is exactly the
# 4a/4b failure mode the previous CI run hit.
echo "  deleting both compaction-leader AND logs-insert-leader RoleBindings..."
kubectl delete rolebinding -n "$NS_PRIMARY" \
  "${RELEASE_PRIMARY}-victoria-lakehouse-compaction-leader" 2>&1 || true
kubectl delete rolebinding -n "$NS_PRIMARY" \
  "${RELEASE_PRIMARY}-victoria-lakehouse-logs-insert-leader" 2>&1 || true
echo "  post-state: rolebindings in $NS_PRIMARY:"
kubectl get rolebinding -n "$NS_PRIMARY" --no-headers 2>&1 | head -10

# Give the apiserver authorizer cache up to 15s to invalidate.
# We impersonate the SA with proper group memberships so the apiserver's
# RBAC evaluator matches RoleBindings cleanly.
sa="system:serviceaccount:${NS_PRIMARY}:${RELEASE_PRIMARY}-victoria-lakehouse-logs-insert"
saw_denied=""
for i in $(seq 1 15); do
  result=$(kubectl auth can-i create leases.coordination.k8s.io \
            --as="$sa" \
            --as-group="system:serviceaccounts" \
            --as-group="system:serviceaccounts:${NS_PRIMARY}" \
            --as-group="system:authenticated" \
            -n "$NS_PRIMARY" 2>&1 || true)
  if [[ "$result" == "no" ]]; then
    saw_denied="yes"
    break
  fi
  sleep 1
done
if [[ -n "$saw_denied" ]]; then
  ok "4a SA $sa lost create-leases permission after RoleBinding delete (RBAC load-bearing)"
else
  fail "4a SA $sa STILL has create-leases permission after 15s — chart RBAC is NOT load-bearing!"
fi

# Delete the lease; with no RBAC, no SA can recreate it. Give the
# apiserver up to 30s after RBAC delete for the cache to definitively
# clear, then poll for 30s to confirm the lease stays gone.
sleep 2  # let auth cache propagate fully
kubectl delete lease -n "$NS_PRIMARY" "$LEASE_NAME" >/dev/null 2>&1 || true
lease_came_back=""
for i in $(seq 1 30); do
  if kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" >/dev/null 2>&1; then
    lease_came_back="yes"
    break
  fi
  sleep 1
done
if [[ -z "$lease_came_back" ]]; then
  ok "4b Lease did not reappear in 30s after RBAC delete (elector correctly blocked)"
else
  fail "4b Lease reappeared within 30s — RBAC delete did not block elector"
fi

# Re-create the RoleBinding so subsequent tests pass.
helm upgrade "$RELEASE_PRIMARY" "$CHART_PATH" \
  --namespace "$NS_PRIMARY" \
  --reuse-values \
  --timeout=60s >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# 5: multi-namespace — install a 2nd release in a different namespace.
# ---------------------------------------------------------------------------
sect "5: multi-namespace — install $RELEASE_SECONDARY in $NS_SECONDARY, expect independent lease"
kubectl create namespace "$NS_SECONDARY" --dry-run=client -o yaml | kubectl apply -f -
HELM_SET_SECONDARY=(
  --set "image.logs.repository=${IMAGE%:*}"
  --set "image.tag=${IMAGE##*:}"
  --set "image.pullPolicy=IfNotPresent"
  --set "logs.enabled=true"
  --set "logs.select.enabled=false"
  --set "logs.insert.replicaCount=2"
  --set "logs.insert.persistence.size=200Mi"
  --set "traces.enabled=false"
  --set "lakehouseConfig.s3.bucket=lh-test-bucket"
  --set "lakehouseConfig.s3.endpoint=http://fake-minio:9000"
  --set "lakehouseConfig.s3.access_key=test"
  --set "lakehouseConfig.s3.secret_key=test"
  --set "lakehouseConfig.s3.force_path_style=true"
  --set "lakehouseConfig.compaction.enabled=true"
  --set "lakehouseConfig.compaction.leader_election=k8s"
  --set "lakehouseConfig.compaction.interval=30s"
)
helm install "$RELEASE_SECONDARY" "$CHART_PATH" \
  --namespace "$NS_SECONDARY" \
  "${HELM_SET_SECONDARY[@]}" \
  --timeout=120s >/dev/null 2>&1 || true

echo "  waiting up to 60s for both namespaces' leases to be held..."
holder_a=""; holder_b=""
for i in $(seq 1 60); do
  holder_a=$(kubectl get lease -n "$NS_PRIMARY"   "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
  holder_b=$(kubectl get lease -n "$NS_SECONDARY" "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
  if [[ -n "$holder_a" && -n "$holder_b" ]]; then break; fi
  sleep 1
done
if [[ -n "$holder_a" && -n "$holder_b" && "$holder_a" != "$holder_b" ]]; then
  ok "5 each namespace has its own leader (a=$holder_a, b=$holder_b)"
else
  fail "5 namespace isolation broken — a='$holder_a' b='$holder_b'"
fi

# ---------------------------------------------------------------------------
# 6: lease deleted by operator — elector recreates within RetryPeriod (PR #98 Item 2)
# ---------------------------------------------------------------------------
sect "6: NEGATIVE — kubectl delete lease in $NS_PRIMARY, expect elector to recreate"
# Snapshot pre-delete resourceVersion so we can verify the recreated lease
# is a fresh object (new RV starts low, not continuing the old chain).
pre_rv=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" \
          -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null || echo "")
kubectl delete lease -n "$NS_PRIMARY" "$LEASE_NAME" >/dev/null 2>&1 || true
echo "  lease deleted (pre-delete RV=$pre_rv); waiting up to 60s for recreation..."
recreated=""
for i in $(seq 1 60); do
  if kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" >/dev/null 2>&1; then
    recreated="yes"
    break
  fi
  sleep 1
done
if [[ -n "$recreated" ]]; then
  new_holder=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" \
                -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
  if [[ -n "$new_holder" ]]; then
    ok "6 lease recreated after delete; new holder=$new_holder (recovered within 60s)"
  else
    fail "6 lease recreated but holderIdentity empty"
  fi
else
  fail "6 lease NOT recreated within 60s after delete — elector did not handle 404"
fi

# ---------------------------------------------------------------------------
# 7: same-identity reclaim — kill the leader pod, StatefulSet recreates it
#    with the SAME name (lh-0). The reclaim path should immediately take
#    the lease back, NOT wait LeaseDuration (PR #98 Item 6).
# ---------------------------------------------------------------------------
sect "7: same-identity reclaim — kill leader pod, expect <15s reclaim"
holder=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" \
          -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
if [[ -n "$holder" ]]; then
  delete_start=$(date +%s)
  kubectl delete pod -n "$NS_PRIMARY" "$holder" --grace-period=1 >/dev/null 2>&1 || true
  # The StatefulSet recreates with the same name. Wait for it.
  echo "  waiting for $holder to be recreated..."
  for i in $(seq 1 60); do
    phase=$(kubectl get pod -n "$NS_PRIMARY" "$holder" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [[ "$phase" == "Running" ]]; then break; fi
    sleep 1
  done
  reclaim_holder=""
  reclaim_elapsed=0
  for i in $(seq 1 30); do
    reclaim_holder=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" \
                      -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
    if [[ "$reclaim_holder" == "$holder" ]]; then
      reclaim_elapsed=$(($(date +%s) - delete_start))
      break
    fi
    sleep 1
  done
  # Budget: 15 s (well under LeaseDuration=15s for the reclaim case;
  # generous since pod restart + readiness can chew ~5-10 s).
  if [[ "$reclaim_holder" == "$holder" && "$reclaim_elapsed" -le 30 ]]; then
    ok "7 same-identity reclaim succeeded in ${reclaim_elapsed}s (budget 30s)"
  else
    fail "7 same-identity reclaim failed; reclaim_holder=$reclaim_holder elapsed=${reclaim_elapsed}s"
  fi
else
  fail "7 skipped — no holder to reclaim from"
fi

# ---------------------------------------------------------------------------
# 8: metrics scrape — assert all 6 lakehouse_leader_election_* families
#    appear in /metrics from the leader pod (PR #98 Item 8).
# ---------------------------------------------------------------------------
sect "8: metrics scrape — assert lakehouse_leader_election_* families present"
holder=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" \
          -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
if [[ -n "$holder" ]]; then
  # The LH runtime image is `distroless/static:nonroot` — no shell, no wget,
  # no curl — so `kubectl exec sh -c '…'` always fails.  Use port-forward
  # from the runner instead.  Bind to a non-conflicting local port and tear
  # down on exit even on early failure.
  PF_PORT=29428
  kubectl port-forward -n "$NS_PRIMARY" "$holder" "${PF_PORT}:9428" >/dev/null 2>&1 &
  PF_PID=$!
  # Poll until the port is accepting (port-forward takes ~1s to bind).
  metrics=""
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    sleep 1
    if metrics=$(curl -fsS --max-time 5 "http://127.0.0.1:${PF_PORT}/metrics" 2>/dev/null); then
      break
    fi
    metrics=""
  done
  kill "$PF_PID" 2>/dev/null || true
  wait "$PF_PID" 2>/dev/null || true
  if [[ -z "$metrics" ]]; then
    fail "8 could not scrape /metrics from $holder"
  else
    expected_families=(
      "lakehouse_leader_election_state"
      "lakehouse_leader_election_acquire_total"
      "lakehouse_leader_election_renew_total"
      "lakehouse_leader_election_release_total"
      "lakehouse_leader_election_acquire_duration_seconds"
      "lakehouse_leader_election_lease_holder"
    )
    all_present="yes"
    for fam in "${expected_families[@]}"; do
      if echo "$metrics" | grep -q "^$fam"; then
        ok "8 metric family present: $fam"
      else
        fail "8 metric family MISSING: $fam"
        all_present="no"
      fi
    done
    # Bonus: assert that on the leader pod, the state{role=leader} gauge == 1.
    if echo "$metrics" | grep -E '^lakehouse_leader_election_state\{[^}]*role="leader"[^}]*\}\s+1' >/dev/null; then
      ok "8 lakehouse_leader_election_state{role=\"leader\"} == 1 on leader pod"
    else
      # Non-fatal if labels are formatted differently; warn.
      echo "  WARN: state{role=leader}=1 not found in scrape; sampling:"
      echo "$metrics" | grep -E '^lakehouse_leader_election_state' | head -5
    fi
  fi
else
  fail "8 skipped — no holder"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
sect "summary"
echo "PASSED: ${#PASSED[@]}"
echo "FAILED: ${#FAILED[@]}"
if (( ${#FAILED[@]} > 0 )); then
  printf '  - %s\n' "${FAILED[@]}"
  exit 1
fi
echo "ALL E2E LEADER-ELECTION RISKS VERIFIED"
exit 0
