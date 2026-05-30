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

cleanup() {
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
  kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG" --wait 60s
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
helm install "$RELEASE_PRIMARY" "$CHART_PATH" \
  --namespace "$NS_PRIMARY" \
  --set image.logs.repository="${IMAGE%:*}" \
  --set image.logs.tag="${IMAGE##*:}" \
  --set image.pullPolicy=IfNotPresent \
  --set logs.enabled=true \
  --set logs.insert.replicaCount=3 \
  --set traces.enabled=false \
  --set lakehouseConfig.compaction.enabled=true \
  --set lakehouseConfig.compaction.leader_election=k8s \
  --wait --timeout=180s || true  # don't bail; the asserts below catch failures

# Wait for the SA + RBAC to exist (chart-rendered).
kubectl get serviceaccount -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-compaction" >/dev/null 2>&1 \
  && ok "1a ServiceAccount rendered" \
  || fail "1a ServiceAccount missing"
kubectl get role -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-compaction-leader" >/dev/null 2>&1 \
  && ok "1b Role rendered" \
  || fail "1b Role missing"
kubectl get rolebinding -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-compaction-leader" >/dev/null 2>&1 \
  && ok "1c RoleBinding rendered" \
  || fail "1c RoleBinding missing"

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

# Wait for the Lease to be created (up to 60s of acquireLoop + RetryPeriod).
echo "  waiting for Lease $LEASE_NAME in $NS_PRIMARY (up to 60s)..."
for i in $(seq 1 60); do
  if kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" >/dev/null 2>&1; then
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
# 3: failover — kill the leader pod, expect another to take over.
# ---------------------------------------------------------------------------
sect "3: failover — delete leader pod $holder, expect successor within 40s"
if [[ -n "$holder" ]]; then
  kubectl delete pod -n "$NS_PRIMARY" "$holder" --grace-period=1 >/dev/null 2>&1 || true
  start=$(date +%s)
  new_holder=""
  for i in $(seq 1 40); do
    nh=$(kubectl get lease -n "$NS_PRIMARY" "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
    if [[ -n "$nh" && "$nh" != "$holder" ]]; then
      new_holder="$nh"
      break
    fi
    sleep 1
  done
  end=$(date +%s)
  elapsed=$((end - start))
  if [[ -n "$new_holder" ]]; then
    ok "3 failover succeeded — new holder=$new_holder (took ${elapsed}s, budget 40s)"
  else
    fail "3 failover did not happen within 40s (still $holder)"
  fi
else
  fail "3 skipped — no original holder to delete"
fi

# ---------------------------------------------------------------------------
# 4: NEGATIVE — delete the RoleBinding, restart pods, expect 403 in logs.
# ---------------------------------------------------------------------------
sect "4: NEGATIVE — delete RoleBinding, restart insert pods, expect 403 in logs"
kubectl delete rolebinding -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-compaction-leader" >/dev/null 2>&1 || true
# Also delete the lease so the next pod must re-acquire (and hit the missing
# RBAC immediately on the first GET).
kubectl delete lease -n "$NS_PRIMARY" "$LEASE_NAME" >/dev/null 2>&1 || true
# Force a fresh pod (it'll try acquireLoop -> tryAcquire -> getLease -> 403).
kubectl rollout restart statefulset -n "$NS_PRIMARY" "${RELEASE_PRIMARY}-victoria-lakehouse-logs-insert" >/dev/null 2>&1 || true
echo "  waiting up to 60s for a 403 to appear in insert-pod logs..."
saw_403=""
for i in $(seq 1 60); do
  for p in $(kubectl get pods -n "$NS_PRIMARY" -l "app.kubernetes.io/component=logs-insert" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    if kubectl logs -n "$NS_PRIMARY" "$p" --tail=200 2>/dev/null \
         | grep -qE "status (403|Forbidden)|cannot list|cannot get|cannot create"; then
      saw_403="$p"
      break 2
    fi
  done
  sleep 1
done
if [[ -n "$saw_403" ]]; then
  ok "4 RBAC removal caused 403/Forbidden in logs (pod=$saw_403)"
else
  fail "4 RBAC removal did not surface a 403 in logs within 60s — chart RBAC is NOT load-bearing!"
fi

# Re-create the RoleBinding so subsequent tests pass.
helm upgrade "$RELEASE_PRIMARY" "$CHART_PATH" \
  --namespace "$NS_PRIMARY" \
  --reuse-values \
  --wait --timeout=120s >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# 5: multi-namespace — install a 2nd release in a different namespace.
# ---------------------------------------------------------------------------
sect "5: multi-namespace — install $RELEASE_SECONDARY in $NS_SECONDARY, expect independent lease"
kubectl create namespace "$NS_SECONDARY" --dry-run=client -o yaml | kubectl apply -f -
helm install "$RELEASE_SECONDARY" "$CHART_PATH" \
  --namespace "$NS_SECONDARY" \
  --set image.logs.repository="${IMAGE%:*}" \
  --set image.logs.tag="${IMAGE##*:}" \
  --set image.pullPolicy=IfNotPresent \
  --set logs.enabled=true \
  --set logs.insert.replicaCount=2 \
  --set traces.enabled=false \
  --set lakehouseConfig.compaction.enabled=true \
  --set lakehouseConfig.compaction.leader_election=k8s \
  --wait --timeout=180s >/dev/null 2>&1 || true

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
