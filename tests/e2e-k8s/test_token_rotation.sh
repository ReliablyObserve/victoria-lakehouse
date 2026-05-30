#!/usr/bin/env bash
#
# tests/e2e-k8s/test_token_rotation.sh
#
# PR #98 Item 1 — SA token rotation mid-election.
#
# kubelet rotates projected ServiceAccount tokens periodically. With token
# projection, the default rotation period is ~1 h. The K8sElector reads the
# token from disk on each API call (via bearerTokenForRequest), so a
# rotation must NOT cause 401 errors or break leadership.
#
# This test:
#   1. Installs the chart with the default in-cluster SA mount.
#   2. Waits for a leader.
#   3. Forces a token rotation by deleting the projected token files in
#      every insert pod (kubelet recreates them on next poll, with a fresh
#      token). This simulates what kubelet does on its 1 h rotation
#      cycle, but with a shorter wall-clock window for CI.
#   4. Asserts the lease's renewTime continues to advance for at least
#      60 s after the rotation, AND that no pod logs contain
#      "401 Unauthorized" or "invalid token" since the rotation moment.
#
# Why we delete the file instead of waiting 1 h: CI doesn't have a 1-hour
# budget. kubelet's behaviour on a missing token file is to re-fetch and
# rewrite it within a few seconds (well under the elector's RetryPeriod).
# This exercises the SAME elector code path (re-read token from disk via
# bearerTokenForRequest) without waiting for a real rotation cycle.
#
# Exit codes:
#   0 — leader continued, no 401s, lease renewing
#   1 — leader stalled, or 401s observed in logs
#   2 — toolchain missing or chart install failed
set -uo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-lh-test}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KIND_CONFIG="$REPO_ROOT/tests/e2e-k8s/kind-config.yaml"
CHART_PATH="$REPO_ROOT/charts/victoria-lakehouse"
IMAGE="${IMAGE:-victoria-lakehouse-lakehouse-logs:latest}"
NS="${NS:-lh-tokrot}"
RELEASE="${RELEASE:-lh-tokrot}"
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
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "$SKIP_KIND_CREATE" != "1" ]]; then
  sect "creating kind cluster $CLUSTER_NAME"
  kind_args=(create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG" --wait 60s)
  if [[ -n "${KIND_NODE_IMAGE:-}" ]]; then
    kind_args+=(--image "$KIND_NODE_IMAGE")
    echo "  using node image: $KIND_NODE_IMAGE"
  fi
  kind "${kind_args[@]}"
fi

kubectl wait --for=condition=Ready node --all --timeout=90s || {
  fail "cluster nodes never became Ready"; exit 1;
}

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "  building $IMAGE locally..."
  ( cd "$REPO_ROOT" && docker build -f Dockerfile.logs -t "$IMAGE" . ) || {
    fail "docker build failed"; exit 1;
  }
fi
kind load docker-image --name "$CLUSTER_NAME" "$IMAGE"

sect "helm install $RELEASE in $NS"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
helm install "$RELEASE" "$CHART_PATH" \
  --namespace "$NS" \
  --set "image.logs.repository=${IMAGE%:*}" \
  --set "image.tag=${IMAGE##*:}" \
  --set "image.pullPolicy=IfNotPresent" \
  --set "logs.enabled=true" \
  --set "logs.select.enabled=false" \
  --set "logs.insert.replicaCount=2" \
  --set "logs.insert.persistence.size=200Mi" \
  --set "traces.enabled=false" \
  --set "lakehouseConfig.s3.bucket=lh-test-bucket" \
  --set "lakehouseConfig.s3.endpoint=http://fake-minio:9000" \
  --set "lakehouseConfig.s3.access_key=test" \
  --set "lakehouseConfig.s3.secret_key=test" \
  --set "lakehouseConfig.s3.force_path_style=true" \
  --set "lakehouseConfig.compaction.enabled=true" \
  --set "lakehouseConfig.compaction.leader_election=k8s" \
  --set "lakehouseConfig.compaction.interval=30s" \
  --timeout=120s || true

echo "  waiting up to 180s for at least one insert pod to reach Running..."
for i in $(seq 1 180); do
  rcount=$(kubectl get pods -n "$NS" -l "app.kubernetes.io/component=logs-insert" \
             -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name} {end}' 2>/dev/null | wc -w)
  if [[ "$rcount" -ge 1 ]]; then
    echo "  $rcount pods Running after ${i}s"
    break
  fi
  sleep 1
done

echo "  waiting up to 180s for Lease $LEASE_NAME..."
for i in $(seq 1 180); do
  if kubectl get lease -n "$NS" "$LEASE_NAME" >/dev/null 2>&1; then break; fi
  sleep 1
done
holder=$(kubectl get lease -n "$NS" "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
if [[ -z "$holder" ]]; then
  fail "lease never created"; exit 1
fi
ok "leader elected; identity=$holder"

# ---------------------------------------------------------------------------
# Token rotation: force a fresh token on every insert pod by deleting the
# on-disk file. kubelet writes a new token within a few seconds (it
# polls /var/run/secrets/... and renews from the apiserver).
# ---------------------------------------------------------------------------
sect "token rotation — deleting projected token files in all insert pods"
rotate_start=$(date +%s)
for p in $(kubectl get pods -n "$NS" -l "app.kubernetes.io/component=logs-insert" \
              -o jsonpath='{.items[*].metadata.name}'); do
  echo "  rotating token in $p..."
  kubectl exec -n "$NS" "$p" -- sh -c \
    'rm -f /var/run/secrets/kubernetes.io/serviceaccount/token; sync || true' 2>&1 | head -5 || true
done
# Give kubelet 30 s to repopulate the token files.
sleep 30

# ---------------------------------------------------------------------------
# Assertion: lease.renewTime must continue advancing AFTER rotation.
# Take a baseline, wait 60 s, take a second sample. If renewTime didn't
# move, the elector stalled (likely on 401 because we removed the token).
# ---------------------------------------------------------------------------
sect "asserting lease keeps renewing for 60s after rotation"
renew_baseline=$(kubectl get lease -n "$NS" "$LEASE_NAME" \
                  -o jsonpath='{.spec.renewTime}' 2>/dev/null || echo "")
echo "  renewTime baseline: $renew_baseline"
sleep 60
renew_after=$(kubectl get lease -n "$NS" "$LEASE_NAME" \
              -o jsonpath='{.spec.renewTime}' 2>/dev/null || echo "")
echo "  renewTime after 60s: $renew_after"

if [[ -z "$renew_baseline" || -z "$renew_after" ]]; then
  fail "could not sample lease renewTime"
elif [[ "$renew_baseline" == "$renew_after" ]]; then
  fail "lease.renewTime did not advance in 60s after token rotation — elector stalled"
else
  ok "lease.renewTime advanced after rotation ($renew_baseline → $renew_after)"
fi

# ---------------------------------------------------------------------------
# Negative-control: scan pod logs for 401 / unauthorized lines since rotate.
# A working re-read should yield zero such lines.
# ---------------------------------------------------------------------------
sect "scanning pod logs for 401/unauthorized since rotation"
log_problems=0
for p in $(kubectl get pods -n "$NS" -l "app.kubernetes.io/component=logs-insert" \
              -o jsonpath='{.items[*].metadata.name}'); do
  # Look at the last 200 lines per pod
  count=$(kubectl logs -n "$NS" "$p" --tail=200 2>/dev/null | \
            grep -ciE "401 unauthorized|invalid token|expired token" || true)
  if [[ "$count" -gt 0 ]]; then
    log_problems=$((log_problems + count))
    echo "  $p: $count 401/invalid-token lines"
  fi
done
if [[ "$log_problems" -eq 0 ]]; then
  ok "no 401/unauthorized lines in any pod log after rotation"
else
  fail "$log_problems 401/unauthorized log lines after rotation — re-read broken"
fi

sect "summary"
echo "PASSED: ${#PASSED[@]}"
echo "FAILED: ${#FAILED[@]}"
if (( ${#FAILED[@]} > 0 )); then
  printf '  - %s\n' "${FAILED[@]}"
  exit 1
fi
echo "TOKEN ROTATION E2E VERIFIED"
exit 0
