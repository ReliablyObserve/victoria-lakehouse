#!/usr/bin/env bash
#
# tests/e2e-k8s/test_no_sa_token.sh
#
# PR #98 Item 4 — pod without ServiceAccount token must fail loudly.
#
# When the chart is deployed with `automountServiceAccountToken: false`
# (or a misconfigured projection), the elector must:
#   1. Detect the missing token at startup.
#   2. Log a canonical "service account token not found" error.
#   3. Increment the lakehouse_leader_election_startup_errors_total metric.
#   4. NOT silently disable election and pretend leadership is working.
#
# This test:
#   1. Installs the chart WITH the default SA mount → leader elected
#      (positive control).
#   2. Installs a second release with automountServiceAccountToken=false on
#      the insert SA → asserts the elector logs the canonical error and
#      no lease is ever created (negative control).
#
# Exit codes:
#   0 — both positive and negative controls match expectation
#   1 — one or both controls failed
#   2 — toolchain missing
set -uo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-lh-test}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KIND_CONFIG="$REPO_ROOT/tests/e2e-k8s/kind-config.yaml"
CHART_PATH="$REPO_ROOT/charts/victoria-lakehouse"
IMAGE="${IMAGE:-victoria-lakehouse-lakehouse-logs:latest}"
NS_GOOD="${NS_GOOD:-lh-sa-good}"
NS_BAD="${NS_BAD:-lh-sa-bad}"
REL_GOOD="${REL_GOOD:-lh-sa-good}"
REL_BAD="${REL_BAD:-lh-sa-bad}"
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

kubectl wait --for=condition=Ready node --all --timeout=90s

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  ( cd "$REPO_ROOT" && docker build -f Dockerfile.logs -t "$IMAGE" . ) || {
    fail "docker build failed"; exit 1;
  }
fi
kind load docker-image --name "$CLUSTER_NAME" "$IMAGE"

helm_args_common=(
  --set "image.logs.repository=${IMAGE%:*}"
  --set "image.tag=${IMAGE##*:}"
  --set "image.pullPolicy=IfNotPresent"
  --set "logs.enabled=true"
  --set "logs.select.enabled=false"
  --set "logs.insert.replicaCount=1"
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

# ---------------------------------------------------------------------------
# Positive control: default SA mount → leader must be elected.
# ---------------------------------------------------------------------------
sect "POSITIVE: install $REL_GOOD with default SA mount → expect leader"
kubectl create namespace "$NS_GOOD" --dry-run=client -o yaml | kubectl apply -f -
helm install "$REL_GOOD" "$CHART_PATH" \
  --namespace "$NS_GOOD" \
  "${helm_args_common[@]}" \
  --timeout=120s >/dev/null 2>&1 || true

echo "  waiting up to 180s for lease..."
for i in $(seq 1 180); do
  if kubectl get lease -n "$NS_GOOD" "$LEASE_NAME" >/dev/null 2>&1; then break; fi
  sleep 1
done
good_holder=$(kubectl get lease -n "$NS_GOOD" "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
if [[ -n "$good_holder" ]]; then
  ok "positive control: leader elected ($good_holder) with default SA mount"
else
  fail "positive control: no leader with default SA mount"
fi

# ---------------------------------------------------------------------------
# Negative control: automountServiceAccountToken: false on the SA.
# The chart doesn't expose this directly; the cleanest path is to patch the
# SA after helm install to set automountServiceAccountToken: false, then
# delete the existing pod so the kubelet recreates it WITHOUT the token mount.
# ---------------------------------------------------------------------------
sect "NEGATIVE: install $REL_BAD then patch SA to automountServiceAccountToken=false"
kubectl create namespace "$NS_BAD" --dry-run=client -o yaml | kubectl apply -f -
helm install "$REL_BAD" "$CHART_PATH" \
  --namespace "$NS_BAD" \
  "${helm_args_common[@]}" \
  --timeout=120s >/dev/null 2>&1 || true

# Wait for insert SA to exist, then patch it.
SA_NAME="${REL_BAD}-victoria-lakehouse-logs-insert"
for i in $(seq 1 60); do
  if kubectl get sa -n "$NS_BAD" "$SA_NAME" >/dev/null 2>&1; then break; fi
  sleep 1
done
kubectl patch sa -n "$NS_BAD" "$SA_NAME" \
  --type=merge -p='{"automountServiceAccountToken":false}' || {
    fail "patch SA failed"; exit 1;
  }
echo "  patched SA $SA_NAME with automountServiceAccountToken=false"

# Now delete the pod so kubelet recreates it WITHOUT the token mount.
# Find the StatefulSet's only pod (replicaCount=1 above).
bad_pod=$(kubectl get pods -n "$NS_BAD" -l "app.kubernetes.io/component=logs-insert" \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [[ -n "$bad_pod" ]]; then
  echo "  deleting pod $bad_pod to force recreation without token mount..."
  kubectl delete pod -n "$NS_BAD" "$bad_pod" --grace-period=1 >/dev/null 2>&1 || true
fi

# Wait for the StatefulSet to recreate the pod. The pod's spec now has
# automountServiceAccountToken=false, so the kubelet won't mount a token
# at /var/run/secrets/.../token. The elector's bootstrap pre-flight will
# detect this and log the canonical error.
echo "  waiting up to 120s for recreated pod to run + log the SA error..."
seen_canonical_error=""
for i in $(seq 1 120); do
  for p in $(kubectl get pods -n "$NS_BAD" -l "app.kubernetes.io/component=logs-insert" \
              -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    if kubectl logs -n "$NS_BAD" "$p" --tail=200 2>/dev/null | grep -q "service account token not found"; then
      seen_canonical_error="yes"
      echo "  canonical error seen in $p logs"
      break 2
    fi
  done
  sleep 1
done

if [[ -n "$seen_canonical_error" ]]; then
  ok "negative control: elector logged canonical 'service account token not found' error"
else
  fail "negative control: canonical error NEVER appeared in any pod's logs"
fi

# Also assert no lease was ever created in $NS_BAD.
sleep 5 # short grace for any late lease creation
if kubectl get lease -n "$NS_BAD" "$LEASE_NAME" >/dev/null 2>&1; then
  fail "negative control: lease was created despite missing SA token (elector silently disabled?)"
else
  ok "negative control: no lease created (elector correctly refused to start)"
fi

sect "summary"
echo "PASSED: ${#PASSED[@]}"
echo "FAILED: ${#FAILED[@]}"
if (( ${#FAILED[@]} > 0 )); then
  printf '  - %s\n' "${FAILED[@]}"
  exit 1
fi
echo "NO-SA-TOKEN E2E VERIFIED (positive + negative controls)"
exit 0
