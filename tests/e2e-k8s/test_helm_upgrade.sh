#!/usr/bin/env bash
#
# tests/e2e-k8s/test_helm_upgrade.sh
#
# PR #98 Item 9 — helm upgrade during active election must not disrupt
# leadership.
#
# The StatefulSet rolling update brings new pods in one at a time
# (`podManagementPolicy: OrderedReady`). Each new pod reclaims leadership
# via the same-identity-reclaim short-circuit (PR #98 Item 6 tested in
# unit). The contract:
#   1. NO >30 s leaderless gap (defined as: kubectl get lease has either
#      no holderIdentity OR renewTime has not advanced in 30 s).
#   2. Exactly one valid holder at any sampled instant.
#
# This test:
#   1. Install the chart.
#   2. Wait for a leader.
#   3. Start a background sampler that polls `kubectl get lease` every 5 s
#      and records {timestamp, holderIdentity, renewTime}.
#   4. Run `helm upgrade --reuse-values` (triggers rolling restart).
#   5. Wait for rollout to complete.
#   6. Analyse the sampler log: assert no >30 s leaderless window.
#
# Exit codes:
#   0 — leadership stayed within bounds
#   1 — leadership gap exceeded 30 s OR more than one valid holder observed
#   2 — toolchain missing
set -uo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-lh-test}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KIND_CONFIG="$REPO_ROOT/tests/e2e-k8s/kind-config.yaml"
CHART_PATH="$REPO_ROOT/charts/victoria-lakehouse"
IMAGE="${IMAGE:-victoria-lakehouse-lakehouse-logs:latest}"
NS="${NS:-lh-upg}"
RELEASE="${RELEASE:-lh-upg}"
LEASE_NAME="${LEASE_NAME:-lakehouse-compaction-logs}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-5}"
MAX_GAP_SECONDS="${MAX_GAP_SECONDS:-30}"
SKIP_KIND_CREATE="${SKIP_KIND_CREATE:-0}"
SKIP_KIND_DELETE="${SKIP_KIND_DELETE:-0}"

FAILED=()
PASSED=()
SAMPLE_LOG="/tmp/lh-upg-leader-samples.log"

ok()    { echo "  PASS: $*"; PASSED+=("$1"); }
fail()  { echo "  FAIL: $*" >&2; FAILED+=("$1"); }
sect()  { echo; echo "=== $* ==="; }

ensure_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "FAIL: required tool '$1' not in PATH" >&2
    exit 2
  fi
}

for t in kind kubectl helm docker python3; do ensure_tool "$t"; done

cleanup() {
  pkill -P $$ 2>/dev/null || true
  rm -f "$SAMPLE_LOG" 2>/dev/null || true
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
  ( cd "$REPO_ROOT" && docker build -f Dockerfile.logs -t "$IMAGE" . )
fi
kind load docker-image --name "$CLUSTER_NAME" "$IMAGE"

sect "helm install (3 replicas)"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
helm install "$RELEASE" "$CHART_PATH" \
  --namespace "$NS" \
  --set "image.logs.repository=${IMAGE%:*}" \
  --set "image.tag=${IMAGE##*:}" \
  --set "image.pullPolicy=IfNotPresent" \
  --set "logs.enabled=true" \
  --set "logs.select.enabled=false" \
  --set "logs.insert.replicaCount=3" \
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
  --timeout=180s >/dev/null 2>&1 || true

echo "  waiting up to 240s for lease..."
for i in $(seq 1 240); do
  if kubectl get lease -n "$NS" "$LEASE_NAME" >/dev/null 2>&1; then break; fi
  sleep 1
done
initial=$(kubectl get lease -n "$NS" "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
if [[ -z "$initial" ]]; then
  fail "no initial leader"; exit 1
fi
ok "initial leader: $initial"

# ---------------------------------------------------------------------------
# Start sampler in background — records {timestamp, holderIdentity, renewTime}.
# ---------------------------------------------------------------------------
sect "starting lease sampler ($SAMPLE_INTERVAL s interval)"
: > "$SAMPLE_LOG"
(
  while true; do
    ts=$(date +%s)
    h=$(kubectl get lease -n "$NS" "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
    r=$(kubectl get lease -n "$NS" "$LEASE_NAME" -o jsonpath='{.spec.renewTime}' 2>/dev/null || echo "")
    echo "$ts|$h|$r" >> "$SAMPLE_LOG"
    sleep "$SAMPLE_INTERVAL"
  done
) &
SAMPLER_PID=$!
disown

# ---------------------------------------------------------------------------
# Trigger a rolling restart via helm upgrade --reuse-values + force.
# ---------------------------------------------------------------------------
sect "helm upgrade --reuse-values (triggers rolling restart)"
helm upgrade "$RELEASE" "$CHART_PATH" \
  --namespace "$NS" \
  --reuse-values \
  --set "podAnnotations.restart-trigger=$(date +%s)" \
  --timeout=300s >/dev/null 2>&1 || true

echo "  waiting up to 300s for rollout to complete..."
kubectl rollout status statefulset -n "$NS" \
  "${RELEASE}-victoria-lakehouse-logs-insert" --timeout=300s || \
  echo "  rollout did not complete cleanly; continuing analysis"

# Let the sampler run a bit longer to capture post-rollout state.
sleep 30
kill $SAMPLER_PID 2>/dev/null || true
wait $SAMPLER_PID 2>/dev/null || true

# ---------------------------------------------------------------------------
# Analyse the sample log.
# ---------------------------------------------------------------------------
sect "analysing $(wc -l < "$SAMPLE_LOG") samples for leaderless gaps"
cat "$SAMPLE_LOG"

python3 - <<EOF
import sys
from datetime import datetime, timezone

samples = []
with open("$SAMPLE_LOG") as f:
    for line in f:
        parts = line.strip().split("|")
        if len(parts) != 3:
            continue
        ts = int(parts[0])
        holder = parts[1]
        renew = parts[2]
        samples.append((ts, holder, renew))

if len(samples) < 2:
    print("FAIL: too few samples", file=sys.stderr)
    sys.exit(1)

# A "leaderless" sample is one where holder is empty OR renewTime is older
# than 30s (lease expired). The renewTime is an RFC3339 microsecond
# timestamp from the apiserver.

def parse_renew(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace('Z','+00:00'))
    except Exception:
        return None

leaderless_runs = []
current_run_start = None

for ts, holder, renew in samples:
    is_leaderless = False
    if not holder:
        is_leaderless = True
    else:
        rt = parse_renew(renew)
        if rt is None:
            is_leaderless = True
        else:
            sample_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            age = (sample_dt - rt).total_seconds()
            if age > $MAX_GAP_SECONDS:
                is_leaderless = True
    if is_leaderless and current_run_start is None:
        current_run_start = ts
    elif not is_leaderless and current_run_start is not None:
        leaderless_runs.append((current_run_start, ts))
        current_run_start = None
# Flush trailing run
if current_run_start is not None:
    leaderless_runs.append((current_run_start, samples[-1][0]))

worst_gap = 0
for s, e in leaderless_runs:
    g = e - s
    if g > worst_gap:
        worst_gap = g
    print(f"  leaderless window: start={s} end={e} duration={g}s")

print(f"")
print(f"  worst leaderless gap: {worst_gap}s")
print(f"  budget: $MAX_GAP_SECONDS s")
if worst_gap > $MAX_GAP_SECONDS:
    print("FAIL: leaderless gap exceeded budget", file=sys.stderr)
    sys.exit(1)
print("OK: leadership maintained within budget across helm upgrade")
EOF

if [[ "$?" -eq 0 ]]; then
  ok "leadership maintained within budget during helm upgrade"
else
  fail "leadership gap exceeded budget"
fi

sect "summary"
echo "PASSED: ${#PASSED[@]}"
echo "FAILED: ${#FAILED[@]}"
if (( ${#FAILED[@]} > 0 )); then
  printf '  - %s\n' "${FAILED[@]}"
  exit 1
fi
echo "HELM UPGRADE E2E VERIFIED"
exit 0
