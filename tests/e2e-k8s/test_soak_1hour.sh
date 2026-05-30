#!/usr/bin/env bash
#
# tests/e2e-k8s/test_soak_1hour.sh
#
# PR #98 Tier 2 Item 13 — real-cluster soak with chaos.
#
# Opt-in via SOAK=1 env var. Default is "skip" so this never burns CI
# wall time unless explicitly enabled.
#
# Scenario:
#   1. Install chart with 3 replicas.
#   2. Wait for leader.
#   3. For 1 hour: every 30 s, delete a random insert pod (`kubectl delete
#      pod <random> --grace-period=1`). Tracks {timestamp, holderIdentity,
#      renewTime} every 5 s in a sample log.
#   4. At end, analyse sample log: assert no leaderless window > 30 s.
#
# Justification: this is the only way to exercise the
#   chaos × time × multiple-pod-restarts × CAS-fairness
# matrix that unit tests can't reach hermetically. We catch real-world
# regressions (e.g., a renew deadlock that only fires under sustained
# restart churn).
#
# Run with:
#   SOAK=1 bash tests/e2e-k8s/test_soak_1hour.sh
#
# Exit codes:
#   0 — soak passed (no leaderless gap > 30 s)
#   1 — gap exceeded budget
#   2 — toolchain missing
#  77 — soak skipped (SOAK != 1) — POSIX skip exit code
set -uo pipefail

if [[ "${SOAK:-}" != "1" ]]; then
  echo "SKIP: soak test is opt-in. Set SOAK=1 to run."
  exit 77
fi

CLUSTER_NAME="${CLUSTER_NAME:-lh-test}"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
KIND_CONFIG="$REPO_ROOT/tests/e2e-k8s/kind-config.yaml"
CHART_PATH="$REPO_ROOT/charts/victoria-lakehouse"
IMAGE="${IMAGE:-victoria-lakehouse-lakehouse-logs:latest}"
NS="${NS:-lh-soak}"
RELEASE="${RELEASE:-lh-soak}"
LEASE_NAME="${LEASE_NAME:-lakehouse-compaction-logs}"
SOAK_SECONDS="${SOAK_SECONDS:-3600}"        # 1 hour default
CHAOS_INTERVAL="${CHAOS_INTERVAL:-30}"      # kill a pod every 30 s
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-5}"     # sample lease every 5 s
MAX_GAP_SECONDS="${MAX_GAP_SECONDS:-30}"

SAMPLE_LOG="/tmp/lh-soak-leader-samples.log"
CHAOS_LOG="/tmp/lh-soak-chaos.log"

cleanup() {
  pkill -P $$ 2>/dev/null || true
  echo "Sample log left at $SAMPLE_LOG"
  echo "Chaos log left at $CHAOS_LOG"
  kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for t in kind kubectl helm docker python3 shuf; do
  command -v "$t" >/dev/null 2>&1 || { echo "FAIL: missing tool: $t"; exit 2; }
done

echo "=== creating kind cluster"
kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG" --wait 60s
kubectl wait --for=condition=Ready node --all --timeout=90s

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  ( cd "$REPO_ROOT" && docker build -f Dockerfile.logs -t "$IMAGE" . )
fi
kind load docker-image --name "$CLUSTER_NAME" "$IMAGE"

echo "=== helm install (3 replicas)"
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

echo "=== waiting up to 240s for lease..."
for i in $(seq 1 240); do
  if kubectl get lease -n "$NS" "$LEASE_NAME" >/dev/null 2>&1; then break; fi
  sleep 1
done
holder=$(kubectl get lease -n "$NS" "$LEASE_NAME" -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
[[ -z "$holder" ]] && { echo "FAIL: no initial leader"; exit 1; }

echo "=== initial leader: $holder"
echo "=== starting soak: ${SOAK_SECONDS}s, chaos every ${CHAOS_INTERVAL}s"

: > "$SAMPLE_LOG"
: > "$CHAOS_LOG"

# Sampler
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

# Chaos
(
  while true; do
    pods=($(kubectl get pods -n "$NS" -l "app.kubernetes.io/component=logs-insert" \
              -o jsonpath='{.items[*].metadata.name}' 2>/dev/null))
    if [[ ${#pods[@]} -gt 0 ]]; then
      target=$(printf '%s\n' "${pods[@]}" | shuf | head -1)
      ts=$(date +%s)
      echo "$ts|kill|$target" >> "$CHAOS_LOG"
      kubectl delete pod -n "$NS" "$target" --grace-period=1 >/dev/null 2>&1 || true
    fi
    sleep "$CHAOS_INTERVAL"
  done
) &
CHAOS_PID=$!
disown

# Wait for soak window.
sleep "$SOAK_SECONDS"

kill $SAMPLER_PID $CHAOS_PID 2>/dev/null || true
wait $SAMPLER_PID $CHAOS_PID 2>/dev/null || true

echo "=== soak complete; analysing $(wc -l < "$SAMPLE_LOG") samples"

python3 - <<EOF
import sys
from datetime import datetime, timezone

samples = []
with open("$SAMPLE_LOG") as f:
    for line in f:
        parts = line.strip().split("|")
        if len(parts) != 3:
            continue
        try:
            ts = int(parts[0])
        except ValueError:
            continue
        samples.append((ts, parts[1], parts[2]))

if len(samples) < 100:
    print("FAIL: too few samples ({})".format(len(samples)), file=sys.stderr)
    sys.exit(1)

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
if current_run_start is not None:
    leaderless_runs.append((current_run_start, samples[-1][0]))

worst = max([e - s for s, e in leaderless_runs], default=0)
print(f"")
print(f"  leaderless windows: {len(leaderless_runs)}")
for s, e in leaderless_runs[:10]:
    print(f"    start={s} end={e} duration={e-s}s")
if len(leaderless_runs) > 10:
    print(f"    ... and {len(leaderless_runs)-10} more")
print(f"")
print(f"  worst gap: {worst}s; budget: $MAX_GAP_SECONDS s")
if worst > $MAX_GAP_SECONDS:
    print("FAIL: soak detected leaderless gap exceeding budget", file=sys.stderr)
    sys.exit(1)
print("OK: soak passed; leadership maintained within bounds")
EOF

if [[ "$?" -eq 0 ]]; then
  echo "SOAK PASSED"
  exit 0
else
  echo "SOAK FAILED"
  exit 1
fi
