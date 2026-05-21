#!/usr/bin/env bash
set -euo pipefail

# Fresh restore test: validates cold-start behavior of a Lakehouse instance.
# Starts a fresh instance with zero cache, monitors readiness, warmup progress,
# and query latency until steady state is reached.
#
# Usage:
#   ./scripts/fresh-restore-test.sh [options]
#
# Options:
#   --compose-file FILE   Docker compose file (default: deployment/docker/docker-compose-benchmark.yml)
#   --dataset-size SIZE   small|medium|large (default: medium)
#   --target URL          Lakehouse endpoint (default: http://localhost:29428)
#   --output FILE         Output JSON file (default: results/restore-YYYYMMDD.json)
#   --skip-ingest         Skip data generation, assume data exists in S3

COMPOSE_FILE="deployment/docker/docker-compose-benchmark.yml"
DATASET_SIZE="medium"
TARGET="http://localhost:39428"
OUTPUT=""
SKIP_INGEST=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compose-file) COMPOSE_FILE="$2"; shift 2 ;;
    --dataset-size) DATASET_SIZE="$2"; shift 2 ;;
    --target)       TARGET="$2"; shift 2 ;;
    --output)       OUTPUT="$2"; shift 2 ;;
    --skip-ingest)  SKIP_INGEST=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ -z "$OUTPUT" ]]; then
  mkdir -p results
  OUTPUT="results/restore-$(date +%Y%m%d-%H%M%S).json"
fi

case "$DATASET_SIZE" in
  small)
    LOGS=50000
    HOURS=24
    READY_SLA=10
    WARMUP_SLA=30
    STEADY_SLA=120
    EXPECTED_HIT_RATIO=90
    ;;
  medium)
    LOGS=500000
    HOURS=168
    READY_SLA=30
    WARMUP_SLA=120
    STEADY_SLA=300
    EXPECTED_HIT_RATIO=80
    ;;
  large)
    LOGS=2500000
    HOURS=720
    READY_SLA=60
    WARMUP_SLA=300
    STEADY_SLA=600
    EXPECTED_HIT_RATIO=70
    ;;
  *)
    echo "Invalid dataset-size: $DATASET_SIZE (must be small|medium|large)"
    exit 1
    ;;
esac

echo "=== Fresh Restore Test ==="
echo "Dataset: $DATASET_SIZE ($LOGS logs, ${HOURS}h back)"
echo "SLAs: ready<${READY_SLA}s, warmup<${WARMUP_SLA}s, steady<${STEADY_SLA}s"
echo ""

query_latency_ms() {
  local query="$1"
  local start end
  start=$(date +%s%N)
  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" \
    --max-time 30 \
    "${TARGET}/select/logsql/query" \
    --data-urlencode "query=${query}" \
    --data-urlencode "limit=100" 2>/dev/null || echo "000")
  end=$(date +%s%N)
  if [[ "$http_code" == "200" ]]; then
    echo $(( (end - start) / 1000000 ))
  else
    echo "-1"
  fi
}

get_metric() {
  local metric="$1"
  curl -s "${TARGET}/metrics" 2>/dev/null | grep "^${metric}" | head -1 | awk '{print $2}' || echo "0"
}

# Phase 1: Ensure data exists in S3
if [[ "$SKIP_INGEST" == "false" ]]; then
  echo "--- Phase 1: Ingesting seed data ---"
  docker compose -f "$COMPOSE_FILE" up -d minio minio-init lakehouse-logs
  echo "Waiting for lakehouse to be ready..."
  until curl -sf "${TARGET}/health" > /dev/null 2>&1; do sleep 1; done

  docker compose -f "$COMPOSE_FILE" run --rm datagen-seed \
    --logs="$LOGS" --hours-back="$HOURS" \
    --lh-logs-endpoint=http://lakehouse-logs:9428

  echo "Waiting for flush..."
  sleep 45
  echo "Data ingestion complete."
else
  echo "--- Phase 1: Skipping ingest (--skip-ingest) ---"
fi

# Phase 2: Stop Lakehouse, clear cache volume, restart
echo ""
echo "--- Phase 2: Cold restart ---"
docker compose -f "$COMPOSE_FILE" stop lakehouse-logs
docker compose -f "$COMPOSE_FILE" rm -f lakehouse-logs
docker volume rm victoria-lakehouse-benchmark_lakehouse-cache-logs 2>/dev/null || true

RESTART_START=$(date +%s)
docker compose -f "$COMPOSE_FILE" up -d lakehouse-logs

# Phase 3: Measure time to ready
echo "Waiting for health endpoint..."
READY_ELAPSED=0
until curl -sf "${TARGET}/health" > /dev/null 2>&1; do
  sleep 0.5
  READY_ELAPSED=$(( $(date +%s) - RESTART_START ))
  if [[ $READY_ELAPSED -gt $((READY_SLA * 3)) ]]; then
    echo "FAIL: Instance not ready after ${READY_ELAPSED}s (SLA: ${READY_SLA}s)"
    break
  fi
done
READY_ELAPSED=$(( $(date +%s) - RESTART_START ))
echo "Ready in ${READY_ELAPSED}s (SLA: ${READY_SLA}s)"

# Phase 4: Monitor warmup and query latency
echo ""
echo "--- Phase 3: Monitoring warmup ---"
echo "timestamp,elapsed_s,query_latency_ms,l2_hit_ratio,footer_cache_hits,warmup_done"

RESULTS=()
WARMUP_DONE_AT=""
STEADY_AT=""
MONITOR_START=$(date +%s)
MONITOR_LIMIT=$((STEADY_SLA * 2))

SAMPLE_QUERY='_time:1h'

while true; do
  ELAPSED=$(( $(date +%s) - RESTART_START ))

  LATENCY=$(query_latency_ms "$SAMPLE_QUERY")

  L2_HITS=$(get_metric "lakehouse_cache_l2_hits_total")
  L2_MISSES=$(get_metric "lakehouse_cache_l2_misses_total")
  FOOTER_HITS=$(get_metric "lakehouse_footer_cache_hits_total")

  if [[ $(echo "$L2_HITS + $L2_MISSES" | bc) -gt 0 ]]; then
    L2_RATIO=$(echo "scale=1; $L2_HITS * 100 / ($L2_HITS + $L2_MISSES)" | bc)
  else
    L2_RATIO="0"
  fi

  # Check if warmup is progressing (footer cache populating)
  WARMUP_STATUS="warming"
  if [[ -n "$WARMUP_DONE_AT" ]]; then
    WARMUP_STATUS="done"
  elif [[ "$LATENCY" -gt 0 && "$LATENCY" -lt 500 ]]; then
    WARMUP_DONE_AT="$ELAPSED"
    WARMUP_STATUS="done"
  fi

  if [[ -z "$STEADY_AT" && "$L2_RATIO" != "0" ]]; then
    RATIO_INT=${L2_RATIO%.*}
    if [[ "$RATIO_INT" -ge "$EXPECTED_HIT_RATIO" ]]; then
      STEADY_AT="$ELAPSED"
    fi
  fi

  TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "${TIMESTAMP},${ELAPSED},${LATENCY},${L2_RATIO}%,${FOOTER_HITS},${WARMUP_STATUS}"

  RESULTS+=("{\"elapsed\":${ELAPSED},\"latency_ms\":${LATENCY},\"l2_hit_ratio\":${L2_RATIO},\"footer_hits\":${FOOTER_HITS}}")

  MONITOR_ELAPSED=$(( $(date +%s) - MONITOR_START ))
  if [[ $MONITOR_ELAPSED -gt $MONITOR_LIMIT ]]; then
    echo "Monitor limit reached (${MONITOR_LIMIT}s)"
    break
  fi

  if [[ -n "$STEADY_AT" ]]; then
    echo "Steady state reached at ${STEADY_AT}s"
    break
  fi

  sleep 5
done

# Phase 5: Run a few representative queries at current state
echo ""
echo "--- Phase 4: Post-warmup query latencies ---"
declare -A QUERIES=(
  ["time_range"]="_time:1h"
  ["service_exact"]='service.name:="api-gateway"'
  ["level_filter"]='level:="error"'
  ["keyword"]="timeout"
  ["combined"]='service.name:="api-gateway" AND level:="error"'
)

QUERY_RESULTS=""
for name in "${!QUERIES[@]}"; do
  LATENCIES=()
  for i in $(seq 1 5); do
    L=$(query_latency_ms "${QUERIES[$name]}")
    LATENCIES+=("$L")
    sleep 0.5
  done

  IFS=$'\n' SORTED=($(sort -n <<<"${LATENCIES[*]}")); unset IFS
  P50=${SORTED[2]}
  P95=${SORTED[4]}

  echo "  ${name}: p50=${P50}ms p95=${P95}ms"
  QUERY_RESULTS="${QUERY_RESULTS}{\"name\":\"${name}\",\"p50\":${P50},\"p95\":${P95}},"
done
QUERY_RESULTS="[${QUERY_RESULTS%,}]"

# Phase 6: Assess results
echo ""
echo "=== Results ==="
READY_PASS=$([[ $READY_ELAPSED -le $READY_SLA ]] && echo "PASS" || echo "FAIL")
WARMUP_PASS=$([[ -n "$WARMUP_DONE_AT" && "$WARMUP_DONE_AT" -le "$WARMUP_SLA" ]] && echo "PASS" || echo "FAIL")
STEADY_PASS=$([[ -n "$STEADY_AT" && "$STEADY_AT" -le "$STEADY_SLA" ]] && echo "PASS" || echo "FAIL")

echo "Ready:        ${READY_ELAPSED}s (SLA: ${READY_SLA}s) — ${READY_PASS}"
echo "Warmup done:  ${WARMUP_DONE_AT:-never}s (SLA: ${WARMUP_SLA}s) — ${WARMUP_PASS}"
echo "Steady state: ${STEADY_AT:-never}s (SLA: ${STEADY_SLA}s) — ${STEADY_PASS}"

# Write JSON output
IFS=, SAMPLES_JSON="${RESULTS[*]}"; unset IFS
cat > "$OUTPUT" <<ENDJSON
{
  "test": "fresh-restore",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "dataset_size": "$DATASET_SIZE",
  "logs": $LOGS,
  "hours_back": $HOURS,
  "results": {
    "ready_seconds": $READY_ELAPSED,
    "ready_sla": $READY_SLA,
    "ready_pass": "$READY_PASS",
    "warmup_seconds": ${WARMUP_DONE_AT:-null},
    "warmup_sla": $WARMUP_SLA,
    "warmup_pass": "$WARMUP_PASS",
    "steady_state_seconds": ${STEADY_AT:-null},
    "steady_state_sla": $STEADY_SLA,
    "steady_state_pass": "$STEADY_PASS",
    "final_l2_hit_ratio": $L2_RATIO
  },
  "post_warmup_queries": $QUERY_RESULTS,
  "samples": [${SAMPLES_JSON}]
}
ENDJSON

echo ""
echo "Results written to: $OUTPUT"

OVERALL="PASS"
[[ "$READY_PASS" == "FAIL" || "$WARMUP_PASS" == "FAIL" || "$STEADY_PASS" == "FAIL" ]] && OVERALL="FAIL"
echo "Overall: $OVERALL"

[[ "$OVERALL" == "PASS" ]]
