#!/usr/bin/env bash
set -euo pipefail

# Cache sizing test: monitors memory, disk, and cache metrics during sustained load.
# Reports RSS, L1/L2 hit ratios, footer cache stats, S3 requests, eviction rates.
#
# Usage:
#   ./scripts/cache-sizing-test.sh [options]
#
# Options:
#   --target URL      Lakehouse endpoint (default: http://localhost:29428)
#   --vm URL          VictoriaMetrics endpoint for querying (default: http://localhost:28428)
#   --duration MINS   Monitoring duration in minutes (default: 10)
#   --query-load      Run query load during monitoring (default: false)
#   --output FILE     Output JSON file (default: results/cache-sizing-YYYYMMDD.json)

TARGET="http://localhost:29428"
VM_URL="http://localhost:28428"
DURATION=10
QUERY_LOAD=false
OUTPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)     TARGET="$2"; shift 2 ;;
    --vm)         VM_URL="$2"; shift 2 ;;
    --duration)   DURATION="$2"; shift 2 ;;
    --query-load) QUERY_LOAD=true; shift ;;
    --output)     OUTPUT="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ -z "$OUTPUT" ]]; then
  mkdir -p results
  OUTPUT="results/cache-sizing-$(date +%Y%m%d-%H%M%S).json"
fi

echo "=== Cache Sizing Test ==="
echo "Target: $TARGET"
echo "Duration: ${DURATION}m"
echo "Query load: $QUERY_LOAD"
echo ""

get_metric() {
  local metric="$1"
  curl -s "${TARGET}/metrics" 2>/dev/null | grep "^${metric}" | head -1 | awk '{print $2}' || echo "0"
}

get_metric_sum() {
  local metric="$1"
  curl -s "${TARGET}/metrics" 2>/dev/null | grep "^${metric}" | awk '{s+=$2} END {print s+0}' || echo "0"
}

get_container_rss() {
  # Try docker stats for the lakehouse container
  local rss
  rss=$(docker stats --no-stream --format "{{.MemUsage}}" lakehouse-logs 2>/dev/null | head -1 | awk -F/ '{print $1}' | sed 's/[[:space:]]//g' || echo "unknown")
  echo "$rss"
}

query_latency_ms() {
  local query="$1"
  local start end
  start=$(date +%s%N)
  curl -s -o /dev/null --max-time 30 \
    "${TARGET}/select/logsql/query" \
    --data-urlencode "query=${query}" \
    --data-urlencode "limit=100" 2>/dev/null || true
  end=$(date +%s%N)
  echo $(( (end - start) / 1000000 ))
}

# Background query load if requested
LOAD_PID=""
if [[ "$QUERY_LOAD" == "true" ]]; then
  echo "Starting background query load..."
  (
    QUERIES=(
      "_time:1h"
      'service.name:="api-gateway"'
      'level:="error"'
      "timeout"
      'service.name:="api-gateway" AND level:="error"'
      '_time:6h AND level:="warn"'
      'namespace:="production"'
      "connection refused"
    )
    while true; do
      for q in "${QUERIES[@]}"; do
        curl -s -o /dev/null --max-time 15 \
          "${TARGET}/select/logsql/query" \
          --data-urlencode "query=${q}" \
          --data-urlencode "limit=100" 2>/dev/null || true
        sleep 0.5
      done
    done
  ) &
  LOAD_PID=$!
  trap 'kill $LOAD_PID 2>/dev/null || true' EXIT
fi

# Collect initial metrics
echo "--- Collecting baseline ---"
INITIAL_L1_HITS=$(get_metric "lakehouse_cache_l1_hits_total")
INITIAL_L1_MISSES=$(get_metric "lakehouse_cache_l1_misses_total")
INITIAL_L2_HITS=$(get_metric "lakehouse_cache_l2_hits_total")
INITIAL_L2_MISSES=$(get_metric "lakehouse_cache_l2_misses_total")
INITIAL_FOOTER_HITS=$(get_metric "lakehouse_footer_cache_hits_total")
INITIAL_FOOTER_EVICTIONS=$(get_metric "lakehouse_footer_cache_evictions_total")
INITIAL_S3_GETS=$(get_metric_sum "lakehouse_s3_requests_total")

echo ""
echo "--- Monitoring (${DURATION}m) ---"
echo "time,rss,l1_hits,l1_misses,l1_ratio,l2_hits,l2_misses,l2_ratio,footer_hits,footer_evictions,s3_gets,query_ms"

SAMPLES=()
END_TIME=$(( $(date +%s) + DURATION * 60 ))
INTERVAL=10

while [[ $(date +%s) -lt $END_TIME ]]; do
  RSS=$(get_container_rss)

  L1_HITS=$(get_metric "lakehouse_cache_l1_hits_total")
  L1_MISSES=$(get_metric "lakehouse_cache_l1_misses_total")
  L2_HITS=$(get_metric "lakehouse_cache_l2_hits_total")
  L2_MISSES=$(get_metric "lakehouse_cache_l2_misses_total")
  FOOTER_HITS=$(get_metric "lakehouse_footer_cache_hits_total")
  FOOTER_EVICTIONS=$(get_metric "lakehouse_footer_cache_evictions_total")
  S3_GETS=$(get_metric_sum "lakehouse_s3_requests_total")

  # Delta calculations
  D_L1_HITS=$(echo "$L1_HITS - $INITIAL_L1_HITS" | bc)
  D_L1_MISSES=$(echo "$L1_MISSES - $INITIAL_L1_MISSES" | bc)
  D_L2_HITS=$(echo "$L2_HITS - $INITIAL_L2_HITS" | bc)
  D_L2_MISSES=$(echo "$L2_MISSES - $INITIAL_L2_MISSES" | bc)

  if [[ $(echo "$D_L1_HITS + $D_L1_MISSES" | bc) -gt 0 ]]; then
    L1_RATIO=$(echo "scale=1; $D_L1_HITS * 100 / ($D_L1_HITS + $D_L1_MISSES)" | bc)
  else
    L1_RATIO="0"
  fi

  if [[ $(echo "$D_L2_HITS + $D_L2_MISSES" | bc) -gt 0 ]]; then
    L2_RATIO=$(echo "scale=1; $D_L2_HITS * 100 / ($D_L2_HITS + $D_L2_MISSES)" | bc)
  else
    L2_RATIO="0"
  fi

  # Spot query latency
  LATENCY=$(query_latency_ms "_time:1h")

  TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  ELAPSED=$(( END_TIME - $(date +%s) ))
  echo "${TIMESTAMP},${RSS},${D_L1_HITS},${D_L1_MISSES},${L1_RATIO}%,${D_L2_HITS},${D_L2_MISSES},${L2_RATIO}%,${FOOTER_HITS},${FOOTER_EVICTIONS},${S3_GETS},${LATENCY}ms"

  SAMPLES+=("{\"timestamp\":\"${TIMESTAMP}\",\"rss\":\"${RSS}\",\"l1_hits\":${D_L1_HITS},\"l1_misses\":${D_L1_MISSES},\"l1_ratio\":${L1_RATIO},\"l2_hits\":${D_L2_HITS},\"l2_misses\":${D_L2_MISSES},\"l2_ratio\":${L2_RATIO},\"footer_hits\":${FOOTER_HITS},\"footer_evictions\":${FOOTER_EVICTIONS},\"s3_gets\":${S3_GETS},\"query_ms\":${LATENCY}}")

  sleep $INTERVAL
done

# Stop background load
if [[ -n "$LOAD_PID" ]]; then
  kill "$LOAD_PID" 2>/dev/null || true
  wait "$LOAD_PID" 2>/dev/null || true
fi

# Final metrics
echo ""
echo "--- Final State ---"

FINAL_L1_HITS=$(get_metric "lakehouse_cache_l1_hits_total")
FINAL_L1_MISSES=$(get_metric "lakehouse_cache_l1_misses_total")
FINAL_L2_HITS=$(get_metric "lakehouse_cache_l2_hits_total")
FINAL_L2_MISSES=$(get_metric "lakehouse_cache_l2_misses_total")
FINAL_FOOTER_HITS=$(get_metric "lakehouse_footer_cache_hits_total")
FINAL_FOOTER_EVICTIONS=$(get_metric "lakehouse_footer_cache_evictions_total")
FINAL_S3_GETS=$(get_metric_sum "lakehouse_s3_requests_total")
FINAL_RSS=$(get_container_rss)

TOTAL_L1_HITS=$(echo "$FINAL_L1_HITS - $INITIAL_L1_HITS" | bc)
TOTAL_L1_MISSES=$(echo "$FINAL_L1_MISSES - $INITIAL_L1_MISSES" | bc)
TOTAL_L2_HITS=$(echo "$FINAL_L2_HITS - $INITIAL_L2_HITS" | bc)
TOTAL_L2_MISSES=$(echo "$FINAL_L2_MISSES - $INITIAL_L2_MISSES" | bc)
TOTAL_S3=$(echo "$FINAL_S3_GETS - $INITIAL_S3_GETS" | bc)
TOTAL_FOOTER_EVICTIONS=$(echo "$FINAL_FOOTER_EVICTIONS - $INITIAL_FOOTER_EVICTIONS" | bc)

if [[ $(echo "$TOTAL_L1_HITS + $TOTAL_L1_MISSES" | bc) -gt 0 ]]; then
  FINAL_L1_RATIO=$(echo "scale=1; $TOTAL_L1_HITS * 100 / ($TOTAL_L1_HITS + $TOTAL_L1_MISSES)" | bc)
else
  FINAL_L1_RATIO="0"
fi

if [[ $(echo "$TOTAL_L2_HITS + $TOTAL_L2_MISSES" | bc) -gt 0 ]]; then
  FINAL_L2_RATIO=$(echo "scale=1; $TOTAL_L2_HITS * 100 / ($TOTAL_L2_HITS + $TOTAL_L2_MISSES)" | bc)
else
  FINAL_L2_RATIO="0"
fi

echo "RSS:              $FINAL_RSS"
echo "L1 hit ratio:     ${FINAL_L1_RATIO}% (${TOTAL_L1_HITS} hits / ${TOTAL_L1_MISSES} misses)"
echo "L2 hit ratio:     ${FINAL_L2_RATIO}% (${TOTAL_L2_HITS} hits / ${TOTAL_L2_MISSES} misses)"
echo "Footer hits:      $FINAL_FOOTER_HITS"
echo "Footer evictions: $TOTAL_FOOTER_EVICTIONS"
echo "S3 GETs:          $TOTAL_S3"

# Write JSON output
IFS=, SAMPLES_JSON="${SAMPLES[*]}"; unset IFS
cat > "$OUTPUT" <<ENDJSON
{
  "test": "cache-sizing",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "duration_minutes": $DURATION,
  "query_load": $QUERY_LOAD,
  "target": "$TARGET",
  "summary": {
    "rss": "$FINAL_RSS",
    "l1_hit_ratio": $FINAL_L1_RATIO,
    "l1_hits": $TOTAL_L1_HITS,
    "l1_misses": $TOTAL_L1_MISSES,
    "l2_hit_ratio": $FINAL_L2_RATIO,
    "l2_hits": $TOTAL_L2_HITS,
    "l2_misses": $TOTAL_L2_MISSES,
    "footer_cache_hits": $FINAL_FOOTER_HITS,
    "footer_cache_evictions": $TOTAL_FOOTER_EVICTIONS,
    "s3_requests": $TOTAL_S3
  },
  "samples": [${SAMPLES_JSON}]
}
ENDJSON

echo ""
echo "Results written to: $OUTPUT"
