#!/usr/bin/env bash
set -euo pipefail

# Comparative benchmark: runs identical query scenarios against Lakehouse, VictoriaLogs, and Loki.
# Measures p50/p95/p99 per system per scenario. Outputs JSON results.
#
# Usage:
#   ./scripts/comparative-benchmark.sh [options]
#
# Options:
#   --lh URL        Lakehouse endpoint (default: http://localhost:29428)
#   --vl URL        VictoriaLogs endpoint (default: http://localhost:29401)
#   --loki URL      Loki endpoint (default: http://localhost:23100)
#   --iterations N  Iterations per scenario (default: 10)
#   --warmup N      Warmup iterations (default: 3)
#   --output FILE   Output JSON file (default: results/comparative-YYYYMMDD.json)
#   --skip-ingest   Skip data generation (data already loaded)

LH_URL="http://localhost:39428"
VL_URL="http://localhost:39401"
LOKI_URL="http://localhost:33100"
ITERATIONS=10
WARMUP=3
OUTPUT=""
SKIP_INGEST=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lh)       LH_URL="$2"; shift 2 ;;
    --vl)       VL_URL="$2"; shift 2 ;;
    --loki)     LOKI_URL="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --warmup)   WARMUP="$2"; shift 2 ;;
    --output)   OUTPUT="$2"; shift 2 ;;
    --skip-ingest) SKIP_INGEST=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ -z "$OUTPUT" ]]; then
  mkdir -p results
  OUTPUT="results/comparative-$(date +%Y%m%d-%H%M%S).json"
fi

# Always run preflight validation before benchmarks.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREFLIGHT_ARGS=(--lh "$LH_URL" --vl "$VL_URL" --loki "$LOKI_URL")
if [[ "$SKIP_INGEST" == "true" ]]; then
  PREFLIGHT_ARGS+=(--min-logs 100000)
else
  PREFLIGHT_ARGS+=(--min-logs 500000)
fi
echo "--- Running preflight validation ---"
if ! bash "$SCRIPT_DIR/benchmark-preflight.sh" "${PREFLIGHT_ARGS[@]}"; then
  echo "ABORT: Preflight validation failed. Fix issues before benchmarking."
  exit 1
fi
echo ""

NOW_NS=$(date +%s)000000000
ONE_HOUR_AGO_NS=$(( ($(date +%s) - 3600) ))000000000
SIX_HOURS_AGO_NS=$(( ($(date +%s) - 21600) ))000000000
ONE_DAY_AGO_NS=$(( ($(date +%s) - 86400) ))000000000
TWO_DAYS_AGO_NS=$(( ($(date +%s) - 172800) ))000000000
FUTURE_NS=$(( ($(date +%s) + 31536000) ))000000000
FUTURE_END_NS=$(( ($(date +%s) + 31539600) ))000000000

NOW_S=$(date +%s)
ONE_HOUR_AGO_S=$(( NOW_S - 3600 ))
SIX_HOURS_AGO_S=$(( NOW_S - 21600 ))
ONE_DAY_AGO_S=$(( NOW_S - 86400 ))
TWO_DAYS_AGO_S=$(( NOW_S - 172800 ))

measure_query() {
  local name="$1"
  local system="$2"
  local url="$3"
  local iters="$4"
  local warmup_iters="$5"

  # Warmup
  for (( i=0; i<warmup_iters; i++ )); do
    curl -sf -o /dev/null "$url" 2>/dev/null || true
  done

  # Measure
  local latencies=()
  local total_bytes=0
  for (( i=0; i<iters; i++ )); do
    local start_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    local curl_out
    curl_out=$(curl -sf -o /dev/null -w "%{http_code} %{size_download}" "$url" 2>/dev/null) || curl_out="000 0"
    local end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    local elapsed=$(( end_ms - start_ms ))
    local status=${curl_out%% *}
    local bytes=${curl_out##* }
    if [[ "$status" =~ ^2 ]]; then
      latencies+=("$elapsed")
      total_bytes=$(( total_bytes + ${bytes%%.*} ))
    fi
  done

  if [[ ${#latencies[@]} -eq 0 ]]; then
    echo "{\"name\":\"$name\",\"system\":\"$system\",\"p50_ms\":null,\"p95_ms\":null,\"p99_ms\":null,\"iterations\":0,\"errors\":$iters,\"avg_bytes\":0}"
    return
  fi

  # Sort and compute percentiles
  IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}")); unset IFS
  local n=${#sorted[@]}
  local p50_idx=$(( n * 50 / 100 ))
  local p95_idx=$(( n * 95 / 100 ))
  local p99_idx=$(( n * 99 / 100 ))
  [[ $p50_idx -ge $n ]] && p50_idx=$(( n - 1 ))
  [[ $p95_idx -ge $n ]] && p95_idx=$(( n - 1 ))
  [[ $p99_idx -ge $n ]] && p99_idx=$(( n - 1 ))
  local errors=$(( iters - n ))
  local avg_bytes=$(( total_bytes / n ))

  echo "{\"name\":\"$name\",\"system\":\"$system\",\"p50_ms\":${sorted[$p50_idx]},\"p95_ms\":${sorted[$p95_idx]},\"p99_ms\":${sorted[$p99_idx]},\"min_ms\":${sorted[0]},\"max_ms\":${sorted[$((n-1))]},\"iterations\":$n,\"errors\":$errors,\"avg_bytes\":$avg_bytes}"
}

# Each scenario: name | LH URL | VL URL | Loki URL
run_scenario() {
  local name="$1"
  local lh_url="$2"
  local vl_url="$3"
  local loki_url="$4"

  printf "  %-40s " "$name"

  local lh_result vl_result loki_result
  lh_result=$(measure_query "$name" "lakehouse" "$lh_url" "$ITERATIONS" "$WARMUP")
  vl_result=$(measure_query "$name" "victorialogs" "$vl_url" "$ITERATIONS" "$WARMUP")
  loki_result=$(measure_query "$name" "loki" "$loki_url" "$ITERATIONS" "$WARMUP")

  local lh_p95=$(echo "$lh_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")
  local vl_p95=$(echo "$vl_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")
  local loki_p95=$(echo "$loki_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")

  printf "LH=%sms  VL=%sms  Loki=%sms\n" "$lh_p95" "$vl_p95" "$loki_p95"

  echo "$lh_result" >> "$TMPFILE"
  echo "$vl_result" >> "$TMPFILE"
  echo "$loki_result" >> "$TMPFILE"
}

echo "=== Comparative Benchmark ==="
echo "Lakehouse: $LH_URL"
echo "VictoriaLogs: $VL_URL"
echo "Loki: $LOKI_URL"
echo "Iterations: $ITERATIONS  Warmup: $WARMUP"
echo ""

TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

echo "--- Fast Path ---"
run_scenario "manifest_nothing_here" \
  "${LH_URL}/select/logsql/query?query=*&start=${FUTURE_NS}&end=${FUTURE_END_NS}" \
  "${VL_URL}/select/logsql/query?query=*&start=${FUTURE_NS}&end=${FUTURE_END_NS}" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D&start=${FUTURE_NS}&end=${FUTURE_END_NS}&limit=1"

echo ""
echo "--- Point Lookups ---"
run_scenario "bloom_trace_id_hit" \
  "${LH_URL}/select/logsql/query?query=trace_id%3A%3D%220000000000000001%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=1" \
  "${VL_URL}/select/logsql/query?query=trace_id%3A%3D%220000000000000001%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=1" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D%20%7C%3D%20%220000000000000001%22&start=${TWO_DAYS_AGO_S}&end=${NOW_S}&limit=1"

run_scenario "bloom_trace_id_miss" \
  "${LH_URL}/select/logsql/query?query=trace_id%3A%3D%22ffffffffffffffffffffffffffffffff%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=1" \
  "${VL_URL}/select/logsql/query?query=trace_id%3A%3D%22ffffffffffffffffffffffffffffffff%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=1" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D%20%7C%3D%20%22ffffffffffffffffffffffffffffffff%22&start=${TWO_DAYS_AGO_S}&end=${NOW_S}&limit=1"

run_scenario "bloom_service_exact" \
  "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22api-gateway%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=10" \
  "${VL_URL}/select/logsql/query?query=service.name%3A%3D%22api-gateway%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=10" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D%22api-gateway%22%7D&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=10"

echo ""
echo "--- Short Range (1h) ---"
run_scenario "short_range_1h_wildcard" \
  "${LH_URL}/select/logsql/query?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${VL_URL}/select/logsql/query?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=100"

run_scenario "short_range_1h_filtered" \
  "${LH_URL}/select/logsql/query?query=level%3A%3D%22ERROR%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${VL_URL}/select/logsql/query?query=level%3A%3D%22ERROR%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Blevel%3D%22ERROR%22%7D&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=100"

run_scenario "short_range_1h_service_level" \
  "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22api-gateway%22%20AND%20level%3A%3D%22ERROR%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${VL_URL}/select/logsql/query?query=service.name%3A%3D%22api-gateway%22%20AND%20level%3A%3D%22ERROR%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D%22api-gateway%22%2Clevel%3D%22ERROR%22%7D&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=50"

echo ""
echo "--- Medium Range (6h) ---"
run_scenario "medium_range_6h_wildcard" \
  "${LH_URL}/select/logsql/query?query=*&start=${SIX_HOURS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${VL_URL}/select/logsql/query?query=*&start=${SIX_HOURS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D&start=${SIX_HOURS_AGO_S}&end=${NOW_S}&limit=200"

run_scenario "medium_range_6h_substring" \
  "${LH_URL}/select/logsql/query?query=%22database%20query%22&start=${SIX_HOURS_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${VL_URL}/select/logsql/query?query=%22database%20query%22&start=${SIX_HOURS_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D%20%7C%3D%20%22database%20query%22&start=${SIX_HOURS_AGO_S}&end=${NOW_S}&limit=100"

echo ""
echo "--- Long Range (24h-48h) ---"
run_scenario "long_range_24h_wildcard" \
  "${LH_URL}/select/logsql/query?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${VL_URL}/select/logsql/query?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D&start=${ONE_DAY_AGO_S}&end=${NOW_S}&limit=500"

run_scenario "long_range_48h_service_filter" \
  "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22payment-service%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${VL_URL}/select/logsql/query?query=service.name%3A%3D%22payment-service%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D%22payment-service%22%7D&start=${TWO_DAYS_AGO_S}&end=${NOW_S}&limit=200"

run_scenario "long_range_48h_all_errors" \
  "${LH_URL}/select/logsql/query?query=level%3A%3D%22ERROR%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${VL_URL}/select/logsql/query?query=level%3A%3D%22ERROR%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Blevel%3D%22ERROR%22%7D&start=${TWO_DAYS_AGO_S}&end=${NOW_S}&limit=500"

echo ""
echo "--- Aggregation ---"
run_scenario "stats_1h_count" \
  "${LH_URL}/select/logsql/stats_query?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}" \
  "${VL_URL}/select/logsql/stats_query?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}" \
  "${LOKI_URL}/loki/api/v1/query?query=sum(count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B1h%5D))&time=${NOW_S}"

run_scenario "stats_24h_count" \
  "${LH_URL}/select/logsql/stats_query?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}" \
  "${VL_URL}/select/logsql/stats_query?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}" \
  "${LOKI_URL}/loki/api/v1/query?query=sum(count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B24h%5D))&time=${NOW_S}"

run_scenario "stats_range_1h_step_5m" \
  "${LH_URL}/select/logsql/stats_query_range?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&step=300s" \
  "${VL_URL}/select/logsql/stats_query_range?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&step=300s" \
  "${LOKI_URL}/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B5m%5D)&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&step=300"

run_scenario "stats_range_24h_step_1h" \
  "${LH_URL}/select/logsql/stats_query_range?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&step=3600s" \
  "${VL_URL}/select/logsql/stats_query_range?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&step=3600s" \
  "${LOKI_URL}/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B1h%5D)&start=${ONE_DAY_AGO_S}&end=${NOW_S}&step=3600"

echo ""
echo "--- Metadata ---"
run_scenario "field_names" \
  "${LH_URL}/select/logsql/field_names?query=*&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}" \
  "${VL_URL}/select/logsql/field_names?query=*&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}" \
  "${LOKI_URL}/loki/api/v1/labels?start=${TWO_DAYS_AGO_S}&end=${NOW_S}"

run_scenario "field_values_service" \
  "${LH_URL}/select/logsql/field_values?query=*&field=service.name&limit=100&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}" \
  "${VL_URL}/select/logsql/field_values?query=*&field=service.name&limit=100&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}" \
  "${LOKI_URL}/loki/api/v1/label/service_name/values?start=${TWO_DAYS_AGO_S}&end=${NOW_S}"

run_scenario "streams_list" \
  "${LH_URL}/select/logsql/streams?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}" \
  "${VL_URL}/select/logsql/streams?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}" \
  "${LOKI_URL}/loki/api/v1/series?match[]=%7Bservice_name%3D~%22.%2B%22%7D&start=${ONE_HOUR_AGO_S}&end=${NOW_S}"

echo ""
echo "--- Histogram ---"
run_scenario "hits_1h_step_5m" \
  "${LH_URL}/select/logsql/hits?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&step=300s" \
  "${VL_URL}/select/logsql/hits?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&step=300s" \
  "${LOKI_URL}/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B5m%5D)&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&step=300"

run_scenario "hits_24h_step_1h" \
  "${LH_URL}/select/logsql/hits?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&step=3600s" \
  "${VL_URL}/select/logsql/hits?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&step=3600s" \
  "${LOKI_URL}/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B1h%5D)&start=${ONE_DAY_AGO_S}&end=${NOW_S}&step=3600"

echo ""
echo "=== Results ==="

# Assemble JSON output
python3 -c "
import json, sys
results = []
with open('$TMPFILE') as f:
    for line in f:
        line = line.strip()
        if line:
            results.append(json.loads(line))

# Group by scenario
scenarios = {}
for r in results:
    name = r['name']
    if name not in scenarios:
        scenarios[name] = {}
    scenarios[name][r['system']] = r

output = {
    'timestamp': '$(date -u +%Y-%m-%dT%H:%M:%SZ)',
    'config': {
        'iterations': $ITERATIONS,
        'warmup': $WARMUP,
        'lakehouse_url': '$LH_URL',
        'victorialogs_url': '$VL_URL',
        'loki_url': '$LOKI_URL'
    },
    'scenarios': scenarios
}

with open('$OUTPUT', 'w') as f:
    json.dump(output, f, indent=2)

# Print summary table
print()
print(f'  {\"Scenario\":<40} {\"LH p95\":>10} {\"VL p95\":>10} {\"Loki p95\":>10} {\"LH/VL\":>8} {\"LH/Loki\":>8}')
print('  ' + '-' * 90)
for name in dict.fromkeys(r['name'] for r in results):
    s = scenarios.get(name, {})
    lh = s.get('lakehouse', {}).get('p95_ms')
    vl = s.get('victorialogs', {}).get('p95_ms')
    loki = s.get('loki', {}).get('p95_ms')
    lh_str = f'{lh}ms' if lh is not None else 'N/A'
    vl_str = f'{vl}ms' if vl is not None else 'N/A'
    loki_str = f'{loki}ms' if loki is not None else 'N/A'
    ratio_vl = f'{lh/vl:.1f}x' if lh and vl else 'N/A'
    ratio_loki = f'{lh/loki:.1f}x' if lh and loki else 'N/A'
    print(f'  {name:<40} {lh_str:>10} {vl_str:>10} {loki_str:>10} {ratio_vl:>8} {ratio_loki:>8}')

print()
print(f'Results saved to: $OUTPUT')
"
