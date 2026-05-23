#!/usr/bin/env bash
set -euo pipefail

# Focused comparative benchmark: runs the worst-performing query scenarios
# against Lakehouse, VictoriaLogs, Loki, and ClickHouse (reading LH parquet from S3).
# Captures pprof CPU profile from Lakehouse during each scenario.
#
# Usage:
#   ./scripts/comparative-benchmark-focused.sh [options]
#
# Options:
#   --lh URL        Lakehouse logs endpoint (default: http://localhost:39428)
#   --vl URL        VictoriaLogs endpoint (default: http://localhost:39401)
#   --loki URL      Loki endpoint (default: http://localhost:33100)
#   --ch URL        ClickHouse HTTP endpoint (default: http://localhost:38123)
#   --iterations N  Iterations per scenario (default: 5)
#   --warmup N      Warmup iterations (default: 2)
#   --output FILE   Output JSON file
#   --pprof-dir DIR Directory for pprof profiles (default: results/pprof)
#   --skip-pprof    Skip pprof collection

LH_URL="http://localhost:39428"
VL_URL="http://localhost:39401"
LOKI_URL="http://localhost:33100"
CH_URL="http://localhost:38123"
CH_USER="default"
CH_PASS="benchmark"
ITERATIONS=5
WARMUP=2
OUTPUT=""
PPROF_DIR="results/pprof"
SKIP_PPROF=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lh)         LH_URL="$2"; shift 2 ;;
    --vl)         VL_URL="$2"; shift 2 ;;
    --loki)       LOKI_URL="$2"; shift 2 ;;
    --ch)         CH_URL="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --warmup)     WARMUP="$2"; shift 2 ;;
    --output)     OUTPUT="$2"; shift 2 ;;
    --pprof-dir)  PPROF_DIR="$2"; shift 2 ;;
    --skip-pprof) SKIP_PPROF=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ -z "$OUTPUT" ]]; then
  mkdir -p results
  OUTPUT="results/comparative-focused-$(date +%Y%m%d-%H%M%S).json"
fi

mkdir -p "$PPROF_DIR"

NOW_NS=$(date +%s)000000000
NOW_S=$(date +%s)
ONE_HOUR_AGO_NS=$(( (NOW_S - 3600) ))000000000
ONE_HOUR_AGO_S=$(( NOW_S - 3600 ))
SIX_HOURS_AGO_NS=$(( (NOW_S - 21600) ))000000000
SIX_HOURS_AGO_S=$(( NOW_S - 21600 ))
ONE_DAY_AGO_NS=$(( (NOW_S - 86400) ))000000000
ONE_DAY_AGO_S=$(( NOW_S - 86400 ))
TWO_DAYS_AGO_NS=$(( (NOW_S - 172800) ))000000000
TWO_DAYS_AGO_S=$(( NOW_S - 172800 ))

measure_query() {
  local name="$1"
  local system="$2"
  local url="$3"
  local iters="$4"
  local warmup_iters="$5"
  local method="${6:-GET}"
  local post_data="${7:-}"

  # Warmup
  for (( i=0; i<warmup_iters; i++ )); do
    if [[ "$method" == "POST" ]]; then
      curl -sf -o /dev/null -X POST -d "$post_data" "$url" 2>/dev/null || true
    else
      curl -sf -o /dev/null "$url" 2>/dev/null || true
    fi
  done

  local latencies=()
  local total_bytes=0
  local http_codes=""
  for (( i=0; i<iters; i++ )); do
    local start_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    local curl_out
    if [[ "$method" == "POST" ]]; then
      curl_out=$(curl -sf -o /dev/null -w "%{http_code} %{size_download}" -X POST -d "$post_data" "$url" 2>/dev/null) || curl_out="000 0"
    else
      curl_out=$(curl -sf -o /dev/null -w "%{http_code} %{size_download}" "$url" 2>/dev/null) || curl_out="000 0"
    fi
    local end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    local elapsed=$(( end_ms - start_ms ))
    local status=${curl_out%% *}
    local bytes=${curl_out##* }
    http_codes="${http_codes:+$http_codes,}$status"
    if [[ "$status" =~ ^2 ]]; then
      latencies+=("$elapsed")
      total_bytes=$(( total_bytes + ${bytes%%.*} ))
    fi
  done

  if [[ ${#latencies[@]} -eq 0 ]]; then
    echo "{\"name\":\"$name\",\"system\":\"$system\",\"p50_ms\":null,\"p95_ms\":null,\"p99_ms\":null,\"iterations\":0,\"errors\":$iters,\"avg_bytes\":0,\"http_codes\":\"$http_codes\",\"qps\":0}"
    return
  fi

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
  local total_ms=0
  for l in "${latencies[@]}"; do total_ms=$((total_ms + l)); done
  local qps
  if [[ $total_ms -gt 0 ]]; then
    qps=$(python3 -c "print(round($n / ($total_ms / 1000.0), 1))")
  else
    qps=0
  fi

  echo "{\"name\":\"$name\",\"system\":\"$system\",\"p50_ms\":${sorted[$p50_idx]},\"p95_ms\":${sorted[$p95_idx]},\"p99_ms\":${sorted[$p99_idx]},\"min_ms\":${sorted[0]},\"max_ms\":${sorted[$((n-1))]},\"iterations\":$n,\"errors\":$errors,\"avg_bytes\":$avg_bytes,\"http_codes\":\"${http_codes}\",\"qps\":$qps}"
}

collect_pprof() {
  local name="$1"
  local duration="$2"
  if [[ "$SKIP_PPROF" == "true" ]]; then return; fi
  local pprof_file="${PPROF_DIR}/${name}.pb.gz"
  curl -sf -o "$pprof_file" "${LH_URL}/debug/pprof/profile?seconds=${duration}" 2>/dev/null &
  PPROF_PID=$!
}

wait_pprof() {
  if [[ "$SKIP_PPROF" == "true" ]]; then return; fi
  if [[ -n "${PPROF_PID:-}" ]]; then
    wait "$PPROF_PID" 2>/dev/null || true
    unset PPROF_PID
  fi
}

run_scenario() {
  local name="$1"
  local lh_url="$2"
  local vl_url="$3"
  local loki_url="$4"
  local ch_query="${5:-}"

  printf "  %-40s " "$name"

  # Start pprof for LH during measurement
  local pprof_secs=$(( (WARMUP + ITERATIONS) * 2 + 5 ))
  collect_pprof "$name" "$pprof_secs"

  local lh_result vl_result loki_result ch_result
  lh_result=$(measure_query "$name" "lakehouse" "$lh_url" "$ITERATIONS" "$WARMUP")
  vl_result=$(measure_query "$name" "victorialogs" "$vl_url" "$ITERATIONS" "$WARMUP")
  loki_result=$(measure_query "$name" "loki" "$loki_url" "$ITERATIONS" "$WARMUP")

  if [[ -n "$ch_query" ]]; then
    ch_result=$(measure_query "$name" "clickhouse" "${CH_URL}/?database=lakehouse&user=${CH_USER}&password=${CH_PASS}" "$ITERATIONS" "$WARMUP" "POST" "$ch_query")
  else
    ch_result="{\"name\":\"$name\",\"system\":\"clickhouse\",\"p50_ms\":null,\"p95_ms\":null,\"p99_ms\":null,\"iterations\":0,\"errors\":0,\"avg_bytes\":0,\"http_codes\":\"\",\"qps\":0}"
  fi

  wait_pprof

  local lh_p95=$(echo "$lh_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")
  local vl_p95=$(echo "$vl_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")
  local loki_p95=$(echo "$loki_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")
  local ch_p95=$(echo "$ch_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")

  printf "LH=%sms  VL=%sms  Loki=%sms  CH=%sms\n" "$lh_p95" "$vl_p95" "$loki_p95" "$ch_p95"

  echo "$lh_result" >> "$TMPFILE"
  echo "$vl_result" >> "$TMPFILE"
  echo "$loki_result" >> "$TMPFILE"
  echo "$ch_result" >> "$TMPFILE"
}

echo "=== Focused Comparative Benchmark (worst scenarios + ClickHouse + pprof) ==="
echo "Lakehouse:    $LH_URL"
echo "VictoriaLogs: $VL_URL"
echo "Loki:         $LOKI_URL"
echo "ClickHouse:   $CH_URL"
echo "Iterations:   $ITERATIONS  Warmup: $WARMUP"
echo "Pprof:        ${SKIP_PPROF:-false} -> $PPROF_DIR"
echo ""

TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

echo "--- Long Range (24h-48h) ---"
run_scenario "long_range_24h_wildcard" \
  "${LH_URL}/select/logsql/query?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${VL_URL}/select/logsql/query?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D&start=${ONE_DAY_AGO_S}&end=${NOW_S}&limit=500" \
  "SELECT * FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${ONE_DAY_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) LIMIT 500 FORMAT JSON"

run_scenario "long_range_48h_service_filter" \
  "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22payment-service%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${VL_URL}/select/logsql/query?query=service.name%3A%3D%22payment-service%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D%22payment-service%22%7D&start=${TWO_DAYS_AGO_S}&end=${NOW_S}&limit=200" \
  "SELECT * FROM lakehouse.otel_logs WHERE ServiceName = 'payment-service' AND Timestamp >= toDateTime64(${TWO_DAYS_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) LIMIT 200 FORMAT JSON"

run_scenario "long_range_48h_all_errors" \
  "${LH_URL}/select/logsql/query?query=level%3A%3D%22ERROR%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${VL_URL}/select/logsql/query?query=level%3A%3D%22ERROR%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Blevel%3D%22ERROR%22%7D&start=${TWO_DAYS_AGO_S}&end=${NOW_S}&limit=500" \
  "SELECT * FROM lakehouse.otel_logs WHERE SeverityText = 'ERROR' AND Timestamp >= toDateTime64(${TWO_DAYS_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) LIMIT 500 FORMAT JSON"

echo ""
echo "--- Aggregation (stats) ---"
run_scenario "stats_1h_count" \
  "${LH_URL}/select/logsql/stats_query?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}" \
  "${VL_URL}/select/logsql/stats_query?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}" \
  "${LOKI_URL}/loki/api/v1/query?query=sum(count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B1h%5D))&time=${NOW_S}" \
  "SELECT count(*) as total FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${ONE_HOUR_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) FORMAT JSON"

run_scenario "stats_24h_count" \
  "${LH_URL}/select/logsql/stats_query?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}" \
  "${VL_URL}/select/logsql/stats_query?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}" \
  "${LOKI_URL}/loki/api/v1/query?query=sum(count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B24h%5D))&time=${NOW_S}" \
  "SELECT count(*) as total FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${ONE_DAY_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) FORMAT JSON"

run_scenario "stats_range_1h_step_5m" \
  "${LH_URL}/select/logsql/stats_query_range?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&step=300s" \
  "${VL_URL}/select/logsql/stats_query_range?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&step=300s" \
  "${LOKI_URL}/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B5m%5D)&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&step=300" \
  "SELECT toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) as bucket, count(*) as total FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${ONE_HOUR_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) GROUP BY bucket ORDER BY bucket FORMAT JSON"

run_scenario "stats_range_24h_step_1h" \
  "${LH_URL}/select/logsql/stats_query_range?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&step=3600s" \
  "${VL_URL}/select/logsql/stats_query_range?query=*%20%7C%20stats%20count(*)%20as%20total&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&step=3600s" \
  "${LOKI_URL}/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B1h%5D)&start=${ONE_DAY_AGO_S}&end=${NOW_S}&step=3600" \
  "SELECT toStartOfInterval(Timestamp, INTERVAL 1 HOUR) as bucket, count(*) as total FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${ONE_DAY_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) GROUP BY bucket ORDER BY bucket FORMAT JSON"

echo ""
echo "--- Histogram (hits) ---"
run_scenario "hits_1h_step_5m" \
  "${LH_URL}/select/logsql/hits?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&step=300s" \
  "${VL_URL}/select/logsql/hits?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&step=300s" \
  "${LOKI_URL}/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B5m%5D)&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&step=300" \
  "SELECT toStartOfInterval(Timestamp, INTERVAL 5 MINUTE) as bucket, count(*) as total FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${ONE_HOUR_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) GROUP BY bucket ORDER BY bucket FORMAT JSON"

run_scenario "hits_24h_step_1h" \
  "${LH_URL}/select/logsql/hits?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&step=3600s" \
  "${VL_URL}/select/logsql/hits?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&step=3600s" \
  "${LOKI_URL}/loki/api/v1/query_range?query=count_over_time(%7Bservice_name%3D~%22.%2B%22%7D%5B1h%5D)&start=${ONE_DAY_AGO_S}&end=${NOW_S}&step=3600" \
  "SELECT toStartOfInterval(Timestamp, INTERVAL 1 HOUR) as bucket, count(*) as total FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${ONE_DAY_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) GROUP BY bucket ORDER BY bucket FORMAT JSON"

echo ""
echo "--- Short/Medium Range (control group) ---"
run_scenario "short_range_1h_wildcard" \
  "${LH_URL}/select/logsql/query?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${VL_URL}/select/logsql/query?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=100" \
  "SELECT * FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${ONE_HOUR_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) LIMIT 100 FORMAT JSON"

run_scenario "medium_range_6h_wildcard" \
  "${LH_URL}/select/logsql/query?query=*&start=${SIX_HOURS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${VL_URL}/select/logsql/query?query=*&start=${SIX_HOURS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D&start=${SIX_HOURS_AGO_S}&end=${NOW_S}&limit=200" \
  "SELECT * FROM lakehouse.otel_logs WHERE Timestamp >= toDateTime64(${SIX_HOURS_AGO_S}, 9) AND Timestamp <= toDateTime64(${NOW_S}, 9) LIMIT 200 FORMAT JSON"

echo ""
echo "--- Point Lookup (bloom) ---"
run_scenario "bloom_trace_id_hit" \
  "${LH_URL}/select/logsql/query?query=trace_id%3A%3D%220000000000000001%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=1" \
  "${VL_URL}/select/logsql/query?query=trace_id%3A%3D%220000000000000001%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=1" \
  "${LOKI_URL}/loki/api/v1/query_range?query=%7Bservice_name%3D~%22.%2B%22%7D%20%7C%3D%20%220000000000000001%22&start=${TWO_DAYS_AGO_S}&end=${NOW_S}&limit=1" \
  "SELECT * FROM lakehouse.otel_logs WHERE TraceId = '0000000000000001' LIMIT 1 FORMAT JSON"

echo ""
echo "=== Results ==="

python3 -c "
import json, sys
results = []
with open('$TMPFILE') as f:
    for line in f:
        line = line.strip()
        if line:
            results.append(json.loads(line))

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
        'loki_url': '$LOKI_URL',
        'clickhouse_url': '$CH_URL'
    },
    'scenarios': scenarios
}

with open('$OUTPUT', 'w') as f:
    json.dump(output, f, indent=2)

print()
print(f'  {\"Scenario\":<40} {\"LH p95\":>10} {\"VL p95\":>10} {\"Loki p95\":>10} {\"CH p95\":>10} {\"LH/VL\":>8} {\"LH/Loki\":>8} {\"LH/CH\":>8}')
print('  ' + '-' * 110)
seen = []
for r in results:
    if r['name'] not in seen:
        seen.append(r['name'])
for name in seen:
    s = scenarios.get(name, {})
    lh = s.get('lakehouse', {}).get('p95_ms')
    vl = s.get('victorialogs', {}).get('p95_ms')
    loki = s.get('loki', {}).get('p95_ms')
    ch = s.get('clickhouse', {}).get('p95_ms')
    lh_str = f'{lh}ms' if lh is not None else 'N/A'
    vl_str = f'{vl}ms' if vl is not None else 'N/A'
    loki_str = f'{loki}ms' if loki is not None else 'N/A'
    ch_str = f'{ch}ms' if ch is not None else 'N/A'
    ratio_vl = f'{lh/vl:.1f}x' if lh and vl else 'N/A'
    ratio_loki = f'{lh/loki:.1f}x' if lh and loki else 'N/A'
    ratio_ch = f'{lh/ch:.1f}x' if lh and ch else 'N/A'
    print(f'  {name:<40} {lh_str:>10} {vl_str:>10} {loki_str:>10} {ch_str:>10} {ratio_vl:>8} {ratio_loki:>8} {ratio_ch:>8}')

print()
print(f'Results saved to: $OUTPUT')
if '$SKIP_PPROF' != 'true':
    print(f'Pprof profiles saved to: $PPROF_DIR/')
    print(f'  Analyze with: go tool pprof -http=:8080 $PPROF_DIR/<scenario>.pb.gz')
"
