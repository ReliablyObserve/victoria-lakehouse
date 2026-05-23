#!/usr/bin/env bash
set -euo pipefail

# Comparative benchmark: runs identical trace query scenarios against Lakehouse-Traces,
# VictoriaTraces, and Tempo. Measures p50/p95/p99 per system per scenario. Outputs JSON results.
#
# Usage:
#   ./scripts/comparative-benchmark-traces.sh [options]
#
# Options:
#   --lh URL        Lakehouse-Traces endpoint (default: http://localhost:30428)
#   --vt URL        VictoriaTraces endpoint (default: http://localhost:30401)
#   --tempo URL     Tempo endpoint (default: http://localhost:33200)
#   --iterations N  Iterations per scenario (default: 10)
#   --warmup N      Warmup iterations (default: 3)
#   --output FILE   Output JSON file (default: results/comparative-traces-YYYYMMDD.json)
#   --skip-ingest   Skip data generation (data already loaded)

LH_URL="http://localhost:30428"
VT_URL="http://localhost:30401"
TEMPO_URL="http://localhost:33200"
ITERATIONS=10
WARMUP=3
OUTPUT=""
SKIP_INGEST=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lh)       LH_URL="$2"; shift 2 ;;
    --vt)       VT_URL="$2"; shift 2 ;;
    --tempo)    TEMPO_URL="$2"; shift 2 ;;
    --iterations) ITERATIONS="$2"; shift 2 ;;
    --warmup)   WARMUP="$2"; shift 2 ;;
    --output)   OUTPUT="$2"; shift 2 ;;
    --skip-ingest) SKIP_INGEST=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ -z "$OUTPUT" ]]; then
  mkdir -p results
  OUTPUT="results/comparative-traces-$(date +%Y%m%d-%H%M%S).json"
fi

# --- Time ranges ---
NOW_NS=$(date +%s)000000000
ONE_HOUR_AGO_NS=$(( ($(date +%s) - 3600) ))000000000
SIX_HOURS_AGO_NS=$(( ($(date +%s) - 21600) ))000000000
ONE_DAY_AGO_NS=$(( ($(date +%s) - 86400) ))000000000
TWO_DAYS_AGO_NS=$(( ($(date +%s) - 172800) ))000000000

NOW_S=$(date +%s)
ONE_HOUR_AGO_S=$(( NOW_S - 3600 ))
SIX_HOURS_AGO_S=$(( NOW_S - 21600 ))
ONE_DAY_AGO_S=$(( NOW_S - 86400 ))
TWO_DAYS_AGO_S=$(( NOW_S - 172800 ))

# --- Sample trace/span IDs for point lookups ---
# Auto-detect a real trace_id — try VT first (fastest), then LH
SAMPLE_TRACE_ID=""
for src_url in "$VT_URL" "$LH_URL"; do
  SAMPLE_TRACE_ID=$(curl -sf "${src_url}/select/logsql/query?query=*&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=1" 2>/dev/null | python3 -c "import sys,json; print(json.loads(sys.stdin.readline()).get('trace_id',''))" 2>/dev/null || true)
  if [[ -n "$SAMPLE_TRACE_ID" ]]; then break; fi
done
if [[ -z "$SAMPLE_TRACE_ID" ]]; then
  SAMPLE_TRACE_ID="82ad65251702e15f9823289f4748b11b"
  echo "WARNING: could not auto-detect trace_id, using fallback"
fi
echo "Sample trace_id: $SAMPLE_TRACE_ID"
SAMPLE_TRACE_ID_MISS="ffffffffffffffffffffffffffffffff"

# --- Measurement function ---
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
    local start_ms
    start_ms=$(python3 -c "import time; print(int(time.time()*1000))")
    local curl_out
    curl_out=$(curl -sf -o /dev/null -w "%{http_code} %{size_download}" "$url" 2>/dev/null) || curl_out="000 0"
    local end_ms
    end_ms=$(python3 -c "import time; print(int(time.time()*1000))")
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

# --- Scenario runner ---
run_scenario() {
  local name="$1"
  local lh_url="$2"
  local vt_url="$3"
  local tempo_url="$4"

  printf "  %-45s " "$name"

  local lh_result vt_result tempo_result
  lh_result=$(measure_query "$name" "lakehouse-traces" "$lh_url" "$ITERATIONS" "$WARMUP")
  vt_result=$(measure_query "$name" "victoriatraces" "$vt_url" "$ITERATIONS" "$WARMUP")
  tempo_result=$(measure_query "$name" "tempo" "$tempo_url" "$ITERATIONS" "$WARMUP")

  local lh_p95 vt_p95 tempo_p95
  lh_p95=$(echo "$lh_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")
  vt_p95=$(echo "$vt_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")
  tempo_p95=$(echo "$tempo_result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('p95_ms','N/A'))")

  printf "LH=%sms  VT=%sms  Tempo=%sms\n" "$lh_p95" "$vt_p95" "$tempo_p95"

  echo "$lh_result" >> "$TMPFILE"
  echo "$vt_result" >> "$TMPFILE"
  echo "$tempo_result" >> "$TMPFILE"
}

# --- Header ---
echo "=== Comparative Trace Benchmark ==="
echo "Lakehouse-Traces: $LH_URL"
echo "VictoriaTraces:   $VT_URL"
echo "Tempo:            $TEMPO_URL"
echo "Iterations: $ITERATIONS  Warmup: $WARMUP"
echo ""

TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

# ============================================================
# Scenario 1: trace_id exact lookup (hit)
# ============================================================
echo "--- Point Lookups ---"
run_scenario "trace_id_exact_hit" \
  "${LH_URL}/select/logsql/query?query=trace_id%3A%3D%22${SAMPLE_TRACE_ID}%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${VT_URL}/select/logsql/query?query=trace_id%3A%3D%22${SAMPLE_TRACE_ID}%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${TEMPO_URL}/api/traces/${SAMPLE_TRACE_ID}"

# ============================================================
# Scenario 1b: trace_id exact lookup (miss)
# ============================================================
run_scenario "trace_id_exact_miss" \
  "${LH_URL}/select/logsql/query?query=trace_id%3A%3D%22${SAMPLE_TRACE_ID_MISS}%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=1" \
  "${VT_URL}/select/logsql/query?query=trace_id%3A%3D%22${SAMPLE_TRACE_ID_MISS}%22&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}&limit=1" \
  "${TEMPO_URL}/api/traces/${SAMPLE_TRACE_ID_MISS}"

# ============================================================
# Scenario 2: service.name filter
# ============================================================
echo ""
echo "--- Service & Span Filters ---"
run_scenario "service_name_filter" \
  "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22api-gateway%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${VT_URL}/select/logsql/query?query=resource_attr%3Aservice.name%3A%3D%22api-gateway%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${TEMPO_URL}/api/search?tags=service.name%3Dapi-gateway&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=50"

# ============================================================
# Scenario 3: span name filter
# ============================================================
run_scenario "span_name_filter" \
  "${LH_URL}/select/logsql/query?query=name%3A~%22HTTP%20GET%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${VT_URL}/select/logsql/query?query=name%3A~%22HTTP%20GET%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${TEMPO_URL}/api/search?tags=name%3D%22HTTP%20GET%22&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=50"

# ============================================================
# Scenario 4: duration filter (slow spans > 1s)
# ============================================================
run_scenario "duration_slow_spans" \
  "${LH_URL}/select/logsql/query?query=duration%3A%3E1000000000&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${VT_URL}/select/logsql/query?query=duration%3A%3E1000000000&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${TEMPO_URL}/api/search?minDuration=1s&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=50"

# ============================================================
# Scenario 5: status error filter
# ============================================================
run_scenario "status_error_filter" \
  "${LH_URL}/select/logsql/query?query=status_code%3A%3D%222%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${VT_URL}/select/logsql/query?query=status_code%3A%3D%222%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${TEMPO_URL}/api/search?tags=status%3Derror&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=50"

# ============================================================
# Scenario 6: service + time range (1h)
# ============================================================
echo ""
echo "--- Time Range Queries ---"
run_scenario "service_range_1h" \
  "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22api-gateway%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${VT_URL}/select/logsql/query?query=resource_attr%3Aservice.name%3A%3D%22api-gateway%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=100" \
  "${TEMPO_URL}/api/search?tags=service.name%3Dapi-gateway&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=100"

# ============================================================
# Scenario 7: service + time range (6h)
# ============================================================
run_scenario "service_range_6h" \
  "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22api-gateway%22&start=${SIX_HOURS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${VT_URL}/select/logsql/query?query=resource_attr%3Aservice.name%3A%3D%22api-gateway%22&start=${SIX_HOURS_AGO_NS}&end=${NOW_NS}&limit=200" \
  "${TEMPO_URL}/api/search?tags=service.name%3Dapi-gateway&start=${SIX_HOURS_AGO_S}&end=${NOW_S}&limit=200"

# ============================================================
# Scenario 8: service + status error
# ============================================================
echo ""
echo "--- Combined Filters ---"
run_scenario "service_and_error" \
  "${LH_URL}/select/logsql/query?query=service.name%3A%3D%22api-gateway%22%20AND%20status_code%3A%3D%222%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${VT_URL}/select/logsql/query?query=resource_attr%3Aservice.name%3A%3D%22api-gateway%22%20AND%20status_code%3A%3D%222%22&start=${ONE_HOUR_AGO_NS}&end=${NOW_NS}&limit=50" \
  "${TEMPO_URL}/api/search?tags=service.name%3Dapi-gateway%20status%3Derror&start=${ONE_HOUR_AGO_S}&end=${NOW_S}&limit=50"

# ============================================================
# Scenario 9: wide time range (24h) all spans
# ============================================================
echo ""
echo "--- Wide Range ---"
run_scenario "wide_range_24h_all_spans" \
  "${LH_URL}/select/logsql/query?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${VT_URL}/select/logsql/query?query=*&start=${ONE_DAY_AGO_NS}&end=${NOW_NS}&limit=500" \
  "${TEMPO_URL}/api/search?start=${ONE_DAY_AGO_S}&end=${NOW_S}&limit=500"

# ============================================================
# Scenario 10: metadata - tag names
# ============================================================
echo ""
echo "--- Metadata ---"
run_scenario "metadata_tag_names" \
  "${LH_URL}/select/logsql/field_names?query=*&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}" \
  "${VT_URL}/select/logsql/field_names?query=*&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}" \
  "${TEMPO_URL}/api/search/tags"

# ============================================================
# Scenario 11: metadata - tag values for service.name
# ============================================================
run_scenario "metadata_tag_values_service" \
  "${LH_URL}/select/logsql/field_values?query=*&field=service.name&limit=100&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}" \
  "${VT_URL}/select/logsql/field_values?query=*&field=service.name&limit=100&start=${TWO_DAYS_AGO_NS}&end=${NOW_NS}" \
  "${TEMPO_URL}/api/search/tag/service.name/values"

echo ""
echo "=== Results ==="

# Assemble JSON output and print summary table
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
        'lakehouse_traces_url': '$LH_URL',
        'victoriatraces_url': '$VT_URL',
        'tempo_url': '$TEMPO_URL'
    },
    'scenarios': scenarios
}

with open('$OUTPUT', 'w') as f:
    json.dump(output, f, indent=2)

# Print summary table
print()
print(f'  {\"Scenario\":<45} {\"LH p95\":>10} {\"VT p95\":>10} {\"Tempo p95\":>10} {\"LH/VT\":>8} {\"LH/Tempo\":>10}')
print('  ' + '-' * 97)
for name in dict.fromkeys(r['name'] for r in results):
    s = scenarios.get(name, {})
    lh = s.get('lakehouse-traces', {}).get('p95_ms')
    vt = s.get('victoriatraces', {}).get('p95_ms')
    tempo = s.get('tempo', {}).get('p95_ms')
    lh_str = f'{lh}ms' if lh is not None else 'N/A'
    vt_str = f'{vt}ms' if vt is not None else 'N/A'
    tempo_str = f'{tempo}ms' if tempo is not None else 'N/A'
    ratio_vt = f'{lh/vt:.1f}x' if lh and vt else 'N/A'
    ratio_tempo = f'{lh/tempo:.1f}x' if lh and tempo else 'N/A'
    print(f'  {name:<45} {lh_str:>10} {vt_str:>10} {tempo_str:>10} {ratio_vt:>8} {ratio_tempo:>10}')

print()
print(f'Results saved to: $OUTPUT')
"
