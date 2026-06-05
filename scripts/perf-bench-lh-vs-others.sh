#!/usr/bin/env bash
# Comparative perf bench: same trace + log query against LH cold,
# VT hot, VL hot, and ClickHouse against the running e2e compose.
# Measures wall-clock per-query and reports p50/p95/p99.
#
# Usage: scripts/perf-bench-lh-vs-others.sh [iterations]
#   default iterations = 30 (after 5 warmup)
#
# Requires the e2e compose to be up. Internal services (VL, VT, CH)
# are reached via `docker exec grafana-1 curl ...` since they're not
# host-exposed on those ports in the e2e compose.

set -uo pipefail

ITERATIONS=${1:-30}
WARMUP=5

OUT=/tmp/perf-bench-results.csv
echo "engine,scenario,run,latency_ms" > "$OUT"

# Engines + how to reach them. Each value is a curl-able URL or a
# 'docker exec NAME curl' prefix.
declare -A HOTREACH=(
  ["lh_cold_traces"]="curl -s -m 60"
  ["vt_hot"]="docker exec victoria-lakehouse-grafana-1 curl -s -m 60"
  ["lh_cold_logs"]="curl -s -m 60"
  ["vl_hot"]="docker exec victoria-lakehouse-grafana-1 curl -s -m 60"
  ["clickhouse"]="docker exec victoria-lakehouse-clickhouse-1 clickhouse-client"
)

declare -A BASE=(
  ["lh_cold_traces"]="http://localhost:20428"
  ["vt_hot"]="http://victoriatraces:10428"
  ["lh_cold_logs"]="http://localhost:29428"
  ["vl_hot"]="http://victorialogs:9428"
)

run_curl() {
  local engine=$1 url=$2 extract_field=$3
  local body t0 t1
  t0=$(date +%s%N)
  body=$(${HOTREACH[$engine]} "$url" 2>&1)
  t1=$(date +%s%N)
  local ms=$(( (t1 - t0) / 1000000 ))
  if [[ -z "$body" ]] || echo "$body" | grep -q '<title>\|HTTP/'; then
    return 1
  fi
  echo "$ms"
}

run_ch() {
  local query=$1
  local t0 t1
  t0=$(date +%s%N)
  ${HOTREACH[clickhouse]} --query "$query" >/dev/null 2>&1
  t1=$(date +%s%N)
  echo $(( ($(date +%s%N) - t0) / 1000000 ))
}

# Scenarios:
# Trace search: LH cold + VT hot
trace_search() {
  local engine=$1 i=$2
  local url="${BASE[$engine]}/select/jaeger/api/traces?service=api-gateway&limit=10"
  ms=$(run_curl "$engine" "$url" data) && echo "$engine,trace_search,$i,$ms" >> "$OUT"
}

# Dependencies aggregate: LH cold + VT hot
deps() {
  local engine=$1 i=$2
  local url="${BASE[$engine]}/select/jaeger/api/dependencies?lookback=3600000"
  ms=$(run_curl "$engine" "$url" total) && echo "$engine,deps,$i,$ms" >> "$OUT"
}

# Log count over 1h: LH cold-logs + VL hot
log_count() {
  local engine=$1 i=$2
  local url="${BASE[$engine]}/select/logsql/stats_query?query=_time%3A1h%20*%20%7C%20stats%20count%28%29%20as%20n"
  ms=$(run_curl "$engine" "$url" data) && echo "$engine,log_count_1h,$i,$ms" >> "$OUT"
}

# Span count over 1h: LH cold-traces + VT hot
span_count() {
  local engine=$1 i=$2
  local url="${BASE[$engine]}/select/logsql/stats_query?query=_time%3A1h%20*%20%7C%20stats%20count%28%29%20as%20n"
  ms=$(run_curl "$engine" "$url" data) && echo "$engine,span_count_1h,$i,$ms" >> "$OUT"
}

# ClickHouse equivalents over the same S3 parquet. The view names
# come from deployment/docker/clickhouse/init-s3.sql which mirrors the
# upstream OpenTelemetry ClickHouse exporter schema.
ch_log_count() {
  local i=$1
  ms=$(run_ch "SELECT count() FROM lakehouse.otel_logs WHERE Timestamp > now() - INTERVAL 1 HOUR")
  echo "clickhouse,log_count_1h,$i,$ms" >> "$OUT"
}
ch_span_count() {
  local i=$1
  ms=$(run_ch "SELECT count() FROM lakehouse.otel_traces WHERE Timestamp > now() - INTERVAL 1 HOUR")
  echo "clickhouse,span_count_1h,$i,$ms" >> "$OUT"
}
ch_trace_search() {
  local i=$1
  ms=$(run_ch "SELECT count(DISTINCT TraceId) FROM lakehouse.otel_traces WHERE ServiceName = 'api-gateway' AND Timestamp > now() - INTERVAL 1 HOUR")
  echo "clickhouse,trace_search,$i,$ms" >> "$OUT"
}

echo "=== ensuring no S3 latency injection ==="
/Users/slawomirskowron/claude_projects/victoria-lakehouse/scripts/inject-s3-latency.sh clear >/dev/null 2>&1 || true

# Wait for the recent-data settling window before measuring. VT's
# Jaeger search excludes traces newer than 30s (LatencyOffset), so a
# bench started immediately after pod restart sees empty results.
# 60s gives a safe margin.
echo "=== warm-wait 60s for data to settle past LatencyOffset cutoff ==="
sleep 60

echo "=== warmup ${WARMUP} iterations ==="
for w in $(seq 1 $WARMUP); do
  trace_search lh_cold_traces 0 >/dev/null
  trace_search vt_hot 0 >/dev/null
  log_count lh_cold_logs 0 >/dev/null
  log_count vl_hot 0 >/dev/null
  ch_log_count 0
done

# Validate each engine returns real data before measuring (no fast
# error responses pretending to be fast queries).
echo "=== sanity check: each engine returns non-zero results ==="
for engine_name in "LH cold-traces" "VT hot" "ClickHouse otel_traces"; do
  case "$engine_name" in
    "LH cold-traces")  url="${BASE[lh_cold_traces]}/select/jaeger/api/traces?service=api-gateway&limit=1"
                       resp=$(${HOTREACH[lh_cold_traces]} "$url")
                       count=$(echo "$resp" | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("data",[])))') ;;
    "VT hot")          url="${BASE[vt_hot]}/select/jaeger/api/traces?service=api-gateway&limit=1"
                       resp=$(${HOTREACH[vt_hot]} "$url")
                       count=$(echo "$resp" | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("data",[])))') ;;
    "ClickHouse otel_traces")
                       count=$(${HOTREACH[clickhouse]} --query "SELECT count() FROM lakehouse.otel_traces WHERE Timestamp > now() - INTERVAL 1 HOUR" 2>&1) ;;
  esac
  echo "  $engine_name: $count"
  if [[ -z "$count" || "$count" == "0" || "$count" == *"Exception"* ]]; then
    echo "  WARNING: $engine_name returning no data — bench numbers will be misleading"
  fi
done

echo "=== running $ITERATIONS measured iterations ==="
for i in $(seq 1 $ITERATIONS); do
  trace_search lh_cold_traces "$i"
  trace_search vt_hot "$i"
  deps lh_cold_traces "$i"
  deps vt_hot "$i"
  span_count lh_cold_traces "$i"
  span_count vt_hot "$i"
  log_count lh_cold_logs "$i"
  log_count vl_hot "$i"
  ch_log_count "$i"
  ch_span_count "$i"
  ch_trace_search "$i"
  if (( i % 5 == 0 )); then
    echo "  [$i/$ITERATIONS]"
  fi
done

echo
echo "=== summary ==="
python3 << 'EOF'
import csv
from collections import defaultdict
import statistics

stats = defaultdict(list)
with open("/tmp/perf-bench-results.csv") as f:
    r = csv.DictReader(f)
    for row in r:
        try:
            stats[(row['engine'], row['scenario'])].append(int(row['latency_ms']))
        except ValueError:
            pass

scenarios = sorted({k[1] for k in stats.keys()})
engines = sorted({k[0] for k in stats.keys()})
print(f"\n{'scenario':<20} {'engine':<20} {'n':>4} {'p50':>6} {'p95':>6} {'p99':>6} {'max':>6}")
for scn in scenarios:
    for eng in engines:
        lats = stats.get((eng, scn), [])
        if not lats:
            continue
        n = len(lats)
        p50 = int(statistics.median(lats))
        p95 = int(statistics.quantiles(lats, n=100)[94]) if n >= 20 else max(lats)
        p99 = int(statistics.quantiles(lats, n=100)[98]) if n >= 100 else max(lats)
        mx = max(lats)
        print(f"{scn:<20} {eng:<20} {n:>4} {p50:>6} {p95:>6} {p99:>6} {mx:>6}")

print("\nrelative comparison (cold-tier baseline → others):")
for scn in scenarios:
    cold_engine = "lh_cold_traces" if "trace" in scn or "span" in scn or "deps" in scn else "lh_cold_logs"
    if (cold_engine, scn) not in stats:
        continue
    cold_lats = stats[(cold_engine, scn)]
    cold_p50 = statistics.median(cold_lats) if cold_lats else 1
    for eng in engines:
        if eng == cold_engine:
            continue
        if (eng, scn) not in stats:
            continue
        lats = stats[(eng, scn)]
        p50 = statistics.median(lats)
        ratio = p50 / cold_p50 if cold_p50 > 0 else 0
        print(f"  {scn} {eng}: {p50:.0f}ms vs cold {cold_p50:.0f}ms → {ratio:.2f}×")
EOF
echo
echo "results CSV: $OUT"
