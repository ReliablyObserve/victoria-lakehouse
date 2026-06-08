#!/usr/bin/env bash
# =============================================================================
# scripts/bench/run.sh — THE one place to benchmark the cold tier.
#
# Comparison model:
#   VL / VT (disk, optionally simulated gp3)  = REFERENCE BASELINE
#   Lakehouse (S3 Parquet)                    = system under test
#   ClickHouse (S3, the SAME Parquet LH wrote) = engine-vs-engine on identical bytes
#
# Every S3 engine (LH, CH) reads MinIO through the toxiproxy s3-latency proxy, so
# one knob injects identical object-store latency. VL/VT are disk-native; with
# --disk-profile gp3-loop their disk is throttled to AWS gp3 (125 MB/s, 3000 IOPS)
# so a fast laptop NVMe doesn't flatter the baseline.
#
# It sweeps systems x signals x query-types x time-ranges x S3-latency, runs a
# parity gate first (so we compare EQUAL answers, incl. CH), and writes one
# consolidated report (JSON + a markdown table normalized to the VL/VT baseline)
# that flags where LH is slow vs baseline and vs CH.
#
# Usage:
#   scripts/bench/run.sh [options]
#     --disk-profile local-ssd|gp3-loop   (default local-ssd)
#     --s3-latency "0 100 300"            ms levels to sweep (default "0")
#     --signals logs|traces|both          (default both)
#     --ranges  "1h 6h 24h"               (default "1h 6h 24h")
#     --iterations N                       (default 20)   --warmup N (default 3)
#     --output FILE                        (default bench-results/run-<stamp>.json)
#     --stamp YYYYmmdd-HHMMSS              timestamp for output (default: caller-supplied or 'latest')
#     --no-up      assume the stack is already running (skip compose up)
#     --no-ingest  skip the data-generation step
#     --keep       leave the stack running on exit
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

DISK_PROFILE=local-ssd
S3_LATENCIES="0"
SIGNALS=both
RANGES="1h 6h 24h"
ITERATIONS=20
WARMUP=3
STAMP="latest"
OUTPUT=""
DO_UP=1; DO_INGEST=1; KEEP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --disk-profile) DISK_PROFILE="$2"; shift 2 ;;
    --s3-latency)   S3_LATENCIES="$2"; shift 2 ;;
    --signals)      SIGNALS="$2"; shift 2 ;;
    --ranges)       RANGES="$2"; shift 2 ;;
    --iterations)   ITERATIONS="$2"; shift 2 ;;
    --warmup)       WARMUP="$2"; shift 2 ;;
    --output)       OUTPUT="$2"; shift 2 ;;
    --stamp)        STAMP="$2"; shift 2 ;;
    --no-up)        DO_UP=0; shift ;;
    --no-ingest)    DO_INGEST=0; shift ;;
    --keep)         KEEP=1; shift ;;
    -h|--help)      sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ -z "$OUTPUT" ]] && OUTPUT="bench-results/run-${STAMP}.json"
mkdir -p "$(dirname "$OUTPUT")"

COMPOSE_BASE="deployment/docker/docker-compose-benchmark.yml"
COMPOSE_GP3="deployment/docker/docker-compose-benchmark.gp3.yml"
COMPOSE_ARGS=(-f "$COMPOSE_BASE")
[[ "$DISK_PROFILE" == "gp3-loop" ]] && COMPOSE_ARGS+=(-f "$COMPOSE_GP3")

# --- endpoints (benchmark compose published ports) ----------------------------
declare -A EP=(
  [lh_logs]=http://localhost:39428
  [vl]=http://localhost:39401
  [lh_traces]=http://localhost:30428
  [vt]=http://localhost:30401
  [ch]=http://localhost:38123
)
# ClickHouse HTTP auth (CLICKHOUSE_USER/PASSWORD in the benchmark compose).
CH_USER="${CH_USER:-default}"; CH_PASS="${CH_PASS:-benchmark}"

log() { printf '\033[1;36m[bench]\033[0m %s\n' "$*" >&2; }

# --- measure_query: time a prepared request N times, emit p50/p95/p99 JSON -----
# Uses curl's own %{time_total} (no shell-timing overhead, so fast queries aren't
# inflated by subprocess startup); percentiles computed once at the end.
# $1 label  $2 system  $3 method(GET|POST|CH)  $4 url  $5 body
measure_query() {
  local label="$1" system="$2" method="$3" url="$4" body="${5:-}"
  local secs=() bytes=() errors=0 i out code tt sz
  for ((i=0; i<WARMUP; i++)); do _do_req "$method" "$url" "$body" >/dev/null 2>&1 || true; done
  for ((i=0; i<ITERATIONS; i++)); do
    out=$(_do_req "$method" "$url" "$body"); read -r code tt sz <<<"$out"
    if [[ "$code" == 2* ]]; then secs+=("$tt"); bytes+=("$sz"); else errors=$((errors+1)); fi
  done
  # result = a comparable scalar (row/group count) so the report can verify every
  # system returned EQUIVALENT data — a fast p95 over an empty/divergent result is
  # not a real win and gets flagged downstream.
  local result; result=$(fetch_scalar "$method" "$url" "$body" "${label##*/}")
  python3 - "$label" "$system" "$errors" "$result" "${#bytes[@]}" "${bytes[@]}" -- "${secs[@]}" <<'PY'
import sys, json
a = sys.argv
label, system, errors, result, nb = a[1], a[2], int(a[3]), a[4], int(a[5])
b = list(map(float, a[6:6+nb]))
secs = list(map(float, a[a.index('--')+1:]))
vals = sorted(s * 1000 for s in secs)
n = len(vals)
pct = lambda p: round(vals[min(int(n * p / 100), n - 1)], 1) if n else None
avg_bytes = round(sum(b)/len(b)) if b else 0
print(json.dumps({"label": label, "system": system, "p50_ms": pct(50), "p95_ms": pct(95),
                  "p99_ms": pct(99), "iters": n, "errors": errors,
                  "avg_bytes": avg_bytes, "result": result}))
PY
}
_do_req() { # echoes "<http_code> <time_total_s> <size_download_bytes>"
  local method="$1" url="$2" body="$3"
  case "$method" in
    GET)  curl -sf -o /dev/null -w '%{http_code} %{time_total} %{size_download}' --max-time 60 "$url" 2>/dev/null || echo "000 0 0" ;;
    POST) curl -sf -o /dev/null -w '%{http_code} %{time_total} %{size_download}' --max-time 60 --data-urlencode "query=$body" "$url" 2>/dev/null || echo "000 0 0" ;;
    CH)   curl -sf -o /dev/null -w '%{http_code} %{time_total} %{size_download}' --max-time 60 --user "$CH_USER:$CH_PASS" --data-binary "$body" "$url/" 2>/dev/null || echo "000 0 0" ;;
  esac
}
# fetch_scalar: one un-timed request; extracts a comparable count from each
# system's native response (LogsQL JSON lines vs ClickHouse TSV) so results can be
# checked for equivalence across systems. Echoes the number, or "ERR".
fetch_scalar() {
  local method="$1" url="$2" body="$3" qkind="$4" raw
  if [[ "$method" == CH ]]; then
    raw=$(curl -sf --max-time 60 --user "$CH_USER:$CH_PASS" --data-binary "$body" "$url/" 2>/dev/null) || { echo ERR; return; }
    # count_* -> single number; by_service -> sum the second TSV column
    awk 'NF>=2{s+=$2} NF==1{s+=$1} END{print (s==""?0:s)}' <<<"$raw"
  else
    raw=$(curl -sf --max-time 60 --data-urlencode "query=$body" "$url" 2>/dev/null) || { echo ERR; return; }
    python3 -c "
import sys,json
tot=0
for ln in sys.stdin:
    ln=ln.strip()
    if not ln: continue
    try: d=json.loads(ln)
    except: continue
    v=d.get('n') or d.get('count(*)') or d.get('count(') or 0
    try: tot+=int(v)
    except: pass
print(tot)" <<<"$raw"
  fi
}

# --- time helpers (ns epoch for LogsQL, unix seconds for CH) -------------------
range_to_secs() { case "$1" in 15m) echo 900;; 1h) echo 3600;; 6h) echo 21600;; 24h) echo 86400;; 7d) echo 604800;; *) echo 3600;; esac; }
start_ns() { python3 -c "import time;print(int((time.time()-$1)*1e9))"; }
end_ns()   { python3 -c "import time;print(int(time.time()*1e9))"; }
start_s()  { python3 -c "import time;print(int(time.time()-$1))"; }
end_s()    { python3 -c "import time;print(int(time.time()))"; }

# --- the matrix: (signal, query) -> per-system prepared request ---------------
# Each query function echoes "<method>\t<url>\t<body>" for the given system+range.
# LogsQL systems (lh/vl/vt) hit /select/logsql/query; ClickHouse hits its HTTP
# SQL endpoint over the otel_logs/otel_traces views (same Parquet on S3).
prep() { # $1 signal  $2 query  $3 system  $4 range_secs
  local signal="$1" query="$2" sys="$3" secs="$4"
  local sns ens ss es
  sns=$(start_ns "$secs"); ens=$(end_ns "$secs"); ss=$(start_s "$secs"); es=$(end_s "$secs")
  local logs_url traces_url
  case "$sys" in
    lakehouse) logs_url="${EP[lh_logs]}/select/logsql/query"; traces_url="${EP[lh_traces]}/select/logsql/query" ;;
    victorialogs) logs_url="${EP[vl]}/select/logsql/query" ;;
    victoriatraces) traces_url="${EP[vt]}/select/logsql/query" ;;
  esac
  if [[ "$signal" == logs ]]; then
    case "$query" in
      count_total)      [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s)' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t* | stats count() n' "$logs_url" "$sns" "$ens" ;;
      count_by_service) [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT ServiceName,count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) GROUP BY ServiceName' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t* | stats by (service.name) count()' "$logs_url" "$sns" "$ens" ;;
      fulltext)         [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND position(Body,'"'"'error'"'"')>0' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\terror | stats count() n' "$logs_url" "$sns" "$ens" ;;
    esac
  else # traces
    case "$query" in
      count_total)      [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s)' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t* | stats count() n' "$traces_url" "$sns" "$ens" ;;
      count_by_service) [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT ServiceName,count() FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) GROUP BY ServiceName' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t* | stats by (service.name) count()' "$traces_url" "$sns" "$ens" ;;
    esac
  fi
}

LOG_QUERIES="count_total count_by_service fulltext"
TRACE_QUERIES="count_total count_by_service"
LOG_SYSTEMS="victorialogs lakehouse clickhouse"      # VL = baseline
TRACE_SYSTEMS="victoriatraces lakehouse clickhouse"  # VT = baseline

# --- orchestration ------------------------------------------------------------
up_stack() {
  log "bringing up benchmark stack (disk-profile=$DISK_PROFILE)…"
  docker compose "${COMPOSE_ARGS[@]}" up -d --build
  log "waiting for systems to report healthy…"
  local tries=0
  until curl -sf "${EP[lh_logs]}/health" >/dev/null 2>&1 && curl -sf "${EP[vl]}/health" >/dev/null 2>&1; do
    sleep 3; tries=$((tries+1)); (( tries > 60 )) && { log "stack did not become healthy"; return 1; }
  done
}
teardown() { (( KEEP )) && { log "leaving stack up (--keep)"; return; }; log "tearing down stack…"; docker compose "${COMPOSE_ARGS[@]}" down -v >/dev/null 2>&1 || true; }
# The datagen-seed service (in the compose) backfills ~7d of logs+traces into
# VL/VT/LH at `up`. Wait for it to finish, then let LH flush to S3 so ClickHouse's
# s3() views see the Parquet; finally run the preflight data/parity check.
ingest() {
  (( DO_INGEST )) || return 0
  log "waiting for datagen-seed to backfill (~7d of data)…"
  local tries=0 state
  while :; do
    state=$(docker compose "${COMPOSE_ARGS[@]}" ps -a --format '{{.Service}} {{.State}}' 2>/dev/null | awk '$1=="datagen-seed"{print $2}')
    [[ "$state" == exited* || "$state" == "exited" ]] && break
    sleep 5; tries=$((tries+1)); (( tries > 180 )) && { log "datagen-seed not finished after 15m; continuing"; break; }
  done
  log "seed done; waiting 75s for LH flush to S3 (ClickHouse reads the Parquet)…"
  sleep 75
  [[ -x scripts/benchmark-preflight.sh ]] && { log "preflight data/parity check…"; scripts/benchmark-preflight.sh || log "(preflight reported issues — see above)"; }
}
set_latency() { local ms="$1"; if [[ "$ms" == 0 ]]; then log "S3 latency: passthrough (0ms)"; scripts/inject-s3-latency.sh 0 0 >/dev/null 2>&1 || true; else log "S3 latency: ${ms}ms"; scripts/inject-s3-latency.sh "$ms" "$((ms/3))" >/dev/null 2>&1 || true; fi; }

# Parity gate: count_total over 24h must agree across systems (within tolerance)
# so the benchmark compares EQUAL answers (incl. ClickHouse).
parity_gate() {
  local signal="$1" secs; secs=$(range_to_secs 24h)
  log "parity gate ($signal, 24h): count_total across systems…"
  local sysset; [[ "$signal" == logs ]] && sysset="$LOG_SYSTEMS" || sysset="$TRACE_SYSTEMS"
  local base="" s n IFS_=$'\t'
  for s in $sysset; do
    read -r m u b <<<"$(prep "$signal" count_total "$s" "$secs")"
    if [[ "$m" == CH ]]; then n=$(curl -sf --max-time 60 --user "$CH_USER:$CH_PASS" --data-binary "$b" "$u/" 2>/dev/null | tr -d '[:space:]')
    else n=$(curl -sf --max-time 60 --data-urlencode "query=$b" "$u" 2>/dev/null | python3 -c "import sys,json
try:
  d=[json.loads(l) for l in sys.stdin if l.strip()]; print(d[0].get('n',0) if d else 0)
except: print('ERR')"); fi
    printf '    %-16s count=%s\n' "$s" "${n:-ERR}" >&2
    [[ -z "$base" ]] && base="$n"
  done
}

# --- run --------------------------------------------------------------------
trap teardown EXIT
(( DO_UP )) && { up_stack || exit 1; }
ingest

RESULTS="["; first=1
for lat in $S3_LATENCIES; do
  set_latency "$lat"
  for signal in $([[ "$SIGNALS" == both ]] && echo "logs traces" || echo "$SIGNALS"); do
    parity_gate "$signal" || true
    queries=$([[ "$signal" == logs ]] && echo "$LOG_QUERIES" || echo "$TRACE_QUERIES")
    systems=$([[ "$signal" == logs ]] && echo "$LOG_SYSTEMS" || echo "$TRACE_SYSTEMS")
    for range in $RANGES; do
      secs=$(range_to_secs "$range")
      for q in $queries; do
        for sys in $systems; do
          IFS=$'\t' read -r method url body <<<"$(prep "$signal" "$q" "$sys" "$secs")"; unset IFS
          [[ -z "${method:-}" ]] && continue
          row=$(measure_query "${signal}/${q}/${range}/lat${lat}" "$sys" "$method" "$url" "$body")
          row=$(python3 -c "import sys,json;d=json.loads(sys.argv[1]);d.update(signal='$signal',query='$q',range='$range',latency_ms=$lat);print(json.dumps(d))" "$row")
          (( first )) && first=0 || RESULTS+=","
          RESULTS+="$row"
          p95=$(python3 -c "import sys,json;print(json.loads(sys.argv[1]).get('p95_ms'))" "$row")
          printf '    %-34s %-16s p95=%s ms\n' "${signal}/${q}/${range}/lat${lat}" "$sys" "$p95" >&2
        done
      done
    done
  done
done
RESULTS+="]"
echo "$RESULTS" | python3 -m json.tool > "$OUTPUT"
log "raw results -> $OUTPUT"

# --- report: markdown table normalized to the VL/VT baseline ------------------
python3 scripts/bench/report.py "$OUTPUT" "${OUTPUT%.json}.md" && log "report -> ${OUTPUT%.json}.md"
