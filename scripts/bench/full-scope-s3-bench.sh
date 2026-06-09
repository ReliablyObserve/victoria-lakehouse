#!/usr/bin/env bash
# Full-scope S3 / scan benchmark: every query class that drives S3 operations or
# column scans, compared across cold LH (Parquet/S3), hot VL (in-memory), and
# ClickHouse-over-S3 — so we can see WHERE LH is slow, what it lacks, and whether
# CH does pure-S3 operations better than we do.
#
# Run it under the scoped-latency wrapper so the toxic is always cleaned up:
#   scripts/bench/with-s3-latency.sh 100 30 -- scripts/bench/full-scope-s3-bench.sh [iters]
#   scripts/bench/with-s3-latency.sh 0   0  -- scripts/bench/full-scope-s3-bench.sh [iters]   # no added latency
#
# Output: CSV at $OUT and a markdown summary at $SUMMARY (p50/p95 per engine per
# scenario + LH/VL and LH/CH ratios + optimisation flags).
set -uo pipefail

ITERS=${1:-15}
WARMUP=${WARMUP:-3}
OUT=${OUT:-/tmp/full-scope-s3-bench.csv}
SUMMARY=${SUMMARY:-/tmp/full-scope-s3-bench.md}

LH=http://localhost:29428          # cold LH logs (host-exposed)
VL=http://localhost:19428          # hot VL (e2e-victorialogs, host-exposed)
CH_CONTAINER=${CH_CONTAINER:-victoria-lakehouse-clickhouse-1}

now_ns() { python3 -c "import time;print(int(time.time()*1e9))"; }
ago_ns()  { python3 -c "import time;print(int((time.time()-$1)*1e9))"; }
ms_now() { python3 -c "import time;print(time.time())"; }

have_ch=0
docker exec "$CH_CONTAINER" clickhouse-client --query "SELECT 1" >/dev/null 2>&1 && have_ch=1

echo "engine,scenario,run,ms" > "$OUT"
echo "full-scope S3/scan bench — iters=$ITERS (warmup=$WARMUP), CH=$have_ch, $(date -u +%FT%TZ)" >&2

# time_http <engine> <scenario> <url> : record wall-clock ms (only successful)
time_http() {
  local engine=$1 scenario=$2 url=$3 i t0 t1
  for ((i=0; i<WARMUP; i++)); do curl -s -m 120 "$url" >/dev/null 2>&1; done
  for ((i=0; i<ITERS; i++)); do
    t0=$(ms_now); curl -s -m 120 "$url" >/dev/null 2>&1; t1=$(ms_now)
    echo "$engine,$scenario,$i,$(python3 -c "print(f'{($t1-$t0)*1000:.1f}')")" >> "$OUT"
  done
}

# time_ch <scenario> <sql>
time_ch() {
  [ "$have_ch" = 1 ] || return 0
  local scenario=$1 sql=$2 i t0 t1
  for ((i=0; i<WARMUP; i++)); do docker exec "$CH_CONTAINER" clickhouse-client --query "$sql" >/dev/null 2>&1; done
  for ((i=0; i<ITERS; i++)); do
    t0=$(ms_now); docker exec "$CH_CONTAINER" clickhouse-client --query "$sql" >/dev/null 2>&1; t1=$(ms_now)
    echo "clickhouse,$scenario,$i,$(python3 -c "print(f'{($t1-$t0)*1000:.1f}')")" >> "$OUT"
  done
}

S24=$(ago_ns 86400); S1=$(ago_ns 3600); E=$(now_ns)
enc() { python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$1"; }

# ---- query classes (each drives a different S3/scan pattern) ----------------
declare -a SCN=(
  # name                         logsql query                                 needs
  "field_values_nolimit|/select/logsql/field_values?field=service.name&query=*"
  "field_values_limit100|/select/logsql/field_values?field=service.name&query=*&limit=100"
  "field_names|/select/logsql/field_names?query=*"
  "count_1h|/select/logsql/query?query=$(enc '* | stats count() c')"
  "count_24h|/select/logsql/query?query=$(enc '* | stats count() c')"
  "fulltext_scan_1h|/select/logsql/query?query=$(enc 'error | stats count() c')"
  "filtered_count_1h|/select/logsql/query?query=$(enc 'service.name:api-gateway | stats count() c')"
  "groupby_service_1h|/select/logsql/query?query=$(enc '* | stats by (service.name) count() c')"
)

for entry in "${SCN[@]}"; do
  scen=${entry%%|*}; path=${entry#*|}
  # 24h scenarios use the wide range; others 1h
  if [[ "$scen" == *_24h || "$scen" == field_* ]]; then SS=$S24; else SS=$S1; fi
  time_http lh_cold "$scen" "${LH}${path}&start=${SS}&end=${E}"
  time_http vl_hot  "$scen" "${VL}${path}&start=${SS}&end=${E}"
done

# ---- ClickHouse equivalents (pure S3-backed table) --------------------------
time_ch count_1h           "SELECT count() FROM lakehouse.otel_logs WHERE Timestamp > now() - INTERVAL 1 HOUR"
time_ch count_24h          "SELECT count() FROM lakehouse.otel_logs WHERE Timestamp > now() - INTERVAL 24 HOUR"
time_ch field_values_nolimit "SELECT DISTINCT ServiceName FROM lakehouse.otel_logs WHERE Timestamp > now() - INTERVAL 24 HOUR"
time_ch fulltext_scan_1h   "SELECT count() FROM lakehouse.otel_logs WHERE Timestamp > now() - INTERVAL 1 HOUR AND position(Body,'error')>0"
time_ch filtered_count_1h  "SELECT count() FROM lakehouse.otel_logs WHERE Timestamp > now() - INTERVAL 1 HOUR AND ServiceName='api-gateway'"
time_ch groupby_service_1h "SELECT ServiceName,count() FROM lakehouse.otel_logs WHERE Timestamp > now() - INTERVAL 1 HOUR GROUP BY ServiceName"

# ---- analysis ---------------------------------------------------------------
python3 - "$OUT" "$SUMMARY" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
by = {}
for r in rows:
    by.setdefault((r['scenario'], r['engine']), []).append(float(r['ms']))
def pct(v,p):
    v=sorted(v);
    return v[min(len(v)-1, int(round((p/100)*(len(v)-1))))]
scenarios = sorted({k[0] for k in by})
engines = ['lh_cold','vl_hot','clickhouse']
out = ["# Full-scope S3 / scan benchmark\n",
       "p50 ms per engine, and LH ratio vs VL / CH (>1 = LH slower).\n",
       "| scenario | LH p50 | VL p50 | CH p50 | LH/VL | LH/CH | flag |",
       "|---|--:|--:|--:|--:|--:|---|"]
for s in scenarios:
    def p50(e):
        v=by.get((s,e));
        return statistics.median(v) if v else None
    lh,vl,ch = p50('lh_cold'),p50('vl_hot'),p50('clickhouse')
    def f(x): return f"{x:.0f}" if x is not None else "—"
    rvl = f"{lh/vl:.1f}x" if lh and vl else "—"
    rch = f"{lh/ch:.1f}x" if lh and ch else "—"
    flag=""
    if lh and vl and lh/vl > 3: flag+="LH≫VL "
    if lh and ch and lh/ch > 2: flag+="CH-wins "
    out.append(f"| {s} | {f(lh)} | {f(vl)} | {f(ch)} | {rvl} | {rch} | {flag} |")
open(sys.argv[2],'w').write("\n".join(out)+"\n")
print("\n".join(out))
PY
echo "" >&2
echo "[full-scope] CSV: $OUT   summary: $SUMMARY" >&2
