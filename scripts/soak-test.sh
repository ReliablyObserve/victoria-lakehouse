#!/usr/bin/env bash
# 30-minute soak test: continuous query mix against the running cold tier
# while toxiproxy injects realistic S3 latency. Tracks per-endpoint
# success/failure counts and latency percentiles so we can spot
# regressions under degraded S3.
#
# Usage: scripts/soak-test.sh [duration_seconds] [latency_ms] [jitter_ms]
#   defaults: 1800 (30min), 200, 50

set -uo pipefail

DUR=${1:-1800}
MEAN=${2:-200}
JITTER=${3:-50}

OUT=/tmp/soak-test-results.csv
SUMMARY=/tmp/soak-test-summary.txt

cold=http://localhost:20428
hot=http://localhost:10428

trap 'scripts/inject-s3-latency.sh clear >/dev/null 2>&1 || true' EXIT INT TERM

echo "=== injecting ${MEAN}ms ± ${JITTER}ms S3 latency ==="
/Users/slawomirskowron/claude_projects/victoria-lakehouse/scripts/inject-s3-latency.sh "$MEAN" "$JITTER" || exit 1

echo "ts,endpoint,status,latency_ms" > "$OUT"
start=$(date +%s)
deadline=$((start + DUR))

echo "=== soak running until $(date -r "$deadline" 2>/dev/null || date -d @"$deadline" 2>/dev/null) ($DUR s) ==="

NOW_S=0
hit() {
  local name=$1 url=$2 expect_field=$3
  local t0 t1
  t0=$(date +%s%N)
  body=$(curl -s -m 30 "$url" 2>&1)
  t1=$(date +%s%N)
  local ms=$(( (t1 - t0) / 1000000 ))
  local status="ok"
  if [[ -z "$body" ]]; then
    status="empty"
  elif [[ -n "$expect_field" ]] && ! echo "$body" | python3 -c "import sys,json; json.load(sys.stdin)" >/dev/null 2>&1; then
    status="parse_err"
  fi
  echo "$(date +%s),$name,$status,$ms" >> "$OUT"
}

i=0
while [[ $(date +%s) -lt $deadline ]]; do
  i=$((i+1))
  NOW_S=$(date +%s)
  W_START=$((NOW_S - 600))

  hit jaeger_services        "$cold/select/jaeger/api/services" data
  hit jaeger_traces          "$cold/select/jaeger/api/traces?service=api-gateway&limit=3" data
  hit jaeger_dependencies    "$cold/select/jaeger/api/dependencies?lookback=1800000" data
  hit tempo_search_tags      "$cold/select/tempo/api/v2/search/tags" scopes
  hit tempo_traceql          "$cold/select/tempo/api/search?q=%7B%7D&limit=3&start=${W_START}&end=${NOW_S}" traces
  hit logsql_overview        "$cold/lakehouse/api/v1/stats/overview" total_files
  hit logsql_tenants         "$cold/lakehouse/api/v1/tenants" tenants
  hit logsql_count_1h        "$cold/select/logsql/stats_query?query=_time%3A1h%20*%20%7C%20stats%20count%28%29%20as%20n" status

  # Pace so we don't hammer the server faster than realistic clients
  sleep 2
  if (( i % 50 == 0 )); then
    elapsed=$(( $(date +%s) - start ))
    echo "  [+${elapsed}s] $i iterations completed"
  fi
done

echo
echo "=== soak finished — analyzing $OUT ==="
python3 << EOF > "$SUMMARY"
import csv
from collections import defaultdict
import statistics

stats = defaultdict(lambda: {'ok': 0, 'empty': 0, 'parse_err': 0, 'latencies': []})
with open("$OUT") as f:
    r = csv.DictReader(f)
    for row in r:
        s = stats[row['endpoint']]
        s[row['status']] = s.get(row['status'], 0) + 1
        try:
            s['latencies'].append(int(row['latency_ms']))
        except ValueError:
            pass

print(f"{'endpoint':<22} {'total':>6} {'ok':>6} {'empty':>6} {'parse_err':>10} {'p50':>6} {'p99':>6} {'max':>6}")
for ep, s in sorted(stats.items()):
    lats = s.pop('latencies')
    total = sum(s.values())
    p50 = int(statistics.median(lats)) if lats else 0
    p99 = int(statistics.quantiles(lats, n=100)[98]) if len(lats) >= 100 else (max(lats) if lats else 0)
    mx = max(lats) if lats else 0
    print(f"{ep:<22} {total:>6} {s.get('ok', 0):>6} {s.get('empty', 0):>6} {s.get('parse_err', 0):>10} {p50:>6} {p99:>6} {mx:>6}")
EOF

echo
echo "=== final state ==="
docker ps --filter "name=victoria-lakehouse" --format "  {{.Names}}: {{.Status}}" | head -15

echo
cat "$SUMMARY"

echo
echo "results CSV: $OUT"
echo "summary:     $SUMMARY"
