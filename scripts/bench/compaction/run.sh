#!/usr/bin/env bash
# run.sh: the ingest of the compaction A/B. Eleven rounds, both signals; every
# call writes 40 rows to BOTH the main and the PR instance, so the two stores
# hold identical data.
#   rounds 1-4:  tenants 1001 (72h late), 1002 (30h late), 1003 (2h late)
#   rounds 5-11: tenant 1003 only, 2h late   (a quiet tenant with a trickle)
# Records ingest start, acks and run ids under $OUT (default ./out).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$here/out}; mkdir -p "$OUT/acks/main" "$OUT/acks/pr"
DB=${DURBENCH:-$here/bin/durbench}
[ -x "$DB" ] || { mkdir -p "$here/bin"; (cd "$here" && GOWORK=off go build -o bin/durbench ./durbench); }
declare -A PORT=( [logs-main]=39701 [logs-pr]=39702 [traces-main]=39703 [traces-pr]=39704 )
declare -A LATE=( [1001]=72h [1002]=30h [1003]=2h )
date +%s > "$OUT/ingest_start"
: > "$OUT/runs.tsv"
for round in $(seq 1 11); do
  if [ "$round" -le 4 ]; then tenants="1001 1002 1003"; else tenants="1003"; fi
  for sig in logs traces; do
    for tenant in $tenants; do
      run="c343-r${round}-${sig}-${tenant}"
      for build in main pr; do
        "$DB" -mode ingest -signal "$sig" -url "http://127.0.0.1:${PORT[$sig-$build]}" \
          -run "$run" -total 40 -batch 40 -conc 1 -late "${LATE[$tenant]}" -account "$tenant" \
          -acks "$OUT/acks/$build/$run.acks" >> "$OUT/ingest.jsonl"
      done
      printf '%s\t%s\t%s\t%s\n' "$sig" "$tenant" "$run" "$round" >> "$OUT/runs.tsv"
    done
  done
  sleep 7
done
echo "ingest done; start=$(cat "$OUT/ingest_start"); run ids in $OUT/runs.tsv"
