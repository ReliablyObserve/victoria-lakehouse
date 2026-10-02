#!/usr/bin/env bash
# verify.sh: exact-row verification of every run ingested by run.sh, per
# instance. Prints one JSON line per instance with: runs, acked,
# returned_distinct, missing_acked, duplicates, unacked_present, non200.
# A healthy build has missing_acked = duplicates = unacked_present = non200 = 0.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
OUT=${OUT:-$here/out}
DB=${DURBENCH:-$here/bin/durbench}
declare -A PORT=( [logs-main]=39701 [logs-pr]=39702 [traces-main]=39703 [traces-pr]=39704 )
for sig in logs traces; do
  for build in main pr; do
    while IFS=$'\t' read -r s tenant run round; do
      [ "$s" = "$sig" ] || continue
      "$DB" -mode verify -signal "$sig" -url "http://127.0.0.1:${PORT[$sig-$build]}" \
        -run "$run" -account "$tenant" -acks "$OUT/acks/$build/$run.acks"
    done < "$OUT/runs.tsv" | SIG=$sig BUILD=$build python3 -c '
import json, os, sys
t = {"runs": 0, "acked": 0, "returned_distinct": 0, "missing_acked": 0, "duplicates": 0, "unacked_present": 0, "non200": 0}
for line in sys.stdin:
    r = json.loads(line)
    t["runs"] += 1
    for k in ("acked", "returned_distinct", "missing_acked", "duplicates", "unacked_present"):
        t[k] += r[k]
    t["non200"] += 1 if r["status"] != 200 else 0
print(json.dumps({"instance": os.environ["SIG"] + "-" + os.environ["BUILD"], **t}))
'
  done
done
