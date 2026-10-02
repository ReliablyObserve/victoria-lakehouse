#!/usr/bin/env bash
# Ingest the reader-matrix fixture into the running lhreaders stack: logs AND traces,
# for every tenant form, in two rounds so each partition ends up with several small
# files (what compaction later merges).
#
#   numeric   AccountID 4401  ProjectID 1        (header form)
#   alias     X-Scope-OrgID: acme-corp -> 1001:0 (string form)
#   big       AccountID 3000000000 ProjectID 0   (>= 2^31: the unsigned 32-bit column)
#   golden    AccountID 4294967294 ProjectID 0   (hand-written edge cases, golden.py)
#   bloom     AccountID 4402 ProjectID 3         (60 rows in one hour per round: 96-byte bloom filters, golden.py)
#
# Every batch writes its writer-side manifest (counts, services, errors, map keys, exact
# nanosecond timestamps, spans per trace) to $MANIFEST_DIR before the rows are sent; the
# generated tenants are seeded, so the same command produces the same rows.
#
# Rounds are separated by one flush interval so every round lands in its own file.
set -euo pipefail
cd "$(dirname "$0")"
P=${COMPOSE_PROJECT_NAME:-lhreaders}
PY=${PYTHON:-python3}
FLUSH_WAIT=${FLUSH_WAIT:-20}
OUT=${OUT:-$PWD/.out}
export MANIFEST_DIR=${MANIFEST_DIR:-$OUT/manifest}
rm -rf "$MANIFEST_DIR"; mkdir -p "$MANIFEST_DIR"; chmod 777 "$MANIFEST_DIR"   # the datagen image runs as a non-root user
ENDPOINTS=(--lh-logs-endpoint=http://lakehouse-logs:9428 --lh-traces-endpoint=http://lakehouse-traces:10428)

# gen <round> <name> <seed> <datagen args...>
gen() {
  local round=$1 name=$2 seed=$3; shift 3
  docker compose -p "$P" --profile datagen run --rm -T datagen "$@" "${ENDPOINTS[@]}" \
    --seed="$seed" --manifest="/manifest/$name-r$round.json" --manifest-name="$name" 2>&1 | tail -1
}

for round in 1 2; do
  echo "== round $round"
  gen $round numeric $((4401000 + round)) --logs=1500 --traces=300 --hours-back=48 --account-id=4401 --project-id=1
  gen $round big $((3000000 + round)) --logs=300 --traces=60 --hours-back=48 --account-id=3000000000 --project-id=0
  gen $round alias $((1001000 + round)) --logs=300 --traces=60 --hours-back=48 --org-id=acme-corp
  "$PY" golden.py send $round "$MANIFEST_DIR"
  [ "$round" = 1 ] && sleep "$FLUSH_WAIT"
done
echo "ingest done"
