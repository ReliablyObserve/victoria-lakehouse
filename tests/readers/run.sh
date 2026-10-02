#!/usr/bin/env bash
# External Parquet reader matrix: every engine the docs name reads real Lakehouse files.
#
#   run.sh fixture            build + start the stack (compose project lhreaders, ports 39400-39499),
#                             ingest logs and traces for three tenants, flush, record the oracle
#                             (raw), copy the raw objects aside, compact, record the oracle again
#                             (compacted), dump the Parquet objects to $OUT/fixture and tear down
#   run.sh engines <list>     start S3 (+ClickHouse/Trino as needed), restore the fixture, run the
#                             documented example of every listed engine, tear down
#   run.sh all                fixture, then every engine
#   run.sh down               tear the project down (volumes included); never prunes anything
#
# Environment: PYTHON (default python3), OUT (default tests/readers/.out), COMPOSE_PROJECT_NAME.
set -euo pipefail
cd "$(dirname "$0")"
HERE=$PWD
PY=${PYTHON:-python3}
OUT=${OUT:-$HERE/.out}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-lhreaders}
export PYTHONPATH=$HERE
P=$COMPOSE_PROJECT_NAME
DC=(docker compose -p "$P")
mkdir -p "$OUT"

log() { printf '\n== %s  [%s]\n' "$*" "$(date -u +%H:%M:%S)"; }

wait_scan() { # wait until $1 (container) logged a compaction scan that compacted something
  local c=$1
  for _ in $(seq 1 90); do
    if docker logs "$c" 2>&1 | grep -E 'scan completed; compactions=[1-9]' >/dev/null; then return 0; fi
    sleep 2
  done
  echo "no compaction scan in $c" >&2; docker logs "$c" 2>&1 | tail -20 >&2; return 1
}

fixture() {
  log "build and start the Lakehouse stack"
  "${DC[@]}" --profile datagen build
  "${DC[@]}" up -d --wait lakehouse-logs lakehouse-traces
  log "ingest (logs and traces, numeric / alias / >=2^31 tenants, two rounds)"
  ./ingest.sh
  "$PY" fixture.py wait-flushed
  log "oracle before compaction (raw layer)"
  "$PY" oracle.py record raw "$OUT/oracle-raw.json"
  "$PY" fixture.py snapshot-raw
  log "compact (one scan, then freeze)"
  LH_COMPACTION_INTERVAL=${LH_COMPACT_SCAN:-30s} "${DC[@]}" up -d --force-recreate --wait lakehouse-logs lakehouse-traces
  wait_scan "${P}-lakehouse-logs-1"
  wait_scan "${P}-lakehouse-traces-1"
  "${DC[@]}" up -d --force-recreate --wait lakehouse-logs lakehouse-traces   # default interval: no more scans
  "$PY" fixture.py verify-compacted
  log "oracle after compaction"
  "$PY" oracle.py record compacted "$OUT/oracle-compacted.json" --params "$OUT/oracle-raw.json"
  "$PY" oracle.py compare "$OUT/oracle-raw.json" "$OUT/oracle-compacted.json"
  "$PY" fixture.py make-prune-fixture "$OUT/oracle-compacted.json"
  "$PY" fixture.py inventory | tee "$OUT/inventory.txt"
  rm -rf "$OUT/fixture"
  "$PY" fixture.py dump "$OUT/fixture"
  log "tear down the Lakehouse stack"
  "${DC[@]}" down -v --remove-orphans
}

engines() {
  local list=$1 profiles=() svc=(s3)
  [[ ",$list," == *,clickhouse,* ]] && { profiles+=(--profile clickhouse); svc+=(clickhouse); }
  [[ ",$list," == *,trino,* ]] && { profiles+=(--profile trino); svc+=(trino); }
  log "start S3 ${svc[*]:2} and restore the fixture"
  "${DC[@]}" "${profiles[@]}" up -d --wait "${svc[@]}"
  "${DC[@]}" run --rm -T s3-init
  "$PY" fixture.py restore "$OUT/fixture"
  log "run: $list"
  local rc=0
  READERS_NET=${P}_net "$PY" matrix.py --engines "$list" --pruning \
    --oracle-raw "$OUT/oracle-raw.json" --oracle-compacted "$OUT/oracle-compacted.json" \
    --out "$OUT/result-${list//,/+}.json" || rc=$?
  "${DC[@]}" "${profiles[@]}" down -v --remove-orphans
  return $rc
}

case "${1:-}" in
  fixture) fixture ;;
  engines) engines "${2:?engine list}" ;;
  all)
    fixture
    rc=0
    for l in duckdb,pyarrow,pandas,polars,datafusion,parquet-tools clickhouse trino spark; do engines "$l" || rc=1; done
    "$PY" report.py "$OUT" > "$OUT/report.md" || rc=1
    cat "$OUT/report.md"
    exit $rc ;;
  down) "${DC[@]}" --profile clickhouse --profile trino --profile datagen down -v --remove-orphans ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac
