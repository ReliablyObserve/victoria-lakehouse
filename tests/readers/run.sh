#!/usr/bin/env bash
# External Parquet reader matrix: every engine the docs name reads real Lakehouse files.
#
#   run.sh fixture            build + start the stack (compose project lhreaders, ports 39400-39499),
#                             ingest logs and traces for five tenants with writer-side manifests, flush,
#                             record the oracle (raw) and compare it and the files with the truth, copy the
#                             bucket aside (raw), compact until every group is compacted (later scans must do nothing), record the oracle
#                             again (compacted), dump every object of both buckets to $OUT/fixture, tear down
#   run.sh engines <list>     start S3 (+ClickHouse/Trino as needed), restore the fixture, run the
#                             documented example of every listed engine, tear down
#   run.sh all                fixture, then every engine
#   run.sh down               tear the project down (volumes included); never prunes anything
#
# Environment: PYTHON (default python3), OUT (default tests/readers/.out), COMPOSE_PROJECT_NAME,
# FIXTURE_CONTINUE (mutation-proof.sh: keep going after a failed truth check), SECOND_SCAN_WAIT (seconds).
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

scans_with_compactions() { docker logs "$1" 2>&1 | grep -cE 'scan completed; compactions=[1-9]' || true; }

SOFT_FAIL=
# check <cmd...>: a correctness check against the writer-side truth. It stops the run, unless
# FIXTURE_CONTINUE is set (mutation-proof.sh): then the failure is remembered, the run goes on to the
# engines so they can be shown to catch the same defect, and the fixture still exits non-zero at the end.
check() {
  "$@" || {
    [ -n "${FIXTURE_CONTINUE:-}" ] || return 1
    echo "RED (continuing): $*" >&2
    SOFT_FAIL=1
  }
}

fixture() {
  log "build and start the Lakehouse stack"
  # CI builds the three images itself with a layer cache (SKIP_BUILD=1) and loads them under the compose names.
  [ -n "${SKIP_BUILD:-}" ] || "${DC[@]}" --profile datagen build
  "${DC[@]}" up -d --wait lakehouse-logs lakehouse-traces
  log "ingest (logs and traces: numeric, alias, >=2^31, golden and bloom tenants, two rounds, manifests written first)"
  OUT=$OUT ./ingest.sh
  "$PY" fixture.py wait-flushed
  log "writer-side truth: expected answers of every check, from the manifests"
  "$PY" truth.py build "$OUT/manifest" "$OUT/manifest.json" "$OUT/truth.json"
  "$PY" fixture.py snapshot-raw
  log "oracle before compaction (raw layer); Lakehouse and the raw files are compared with the truth"
  "$PY" oracle.py record raw "$OUT/oracle-raw.json" --params "$OUT/truth.json"
  check "$PY" truth.py compare-oracle "$OUT/truth.json" "$OUT/oracle-raw.json"
  check "$PY" truth.py verify-files "$OUT/manifest.json" raw
  log "compact (until every group is compacted, then later scans that must do nothing, then freeze)"
  LH_COMPACTION_INTERVAL=${LH_COMPACT_SCAN:-30s} "${DC[@]}" up -d --force-recreate --wait lakehouse-logs lakehouse-traces
  wait_scan "${P}-lakehouse-logs-1"
  wait_scan "${P}-lakehouse-traces-1"
  # Converge: small partitions become eligible when their files are old enough (min_age), so the first productive
  # scan does not always take everything. Wait until every tenant and signal has compacted objects and the rows
  # still equal Lakehouse's count. A planner that never stops rewriting (#343) never converges and fails here.
  converged=
  for _ in $(seq 1 ${CONVERGE_TRIES:-45}); do
    if "$PY" fixture.py verify-compacted >/dev/null 2>&1; then converged=1; break; fi
    sleep 10
  done
  [ -n "$converged" ] || { "$PY" fixture.py verify-compacted; echo "compaction did not converge" >&2; return 1; }
  "$PY" fixture.py snapshot-archive "$OUT/archive-scan1.json"
  L0=$(scans_with_compactions "${P}-lakehouse-logs-1"); T0=$(scans_with_compactions "${P}-lakehouse-traces-1")
  # Two more intervals after convergence: a compaction planner that keeps rewriting what it just wrote (#343) would show here.
  sleep ${SECOND_SCAN_WAIT:-80}
  L1=$(scans_with_compactions "${P}-lakehouse-logs-1"); T1=$(scans_with_compactions "${P}-lakehouse-traces-1")
  check "$PY" fixture.py storage-health "$OUT/archive-scan1.json" $((L1 - L0)) $((T1 - T0)) "$OUT/storage-health.json"
  "${DC[@]}" up -d --force-recreate --wait lakehouse-logs lakehouse-traces   # default interval: no more scans
  "$PY" fixture.py verify-compacted
  check "$PY" fixture.py verify-layers
  log "oracle after compaction"
  "$PY" oracle.py record compacted "$OUT/oracle-compacted.json" --params "$OUT/truth.json"
  check "$PY" truth.py compare-oracle "$OUT/truth.json" "$OUT/oracle-compacted.json"
  check "$PY" truth.py verify-files "$OUT/manifest.json" compacted
  check "$PY" oracle.py compare "$OUT/oracle-raw.json" "$OUT/oracle-compacted.json"
  # The pruning fixture (one real day of the compacted layer plus a garbage partition) exists before the files are read for facts.
  "$PY" fixture.py make-prune-fixture "$OUT/truth.json"
  log "what the files say (bloom filter sizes, non-UTF-8 footer values): the cells where the known gaps #340 / #341 are expected"
  "$PY" truth.py facts "$OUT/facts.json"
  check "$PY" truth.py assert-bloom "$OUT/facts.json"
  "$PY" fixture.py inventory | tee "$OUT/inventory.txt"
  rm -rf "$OUT/fixture"
  "$PY" fixture.py dump "$OUT/fixture"
  log "tear down the Lakehouse stack"
  "${DC[@]}" down -v --remove-orphans
  [ -z "$SOFT_FAIL" ] || { echo "fixture checks failed against the writer-side truth (see RED lines above)" >&2; return 1; }
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
    --truth "$OUT/truth.json" --facts "$OUT/facts.json" --fixture "$OUT/fixture" \
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
