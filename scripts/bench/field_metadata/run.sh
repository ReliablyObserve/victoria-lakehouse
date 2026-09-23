#!/usr/bin/env bash
# Field-metadata performance matrix, before/after, interleaved.
#
# Builds the logs and traces test binaries of two trees — the current checkout
# ("after") and a detached worktree of BEFORE_REF ("before") into which the
# harness files are copied — and runs TestFieldMetadataMatrix /
# TestFieldMetadataMatrixTraces alternately (A/B, B/A, ...) for ROUNDS rounds.
# Every round records the host load average. aggregate.py turns the JSONL
# into the per-cell before/after table (docs/perf/field-metadata-cells.md).
#
# Usage:
#   scripts/bench/field_metadata/run.sh [OUT_DIR]
# Env:
#   BEFORE_REF   ref of the "before" tree            (default 0ac43468 = v0.143.0 release commit)
#   BEFORE_DIR   where its worktree lives             (default .before-<ref>)
#   ROUNDS       interleaving rounds                  (default 5)
#   ITERS        measured iterations per cell/round   (default 2 → n = ROUNDS*ITERS)
#   WARMUP       unrecorded iterations per cell/round (default 1)
#   LATENCIES_MS S3 first-byte latencies              (default "0,100")
#   SIGNALS      "logs traces" or a subset            (default both)
#   VL           1 = also run the hot-VictoriaLogs reference each round (default 1)
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
OUT=${1:-$ROOT/bench-results/field-metadata/$(date -u +%Y%m%dT%H%M%SZ)}
BEFORE_REF=${BEFORE_REF:-0ac43468}
BEFORE_DIR=${BEFORE_DIR:-$ROOT/.before-$BEFORE_REF}
ROUNDS=${ROUNDS:-5}
ITERS=${ITERS:-2}
WARMUP=${WARMUP:-1}
LATENCIES_MS=${LATENCIES_MS:-0,100}
SIGNALS=${SIGNALS:-logs traces}
GIT=${GIT:-git}
export GOWORK=off

mkdir -p "$OUT"

if [ ! -d "$BEFORE_DIR" ]; then
  "$GIT" -C "$ROOT" worktree add --detach "$BEFORE_DIR" "$BEFORE_REF"
fi
# deps/ is not versioned: the before tree borrows the current checkout's pins.
[ -d "$BEFORE_DIR/deps" ] || cp -R "$ROOT/deps" "$BEFORE_DIR/"
[ -d "$BEFORE_DIR/lakehouse-traces/deps" ] || cp -R "$ROOT/lakehouse-traces/deps" "$BEFORE_DIR/lakehouse-traces/"
cp "$ROOT/internal/storage/parquets3/field_values_bench_test.go" "$BEFORE_DIR/internal/storage/parquets3/"
cp "$ROOT/lakehouse-traces/internal/storage/parquets3/field_values_bench_test.go" "$BEFORE_DIR/lakehouse-traces/internal/storage/parquets3/"

build() { # tree label
  (cd "$1" && go test -c -o "$OUT/$2-logs.test" ./internal/storage/parquets3/)
  (cd "$1/lakehouse-traces" && go test -c -o "$OUT/$2-traces.test" ./internal/storage/parquets3/)
}
build "$BEFORE_DIR" before
build "$ROOT" after
{
  echo "before=$("$GIT" -C "$BEFORE_DIR" rev-parse HEAD)"
  echo "after=$("$GIT" -C "$ROOT" rev-parse HEAD)"
  echo "rounds=$ROUNDS iters=$ITERS warmup=$WARMUP latencies_ms=$LATENCIES_MS"
  echo "host=$(uname -srm) cpus=$(getconf _NPROCESSORS_ONLN)"
} > "$OUT/meta.txt"

run_one() { # build signal round
  local test=TestFieldMetadataMatrix
  [ "$2" = traces ] && test=TestFieldMetadataMatrixTraces
  FM_MATRIX_OUT="$OUT/matrix.jsonl" FM_BUILD="$1" FM_ROUND="$3" FM_ITERS="$ITERS" \
    FM_WARMUP="$WARMUP" FM_LATENCIES_MS="$LATENCIES_MS" \
    "$OUT/$1-$2.test" -test.run "^$test\$" -test.count=1 > "$OUT/$1-$2-r$3.log" 2>&1 \
    || { echo "run failed: $1 $2 round $3 (see $OUT/$1-$2-r$3.log)" >&2; exit 1; }
}

for r in $(seq 1 "$ROUNDS"); do
  order="before after"
  [ $((r % 2)) -eq 0 ] && order="after before"
  echo "round $r ($order) load: $(uptime | sed 's/.*load averages*: //')" | tee -a "$OUT/load.txt"
  for b in $order; do
    for sig in $SIGNALS; do
      run_one "$b" "$sig" "$r"
    done
  done
  # Hot VictoriaLogs reference on the same rows (logs cells, no S3, no pmeta).
  if [ "${VL:-1}" = 1 ] && [[ " $SIGNALS " == *" logs "* ]]; then
    FM_MATRIX_OUT="$OUT/matrix.jsonl" FM_ROUND="$r" FM_ITERS="$ITERS" FM_WARMUP=1 \
      "$OUT/after-logs.test" -test.run '^TestFieldMetadataMatrixVL$' -test.count=1 > "$OUT/vl-r$r.log" 2>&1 \
      || { echo "vl run failed: round $r (see $OUT/vl-r$r.log)" >&2; exit 1; }
  fi
done
echo "end load: $(uptime | sed 's/.*load averages*: //')" | tee -a "$OUT/load.txt"

python3 "$(dirname "$0")/aggregate.py" "$OUT/matrix.jsonl" > "$OUT/matrix.md"
echo "wrote $OUT/matrix.md"
