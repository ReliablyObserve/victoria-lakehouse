#!/usr/bin/env bash
# Cold-read measurement matrix: BASE tree vs PR tree, in-process, deterministic.
#
# Both trees run the same Go harness (internal/storage/parquets3/cold_read_bench_test.go and its
# traces twin, TestColdReadProfile) against the in-process counting S3 server
# (s3count_test.go) with latency injected as time to first byte, so GETs, bytes and the
# sequential round-trip chain are counted at the HTTP layer and wall time is
# chain x latency, independent of the machine. Every timed answer is compared with the
# generator's ground truth (count shapes) and, by table.py, between the two builds.
#
# Usage: scripts/bench/cold-read/run.sh <base-tree> <pr-tree> <out-dir> [quick|full]
#   <base-tree>  a checkout (git worktree or `git archive` copy) of the build to compare against;
#                the harness files are copied in from <pr-tree> when it lacks them (test-only files).
#   <pr-tree>    the checkout under test (deps/ in place, as for `go test`).
#   <out-dir>    jsonl + logs + table.md are written here.
#   quick        (default) Layout A, 11 files, logs + traces, 0 and 50 ms, core shapes.
#   full         adds Layout B, 50 files, 20 and 100 ms, and every shape.
# Needs GOWORK=off (the script sets it) and Go on PATH.
set -euo pipefail
export GOWORK=off GOTOOLCHAIN=${GOTOOLCHAIN:-auto}
BASE=$(cd "$1" && pwd); PR=$(cd "$2" && pwd); OUT=$3; MODE=${4:-quick}
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)

# Pin the data anchor once: the harness otherwise derives it from the wall-clock hour, and a run that
# crosses an hour boundary would compare two builds on differently-timestamped rows (the answers' _time
# buckets then differ and table.py reports a false mismatch).
export PROFILE_ANCHOR_NS=${PROFILE_ANCHOR_NS:-$(python3 -c 'import time; print((int(time.time())//3600*3600-3600+2400)*10**9)')}
# PROFILE_READMODE=sync|async selects parquet-go's page read mode for both builds (default: the build's own).

LOGS_PKG=internal/storage/parquets3
TRACES_PKG=lakehouse-traces/internal/storage/parquets3
for f in cold_read_bench_test.go s3count_test.go; do
  [ -f "$BASE/$LOGS_PKG/$f" ]   || cp "$PR/$LOGS_PKG/$f"   "$BASE/$LOGS_PKG/$f"
  [ -f "$BASE/$TRACES_PKG/$f" ] || cp "$PR/$TRACES_PKG/$f" "$BASE/$TRACES_PKG/$f"
done

echo "building test binaries" >&2
for b in base pr; do
  tree=$BASE; [ $b = pr ] && tree=$PR
  (cd "$tree" && go test -c -o "$OUT/$b-logs.test" "./$LOGS_PKG/")
  (cd "$tree/lakehouse-traces" && go test -c -o "$OUT/$b-traces.test" "./internal/storage/parquets3/")
done

run() { # signal build latency_ms reps files layout only
  local sig=$1 b=$2 lat=$3 reps=$4 files=$5 lay=$6 only=$7
  local tag="$sig-$b-f${files:-def}-L$lay-${lat}ms"
  LH_COLD_PROFILE=1 PROFILE_LAT_MS=$lat PROFILE_REPS=$reps PROFILE_BUILD=$b PROFILE_FILES=$files \
    PROFILE_LAYOUT=$lay PROFILE_ONLY=$only PROFILE_OUT="$OUT/$tag.jsonl" \
    "$OUT/$b-$sig.test" -test.run 'TestColdReadProfile$' -test.timeout 120m >"$OUT/$tag.log" 2>&1 \
    || { echo "FAILED: $tag (see $OUT/$tag.log)" >&2; return 1; }
}

CORE="L01,L02,L06,L09,L11,L13,L20,D2"
ALL="L01,L02,L03,L04,L05,L06,L07,L08,L09,L10,L11,L12,L13,L14,L15,L16,L17,L18,L19,L20,D1,D2,D3,D4,D5"
if [ "$MODE" = full ]; then LAT="0 20 50 100"; SHAPES=$ALL; else LAT="0 50"; SHAPES=$CORE; fi
for lat in $LAT; do
  reps=9; [ "$lat" != 0 ] && reps=3
  for b in base pr; do run logs "$b" "$lat" "$reps" "" A "$SHAPES"; done        # interleaved per latency
  for b in base pr; do run traces "$b" "$lat" "$reps" "" A "T"; done
  if [ "$MODE" = full ]; then
    for b in base pr; do run logs "$b" "$lat" "$reps" 50 A "L02,L09,L11,D2,L13"; done
    for b in base pr; do run logs "$b" "$lat" "$reps" "" B "$CORE"; done
  fi
done
python3 "$HERE/table.py" "$OUT" > "$OUT/table.md"
cat "$OUT/table.md"
