#!/usr/bin/env bash
# Proof that the reader matrix turns red when the writer (or the harness) breaks.
#
# Every mutant is built and run in its own isolated copy (git archive of HEAD plus this working tree's
# tests/readers), never in the checkout, as compose project lhreaders (ports 39400-39499) with its own
# image tags, which are removed afterwards. Defects an external reader would notice and Lakehouse itself
# would NOT (it reads its own files back the same way) are the point of the writer-side truth:
#
#   M1   traces writer stores `status.code` under another column name (status_code)             engines
#   M2   logs writer stores timestamp_unix_nano one hour late (dt=/hour= still say the real hour) engines
#   MA   traces writer drops the span MAP key rpc.system                                          fixture + engines
#   MB   logs writer truncates timestamps to microseconds                                         fixture + engines
#   MC   logs writer drops the log MAP key `format`                                               fixture + engines
#   MD2  logs flush writes every 10th acme-corp row with tenant columns 4401:1                    fixture + engines
#   H1a  harness: the "raw" layer is snapshotted after compaction (it holds compacted objects)    fixture
#   H1b  harness: the engines read the compacted bucket for the "raw" layer                       engines
#
# "fixture" = `run.sh fixture` must fail (Lakehouse, or the files, differ from the writer-side manifest;
# or the layers are not structurally different); "engines" = `run.sh engines` must fail (an external
# engine's answer differs from the manifest, or the files it read are not the layer's inventory). The
# fixture runs with FIXTURE_CONTINUE=1 so the engines are run on the broken files even when the fixture
# already caught the defect: both lines of defence are shown.
#
#   mutation-proof.sh [engines [mutant ...]]     default engines: duckdb,pyarrow; default: every mutant
set -euo pipefail
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
ENGINES=${1:-duckdb,pyarrow}
shift || true
MUTANTS=${*:-M1 M2 MA MB MC MD2 H1a H1b}
PY=${PYTHON:-python3}
SUMMARY=$(mktemp)

expect() { # expected red lines per mutant
  case $1 in
    M1|M2|H1b) echo engines ;;
    H1a) echo fixture ;;
    *) echo "fixture engines" ;;
  esac
}

run_mutant() {
  local m=$1 tmp tag
  tmp=$(mktemp -d)
  tag=$(echo "$m" | tr 'A-Z' 'a-z')
  export LH_LOGS_IMAGE=lhreaders-mut-$tag-logs:proof LH_TRACES_IMAGE=lhreaders-mut-$tag-traces:proof DATAGEN_IMAGE=lhreaders-mut-$tag-datagen:proof
  git -C "$ROOT" archive HEAD | tar -x -C "$tmp"
  rm -rf "$tmp/tests/readers"
  mkdir -p "$tmp/tests/readers"
  ( cd "$ROOT/tests/readers" && tar --exclude=.out --exclude=__pycache__ --exclude=.pytest_cache -cf - . ) | tar -x -C "$tmp/tests/readers"
  # The working tree's datagen (manifest support) and the documentation the engines run: HEAD may not have them yet.
  cp "$ROOT"/cmd/datagen/*.go "$tmp/cmd/datagen/"
  cp "$ROOT/docs/open-parquet-format.md" "$tmp/docs/open-parquet-format.md"
  "$PY" "$ROOT/tests/readers/mutants.py" "$m" "$tmp"
  local fx=0 en=0 out="$tmp/out"
  ( cd "$tmp/tests/readers" && export PYTHON=$PY OUT="$out" FIXTURE_CONTINUE=1 && ./run.sh fixture ) || fx=$?
  if [ -d "$out/fixture" ]; then
    ( cd "$tmp/tests/readers" && export PYTHON=$PY OUT="$out" && ./run.sh engines "$ENGINES" ) || en=$?
  else
    en=99   # no fixture dump: the stack broke before the engines could run
  fi
  ( cd "$tmp/tests/readers" && ./run.sh down >/dev/null 2>&1 ) || true
  docker image rm "$LH_LOGS_IMAGE" "$LH_TRACES_IMAGE" "$DATAGEN_IMAGE" >/dev/null 2>&1 || true
  local want ok=1
  want=$(expect "$m")
  for w in $want; do
    case $w in
      fixture) [ "$fx" -ne 0 ] || ok=0 ;;
      engines) [ "$en" -ne 0 ] && [ "$en" -ne 99 ] || ok=0 ;;
    esac
  done
  printf '%-4s fixture rc=%-3s engines rc=%-3s expected red: %-16s %s\n' "$m" "$fx" "$en" "$want" "$([ $ok = 1 ] && echo RED-AS-REQUIRED || echo STAYED-GREEN)" | tee -a "$SUMMARY"
  [ -d "$out" ] && { "$PY" "$ROOT/tests/readers/report.py" "$out" 2>/dev/null | sed -n '1,25p' || true; }
  rm -rf "$tmp"
  return $((1 - ok))
}

rc=0
for m in $MUTANTS; do
  echo "=================== mutant $m"
  run_mutant "$m" || rc=1
done
echo "=================== summary"
cat "$SUMMARY"
rm -f "$SUMMARY"
if [ "$rc" -ne 0 ]; then
  echo "PROOF FAILED: a mutant stayed green" >&2
  exit 1
fi
echo "PROOF OK: every mutant turned the matrix red where it must"
