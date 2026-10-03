#!/usr/bin/env bash
# Proof that the reader matrix turns red when the writer breaks.
#
# Works in an isolated copy (git archive of HEAD plus this working tree's tests/readers), never in the
# checkout: two writer defects an external reader would notice and Lakehouse itself would not.
#
#   M1  the traces writer stores `status.code` under another column name (status_code)
#   M2  the logs writer stores timestamp_unix_nano one hour late (the dt=/hour= directory still says the real hour)
#
# The copy is built into its own image tags, run as compose project lhreaders (ports 39400-39499) and the
# tags are removed afterwards. Exit 0 means the matrix went red as it must; any other status means it did not.
#
#   mutation-proof.sh [engines]     default: duckdb,pyarrow
set -euo pipefail
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
ENGINES=${1:-duckdb,pyarrow}
PY=${PYTHON:-python3}
TMP=$(mktemp -d)
export LH_LOGS_IMAGE=lhreaders-mutant-logs:proof LH_TRACES_IMAGE=lhreaders-mutant-traces:proof DATAGEN_IMAGE=lhreaders-mutant-datagen:proof
cleanup() {
  ( cd "$TMP/tests/readers" 2>/dev/null && ./run.sh down >/dev/null 2>&1 ) || true
  docker image rm "$LH_LOGS_IMAGE" "$LH_TRACES_IMAGE" "$DATAGEN_IMAGE" >/dev/null 2>&1 || true
  rm -rf "$TMP"
}
trap cleanup EXIT

git -C "$ROOT" archive HEAD | tar -x -C "$TMP"
rm -rf "$TMP/tests/readers"
mkdir -p "$TMP/tests/readers"
( cd "$ROOT/tests/readers" && tar --exclude=.out --exclude=__pycache__ -cf - . ) | tar -x -C "$TMP/tests/readers"

"$PY" - "$TMP" <<'PYEOF'
import re, sys
root = sys.argv[1]
# M1: TraceRow.StatusCode column renamed.
p = root + "/internal/schema/row.go"
s = open(p).read()
i = s.index("type TraceRow struct")
old = 'parquet:"status.code"'
j = s.index(old, i)
s = s[:j] + 'parquet:"status_code"' + s[j + len(old):]
open(p, "w").write(s)
# M2: logs timestamps written one hour late.
p = root + "/internal/storage/parquets3/writer.go"
s = open(p).read()
sig = "func writeLogsParquet(rows []schema.LogRow, rowGroupSize int, compressionLevel int) (*flushResult, error) {\n"
assert sig in s
s = s.replace(sig, sig + "\tfor i := range rows {\n\t\trows[i].TimestampUnixNano += 3600 * 1000000000\n\t}\n")
open(p, "w").write(s)
print("mutated: TraceRow status.code -> status_code; logs timestamp +1h")
PYEOF

cd "$TMP/tests/readers"
export PYTHON=$PY OUT="$TMP/out"
./run.sh fixture
rc=0
./run.sh engines "$ENGINES" || rc=$?
if [ "$rc" -eq 0 ]; then
  echo "PROOF FAILED: the matrix stayed green with a broken writer" >&2
  exit 1
fi
echo "PROOF OK: the matrix is red (exit $rc) with a broken writer"
"$PY" report.py "$OUT" || true
