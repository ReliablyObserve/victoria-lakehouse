#!/usr/bin/env bash
# Self-test for run.sh's _prep_body(): every (signal, query, system) the matrix
# can ask for must produce a request line, under `set -u` exactly as run.sh
# runs — an unset variable for one system (e.g. ClickHouse has no LogsQL base
# URL) must never abort the benchmark before a single cell is measured.
#
# Usage: scripts/bench/tests/prep_body_test.sh
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1
RUN_SH="$PWD/scripts/bench/run.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Function definitions only: the validation helpers (up to "time helpers") and
# the request matrix (time helpers .. the query lists), never the orchestration.
sed -n '1,/^# --- time helpers/p' scripts/bench/run.sh | sed '$d' > "$TMP/funcs.sh"
sed -n '/^# --- time helpers/,/^LOG_QUERIES=/p' scripts/bench/run.sh | sed '$d' >> "$TMP/funcs.sh"
BENCH_TMP="$TMP"
# shellcheck disable=SC1090
source "$TMP/funcs.sh"
log() { :; }
SAMPLE_TID="t"; SCAN_LIMIT=10
# The query lists are run.sh's own, so a kind added there is tested here.
eval "$(grep -E '^(LOG|TRACE)_QUERIES=' "$RUN_SH")"
LOGS_Q="$LOG_QUERIES"
TRACES_Q="$TRACE_QUERIES"
[[ " $TRACES_Q " == *" streams_list "* && " $LOGS_Q " == *" streams_list "* ]] || { echo "FAIL: streams must be measured on both signals" >&2; exit 1; }

pass=0 fail=0
run() { # $1 signal $2 query $3 system
  local out
  if ! out="$(set -u; _prep_body "$1" "$2" "$3" 100 200 2>&1)" || [[ -z "$out" ]]; then
    fail=$((fail+1)); printf 'FAIL: %s/%s %s -> %q\n' "$1" "$2" "$3" "$out" >&2; return
  fi
  pass=$((pass+1))
}
for q in $LOGS_Q; do for s in lakehouse victorialogs clickhouse; do run logs "$q" "$s"; done; done
for q in $TRACES_Q; do for s in lakehouse victoriatraces clickhouse; do run traces "$q" "$s"; done; done

# The field-metadata kinds must reach the right endpoint and SQL.
out="$(_prep_body logs fv_level victorialogs 100 200)"
[[ "$out" == POST*"/select/logsql/field_values?start=100&end=200&field=level"* ]] && pass=$((pass+1)) || { fail=$((fail+1)); echo "FAIL: fv_level VL request: $out" >&2; }
out="$(_prep_body traces fv_name clickhouse 100 200)"
[[ "$out" == CH*"SELECT SpanName AS value, count() AS hits"*"GROUP BY value FORMAT JSONEachRow"* ]] && pass=$((pass+1)) || { fail=$((fail+1)); echo "FAIL: fv_name CH request: $out" >&2; }

out="$(_prep_body traces streams_list victoriatraces 100 200)"
[[ "$out" == POST*"/select/logsql/streams?start=100&end=200"*"trace_id:*" ]] && pass=$((pass+1)) || { fail=$((fail+1)); echo "FAIL: traces streams VT request: $out" >&2; }
out="$(_prep_body traces streams_list clickhouse 100 200)"
[[ "$out" == CH*"SELECT Stream AS value, count() AS hits FROM lakehouse.otel_traces"* ]] && pass=$((pass+1)) || { fail=$((fail+1)); echo "FAIL: traces streams CH request: $out" >&2; }

echo "prep_body_test.sh: $pass passed, $fail failed" >&2
[[ "$fail" == 0 ]]
