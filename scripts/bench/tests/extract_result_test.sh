#!/usr/bin/env bash
# Self-test for run.sh's extract_result() — the per-iteration response
# validator (Task 6). Sources run.sh's function definitions (without running
# its top-level orchestration, which needs a live stack) against fixture
# response bodies covering every qkind/system combination, plus a malformed
# body, and asserts the expected comparable-value string or "invalid:*".
#
# Usage: scripts/bench/tests/extract_result_test.sh
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Pull in just the function definitions from run.sh (everything up to the
# "time helpers" section, before the top-level `trap teardown EXIT` /
# orchestration runs) so this test never touches Docker or the network.
FUNCS="$TMP/funcs.sh"
sed -n '1,/^# --- time helpers/p' scripts/bench/run.sh | sed '$d' > "$FUNCS"
BENCH_TMP="$TMP" # extract_result doesn't use BENCH_TMP itself, but _do_req would
# shellcheck disable=SC1090
source "$FUNCS"

pass=0 fail=0

check() { # $1 description  $2 actual  $3 expected(exact) OR expected-prefix ending in '*'
  local desc="$1" actual="$2" expected="$3"
  if [[ "$expected" == *'*' ]]; then
    if [[ "$actual" == ${expected%\*}* ]]; then pass=$((pass+1)); return; fi
  else
    if [[ "$actual" == "$expected" ]]; then pass=$((pass+1)); return; fi
  fi
  fail=$((fail+1))
  printf 'FAIL: %s\n  expected: %s\n  actual:   %s\n' "$desc" "$expected" "$actual" >&2
}

# --- fixtures ------------------------------------------------------------
printf '{"n":"17132"}\n' > "$TMP/scalar.jsonl"
printf '{"service.name":"api-gateway","n":"3499"}\n{"service.name":"web","n":"100"}\n' > "$TMP/groupby.jsonl"
printf '{"_msg":"hello","service.name":"a"}\n{"_msg":"world","service.name":"b"}\n' > "$TMP/scan_logs.jsonl"
printf '{"trace_id":"t1","span_id":"s1"}\n{"trace_id":"t2","span_id":"s2"}\n' > "$TMP/scan_traces.jsonl"
printf '{"trace_id":"t2","span_id":"s2"}\n{"trace_id":"t1","span_id":"s1"}\n' > "$TMP/scan_traces_reordered.jsonl"
printf '{"n":"8"}\n' > "$TMP/trace_by_id.jsonl"
: > "$TMP/miss_empty.jsonl"                    # legitimate 0-match LogsQL body
printf 'not json at all, definitely not\n' > "$TMP/malformed.txt"
printf '17132\n' > "$TMP/ch_scalar.tsv"
printf 'api-gateway\t3499\nweb\t100\n' > "$TMP/ch_groupby.tsv"
printf 'a\tb\tc\nd\te\tf\n' > "$TMP/ch_scan.tsv"
printf '8\n' > "$TMP/ch_trace_by_id.tsv"
: > "$TMP/ch_empty.tsv"                        # CH count() always emits a row -> empty is broken

# --- VL/VT/LH (JSON lines) -------------------------------------------------
check "scalar count"            "$(extract_result count_total   victorialogs   "$TMP/scalar.jsonl")"          "17132"
check "group-by sums to total"  "$(extract_result count_by_service victorialogs "$TMP/groupby.jsonl")"        "3599"
check "scan (logs, _msg key)"   "$(extract_result scan          victorialogs   "$TMP/scan_logs.jsonl")"       "rows=2;hash=*"
check "scan (traces, trace:span key)" "$(extract_result scan    victoriatraces "$TMP/scan_traces.jsonl")"    "rows=2;hash=*"
h1=$(extract_result scan victoriatraces "$TMP/scan_traces.jsonl")
h2=$(extract_result scan victoriatraces "$TMP/scan_traces_reordered.jsonl")
check "scan hash is order-independent (sorted key set)" "$h1" "$h2"
check "trace_by_id"              "$(extract_result trace_by_id   victoriatraces "$TMP/trace_by_id.jsonl")"    "spans=8"
check "trace_lookup miss (empty body = 0, not invalid)" "$(extract_result trace_lookup victorialogs "$TMP/miss_empty.jsonl")" "spans=0"
check "malformed body -> invalid" "$(extract_result count_total victorialogs "$TMP/malformed.txt")"           "invalid:parse-error"

# --- ClickHouse (TSV) -------------------------------------------------------
check "CH scalar"                "$(extract_result count_total   clickhouse "$TMP/ch_scalar.tsv")"            "17132"
check "CH group-by sums to total" "$(extract_result count_by_service clickhouse "$TMP/ch_groupby.tsv")"       "3599"
check "CH scan is rows-only (no hash)" "$(extract_result scan    clickhouse "$TMP/ch_scan.tsv")"              "rows=2"
check "CH trace_by_id"           "$(extract_result trace_by_id   clickhouse "$TMP/ch_trace_by_id.tsv")"       "spans=8"
check "CH empty body -> invalid (count() always emits a row)" "$(extract_result count_total clickhouse "$TMP/ch_empty.tsv")" "invalid:empty-body"

# --- result_is_empty / is_miss_query ---------------------------------------
result_is_empty "spans=0" && check "result_is_empty(spans=0)" "empty" "empty" || check "result_is_empty(spans=0)" "not-empty" "empty"
result_is_empty "rows=0;hash=abc" && check "result_is_empty(rows=0;hash=..)" "empty" "empty" || check "result_is_empty(rows=0;hash=..)" "not-empty" "empty"
result_is_empty "17132" && check "result_is_empty(17132)" "empty" "not-empty" || check "result_is_empty(17132)" "not-empty" "not-empty"
is_miss_query trace_lookup && check "is_miss_query(trace_lookup)" "miss" "miss" || check "is_miss_query(trace_lookup)" "not-miss" "miss"
is_miss_query count_total && check "is_miss_query(count_total)" "miss" "not-miss" || check "is_miss_query(count_total)" "not-miss" "not-miss"

echo "extract_result_test.sh: $pass passed, $fail failed" >&2
[[ "$fail" == 0 ]]
