#!/usr/bin/env bash
# Self-test for run.sh's extract_result() — the per-iteration response
# validator (v3 validation). Sources run.sh's function definitions (without running
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
printf '{"service.name":"api-gateway","n":"3000"}\n{"service.name":"web","n":"599"}\n' > "$TMP/groupby_same_total_diff_groups.jsonl"
printf '{"_msg":"hello","service.name":"a"}\n{"_msg":"world","service.name":"b"}\n' > "$TMP/scan_logs.jsonl"
printf '{"trace_id":"t1","span_id":"s1"}\n{"trace_id":"t2","span_id":"s2"}\n' > "$TMP/scan_traces.jsonl"
printf '{"trace_id":"t2","span_id":"s2"}\n{"trace_id":"t1","span_id":"s1"}\n' > "$TMP/scan_traces_reordered.jsonl"
printf '{"n":"8"}\n' > "$TMP/trace_by_id.jsonl"
: > "$TMP/miss_empty.jsonl"                    # legitimate 0-match LogsQL body
printf 'not json at all, definitely not\n' > "$TMP/malformed.txt"
printf '{"n":"1"}\nnot json\n{"n":"2"}\n' > "$TMP/partial_parse.jsonl"   # M-1: some lines parse, some don't
printf '17132\n' > "$TMP/ch_scalar.tsv"
# CH group-by is FORMAT JSONEachRow with count() AS n, same shape as VL/VT/LH
# (M-3: no CH-specific TSV branch — one extractor for every system).
printf '{"ServiceName":"api-gateway","n":"3499"}\n{"ServiceName":"web","n":"100"}\n' > "$TMP/ch_groupby.jsonl"
printf '{"ServiceName":"api-gateway","n":"3000"}\n{"ServiceName":"web","n":"599"}\n' > "$TMP/ch_groupby_same_total_diff_groups.jsonl"
printf '{"_msg":"hello","ServiceName":"a"}\n{"_msg":"world","ServiceName":"b"}\n' > "$TMP/ch_scan.jsonl"  # CH scan: FORMAT JSONEachRow, Body AS _msg
printf '{"trace_id":"t1","span_id":"s1"}\n{"trace_id":"t2","span_id":"s2"}\n' > "$TMP/ch_scan_traces.jsonl"  # TraceId AS trace_id, SpanId AS span_id
printf '8\n' > "$TMP/ch_trace_by_id.tsv"
: > "$TMP/ch_empty.tsv"                        # CH count() always emits a row -> empty is broken
: > "$TMP/ch_scan_empty.jsonl"                 # CH scan with 0 matching rows -> legitimate

# --- VL/VT/LH (JSON lines) -------------------------------------------------
check "scalar count"            "$(extract_result count_total   victorialogs   "$TMP/scalar.jsonl")"          "17132"
check "group-by hashes pairs (not just the total)" "$(extract_result count_by_service victorialogs "$TMP/groupby.jsonl")" "rows=2;total=3599;hash=*"
g1=$(extract_result count_by_service victorialogs "$TMP/groupby.jsonl")
g2=$(extract_result count_by_service victorialogs "$TMP/groupby_same_total_diff_groups.jsonl")
check "group-by: same total (3599), different groups -> different hash" "$([[ "$g1" != "$g2" ]] && echo differ || echo same)" "differ"
check "scan (logs, _msg key)"   "$(extract_result scan          victorialogs   "$TMP/scan_logs.jsonl")"       "rows=2;hash=*"
check "scan (traces, trace:span key)" "$(extract_result scan    victoriatraces "$TMP/scan_traces.jsonl")"    "rows=2;hash=*"
h1=$(extract_result scan victoriatraces "$TMP/scan_traces.jsonl")
h2=$(extract_result scan victoriatraces "$TMP/scan_traces_reordered.jsonl")
check "scan hash is order-independent (sorted key set)" "$h1" "$h2"
check "trace_by_id"              "$(extract_result trace_by_id   victoriatraces "$TMP/trace_by_id.jsonl")"    "spans=8"
check "trace_lookup miss (empty body = 0, not invalid)" "$(extract_result trace_lookup victorialogs "$TMP/miss_empty.jsonl")" "spans=0"
check "malformed body -> invalid" "$(extract_result count_total victorialogs "$TMP/malformed.txt")"           "invalid:parse-error"
check "partial parse (some lines bad) -> invalid" "$(extract_result count_total victorialogs "$TMP/partial_parse.jsonl")" "invalid:parse-error"

# --- ClickHouse -------------------------------------------------------------
check "CH scalar"                "$(extract_result count_total   clickhouse "$TMP/ch_scalar.tsv")"            "17132"
check "CH group-by hashes pairs (JSONEachRow, same extractor as VL/VT/LH)" "$(extract_result count_by_service clickhouse "$TMP/ch_groupby.jsonl")" "rows=2;total=3599;hash=*"
cg1=$(extract_result count_by_service clickhouse "$TMP/ch_groupby.jsonl")
cg2=$(extract_result count_by_service clickhouse "$TMP/ch_groupby_same_total_diff_groups.jsonl")
check "CH group-by: same total, different groups -> different hash" "$([[ "$cg1" != "$cg2" ]] && echo differ || echo same)" "differ"
check "CH group-by and VL group-by agree on the SAME data (equal hash)" \
  "$(extract_result count_by_service clickhouse "$TMP/ch_groupby.jsonl")" \
  "$(extract_result count_by_service victorialogs "$TMP/groupby.jsonl")"
check "CH scan goes through the JSON extractor now (real key, real hash)" "$(extract_result scan clickhouse "$TMP/ch_scan.jsonl")" "rows=2;hash=*"
check "CH scan (traces) uses trace_id:span_id like VT/LH" "$(extract_result scan clickhouse "$TMP/ch_scan_traces.jsonl")" "rows=2;hash=*"
check "CH scan and VT scan agree on the SAME traces (equal hash)" \
  "$(extract_result scan clickhouse "$TMP/ch_scan_traces.jsonl")" \
  "$(extract_result scan victoriatraces "$TMP/scan_traces.jsonl")"
check "CH scan with 0 matching rows -> legitimate rows=0" "$(extract_result scan clickhouse "$TMP/ch_scan_empty.jsonl")" "rows=0;hash=*"
check "CH trace_by_id"           "$(extract_result trace_by_id   clickhouse "$TMP/ch_trace_by_id.tsv")"       "spans=8"
check "CH empty body (non-scan) -> invalid (count() always emits a row)" "$(extract_result count_total clickhouse "$TMP/ch_empty.tsv")" "invalid:empty-body"

# --- result_is_empty / is_miss_query / is_groupby_query --------------------
result_is_empty "spans=0" && check "result_is_empty(spans=0)" "empty" "empty" || check "result_is_empty(spans=0)" "not-empty" "empty"
result_is_empty "rows=0;hash=abc" && check "result_is_empty(rows=0;hash=..)" "empty" "empty" || check "result_is_empty(rows=0;hash=..)" "not-empty" "empty"
result_is_empty "17132" && check "result_is_empty(17132)" "empty" "not-empty" || check "result_is_empty(17132)" "not-empty" "not-empty"
is_miss_query trace_lookup && check "is_miss_query(trace_lookup)" "miss" "miss" || check "is_miss_query(trace_lookup)" "not-miss" "miss"
is_miss_query count_total && check "is_miss_query(count_total)" "miss" "not-miss" || check "is_miss_query(count_total)" "not-miss" "not-miss"
is_groupby_query count_by_service && check "is_groupby_query(count_by_service)" "groupby" "groupby" || check "is_groupby_query(count_by_service)" "not-groupby" "groupby"
is_groupby_query high_card && check "is_groupby_query(high_card)" "groupby" "groupby" || check "is_groupby_query(high_card)" "not-groupby" "groupby"
is_groupby_query count_total && check "is_groupby_query(count_total)" "groupby" "not-groupby" || check "is_groupby_query(count_total)" "not-groupby" "not-groupby"

# --- field metadata (fv_level/fv_service/streams_list/fv_name) -------------
# LogsQL systems answer {"values":[...]}; ClickHouse answers JSONEachRow of the
# same (value, hits) pairs. Same data must give the same result string; a
# sample of the values, or right values with wrong hits, must not.
printf '{"values":[{"value":"INFO","hits":30},{"value":"ERROR","hits":5},{"value":"WARN","hits":7}]}\n' > "$TMP/fv_vl.json"
printf '{"value":"WARN","hits":"7"}\n{"value":"ERROR","hits":"5"}\n{"value":"INFO","hits":"30"}\n' > "$TMP/fv_ch.jsonl"
printf '{"values":[{"value":"INFO","hits":30},{"value":"WARN","hits":7}]}\n' > "$TMP/fv_sample.json"
printf '{"values":[{"value":"INFO","hits":1},{"value":"ERROR","hits":1},{"value":"WARN","hits":1}]}\n' > "$TMP/fv_hits1.json"
printf '{"values":[]}\n' > "$TMP/fv_empty.json"
fv_vl="$(extract_result fv_level victorialogs "$TMP/fv_vl.json")"
check "fv values: rows/total"            "$fv_vl" "rows=3;total=42;hash=*"
check "fv values: CH JSONEachRow matches VL" "$(extract_result fv_level clickhouse "$TMP/fv_ch.jsonl")" "$fv_vl"
check "fv values: LH same as VL"         "$(extract_result fv_level lakehouse "$TMP/fv_vl.json")" "$fv_vl"
[[ "$(extract_result fv_level lakehouse "$TMP/fv_sample.json")" != "$fv_vl" ]] && check "fv values: a sample differs" ok ok || check "fv values: a sample differs" same differ
[[ "$(extract_result fv_level lakehouse "$TMP/fv_hits1.json")" != "$fv_vl" ]] && check "fv values: hits=1 differs" ok ok || check "fv values: hits=1 differs" same differ
check "fv values: empty is invalid"      "$(extract_result fv_level lakehouse "$TMP/fv_empty.json")" "invalid:empty-body"
check "fv values: malformed is invalid"  "$(extract_result streams_list clickhouse "$TMP/malformed.txt")" "invalid:parse-error"
check "fv values: traces kind"           "$(extract_result fv_name victoriatraces "$TMP/fv_vl.json")" "$fv_vl"
is_values_query fv_service && check "is_values_query(fv_service)" "values" "values" || check "is_values_query(fv_service)" "not-values" "values"
is_values_query count_total && check "is_values_query(count_total)" "values" "not-values" || check "is_values_query(count_total)" "not-values" "not-values"

echo "extract_result_test.sh: $pass passed, $fail failed" >&2
[[ "$fail" == 0 ]]
