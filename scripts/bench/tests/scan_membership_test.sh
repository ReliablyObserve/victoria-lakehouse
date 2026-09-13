#!/usr/bin/env bash
# Self-test for the truncated-scan membership-validation rule: VictoriaLogs
# documents that `limit N` without `sort` returns rows "selected in arbitrary
# order because of performance reasons … can return different sets of logs
# every time" once more than N rows match — so per-iteration IDENTITY can
# never hold for a truncated scan. Validity there is MEMBERSHIP (every
# returned row is a real row of the reference window) + CARDINALITY (exactly
# SCAN_LIMIT rows returned), not identity. Exercises _validate_iter() directly
# against fixture bodies and a reference window-key file BUILT VIA
# extract_result() itself (not hand-crafted) — no live stack needed.
#
# Usage: scripts/bench/tests/scan_membership_test.sh
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
BENCH_TMP="$TMP"

FUNCS="$TMP/funcs.sh"
sed -n '1,/^# --- time helpers/p' scripts/bench/run.sh | sed '$d' > "$FUNCS"
# shellcheck disable=SC1090
source "$FUNCS"

pass=0 fail=0
check() {
  local desc="$1" actual="$2" expected="$3"
  if [[ "$actual" == "$expected" ]]; then pass=$((pass+1)); return; fi
  fail=$((fail+1))
  printf 'FAIL: %s\n  expected: %s\n  actual:   %s\n' "$desc" "$expected" "$actual" >&2
}
contains() {
  local desc="$1" haystack="$2" needle="$3"
  if [[ "$haystack" == *"$needle"* ]]; then pass=$((pass+1)); return; fi
  fail=$((fail+1))
  printf 'FAIL: %s\n  expected substring: %s\n  actual: %s\n' "$desc" "$needle" "$haystack" >&2
}

# gen_iter <outfile> <msg>...: writes one valid JSON line per _msg value via
# python's json.dumps (not printf) so a message containing special
# characters — including a REAL embedded newline, like a Java stack trace —
# round-trips exactly as it would over the wire (JSON escapes it; json.loads
# decodes it back to a literal newline in the python string).
gen_iter() {
  local out="$1"; shift
  python3 -c "
import json, sys
with open(sys.argv[1], 'w') as f:
    for m in sys.argv[2:]:
        f.write(json.dumps({'_msg': m, 'service.name': 'a'}) + '\n')
" "$out" "$@"
}

MULTILINE=$'java.lang.OutOfMemoryError: Java heap space\n\tat Foo.process(Foo.java:1)\n\tat Bar.process(Bar.java:2)'

# --- Reference window: build it THROUGH extract_result (the same code path
# run.sh uses), from a synthetic "full window" containing every message the
# fixtures below use, INCLUDING the multi-line one — this is what regressed
# before the NUL-delimited keysout fix (a newline-delimited key file
# silently fragmented a multi-line message into bogus extra "keys", breaking
# `comm`'s line-based comparison for messages that were genuinely present).
gen_iter "$TMP/window_src.jsonl" msgA msgB "$MULTILINE"
WINDOW_KEYS="$TMP/window.keys"
extract_result scan victorialogs "$TMP/window_src.jsonl" "$WINDOW_KEYS" >/dev/null
WINDOW_ROWS=1500   # as if 1500 real (heavily-duplicated) rows matched the window

# --- 1: member ok — 1000 rows, every _msg drawn from the window's key set,
# including the multi-line one (the regression case). ------------------------
{ for i in $(seq 1 499); do printf '%s\n' msgA; done
  for i in $(seq 1 500); do printf '%s\n' msgB; done
  printf '%s\n' MULTI; } > "$TMP/vals_ok.txt"
mapfile -t vals_ok < "$TMP/vals_ok.txt"
for i in "${!vals_ok[@]}"; do [[ "${vals_ok[$i]}" == MULTI ]] && vals_ok[$i]="$MULTILINE"; done
gen_iter "$TMP/iter_ok.jsonl" "${vals_ok[@]}"
_validate_iter scan victorialogs 200 "$TMP/iter_ok.jsonl" "" "$WINDOW_ROWS" "$WINDOW_KEYS"
check "member ok (incl. multi-line message) -> VALID=1" "$VALID" "1"
check "member ok -> REASON empty" "$REASON" ""

# --- 2: foreign key — 1000 rows, one _msg NOT in the window's key set. ------
{ for i in $(seq 1 499); do printf '%s\n' msgA; done
  for i in $(seq 1 500); do printf '%s\n' msgB; done
  printf '%s\n' msgZZZ_not_in_window; } > "$TMP/vals_foreign.txt"
mapfile -t vals_foreign < "$TMP/vals_foreign.txt"
gen_iter "$TMP/iter_foreign.jsonl" "${vals_foreign[@]}"
_validate_iter scan victorialogs 200 "$TMP/iter_foreign.jsonl" "" "$WINDOW_ROWS" "$WINDOW_KEYS"
check "foreign key -> VALID=0" "$VALID" "0"
contains "foreign key -> reason mentions membership" "$REASON" "membership"

# --- 3: count != limit — 998 rows, all real window members. -----------------
{ for i in $(seq 1 498); do printf '%s\n' msgA; done
  for i in $(seq 1 500); do printf '%s\n' msgB; done; } > "$TMP/vals_short.txt"
mapfile -t vals_short < "$TMP/vals_short.txt"
gen_iter "$TMP/iter_short.jsonl" "${vals_short[@]}"
_validate_iter scan victorialogs 200 "$TMP/iter_short.jsonl" "" "$WINDOW_ROWS" "$WINDOW_KEYS"
check "count != limit -> VALID=0" "$VALID" "0"
contains "count != limit -> reason mentions expected count" "$REASON" "998 rows, expected 1000"

# --- 4: ClickHouse truncated scan — now goes through the SAME JSON extractor
# and membership check as VL/VT/LH (Body/TraceId/SpanId aliased to
# _msg/trace_id/span_id, FORMAT JSONEachRow — see _prep_body), so it gets a
# real reference window + real membership/cardinality validation too.
gen_ch_iter() { # $1 outfile  $2... _msg values (CH's aliased column name)
  local out="$1"; shift
  python3 -c "
import json, sys
with open(sys.argv[1], 'w') as f:
    for m in sys.argv[2:]:
        f.write(json.dumps({'_msg': m, 'ServiceName': 'a'}) + '\n')
" "$out" "$@"
}
gen_ch_iter "$TMP/ch_window_src.jsonl" msgA msgB
CH_WINDOW_KEYS="$TMP/ch_window.keys"
extract_result scan clickhouse "$TMP/ch_window_src.jsonl" "$CH_WINDOW_KEYS" >/dev/null

{ for i in $(seq 1 500); do printf '%s\n' msgA; done
  for i in $(seq 1 500); do printf '%s\n' msgB; done; } > "$TMP/ch_vals_ok.txt"
mapfile -t ch_vals_ok < "$TMP/ch_vals_ok.txt"
gen_ch_iter "$TMP/ch_iter_ok.jsonl" "${ch_vals_ok[@]}"
_validate_iter scan clickhouse 200 "$TMP/ch_iter_ok.jsonl" "" "$WINDOW_ROWS" "$CH_WINDOW_KEYS"
check "CH member ok, 1000 rows -> VALID=1" "$VALID" "1"

{ for i in $(seq 1 499); do printf '%s\n' msgA; done
  for i in $(seq 1 500); do printf '%s\n' msgB; done
  printf '%s\n' msgZZZ_not_in_window; } > "$TMP/ch_vals_foreign.txt"
mapfile -t ch_vals_foreign < "$TMP/ch_vals_foreign.txt"
gen_ch_iter "$TMP/ch_iter_foreign.jsonl" "${ch_vals_foreign[@]}"
_validate_iter scan clickhouse 200 "$TMP/ch_iter_foreign.jsonl" "" "$WINDOW_ROWS" "$CH_WINDOW_KEYS"
check "CH foreign key -> VALID=0" "$VALID" "0"
contains "CH foreign key -> reason mentions membership" "$REASON" "membership"

{ for i in $(seq 1 498); do printf '%s\n' msgA; done
  for i in $(seq 1 500); do printf '%s\n' msgB; done; } > "$TMP/ch_vals_short.txt"
mapfile -t ch_vals_short < "$TMP/ch_vals_short.txt"
gen_ch_iter "$TMP/ch_iter_short.jsonl" "${ch_vals_short[@]}"
_validate_iter scan clickhouse 200 "$TMP/ch_iter_short.jsonl" "" "$WINDOW_ROWS" "$CH_WINDOW_KEYS"
check "CH truncated, 998 rows -> VALID=0" "$VALID" "0"
contains "CH count != limit -> reason mentions expected count" "$REASON" "998 rows, expected 1000"

# --- 6: traces-keyed membership (trace_id:span_id), not the logs _msg key. --
gen_trace_iter() { # $1 outfile  $2... "trace_id:span_id" pairs
  local out="$1"; shift
  python3 -c "
import json, sys
with open(sys.argv[1], 'w') as f:
    for pair in sys.argv[2:]:
        tid, sid = pair.split(':', 1)
        f.write(json.dumps({'trace_id': tid, 'span_id': sid, 'name': 'x'}) + '\n')
" "$out" "$@"
}
gen_trace_iter "$TMP/trace_window_src.jsonl" t1:s1 t2:s2 t3:s3
TRACE_WINDOW_KEYS="$TMP/trace_window.keys"
extract_result scan victoriatraces "$TMP/trace_window_src.jsonl" "$TRACE_WINDOW_KEYS" >/dev/null

{ for i in $(seq 1 500); do printf 't1:s1\n'; done
  for i in $(seq 1 500); do printf 't2:s2\n'; done; } > "$TMP/trace_vals_ok.txt"
mapfile -t trace_vals_ok < "$TMP/trace_vals_ok.txt"
gen_trace_iter "$TMP/trace_iter_ok.jsonl" "${trace_vals_ok[@]}"
_validate_iter scan victoriatraces 200 "$TMP/trace_iter_ok.jsonl" "" "$WINDOW_ROWS" "$TRACE_WINDOW_KEYS"
check "traces member ok (trace_id:span_id key) -> VALID=1" "$VALID" "1"

{ for i in $(seq 1 499); do printf 't1:s1\n'; done
  for i in $(seq 1 500); do printf 't2:s2\n'; done
  printf 't99:s99_not_in_window\n'; } > "$TMP/trace_vals_foreign.txt"
mapfile -t trace_vals_foreign < "$TMP/trace_vals_foreign.txt"
gen_trace_iter "$TMP/trace_iter_foreign.jsonl" "${trace_vals_foreign[@]}"
_validate_iter scan victoriatraces 200 "$TMP/trace_iter_foreign.jsonl" "" "$WINDOW_ROWS" "$TRACE_WINDOW_KEYS"
check "traces foreign trace_id:span_id -> VALID=0" "$VALID" "0"
contains "traces foreign -> reason mentions membership" "$REASON" "membership"

# --- 5: untruncated scan (window_rows <= SCAN_LIMIT) still uses identity. ---
gen_iter "$TMP/iter_small.jsonl" msgA msgB
_validate_iter scan victorialogs 200 "$TMP/iter_small.jsonl" "" 2 "$WINDOW_KEYS"
first="$RESULT"
check "untruncated, first iteration -> VALID=1" "$VALID" "1"
gen_iter "$TMP/iter_small2.jsonl" msgA msgC
_validate_iter scan victorialogs 200 "$TMP/iter_small2.jsonl" "$first" 2 "$WINDOW_KEYS"
check "untruncated, flapping vs first result -> VALID=0" "$VALID" "0"
contains "untruncated flapping -> reason mentions flapping" "$REASON" "flapping"

echo "scan_membership_test.sh: $pass passed, $fail failed" >&2
[[ "$fail" == 0 ]]
