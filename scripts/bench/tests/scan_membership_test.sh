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

# --- 4: ClickHouse truncated scan — rows-only, no membership possible. ------
python3 -c "
for _ in range(1000):
    print('a\tb\tc')
" > "$TMP/ch_iter_1000.tsv"
_validate_iter scan clickhouse 200 "$TMP/ch_iter_1000.tsv" "" "$WINDOW_ROWS" ""
check "CH truncated, 1000 rows -> VALID=1" "$VALID" "1"

python3 -c "
for _ in range(998):
    print('a\tb\tc')
" > "$TMP/ch_iter_998.tsv"
_validate_iter scan clickhouse 200 "$TMP/ch_iter_998.tsv" "" "$WINDOW_ROWS" ""
check "CH truncated, 998 rows -> VALID=0" "$VALID" "0"

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
