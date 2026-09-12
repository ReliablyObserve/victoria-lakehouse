#!/usr/bin/env bash
# Self-tests for run.sh's measure_query(), _validate_iter()'s non-scan
# branches, and build_scan_window()/strip_scan_limit() — no live stack
# needed. _do_req is stubbed with a scripted sequence of canned responses so
# these run deterministically against fixture bodies instead of the network.
#
# Usage: scripts/bench/tests/measure_query_test.sh
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

# --- _do_req stub: pops one scripted response per call ----------------------
# Each queue entry is "code|time|size|bodyfile" (bodyfile "" -> empty body),
# one per line in a FILE (not a bash array): measure_query calls _do_req via
# `$(...)`, which forks a subshell, so an array popped inside _do_req would
# never be visible to the next call — a real file's mutations DO persist
# across that subshell boundary. Overrides the real _do_req (sourced above)
# so these tests never touch curl.
STUB_QUEUE_FILE="$TMP/stub_queue.txt"
set_stub_queue() { printf '%s\n' "$@" > "$STUB_QUEUE_FILE"; }
_do_req() {
  local entry; entry=$(head -n1 "$STUB_QUEUE_FILE" 2>/dev/null)
  tail -n +2 "$STUB_QUEUE_FILE" > "$STUB_QUEUE_FILE.next" 2>/dev/null && mv "$STUB_QUEUE_FILE.next" "$STUB_QUEUE_FILE"
  local code time size bodyfile
  IFS='|' read -r code time size bodyfile <<<"$entry"
  local tmp; tmp=$(mktemp "$BENCH_TMP/stub.XXXXXX")
  if [[ -n "$bodyfile" && -f "$bodyfile" ]]; then cp "$bodyfile" "$tmp"; else : > "$tmp"; fi
  printf '%s %s %s %s\n' "${code:-000}" "${time:-0}" "${size:-0}" "$tmp"
}

VALID_BODY="$TMP/valid.json"
printf '{"n":"5"}\n' > "$VALID_BODY"
EMPTY_BODY="$TMP/empty.json"
: > "$EMPTY_BODY"

# =============================================================================
# I-7(1)/R-5: measure_query — 18 valid iterations at DISTINCT 1..18ms samples
# (not all-identical, so a leaked invalid sample is actually detectable in
# the percentiles), 1x http-500 at a tiny 0.1ms (would drag percentiles WAY
# down if it leaked into the valid set), 1x200-empty-body at a huge 500ms
# (would drag them WAY up if it leaked) — in that order, so invalid_reasons
# preserves first-seen order ("http 500" before "empty result").
# =============================================================================
WARMUP=0 ITERATIONS=20
queue=("500|0.0001|50|")
for i in $(seq 1 18); do
  t=$(python3 -c "print($i/1000)")
  queue+=("200|$t|20|$VALID_BODY")
done
queue+=("200|0.5|0|$EMPTY_BODY")
set_stub_queue "${queue[@]}"

row=$(measure_query "logs/count_total/1h/lat0" victorialogs POST "http://x" "* | stats count() n" count_total)
gv() { python3 -c "import json,sys;print(json.loads(sys.argv[1])['$2'])" "$1"; }
iters=$(gv "$row" iters)
iv=$(gv "$row" iters_valid)
ii=$(gv "$row" iters_invalid)
err=$(gv "$row" errors)
reasons=$(gv "$row" invalid_reasons)
p50=$(gv "$row" p50_ms)
p90=$(gv "$row" p90_ms)
p99=$(gv "$row" p99_ms)
check "measure_query: iters=18 (only the valid, timed samples)" "$iters" "18"
check "measure_query: iters_valid=18" "$iv" "18"
check "measure_query: iters_invalid=2" "$ii" "2"
check "measure_query: errors=1 (only the http 500)" "$err" "1"
check "measure_query: invalid_reasons order (500 seen before empty)" "$reasons" "http 500; empty result"
# sorted [1..18]ms, 0-indexed: p50=vals[9]=10, p90=vals[16]=17, p99=vals[17]=18.
# If either invalid sample (0.1ms or 500ms) had leaked in, these would be
# wrong in an obvious, easy-to-notice way (e.g. p99 would jump to 500.0).
check "measure_query: p50 == 10.0 (median of 1..18, invalid samples excluded)" "$p50" "10.0"
check "measure_query: p90 == 17.0" "$p90" "17.0"
check "measure_query: p99 == 18.0 (the true max, NOT the 500ms invalid sample)" "$p99" "18.0"

# =============================================================================
# I-7(2): _validate_iter non-scan branches
# =============================================================================
_validate_iter count_total victorialogs 500 "$EMPTY_BODY" "" "" ""
check "_validate_iter: http 500 -> invalid" "$VALID" "0"
check "_validate_iter: http 500 -> reason" "$REASON" "http 500"

_validate_iter count_total victorialogs 200 "$EMPTY_BODY" "" "" ""
check "_validate_iter: count_total empty body -> invalid (empty result)" "$VALID" "0"
check "_validate_iter: count_total empty body -> reason" "$REASON" "empty result"

_validate_iter trace_lookup victorialogs 200 "$EMPTY_BODY" "" "" ""
check "_validate_iter: trace_lookup empty body -> VALID (documented miss)" "$VALID" "1"

MALFORMED="$TMP/malformed.txt"
printf 'not json\n' > "$MALFORMED"
_validate_iter count_total victorialogs 200 "$MALFORMED" "" "" ""
check "_validate_iter: malformed body -> invalid" "$VALID" "0"
check "_validate_iter: malformed body -> reason is parse-error" "$REASON" "parse-error"

_validate_iter count_total victorialogs 200 "$VALID_BODY" "" "" ""
first="$RESULT"
check "_validate_iter: scalar first iteration -> VALID" "$VALID" "1"
OTHER_BODY="$TMP/other.json"
printf '{"n":"9"}\n' > "$OTHER_BODY"
_validate_iter count_total victorialogs 200 "$OTHER_BODY" "$first" "" ""
check "_validate_iter: scalar flapping vs first_result -> invalid" "$VALID" "0"
contains "_validate_iter: scalar flapping -> reason mentions flapping" "$REASON" "flapping"

# =============================================================================
# I-7(3): build_scan_window + strip_scan_limit for LogsQL and CH forms
# =============================================================================
check "strip_scan_limit: LogsQL form" \
  "$(strip_scan_limit POST "* | fields _msg, service.name | limit $SCAN_LIMIT")" \
  "* | fields _msg, service.name"
check "strip_scan_limit: CH form (LIMIT before FORMAT, not a trailing suffix)" \
  "$(strip_scan_limit CH "SELECT Body AS _msg FROM t WHERE x LIMIT $SCAN_LIMIT FORMAT JSONEachRow")" \
  "SELECT Body AS _msg FROM t WHERE x FORMAT JSONEachRow"

WINSRC="$TMP/win_src.json"
printf '{"_msg":"a"}\n{"_msg":"b"}\n' > "$WINSRC"
set_stub_queue "200|0.01|30|$WINSRC"
build_scan_window victorialogs POST "http://x" "* | fields _msg, service.name | limit $SCAN_LIMIT"
check "build_scan_window: WINDOW_ROWS from a 2xx response" "$WINDOW_ROWS" "2"
check "build_scan_window: WINDOW_KEYS_FILE exists" "$([[ -f "$WINDOW_KEYS_FILE" ]] && echo yes || echo no)" "yes"
check "build_scan_window: WINDOW_HASH non-empty" "$([[ -n "$WINDOW_HASH" ]] && echo yes || echo no)" "yes"

set_stub_queue "500|0.01|0|"
build_scan_window victorialogs POST "http://x" "* | fields _msg, service.name | limit $SCAN_LIMIT"
check "build_scan_window: non-2xx -> WINDOW_ROWS empty (identity fallback)" "$WINDOW_ROWS" ""
check "build_scan_window: non-2xx -> WINDOW_KEYS_FILE empty" "$WINDOW_KEYS_FILE" ""

BADSRC="$TMP/bad_src.txt"
printf 'not json at all\n' > "$BADSRC"
set_stub_queue "200|0.01|10|$BADSRC"
build_scan_window victorialogs POST "http://x" "* | fields _msg, service.name | limit $SCAN_LIMIT"
check "build_scan_window: unparseable -> WINDOW_ROWS empty" "$WINDOW_ROWS" ""
check "build_scan_window: unparseable -> WINDOW_KEYS_FILE removed (empty)" "$WINDOW_KEYS_FILE" ""

# =============================================================================
# R-4: window_bounds — must emit ONLY ns bounds (no second-granularity pair,
# the source of the CH/LogsQL skew this round fixed), from one time.time()
# call so ens-sns is exactly secs*1e9 (not off by whatever a second call to
# time.time() would drift).
# =============================================================================
read -r wb_sns wb_ens <<<"$(window_bounds 3600)"
check "window_bounds: exactly 2 fields emitted (no ss/es)" "$(window_bounds 3600 | wc -w | tr -d ' ')" "2"
check "window_bounds: sns is an integer" "$([[ "$wb_sns" =~ ^[0-9]+$ ]] && echo yes || echo no)" "yes"
check "window_bounds: ens is an integer" "$([[ "$wb_ens" =~ ^[0-9]+$ ]] && echo yes || echo no)" "yes"
check "window_bounds: ens - sns == secs * 1e9 (one time.time() call)" "$((wb_ens - wb_sns))" "3600000000000"

echo "measure_query_test.sh: $pass passed, $fail failed" >&2
[[ "$fail" == 0 ]]
