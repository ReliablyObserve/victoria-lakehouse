#!/usr/bin/env bash
# Self-test for scripts/ci/check_coverage.sh: how it reads `go test -cover`
# lines. Covers a nested package below the floor (the parquets3 case the gate
# skipped), the explicit exclusions, a required package that is missing,
# lines it must ignore, and bad invocation.
#
# Usage: scripts/ci/tests/check_coverage_test.sh
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1

GATE="scripts/ci/check_coverage.sh"
M="github.com/ReliablyObserve/victoria-lakehouse"
pass=0 fail=0

# run <input> [gate args...] — sets $out and $rc.
run() {
  local input="$1"; shift
  out="$(printf '%s' "$input" | "$GATE" "$@" 2>&1)"
  rc=$?
}
check_rc() {
  if [[ "$2" == "$3" ]]; then pass=$((pass + 1)); return; fi
  fail=$((fail + 1)); printf 'FAIL: %s\n  expected exit %s, got %s\n  output: %s\n' "$1" "$3" "$2" "$out" >&2
}
check_contains() {
  if [[ "$2" == *"$3"* ]]; then pass=$((pass + 1)); return; fi
  fail=$((fail + 1)); printf 'FAIL: %s\n  expected substring: %s\n  actual: %s\n' "$1" "$3" "$2" >&2
}
check_not_contains() {
  if [[ "$2" != *"$3"* ]]; then pass=$((pass + 1)); return; fi
  fail=$((fail + 1)); printf 'FAIL: %s\n  unexpected substring: %s\n  actual: %s\n' "$1" "$3" "$2" >&2
}
# cov <path> <pct>... — one `go test -cover` line per pair, newline-separated.
cov() {
  while [[ $# -ge 2 ]]; do
    printf 'ok  \t%s/internal/%s\t1.2s\tcoverage: %s%% of statements\n' "$M" "$1" "$2"
    shift 2
  done
}

run "$(cov cache 95.0 storage/parquets3 91.3)"
check_rc "packages above the floor pass" "$rc" 0
check_contains "reports how many were checked" "$out" "All 2 packages"

# The defect: internal/storage/parquets3 resolved to "storage" and was skipped.
run "$(cov cache 95.0 storage/parquets3 89.9)"
check_rc "a nested package below the floor fails" "$rc" 1
check_contains "names the full path" "$out" "Coverage for storage/parquets3 is 89.9% (minimum: 90%)"

run "$(cov storage 80.0)"
check_rc "the parent of a nested package is checked on its own" "$rc" 1
check_contains "names it" "$out" "Coverage for storage is 80.0%"

run "$(cov storage/parquets3 50.0 testutil/storageinvariants 10.0)" -x 'testutil' -x 'testutil/*'
check_rc "an exclusion matches the whole path, not the first segment" "$rc" 1
check_contains "the nested package is still checked" "$out" "storage/parquets3 is 50.0%"
check_not_contains "the excluded subtree is not reported" "$out" "testutil"

run "$(cov testutil 10.0 testutil/storageinvariants 10.0 cache 99.0)" -x 'testutil' -x 'testutil/*'
check_rc "excluded paths are skipped" "$rc" 0

run "$(cov testutil/storageinvariants 10.0)" -x 'testutil'
check_rc "excluding a parent does not exclude its subtree" "$rc" 1

run "$(cov cache 95.0)" -r storage/parquets3
check_rc "a required package that was not measured fails" "$rc" 1
check_contains "says it was not measured" "$out" "Coverage for storage/parquets3 was not measured"

run "$(cov storage/parquets3 91.3)" -r storage/parquets3
check_rc "a required package that was measured passes" "$rc" 0

run "$(cov cache 90.0)"
check_rc "exactly the floor passes" "$rc" 0
run "$(cov cache 89.99)"
check_rc "just below the floor fails" "$rc" 1
run "$(cov cache 75.0)" -t 70
check_rc "the threshold is configurable" "$rc" 0

run "$(printf '?   \t%s/internal/x\t[no test files]\n' "$M")
$(printf 'ok  \t%s/cmd/lakehouse-logs\t1s\tcoverage: 5.0%% of statements\n' "$M")
$(printf 'ok  \t%s/internal/cache\t1s\tcoverage: [no statements]\n' "$M")"
check_rc "lines without a measurement or outside internal/ are ignored" "$rc" 0
check_contains "nothing counted" "$out" "All 0 packages"

run "" 
check_rc "empty input passes without -r" "$rc" 0
run "" -r cache
check_rc "empty input fails when something is required" "$rc" 1

out="$("$GATE" -t abc </dev/null 2>&1)"; rc=$?
check_rc "a non-numeric threshold exits 2" "$rc" 2
out="$("$GATE" -z </dev/null 2>&1)"; rc=$?
check_rc "an unknown option exits 2" "$rc" 2

printf 'check_coverage_test: %d passed, %d failed\n' "$pass" "$fail"
[[ "$fail" -eq 0 ]]
