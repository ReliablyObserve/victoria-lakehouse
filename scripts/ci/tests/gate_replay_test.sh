#!/usr/bin/env bash
# Tests for scripts/ci/gate_replay.sh itself: the replay must notice a wrong verdict, a wrong
# set of failing rules, a missing report fragment and an unreachable recorded commit, and it
# must never turn an unreachable merged commit into a pass.
#
# The gate under replay is a stub in a tiny fixture repository (--gate-dir), so this runs in
# seconds and tests the harness, not the real gate; the real gate is exercised by the replay
# itself (scripts/ci/gate_replay.sh over scripts/ci/tests/gate_replay_expected.txt).
#
# Usage: bash scripts/ci/tests/gate_replay_test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPLAY="$SCRIPT_DIR/../gate_replay.sh"
command -v go >/dev/null 2>&1 || { echo "skip - gate replay self-test (no go toolchain)"; exit 0; }
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
pass=0; fail=0

# The fixture: the files the synthetic cases edit, a stub testlinks (the replay builds it), and a
# stub gate that fails Rule 1 when anything under internal/ changed and passes otherwise.
FX="$T/fixture"
mkdir -p "$FX/scripts/ci" "$FX/tests/conformance/cmd/testlinks" "$FX/tests/parity" "$FX/internal/a" "$FX/docs"
(
  cd "$FX" || exit 1
  printf 'module fixture\n\ngo 1.22.0\n' > go.mod
  printf 'FROM golang:1.22.0-alpine3.20 AS builder\nRUN make\n' > Dockerfile.logs
  printf 'services:\n  build:\n    image: golang:1.22.0\n' > tests/parity/docker-compose.yml
  printf 'package a\n\nfunc F() {}\n' > internal/a/a.go
  printf 'package parity\n\nimport "testing"\n\nfunc TestP(t *testing.T) {}\n' > tests/parity/p_test.go
  printf 'package main\n\nfunc main() {}\n' > tests/conformance/cmd/testlinks/main.go
  printf 'TestP 3\n' > tests/parity/lock_cells.txt
  printf '| id | note |\n' > docs/parity-and-gaps.md
  cat > scripts/ci/check_registry_touch.sh <<'STUB'
#!/usr/bin/env bash
# stub gate: Rule 1 fails when internal/ changed; otherwise the PR passes as a toolchain bump
files=$(git diff --name-only "$1" "${HEAD_REV:-HEAD}")
if grep -q '^internal/' <<<"$files"; then
  printf '## Registry gate: FAIL\n\n| Rule | Verdict | Detail |\n|---|---|---|\n| Rule 1: stub | FAIL | internal changed |\n' >> "$GITHUB_STEP_SUMMARY"
  exit 1
fi
printf '## Registry gate: PASS\n\nPR kind: `toolchain-only`\n\ntitle=%s subject=%s\na|b piped\n\n| Rule | Verdict | Detail |\n|---|---|---|\n| Rule 3: stub | PASS | ok |\n' "$PR_TITLE" "$(git log -1 --format=%s "${HEAD_REV:-HEAD}")" >> "$GITHUB_STEP_SUMMARY"
exit 0
STUB
  git init -q -b main && git config user.email t@e && git config user.name t && git config commit.gpgsign false
  git add -A && git commit -q -m fixture
) >/dev/null 2>&1

# check <name> <want: 0 = replay succeeds, 1 = replay fails> <expected fragment of the output or ""> <table body> [replay args...]
check() {
  local name=$1 want=$2 frag=$3 table=$4; shift 4
  printf '%s\n' "$table" > "$T/table.txt"
  local out rc
  out=$(bash "$REPLAY" --gate-dir "$FX" --expected "$T/table.txt" "$@" 2>&1); rc=$?
  local ok=1
  if [[ $want -eq 0 ]]; then [[ $rc -eq 0 ]] || ok=0; else [[ $rc -ne 0 ]] || ok=0; fi
  [[ -z "$frag" ]] || grep -qF -- "$frag" <<<"$out" || ok=0
  if [[ $ok -eq 1 ]]; then echo "ok   - $name"; pass=$((pass + 1)); else echo "FAIL - $name (rc=$rc)"; echo "$out" | sed 's/^/       /' | head -20; fail=$((fail + 1)); fi
}

GOOD='tc    | synthetic | - | toolchain_bump | replay | PASS | - | toolchain-only |
prod  | synthetic | - | product_change_no_registry | replay | FAIL | Rule 1 | - |
lcode | synthetic | - | lock_code_edit | replay | PASS | - | Rule 3: stub |
tc2   | synthetic | - | toolchain_bump | replay | PASS | - | - |'
GONE='gone | main | 0000000000000000000000000000000000000001 | 0000000000000000000000000000000000000002 | x | PASS | - | - |'
OPEN='pr9999 | open | 0000000000000000000000000000000000000001 | 0000000000000000000000000000000000000002 | x | PASS | - | - |'

check "the recorded expectations replay clean" 0 "4 case(s), 0 mismatch(es)" "$GOOD"
check "a wrong verdict is a mismatch" 1 "WRONG VERDICT" "tc | synthetic | - | toolchain_bump | replay | FAIL | Rule 1 | - |"
check "a right verdict for the wrong reason is a mismatch" 1 "WRONG RULES" "prod | synthetic | - | product_change_no_registry | replay | FAIL | Rule 3 | - |"
check "a missing report fragment is a mismatch" 1 "REPORT LACKS" "tc | synthetic | - | toolchain_bump | replay | PASS | - | no such fragment |"
check "an unknown synthetic case is a setup error, not a pass" 1 "SETUP ERROR" "x | synthetic | - | no_such_case | replay | PASS | - | - |"
check "a merged commit that cannot be fetched fails" 1 "MISSING" "$GONE"
check "an open pull request head that cannot be fetched fails without the flag" 1 "MISSING" "$OPEN"
check "an unreachable open head is UNAVAILABLE with the flag, and the rest still replays" 0 "1 unavailable" "$OPEN
$GOOD" --allow-unavailable-open
check "the flag never excuses a merged commit" 1 "MISSING" "$GONE" --allow-unavailable-open
check "an empty table replays nothing and fails" 1 "nothing replayed" "# only a comment"
check "--only replays the matching cases only" 0 "1 case(s), 0 mismatch(es)" "$GOOD" --only '^prod$'

# the title column is the PR title the gate sees and, for a synthetic case, the commit subject
check "a synthetic case's title reaches the gate as the PR title and the commit subject" 0 "1 case(s), 0 mismatch(es)" \
  "t | synthetic | - | toolchain_bump | replay | PASS | - | title=build(deps): bump x subject=build(deps): bump x | build(deps): bump x"
check "without a title the default is replay: <case>" 0 "1 case(s), 0 mismatch(es)" \
  "t | synthetic | - | toolchain_bump | replay | PASS | - | title=replay: toolchain_bump subject=replay: toolchain_bump |"
# a literal bar in a cell is written \|
check "an escaped bar inside the report fragment is matched" 0 "1 case(s), 0 mismatch(es)" \
  'b | synthetic | - | toolchain_bump | replay | PASS | - | a\|b piped | replay: b'
# the table is strict: a wrong column count is an error, never a silent shift
check "a row with eight columns is refused" 1 "malformed row" "t | synthetic | - | toolchain_bump | replay | PASS | - | toolchain-only"
check "a row with ten columns is refused" 1 "malformed row" "t | synthetic | - | toolchain_bump | replay | PASS | - | - | a | b"
check "an unescaped bar in a cell is a ten-column row" 1 "malformed row" "t | synthetic | - | toolchain_bump | replay | PASS | - | a|b | title"

# shards partition the table: together they replay every case exactly once
printf '%s\n' "$GOOD" > "$T/shard.txt"
out0=$(bash "$REPLAY" --gate-dir "$FX" --expected "$T/shard.txt" --shard 0/2 2>&1); rc0=$?
out1=$(bash "$REPLAY" --gate-dir "$FX" --expected "$T/shard.txt" --shard 1/2 2>&1); rc1=$?
n0=$(grep -c '^[a-z0-9]* *\(PASS\|FAIL\)' <<<"$out0"); n1=$(grep -c '^[a-z0-9]* *\(PASS\|FAIL\)' <<<"$out1")
if [[ $rc0 -eq 0 && $rc1 -eq 0 && $((n0 + n1)) -eq 4 && $n0 -eq 2 && $n1 -eq 2 ]]; then echo "ok   - --shard 0/2 and 1/2 replay disjoint halves of the table ($n0 + $n1 cases)"; pass=$((pass + 1)); else echo "FAIL - shards ($rc0/$rc1: $n0 + $n1 cases)"; fail=$((fail + 1)); fi
bash "$REPLAY" --gate-dir "$FX" --expected "$T/shard.txt" --shard 2/2 >/dev/null 2>&1; rc=$?
if [[ $rc -eq 2 ]]; then echo "ok   - a bad --shard is a usage error"; pass=$((pass + 1)); else echo "FAIL - bad shard (rc=$rc)"; fail=$((fail + 1)); fi

# the replay never touches the global git configuration
if ! git config --global --get-all safe.directory 2>/dev/null | grep -qx '\*'; then echo "ok   - the replay leaves safe.directory alone"; pass=$((pass + 1)); else echo "FAIL - safe.directory '*' is set globally"; fail=$((fail + 1)); fi

echo
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
