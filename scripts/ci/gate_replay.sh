#!/usr/bin/env bash
# Replays the registry gate over recorded pull requests and over synthetic pull
# requests, and compares each verdict with scripts/ci/tests/gate_replay_expected.txt.
# It is the automated check that a change to the gate (or to the registry rules it
# enforces) does not accept what it must reject or reject what it must accept: the
# gate is judged on real history, not only on its own unit tests.
#
#   gate_replay.sh [--expected FILE] [--only REGEX] [--shard I/N] [--allow-unavailable-open]
#                  [--report-only] [--gate-dir DIR]
#
#   --only REGEX    replay only the cases whose id matches.
#   --shard I/N     replay every N-th case starting at I (0-based), so CI can split the table
#                   over N jobs; the shards together cover every case exactly once.
#   --gate-dir DIR   the gate code under test (default: this checkout). Pointing it at a
#                    checkout of an older revision prints that gate's verdicts, which is how
#                    the "old gate" column of a gate PR is produced (use with --report-only).
#   --report-only    print the verdicts, compare nothing, exit 0.
#   --allow-unavailable-open   a recorded OPEN pull request whose head commit can no longer
#                    be fetched (force-pushed, branch deleted) is reported as UNAVAILABLE and
#                    does not fail the run. Recorded merged commits and synthetic cases are
#                    never allowed to be unavailable.
#
# Table format (one case per line, '|' separated, '#' starts a comment):
#   id | kind | base | head | author | verdict | failing rules | report must contain | title
#     kind      main       a squash commit of main: head is the commit, base its parent
#               open       a pull request that was open when recorded: head is its head sha
#               synthetic  built in a scratch clone by the function syn_<head> of this script
#     verdict   PASS or FAIL
#     failing   the exact set of rules that fail, comma separated ("-" for none): the rows of
#               the report whose verdict is FAIL, named by the text before the first colon
#   Pinned by sha: the replay is deterministic.
#
# Needs git, python3, go (to build tests/conformance/cmd/testlinks), and for the recorded
# pull requests a clone with the history (fetch-depth: 0) and read access to origin.
set -uo pipefail
export LC_ALL=C

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
GATE_DIR=$ROOT
EXPECTED=$HERE/tests/gate_replay_expected.txt
ONLY=""
SHARD_I=0
SHARD_N=1
ALLOW_UNAVAILABLE_OPEN=""
REPORT_ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
  --expected) EXPECTED=$2; shift 2 ;;
  --only) ONLY=$2; shift 2 ;;
  --shard) [[ "$2" =~ ^([0-9]+)/([0-9]+)$ && ${BASH_REMATCH[2]} -gt 0 && ${BASH_REMATCH[1]} -lt ${BASH_REMATCH[2]} ]] || { echo "bad --shard '$2' (want I/N with I < N)" >&2; exit 2; }
    SHARD_I=${BASH_REMATCH[1]}; SHARD_N=${BASH_REMATCH[2]}; shift 2 ;;
  --allow-unavailable-open) ALLOW_UNAVAILABLE_OPEN=1; shift ;;
  --report-only) REPORT_ONLY=1; shift ;;
  --gate-dir) GATE_DIR=$(cd "$2" && pwd); shift 2 ;;
  *) echo "usage: gate_replay.sh [--expected FILE] [--only REGEX] [--shard I/N] [--allow-unavailable-open] [--report-only] [--gate-dir DIR]" >&2; exit 2 ;;
  esac
done
[[ -f "$EXPECTED" ]] || { echo "no expected table at $EXPECTED" >&2; exit 2; }
[[ -f "$GATE_DIR/scripts/ci/check_registry_touch.sh" ]] || { echo "no gate at $GATE_DIR" >&2; exit 2; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

TL="$WORK/testlinks"
(cd "$GATE_DIR" && GOWORK=off go build -o "$TL" ./tests/conformance/cmd/testlinks) || { echo "cannot build testlinks from $GATE_DIR" >&2; exit 2; }

trim() { local s=$1; s="${s#"${s%%[![:space:]]*}"}"; s="${s%"${s##*[![:space:]]}"}"; printf '%s' "$s"; }

# have <repo> <sha>: succeeds when the commit exists in the repo.
have() { git -C "$1" cat-file -e "$2^{commit}" 2>/dev/null; }

# ensure <repo> <id> <sha>: fetches the commit when it is missing; for a pull request tries its ref.
ensure() {
  local repo=$1 id=$2 sha=$3
  have "$repo" "$sha" && return 0
  git -C "$repo" fetch -q --no-tags origin "$sha" 2>/dev/null && have "$repo" "$sha" && return 0
  if [[ "$id" =~ ^pr([0-9]+)$ ]]; then
    git -C "$repo" fetch -q --no-tags origin "refs/pull/${BASH_REMATCH[1]}/head" 2>/dev/null
  fi
  have "$repo" "$sha"
}

# run_gate <repo> <base> <head> <author> <title> <report-file> <log-file>: the gate as CI runs it.
run_gate() {
  local repo=$1 base=$2 head=$3 author=$4 title=$5 report=$6 log=$7
  : > "$report"
  (cd "$repo" && env -i PATH="$PATH" HOME="${HOME:-/tmp}" TMPDIR="$WORK" LANG=C.UTF-8 GOWORK=off GOTOOLCHAIN=local \
    GIT_TERMINAL_PROMPT=0 TESTLINKS_BIN="$TL" HEAD_REV="$head" GITHUB_STEP_SUMMARY="$report" \
    PR_AUTHOR="$author" PR_TITLE="$title" bash "$GATE_DIR/scripts/ci/check_registry_touch.sh" "$base") > "$log" 2>&1
}

# failing_rules <report> <log>: the failing rules, sorted, comma separated ("-" for none).
# The new gate names them in its report table; an older gate has no table, so its
# ::error:: headlines stand in.
failing_rules() {
  local report=$1 log=$2 out
  out=$(awk -F'|' '$3 ~ /^ *FAIL *$/ { n=$2; gsub(/^ +| +$/, "", n); sub(/:.*/, "", n); print n }' "$report" | sort -u | paste -s -d, -)
  if [[ -z "$out" ]] && grep -q '::error::' "$log"; then
    out=$(grep -m1 '::error::' "$log" | cut -c1-60)
  fi
  printf '%s' "${out:--}"
}

# ---------------------------------------------------------------- synthetic ---
# Each syn_<name> edits the scratch clone's working tree on top of the base commit.
# A case whose edit changes nothing is a setup error, not a pass.

# bump_go: a Go toolchain bump as PR #491 made it (go directive, Dockerfile builder tag, compose image tag).
bump_go() {
  python3 - <<'PY'
import re
def sub(path, pat, fn):
    s = open(path, encoding="utf-8").read()
    n = re.sub(pat, fn, s, count=1, flags=re.M)
    assert n != s, path
    open(path, "w", encoding="utf-8").write(n)
bump = lambda m: m.group(1) + str(int(m.group(2)) + 1)
sub("go.mod", r"^(go \d+\.\d+\.)(\d+)$", bump)
sub("Dockerfile.logs", r"(golang:\d+\.\d+\.)(\d+)", bump)
sub("tests/parity/docker-compose.yml", r"(image: golang:\d+\.\d+\.)(\d+)", bump)
PY
}

syn_toolchain_bump() { bump_go; }

syn_toolchain_bump_and_base_image() {
  bump_go
  python3 - <<'PY'
import re
s = open("Dockerfile.logs", encoding="utf-8").read()
n = re.sub(r"golang:[^ \t\n]+", "debian:12", s, count=1)
assert n != s
open("Dockerfile.logs", "w", encoding="utf-8").write(n)
PY
}

syn_toolchain_bump_and_lower_floor() {
  bump_go
  python3 - <<'PY'
import re
s = open("tests/parity/lock_cells.txt", encoding="utf-8").read()
for m in re.finditer(r"^(\S+)\s+(\d+)\b", s, re.M):
    if not m.group(1).startswith("#") and int(m.group(2)) > 1:
        n = s[:m.start(2)] + str(int(m.group(2)) - 1) + s[m.end(2):]
        open("tests/parity/lock_cells.txt", "w", encoding="utf-8").write(n)
        raise SystemExit(0)
raise SystemExit("no lock floor above 1 to lower")
PY
}

# A parity-fix trigger (a divergence born resolved in docs/) rides in a file the toolchain-only
# kind allows: Rule 1 is waived, Rule 3 must still demand the locks.
syn_toolchain_bump_and_resolve_divergence() {
  bump_go
  printf '\n| **Replay divergence** | **Resolved** | n/a | a divergence marked resolved without a lock |\n' >> docs/parity-and-gaps.md
}

syn_toolchain_bump_and_product_change() {
  bump_go
  local f
  f=$(git ls-files 'internal/*.go' | grep -v '_test\.go$' | head -1)
  printf '\n// replay: a product edit riding with a toolchain bump\n' >> "$f"
}

syn_delete_lock_reference() {
  python3 - <<'PY'
import glob, re, sys, yaml
for path in sorted(glob.glob("tests/conformance/registry/rows/**/*.yaml", recursive=True)):
    text = open(path, encoding="utf-8").read()
    rows = yaml.safe_load(text) or []
    for r in rows if isinstance(rows, list) else []:
        cmp_ = (r.get("compare") or {}).get("type") if isinstance(r.get("compare"), dict) else r.get("compare")
        refs = [t for t in ((r.get("refs") or {}).get("tests") or []) if "#" in t]
        if r.get("expect") == "pass" and cmp_ in ("exact-json", "count", "trace", "ndjson-multiset") and refs:
            line = re.compile(r"^[ \t]*-[ \t]+" + re.escape(refs[0]) + r"[ \t]*\n", re.M)
            n = line.sub("", text, count=1)
            if n != text:
                open(path, "w", encoding="utf-8").write(n)
                sys.exit(0)
sys.exit("no lock row with a block-style test reference found")
PY
}

syn_product_change_no_registry() {
  local f
  f=$(git ls-files 'internal/*.go' | grep -v '_test\.go$' | head -1)
  printf '\n// replay: a product edit without a registry change\n' >> "$f"
}

syn_unlinked_new_test() {
  local d pkg
  d=$(git ls-files 'internal/*.go' | grep -v '_test\.go$' | head -1 | xargs dirname)
  pkg=$(grep -h -m1 '^package ' "$d"/*.go | grep -v '_test' | head -1 | awk '{print $2}')
  printf 'package %s\n\nimport "testing"\n\nfunc TestGateReplayUnlinkedNewTest(t *testing.T) {}\n' "$pkg" > "$d/zz_gate_replay_test.go"
}

syn_lock_code_edit() {
  local f
  f=$(git ls-files 'tests/parity/*_test.go' | head -1)
  printf '\n// replay: an edit of a file in a lock package\n' >> "$f"
}

# run_synthetic <name> <author> <report> <log>: builds the case in a scratch clone and runs the gate.
run_synthetic() {
  local name=$1 author=$2 report=$3 log=$4 clone="$WORK/syn.$1" base
  base=$(git -C "$GATE_DIR" rev-parse HEAD 2>/dev/null) || return 3
  git clone -q --shared --no-checkout "$GATE_DIR" "$clone" 2>/dev/null || return 3
  (
    cd "$clone" || exit 1
    git branch -q replay-base "$base" && git checkout -q -b replay-pr replay-base || exit 1
    git config user.email replay@example.com; git config user.name replay; git config commit.gpgsign false
    # the working tree of the gate under test may hold uncommitted edits: the clone starts at its HEAD
    declare -F "syn_$name" >/dev/null || { echo "no synthetic case syn_$name" >&2; exit 3; }
    "syn_$name" || exit 3
    git add -A
    git diff --cached --quiet && { echo "synthetic case $name changed nothing" >&2; exit 3; }
    git commit -q -m "replay: $name" || exit 3
  ) 2>"$log" || { cat "$log" >&2; return 3; }
  run_gate "$clone" replay-base HEAD "$author" "replay: $name" "$report" "$log"
  local rc=$?
  rm -rf "$clone"
  return $rc
}

# -------------------------------------------------------------------- main ---
checked=0; mismatched=0; unavailable=0; row=-1
TABLE=""
SUMMARY="$WORK/summary.md"
printf '| case | old/new verdict | failing rules | expected | result |\n|---|---|---|---|---|\n' > "$SUMMARY"

while IFS= read -r raw || [[ -n "$raw" ]]; do
  line=${raw%%#*}
  [[ -n "$(trim "$line")" ]] || continue
  IFS='|' read -r id kind base head author verdict failing contains title <<< "$line"
  id=$(trim "$id"); kind=$(trim "$kind"); base=$(trim "$base"); head=$(trim "$head"); author=$(trim "$author")
  verdict=$(trim "$verdict"); failing=$(trim "$failing"); contains=$(trim "$contains"); title=$(trim "${title:-}")
  [[ -z "$ONLY" || "$id" =~ $ONLY ]] || continue
  row=$((row + 1))
  [[ $((row % SHARD_N)) -eq $SHARD_I ]] || continue
  report="$WORK/report.$id.md"; log="$WORK/log.$id.txt"
  rc=0
  case "$kind" in
  main|open)
    if ! { ensure "$ROOT" "$id" "$base" && ensure "$ROOT" "$id" "$head"; }; then
      if [[ "$kind" == open && -n "$ALLOW_UNAVAILABLE_OPEN" ]]; then
        printf '%-34s UNAVAILABLE (head %s can no longer be fetched; not verified)\n' "$id" "${head:0:10}"
        printf '| %s | UNAVAILABLE | - | %s %s | not verified |\n' "$id" "$verdict" "$failing" >> "$SUMMARY"
        unavailable=$((unavailable + 1)); continue
      fi
      printf '%-34s MISSING (cannot fetch %s..%s)\n' "$id" "${base:0:10}" "${head:0:10}"
      printf '| %s | MISSING | - | %s %s | FAIL |\n' "$id" "$verdict" "$failing" >> "$SUMMARY"
      mismatched=$((mismatched + 1)); continue
    fi
    run_gate "$ROOT" "$base" "$head" "$author" "$title" "$report" "$log" || rc=$?
    ;;
  synthetic)
    run_synthetic "$head" "${author:-replay}" "$report" "$log" || rc=$?
    if [[ $rc -eq 3 ]]; then
      printf '%-34s SETUP ERROR\n' "$id"; sed 's/^/    /' "$log" | head -5
      printf '| %s | SETUP ERROR | - | %s %s | FAIL |\n' "$id" "$verdict" "$failing" >> "$SUMMARY"
      mismatched=$((mismatched + 1)); continue
    fi
    ;;
  *) echo "bad kind '$kind' for $id" >&2; exit 2 ;;
  esac
  got=PASS; [[ $rc -eq 0 ]] || got=FAIL
  gotfail=$(failing_rules "$report" "$log")
  status=ok
  if [[ -z "$REPORT_ONLY" ]]; then
    [[ "$got" == "$verdict" ]] || status="WRONG VERDICT"
    [[ "$status" != ok || "$gotfail" == "$failing" ]] || status="WRONG RULES"
    if [[ "$status" == ok && -n "$contains" && "$contains" != "-" ]] && ! grep -qF -- "$contains" "$report"; then
      status="REPORT LACKS '$contains'"
    fi
  else
    status="-"
  fi
  checked=$((checked + 1))
  [[ "$status" == ok || "$status" == "-" ]] || mismatched=$((mismatched + 1))
  printf '%-34s %-5s fails: %-44s expected: %-5s %-30s %s\n' "$id" "$got" "$gotfail" "$verdict" "${failing}" "$status"
  printf '| %s | %s | %s | %s %s | %s |\n' "$id" "$got" "$gotfail" "$verdict" "$failing" "$status" >> "$SUMMARY"
  if [[ "$status" != ok && "$status" != "-" ]]; then
    sed 's/^/    | /' "$log" | head -25
  fi
done < "$EXPECTED"

echo
echo "replayed $checked case(s), $mismatched mismatch(es), $unavailable unavailable"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  { echo "## Gate replay"; echo; cat "$SUMMARY"; echo; echo "$checked replayed, $mismatched mismatched, $unavailable unavailable (open PR heads that can no longer be fetched)."; } >> "$GITHUB_STEP_SUMMARY"
fi
[[ -n "$REPORT_ONLY" ]] && exit 0
[[ $checked -gt 0 ]] || { echo "nothing replayed" >&2; exit 1; }
[[ $mismatched -eq 0 ]]
