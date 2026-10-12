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
# Table format (one case per line, '|' separated, '#' starts a comment; a literal '|' inside a
# cell is written '\|'). Exactly nine columns: a row with another count is an error (exit 2),
# never silently shifted:
#   id | kind | base | head | author | verdict | failing rules | report must contain | title
#     kind      main       a squash commit of main: head is the commit, base its parent
#               open       a pull request that was open when recorded: head is its head sha
#               synthetic  built in a scratch clone by the function syn_<head> of this script
#     title     the pull request title; for a synthetic case it is also the subject of the one
#               commit (default "replay: <head>"), so a case can wear the title an attacker would
#               choose (the gate decides from the diff and must not care)
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

# --- smuggles found by the independent review: a line break the gate's reader saw and Docker/Go did not.
# cr_fips: the Go tag moves and the LF between a comment and `ARG FIPS=0` becomes a CR (Docker reads one
# comment line; the FIPS build arg is gone). cr_replace_tc: a go.mod `replace` is hidden behind a CR
# in a comment. u2028_replace_dep / cr_replace_dep: the same under a build(deps) title with a
# dependency version bump. tc_unidigit: an Arabic-Indic digit in the golang tag.
hide_replace() { # hide_replace <separator escape as python source>
  python3 - "$1" <<'PY'
import re, sys
sep = sys.argv[1].encode().decode("unicode_escape")
s = open("go.mod", encoding="utf-8", newline="").read()
n = re.sub(r"^replace github.com/VictoriaMetrics/VictoriaLogs ", "// local tree" + sep + "replace github.com/VictoriaMetrics/VictoriaLogs ", s, count=1, flags=re.M)
assert n != s
open("go.mod", "w", encoding="utf-8", newline="").write(n)
PY
}

bump_dep() { # a non-critical require version
  python3 - <<'PY'
import re
s = open("go.mod", encoding="utf-8", newline="").read()
n = re.sub(r"^(\tgolang\.org/x/net v[0-9]+\.[0-9]+\.)([0-9]+)", lambda m: m.group(1) + str(int(m.group(2)) + 1), s, count=1, flags=re.M)
assert n != s
open("go.mod", "w", encoding="utf-8", newline="").write(n)
PY
}

# bump_go_min: the Go bump without the parity compose file, which the gate of main treats as lock code
# (so main rejects any bump for that reason); with only go.mod and Dockerfile.logs the verdict of
# every gate generation below is decided by Rule 1, which is what the smuggles attack.
bump_go_min() {
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
PY
}

syn_cr_fips() {
  bump_go_min
  python3 - <<'PY'
s = open("Dockerfile.logs", encoding="utf-8", newline="").read()
n = s.replace("per deployment.\nARG FIPS=0", "per deployment.\rARG FIPS=0", 1)
assert n != s
open("Dockerfile.logs", "w", encoding="utf-8", newline="").write(n)
PY
}

syn_cr_replace_tc() { bump_go_min; hide_replace '\r'; }
syn_cr_replace_dep() { bump_dep; hide_replace '\r'; }
syn_u2028_replace_dep() { bump_dep; hide_replace '\u2028'; }
syn_dependency_bump() { bump_dep; }

syn_tc_unidigit() {
  bump_go_min
  python3 - <<'PY'
import re
s = open("Dockerfile.logs", encoding="utf-8", newline="").read()
n = re.sub(r"(golang:[0-9]+\.[0-9]+\.)[0-9]+", lambda m: m.group(1) + "\u0663", s, count=1)
assert n != s
open("Dockerfile.logs", "w", encoding="utf-8", newline="").write(n)
PY
}

# init_exit: a new test file in a lock package whose init() ends the test binary with success when the
# lock run's nonce is set, so every lock of the package "passes" without running.
syn_init_exit() {
  printf 'package parity\n\nimport "os"\n\nfunc init() {\n\tif os.Getenv("PARITY_LOCK_NONCE") != "" {\n\t\tos.Exit(0)\n\t}\n}\n' > tests/parity/zz_env_test.go
}

syn_lock_code_edit() {
  local f
  f=$(git ls-files 'tests/parity/*_test.go' | head -1)
  printf '\n// replay: an edit of a file in a lock package\n' >> "$f"
}

# --- the hardening of issue #502 (second independent review) ---------------------------------------
# link_test_in_feature <file#Test>: reference a new test from the first `tests:` list of ops.yaml, so
# Rule 2 holds and the case tests only what it names.
link_test_in_feature() {
  python3 - "$1" <<'PY'
import sys
p = "tests/conformance/registry/features/ops.yaml"
s = open(p, encoding="utf-8").read()
n = s.replace("  tests:\n", "  tests:\n    - " + sys.argv[1] + "\n", 1)
assert n != s
open(p, "w", encoding="utf-8").write(n)
PY
}

# warmup_test (M-A): a new func-only test file that sorts first and repoints the shared base URLs, so every
# later lock compares Lakehouse with itself. Neither failed nor listed before; now listed.
syn_warmup_test() {
  printf 'package parity\n\nimport "testing"\n\nfunc TestAAAWarmup(t *testing.T) {\n\tvlBaseURL = lhBaseURL\n\tvtBaseURL = lhtBaseURL\n}\n' > tests/parity/aaa_warmup_test.go
  link_test_in_feature tests/parity/aaa_warmup_test.go#TestAAAWarmup
}

# forged_transcript_exit (M-A): prints a forged lock-cells line and ends the binary with success.
syn_forged_transcript_exit() {
  printf 'package parity\n\nimport (\n\t"fmt"\n\t"os"\n\t"syscall"\n\t"testing"\n)\n\nfunc TestZZForged(t *testing.T) {\n\tfmt.Println("lock-cells", os.Getenv("PARITY_LOCK_NONCE"), "TestParity_Forged 50")\n\tsyscall.Exit(0)\n}\n' > tests/parity/zz_forged_test.go
  link_test_in_feature tests/parity/zz_forged_test.go#TestZZForged
}

# blank_import_hook (M-A): a new package whose init() ends the binary, blank-imported by a new test file.
syn_blank_import_hook() {
  mkdir -p tests/parityhook
  printf 'package parityhook\n\nimport "os"\n\nfunc init() {\n\tif os.Getenv("PARITY_LOCK_NONCE") != "" {\n\t\tos.Exit(0)\n\t}\n}\n' > tests/parityhook/hook.go
  printf 'package parity\n\nimport _ "github.com/ReliablyObserve/victoria-lakehouse/tests/parityhook"\n' > tests/parity/zz_hook_test.go
}

# cgo_constructor (M-A): a C constructor in a new non-test file of the lock package. Hard failure (Rule 3).
syn_cgo_constructor() {
  printf 'package parity\n\n/*\nvoid __attribute__((constructor)) boot(void) {}\n*/\nimport "C"\n' > tests/parity/zz_cgo.go
}

# allowlist_nel (M-B): a U+2028 inside an existing allowlist line hides a smuggled entry from the Go gate
# (it splits on LF) while the Python ratchet's splitlines() saw it.
syn_allowlist_nel() {
  python3 - <<'PY'
p = "tests/parity/known_failures.txt"
lines = open(p, encoding="utf-8", newline="").read().split("\n")
for i, ln in enumerate(lines):
    if ln.strip() and not ln.lstrip().startswith("#"):
        lines[i] = ln + " TestParity_Smuggled  # B9: smuggled behind a line separator"
        break
else:
    raise SystemExit("no allowlist entry to hide behind")
open(p, "w", encoding="utf-8", newline="").write("\n".join(lines))
PY
}

# fastjson_bump (M-C): valyala/fastjson parses ingest JSON; it was not on the critical list.
syn_fastjson_bump() {
  python3 - <<'PY'
import re
for f in ("go.mod", "lakehouse-traces/go.mod"):
    s = open(f, encoding="utf-8", newline="").read()
    n = re.sub(r"(github\.com/valyala/fastjson v[0-9]+\.[0-9]+\.)([0-9]+)", lambda m: m.group(1) + str(int(m.group(2)) + 1), s, count=1)
    assert n != s, f
    open(f, "w", encoding="utf-8", newline="").write(n)
PY
}

# toolchain_downgrade (minor 4): the Go directive and the builder image go BACK.
syn_toolchain_downgrade() {
  python3 - <<'PY'
import re
def sub(path, pat, repl):
    s = open(path, encoding="utf-8").read()
    n = re.sub(pat, repl, s, count=1, flags=re.M)
    assert n != s, path
    open(path, "w", encoding="utf-8").write(n)
sub("go.mod", r"^go \d+\.\d+\.\d+$", "go 1.21.0")
sub("Dockerfile.logs", r"golang:\d+\.\d+\.\d+", "golang:1.21.0")
PY
}

# dependency_new_require (minor 6): a newly required module is not a version bump.
syn_dependency_new_require() {
  python3 - <<'PY'
s = open("go.mod", encoding="utf-8", newline="").read()
n = s.replace("\tgolang.org/x/net ", "\tgolang.org/x/crypto v0.1.0 // indirect\n\tgolang.org/x/net ", 1)
assert n != s
open("go.mod", "w", encoding="utf-8", newline="").write(n)
PY
}

# loadtime_var_move (minor 5): a called package-level var moves to a new file of the same package.
syn_loadtime_var_move() {
  python3 - <<'PY'
p = "tests/parity/helpers.go"
s = open(p, encoding="utf-8").read()
line = '\tvlBaseURL  = envOrDefault("VL_BASE_URL", "http://localhost:39428")\n'
assert line in s
open(p, "w", encoding="utf-8").write(s.replace(line, "", 1))
# the file it leaves is built under `parity` (a //go:build line): the new one carries it too, so this stays a pure move
head = s.split("\n", 1)[0]
build = head + "\n\n" if head.startswith("//go:build ") else ""
open("tests/parity/urls.go", "w", encoding="utf-8").write(build + 'package parity\n\nvar vlBaseURL = envOrDefault("VL_BASE_URL", "http://localhost:39428")\n')
PY
}

# loadtime_file_rename (minor 5): the file that holds the load-time declarations is renamed.
syn_loadtime_file_rename() { git mv tests/parity/helpers.go tests/parity/harness_helpers.go; }

# path_with_newline (minor 1): a route registration in a path with a line break raised no signal.
syn_path_with_newline() {
  printf 'package stats\n\nfunc registerZZ(mux interface{ HandleFunc(string, any) }) { mux.HandleFunc("/zz", nil) }\n' > "internal/stats/zz"$'\n'"route.go"
}

# gate_file_listing (M-B/6): files that shape the verdict are listed as gate files (a pass, with the list).
syn_gate_file_listing() {
  printf '# replay: a note\n' >> scripts/ci/tests/gate_replay_expected.txt
  printf '\n# replay: a note\n' >> .github/CODEOWNERS
}

# --- the third review of the gate (symlinks, alias swaps, packages a lock package imports, min-pass) ---
# MAJOR-1. The target of every link is a docs file holding what a lock-silencing init() would hold; `go test`
# compiles a linked .go file and no lock rule can read it.
link_target() {
  printf '//go:build e2e\n\npackage e2e\n\nimport "os"\n\nfunc init() {\n\tif os.Getenv("LH_NONCE") != "" {\n\t\tos.Exit(0)\n\t}\n}\n' > docs/zz.txt
}
syn_s_newlink_e2e_nontest() { link_target; ln -s ../../docs/zz.txt tests/e2e/zz_link.go; }
syn_s_newlink_parity() { link_target; ln -s ../../docs/zz.txt tests/parity/zz_link.go; }
syn_s_newlink_product_lockpkg() { link_target; ln -s ../../../docs/zz.txt internal/storage/parquets3/zz_link.go; }
# an existing lock-package file replaced by a link to a copy of itself (a blob that is the link target, not Go)
syn_s_replace_lockfile() { cp tests/parity/helpers.go docs/zz.txt; rm tests/parity/helpers.go; ln -s ../../docs/zz.txt tests/parity/helpers.go; }
syn_s_symlinked_test() { link_target; ln -s ../../docs/zz.txt tests/parity/zz_link_test.go; }
syn_s_symlink_dir() { ln -s ../../docs tests/e2e/zzdir; }
syn_s_symlink_docs_only() { ln -s ../README.md docs/zz_link.md; }

# MAJOR-2. alias_attacker: a package whose function ends the test binary, imported under the name `schema`.
mk_evil_schema() {
  mkdir -p tests/evilschema
  printf 'package evilschema\n\nimport "os"\n\ntype LogRow struct{}\n\nfunc EstimateRawBytesLogs([]LogRow) int { os.Exit(0); return 0 }\n' > tests/evilschema/evil.go
}
drop_fixed_log_row_bytes() {
  python3 - <<'PY2'
p = "internal/storage/parquets3/raw_bytes_test.go"
s = open(p, encoding="utf-8").read()
line = "var fixedLogRowBytes = schema.EstimateRawBytesLogs([]schema.LogRow{{}})\n"
assert line in s
open(p, "w", encoding="utf-8").write(s.replace(line, "", 1))
PY2
}
# the reviewer's m_alias_swap: the declaration moves to a new file where `schema` is another package
syn_m_alias_swap() {
  mk_evil_schema; drop_fixed_log_row_bytes
  printf 'package parquets3\n\nimport schema "github.com/ReliablyObserve/victoria-lakehouse/tests/evilschema"\n\nvar fixedLogRowBytes = schema.EstimateRawBytesLogs([]schema.LogRow{{}})\n' > internal/storage/parquets3/zz_moved_test.go
}
# ... and with no alias at all: the package clause of the repository package names it
syn_m_alias_noalias() {
  mkdir -p tests/evilschema
  printf 'package schema\n\nimport "os"\n\ntype LogRow struct{}\n\nfunc EstimateRawBytesLogs([]LogRow) int { os.Exit(0); return 0 }\n' > tests/evilschema/evil.go
  drop_fixed_log_row_bytes
  printf 'package parquets3\n\nimport "github.com/ReliablyObserve/victoria-lakehouse/tests/evilschema"\n\nvar fixedLogRowBytes = schema.EstimateRawBytesLogs([]schema.LogRow{{}})\n' > internal/storage/parquets3/zz_moved_test.go
}
# the control: the same move with the same imports
syn_m_pure_move_imports() {
  drop_fixed_log_row_bytes
  printf 'package parquets3\n\nimport "github.com/ReliablyObserve/victoria-lakehouse/internal/schema"\n\nvar fixedLogRowBytes = schema.EstimateRawBytesLogs([]schema.LogRow{{}})\n' > internal/storage/parquets3/zz_moved_test.go
}
# move_nearby <literal>: `nearby` leaves the var group of rows_ties_harness_test.go for a new file
move_nearby() {
  python3 - "$1" <<'PY2'
import sys
p = "tests/parity/rows_ties_harness_test.go"
s = open(p, encoding="utf-8").read()
line = '\tnearby = row(afterTS, "next microsecond", "api-gateway")\n'
assert line in s
open(p, "w", encoding="utf-8").write(s.replace(line, "", 1))
# the file it leaves is built under `parity`: the new one carries the same constraint, so only the literal differs
open("tests/parity/zz_nearby_test.go", "w", encoding="utf-8").write('//go:build parity\n\npackage parity\n\nvar nearby = row(afterTS, "%s", "api-gateway")\n' % sys.argv[1])
PY2
}
# white space inside a string literal: strings.Fields collapsed it, so this "pure move" passed
syn_m_literal_whitespace() { move_nearby "next  microsecond"; }
syn_m_move_changed() { move_nearby "next nanosecond"; }
syn_m_pure_move_nearby() { move_nearby "next microsecond"; }
# a declaration leaves a //go:build e2e file for an unconstrained one
syn_m_build_constraint() {
  python3 - <<'PY2'
p = "tests/e2e/buffer_query_auth_test.go"
s = open(p, encoding="utf-8").read()
line = '\tbufferAuthOrgID = envOrDefault("LH_BUFFER_AUTH_ORGID", "acme-corp:1001:0")\n'
assert line in s
open(p, "w", encoding="utf-8").write(s.replace(line, "", 1))
open("tests/e2e/zz_orgid_test.go", "w", encoding="utf-8").write('package e2e\n\nvar bufferAuthOrgID = envOrDefault("LH_BUFFER_AUTH_ORGID", "acme-corp:1001:0")\n')
PY2
}

# MAJOR-3. tests/ingestmatrix is imported by tests/e2e and is neither a lock package nor product code.
syn_x_ingestmatrix_init() {
  printf '\nfunc init() {\n\tif os.Getenv("LH_NONCE") != "" {\n\t\tos.Exit(0)\n\t}\n}\n' >> tests/ingestmatrix/sample.go
}
syn_x_ingestmatrix_newfile() { printf 'package ingestmatrix\n\nfunc Extra() {}\n' > tests/ingestmatrix/zz_extra.go; }
# an imported PRODUCT package (internal/pmeta) gains an init()
syn_x_product_adjacent_init() {
  local f
  f=$(git ls-files 'internal/pmeta/*.go' | grep -v '_test\.go$' | head -1)
  printf '\nfunc init() { println("replay") }\n' >> "$f"
}

# min-pass: the expected number of passing parity tests is lowered by one
syn_minpass_lowered() {
  python3 - <<'PY2'
import re
p = "tests/parity/known_failures.txt"
s = open(p, encoding="utf-8").read()
n = re.sub(r"^(# min-pass: )(\d+)$", lambda m: m.group(1) + str(int(m.group(2)) - 1), s, count=1, flags=re.M)
assert n != s
open(p, "w", encoding="utf-8").write(n)
PY2
}

# x/sync (singleflight in the cold read path) and x/sys (CPU feature detection behind parquet-go's encoders)
# are critical now
bump_mod() { # bump_mod <module>: the patch number of its require line in go.mod
  python3 - "$1" <<'PY2'
import re, sys
m = re.escape(sys.argv[1])
s = open("go.mod", encoding="utf-8", newline="").read()
n = re.sub(r"(^\t" + m + r" v[0-9]+\.[0-9]+\.)([0-9]+)", lambda x: x.group(1) + str(int(x.group(2)) + 1), s, count=1, flags=re.M)
assert n != s
open("go.mod", "w", encoding="utf-8", newline="").write(n)
PY2
}
syn_x_sync_bump() { bump_mod golang.org/x/sync; }
syn_x_sys_bump() { bump_mod golang.org/x/sys; }
# a toolchain version that does not fit 31 bits
syn_toolchain_overflow() {
  python3 - <<'PY2'
import re
for path, pat in (("go.mod", r"^(go \d+\.\d+\.)\d+$"), ("Dockerfile.logs", r"(golang:\d+\.\d+\.)\d+")):
    s = open(path, encoding="utf-8").read()
    n = re.sub(pat, lambda m: m.group(1) + "99999999999999999999", s, count=1, flags=re.M)
    assert n != s, path
    open(path, "w", encoding="utf-8").write(n)
PY2
}

# run_synthetic <name> <author> <title> <report> <log>: builds the case in a scratch clone and runs the gate.
run_synthetic() {
  local name=$1 author=$2 title=$3 report=$4 log=$5 clone="$WORK/syn.$1" base
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
    git commit -q -m "$title" || exit 3
  ) 2>"$log" || { cat "$log" >&2; return 3; }
  run_gate "$clone" replay-base HEAD "$author" "$title" "$report" "$log"
  local rc=$?
  rm -rf "$clone"
  return $rc
}

# -------------------------------------------------------------------- main ---
checked=0; mismatched=0; unavailable=0; row=-1
SUMMARY="$WORK/summary.md"
printf '| case | old/new verdict | failing rules | expected | result |\n|---|---|---|---|---|\n' > "$SUMMARY"

while IFS= read -r raw || [[ -n "$raw" ]]; do
  line=${raw%%#*}
  [[ -n "$(trim "$line")" ]] || continue
  # '\|' is a literal bar inside a cell; any other count of bars than eight is a malformed row
  line=${line//\\|/$'\x01'}
  bars=${line//[^|]/}
  if [[ ${#bars} -ne 8 ]]; then
    echo "malformed row (want 9 '|'-separated columns, got $((${#bars} + 1))): ${raw:0:120}" >&2
    exit 2
  fi
  IFS='|' read -r id kind base head author verdict failing contains title <<< "$line"
  id=$(trim "${id//$'\x01'/|}"); kind=$(trim "${kind//$'\x01'/|}"); base=$(trim "${base//$'\x01'/|}"); head=$(trim "${head//$'\x01'/|}")
  author=$(trim "${author//$'\x01'/|}"); verdict=$(trim "${verdict//$'\x01'/|}"); failing=$(trim "${failing//$'\x01'/|}")
  contains=$(trim "${contains//$'\x01'/|}"); title=$(trim "${title//$'\x01'/|}")
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
    run_synthetic "$head" "${author:-replay}" "${title:-replay: $head}" "$report" "$log" || rc=$?
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
