#!/usr/bin/env bash
# Emulates what the registry-gate workflows do around the gate (scripts/ci/gate_bootstrap.sh)
# in a scratch git repository, and checks that nothing a pull request ships runs before or
# inside the gate:
#   W0  a PR that must be rejected is rejected;
#   W1  PR-side Go code (a confgen that rewrites the gate) never executes;
#   W2  a git hook planted by an earlier PR-controlled step does not run on `worktree add`;
#   W3  pull_request_target flavour: the PR is read as data, its edited gate is ignored;
#   W4  a PR that edits gate_bootstrap.sh does not change how it is judged;
#   W5  an exported TESTLINKS_BIN / PATH shim from an earlier step does not reach the gate.
# Usage: bash scripts/ci/tests/test_gate_workflow_emulation.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
command -v go >/dev/null 2>&1 || { echo "skip - emulation (no go toolchain)"; exit 0; }
pass=0
fail=0
# A developer's Go may be older than go.mod; CI's setup-go installs the exact one.
export GATE_GOTOOLCHAIN="${GATE_GOTOOLCHAIN:-auto}"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT

# scratch_repo <dir>: a repo whose main is this checkout's tracked and new files.
scratch_repo() {
  local d="$1"
  mkdir -p "$d"
  (cd "$REPO_ROOT" && git ls-files -z --cached --others --exclude-standard | tar --null -T - -cf -) | tar -x -C "$d"
  (
    cd "$d" || exit 1
    git init -q -b main
    git config user.email t@example.com
    git config user.name t
    git config commit.gpgsign false
    git add -A
    git commit -q -m base
    git update-ref refs/remotes/origin/main HEAD
  ) >/dev/null 2>&1
}

# bad_pr: product change, no registry change, and a new test nobody references.
bad_pr() {
  echo '// behaviour change' >> internal/config/config.go
  printf 'package config\n\nimport "testing"\n\nfunc TestUnlinkedEmulation(t *testing.T) {}\n' > internal/config/unlinked_emulation_test.go
}

commit() { git add -A && git commit -q -m "$1"; }

# run_gate <repo> <flavour>: bootstrap prepare + run, as the workflows do. Echoes output.
run_gate() {
  local d="$1" flavour="${2:-pr}" out rc
  local runner="$T/runner.$RANDOM"
  mkdir -p "$runner"
  (
    cd "$d" || exit 1
    export RUNNER_TEMP="$runner" BASE_REF=main PR_LABELS="" PR_BODY="" PR_TITLE="fix: x" PR_AUTHOR=mallory PR_NUMBER=1 EVENT_ACTION=synchronize LABEL_NAME="" SENDER=mallory
    if [[ "$flavour" == target ]]; then
      # pull_request_target: GATE_DIR is a checkout of the base, the PR is a separate worktree.
      local pr="$runner/pr" base_checkout="$runner/base"
      git worktree add -q --detach "$base_checkout" main
      git worktree add -q --detach "$pr" HEAD
      GATE_DIR="$base_checkout" PR_DIR="$pr" bash "$base_checkout/scripts/ci/gate_bootstrap.sh" run
    else
      # pull_request: the bootstrap is read from the merge base, never from the PR.
      local base
      base=$(git merge-base origin/main HEAD)
      git show "$base:scripts/ci/gate_bootstrap.sh" > "$runner/gate_bootstrap.sh"
      bash "$runner/gate_bootstrap.sh" prepare > "$runner/prep.out" || exit 1
      bash "$runner/gate_bootstrap.sh" run
    fi
  ) 2>&1
}

# rejected <rc> <output>: the gate itself said no (an ::error:: line), not a build or setup failure.
rejected() { [[ $1 -ne 0 ]] && grep -q '^::error::' <<<"$2" && ! grep -q 'go: ' <<<"$2"; }

check() { # check <name> <ok:0|1> [detail]
  if [[ "$2" == 0 ]]; then echo "ok   - $1"; pass=$((pass + 1)); else echo "FAIL - $1 ${3:-}"; fail=$((fail + 1)); fi
}

R="$T/repo"
scratch_repo "$R"
cd "$R" || exit 2

# --- W0 ---
git checkout -q -b w0 main && bad_pr && commit w0
out=$(run_gate "$R"); rc=$?
check "W0: a bad PR is rejected by the gate run from the merge base" "$(rejected $rc "$out" && grep -q 'makes no real content change' <<<"$out"; echo $?)" "rc=$rc"$'\n'"$out"
git checkout -q main

# --- W1 ---
git checkout -q -b w1 main && bad_pr
cat > tests/conformance/cmd/confgen/main.go <<'GO'
package main

import "os"

func main() {
	_ = os.WriteFile(os.Getenv("RUNNER_TEMP")+"/PWNED-confgen", []byte("x"), 0o644)
	_ = os.WriteFile(os.Getenv("RUNNER_TEMP")+"/gate-base/scripts/ci/pr_classify.py", []byte("print('exempt=release-metadata')\n"), 0o644)
}
GO
sed -i.bak 's/^## \[Unreleased\]$/## [Unreleased]\n\n### Added\n\n- **Emulated new thing.** x/' CHANGELOG.md && rm -f CHANGELOG.md.bak
commit w1
out=$(run_gate "$R"); rc=$?
marker=$(ls "$T"/runner.*/PWNED-confgen 2>/dev/null | head -1)
check "W1: PR-side confgen is never executed by the gate" "$([[ -z "$marker" ]]; echo $?)" "marker=$marker"
check "W1: the gate still rejects the PR" "$(rejected $rc "$out"; echo $?)" "$out"
check "W1: the base gate's classifier was not rewritten" "$(! grep -q "exempt=release-metadata'" "$T"/runner.*/gate-base/scripts/ci/pr_classify.py 2>/dev/null; echo $?)"
git checkout -q main

# --- W2 ---
git checkout -q -b w2 main && bad_pr && commit w2
hook_marker="$T/hook-ran"
printf '#!/bin/sh\ntouch "%s"\n' "$hook_marker" > "$R/.git/hooks/post-checkout"
chmod +x "$R/.git/hooks/post-checkout"
# control: without the guard, `git worktree add` does run the hook, so the test proves something
git worktree add -q --detach "$T/control-worktree" main >/dev/null 2>&1
check "W2 control: an unguarded git worktree add runs the planted hook" "$([[ -f "$hook_marker" ]]; echo $?)"
rm -f "$hook_marker"
out=$(run_gate "$R"); rc=$?
check "W2: the gate's worktree add does not run the planted hook" "$([[ ! -f "$hook_marker" ]]; echo $?)"
check "W2: the gate still rejects the PR" "$(rejected $rc "$out"; echo $?)" "$out"
rm -f "$R/.git/hooks/post-checkout"
git checkout -q main

# --- W3 ---
git checkout -q -b w3 main && bad_pr
printf '#!/usr/bin/env bash\necho registry-touch check OK\nexit 0\n' > scripts/ci/check_registry_touch.sh
commit w3
out=$(run_gate "$R" target); rc=$?
check "W3: pull_request_target flavour runs the base gate, not the PR's edited one" "$(rejected $rc "$out"; echo $?)" "rc=$rc"$'\n'"$out"
check "W3: the PR worktree was only read" "$(cd "$R" && git diff --quiet main w3 -- internal/config/unlinked_emulation_test.go; [[ $? -eq 1 ]]; echo $?)"
git checkout -q main

# --- W4 ---
git checkout -q -b w4 main && bad_pr
printf '#!/usr/bin/env bash\ntouch "%s/bootstrap-pwned"\nexit 0\n' "$T" > scripts/ci/gate_bootstrap.sh
commit w4
out=$(run_gate "$R"); rc=$?
check "W4: an edited gate_bootstrap.sh in the PR is never used" "$([[ ! -f "$T/bootstrap-pwned" ]] && rejected $rc "$out"; echo $?)" "rc=$rc"$'\n'"$out"
git checkout -q main

# --- W5 ---
git checkout -q -b w5 main && bad_pr && commit w5
printf '#!/bin/sh\ntouch "%s/testlinks-pwned"\nexit 0\n' "$T" > "$T/evil-testlinks"
chmod +x "$T/evil-testlinks"
mkdir -p "$T/shim" && printf '#!/bin/sh\ntouch "%s/python-shim-pwned"\nexec /usr/bin/python3 "$@"\n' "$T" > "$T/shim/python3" && chmod +x "$T/shim/python3"
out=$(TESTLINKS_BIN="$T/evil-testlinks" PATH="$T/shim:$PATH" run_gate "$R"); rc=$?
check "W5: an exported TESTLINKS_BIN and a PATH shim from an earlier step do not reach the gate" "$([[ ! -f "$T/testlinks-pwned" && ! -f "$T/python-shim-pwned" ]] && rejected $rc "$out"; echo $?)" "rc=$rc"$'\n'"$out"
git checkout -q main

echo
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
