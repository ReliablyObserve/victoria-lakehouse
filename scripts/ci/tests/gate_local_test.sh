#!/usr/bin/env bash
# Tests for scripts/ci/gate_local.sh (make registry-gate): it fails when origin/main cannot be
# fetched, honours REGISTRY_GATE_OFFLINE=1, warns about uncommitted edits and says when a branch
# changes the gate. The gate itself is a stub (GATE_SCRIPT), so this runs in a second.
#
# Usage: bash scripts/ci/tests/gate_local_test.sh
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL="$HERE/../gate_local.sh"
CLASSIFY="$HERE/../pr_classify.py"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
pass=0; fail=0
ok() { echo "ok   - $1"; pass=$((pass + 1)); }
bad() { echo "FAIL - $1"; shift; printf '       %s\n' "$@"; fail=$((fail + 1)); }

mkrepo() { # mkrepo <dir> [--with-origin]
  mkdir -p "$1" && (
    cd "$1" || exit 1
    git init -q -b main && git config user.email t@e && git config user.name t && git config commit.gpgsign false
    mkdir -p scripts/ci && cp "$CLASSIFY" scripts/ci/pr_classify.py && cp "$HERE/../check_changelog_pr.py" scripts/ci/
    # shellcheck disable=SC2016  # expanded when the stub runs
    printf '#!/usr/bin/env bash\necho "STUB GATE base=$1 head=$(git rev-parse --short HEAD)"\n' > stub_gate.sh
    echo base > f && git add -A && git commit -q -m base
    if [[ "${2:-}" == --with-origin ]]; then
      git clone -q --bare . "$1.origin.git" && git remote add origin "$1.origin.git" && git fetch -q origin && git branch -q -u origin/main
    fi
  ) >/dev/null 2>&1
}

run() { # run <dir> [env...] -- prints output, sets rc
  local d=$1; shift
  out=$(cd "$d" && env GATE_SCRIPT=stub_gate.sh "$@" bash "$LOCAL" 2>&1); rc=$?
}

# 1. no origin at all: the fetch fails, the run fails and says why
mkrepo "$T/noorigin"
run "$T/noorigin" REGISTRY_GATE_OFFLINE=
if [[ $rc -ne 0 ]] && grep -q 'cannot fetch origin/main' <<<"$out" && ! grep -q 'STUB GATE' <<<"$out"; then ok "a failed fetch is a failure, not a verdict against a stale base"; else bad "failed fetch (rc=$rc)" "$out"; fi

# 2. REGISTRY_GATE_OFFLINE=1 judges anyway (needs a local origin/main ref)
(cd "$T/noorigin" && git update-ref refs/remotes/origin/main HEAD && touch -t 202001010000 "$(git rev-parse --git-path FETCH_HEAD)")
run "$T/noorigin" REGISTRY_GATE_OFFLINE=1
if [[ $rc -eq 0 ]] && grep -q 'STUB GATE base=origin/main' <<<"$out" && grep -q 'last fetched more than 24h ago' <<<"$out"; then ok "REGISTRY_GATE_OFFLINE=1 judges against the local origin/main and warns about its age"; else bad "offline (rc=$rc)" "$out"; fi

# 3. with an origin: fetched, judged, no warnings on a clean tree
mkrepo "$T/withorigin" --with-origin
run "$T/withorigin" REGISTRY_GATE_OFFLINE=
if [[ $rc -eq 0 ]] && grep -q 'STUB GATE' <<<"$out" && ! grep -q 'warning' <<<"$out"; then ok "a clean tree with a reachable origin is judged without warnings"; else bad "clean (rc=$rc)" "$out"; fi

# 4. uncommitted edits are warned about
echo dirty >> "$T/withorigin/f"
run "$T/withorigin" REGISTRY_GATE_OFFLINE=
if [[ $rc -eq 0 ]] && grep -q 'uncommitted changes are NOT judged' <<<"$out" && grep -q ' M f' <<<"$out"; then ok "uncommitted edits are listed and ignored"; else bad "dirty (rc=$rc)" "$out"; fi
(cd "$T/withorigin" && git checkout -q -- f)

# 5. a branch that changes the gate gets the notice
(cd "$T/withorigin" && echo '# edit' >> scripts/ci/pr_classify.py && git commit -q -am "gate edit") >/dev/null 2>&1
run "$T/withorigin" REGISTRY_GATE_OFFLINE=
if [[ $rc -eq 0 ]] && grep -q "this branch changes the gate itself" <<<"$out" && grep -q "gate code of origin/main" <<<"$out"; then ok "a branch that edits the gate is told CI judges it with main's gate"; else bad "gate notice (rc=$rc)" "$out"; fi

# 6. a branch that does not change the gate gets no notice
mkrepo "$T/plain" --with-origin
(cd "$T/plain" && echo more >> f && git commit -q -am "plain") >/dev/null 2>&1
run "$T/plain" REGISTRY_GATE_OFFLINE=
if [[ $rc -eq 0 ]] && ! grep -q 'changes the gate itself' <<<"$out"; then ok "no notice when the gate is not edited"; else bad "no notice (rc=$rc)" "$out"; fi

# 7. a non-zero verdict of the gate is the exit status
printf '#!/usr/bin/env bash\nexit 1\n' > "$T/plain/stub_gate.sh"
run "$T/plain" REGISTRY_GATE_OFFLINE=
if [[ $rc -ne 0 ]]; then ok "the gate's failure is the exit status"; else bad "exit status (rc=$rc)" "$out"; fi

echo
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
