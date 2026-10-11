#!/usr/bin/env bash
# `make registry-gate`: the registry gate on the current HEAD against origin/main.
#
#   gate_local.sh [base]        base defaults to origin/main
#
# What it is and is not:
#   - It runs scripts/ci/check_registry_touch.sh from YOUR WORKING TREE. CI judges a pull request with
#     the gate code of the BASE branch (so a PR cannot weaken the gate that judges it): when your branch
#     changes the gate itself, CI's verdict comes from main's copy, and this run is only a preview of
#     what the gate will say once your change is merged. A notice says so.
#   - It judges HEAD only. Uncommitted edits are ignored (a warning lists them).
#   - It fetches origin/main first and FAILS when it cannot, because a stale origin/main would be a
#     verdict against the wrong base. REGISTRY_GATE_OFFLINE=1 judges against the local origin/main
#     anyway (with a warning when it was last fetched more than 24h ago).
set -euo pipefail
BASE=${1:-origin/main}
GATE=${GATE_SCRIPT:-scripts/ci/check_registry_touch.sh}

if [[ "$BASE" == origin/main ]]; then
  if [[ -n "${REGISTRY_GATE_OFFLINE:-}" ]]; then
    fetch_head=$(git rev-parse --git-path FETCH_HEAD)
    # find -mmin is the same on BSD and GNU (stat is not): a missing or older-than-24h FETCH_HEAD warns
    if [[ ! -e "$fetch_head" || -n "$(find "$fetch_head" -mmin +1440 2>/dev/null)" ]]; then
      echo "warning: REGISTRY_GATE_OFFLINE=1 and origin/main was last fetched more than 24h ago (or never): the verdict may be against a stale base" >&2
    fi
  elif ! git fetch -q origin main; then
    echo "registry-gate: cannot fetch origin/main, so the verdict would be against a stale base." >&2
    echo "  Fix the network or credentials, or set REGISTRY_GATE_OFFLINE=1 to judge against the local origin/main anyway." >&2
    exit 2
  fi
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "warning: uncommitted changes are NOT judged: the gate reads HEAD ($(git rev-parse --short HEAD)), not the working tree." >&2
  git status --porcelain | sed 's/^/  /' >&2
fi

merge_base=$(git merge-base "$BASE" HEAD)
if [[ -f scripts/ci/pr_classify.py ]] && python3 scripts/ci/pr_classify.py --base "$merge_base" --head HEAD | grep -q '^gate_file='; then
  echo "notice: this branch changes the gate itself. CI judges it with the gate code of $BASE (main's copy), not this working tree's; this run previews how the gate you are changing judges the branch." >&2
fi

GOWORK=off bash "$GATE" "$BASE"
