#!/usr/bin/env bash
# Runs the registry gate on a PR using the gate code of the PR's MERGE BASE, and
# executes nothing from the PR: the PR tree is only read as data. The workflows
# fetch THIS file from the base (`git show <merge-base>:scripts/ci/gate_bootstrap.sh`),
# so a PR cannot change how it is judged.
#
#   gate_bootstrap.sh prepare   create the merge-base checkout; print gate_dir=<path>
#   gate_bootstrap.sh run       build testlinks from it and run the gate
#
# Env:
#   BASE_REF     base branch name (default main); origin/$BASE_REF must exist
#   PR_DIR       the PR checkout (default: the current directory)
#   GATE_DIR     a checkout of trusted gate code. Default: a worktree of the merge
#                base at $RUNNER_TEMP/gate-base (pull_request); the workflow for
#                pull_request_target passes its own base checkout instead.
#   RUNNER_TEMP  scratch directory (default /tmp)
#   PR_LABELS PR_BODY PR_TITLE PR_AUTHOR PR_NUMBER EVENT_ACTION LABEL_NAME SENDER
#                the event data the gate reads; passed through, nothing else is
#   GITHUB_STEP_SUMMARY GITHUB_OUTPUT   passed through when set
#   GATE_GOTOOLCHAIN  GOTOOLCHAIN for the testlinks build (default local: the workflow
#                installed the exact Go the base's go.mod asks for)
set -euo pipefail

mode=${1:?usage: gate_bootstrap.sh prepare|run}
BASE_REF=${BASE_REF:-main}
PR_DIR=$(cd "${PR_DIR:-.}" && pwd)
TMP=${RUNNER_TEMP:-/tmp}

# No git hook may run, whatever the PR or an earlier step put in .git/hooks.
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.hooksPath GIT_CONFIG_VALUE_0=/dev/null
export GIT_TERMINAL_PROMPT=0

cd "$PR_DIR"
merge_base=$(git merge-base "origin/$BASE_REF" HEAD)
gate=${GATE_DIR:-$TMP/gate-base}

case "$mode" in
prepare)
  if [[ -z "${GATE_DIR:-}" && ! -d "$gate" ]]; then
    git worktree add --detach "$gate" "$merge_base" >&2
  fi
  [[ -f "$gate/scripts/ci/check_registry_touch.sh" ]] || { echo "::error::no gate code at $gate" >&2; exit 1; }
  echo "gate_dir=$gate"
  ;;
run)
  [[ -d "$gate/scripts/ci" ]] || { echo "::error::no gate checkout at $gate (run prepare first)" >&2; exit 1; }
  (cd "$gate" && GOWORK=off GOTOOLCHAIN="${GATE_GOTOOLCHAIN:-local}" go build -o "$TMP/testlinks" ./tests/conformance/cmd/testlinks)
  # An allow-list environment: nothing an earlier step exported (PATH additions via
  # GITHUB_PATH, GITHUB_ENV, LD_PRELOAD, GOFLAGS ...) reaches the gate.
  pass=()
  for v in PR_LABELS PR_BODY PR_TITLE PR_AUTHOR PR_NUMBER EVENT_ACTION LABEL_NAME SENDER GITHUB_STEP_SUMMARY GITHUB_OUTPUT; do
    [[ -n "${!v+x}" ]] && pass+=("$v=${!v}")
  done
  exec env -i PATH="/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin" HOME="${HOME:-/tmp}" TMPDIR="$TMP" LANG=C.UTF-8 \
    GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=core.hooksPath GIT_CONFIG_VALUE_0=/dev/null GIT_TERMINAL_PROMPT=0 \
    GOWORK=off GOTOOLCHAIN=local \
    TESTLINKS_BIN="$TMP/testlinks" BASE_REF="$BASE_REF" \
    ${pass[@]+"${pass[@]}"} \
    "$gate/scripts/ci/check_registry_touch.sh" "origin/$BASE_REF"
  ;;
*)
  echo "usage: gate_bootstrap.sh prepare|run" >&2
  exit 2
  ;;
esac
