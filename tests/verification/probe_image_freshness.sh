#!/usr/bin/env bash
#
# Regression smoke-probe for the stale-binary regression class.
#
# Symptom this catches: the running lakehouse-logs and/or lakehouse-traces
# container is built from a binary that predates the most-recent source
# change to that module. The user "recreated" the container but did not
# "rebuild" the image, so the new container restarts with the SAME stale
# binary it had before.
#
# Failure mode this fix-class belongs to:
#   commit A: code change to lakehouse-traces/internal/storage/...
#   commit A+: probes pass (image rebuilt as part of verification)
#   commit B: unrelated change, no traces source change
#   user: `docker compose up -d --force-recreate lakehouse-traces`
#         (recreate, no `build`)
#   result: container running 13:13 binary, source is at commit A (13:19)
#           → all the bug fixes from A are invisible at runtime
#           → probes for those fixes regress with no apparent cause
#
# Root cause for the original instance: the [[feedback_rebuild_after_each_fix]]
# rule was followed for logs but NOT for traces on commit fa0f39c, leaving
# the traces container running a pre-42d7e09 binary that was missing the
# four Jaeger / large-data fixes.
#
# What this probe does: compares each lakehouse container's image
# CreatedAt timestamp against the git CommitDate of the most-recent commit
# touching that module's source tree. If the image is older than the
# newest source commit, fail with a clear message telling the user to
# `docker compose build` (not just `up`).
#
# Usage:
#   tests/verification/probe_image_freshness.sh
#
# Exit codes:
#   0 - both images are at least as new as the most-recent source commit
#   1 - at least one image is stale (regression: stale binary running)
#   2 - probe could not run (docker / git unavailable)
#
# Negative-control verification: run
#   docker image tag victoria-lakehouse-lakehouse-traces:latest victoria-lakehouse-lakehouse-traces:tmp
#   docker rmi victoria-lakehouse-lakehouse-traces:latest
#   docker tag <some-older-image-sha> victoria-lakehouse-lakehouse-traces:latest
#   ./probe_image_freshness.sh
# It MUST fail with exit 1. Restore with
#   docker tag victoria-lakehouse-lakehouse-traces:tmp victoria-lakehouse-lakehouse-traces:latest

set -euo pipefail

REPO_ROOT="${LH_REPO_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || echo .)}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not available" >&2
  exit 2
fi
if ! command -v git >/dev/null 2>&1; then
  echo "git not available" >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not available (used for timestamp comparison)" >&2
  exit 2
fi

# Resolve image CreatedAt to a unix epoch (seconds).
image_created_epoch() {
  local image="$1"
  local raw
  raw=$(docker image inspect --format '{{.Created}}' "${image}" 2>/dev/null || true)
  if [[ -z "${raw}" ]]; then
    echo ""
    return
  fi
  python3 -c "
import sys, datetime
raw='${raw}'
# strip trailing 'Z' and sub-second fractions for fromisoformat
if raw.endswith('Z'): raw = raw[:-1]
if '.' in raw:
    base, frac = raw.split('.', 1)
    # truncate fractional seconds to 6 digits (Python max)
    frac = frac[:6]
    raw = base + '.' + frac
try:
    dt = datetime.datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    print(int(dt.timestamp()))
except Exception as e:
    sys.stderr.write(f'parse error: {e}\n')
"
}

# Resolve newest commit date for a given pathspec under the repo.
source_newest_epoch() {
  local pathspec="$1"
  local epoch
  epoch=$(git -C "${REPO_ROOT}" log -1 --format='%ct' -- "${pathspec}" 2>/dev/null || true)
  echo "${epoch}"
}

# Render a human-readable timestamp for diagnostics.
ts_str() {
  python3 -c "import datetime; print(datetime.datetime.fromtimestamp($1, tz=datetime.timezone.utc).isoformat())"
}

declare -A IMAGES=(
  ["victoria-lakehouse-lakehouse-logs:latest"]="lakehouse-logs internal Dockerfile.logs go.mod go.sum"
  ["victoria-lakehouse-lakehouse-traces:latest"]="lakehouse-traces internal Dockerfile.traces go.mod go.sum"
)

stale_count=0
ok_count=0

for image in "${!IMAGES[@]}"; do
  pathspec="${IMAGES[$image]}"

  img_epoch=$(image_created_epoch "${image}")
  if [[ -z "${img_epoch}" ]]; then
    echo "SKIP: image ${image} not present locally (cannot check)" >&2
    continue
  fi

  # newest of all touched paths
  newest_src_epoch=0
  for path in ${pathspec}; do
    e=$(source_newest_epoch "${path}")
    if [[ -n "${e}" && "${e}" -gt "${newest_src_epoch}" ]]; then
      newest_src_epoch="${e}"
    fi
  done

  if [[ "${newest_src_epoch}" -eq 0 ]]; then
    echo "SKIP: no source commits found for ${pathspec} (cannot check)" >&2
    continue
  fi

  img_ts=$(ts_str "${img_epoch}")
  src_ts=$(ts_str "${newest_src_epoch}")

  if [[ "${img_epoch}" -lt "${newest_src_epoch}" ]]; then
    skew=$((newest_src_epoch - img_epoch))
    echo "FAIL: image ${image} is stale" >&2
    echo "  image_created  : ${img_ts}" >&2
    echo "  newest_source  : ${src_ts}" >&2
    echo "  source_paths   : ${pathspec}" >&2
    echo "  age_skew_sec   : ${skew}" >&2
    echo "  remediation    : rebuild and recreate before running probes:" >&2
    echo "    cd deployment/docker && \\" >&2
    echo "      docker compose -f docker-compose-e2e.yml build $(echo "${image}" | cut -d: -f1 | sed 's/^victoria-lakehouse-//') && \\" >&2
    echo "      docker compose -f docker-compose-e2e.yml up -d --force-recreate $(echo "${image}" | cut -d: -f1 | sed 's/^victoria-lakehouse-//')" >&2
    stale_count=$((stale_count + 1))
  else
    echo "OK:   image ${image}"
    echo "      image_created : ${img_ts}"
    echo "      newest_source : ${src_ts}"
    ok_count=$((ok_count + 1))
  fi
done

if [[ "${stale_count}" -gt 0 ]]; then
  echo "" >&2
  echo "FAIL: ${stale_count} stale image(s) detected (${ok_count} OK)" >&2
  echo "This is the same regression class as the post-fa0f39c traces probes:" >&2
  echo "the container was 'recreated' but the binary was NEVER rebuilt." >&2
  exit 1
fi

echo "PASS: all ${ok_count} lakehouse image(s) are at least as new as their source"
exit 0
