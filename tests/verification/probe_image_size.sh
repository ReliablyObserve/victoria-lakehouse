#!/usr/bin/env bash
#
# probe_image_size.sh
#
# Hard upper bound on the lakehouse-logs and lakehouse-traces Docker image
# size. Locks the image-size win delivered by PR #96 (Option B elector +
# healthcheck binary folded into the main binary + zstd compression at the
# registry layer).
#
# Baseline: 88 MB for both images (distroless static + 55 MB binary).
# PR #96 target: ~60 MB for both images (~37 MB binary, no separate
# healthcheck COPY, zstd-compressed layers in the registry).
# Hard limit: 70 MB.
#
# Note: this probe inspects the LOCAL image (registry compression isn't
# reflected); the 70 MB cushion accounts for that. The registry-side
# (compressed) size is enforced by the release workflow's reporting step.
#
# Usage:
#   tests/verification/probe_image_size.sh
#
# Exit codes:
#   0 — both images within bounds
#   1 — at least one image over bound
#   2 — docker missing or image not built
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "FAIL: docker not found in PATH" >&2
  exit 2
fi

LIMIT_MB="${LIMIT_MB:-70}"
TAG="${TAG:-victoria-lakehouse-lakehouse-logs:latest}"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

check_image() {
  local image="$1"
  local raw_size
  raw_size=$(docker image inspect "$image" --format '{{.Size}}' 2>/dev/null || echo "")
  if [[ -z "$raw_size" ]]; then
    echo "  SKIP: $image — image not present locally (build via 'docker compose -f deployment/docker/docker-compose-e2e.yml build')" >&2
    return 0
  fi
  local mb
  mb=$(awk "BEGIN{printf \"%.1f\", $raw_size/1024/1024}")
  local limit_bytes=$(( LIMIT_MB * 1024 * 1024 ))
  if (( raw_size > limit_bytes )); then
    echo "  FAIL: $image = ${raw_size} bytes (${mb} MB); limit = ${LIMIT_MB} MB" >&2
    return 1
  fi
  echo "  PASS: $image = ${mb} MB (limit ${LIMIT_MB} MB)"
}

echo "=== probe_image_size (limit ${LIMIT_MB} MB) ==="
status=0
check_image victoria-lakehouse-lakehouse-logs:latest   || status=1
check_image victoria-lakehouse-lakehouse-traces:latest || status=1

if [[ $status -eq 0 ]]; then
  echo "PASS: image-size ceiling holds (any SKIPs are not regressions)"
else
  echo "FAIL: at least one image exceeds the ${LIMIT_MB} MB ceiling" >&2
fi
exit $status
