#!/usr/bin/env bash
# A PR that changes HTTP route registrations, upstream pins or patches must also
# change the conformance registry (or the inventory). A PR that changes the
# inventory extractors must also regenerate the committed inventory.
# Base ref in $1 (default origin/main).
set -euo pipefail
BASE=${1:-origin/main}
changed=$(git diff --name-only "$BASE"...HEAD)

touches_routes=$(echo "$changed" | grep -E '^(internal/selectapi/|lakehouse-traces/internal/selectapi/|cmd/lakehouse-logs/main.go|lakehouse-traces/main.go|internal/(stats|delete|tenant|lifecycle|crosssignal|peercache|ui)/.*handler.*\.go|patches/|Makefile|\.upstream-versions\.json)' || true)
touches_registry=$(echo "$changed" | grep -E '^tests/conformance/(registry/rows/|inventory\.generated\.yaml)' || true)
if [[ -n "$touches_routes" && -z "$touches_registry" ]]; then
  echo "::error::this PR changes route/handler/upstream files but not tests/conformance/registry/rows/ — add or update the rows (see tests/conformance/README.md)"
  echo "$touches_routes"
  exit 1
fi

touches_extractors=$(echo "$changed" | grep -E '^tests/conformance/inventory/[^/]+\.go$' | grep -v '_test\.go$' || true)
touches_generated_inventory=$(echo "$changed" | grep -E '^tests/conformance/inventory\.generated\.yaml$' || true)
if [[ -n "$touches_extractors" && -z "$touches_generated_inventory" ]]; then
  echo "::error::this PR changes tests/conformance/inventory/ extractors but not tests/conformance/inventory.generated.yaml — run 'make conformance-gen' and commit the result"
  echo "$touches_extractors"
  exit 1
fi

echo "registry-touch check OK"
