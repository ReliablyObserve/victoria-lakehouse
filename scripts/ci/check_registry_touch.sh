#!/usr/bin/env bash
# A PR that changes HTTP route registrations, upstream pins or patches must also
# change the conformance registry (or the inventory). A PR that changes the
# inventory extractors must also regenerate the committed inventory.
# Base ref in $1 (default origin/main).
set -euo pipefail
BASE=${1:-origin/main}
changed=$(git diff --name-only "$BASE"...HEAD)

# Path patterns that always count as route-touching, regardless of content:
# whole route-dispatch directories, the binary entrypoints, patches, and the
# upstream-versions manifest. Makefile and individual .go files are handled
# separately below (content-based), since a filename match alone is either
# too broad (any Makefile edit) or too narrow (misses handler registrations
# in files that don't have "handler" in their name).
touches_direct=$(echo "$changed" | grep -E '^(internal/selectapi/|lakehouse-traces/internal/selectapi/|cmd/lakehouse-logs/main\.go|lakehouse-traces/main\.go|patches/|\.upstream-versions\.json)' || true)

# Any changed non-test .go file whose diff adds or removes an HTTP
# route-registration call — HandleFunc(, mux.Handle(, or a plain .Handle( —
# counts as route-touching. This is content-based rather than filename-based
# so a route registration in a file without "handler" in its name (e.g.
# internal/stats/api.go, parity.go, internal/ui/ui.go, internal/ui/vmui.go)
# is still caught. _test.go files are excluded: a test double registering a
# handler on a throwaway mux (table tests, httptest servers) is not a real
# route change and must never require a registry touch.
touches_handle_calls=""
while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  if git diff "$BASE"...HEAD -- "$f" | grep -qE '^[+-].*(HandleFunc\(|mux\.Handle\(|\.Handle\()'; then
    touches_handle_calls="$touches_handle_calls
$f"
  fi
done <<< "$(echo "$changed" | grep -E '\.go$' | grep -v '_test\.go$' || true)"

# Makefile counts only when the diff actually adds or removes one of the
# upstream pin variable assignments — an unrelated Makefile edit (a comment,
# a new target, reformatting) must not require a registry touch. The regex
# tolerates both "VAR := value" and "VAR = value" (with or without space
# before the (:)= ) so a reformatted assignment is still recognized.
touches_pin_makefile=""
if echo "$changed" | grep -qE '^Makefile$'; then
  if git diff "$BASE"...HEAD -- Makefile | grep -qE '^[+-](VL_VERSION_LOGS|VL_COMMIT_TRACES|VT_VERSION)[[:space:]]*:?='; then
    touches_pin_makefile="Makefile"
  fi
fi

touches_routes=$(printf '%s\n%s\n%s\n' "$touches_direct" "$touches_handle_calls" "$touches_pin_makefile" | sed '/^$/d')
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
