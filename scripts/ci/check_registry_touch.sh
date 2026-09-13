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

# --- feature catalog ---------------------------------------------------------
# A PR that adds or extends a Lakehouse feature must also carry that feature in
# the catalog, and must ship the regenerated documents. "Adds or extends a
# feature" is decided from the diff, by any of three independent signals:
#
#   1. a new CHANGELOG `### Added` bullet with a bold lead-in (the exact form
#      the catalog gate matches on — see tests/conformance/registry/features.go);
#   2. a route or handler registration, or a `lakehouse.*` flag definition,
#      added or removed in non-test Go under internal/, cmd/ or
#      lakehouse-traces/;
#   3. a new Lakehouse registry row (`- id: lh.…`) under
#      tests/conformance/registry/rows/.
#
# Any of those without a change under tests/conformance/registry/features/ means
# a shipped capability that the catalog — and therefore docs/features.md and the
# README's Key Features block — does not know about.
feature_signals=""

if git diff "$BASE"...HEAD -- CHANGELOG.md | grep -qE '^\+- \*\*'; then
  feature_signals="$feature_signals
CHANGELOG.md: a new '### Added' bullet"
fi

while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  if git diff "$BASE"...HEAD -- "$f" | grep -qE '^[+-].*(HandleFunc\(|mux\.Handle\(|\.Handle\(|flag\.[A-Za-z]+\("lakehouse\.)'; then
    feature_signals="$feature_signals
$f: a route, handler or lakehouse.* flag"
  fi
done <<< "$(echo "$changed" | grep -E '^(internal/|cmd/|lakehouse-traces/).*\.go$' | grep -v '_test\.go$' || true)"

while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  if git diff "$BASE"...HEAD -- "$f" | grep -qE '^\+[[:space:]]*-[[:space:]]*id:[[:space:]]*lh\.'; then
    feature_signals="$feature_signals
$f: a new Lakehouse registry row"
  fi
done <<< "$(echo "$changed" | grep -E '^tests/conformance/registry/rows/.*\.yaml$' || true)"

feature_signals=$(echo "$feature_signals" | sed '/^$/d')
touches_features=$(echo "$changed" | grep -E '^tests/conformance/registry/features/' || true)

if [[ -n "$feature_signals" && -z "$touches_features" ]]; then
  echo "::error::this PR adds or extends a Lakehouse feature but does not touch tests/conformance/registry/features/ — every shipped feature must be in the catalog with its rows, tests and docs"
  echo "$feature_signals"
  echo "steps:"
  echo "  1. add or update the feature in tests/conformance/registry/features/<area>.yaml"
  echo "     (id, title, status, since, area, surfaces, rows, tests, docs, highlight, description, changelog_bullets)"
  echo "  2. run 'make conformance-gen' and commit the regenerated docs/features.md and README.md"
  echo "  3. see tests/conformance/README.md, section 'Adding or extending a feature'"
  exit 1
fi

# Generated documents must be current on a feature PR. Skipped where confgen
# cannot run (a synthetic fixture repo in the script's own tests, or a checkout
# without the Go toolchain); CI always has both, and `make conformance-check`
# runs the same check independently.
if [[ -n "$feature_signals" && -z "${SKIP_CONFGEN_CHECK:-}" ]]; then
  if command -v go >/dev/null 2>&1 && [[ -d tests/conformance/cmd/confgen ]]; then
    if ! GOWORK=off go run ./tests/conformance/cmd/confgen -check; then
      echo "::error::this PR adds or extends a Lakehouse feature but the generated documents are stale or the feature gate fails"
      echo "steps:"
      echo "  1. fix the reported feature-catalog failures in tests/conformance/registry/features/"
      echo "  2. run 'make conformance-gen' and commit docs/features.md and README.md"
      exit 1
    fi
  fi
fi

echo "registry-touch check OK"
