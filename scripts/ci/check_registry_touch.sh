#!/usr/bin/env bash
# A PR that changes HTTP route registrations, upstream pins or patches must also
# change the conformance registry (or the inventory). A PR that changes the
# inventory extractors must also regenerate the committed inventory.
# Base ref in $1 (default origin/main).
set -euo pipefail
# Byte-order collation, so `sort` and `comm` agree on every runner and locale.
export LC_ALL=C
BASE=${1:-origin/main}
changed=$(git diff --name-only "$BASE"...HEAD)
# Content comparisons read files at the merge base: the revision
# `git diff BASE...HEAD` diffs against, so a PR is judged on its own changes
# only, never on what landed on the base branch after it forked.
MERGE_BASE=$(git merge-base "$BASE" HEAD)

# matching_lines <rev> <path> <ERE>: the distinct lines of <path> at <rev> that
# match <ERE>, with surrounding whitespace trimmed. A path that does not exist
# at <rev> (a file the PR adds or deletes) has no lines.
matching_lines() {
  git show "$1:$2" 2>/dev/null | grep -E -- "$3" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//' | sort -u || true
}

# registration_changed <path> <ERE>: succeeds when the set of lines matching
# <ERE> differs between the merge base and HEAD, i.e. a registration was added
# or removed. Comparing sets rather than grepping the diff's +/- lines means a
# registration that only moved within its file, or was re-indented, is not a
# change. A registration moved to a different file still counts, in both
# files: the check errs towards asking for a registry or catalog touch rather
# than risk missing a real change.
registration_changed() {
  [[ "$(matching_lines "$MERGE_BASE" "$1" "$2")" != "$(matching_lines HEAD "$1" "$2")" ]]
}

# Path patterns that always count as route-touching, regardless of content:
# whole route-dispatch directories, the binary entrypoints and patches (the
# upstream pins live in the Makefile alone). Makefile and individual .go files
# are handled separately below (content-based), since a filename match alone is
# either too broad (any Makefile edit) or too narrow (misses handler
# registrations in files that don't have "handler" in their name).
touches_direct=$(echo "$changed" | grep -E '^(internal/selectapi/|lakehouse-traces/internal/selectapi/|cmd/lakehouse-logs/main\.go|lakehouse-traces/main\.go|patches/)' || true)

# Any changed non-test .go file whose HTTP route-registration calls —
# HandleFunc(, mux.Handle(, or a plain .Handle( — were added or removed counts
# as route-touching. This is content-based rather than filename-based so a
# route registration in a file without "handler" in its name (e.g.
# internal/stats/api.go, parity.go, internal/ui/ui.go, internal/ui/vmui.go)
# is still caught. _test.go files are excluded: a test double registering a
# handler on a throwaway mux (table tests, httptest servers) is not a real
# route change and must never require a registry touch.
ROUTE_RE='(HandleFunc\(|mux\.Handle\(|\.Handle\()'
touches_handle_calls=""
while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  if registration_changed "$f" "$ROUTE_RE"; then
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
if grep -qE '^Makefile$' <<< "$changed"; then
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
# feature" is decided from the PR's changes, by any of four independent signals:
#
#   1. a `### Added` bullet lead-in in CHANGELOG.md that the merge base does not
#      have (the bold lead-in is what the catalog gate matches on — see
#      tests/conformance/registry/changelog.go);
#   2. a route or handler registration, or a `lakehouse.*` flag definition,
#      added or removed in non-test Go under internal/, cmd/ or
#      lakehouse-traces/;
#   3. a YAML config key added to a config struct in internal/config/ (a knob
#      that exists only in the config file has no flag for signal 2 to see);
#   4. a new Lakehouse registry row (`- id: lh.…`) under
#      tests/conformance/registry/rows/.
#
# Signals 1-3 compare sets (lead-ins, registration lines, struct-qualified YAML
# keys) between the merge base and HEAD instead of grepping the diff, so moving
# text around is never mistaken for a new capability. That matters most for
# signal 1: after every release the release workflow opens a "release metadata"
# PR that moves the `[Unreleased]` bullets under a new version heading, and
# that PR adds no lead-in to the `### Added` sections — while a `### Fixed` or
# `### Changed` bullet was never an `### Added` one.
#
# Any of those without a change under tests/conformance/registry/features/ means
# a shipped capability that the catalog — and therefore docs/features.md and the
# README's Key Features block — does not know about.
feature_signals=""

# added_leadins <rev>: the bold lead-ins of the top-level bullets in every
# `### Added` section of CHANGELOG.md at <rev>, one per line, sorted. Sections
# are tracked the way the catalog's changelog parser tracks them: a
# `## [version]` heading resets the section, a `### <name>` heading sets it,
# and a bullet has a lead-in when its line opens with `- **` in column 0.
added_leadins() {
  git show "$1:CHANGELOG.md" 2>/dev/null | awk '
    # A lead-in may be WRAPPED: the entries are long and the file is wrapped at
    # 96 columns, so the closing ** can sit on a later line. Reading only the
    # first line then gives a truncated fragment as the feature key — silently,
    # and differently before and after any re-wrap. So accumulate lines until
    # the closing ** is seen, then take the text between the markers.
    /^## \[/ { sec = ""; acc = ""; next }
    /^### /  { sec = $0; sub(/^### +/, "", sec); sub(/[ \t]+$/, "", sec); acc = ""; next }
    sec != "Added" { acc = ""; next }
    acc != "" {
      cont = $0
      sub(/^[ \t]+/, "", cont)          # the wrap indent is not part of the title
      if (cont == "") { acc = ""; next }
      acc = acc " " cont
      if (acc ~ /\*\*/) {
        sub(/\*\*.*$/, "", acc)
        gsub(/[ \t]+/, " ", acc)        # one space per break, whatever the indent was
        sub(/[ \t]+$/, "", acc)
        print acc
        acc = ""
      }
      next
    }
    /^- \*\*/ {
      s = substr($0, 5)
      if (s ~ /\*\*/) { sub(/\*\*.*$/, "", s); print s }
      else { acc = s }   # lead-in continues on the next line
    }
  ' | sort -u || true
}

if grep -qFx 'CHANGELOG.md' <<< "$changed"; then
  while IFS= read -r lead; do
    [[ -z "$lead" ]] && continue
    feature_signals="$feature_signals
CHANGELOG.md: a new '### Added' bullet: $lead"
  done <<< "$(comm -13 <(added_leadins "$MERGE_BASE") <(added_leadins HEAD))"
fi

FEATURE_CODE_RE='(HandleFunc\(|mux\.Handle\(|\.Handle\(|flag\.[A-Za-z]+\("lakehouse\.)'
while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  if registration_changed "$f" "$FEATURE_CODE_RE"; then
    feature_signals="$feature_signals
$f: a route, handler or lakehouse.* flag"
  fi
done <<< "$(echo "$changed" | grep -E '^(internal/|cmd/|lakehouse-traces/).*\.go$' | grep -v '_test\.go$' || true)"

# config_keys <rev> <path>: every YAML key the config structs of <path> declare
# at <rev>, qualified by the struct type ("InsertConfig.target_file_size"), so
# the same key name in two structs stays distinct and a moved field is not a
# new key. `yaml:"-"` and `yaml:",inline"` declare no key.
config_keys() {
  git show "$1:$2" 2>/dev/null | awk '
    /^type [A-Za-z_][A-Za-z0-9_]* struct/ { typ = $2 }
    /^}/ { typ = "" }
    match($0, /yaml:"[^",]*/) {
      key = substr($0, RSTART + 6, RLENGTH - 6)
      if (key != "" && key != "-") print (typ == "" ? "(no struct)" : typ) "." key
    }
  ' | sort -u || true
}

while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  new_keys=$(comm -13 <(config_keys "$MERGE_BASE" "$f") <(config_keys HEAD "$f"))
  if [[ -n "$new_keys" ]]; then
    feature_signals="$feature_signals
$f: a new YAML config key: $(echo "$new_keys" | paste -s -d ' ' -)"
  fi
done <<< "$(echo "$changed" | grep -E '^internal/config/[^/]+\.go$' | grep -v '_test\.go$' || true)"

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
  echo "     (id, title, status, area, surfaces, rows, tests, docs, highlight, description, changelog_bullets)"
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
