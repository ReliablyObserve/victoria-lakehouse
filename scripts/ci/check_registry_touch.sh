#!/usr/bin/env bash
# The registry gate. It decides from the verified content of the diff alone: there is
# no exemption by label, by PR text or by who asks. Run it locally with
# `make registry-gate`: it is the same code path as the two CI checks.
#
# A PR that changes HTTP route registrations, upstream pins or patches must also
# change the conformance registry (or the inventory). A PR that changes the
# inventory extractors must also regenerate the committed inventory. Rules 1-3
# (below) are decided by tests/conformance/cmd/testlinks.
#
# The verdict is ONE Markdown report: appended to $GITHUB_STEP_SUMMARY in CI, printed
# otherwise. Exit status is nonzero only for a hard failure. Edits of lock code and of
# the gate's own files are listed in the report, not failed: the pull request's
# required review covers them.
# Base ref in $1 (default origin/main).
set -euo pipefail
# Byte-order collation, so `sort` and `comm` agree on every runner and locale.
export LC_ALL=C
BASE=${1:-origin/main}
# The PR's head revision. The gate reads blobs of it (git show / git diff) and never
# the working tree, so a PR cannot make it follow a symlink or smudge an LFS file.
HEADREV=${HEAD_REV:-HEAD}
# Never let a .gitattributes of the PR hide a diff from the gate: no textconv, no
# external diff driver, binary files diffed as text.
GITDIFF=(git diff --text --no-textconv --no-ext-diff)

# ---- the report ------------------------------------------------------------
REPORT_DIR=$(mktemp -d)
ROWS_FILE="$REPORT_DIR/shell-rows.md"
FIXES_FILE="$REPORT_DIR/shell-fixes.md"
: > "$ROWS_FILE"; : > "$FIXES_FILE"
HARD_FAIL=""
KIND_LINE=""
GATE_TOUCHED=""
approvers_file="$REPORT_DIR/approvers"

# Everything an author controls (file names, row ids, test names, YAML values) is escaped for the
# place it lands in: Markdown for the report, workflow-command syntax for annotations. Nothing the
# author wrote reaches either raw. (Paths come from git, which quotes CR/LF itself; the control
# characters are dropped here as well.)
#
# md_esc <text>: one Markdown-safe line: & < > | and the backtick are neutralised, CR/LF/TAB
# become spaces and other control characters are dropped.
md_esc() {
  printf '%s' "$1" | tr '\r\n\t' '   ' | tr -d '\000-\010\013-\037\177' \
    | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e "s/\`/'/g" -e 's/|/\\|/g'
}
# wf_prop <text>: a workflow-command property value (`file=<prop>`): % : , escaped.
wf_prop() {
  printf '%s' "$1" | tr -d '\000-\037\177' | sed -e 's/%/%25/g' -e 's/:/%3A/g' -e 's/,/%2C/g'
}
# plain_list: stdin lines as inert log lines. The runner trims leading spaces before it looks for a
# `::` command, so a path named `::error::x` must not start a line: every line gets a "- " prefix.
plain_list() { tr -d '\000-\010\013-\037\177' | sed 's/^/  - /'; }

# rule_row <rule> <PASS|FAIL|n/a> <detail>
rule_row() { printf '| %s | %s | %s |\n' "$1" "$2" "$(md_esc "$3")" >> "$ROWS_FILE"; }
# hard_fail <rule> <detail> <how to fix>
hard_fail() { HARD_FAIL=1; rule_row "$1" FAIL "$2"; printf -- '- **%s**: %s\n' "$1" "$3" >> "$FIXES_FILE"; }

render_report() {
  local verdict=$1 rules_md="$REPORT_DIR/rules.md" fixes_md="$REPORT_DIR/fixes.md" details_md="$REPORT_DIR/details.md"
  if [[ "$verdict" == PASS ]]; then verdict="PASS$(review_suffix)"; fi
  echo "## Registry gate: $verdict"
  echo
  [[ -z "$KIND_LINE" ]] || { echo "$KIND_LINE"; echo; }
  echo "| Rule | Verdict | Detail |"
  echo "|---|---|---|"
  cat "$ROWS_FILE"
  [[ -f "$rules_md" ]] && cat "$rules_md"
  echo
  if [[ -s "$FIXES_FILE" || -s "$fixes_md" ]]; then
    echo "### How to fix"
    echo
    cat "$FIXES_FILE"
    [[ -f "$fixes_md" ]] && cat "$fixes_md"
    echo
  fi
  if [[ -n "$GATE_TOUCHED" ]]; then
    echo "### Gate files changed (informational, covered by the required review)"
    echo
    while IFS= read -r g; do
      # shellcheck disable=SC2016  # the backticks are Markdown, not a command substitution
      [[ -z "$g" ]] || printf -- '- `%s`\n' "$(md_esc "$g")"
    done <<< "$GATE_TOUCHED"
    echo
  fi
  [[ -f "$details_md" ]] && cat "$details_md"
  echo "Preview locally: \`make registry-gate\` (your working tree's copy of this script; CI judges with the base branch's)."
}

# review_suffix: " - N lock-affecting files and M gate files need review (listed below)", or nothing.
# A bare PASS would hide that a person has to look: the pull request's required approval is the control
# for these files.
review_suffix() {
  local n_lock=0 n_gate=0 parts=()
  [[ -f "$REPORT_DIR/review-count" ]] && n_lock=$(tr -dc '0-9' < "$REPORT_DIR/review-count")
  [[ -z "$GATE_TOUCHED" ]] || n_gate=$(grep -c . <<< "$GATE_TOUCHED")
  n_lock=${n_lock:-0}
  [[ $n_lock -eq 0 ]] || parts+=("$n_lock lock-affecting file$([[ $n_lock -eq 1 ]] || echo s)")
  [[ $n_gate -eq 0 ]] || parts+=("$n_gate gate file$([[ $n_gate -eq 1 ]] || echo s)")
  [[ ${#parts[@]} -gt 0 ]] || return 0
  local joined=${parts[0]} total=$((n_lock + n_gate))
  [[ ${#parts[@]} -lt 2 ]] || joined="${parts[0]} and ${parts[1]}"
  printf ' - %s %s review (listed below)' "$joined" "$([[ $total -eq 1 ]] && echo needs || echo need)"
}

# gate_annotations: one inline `::warning file=...::` per changed gate file (at most 10, then one
# "... and N more"), so the Files tab of the pull request shows them. Lock-affecting files get the same
# from testlinks.
gate_annotations() {
  local n=0 total
  [[ -n "$GATE_TOUCHED" ]] || return 0
  total=$(grep -c . <<< "$GATE_TOUCHED")
  while IFS= read -r g; do
    [[ -n "$g" ]] || continue
    if [[ $n -eq 10 ]]; then
      echo "::warning::... and $((total - 10)) more gate files (see the job summary)"
      break
    fi
    echo "::warning file=$(wf_prop "$g")::gate file: the owner's approval is the control"
    n=$((n + 1))
  done <<< "$GATE_TOUCHED"
}

finish() {
  local rc=$? verdict=PASS
  [[ $rc -eq 0 ]] || verdict=FAIL
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    render_report "$verdict" >> "$GITHUB_STEP_SUMMARY" 2>/dev/null || true
  else
    render_report "$verdict" 2>/dev/null || true
  fi
  rm -rf "$REPORT_DIR"
}
trap finish EXIT

changed=$("${GITDIFF[@]}" --name-only "$BASE"..."$HEADREV")
# Content comparisons read files at the merge base: the revision
# `git diff BASE...HEAD` diffs against, so a PR is judged on its own changes
# only, never on what landed on the base branch after it forked.
MERGE_BASE=$(git merge-base "$BASE" "$HEADREV")

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
  [[ "$(matching_lines "$MERGE_BASE" "$1" "$2")" != "$(matching_lines "$HEADREV" "$1" "$2")" ]]
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
  if "${GITDIFF[@]}" "$BASE"..."$HEADREV" -- Makefile | grep -qE '^[+-](VL_VERSION_LOGS|VL_COMMIT_TRACES|VT_VERSION)[[:space:]]*:?='; then
    touches_pin_makefile="Makefile"
  fi
fi

touches_routes=$(printf '%s\n%s\n%s\n' "$touches_direct" "$touches_handle_calls" "$touches_pin_makefile" | sed '/^$/d')
touches_registry=$(echo "$changed" | grep -E '^tests/conformance/(registry/rows/|inventory\.generated\.yaml)' || true)
if [[ -n "$touches_routes" && -z "$touches_registry" ]]; then
  echo "::error::this PR changes route/handler/upstream files but not tests/conformance/registry/rows/ — add or update the rows (see tests/conformance/README.md)"
  plain_list <<< "$touches_routes"
  hard_fail "Routes, handlers, pins" "changes $(echo "$touches_routes" | head -3 | paste -s -d ' ' -) but no registry row" \
    "add or update the rows under tests/conformance/registry/rows/ (tests/conformance/README.md)"
elif [[ -n "$touches_routes" ]]; then
  rule_row "Routes, handlers, pins" PASS "route/pin change, registry rows touched"
else
  rule_row "Routes, handlers, pins" n/a "no route, handler or pin change"
fi

touches_extractors=$(echo "$changed" | grep -E '^tests/conformance/inventory/[^/]+\.go$' | grep -v '_test\.go$' || true)
touches_generated_inventory=$(echo "$changed" | grep -E '^tests/conformance/inventory\.generated\.yaml$' || true)
if [[ -n "$touches_extractors" && -z "$touches_generated_inventory" ]]; then
  echo "::error::this PR changes tests/conformance/inventory/ extractors but not tests/conformance/inventory.generated.yaml — run 'make conformance-gen' and commit the result"
  plain_list <<< "$touches_extractors"
  hard_fail "Inventory extractors" "extractors changed, inventory.generated.yaml not regenerated" \
    "run 'make conformance-gen' and commit the result"
elif [[ -n "$touches_extractors" ]]; then
  rule_row "Inventory extractors" PASS "inventory regenerated"
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
  done <<< "$(comm -13 <(added_leadins "$MERGE_BASE") <(added_leadins "$HEADREV"))"
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
  new_keys=$(comm -13 <(config_keys "$MERGE_BASE" "$f") <(config_keys "$HEADREV" "$f"))
  if [[ -n "$new_keys" ]]; then
    feature_signals="$feature_signals
$f: a new YAML config key: $(echo "$new_keys" | paste -s -d ' ' -)"
  fi
done <<< "$(echo "$changed" | grep -E '^internal/config/[^/]+\.go$' | grep -v '_test\.go$' || true)"

while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  if "${GITDIFF[@]}" "$BASE"..."$HEADREV" -- "$f" | grep -qE '^\+[[:space:]]*-[[:space:]]*id:[[:space:]]*lh\.'; then
    feature_signals="$feature_signals
$f: a new Lakehouse registry row"
  fi
done <<< "$(echo "$changed" | grep -E '^tests/conformance/registry/rows/.*\.yaml$' || true)"

feature_signals=$(echo "$feature_signals" | sed '/^$/d')
touches_features=$(echo "$changed" | grep -E '^tests/conformance/registry/features/' || true)

if [[ -n "$feature_signals" && -z "$touches_features" ]]; then
  echo "::error::this PR adds or extends a Lakehouse feature but does not touch tests/conformance/registry/features/ — every shipped feature must be in the catalog with its rows, tests and docs"
  plain_list <<< "$feature_signals"
  echo "steps:"
  echo "  1. add or update the feature in tests/conformance/registry/features/<area>.yaml"
  echo "     (id, title, status, area, surfaces, rows, tests, docs, highlight, description, changelog_bullets)"
  echo "  2. run 'make conformance-gen' and commit the regenerated docs/features.md and README.md"
  echo "  3. see tests/conformance/README.md, section 'Adding or extending a feature'"
  hard_fail "Feature catalog" "adds or extends a Lakehouse feature ($(echo "$feature_signals" | head -1)) without touching tests/conformance/registry/features/" \
    "add the feature to tests/conformance/registry/features/<area>.yaml, run 'make conformance-gen', commit docs/features.md and README.md (tests/conformance/README.md, 'Adding or extending a feature')"
elif [[ -n "$feature_signals" ]]; then
  rule_row "Feature catalog" PASS "feature signals present, catalog touched"
fi

# --- every product change and every new test is covered in the registry -----
# Owner rule (2026-10-07): no PR may merge unless its behaviour changes are
# covered by registry changes. This section runs from the MERGE BASE in CI (the
# workflow checks out the base's gate code), so a PR cannot weaken the gate that
# judges it; its changes to the gate take effect after merge.
#
#   Rule 1  A product-changing PR (anything under internal/, cmd/,
#           lakehouse-traces/ that is not a test or README/RUNBOOK, anything under
#           patches/, charts/, and shipped build files: Dockerfile, Dockerfile.logs,
#           Dockerfile.traces, root go.mod/go.sum) must change
#           tests/conformance/registry/rows/ or tests/conformance/registry/features/
#           with a REAL content change: comments, blank lines and indentation do
#           not count. See pr_classify.py.
#   Rule 2  Every Test*/Fuzz* function the PR adds in a product package or in
#           tests/{parity,e2e,conformance,ingestmatrix} must be referenced by a
#           row (refs.tests) or a feature (tests:), and references to removed
#           or renamed tests must not be left behind (tests/conformance/cmd/testlinks).
#   Rule 3  Parity fixes ship locks; locks are never weakened (testlinks).
#
# There is no exemption by label or by PR text. What the owner's required approval
# covers (edits of lock code, of the gate's own files) is listed in the report, not
# failed.
#
# Exempt kinds, decided first and only from the content of the diff, read as bytes (pr_classify.py):
#   release-metadata   exact release-metadata shape, by the release bot or an approver;
#                      Rules 1-3 skipped
#   dependency-only    go.mod `require` versions outside the storage-critical modules, go.sum
#                      hash lines, requirements*.txt package lines; no title or commit subject is
#                      read; Rule 1 is waived, Rules 2 and 3 still run
#   toolchain-only     a Go toolchain bump: go/toolchain directives, dependency versions,
#                      golang image tags, plus docs/CI/scripts; Rule 1 is waived, Rules 2
#                      and 3 still run
# Docs-only and CI-only PRs are not product-changing, so Rule 1 passes them by
# classification.
# Env: PR_AUTHOR (optional; the release-metadata kind). PR_TITLE is accepted and ignored.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# The release-metadata approvers come from the merge base, so a PR cannot approve itself. The file
# was named registry-exempt-approvers before there was nothing left to exempt: the new name wins
# when the merge base has it, the old one is read otherwise (an older merge base), and the old file
# stays in the tree for one release so such a base still resolves.
approvers_src=.github/release-metadata-approvers
git cat-file -e "$MERGE_BASE:$approvers_src" 2>/dev/null || approvers_src=.github/registry-exempt-approvers
git show "$MERGE_BASE:$approvers_src" > "$approvers_file" 2>/dev/null || : > "$approvers_file"
# No title and no commit subject is passed: the kind is decided from the diff alone.
CLASSIFY=$(python3 "$HERE/pr_classify.py" --base "$MERGE_BASE" --head "$HEADREV" \
  --author "${PR_AUTHOR:-}" --approvers "$approvers_file") || { echo "::error::pr_classify.py failed"; exit 1; }
class_field() { sed -n "s/^$1=//p" <<< "$CLASSIFY" | head -1; }
exempt_kind=$(class_field exempt)
is_product=$(class_field product)
product_reason=$(class_field reason)
exempt_detail=$(class_field detail)
GATE_TOUCHED=$(sed -n 's/^gate_file=//p' <<< "$CLASSIFY")

gate_annotations

if [[ "$exempt_kind" != none ]]; then
  KIND_LINE="PR kind: \`$exempt_kind\` (decided from the diff: $(md_esc "$exempt_detail"))"
elif [[ "$is_product" == 1 ]]; then
  KIND_LINE="PR kind: product change ($(md_esc "$product_reason"))"
else
  KIND_LINE="PR kind: not a product change (tests, docs, CI or tooling only)"
fi

case "$exempt_kind" in
release-metadata)
  echo "registry coverage gate: skipped ($exempt_kind PR)"
  rule_row "Rules 1-3" n/a "skipped ($exempt_kind PR, verified from the diff)"
  ;;
*)
  # Rules 1-3 are decided where the registry is parsed: testlinks. A toolchain-only or
  # dependency-only PR is not product-changing for Rule 1 (no -product), but Rules 2 and 3 still run.
  product_args=()
  [[ "$is_product" == 1 ]] && product_args=(-product "$product_reason")
  tl_bin=${TESTLINKS_BIN:-}
  built=""
  if [[ -z "$tl_bin" ]]; then
    if command -v go >/dev/null 2>&1 && [[ -d "$HERE/../../tests/conformance/cmd/testlinks" ]]; then
      tl_bin="$REPORT_DIR/testlinks"
      built=1
      (cd "$HERE/../.." && GOWORK=off go build -o "$tl_bin" ./tests/conformance/cmd/testlinks) || { echo "::error::cannot build tests/conformance/cmd/testlinks"; exit 1; }
    else
      echo "::error::cannot run tests/conformance/cmd/testlinks (no Go toolchain)"
      exit 1
    fi
  fi
  rc=0
  "$tl_bin" -repo . -base "$MERGE_BASE" -head "$HEADREV" -report-dir "$REPORT_DIR" ${product_args[@]+"${product_args[@]}"} || rc=$?
  [[ -z "$built" ]] || rm -f "$tl_bin"
  if [[ $rc -ne 0 ]]; then
    HARD_FAIL=1
    if [[ ! -f "$REPORT_DIR/rules.md" ]]; then  # a tool error: no rule verdicts were written
      rule_row "Rules 1-3" FAIL "testlinks failed to run (exit $rc), see the log"
    fi
  fi
  ;;
esac

[[ -z "$HARD_FAIL" ]] || exit 1
echo "registry-touch check OK"
