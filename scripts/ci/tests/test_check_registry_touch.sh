#!/usr/bin/env bash
# Tests for scripts/ci/check_registry_touch.sh, run against synthetic git
# repositories: each case builds a base commit, commits a change on top, and
# asserts the checker's verdict. The checker is the gate that keeps a feature
# PR from merging without its catalog entry, so its own logic needs coverage
# that does not depend on this repository's current contents.
#
# Usage: bash scripts/ci/tests/test_check_registry_touch.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECKER="$SCRIPT_DIR/../check_registry_touch.sh"
MATERIALIZE="$SCRIPT_DIR/../materialize_unreleased.sh"
[[ -f "$CHECKER" ]] || { echo "checker not found at $CHECKER" >&2; exit 2; }
[[ -f "$MATERIALIZE" ]] || { echo "release script not found at $MATERIALIZE" >&2; exit 2; }

pass=0
fail=0

# confgen cannot run inside a fixture repo (no Go module), and the fixtures
# deliberately have no tests/conformance tree; skip that part of the check.
export SKIP_CONFGEN_CHECK=1

# The test-link gate is a Go program (tests/conformance/cmd/testlinks); the
# fixture repos have no Go module, so build it once from this checkout.
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TESTLINKS_BIN="$(mktemp -d)/testlinks"
if ! (cd "$REPO_ROOT" && GOWORK=off go build -o "$TESTLINKS_BIN" ./tests/conformance/cmd/testlinks); then
  echo "cannot build testlinks" >&2
  exit 2
fi
export TESTLINKS_BIN

# new_repo creates a repo with an initial commit and echoes its path. The base
# changelog has an [Unreleased] section with `### Added` and `### Fixed`
# bullets above a released version, so a case can exercise the section
# tracking and the release workflow's materialization; internal/x/routes.go
# and internal/config/config.go give the content-based signals something to
# compare against.
new_repo() {
  local dir
  dir="$(mktemp -d)"
  (
    cd "$dir" || exit 1
    git init -q -b main
    git config user.email t@example.com
    git config user.name t
    mkdir -p internal/x internal/config cmd/lakehouse-logs tests/conformance/registry/rows/lh tests/conformance/registry/features
    cat > CHANGELOG.md <<'FIXTURE'
# Changelog

## [Unreleased]

### Added

- **Unreleased thing.** text.
- **Another unreleased thing.** text.

### Fixed

- **Unreleased fix.** text.

## [1.0.0]

### Added

- **Existing thing.** text.
FIXTURE
    printf 'package x\n\nfunc F() {}\n' > internal/x/x.go
    cat > internal/x/routes.go <<'FIXTURE'
package x

func register(mux Mux) {
	mux.HandleFunc("/lakehouse/api/v1/a", a)
	mux.HandleFunc("/lakehouse/api/v1/b", b)
}
FIXTURE
    cat > internal/config/config.go <<'FIXTURE'
package config

type InsertConfig struct {
	FlushInterval  string `yaml:"flush_interval"`
	TargetFileSize string `yaml:"target_file_size"`
}

type S3Config struct {
	Bucket string `yaml:"bucket"`
	Hidden string `yaml:"-"`
}
FIXTURE
    printf -- '- id: lh.existing.row\n  title: existing\n  refs:\n    tests:\n      - tests/parity/parity_test.go#TestOldParity\n' > tests/conformance/registry/rows/lh/endpoints.yaml
    mkdir -p tests/parity docs .github/workflows scripts/ci
    printf 'package parity\n\nimport "testing"\n\nfunc TestOldParity(t *testing.T) {}\n' > tests/parity/parity_test.go
    printf 'module x\n' > go.mod
    printf -- '- id: lh.feature.storage.existing\n  title: existing\n' > tests/conformance/registry/features/storage.yaml
    git add -A
    git commit -q -m base
    git branch -q base
  ) >/dev/null 2>&1
  echo "$dir"
}

# run_case <name> <want: ok|fail> <expected message fragment or ""> -- commands...
#
# When PRECONDITION is set (e.g. `PRECONDITION='…' run_case …`), it is
# evaluated in the fixture repo after the change is committed and must
# succeed: it proves the fixture really has the shape the case is about, so a
# case cannot pass vacuously because its setup silently did something else.
run_case() {
  local name="$1" want="$2" fragment="$3"
  shift 3
  local dir out rc pre_ok=1
  dir="$(new_repo)"
  (
    cd "$dir" || exit 1
    "$@"
    git add -A
    git commit -q -m "${COMMIT_MSG:-change}"
  ) >/dev/null 2>&1
  if [[ -n "${PRECONDITION:-}" ]] && ! (cd "$dir" && eval "$PRECONDITION") >/dev/null 2>&1; then
    pre_ok=0
  fi
  out="$(cd "$dir" && bash "$CHECKER" base 2>&1)"
  rc=$?
  rm -rf "$dir"

  local ok=1
  if [[ "$want" == ok && $rc -ne 0 ]]; then ok=0; fi
  if [[ "$want" == fail && $rc -eq 0 ]]; then ok=0; fi
  if [[ -n "$fragment" ]] && ! grep -qF -- "$fragment" <<<"$out"; then ok=0; fi
  [[ $pre_ok -eq 1 ]] || ok=0

  if [[ $ok -eq 1 ]]; then
    echo "ok   - $name"
    pass=$((pass + 1))
  else
    echo "FAIL - $name (rc=$rc, want $want$([[ $pre_ok -eq 1 ]] || echo ', precondition failed'))"
    echo "$out" | sed 's/^/       /'
    fail=$((fail + 1))
  fi
}

add_changelog_bullet() {
  printf -- '- **A brand new feature.** text.\n' >> CHANGELOG.md
}
add_route() {
  printf 'package main\n\nfunc main() { mux.HandleFunc("/lakehouse/api/v1/new", nil) }\n' > cmd/lakehouse-logs/main.go
}
add_lh_row() {
  printf -- '- id: lh.brand.new\n  title: new\n' >> tests/conformance/registry/rows/lh/endpoints.yaml
}
touch_features() {
  printf -- '- id: lh.feature.storage.new\n  title: new\n' >> tests/conformance/registry/features/storage.yaml
}
touch_rows() {
  printf -- '  notes: touched\n' >> tests/conformance/registry/rows/lh/endpoints.yaml
}
touch_unrelated() {
  printf 'doc\n' >> README.md
}
add_test_only_handler() {
  mkdir -p internal/x
  printf 'package x\n\nfunc registerForTest() { mux.HandleFunc("/x", nil) }\n' > internal/x/x_test.go
}

# rewrite <file> <awk program>: rewrites a fixture file in place with awk
# (portable across the BSD and GNU tool sets, unlike `sed -i`).
rewrite() {
  local file="$1" program="$2"
  awk "$program" "$file" > "$file.tmp" && mv "$file.tmp" "$file"
}

# materialize_release <version>: the release workflow's CHANGELOG step — the
# very script .github/workflows/auto-release.yaml runs, which turns
# `## [Unreleased]` into an empty [Unreleased] followed by `## [<version>]`.
# That script relies on GNU sed expanding `\n` in a replacement, which BSD sed
# (macOS) does not do; there the identical edit is applied with awk instead, so
# the case models the same transformation on every developer machine while CI
# (GNU sed) runs the real script.
materialize_release() {
  if sed --version >/dev/null 2>&1; then
    CHANGELOG=CHANGELOG.md bash "$MATERIALIZE" "$1"
  else
    rewrite CHANGELOG.md '{ print } /^## \[Unreleased\]$/ { print ""; print "## ['"$1"'] - 2026-01-01" }'
  fi
}

# reassemble_release <version>: a hand-assembled release-metadata PR — the
# shape of the one that materialized four releases at once: the [Unreleased]
# bullets are deleted and re-emitted, reordered, under a new version heading,
# so the diff carries `+- **` lines for bullets that are not new at all.
reassemble_release() {
  cat > CHANGELOG.md <<FIXTURE
# Changelog

## [Unreleased]

## [$1] - 2026-01-01

### Fixed

- **Unreleased fix.** text.

### Added

- **Another unreleased thing.** text.
- **Unreleased thing.** text.

## [1.0.0]

### Added

- **Existing thing.** text.
FIXTURE
}

added_bullet_then_release() {
  rewrite CHANGELOG.md '{ print } /^- \*\*Another unreleased thing\.\*\*/ { print "- **A brand new feature.** text." }'
  materialize_release 9.9.9
}
fixed_bullet_in_unreleased() {
  rewrite CHANGELOG.md '{ print } /^- \*\*Unreleased fix\.\*\*/ { print "- **Another fix.** text." }'
}
reword_bullet_body() {
  rewrite CHANGELOG.md '{ sub(/^- \*\*Unreleased thing\.\*\* text\./, "- **Unreleased thing.** reworded text, same lead-in."); print }'
}
move_route_within_file() {
  # Line a moves below line b and is re-indented with spaces.
  rewrite internal/x/routes.go '/api\/v1\/a"/ { held = $0; next } { print } /api\/v1\/b"/ { print "    " held }'
}
add_route_to_routes_file() {
  rewrite internal/x/routes.go '{ print } /api\/v1\/b"/ { print "\tmux.HandleFunc(\"/lakehouse/api/v1/c\", c)" }'
}
add_route_to_routes_file_with_rows() {
  add_route_to_routes_file
  touch_rows
}
remove_route_with_rows() {
  rewrite internal/x/routes.go '!/api\/v1\/b"/'
  touch_rows
}
add_config_key() {
  rewrite internal/config/config.go '{ print } /yaml:"target_file_size"/ { print "\tMaxBufferRows  int    `yaml:\"max_buffer_rows\"`" }'
}
add_existing_key_name_to_other_struct() {
  rewrite internal/config/config.go '{ print } /yaml:"bucket"/ { print "\tFlushInterval string `yaml:\"flush_interval\"`" }'
}
move_config_field() {
  rewrite internal/config/config.go '/yaml:"flush_interval"/ { held = $0; next } { print } /yaml:"target_file_size"/ { print held }'
}
add_config_key_in_test_file() {
  cat > internal/config/config_test.go <<'FIXTURE'
package config

type fixture struct {
	Knob string `yaml:"knob"`
}
FIXTURE
}

echo "== feature-PR classification =="
run_case "changelog Added bullet without catalog fails" fail \
  "does not touch tests/conformance/registry/features/" add_changelog_bullet
run_case "changelog Added bullet with catalog passes" ok "" \
  bash -c 'printf -- "- **A brand new feature.** text.\n" >> CHANGELOG.md; printf -- "- id: lh.feature.storage.new\n  title: new\n" >> tests/conformance/registry/features/storage.yaml'
run_case "new route without catalog fails" fail \
  "a route, handler or lakehouse.* flag" bash -c 'printf "package main\n\nfunc main() { mux.HandleFunc(\"/lakehouse/api/v1/new\", nil) }\n" > cmd/lakehouse-logs/main.go; printf -- "  notes: touched\n" >> tests/conformance/registry/rows/lh/endpoints.yaml'
run_case "new lakehouse flag without catalog fails" fail \
  "a route, handler or lakehouse.* flag" bash -c 'printf "package main\n\nvar f = flag.String(\"lakehouse.new.knob\", \"\", \"doc\")\n" > cmd/lakehouse-logs/flags.go; printf -- "  notes: touched\n" >> tests/conformance/registry/rows/lh/endpoints.yaml'
run_case "new lh registry row without catalog fails" fail \
  "a new Lakehouse registry row" add_lh_row
run_case "failure message lists the steps" fail \
  "run 'make conformance-gen'" add_changelog_bullet

echo
echo "== non-feature changes stay green =="
run_case "unrelated doc change passes" ok "registry-touch check OK" touch_unrelated
run_case "test-only handler registration is not a feature signal" ok "" add_test_only_handler
run_case "catalog-only change passes" ok "" touch_features

echo
echo "== the changelog signal reads '### Added' sections only =="
run_case "a bold bullet in a new '### Fixed' section is not a feature signal" ok "registry-touch check OK" \
  bash -c 'printf -- "\n### Fixed\n\n- **A released fix.** text.\n" >> CHANGELOG.md'
run_case "a bold bullet in a new '### Changed' section is not a feature signal" ok "registry-touch check OK" \
  bash -c 'printf -- "\n### Changed\n\n- **A behavior change.** text.\n" >> CHANGELOG.md'
run_case "a bold bullet added to the existing [Unreleased] '### Fixed' section is not a feature signal" ok "registry-touch check OK" \
  fixed_bullet_in_unreleased
run_case "an edited bullet body under an unchanged lead-in is not a feature signal" ok "registry-touch check OK" \
  reword_bullet_body
PRECONDITION='grep -q "^## \[9\.9\.9\] - " CHANGELOG.md && grep -q "^## \[Unreleased\]$" CHANGELOG.md' \
  run_case "the release workflow's materialization of [Unreleased] is not a feature signal" ok "registry-touch check OK" \
  materialize_release 9.9.9
PRECONDITION='git diff base...HEAD -- CHANGELOG.md | grep -q "^+- \*\*"' \
  run_case "bullets re-emitted under a new version heading, [Unreleased] emptied, are not a feature signal" ok "registry-touch check OK" \
  reassemble_release 9.9.9
PRECONDITION='grep -q "^## \[9\.9\.9\] - " CHANGELOG.md' \
  run_case "a new '### Added' lead-in is still a feature signal alongside a materialization" fail \
  "CHANGELOG.md: a new '### Added' bullet: A brand new feature." \
  added_bullet_then_release

echo
echo "== registrations and config keys are compared as sets =="
PRECONDITION='git diff base...HEAD -- internal/x/routes.go | grep -q "^+.*HandleFunc"' \
  PR_LABELS=registry-exempt PR_BODY='Registry: none — refactor only, no behaviour change' \
  run_case "a registration moved within its file is neither a route change nor a feature signal" ok "registry-touch check OK" \
  move_route_within_file
run_case "a registration added to a non-entrypoint file still needs registry rows" fail \
  "but not tests/conformance/registry/rows/" add_route_to_routes_file
run_case "a registration added to a non-entrypoint file is a feature signal" fail \
  "internal/x/routes.go: a route, handler or lakehouse.* flag" add_route_to_routes_file_with_rows
run_case "a removed registration is a feature signal" fail \
  "internal/x/routes.go: a route, handler or lakehouse.* flag" remove_route_with_rows
run_case "a new YAML config key without the catalog fails" fail \
  "internal/config/config.go: a new YAML config key: InsertConfig.max_buffer_rows" add_config_key
run_case "an existing key name added to another config struct is a new key" fail \
  "a new YAML config key: S3Config.flush_interval" add_existing_key_name_to_other_struct
PRECONDITION='git diff base...HEAD -- internal/config/config.go | grep -q "^+.*yaml:"' \
  PR_LABELS=registry-exempt PR_BODY='Registry: none — refactor only, no behaviour change' \
  run_case "a config field moved within its struct is not a feature signal" ok "registry-touch check OK" \
  move_config_field
run_case "a YAML key declared in a config test file is not a feature signal" ok "registry-touch check OK" \
  add_config_key_in_test_file

echo
echo "== the pre-existing row rule still holds =="
run_case "route change without rows fails" fail \
  "but not tests/conformance/registry/rows/" add_route
run_case "route change with rows and catalog passes" ok "" \
  bash -c 'printf "package main\n\nfunc main() { mux.HandleFunc(\"/x\", nil) }\n" > cmd/lakehouse-logs/main.go; printf -- "  notes: touched\n" >> tests/conformance/registry/rows/lh/endpoints.yaml; printf -- "- id: lh.feature.storage.new\n  title: new\n" >> tests/conformance/registry/features/storage.yaml'


echo
echo "== every product change and every new test is covered in the registry =="
product_change() { printf 'package x\n\nfunc F() { println("changed") }\n' > internal/x/x.go; }
registry_row_change() { printf -- '  notes: real change\n' >> tests/conformance/registry/rows/lh/endpoints.yaml; }
product_and_row() { product_change; registry_row_change; }
product_and_feature() { product_change; printf -- '- id: lh.feature.storage.new\n  title: new\n' >> tests/conformance/registry/features/storage.yaml; }
product_and_whitespace_registry() {
  product_change
  printf '\n# only a comment\n\n' >> tests/conformance/registry/rows/lh/endpoints.yaml
  printf -- '- id: lh.feature.storage.existing\n    title: existing\n' > tests/conformance/registry/features/storage.yaml
}
add_parity_test() {
  printf '\nfunc TestNewParity(t *testing.T) {}\n' >> tests/parity/parity_test.go
}
new_test_unreferenced() { registry_row_change; add_parity_test; }
new_test_referenced() {
  add_parity_test
  printf -- '  refs:\n    tests:\n      - tests/parity/parity_test.go#TestNewParity\n' >> tests/conformance/registry/features/storage.yaml
}
new_test_in_linked_file() {
  add_parity_test
  printf -- '- id: lh.feature.storage.f\n  title: f\n  tests:\n    - tests/parity/parity_test.go\n' >> tests/conformance/registry/features/storage.yaml
}
new_unit_test_unreferenced() {
  registry_row_change
  printf 'package x\n\nimport "testing"\n\nfunc TestNewUnit(t *testing.T) {}\n' > internal/x/new_test.go
}
new_test_helper_not_a_test() {
  registry_row_change
  printf 'package x\n\nimport "testing"\n\nfunc TestMain(m *testing.M) {}\nfunc helper(t *testing.T) {}\nfunc Testify(t *testing.T) {}\n' > internal/x/new_test.go
}
rename_referenced_test() {
  registry_row_change
  sed -i.bak 's/TestOldParity/TestRenamedParity/' tests/parity/parity_test.go && rm -f tests/parity/parity_test.go.bak
}
rename_and_relink_test() {
  rename_referenced_test
  sed -i.bak 's/TestOldParity/TestRenamedParity/' tests/conformance/registry/rows/lh/endpoints.yaml && rm -f tests/conformance/registry/rows/lh/endpoints.yaml.bak
}
delete_referenced_test_file() { registry_row_change; git rm -q tests/parity/parity_test.go; }
move_test_to_other_file() {
  registry_row_change
  git mv tests/parity/parity_test.go tests/parity/moved_test.go
  sed -i.bak 's/parity_test.go/moved_test.go/' tests/conformance/registry/rows/lh/endpoints.yaml && rm -f tests/conformance/registry/rows/lh/endpoints.yaml.bak
}
dependency_bump() { printf 'module x\n\nrequire y v1.2.3\n' > go.mod; printf 'a==1\n' > requirements.txt; }
docs_only() { printf '# doc\n' > docs/x.md; }
ci_only() { printf 'name: x\n' > .github/workflows/x.yaml; printf '#!/bin/sh\n' > scripts/ci/foo.sh; }

run_case "a product change without a registry change fails" fail \
  "makes no real content change under tests/conformance/registry/rows/" product_change
run_case "the failure points at the README" fail "tests/conformance/README.md" product_change
run_case "a product change with a real rows change passes" ok "registry-touch check OK" product_and_row
run_case "a product change with a features change passes" ok "registry-touch check OK" product_and_feature
run_case "a whitespace/comment-only registry change does not count" fail \
  "makes no real content change" product_and_whitespace_registry
run_case "a new parity test nobody references fails" fail \
  "tests/parity/parity_test.go#TestNewParity" new_test_unreferenced
run_case "a new unit test in a product package nobody references fails" fail \
  "internal/x/new_test.go#TestNewUnit" new_unit_test_unreferenced
run_case "a new parity test referenced by a feature passes" ok "testlinks OK (1 tests added, all linked)" new_test_referenced
run_case "a new test in a file-level linked file passes" ok "testlinks OK" new_test_in_linked_file
run_case "TestMain, helpers and Test-prefixed non-tests need no link" ok "testlinks OK (0 tests added" new_test_helper_not_a_test
run_case "a renamed test whose old name is still referenced fails" fail \
  "no \`func TestOldParity(\` in tests/parity/parity_test.go" rename_referenced_test
run_case "a renamed test is reported as added and unlinked too" fail \
  "tests/parity/parity_test.go#TestRenamedParity" rename_referenced_test
run_case "a renamed test with the reference updated passes" ok "testlinks OK" rename_and_relink_test
run_case "a deleted test file whose reference remains fails" fail \
  "registry references point at tests this PR removed" delete_referenced_test_file
run_case "a test moved to another file of its package with the reference updated passes" ok "testlinks OK (0 tests added" \
  move_test_to_other_file

echo
echo "== exemptions =="
run_case "a release-metadata PR passes" ok "skipped (release-metadata PR)" materialize_release 9.9.9
COMMIT_MSG='build(deps): bump y' run_case "a dependency-only PR passes" ok "skipped (dependency-only PR)" dependency_bump
run_case "a go.mod change under a non-dependency commit is a product change" fail "release-impacting" dependency_bump
run_case "a docs-only PR passes" ok "registry-touch check OK" docs_only
run_case "a CI-only PR passes" ok "registry-touch check OK" ci_only
PR_LABELS='bug,registry-exempt' run_case "the exempt label without the body section fails" fail \
  "no 'Registry: none" product_change
PR_LABELS='registry-exempt' PR_BODY='Registry: none —' run_case "the exempt label with an empty reason fails" fail \
  "no 'Registry: none" product_change
PR_BODY='Registry: none — pure refactor, no behaviour change' run_case "the body section without the label does not exempt" fail \
  "makes no real content change" product_change
PR_LABELS='bug,registry-exempt' PR_BODY=$'## Summary\nx\n\nRegistry: none — pure refactor, no behaviour change' \
  run_case "the exempt label with the body section passes" ok "registry-exempt label" product_change
PR_LABELS='registry-exempt' PR_BODY='Registry: none — tests only' run_case "the exemption also covers unlinked new tests" ok "skipped" new_test_unreferenced

echo
echo "== the generated documents must be current on a feature PR =="

# The cases above set SKIP_CONFGEN_CHECK=1, because a synthetic fixture repo
# has no Go module to run confgen in. This last case exercises the part they
# cannot: the checker's own `confgen -check` sub-step, against a real clone of
# this repository, in both directions — stale documents must fail, and the
# same tree must pass once regenerated.
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# clone_repo <case name>: echoes the path of a throwaway clone of this
# repository with the vendored upstream sources linked in, or prints a skip
# line and fails where confgen cannot run.
clone_repo() {
  local name="$1" tmp
  if ! command -v go >/dev/null 2>&1; then
    echo "skip - $name (no go toolchain)" >&2
    return 1
  fi
  if [[ ! -f "$REPO_ROOT/deps/VictoriaLogs/go.mod" || ! -f "$REPO_ROOT/lakehouse-traces/deps/VictoriaTraces/go.mod" ]]; then
    echo "skip - $name (upstream deps missing; run: make deps-logs deps-traces deps-vt)" >&2
    return 1
  fi
  tmp="$(mktemp -d)"
  if ! git clone --quiet --shared "$REPO_ROOT" "$tmp/repo" 2>/dev/null; then
    rm -rf "$tmp"
    echo "skip - $name (cannot clone the repository)" >&2
    return 1
  fi
  # confgen extracts the upstream inventory from the vendored sources, which
  # are .gitignored and therefore absent from the clone: link them in, and
  # keep the links out of the fixture commits (`/deps/` ignores a directory,
  # not a symlink).
  ln -s "$REPO_ROOT/deps" "$tmp/repo/deps"
  ln -s "$REPO_ROOT/lakehouse-traces/deps" "$tmp/repo/lakehouse-traces/deps"
  printf '/deps\n/lakehouse-traces/deps\n' >> "$tmp/repo/.git/info/exclude"
  (cd "$tmp/repo" && git config user.email t@example.com && git config user.name t)
  echo "$tmp"
}

run_confgen_case() {
  local name="the checker fails a feature PR with stale generated docs, and passes once regenerated"
  local tmp
  tmp="$(clone_repo "$name")" || return 0

  local out rc stale_out stale_rc
  (
    cd "$tmp/repo" || exit 1
    git branch -q base

    # A feature PR: a new route (feature + route signal), the rows touched as
    # the pre-existing rule demands, and a real catalog change — but the
    # generated documents are NOT regenerated.
    mkdir -p internal/touchcheckfixture
    printf 'package touchcheckfixture\n\nimport "net/http"\n\nfunc register(mux *http.ServeMux) { mux.HandleFunc("/touchcheck", nil) }\n' \
      > internal/touchcheckfixture/register.go
    printf '\n# touched by scripts/ci/tests/test_check_registry_touch.sh\n' \
      >> tests/conformance/registry/rows/lh/endpoints.yaml
    cat >> tests/conformance/registry/features/ops.yaml <<'FIXTURE'

- id: lh.feature.ops.touch_check_fixture
  title: Touch-check fixture feature
  status: planned
  area: ops
  surfaces: [cli]
  highlight: "**Fixture**: added by scripts/ci/tests/test_check_registry_touch.sh."
  description: >-
    A planned fixture feature the touch-check test appends to a throwaway clone to make the
    generated documents stale. It never exists in the repository itself.
FIXTURE
    git add -A
    git commit -q -m "feature PR with stale generated docs"
  ) >/dev/null 2>&1

  stale_out="$(cd "$tmp/repo" && SKIP_CONFGEN_CHECK= bash "$CHECKER" base 2>&1)"
  stale_rc=$?

  ( cd "$tmp/repo" && GOWORK=off go run ./tests/conformance/cmd/confgen -write && git add -A && git commit -q -m "make conformance-gen" ) >/dev/null 2>&1

  out="$(cd "$tmp/repo" && SKIP_CONFGEN_CHECK= bash "$CHECKER" base 2>&1)"
  rc=$?
  rm -rf "$tmp"

  local ok=1
  [[ $stale_rc -eq 0 ]] && ok=0
  grep -qF "stale:" <<<"$stale_out" || ok=0
  grep -qF "docs/features.md" <<<"$stale_out" || ok=0
  grep -qF "run 'make conformance-gen'" <<<"$stale_out" || ok=0
  [[ $rc -ne 0 ]] && ok=0
  grep -qF "registry-touch check OK" <<<"$out" || ok=0

  if [[ $ok -eq 1 ]]; then
    echo "ok   - $name"
    pass=$((pass + 1))
  else
    echo "FAIL - $name (stale rc=$stale_rc want non-zero, regenerated rc=$rc want 0)"
    echo "$stale_out" | sed 's/^/       stale: /'
    echo "$out" | sed 's/^/       fixed: /'
    fail=$((fail + 1))
  fi
}

run_confgen_case

echo
echo "== a release-metadata commit passes both gates without regenerating =="

# After every release the release workflow opens a PR that only rewrites
# CHANGELOG.md, moving the [Unreleased] bullets under the new version heading;
# it never regenerates docs/features.md. This case replays that against a real
# clone: a feature whose changelog entry is unreleased is merged with its
# regenerated documents, then a release commit materializes [Unreleased] the
# way the workflow does. On that commit `confgen -check` and the checker must
# both pass untouched; regenerating must then name the new version in
# docs/features.md with no catalog change, and stay current and byte-stable;
# a second release on top of the unregenerated commit must pass as well.
run_materialization_case() {
  local name="a release-metadata commit passes confgen -check and the checker; regeneration names the version without catalog changes"
  local tmp
  tmp="$(clone_repo "$name")" || return 0

  local fixture_line='`lh.feature.ops.touch_check_release_fixture` · status: shipped · since:'
  local log="$tmp/log" ok=1 why=""
  : > "$log"
  # check_step <description> <command...>: runs the command in the clone and
  # records the description when it fails.
  check_step() {
    local what="$1"
    shift
    if ! (cd "$tmp/repo" && "$@") >>"$log" 2>&1; then
      ok=0
      why="$why
       - $what"
    fi
  }
  commit_release() {
    materialize_release "$1" && git add CHANGELOG.md && git commit -q -m "chore: release metadata for v$1 [skip release]"
  }

  (
    cd "$tmp/repo" || exit 1
    # A merged feature PR: an unreleased `### Added` entry, its catalog entry,
    # and the documents regenerated as the checker requires.
    rewrite CHANGELOG.md '{ print } /^## \[Unreleased\]$/ { print ""; print "### Added"; print ""; print "- **Touch-check release fixture.** Added by scripts/ci/tests/test_check_registry_touch.sh." }'
    cat >> tests/conformance/registry/features/ops.yaml <<'FIXTURE'

- id: lh.feature.ops.touch_check_release_fixture
  title: Touch-check release fixture
  status: shipped
  area: ops
  surfaces: [cli]
  tests:
    - scripts/ci/tests/test_check_registry_touch.sh
  highlight: "**Release fixture**: added by scripts/ci/tests/test_check_registry_touch.sh."
  description: >-
    A shipped fixture feature whose changelog entry is unreleased, appended to a throwaway clone
    to replay a release. It never exists in the repository itself.
  changelog_bullets:
    - 'Touch-check release fixture.'
FIXTURE
    GOWORK=off go run ./tests/conformance/cmd/confgen -write
    git add -A
    git commit -q -m "feature merged"
    git branch -q base

    # The release workflow's commit: CHANGELOG.md only.
    commit_release 99.0.0
    git branch -q release
  ) >>"$log" 2>&1

  check_step "the fixture feature is committed rendered as not yet released" \
    grep -qF "$fixture_line the release after v" docs/features.md
  check_step "the release commit touches CHANGELOG.md only" \
    bash -c '[[ "$(git diff --name-only base release)" == "CHANGELOG.md" ]]'
  check_step "confgen -check passes the unregenerated release commit" \
    env GOWORK=off go run ./tests/conformance/cmd/confgen -check
  check_step "confgen -check notes the still-true release references" \
    bash -c 'GOWORK=off go run ./tests/conformance/cmd/confgen -check | grep -q "^current: .*docs/features.md"'
  check_step "the checker passes the release commit" \
    env SKIP_CONFGEN_CHECK= bash "$CHECKER" base

  check_step "regeneration succeeds" env GOWORK=off go run ./tests/conformance/cmd/confgen -write
  check_step "regeneration names the released version" \
    grep -qF "$fixture_line v99.0.0 · surfaces: cli" docs/features.md
  check_step "regeneration changes docs/features.md" bash -c '! git diff --quiet -- docs/features.md'
  check_step "regeneration changes no catalog file" git diff --quiet -- tests/conformance/registry/features
  check_step "the regenerated documents are exactly current" \
    bash -c 'out=$(GOWORK=off go run ./tests/conformance/cmd/confgen -check) && ! grep -q "^current:" <<<"$out"'
  check_step "a second regeneration is byte-stable" \
    bash -c 'GOWORK=off go run ./tests/conformance/cmd/confgen -write | grep -qx "up to date"'

  # A second release on top of the unregenerated release commit.
  check_step "back to the release commit's documents" git checkout -q -- docs/features.md
  check_step "a second release is committed" commit_release 99.1.0
  check_step "confgen -check passes a second unregenerated release" \
    env GOWORK=off go run ./tests/conformance/cmd/confgen -check
  check_step "the checker passes the second release commit" \
    env SKIP_CONFGEN_CHECK= bash "$CHECKER" release

  if [[ $ok -eq 1 ]]; then
    echo "ok   - $name"
    pass=$((pass + 1))
  else
    echo "FAIL - $name:$why"
    sed 's/^/       log: /' "$log"
    fail=$((fail + 1))
  fi
  rm -rf "$tmp"
}

run_materialization_case

echo
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
