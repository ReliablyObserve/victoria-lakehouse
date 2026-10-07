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
    printf '# approvers\nszibis\n' > .github/registry-exempt-approvers
    printf 'jobs:\n  parity:\n    steps:\n      - run: |\n          python scripts/ci/parity_ratchet.py \\\n            --allowlist tests/parity/known_failures.txt \\\n            --summary-file x\n' > .github/workflows/parity.yaml
    mkdir -p internal/ui/static charts/victoria-lakehouse/templates
    printf 'window.x = 0;\n' > internal/ui/static/lakehouse-ui.js
    printf 'ARG VL_VERSION=v1.0.0\n' > Dockerfile.logs
    printf 'ARG X=1\n' > Dockerfile.loki-vl-proxy
    printf 'apiVersion: v2\nname: c\ndescription: d\nversion: 1.0.0\nappVersion: "1.0.0"\n' > charts/victoria-lakehouse/Chart.yaml
    printf '# chart\n' > charts/victoria-lakehouse/README.md
    printf '#!/bin/sh\n' > charts/victoria-lakehouse/test_templates.sh
    printf '# x\nversion 1.0.0 here\n' > README.md
    printf 'module x\n\ngo 1.22\n\nrequire (\n\tgithub.com/a/b v1.0.0\n\tgithub.com/klauspost/compress v1.0.0\n\tgithub.com/golang/snappy v1.0.0\n\tgoogle.golang.org/protobuf v1.0.0\n\tgithub.com/pierrec/lz4/v4 v4.0.0\n\tgithub.com/VictoriaMetrics/c v1.0.0\n)\n' > go.mod
    printf 'VL_VERSION_LOGS := v1.0.0\nVL_COMMIT_TRACES := abc123\nVT_VERSION := v0.1.0\nall:\n' > Makefile
    printf 'x\n' > go.sum
    printf '# floors\nTestOldParity  5  # lock\n' > tests/parity/lock_cells.txt
    printf '# allowlist\nTestOldParity  # B1: x\nTestParity_B  # B2: y\n' > tests/parity/known_failures.txt
    printf '| Id | Divergence |\n|---|---|\n| **B1** | open one |\n| **B2** | open two |\n| **Old thing (B0)** | **Resolved** | done |\n' > docs/parity-and-gaps.md
    cat >> tests/conformance/registry/rows/lh/endpoints.yaml <<'ROWS'
- {id: lh.row.gap, title: gap, expect: differ, compare: {type: ndjson-multiset}}
- {id: lh.row.lock, title: lock, expect: pass, compare: {type: exact-json}, request: {method: GET, path: /a}, refs: {tests: [tests/parity/parity_test.go#TestOldParity]}}
- {id: lh.row.vwh, title: vwh, expect: pass, compare: {type: values-with-hits, options: {hits_tolerance: "0"}}, request: {method: GET, path: /b}}
ROWS
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
  printf -- '- {id: lh.row.touched, title: t}\n' >> tests/conformance/registry/rows/lh/endpoints.yaml
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
  "a route, handler or lakehouse.* flag" bash -c 'printf "package main\n\nfunc main() { mux.HandleFunc(\"/lakehouse/api/v1/new\", nil) }\n" > cmd/lakehouse-logs/main.go; printf -- "- {id: lh.row.touched, title: t}\n" >> tests/conformance/registry/rows/lh/endpoints.yaml'
run_case "new lakehouse flag without catalog fails" fail \
  "a route, handler or lakehouse.* flag" bash -c 'printf "package main\n\nvar f = flag.String(\"lakehouse.new.knob\", \"\", \"doc\")\n" > cmd/lakehouse-logs/flags.go; printf -- "- {id: lh.row.touched, title: t}\n" >> tests/conformance/registry/rows/lh/endpoints.yaml'
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
  EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY='Registry: none — refactor only, no behaviour change' \
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
  EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY='Registry: none — refactor only, no behaviour change' \
  run_case "a config field moved within its struct is not a feature signal" ok "registry-touch check OK" \
  move_config_field
run_case "a YAML key declared in a config test file is not a feature signal" ok "registry-touch check OK" \
  add_config_key_in_test_file

echo
echo "== the pre-existing row rule still holds =="
run_case "route change without rows fails" fail \
  "but not tests/conformance/registry/rows/" add_route
run_case "route change with rows and catalog passes" ok "" \
  bash -c 'printf "package main\n\nfunc main() { mux.HandleFunc(\"/x\", nil) }\n" > cmd/lakehouse-logs/main.go; printf -- "- {id: lh.row.touched, title: t}\n" >> tests/conformance/registry/rows/lh/endpoints.yaml; printf -- "- id: lh.feature.storage.new\n  title: new\n" >> tests/conformance/registry/features/storage.yaml'


echo
echo "== every product change and every new test is covered in the registry =="
product_change() { printf 'package x\n\nfunc F() { println("changed") }\n' > internal/x/x.go; }
registry_row_change() { printf -- '- {id: lh.row.touched, title: t}\n' >> tests/conformance/registry/rows/lh/endpoints.yaml; }
product_and_row() { product_change; registry_row_change; }
product_and_feature() { product_change; printf -- '- id: lh.feature.storage.new\n  title: new\n' >> tests/conformance/registry/features/storage.yaml; }
product_and_whitespace_registry() {
  product_change
  printf '\n# only a comment\n\n' >> tests/conformance/registry/rows/lh/endpoints.yaml
  printf -- '- id: lh.feature.storage.existing\n\n  title:   existing\n' > tests/conformance/registry/features/storage.yaml
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
  sed -i.bak 's/TestOldParity/TestRenamedParity/' tests/parity/lock_cells.txt && rm -f tests/parity/lock_cells.txt.bak
}
delete_referenced_test_file() { registry_row_change; git rm -q tests/parity/parity_test.go; }
move_test_to_other_file() {
  registry_row_change
  git mv tests/parity/parity_test.go tests/parity/moved_test.go
  sed -i.bak 's/parity_test.go/moved_test.go/' tests/conformance/registry/rows/lh/endpoints.yaml && rm -f tests/conformance/registry/rows/lh/endpoints.yaml.bak
}
dependency_bump() { sed -i.bak 's#github.com/a/b v1.0.0#github.com/a/b v1.2.3#' go.mod && rm -f go.mod.bak; printf 'a==1\n' > requirements.txt; printf 'y\n' >> go.sum; }
docs_only() { printf '# doc\n' > docs/x.md; }
ci_only() { printf 'name: x\n' > .github/workflows/x.yaml; mkdir -p scripts/bench; printf '#!/bin/sh\n' > scripts/bench/foo.sh; }

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
run_case "a new parity test referenced by a feature passes" ok "testlinks OK (1 tests added, all linked" new_test_referenced
run_case "L1: a file-level reference does not link a new test in that file" fail "tests/parity/parity_test.go#TestNewParity" new_test_in_linked_file
run_case "TestMain, helpers and Test-prefixed non-tests need no link" ok "testlinks OK (0 tests added" new_test_helper_not_a_test
run_case "a renamed test whose old name is still referenced fails" fail \
  "no such test any more in tests/parity/parity_test.go" rename_referenced_test
run_case "a renamed test is reported as added and unlinked too" fail \
  "tests/parity/parity_test.go#TestRenamedParity" rename_referenced_test
run_case "a renamed test with the reference updated passes" ok "testlinks OK" rename_and_relink_test
run_case "a deleted test file whose reference remains fails" fail \
  "registry references point at tests this PR removed" delete_referenced_test_file
run_case "a test moved to another file of its package with the reference updated passes" ok "testlinks OK (0 tests added" \
  move_test_to_other_file

echo
echo "== exemptions =="
COMMIT_MSG='build(deps): bump y' run_case "a dependency-only PR passes" ok "skipped (dependency-only PR)" dependency_bump
run_case "a go.mod bump under a non-dependency commit subject is product (kills the commit-subject mutant)" fail "shipped build file: go.mod" dependency_bump
run_case "a docs-only PR passes" ok "registry-touch check OK" docs_only
run_case "a CI-only PR passes" ok "registry-touch check OK" ci_only
PR_LABELS='bug,registry-exempt' run_case "the exempt label without the body section fails" fail \
  "no 'Registry: none" product_change
PR_LABELS='registry-exempt' PR_BODY='Registry: none —' run_case "the exempt label with an empty reason fails" fail \
  "no 'Registry: none" product_change
PR_BODY='Registry: none — pure refactor, no behaviour change' run_case "the body section without the label does not exempt" fail \
  "makes no real content change" product_change
EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS='bug,registry-exempt' PR_BODY=$'## Summary\nx\n\nRegistry: none — pure refactor, no behaviour change' \
  run_case "the exempt label with the body section passes" ok "registry-exempt label" product_change
EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS='registry-exempt' PR_BODY='Registry: none — tests only' run_case "the exemption also covers unlinked new tests" ok "skipped" new_test_unreferenced



echo
echo "== rule 1 fires on product code only =="
tests_only_change() { mkdir -p tests/e2e; printf 'helper\n' > tests/e2e/data.txt; printf 'x==1\n' > tests/requirements.txt; }
infra_only_change() { printf '\nextra:\n' >> Makefile; printf '#!/bin/sh\n' > scripts/tool.sh; }
test_file_edit_only() { printf '// edit\n' >> tests/parity/parity_test.go; }
chart_change() { mkdir -p charts/c; printf 'a: 1\n' > charts/c/values.yaml; }
patch_change() { mkdir -p patches; printf 'diff\n' > patches/x.patch; }
generated_go_change() { printf '// Code generated by x. DO NOT EDIT.\n\npackage x\n' > internal/x/z.go; }
run_case "tests-only data and requirements changes pass rule 1" ok "registry-touch check OK" tests_only_change
run_case "Makefile and scripts changes pass rule 1" ok "registry-touch check OK" infra_only_change
run_case "a _test.go edit that adds no test passes" ok "registry-touch check OK" test_file_edit_only
run_case "a chart change is product code" fail "makes no real content change" chart_change
run_case "a patches/ change is product code" fail "but not tests/conformance/registry/rows/" patch_change
run_case "a Code-generated marker does not exempt product Go (H3)" fail "product code: internal/x/z.go" generated_go_change
run_case "a feat: commit that touches only docs does not trigger rule 1" ok "registry-touch check OK" docs_only
COMMIT_MSG='feat: only tests' run_case "a feat: subject alone does not trigger rule 1" ok "registry-touch check OK" tests_only_change
run_case "new tests in a tests-only PR still need links (rule 2)" fail "tests/parity/parity_test.go#TestNewParity" add_parity_test

echo
echo "== the exempt label is honoured only when an approver applied it =="
PR_LABELS=registry-exempt PR_BODY='Registry: none — refactor' EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=mallory \
  run_case "label applied by a non-owner fails" fail "not an approver" product_change
PR_LABELS=registry-exempt PR_BODY='Registry: none — refactor' \
  run_case "no labeled event (a local run) fails closed" fail "not by the 'registry-exempt' label being applied" product_change
PR_LABELS=registry-exempt PR_BODY='Registry: none — refactor' EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis \
  run_case "label applied by the owner passes" ok "approver 'szibis'" product_change
PR_LABELS=registry-exempt PR_BODY='Registry: none — refactor' EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=mallory \
  run_case "a PR cannot approve itself by editing the approvers file" fail "not an approver" \
  bash -c "printf 'szibis\nmallory\n' > .github/registry-exempt-approvers; $(declare -f product_change); product_change" 
echo
echo "== parity fixes ship locks; locks are never weakened =="
drop_allowlist_entry() { sed -i.bak '/TestOldParity/d' tests/parity/known_failures.txt && rm -f tests/parity/known_failures.txt.bak; }
touch_parity_test() { printf 'package parity\n\nimport "testing"\n\nfunc TestOldParity(t *testing.T) { _ = 1 }\n' > tests/parity/parity_test.go; }
add_exact_row_for_parity_test() { printf -- '- {id: lh.row.new_lock, title: new lock, expect: pass, compare: {type: exact-json}, refs: {tests: [tests/parity/parity_test.go#TestOldParity]}}\n' >> tests/conformance/registry/rows/lh/endpoints.yaml; }
add_loose_row_for_parity_test() { printf -- '- {id: lh.row.loose, title: loose, expect: pass, compare: {type: status}, refs: {tests: [tests/parity/parity_test.go#TestOldParity]}}\n' >> tests/conformance/registry/rows/lh/endpoints.yaml; }
add_exact_row_other_tests() { printf -- '- {id: lh.row.elsewhere, title: e, expect: pass, compare: {type: exact-json}, refs: {tests: [internal/x/x_test.go]}}\n' >> tests/conformance/registry/rows/lh/endpoints.yaml; }
full_parity_fix() { drop_allowlist_entry; touch_parity_test; add_exact_row_for_parity_test; }
mark_resolved() { sed -i.bak 's/| \*\*B1\*\* | open one |/| **B1** | **Resolved** | fixed |/' docs/parity-and-gaps.md && rm -f docs/parity-and-gaps.md.bak; }
flip_gap_to_pass() { sed -i.bak 's/id: lh.row.gap, title: gap, expect: differ/id: lh.row.gap, title: gap, expect: pass/' tests/conformance/registry/rows/lh/endpoints.yaml && rm -f tests/conformance/registry/rows/lh/endpoints.yaml.bak; }
add_allowlist_entry() { printf 'TestParity_C  # B3: new\n' >> tests/parity/known_failures.txt; registry_row_change; }
weaken_lock_to_differ() { sed -i.bak 's/id: lh.row.lock, title: lock, expect: pass/id: lh.row.lock, title: lock, expect: differ/' tests/conformance/registry/rows/lh/endpoints.yaml && rm -f tests/conformance/registry/rows/lh/endpoints.yaml.bak; }
loosen_lock_compare() { sed -i.bak '/lh.row.lock/s/exact-json/ndjson-multiset/' tests/conformance/registry/rows/lh/endpoints.yaml && rm -f tests/conformance/registry/rows/lh/endpoints.yaml.bak; }
delete_lock_row() { sed -i.bak '/lh.row.lock/d' tests/conformance/registry/rows/lh/endpoints.yaml && rm -f tests/conformance/registry/rows/lh/endpoints.yaml.bak; }

run_case "removing an allowlist entry with no tests or rows fails" fail "does not ship its locks" \
  bash -c "$(declare -f drop_allowlist_entry registry_row_change); drop_allowlist_entry; registry_row_change"
run_case "the failure names the missing parity test" fail "no test function under tests/parity/" \
  bash -c "$(declare -f drop_allowlist_entry registry_row_change); drop_allowlist_entry; registry_row_change"
run_case "a parity fix with a parity test and a new exact row that references it passes" ok "parity-fix PR: true" full_parity_fix
run_case "a parity fix whose parity test is changed but no exact row references it fails" fail \
  "no new or changed registry row is a lock" \
  bash -c "$(declare -f drop_allowlist_entry touch_parity_test add_loose_row_for_parity_test); drop_allowlist_entry; touch_parity_test; add_loose_row_for_parity_test"
run_case "a parity fix whose exact row references other tests fails" fail "no new or changed registry row is a lock" \
  bash -c "$(declare -f drop_allowlist_entry touch_parity_test add_exact_row_other_tests); drop_allowlist_entry; touch_parity_test; add_exact_row_other_tests"
run_case "an UNCHANGED exact row that already references the test is not this PR's lock" fail "no new or changed registry row is a lock" \
  bash -c "$(declare -f drop_allowlist_entry touch_parity_test registry_row_change); drop_allowlist_entry; touch_parity_test; registry_row_change"
run_case "marking a divergence resolved in the docs is a parity fix" fail "divergence marked resolved in docs/parity-and-gaps.md: B1" \
  bash -c "$(declare -f mark_resolved registry_row_change); mark_resolved; registry_row_change"
run_case "flipping a known-gap row to pass is a parity fix" fail "row flipped from known gap (differ) to pass: lh.row.gap" flip_gap_to_pass
run_case "a docs-resolved parity fix with its locks passes" ok "parity-fix PR: true" \
  bash -c "$(declare -f mark_resolved touch_parity_test add_exact_row_for_parity_test); mark_resolved; touch_parity_test; add_exact_row_for_parity_test"
run_case "a PR that leaves the allowlist and docs alone is not a parity fix" ok "parity-fix PR: false" product_and_row
rename_allowlist_entry() { sed -i.bak 's#^TestOldParity #TestOldParity/sub #' tests/parity/known_failures.txt && rm -f tests/parity/known_failures.txt.bak; registry_row_change; }
run_case "renaming an allowlist entry is a weakening with a rename hint" fail "looks like a rename" rename_allowlist_entry
run_case "adding an allowlist entry fails as a weakening" fail "allowlist entry added: TestParity_C" add_allowlist_entry
EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY='Registry: none — owner accepted a new known failure' run_case "the owner exemption allows an allowlist entry" ok "skipped" add_allowlist_entry
run_case "flipping an exact pass row to differ fails as a weakening" fail "pass row weakened to expect=differ" \
  bash -c "$(declare -f weaken_lock_to_differ registry_row_change); weaken_lock_to_differ; registry_row_change"
run_case "loosening an exact row's compare fails as a weakening" fail "compare (type, options or project) changed: lh.row.lock" \
  bash -c "$(declare -f loosen_lock_compare registry_row_change); loosen_lock_compare; registry_row_change"
run_case "deleting an exact row fails as a weakening" fail "pass row deleted: lh.row.lock" \
  bash -c "$(declare -f delete_lock_row registry_row_change); delete_lock_row; registry_row_change"

echo
echo "== hardening: every reviewer bypass is a failing case =="
ROWS=tests/conformance/registry/rows/lh/endpoints.yaml
sedi() { sed -i.bak "$1" "$2" && rm -f "$2.bak"; }
b1_ui_asset() { echo '/* behaviour */' >> internal/ui/static/lakehouse-ui.js; }
b2_test_prefixed_go() { printf 'package x\n\nvar hook = 1\n' > internal/x/test_hooks.go; }
doc_and_testdata_only() { mkdir -p internal/x/testdata; printf 'd\n' > internal/x/testdata/in.json; printf '# r\n' >> internal/x/README.md; printf '# rb\n' > internal/x/RUNBOOK.md; printf '#!/bin/sh\n' > internal/x/test_helper.sh; }
b3_generated_marker() { printf '// Code generated by hand. DO NOT EDIT.\n\npackage x\n\nfunc F() { println("behaviour") }\n' > internal/x/x.go; }
b4_replace_directive() { printf '\nreplace github.com/a/b => github.com/evil/b v0.0.1\n' >> go.mod; }
dep_vm_module() { sedi 's#VictoriaMetrics/c v1.0.0#VictoriaMetrics/c v1.9.9#' go.mod; }
dep_go_directive() { sedi 's#^go 1.22#go 1.23#' go.mod; }
dep_new_require() { sedi 's#^)#\tgithub.com/d/e v1.0.0\n)#' go.mod; }
b5_dockerfile() { sedi 's#^ARG VL_VERSION=.*#ARG VL_VERSION=v1.99.0#' Dockerfile.logs; }
other_dockerfile() { sedi 's#^ARG X=1#ARG X=2#' Dockerfile.loki-vl-proxy; }
traces_go_mod() { mkdir -p lakehouse-traces; printf 'module t\n' > lakehouse-traces/go.mod; }
b6_pass_to_differ() { product_change; sedi '/lh.row.lock/s/expect: pass/expect: differ, differ_note: x/' $ROWS; }
b7_tolerance_raised() { product_change; sedi 's/hits_tolerance: "0"/hits_tolerance: "0.5"/' $ROWS; }
b8_request_repointed() { product_change; sedi '/lh.row.lock/s#path: /a#path: /health#' $ROWS; }
project_changed() { product_change; sedi '/lh.row.vwh/s#compare: {type: values-with-hits#compare: {project: [x], type: values-with-hits#' $ROWS; }
b9_allowlist_renamed() {
  git mv tests/parity/known_failures.txt tests/parity/known_failures_v2.txt
  echo 'TestParity_New  # new gap' >> tests/parity/known_failures_v2.txt
  sedi 's#tests/parity/known_failures.txt#tests/parity/known_failures_v2.txt#' .github/workflows/parity.yaml
  touch_parity_test; add_exact_row_for_parity_test
}
b9_allowlist_deleted() { git rm -q tests/parity/known_failures.txt; registry_row_change; }
b9_workflow_arg_changed() { sedi 's#tests/parity/known_failures.txt#tests/parity/other.txt#' .github/workflows/parity.yaml; : > tests/parity/other.txt; registry_row_change; }
b10_comment_only_parity_touch() {
  drop_allowlist_entry
  printf '\n// touch\n' >> tests/parity/parity_test.go
  sedi 's/title: lock/title: lock (edited)/' $ROWS
}
fix_with_vwh_lock() {
  drop_allowlist_entry; touch_parity_test
  printf -- '- {id: lh.row.vwh2, title: v, expect: pass, compare: {type: values-with-hits, options: {hits_tolerance: "0"}}, refs: {tests: [tests/parity/parity_test.go#TestOldParity]}}\n' >> $ROWS
}
fix_with_loose_vwh() {
  drop_allowlist_entry; touch_parity_test
  printf -- '- {id: lh.row.vwh2, title: v, expect: pass, compare: {type: values-with-hits, options: {hits_tolerance: "0.5"}}, refs: {tests: [tests/parity/parity_test.go#TestOldParity]}}\n' >> $ROWS
}
flip_gap_exact_with_ref() { flip_gap_to_pass; sedi '/lh.row.gap/s#compare: {type: ndjson-multiset}#compare: {type: exact-json}, refs: {tests: [tests/parity/parity_test.go\#TestOldParity]}#' $ROWS; touch_parity_test; }
flip_gap_only_other_lock() { flip_gap_to_pass; touch_parity_test; add_exact_row_for_parity_test; }
b11_unicode_test() { registry_row_change; printf 'package x\n\nimport "testing"\n\nfunc TestÄé(t *testing.T) {}\n' > internal/x/uni_test.go; }
l1_syntax_error_test() { registry_row_change; printf 'package x\n\nfunc TestBroken( {\n' > internal/x/broken_test.go; }
gate_file_change() { printf '#!/usr/bin/env python3\n' > scripts/ci/pr_classify.py; }
chart_version_bump_with_changelog() { materialize_release 9.9.9; sedi 's/^version: 1.0.0/version: 9.9.9/; s/^appVersion: "1.0.0"/appVersion: "9.9.9"/' charts/victoria-lakehouse/Chart.yaml; }
chart_description_edit_with_changelog() { chart_version_bump_with_changelog; sedi 's/^description: d/description: evil/' charts/victoria-lakehouse/Chart.yaml; }
changelog_new_bullet_with_chart() { chart_version_bump_with_changelog; printf -- '- hand written behaviour, not a lead-in\n' >> CHANGELOG.md; }
chart_templates_md() { printf '# n\n' >> charts/victoria-lakehouse/README.md; }
chart_test_script() { printf '# edit\n' >> charts/victoria-lakehouse/test_templates.sh; }
chart_template_file() { printf 'a: 1\n' > charts/victoria-lakehouse/templates/NOTES.txt; }

run_case "B1: an embedded UI asset under internal/ is product code" fail "product code: internal/ui/static/lakehouse-ui.js" b1_ui_asset
run_case "B2: a non-test Go file named test_*.go is product code" fail "product code: internal/x/test_hooks.go" b2_test_prefixed_go
run_case "internal testdata, README, RUNBOOK and test_*.sh scripts are not product" ok "registry-touch check OK" doc_and_testdata_only
run_case "B3: a Code-generated marker on a product file does not exempt it" fail "product code: internal/x/x.go" b3_generated_marker
COMMIT_MSG='chore(deps): bump parquet-go' run_case "B4: a go.mod replace directive is never dependency-only" fail "shipped build file: go.mod" b4_replace_directive
COMMIT_MSG='chore(deps): bump c' run_case "a VictoriaMetrics/* module bump is never dependency-only" fail "shipped build file: go.mod" dep_vm_module
COMMIT_MSG='chore(deps): bump go' run_case "a go directive change is never dependency-only" fail "shipped build file: go.mod" dep_go_directive
COMMIT_MSG='build(deps): add e' run_case "a new require line is dependency-only" ok "skipped (dependency-only PR)" dep_new_require
COMMIT_MSG='build(deps): bump y' run_case "go.sum and requirements.txt alone are dependency-only" ok "skipped (dependency-only PR)" \
  bash -c "printf 'y\n' >> go.sum; printf 'a==1\n' > requirements.txt"
run_case "B5: a shipped Dockerfile change is product" fail "shipped build file: Dockerfile.logs" b5_dockerfile
run_case "the loki-vl-proxy Dockerfile is not shipped product" ok "registry-touch check OK" other_dockerfile
run_case "lakehouse-traces/go.mod is product" fail "product code: lakehouse-traces/go.mod" traces_go_mod
run_case "B6: flipping a pass row to differ is a weakening" fail "pass row weakened to expect=differ: lh.row.lock" b6_pass_to_differ
run_case "B7: raising a hits_tolerance on a pass row is a weakening" fail "compare (type, options or project) changed: lh.row.vwh" b7_tolerance_raised
run_case "changing a pass row's project is a weakening" fail "compare (type, options or project) changed: lh.row.vwh" project_changed
run_case "B8: re-pointing a pass row's request is a weakening" fail "pass row's request changed: lh.row.lock" b8_request_repointed
run_case "B9: renaming the allowlist file (and the workflow argument) fails" fail "--allowlist changed from tests/parity/known_failures.txt to tests/parity/known_failures_v2.txt" b9_allowlist_renamed
run_case "B9: deleting the allowlist file fails" fail "is missing at head" b9_allowlist_deleted
run_case "B9: pointing the workflow at another allowlist fails" fail "--allowlist changed" b9_workflow_arg_changed
run_case "B10: a comment in a parity test file and a title edit do not lock a parity fix" fail "no test function under tests/parity/" b10_comment_only_parity_touch
run_case "a parity fix with a values-with-hits tolerance-0 lock referencing a modified test passes" ok "parity-fix PR: true" fix_with_vwh_lock
run_case "a parity fix whose values-with-hits lock has a tolerance fails" fail "no new or changed registry row is a lock" fix_with_loose_vwh
run_case "a differ-to-pass flip whose row is itself an exact lock with the ref passes" ok "parity-fix PR: true" flip_gap_exact_with_ref
run_case "a differ-to-pass flip is not excused by another lock row" fail "the flipped row lh.row.gap must itself be a lock" flip_gap_only_other_lock
run_case "B11: a unicode-named new test is found and must be linked" fail "internal/x/uni_test.go#TestÄé" b11_unicode_test
run_case "a test file that does not parse is a tool error, not zero tests" fail "expected" l1_syntax_error_test
run_case "a change to the gate itself needs the owner" fail "changes the registry gate itself" gate_file_change
EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY='Registry: none — gate hardening' \
  run_case "a gate change with the owner's verified label passes" ok "skipped" gate_file_change
PR_AUTHOR=szibis run_case "M2: a version-only chart bump with the changelog materialization, by the owner, is release metadata" ok "skipped (release-metadata PR)" chart_version_bump_with_changelog
PR_AUTHOR=github-actions[bot] run_case "M2: the same by the release bot is release metadata" ok "skipped (release-metadata PR)" chart_version_bump_with_changelog
PR_AUTHOR=mallory run_case "M2: the same by anyone else is not" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_version_bump_with_changelog
run_case "M2: the same with no author known is not" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_version_bump_with_changelog
chart_appversion_downgrade() { chart_version_bump_with_changelog; sedi 's/^appVersion: "9.9.9"/appVersion: "0.120.0"/' charts/victoria-lakehouse/Chart.yaml; }
PR_AUTHOR=szibis run_case "N13: an appVersion downgrade hidden in a release PR is not release metadata" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_appversion_downgrade
chart_reindent() { chart_version_bump_with_changelog; sedi 's/^name: c/ name: c/' charts/victoria-lakehouse/Chart.yaml; }
PR_AUTHOR=szibis run_case "N9: a whitespace change in Chart.yaml is not release metadata" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_reindent
PR_AUTHOR=szibis run_case "M2: a chart description edit hidden in a release PR is not release metadata" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_description_edit_with_changelog
PR_AUTHOR=szibis run_case "M2: a hand-written changelog bullet in a release PR is not release metadata" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" changelog_new_bullet_with_chart
PR_AUTHOR=szibis PR_LABELS=registry-exempt run_case "L3: an exempt PR needs no body line for a stray label" ok "skipped (release-metadata PR)" chart_version_bump_with_changelog
run_case "S3: a chart README is documentation" ok "registry-touch check OK" chart_templates_md
run_case "S2: a chart test_*.sh script is a test" ok "registry-touch check OK" chart_test_script
run_case "S3: a chart template file is product even when it is .txt" fail "packaged/patched: charts/victoria-lakehouse/templates/NOTES.txt" chart_template_file
EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY=$'## Summary\n\n> Registry: none — a quoted reason' \
  run_case "M3: the body line may sit in a Markdown blockquote" ok "approver 'szibis'" product_change
EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY=$'text Registry: none — not at the line start' \
  run_case "M3: the body line must start its line" fail "no 'Registry: none" product_change
for body in 'Registry: none —' 'Registry: none —    ' 'Registry: none -' 'Registry: none —  .'; do
  EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY="$body" run_case "S9: an empty reason ('$body') is rejected" fail "no 'Registry: none" product_change
done

echo
echo "== the exempt label is honoured only in the run its own labeled event triggers (H7) =="
for action in synchronize opened reopened edited unlabeled; do
  EVENT_ACTION=$action LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY='Registry: none — r' \
    run_case "H7: a '$action' run denies the label, whoever applied it" fail "last push" product_change
done
EVENT_ACTION=labeled LABEL_NAME=size/XL SENDER=szibis PR_LABELS=registry-exempt,size/XL PR_BODY='Registry: none — r' \
  run_case "H7: a run triggered by another label denies the exemption" fail "not 'registry-exempt'" product_change
EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis PR_LABELS=registry-exempt PR_BODY='Registry: none — r' \
  run_case "H7: the owner's labeled run honours the label" ok "approver 'szibis'" product_change
EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER= PR_LABELS=registry-exempt PR_BODY='Registry: none — r' \
  run_case "H7: an unknown sender denies the label" fail "unknown" product_change

echo
echo "== the job summary lists the registry changes =="
summary_case() {
  local dir out sum
  dir="$(new_repo)"; sum="$(mktemp)"
  ( cd "$dir" && registry_row_change && printf -- '- id: lh.feature.storage.added\n  title: n\n' >> tests/conformance/registry/features/storage.yaml && git add -A && git commit -q -m change ) >/dev/null 2>&1
  out="$(cd "$dir" && GITHUB_STEP_SUMMARY="$sum" bash "$CHECKER" base 2>&1)"
  if grep -q 'rows added (1): lh.row.touched' "$sum" && grep -q 'features added (1): lh.feature.storage.added' "$sum" && grep -q 'rows added' <<<"$out"; then
    echo "ok   - M3: added rows and features are printed and written to the job summary"; pass=$((pass + 1))
  else
    echo "FAIL - M3: job summary"; cat "$sum"; echo "$out" | sed 's/^/       /'; fail=$((fail + 1))
  fi
  rm -rf "$dir" "$sum"
}
summary_case

echo
echo "== integrity round: whole-row locks, lock tests that cannot be disabled, gate files =="
n1_pending_set() { product_change; sedi '/lh.row.lock/s/expect: pass/expect: pass, pending: true/' $ROWS; }
n2_targets_dropped() { product_change; sedi '/lh.row.lock/s/compare:/targets: [hot], compare:/' $ROWS; }
n3_seed_swapped() { product_change; sedi '/lh.row.lock/s/compare:/seed: [logs.empty], compare:/' $ROWS; }
n4_build_constraint() { printf '//go:build ignore\n\n' | cat - tests/parity/parity_test.go > tests/parity/x.tmp && mv tests/parity/x.tmp tests/parity/parity_test.go; registry_row_change; }
n4_build_comment_edit() { printf '// harmless comment\n' | cat - tests/parity/parity_test.go > tests/parity/x.tmp && mv tests/parity/x.tmp tests/parity/parity_test.go; registry_row_change; }
n5_skip_added() { sedi 's/func TestOldParity(t \*testing.T) {}/func TestOldParity(t *testing.T) { t.Skip("flaky") }/' tests/parity/parity_test.go; registry_row_change; }
n6_unrelated_lock() {
  sedi '/TestOldParity/d' tests/parity/known_failures.txt
  touch_parity_test
  printf 'package parity\n\nimport "testing"\n\nfunc TestElse(t *testing.T) { _ = 1 }\n' > tests/parity/else_test.go
  printf -- '- {id: lh.row.else, title: e, expect: pass, compare: {type: exact-json}, refs: {tests: [tests/parity/else_test.go#TestElse]}}\n' >> $ROWS
}
n6_pending_lock() { drop_allowlist_entry; touch_parity_test; printf -- '- {id: lh.row.pend, title: p, expect: pass, pending: true, compare: {type: exact-json}, refs: {tests: [tests/parity/parity_test.go#TestOldParity]}}\n' >> $ROWS; }
n7_prose_only_registry_edit() { product_change; sedi 's/title: lock/title: lock (reworded)/' $ROWS; printf -- '- id: lh.feature.storage.existing\n  title: existing\n  description: Also words\n' > tests/conformance/registry/features/storage.yaml; }
n8_quoted_require() { sedi 's#github.com/VictoriaMetrics/c v1.0.0#"github.com/VictoriaMetrics/c" v1.5.0#' go.mod; }
gate_file_touch() { mkdir -p scripts/ci tests/parity; printf '#!/usr/bin/env python3\n' > scripts/ci/parity_ratchet.py; }
workflow_gate_touch() { printf 'name: x\n' > .github/workflows/$1; }
l2_s3compat_test() { mkdir -p tests/s3compat; printf 'package s3\n\nimport "testing"\n\nfunc TestS3New(t *testing.T) {}\n' > tests/s3compat/a_test.go; registry_row_change; }
renamed_lock_reference() {
  registry_row_change
  sedi 's/TestOldParity/TestRenamedParity/' tests/parity/parity_test.go
  sedi 's/TestOldParity/TestRenamedParity/' $ROWS
  sedi 's/TestOldParity/TestRenamedParity/' tests/parity/lock_cells.txt
}
renamed_lock_without_the_floor() { renamed_lock_reference; sedi 's/TestRenamedParity  5/TestRenamedParity  2/' tests/parity/lock_cells.txt; }
renamed_lock_with_no_floor_entry() { renamed_lock_reference; : > tests/parity/lock_cells.txt; }
lowered_floor() { registry_row_change; sedi 's/TestOldParity  5/TestOldParity  4/' tests/parity/lock_cells.txt; }
removed_floor() { registry_row_change; : > tests/parity/lock_cells.txt; }
raised_floor() { registry_row_change; sedi 's/TestOldParity  5/TestOldParity  9/' tests/parity/lock_cells.txt; }
x1_lock_swapped_for_empty_test() {
  # the lock's test replaced by an empty one, every reference re-pointed, the old name kept as a helper
  registry_row_change
  sedi 's/func TestOldParity(t \*testing.T) {}/func oldParity(t *testing.T) {}\nfunc TestOldParityV2(t *testing.T) {}/' tests/parity/parity_test.go
  sedi 's/TestOldParity/TestOldParityV2/' $ROWS
}
x2_lock_file_renamed() {
  registry_row_change
  git mv tests/parity/parity_test.go tests/parity/parity_windows_test.go
  sedi 's#tests/parity/parity_test.go#tests/parity/parity_windows_test.go#' $ROWS
}
x2b_lock_file_hidden() {
  registry_row_change
  git mv tests/parity/parity_test.go tests/parity/_parity_test.go
  sedi 's#tests/parity/parity_test.go#tests/parity/_parity_test.go#' $ROWS
}
x6_testmain_exit() {
  registry_row_change
  printf 'package parity\n\nimport (\n\t"os"\n\t"testing"\n)\n\nfunc TestMain(m *testing.M) { os.Exit(0) }\n' > tests/parity/zz_main_test.go
}
x6_testmain_edit() {
  registry_row_change
  printf 'package parity\n\nimport (\n\t"os"\n\t"testing"\n)\n\nfunc TestMain(m *testing.M) { os.Exit(m.Run()) }\n' > tests/parity/main_test.go
  git add -A; git commit -q -m "add main"; git branch -f base HEAD
  sedi 's/os.Exit(m.Run())/if os.Getenv("CI") != "" { os.Exit(0) }; os.Exit(m.Run())/' tests/parity/main_test.go
}
dropped_lock_reference() { registry_row_change; sedi 's#refs: {tests: \[tests/parity/parity_test.go\#TestOldParity\]}#refs: {tests: []}#' $ROWS; }

run_case "N1: setting pending on a pass row (the lock stops executing) is a weakening" fail "pass row set to pending (it stops executing): lh.row.lock" n1_pending_set
run_case "N2: dropping a target from a pass row is a weakening" fail "pass row's targets changed: lh.row.lock" n2_targets_dropped
run_case "N3: swapping a pass row's seed is a weakening" fail "pass row's seed changed: lh.row.lock" n3_seed_swapped
run_case "N4: a build constraint on a lock's test file is a weakening" fail "build constraint header changed" n4_build_constraint
run_case "N4: so is a comment-only header edit" fail "build constraint header changed" n4_build_comment_edit
run_case "N5: a Skip added to a lock's test file is a weakening" fail "a Skip call was added" n5_skip_added
run_case "N6: a parity fix locked by an unrelated test does not pass" fail "no lock row names TestOldParity" n6_unrelated_lock
run_case "N6/M2: a pending row IS a lock when its test has a cell floor" ok "parity-fix PR: true" n6_pending_lock
n6_pending_lock_no_floor() { n6_pending_lock; : > tests/parity/lock_cells.txt; }
run_case "N6/M2: a pending row is no lock without a cell floor" fail "no lock row names TestOldParity" n6_pending_lock_no_floor
run_case "N7: a product change 'covered' by a prose edit fails" fail "makes no real content change" n7_prose_only_registry_edit
COMMIT_MSG='build(deps): bump c' run_case "N8: a quoted VictoriaMetrics require is still critical" fail "shipped build file: go.mod" n8_quoted_require
run_case "N10: parity_ratchet.py is a gate file" fail "changes the registry gate itself" gate_file_touch
run_case "N11: the parity workflow is a gate file" fail "changes the registry gate itself" workflow_gate_touch parity.yaml
run_case "C1: conformance.yaml is a gate file" fail "changes the registry gate itself" workflow_gate_touch conformance.yaml
run_case "registry-gate.yaml is a gate file" fail "changes the registry gate itself" workflow_gate_touch registry-gate.yaml
run_case "registry-gate-base.yaml is a gate file" fail "changes the registry gate itself" workflow_gate_touch registry-gate-base.yaml
run_case "L2: a new tests/s3compat test must be linked" fail "tests/s3compat/a_test.go#TestS3New" l2_s3compat_test
run_case "a lock's renamed test replaces its reference without weakening it (the floor moves with it)" ok "testlinks OK" renamed_lock_reference
run_case "a rename that lowers the floor is a weakening" fail "lost its test reference" renamed_lock_without_the_floor
run_case "a rename to a test with no floor entry is a weakening" fail "lost its test reference" renamed_lock_with_no_floor_entry
run_case "a lowered floor is a weakening" fail "cell floor of TestOldParity removed or lowered (5 -> 4)" lowered_floor
run_case "a removed floor is a weakening" fail "cell floor of TestOldParity removed or lowered (5 -> 0)" removed_floor
run_case "a raised floor is free" ok "testlinks OK" raised_floor
run_case "X1: a lock swapped for an empty test with every reference re-pointed fails" fail "lost its test reference" x1_lock_swapped_for_empty_test
run_case "X2: a lock file renamed to *_windows_test.go fails" fail "name keeps the go tool from building it" x2_lock_file_renamed
run_case "X2b: a lock file hidden behind a leading underscore fails" fail "name keeps the go tool from building it" x2b_lock_file_hidden
run_case "X6: a TestMain that exits 0 in a lock's package fails" fail "TestMain of a package that holds a lock changed" x6_testmain_exit
run_case "a dropped lock reference is a weakening even though the test is kept" fail "lost its test reference" dropped_lock_reference

x5b_pending_yes() { sedi '/lh.row.lock/s/expect: pass/expect: pass, pending: yes/' $ROWS; }
x5b_pending_on() { sedi '/lh.row.lock/s/expect: pass/expect: pass, pending: on/' $ROWS; }
x5b_pending_quoted() { sedi '/lh.row.lock/s/expect: pass/expect: pass, pending: "true"/' $ROWS; }
x8_second_document() { printf -- '---\n- {id: lh.row.hidden, title: h, expect: pass, compare: {type: exact-json}}\n' >> $ROWS; registry_row_change; }
x7_gitattributes_hides_the_pin() { printf 'Makefile -diff\n' >> .gitattributes; sedi 's/^VL_VERSION_LOGS := .*/VL_VERSION_LOGS := v9.99.0/' Makefile; }
x7_pin_bump() { sedi 's/^VL_VERSION_LOGS := .*/VL_VERSION_LOGS := v9.99.0/' Makefile; }
x7_traces_pin_bump() { sedi 's/^VL_COMMIT_TRACES := .*/VL_COMMIT_TRACES := def456/' Makefile; }
x7_vt_pin_bump() { sedi 's/^VT_VERSION := .*/VT_VERSION := v0.2.0/' Makefile; }
x7_gitattributes_textconv() { printf '*.yaml diff=none\n*.go -text\n' >> .gitattributes; registry_row_change; }
x7_gitattributes_alone() { printf '* text=auto\n' > .gitattributes; }
x9_bump() { sedi "s#$1 v[0-9.]*#$1 v9.9.9#" go.mod; }
x10_shadow_module() { printf 'import sys\nsys.exit(0)\n' > scripts/ci/dataclasses.py; }

run_case "X5b: pending: yes on a pass row is rejected outright" fail "pending must be true or false" x5b_pending_yes
run_case "X5b: pending: on is rejected outright" fail "pending must be true or false" x5b_pending_on
run_case "X5b: a quoted \"true\" is rejected outright" fail "pending must be true or false" x5b_pending_quoted
run_case "X8: a second YAML document in a registry file is rejected" fail "more than one YAML document" x8_second_document
run_case "X7: a Makefile pin bump hidden by .gitattributes is product and a gate-file change" fail "route/handler/upstream files" x7_gitattributes_hides_the_pin
run_case "X7: .gitattributes is a gate file" fail "changes the registry gate itself" x7_gitattributes_alone
run_case "X7: .gitattributes diff and text attributes do not hide anything" fail "changes the registry gate itself" x7_gitattributes_textconv
run_case "X7: a VL_VERSION_LOGS bump is a product change" fail "route/handler/upstream files" x7_pin_bump
run_case "X7: a VL_COMMIT_TRACES bump is a product change" fail "route/handler/upstream files" x7_traces_pin_bump
run_case "X7: a VT_VERSION bump is a product change" fail "route/handler/upstream files" x7_vt_pin_bump
for mod in github.com/klauspost/compress github.com/golang/snappy google.golang.org/protobuf github.com/pierrec/lz4/v4; do
  COMMIT_MSG="build(deps): bump $mod" run_case "X9: a $mod bump is never dependency-only" fail "shipped build file: go.mod" x9_bump "$mod"
done
lock_helper_touch() { printf '// x\n' >> tests/parity/lock_cells_test.go; }
run_case "the lock-cells helper is a gate file" fail "changes the registry gate itself" lock_helper_touch
x_submodule_registry_entry() {
  registry_row_change
  # a nested repository is recorded by `git add -A` as a gitlink (mode 160000)
  local sub=tests/conformance/registry/rows/lh/zz_sub
  git init -q "$sub" && git -C "$sub" -c user.name=t -c user.email=t@e commit -q --allow-empty -m x
}
run_case "S: a submodule entry in the registry is rejected like a symlink" fail "symlink or submodule" x_submodule_registry_entry

head_rev_case() {
  local dir out rc1 rc2
  dir="$(new_repo)"
  ( cd "$dir" && product_change && git add -A && git commit -q -m change && git branch -q pr && git checkout -q base ) >/dev/null 2>&1
  out="$(cd "$dir" && HEAD_REV=pr bash "$CHECKER" base 2>&1)"; rc1=$?
  (cd "$dir" && bash "$CHECKER" base) >/dev/null 2>&1; rc2=$?
  if [[ $rc1 -ne 0 && $rc2 -eq 0 ]] && grep -q "makes no real content change" <<<"$out"; then
    echo "ok   - HEAD_REV judges that revision, not the checked-out one (the pull_request_target gate has no PR checkout)"; pass=$((pass + 1))
  else
    echo "FAIL - HEAD_REV (rc with=$rc1, without=$rc2)"; echo "$out" | sed 's/^/       /'; fail=$((fail + 1))
  fi
  rm -rf "$dir"
}
head_rev_case
run_case "X10: a new file under scripts/ci that could shadow an import is a gate file" fail "changes the registry gate itself" x10_shadow_module
run_case "a tests/ Makefile-free infra change is still not product" ok "registry-touch check OK" infra_only_change
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
echo "== a release whose feature still has an Unreleased bullet carries the regenerated catalog =="

# PR #440 merged after a release commit, so its bullet stayed under
# [Unreleased] while the new version heading appeared: the feature's "since:
# the release after vX" wording changed and `confgen -check` reported
# docs/features.md stale. The release-metadata PR then must carry the
# regenerated docs/features.md and still pass BOTH gates (changelog gate and
# registry gate). Negatives: the same PR plus a product file, and plus a
# non-generated doc, is no longer a release-metadata sync.
run_unreleased_bullet_case() {
  local name="a release-metadata PR with a feature still unreleased passes both gates with the regenerated features.md; extra product or non-generated files do not"
  local tmp
  tmp="$(clone_repo "$name")" || return 0
  local log="$tmp/log" ok=1 why=""
  : > "$log"
  check_step() {
    local what="$1"
    shift
    if ! (cd "$tmp/repo" && "$@") >>"$log" 2>&1; then
      ok=0
      why="$why
       - $what"
    fi
  }
  # expect_fail <description> <command...>: the command must exit non-zero.
  expect_fail() {
    local what="$1"
    shift
    if (cd "$tmp/repo" && "$@") >>"$log" 2>&1; then
      ok=0
      why="$why
       - $what"
    fi
  }
  local bullet='{ print } /^## \[Unreleased\]$/ { print ""; print "### Added"; print ""; print "- **Touch-check release fixture.** Added by scripts/ci/tests/test_check_registry_touch.sh." }'

  (
    cd "$tmp/repo" || exit 1
    rewrite CHANGELOG.md "$bullet"
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
    # The release commit materializes the heading; the fixture bullet then
    # lands again under [Unreleased] (a PR merged after the release commit).
    # The release cut the heading; the fixture bullet (merged after the release
    # commit) stays under [Unreleased]: only the heading is inserted.
    rewrite CHANGELOG.md '/^## \[[0-9]/ && !done { print "## [99.0.0] - 2026-01-01"; print ""; done = 1 } { print }'
    sed -i.bak -E 's/^version: .*/version: 99.0.0/; s/^appVersion: .*/appVersion: "99.0.0"/' charts/victoria-lakehouse/Chart.yaml && rm -f charts/victoria-lakehouse/Chart.yaml.bak
    git add CHANGELOG.md charts/victoria-lakehouse/Chart.yaml
    git commit -q -m "chore: release metadata for v99.0.0 [skip release]"
    git -c tag.gpgsign=false tag v99.0.0
  ) >>"$log" 2>&1

  expect_fail "confgen -check reports docs/features.md stale on the metadata commit" \
    env GOWORK=off go run ./tests/conformance/cmd/confgen -check
  check_step "regeneration succeeds" env GOWORK=off go run ./tests/conformance/cmd/confgen -write
  check_step "regeneration changes docs/features.md" bash -c '! git diff --quiet -- docs/features.md'
  check_step "the regenerated file is committed" bash -c 'git add docs/features.md UPSTREAM_COVERAGE.md README.md && git commit -q -m "regenerate"'
  check_step "only CHANGELOG.md, Chart.yaml and generated documents differ from base" \
    bash -c '! git diff --name-only base HEAD | grep -vxE "CHANGELOG.md|charts/victoria-lakehouse/Chart.yaml|docs/features.md|UPSTREAM_COVERAGE.md|README.md"'
  check_step "the registry gate passes" env SKIP_CONFGEN_CHECK= PR_AUTHOR=szibis bash "$CHECKER" base
  check_step "the registry gate reports a release-metadata PR" \
    bash -c 'out=$(python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/registry-exempt-approvers); echo "$out"; git diff --stat base HEAD; git diff base HEAD -- docs/features.md | grep "^[-+]" | head -20; grep -qx "exempt=release-metadata" <<<"$out"'
  check_step "the changelog gate passes" \
    python3 scripts/ci/check_changelog_pr.py --base base --head HEAD

  # Negatives, each one commit on top.
  check_step "a product file is added" bash -c 'echo "package server" > internal/zz_relmeta_fixture.go && git add -A && git commit -q -m "product"'
  check_step "the product PR is no longer a release-metadata PR" \
    bash -c 'python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/registry-exempt-approvers | grep -qx "exempt=none"'
  check_step "the product PR is classified as product" \
    bash -c 'python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/registry-exempt-approvers | grep -qx "product=1"'
  expect_fail "the registry gate fails a metadata-looking PR that also changes a product file" \
    env SKIP_CONFGEN_CHECK= PR_AUTHOR=szibis bash "$CHECKER" base
  check_step "the product file is dropped and a non-generated doc added" \
    bash -c 'git rm -q internal/zz_relmeta_fixture.go && echo x > docs/zz-relmeta-fixture.md && git add -A && git commit -q -m "doc"'
  check_step "a non-generated doc is not a release-metadata PR" \
    bash -c 'python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/registry-exempt-approvers | grep -qx "exempt=none"'
  check_step "the non-generated doc is dropped and docs/features.md gets a change beyond version naming" \
    bash -c 'git rm -q docs/zz-relmeta-fixture.md && echo "an invented line" >> docs/features.md && git add -A && git commit -q -m "features edit"'
  check_step "a features.md change beyond version naming is not a release-metadata PR" \
    bash -c 'python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/registry-exempt-approvers | grep -qx "exempt=none"'
  check_step "the changelog gate does not call it a release metadata sync" \
    bash -c '! python3 scripts/ci/check_changelog_pr.py --base base --head HEAD | grep -q "release metadata sync"'

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

run_unreleased_bullet_case

echo
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
