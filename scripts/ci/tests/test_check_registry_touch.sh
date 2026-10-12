#!/usr/bin/env bash
# Tests for scripts/ci/check_registry_touch.sh, run against synthetic git
# repositories: each case builds a base commit, commits a change on top, and
# asserts the checker's verdict. The checker is the gate that keeps a feature
# PR from merging without its catalog entry, so its own logic needs coverage
# that does not depend on this repository's current contents.
#
# Usage: bash scripts/ci/tests/test_check_registry_touch.sh
set -uo pipefail
# the gate prints its report on stdout unless a job summary file is set; these cases read stdout
unset GITHUB_STEP_SUMMARY

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
    printf 'package parity\n\nfunc helper() {}\n' > tests/parity/helpers.go
    printf 'package parity\n\nvar cfg = loadCfg()\n\nfunc init() { warm() }\n' > tests/parity/vars.go
    printf 'services:\n  build:\n    image: golang:1.22.0\n    environment:\n      LH_BASE_URL: lh\n' > tests/parity/docker-compose.yml
    printf '# approvers\nszibis\n' > .github/registry-exempt-approvers
    printf 'jobs:\n  parity:\n    steps:\n      - run: |\n          python scripts/ci/parity_ratchet.py \\\n            --allowlist tests/parity/known_failures.txt \\\n            --summary-file x\n' > .github/workflows/parity.yaml
    mkdir -p internal/ui/static charts/victoria-lakehouse/templates
    printf 'window.x = 0;\n' > internal/ui/static/lakehouse-ui.js
    printf 'FROM --platform=$BUILDPLATFORM golang:1.22.0-alpine3.20 AS builder\nARG VL_VERSION=v1.0.0\n# FIPS is set per deployment.\nARG FIPS=0\nRUN make\n' > Dockerfile.logs
    printf 'ARG X=1\n' > Dockerfile.loki-vl-proxy
    printf 'apiVersion: v2\nname: c\ndescription: d\nversion: 1.0.0\nappVersion: "1.0.0"\n' > charts/victoria-lakehouse/Chart.yaml
    printf '# chart\n' > charts/victoria-lakehouse/README.md
    printf '#!/bin/sh\n' > charts/victoria-lakehouse/test_templates.sh
    printf '# x\nversion 1.0.0 here\n' > README.md
    printf 'module x\n\ngo 1.22\n\nrequire (\n\tgolang.org/x/oauth2 v1.0.0\n\tgithub.com/klauspost/compress v1.0.0\n\tgithub.com/golang/snappy v1.0.0\n\tgoogle.golang.org/protobuf v1.0.0\n\tgithub.com/pierrec/lz4/v4 v4.0.0\n\tgithub.com/VictoriaMetrics/c v1.0.0\n\tgithub.com/cespare/xxhash/v2 v2.3.0\n\tgithub.com/valyala/fastjson v1.6.10\n\tgopkg.in/yaml.v3 v3.0.1\n\tgolang.org/x/net v0.59.0\n\tgolang.org/x/sync v1.0.0\n\tgolang.org/x/sys v0.1.0\n)\n' > go.mod
    printf 'VL_VERSION_LOGS := v1.0.0\nVL_COMMIT_TRACES := abc123\nVT_VERSION := v0.1.0\nall:\n' > Makefile
    printf 'x\n' > go.sum
    printf 'package x\n\nimport "testing"\n\nfunc TestE(t *testing.T) {}\n' > internal/x/e_test.go
    mkdir -p internal/y; printf 'package y\n\nimport "testing"\n\nfunc TestY(t *testing.T) {}\n' > internal/y/y_test.go
    printf '# floors\nTestOldParity  5  # lock\nTestFixParity  3  # fixture parity test no row locks at base\n' > tests/parity/lock_cells.txt
    printf '# allowlist\nTestFixParity  # B1: x\nTestParity_B  # B2: y\n' > tests/parity/known_failures.txt
    printf '| Id | Divergence |\n|---|---|\n| **B1** | open one |\n| **B2** | open two |\n| **Old thing (B0)** | **Resolved** | done |\n' > docs/parity-and-gaps.md
    cat >> tests/conformance/registry/rows/lh/endpoints.yaml <<'ROWS'
- {id: lh.row.gap, title: gap, expect: differ, compare: {type: ndjson-multiset}}
- {id: lh.row.lock, title: lock, expect: pass, compare: {type: exact-json}, request: {method: GET, path: /a}, refs: {tests: [tests/parity/parity_test.go#TestOldParity]}}
- {id: lh.row.prodlock, title: prodlock, expect: pass, compare: {type: exact-json}, request: {method: GET, path: /p}, refs: {tests: [internal/x/e_test.go#TestE]}}
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
  [[ -z "${ONLY:-}" || "$name" == *"$ONLY"* ]] || return 0
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
with_rows() { "$@"; touch_rows; }  # a registry content change, so Rule 1 is not what these cases are about
PRECONDITION='git diff base...HEAD -- internal/x/routes.go | grep -q "^+.*HandleFunc"' \
  run_case "a registration moved within its file is neither a route change nor a feature signal" ok "registry-touch check OK" \
  with_rows move_route_within_file
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
  run_case "a config field moved within its struct is not a feature signal" ok "registry-touch check OK" \
  with_rows move_config_field
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
  printf 'package parity\n\nimport "testing"\n\nfunc TestNewParity(t *testing.T) {}\n' > tests/parity/new_parity_test.go
}
new_test_unreferenced() { registry_row_change; add_parity_test; }
new_test_referenced() {
  add_parity_test
  printf -- '  refs:\n    tests:\n      - tests/parity/new_parity_test.go#TestNewParity\n' >> tests/conformance/registry/features/storage.yaml
}
new_test_in_linked_file() {
  add_parity_test
  printf -- '- id: lh.feature.storage.f\n  title: f\n  tests:\n    - tests/parity/new_parity_test.go\n' >> tests/conformance/registry/features/storage.yaml
}
new_unit_test_unreferenced() {
  registry_row_change
  printf 'package x\n\nimport "testing"\n\nfunc TestNewUnit(t *testing.T) {}\n' > internal/x/new_test.go
}
new_test_helper_not_a_test() {
  registry_row_change
  printf 'package x\n\nimport "testing"\n\nfunc TestMain(m *testing.M) {}\nfunc helper(t *testing.T) {}\nfunc Testify(t *testing.T) {}\n' > internal/y/new_test.go
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
dependency_bump() { sed -i.bak 's#golang.org/x/oauth2 v1.0.0#golang.org/x/oauth2 v1.2.3#' go.mod && rm -f go.mod.bak; printf 'a==1\n' > requirements.txt; printf 'golang.org/x/oauth2 v1.2.3 h1:aaa=\n' >> go.sum; }
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
  "tests/parity/new_parity_test.go#TestNewParity" new_test_unreferenced
run_case "a new unit test in a product package nobody references fails" fail \
  "internal/x/new_test.go#TestNewUnit" new_unit_test_unreferenced
run_case "a new parity test referenced by a feature passes" ok "testlinks OK (1 tests added, all linked" new_test_referenced
run_case "L1: a file-level reference does not link a new test in that file" fail "tests/parity/new_parity_test.go#TestNewParity" new_test_in_linked_file
run_case "TestMain, helpers and Test-prefixed non-tests need no link" ok "testlinks OK (0 tests added" new_test_helper_not_a_test
run_case "a renamed test whose old name is still referenced fails" fail \
  "no such test any more in tests/parity/parity_test.go" rename_referenced_test
run_case "a renamed test is reported as added and unlinked too" fail \
  "tests/parity/parity_test.go#TestRenamedParity" rename_referenced_test
run_case "renaming a lock test with its reference and floor is reported as lock-affecting, not failed" ok "tests/parity/parity_test.go: it holds a lock test" rename_and_relink_test
run_case "a deleted test file whose reference remains fails" fail \
  "registry references point at tests this PR removed" delete_referenced_test_file
run_case "moving a lock test to another file is reported as lock-affecting, not failed" ok "tests/parity/parity_test.go: it holds a lock test" \
  move_test_to_other_file

echo
echo "== exemptions =="
COMMIT_MSG='build(deps): bump y' run_case "a dependency-only PR passes" ok "PR kind: \`dependency-only\`" dependency_bump
# the kind is decided from the diff alone: the commit subject is not an input (it was, and the author writes it)
COMMIT_MSG='feat: not a deps commit, the content decides' run_case "a dependency bump is dependency-only whatever the commit subject says" ok "PR kind: \`dependency-only\`" dependency_bump
run_case "dependency-only waives Rule 1 only: Rules 2 and 3 still report" ok "Rule 3: parity fixes ship locks, locks are not weakened | PASS" dependency_bump
run_case "dependency-only: Rule 1 is n/a" ok "Rule 1: a product change changes the registry | n/a" dependency_bump
run_case "a docs-only PR passes" ok "registry-touch check OK" docs_only
run_case "a CI-only PR passes" ok "registry-touch check OK" ci_only



# There is no exemption: a label, a body line and a label event are not inputs of the gate.
PR_LABELS='registry-exempt' PR_BODY='Registry: none — pure refactor, no behaviour change' EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis \
  run_case "a registry-exempt label and a 'Registry: none' body line exempt nothing" fail "makes no real content change" product_change
PR_LABELS='registry-exempt' PR_BODY='Registry: none — tests only' EVENT_ACTION=labeled LABEL_NAME=registry-exempt SENDER=szibis \
  run_case "nor do they cover unlinked new tests" fail "tests/parity/new_parity_test.go#TestNewParity" new_test_unreferenced

echo
echo "== rule 1 fires on product code only =="
tests_only_change() { mkdir -p tests/e2e; printf 'helper\n' > tests/e2e/data.txt; printf 'x==1\n' > tests/requirements.txt; }
infra_only_change() { printf '\nextra:\n' >> Makefile; printf '#!/bin/sh\n' > scripts/tool.sh; }
test_file_edit_only() { printf '// edit\n' >> internal/y/y_test.go; }
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
run_case "new tests in a tests-only PR still need links (rule 2)" fail "tests/parity/new_parity_test.go#TestNewParity" add_parity_test

echo
echo
echo "== parity fixes ship locks; locks are never weakened =="
drop_allowlist_entry() { sed -i.bak '/TestFixParity/d' tests/parity/known_failures.txt && rm -f tests/parity/known_failures.txt.bak; }
touch_parity_test() { printf 'package parity\n\nimport "testing"\n\nfunc TestFixParity(t *testing.T) { _ = 1 }\n' > tests/parity/fix_test.go; } # a NEW file: editing an existing file of tests/parity is lock code
add_exact_row_for_parity_test() { printf -- '- {id: lh.row.new_lock, title: new lock, expect: pass, compare: {type: exact-json}, refs: {tests: [tests/parity/fix_test.go#TestFixParity]}}\n' >> tests/conformance/registry/rows/lh/endpoints.yaml; }
add_loose_row_for_parity_test() { printf -- '- {id: lh.row.loose, title: loose, expect: pass, compare: {type: status}, refs: {tests: [tests/parity/fix_test.go#TestFixParity]}}\n' >> tests/conformance/registry/rows/lh/endpoints.yaml; }
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
rename_allowlist_entry() { sed -i.bak 's#^TestFixParity #TestFixParity/sub #' tests/parity/known_failures.txt && rm -f tests/parity/known_failures.txt.bak; registry_row_change; }
run_case "renaming an allowlist entry is a weakening with a rename hint" fail "looks like a rename" rename_allowlist_entry
run_case "adding an allowlist entry fails as a weakening" fail "allowlist entry added: TestParity_C" add_allowlist_entry
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
b4_replace_directive() { printf '\nreplace golang.org/x/oauth2 => github.com/evil/b v0.0.1\n' >> go.mod; }
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
  printf '// a comment\n' >> tests/parity/parity_test.go
  sedi 's/title: lock/title: lock (edited)/' $ROWS
}
fix_with_vwh_lock() {
  drop_allowlist_entry; touch_parity_test
  printf -- '- {id: lh.row.vwh2, title: v, expect: pass, compare: {type: values-with-hits, options: {hits_tolerance: "0"}}, refs: {tests: [tests/parity/fix_test.go#TestFixParity]}}\n' >> $ROWS
}
fix_with_loose_vwh() {
  drop_allowlist_entry; touch_parity_test
  printf -- '- {id: lh.row.vwh2, title: v, expect: pass, compare: {type: values-with-hits, options: {hits_tolerance: "0.5"}}, refs: {tests: [tests/parity/fix_test.go#TestFixParity]}}\n' >> $ROWS
}
flip_gap_exact_with_ref() { flip_gap_to_pass; sedi '/lh.row.gap/s#compare: {type: ndjson-multiset}#compare: {type: exact-json}, refs: {tests: [tests/parity/fix_test.go\#TestFixParity]}#' $ROWS; touch_parity_test; }
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
PR_TITLE='build(deps): bump' COMMIT_MSG='feat: x' run_case "and the PR title is no input either: a product go.mod edit titled build(deps) is not dependency-only" fail "shipped build file: go.mod" b4_replace_directive
COMMIT_MSG='chore(deps): bump c' run_case "a VictoriaMetrics/* module bump is never dependency-only" fail "shipped build file: go.mod" dep_vm_module
COMMIT_MSG='chore(deps): bump go' run_case "a go directive change is never dependency-only; it is toolchain-only (Rule 1 waived, Rules 2-3 run)" ok "toolchain-only" dep_go_directive
COMMIT_MSG='build(deps): add e' run_case "a new require line is a new dependency, not a version bump: a product change" fail "shipped build file: go.mod" dep_new_require
COMMIT_MSG='build(deps): bump y' run_case "go.sum hash lines and requirements.txt alone are dependency-only" ok "PR kind: \`dependency-only\`" \
  bash -c "printf 'golang.org/x/oauth2 v1.2.3 h1:aaa=\n' >> go.sum; printf 'a==1\n' > requirements.txt"
COMMIT_MSG='build(deps): bump y' run_case "a go.sum line that is not a hash line is not dependency-only" fail "shipped build file: go.sum" \
  bash -c "printf 'y\n' >> go.sum"
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
run_case "a change to the gate itself is listed in the report, not failed" ok "Gate files changed (informational" gate_file_change
run_case "the gate-file listing names the file" ok "scripts/ci/pr_classify.py" gate_file_change
PR_AUTHOR=szibis run_case "M2: a version-only chart bump with the changelog materialization, by the owner, is release metadata" ok "skipped (release-metadata PR)" chart_version_bump_with_changelog
PR_AUTHOR='github-actions[bot]' run_case "M2: the same by the release bot is release metadata" ok "skipped (release-metadata PR)" chart_version_bump_with_changelog
PR_AUTHOR=mallory run_case "M2: the same by anyone else is not" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_version_bump_with_changelog
run_case "M2: the same with no author known is not" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_version_bump_with_changelog
chart_appversion_downgrade() { chart_version_bump_with_changelog; sedi 's/^appVersion: "9.9.9"/appVersion: "0.120.0"/' charts/victoria-lakehouse/Chart.yaml; }
PR_AUTHOR=szibis run_case "N13: an appVersion downgrade hidden in a release PR is not release metadata" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_appversion_downgrade
chart_reindent() { chart_version_bump_with_changelog; sedi 's/^name: c/ name: c/' charts/victoria-lakehouse/Chart.yaml; }
PR_AUTHOR=szibis run_case "N9: a whitespace change in Chart.yaml is not release metadata" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_reindent
PR_AUTHOR=szibis run_case "M2: a chart description edit hidden in a release PR is not release metadata" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" chart_description_edit_with_changelog
PR_AUTHOR=szibis run_case "M2: a hand-written changelog bullet in a release PR is not release metadata" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" changelog_new_bullet_with_chart
PR_AUTHOR=szibis PR_LABELS=registry-exempt run_case "L3: a stray label changes nothing for a release-metadata PR" ok "skipped (release-metadata PR)" chart_version_bump_with_changelog
run_case "S3: a chart README is documentation" ok "registry-touch check OK" chart_templates_md
run_case "S2: a chart test_*.sh script is a test" ok "registry-touch check OK" chart_test_script
run_case "S3: a chart template file is product even when it is .txt" fail "packaged/patched: charts/victoria-lakehouse/templates/NOTES.txt" chart_template_file

echo

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
  sedi '/TestFixParity/d' tests/parity/known_failures.txt
  touch_parity_test
  printf 'package parity\n\nimport "testing"\n\nfunc TestElse(t *testing.T) { _ = 1 }\n' > tests/parity/else_test.go
  printf -- '- {id: lh.row.else, title: e, expect: pass, compare: {type: exact-json}, refs: {tests: [tests/parity/else_test.go#TestElse]}}\n' >> $ROWS
}
n6_pending_lock() { drop_allowlist_entry; touch_parity_test; printf -- '- {id: lh.row.pend, title: p, expect: pass, pending: true, compare: {type: exact-json}, refs: {tests: [tests/parity/fix_test.go#TestFixParity]}}\n' >> $ROWS; }
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
run_case "N6: a parity fix locked by an unrelated test does not pass" fail "no lock row names TestFixParity" n6_unrelated_lock
run_case "N6/M2: a pending row IS a lock when its test has a cell floor" ok "parity-fix PR: true" n6_pending_lock
n6_pending_lock_no_floor() { n6_pending_lock; : > tests/parity/lock_cells.txt; }
run_case "N6/M2: a pending row is no lock without a cell floor" fail "no lock row names TestFixParity" n6_pending_lock_no_floor
run_case "N7: a product change 'covered' by a prose edit fails" fail "makes no real content change" n7_prose_only_registry_edit
COMMIT_MSG='build(deps): bump c' run_case "N8: a quoted VictoriaMetrics require is still critical" fail "shipped build file: go.mod" n8_quoted_require
run_case "N10: parity_ratchet.py is a gate file (listed)" ok "scripts/ci/parity_ratchet.py" gate_file_touch
run_case "N11: gutting the parity workflow is a Rule 3 failure (its allowlist argument is read from the merge base)" fail "has no --allowlist argument" workflow_gate_touch parity.yaml
run_case "C1: conformance.yaml is a gate file (listed)" ok "Gate files changed" workflow_gate_touch conformance.yaml
run_case "registry-gate.yaml is a gate file (listed)" ok ".github/workflows/registry-gate.yaml" workflow_gate_touch registry-gate.yaml
run_case "registry-gate-base.yaml is a gate file (listed)" ok ".github/workflows/registry-gate-base.yaml" workflow_gate_touch registry-gate-base.yaml
run_case "gate-selftest.yaml is a gate file (listed)" ok ".github/workflows/gate-selftest.yaml" workflow_gate_touch gate-selftest.yaml
run_case "L2: a new tests/s3compat test must be linked" fail "tests/s3compat/a_test.go#TestS3New" l2_s3compat_test
run_case "a lock's renamed test is reported, not failed (the floor moves with it)" ok "tests/parity/parity_test.go: it holds a lock test" renamed_lock_reference
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
run_case "X7: .gitattributes is a gate file (listed)" ok ".gitattributes" x7_gitattributes_alone
run_case "X7: .gitattributes diff and text attributes do not hide anything" ok ".gitattributes" x7_gitattributes_textconv
run_case "X7: a VL_VERSION_LOGS bump is a product change" fail "route/handler/upstream files" x7_pin_bump
run_case "X7: a VL_COMMIT_TRACES bump is a product change" fail "route/handler/upstream files" x7_traces_pin_bump
run_case "X7: a VT_VERSION bump is a product change" fail "route/handler/upstream files" x7_vt_pin_bump
for mod in github.com/klauspost/compress github.com/golang/snappy google.golang.org/protobuf github.com/pierrec/lz4/v4; do
  COMMIT_MSG="build(deps): bump $mod" run_case "X9: a $mod bump is never dependency-only" fail "shipped build file: go.mod" x9_bump "$mod"
done
lock_helper_touch() { printf 'package parity\n\n// x\n' >> tests/parity/lock_cells_test.go; }
run_case "the lock-cells helper is a gate file (listed)" ok "tests/parity/lock_cells_test.go" lock_helper_touch
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
  out="$(cd "$dir" && HEAD_REV="pr" bash "$CHECKER" base 2>&1)"; rc1=$?
  (cd "$dir" && bash "$CHECKER" base) >/dev/null 2>&1; rc2=$?
  if [[ $rc1 -ne 0 && $rc2 -eq 0 ]] && grep -q "makes no real content change" <<<"$out"; then
    echo "ok   - HEAD_REV judges that revision, not the checked-out one (the pull_request_target gate has no PR checkout)"; pass=$((pass + 1))
  else
    echo "FAIL - HEAD_REV (rc with=$rc1, without=$rc2)"; echo "$out" | sed 's/^/       /'; fail=$((fail + 1))
  fi
  rm -rf "$dir"
}
head_rev_case
run_case "X10: a new file under scripts/ci that could shadow an import is a gate file (listed)" ok "scripts/ci/dataclasses.py" x10_shadow_module
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

  stale_out="$(cd "$tmp/repo" && SKIP_CONFGEN_CHECK='' bash "$CHECKER" base 2>&1)"
  stale_rc=$?

  ( cd "$tmp/repo" && GOWORK=off go run ./tests/conformance/cmd/confgen -write && git add -A && git commit -q -m "make conformance-gen" ) >/dev/null 2>&1

  out="$(cd "$tmp/repo" && SKIP_CONFGEN_CHECK='' bash "$CHECKER" base 2>&1)"
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
    env SKIP_CONFGEN_CHECK='' bash "$CHECKER" base

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
    env SKIP_CONFGEN_CHECK='' bash "$CHECKER" release

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
    bash -c 'out=$(python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/release-metadata-approvers); echo "$out"; git diff --stat base HEAD; git diff base HEAD -- docs/features.md | grep "^[-+]" | head -20; grep -qx "exempt=release-metadata" <<<"$out"'
  check_step "the changelog gate passes" \
    python3 scripts/ci/check_changelog_pr.py --base base --head HEAD

  # Negatives, each one commit on top.
  check_step "a product file is added" bash -c 'echo "package server" > internal/zz_relmeta_fixture.go && git add -A && git commit -q -m "product"'
  check_step "the product PR is no longer a release-metadata PR" \
    bash -c 'python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/release-metadata-approvers | grep -qx "exempt=none"'
  check_step "the product PR is classified as product" \
    bash -c 'python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/release-metadata-approvers | grep -qx "product=1"'
  expect_fail "the registry gate fails a metadata-looking PR that also changes a product file" \
    env SKIP_CONFGEN_CHECK= PR_AUTHOR=szibis bash "$CHECKER" base
  check_step "the product file is dropped and a non-generated doc added" \
    bash -c 'git rm -q internal/zz_relmeta_fixture.go && echo x > docs/zz-relmeta-fixture.md && git add -A && git commit -q -m "doc"'
  check_step "a non-generated doc is not a release-metadata PR" \
    bash -c 'python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/release-metadata-approvers | grep -qx "exempt=none"'
  check_step "the non-generated doc is dropped and docs/features.md gets a change beyond version naming" \
    bash -c 'git rm -q docs/zz-relmeta-fixture.md && echo "an invented line" >> docs/features.md && git add -A && git commit -q -m "features edit"'
  check_step "a features.md change beyond version naming is not a release-metadata PR" \
    bash -c 'python3 scripts/ci/pr_classify.py --base base --head HEAD --author szibis --approvers .github/release-metadata-approvers | grep -qx "exempt=none"'
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

echo "== lock packages: lock-affecting files are reported, content weakenings fail =="
lock_test_edited() { registry_row_change; printf 'package parity\n\nimport "testing"\n\nfunc TestOldParity(t *testing.T) { t.Skip() }\n' > tests/parity/parity_test.go; }
lock_helper_added() { registry_row_change; printf '\nfunc judge() bool { return true }\n' >> tests/parity/parity_test.go; }
lock_pkg_init_file() { registry_row_change; printf 'package parity\n\nfunc init() {}\n' > tests/parity/setup_test.go; }
lock_pkg_var_file() { registry_row_change; printf 'package parity\n\nvar _ = func() int { return 0 }()\n' > tests/parity/zz_env_test.go; }
lock_pkg_testmain_file() { registry_row_change; printf 'package parity\n\nimport "testing"\n\nfunc TestMain(m *testing.M) {}\n' > tests/parity/main_test.go; }
lock_pkg_func_only_file() { registry_row_change; printf 'package parity\n\nfunc Helper() {}\n\nconst K = 1\n\ntype T struct{}\n' > tests/parity/rows_ties.go; }
lock_pkg_init_appended_to_nonlock_file() { registry_row_change; printf 'package parity\n\nfunc TestNotALock(t *testing.T) {}\n\nfunc init() {}\n' > tests/parity/parity_test.go; }
lock_pkg_compose_edited() { registry_row_change; mkdir -p tests/parity; printf 'services: {lh: {}}\n' > tests/parity/docker-compose.yml; }
lock_pkg_existing_plain_go_edited() { registry_row_change; printf 'package parity\n\nfunc helperEdited() {}\n' > tests/parity/helpers.go; }
lock_new_test_new_file() {
  printf 'package parity\n\nimport "testing"\n\nfunc TestFreshParity(t *testing.T) {}\n' > tests/parity/fresh_test.go
  printf -- '  refs:\n    tests:\n      - tests/parity/fresh_test.go#TestFreshParity\n' >> tests/conformance/registry/features/storage.yaml
}
other_test_file_edited() { printf 'package x\n\nimport "testing"\n\nfunc TestOther(t *testing.T) {}\n' > internal/x/other_test.go; printf -- '  refs:\n    tests:\n      - internal/x/other_test.go#TestOther\n' >> tests/conformance/registry/features/storage.yaml; }
run_case "adding a Skip to a lock test file is a Rule 3 failure" fail "a Skip call was added" lock_test_edited
run_case "the failure names the file" fail "tests/parity/parity_test.go" lock_test_edited
run_case "adding a helper to a lock test file is reported, not failed" ok "tests/parity/parity_test.go: it holds a lock test" lock_helper_added
run_case "a new file with init() in a lock package is a Rule 3 failure" fail "tests/parity/setup_test.go: adds 1 new init() function(s) to a package that holds a registry lock" lock_pkg_init_file
run_case "G1: a new file with a package-level var initializer is a Rule 3 failure" fail "tests/parity/zz_env_test.go: adds package-level var whose initializer calls a function: _ = func() int { return 0 }() to a package that holds a registry lock" lock_pkg_var_file
run_case "G1: the failure is Rule 3's row, with a how-to-fix line" fail "| Rule 3: parity fixes ship locks, locks are not weakened | FAIL" lock_pkg_var_file
init_exit() {
  # a test file whose init() ends the test binary with success when the nonce of the lock run is set: every lock of the package "passes"
  registry_row_change
  printf 'package parity\n\nimport "os"\n\nfunc init() {\n\tif os.Getenv("PARITY_LOCK_NONCE") != "" {\n\t\tos.Exit(0)\n\t}\n}\n' > tests/parity/zz_env_test.go
}
run_case "init_exit: a new test file whose init() exits 0 under the lock nonce is a Rule 3 failure" fail "adds 1 new init() function(s)" init_exit
run_case "a new TestMain in a lock package is a Rule 3 failure (it can silence every lock of the package)" fail "TestMain of a package that holds a lock changed" lock_pkg_testmain_file
run_case "and the new file is also listed as lock-affecting" fail "tests/parity/main_test.go: a new file in the package of a lock declares func TestMain" lock_pkg_testmain_file
run_case "a new file with only funcs, consts and types is listed as lock-affecting, not failed" ok "tests/parity/rows_ties.go: a new Go file in a package that holds a registry lock" lock_pkg_func_only_file
run_case "and the verdict says a person has to look" ok "PASS - 1 lock-affecting file needs review" lock_pkg_func_only_file
lock_pkg_init_appended() { registry_row_change; printf '\nfunc init() {}\n' >> tests/parity/parity_test.go; }
run_case "G2: an init() appended to a test file of a lock package is a Rule 3 failure" fail "tests/parity/parity_test.go: adds 1 new init() function(s)" lock_pkg_init_appended
lock_pkg_var_edited_in_place() { registry_row_change; printf 'package parity\n\nimport "testing"\n\nvar table = []int{1, 2}\n\nfunc TestOldParity(t *testing.T) {}\n' > tests/parity/parity_test.go; }
lock_pkg_var_table_call() { registry_row_change; printf 'package parity\n\nimport "testing"\n\nvar table = load()\n\nfunc TestOldParity(t *testing.T) {}\n' > tests/parity/parity_test.go; }
run_case "a var in a lock test file whose initializer calls a function is a Rule 3 failure (the file declares code the base did not)" fail "package-level var whose initializer calls a function: table = load()" lock_pkg_var_table_call
run_case "a literal var table in a lock test file runs no code: listed as lock-affecting, not failed (#438 shape)" ok "tests/parity/parity_test.go: it holds a lock test" lock_pkg_var_edited_in_place
run_case "G3: the parity compose runtime config is reported" ok "tests/parity/docker-compose.yml: runtime configuration of the lock suites" lock_pkg_compose_edited
run_case "an existing plain .go helper of a lock package is reported" ok "tests/parity/helpers.go" lock_pkg_existing_plain_go_edited
parity_compose_toolchain_tag() { sedi 's#golang:1.22.0#golang:1.22.1#' tests/parity/docker-compose.yml; registry_row_change; }
run_case "a Go image tag bump of the parity compose file is not lock-affecting" ok "registry-touch check OK" parity_compose_toolchain_tag
run_case "a new test in a new file of a lock package is free" ok "testlinks OK" lock_new_test_new_file
lock_product_test_file_edited() { registry_row_change; printf 'package x\n\nimport "testing"\n\nfunc TestE(t *testing.T) { t.Skip() }\n' > internal/x/e_test.go; }
lock_product_new_nontest_file() { registry_row_change; printf 'package x\n\nvar V = 1\n\nfunc init() {}\n' > internal/x/new.go; }
lock_product_new_test_var() { registry_row_change; printf 'package x\n\nvar V = compute()\n' > internal/x/v_test.go; }
lock_product_new_test_table() { registry_row_change; printf 'package x\n\nvar V = []int{1}\n' > internal/x/v_test.go; }
run_case "a product file edit in a package that holds a lock passes" ok "registry-touch check OK" product_and_row
run_case "a new non-test file in a product lock package is free" ok "" lock_product_new_nontest_file
run_case "adding a Skip to a test file of a product lock package fails Rule 3" fail "internal/x/e_test.go" lock_product_test_file_edited
run_case "a new test file with a called package var in a product lock package is a Rule 3 failure" fail "internal/x/v_test.go: adds package-level var whose initializer calls a function: V = compute()" lock_product_new_test_var
run_case "a new test file with a literal table in a product lock package is free" ok "registry-touch check OK" lock_product_new_test_table
run_case "a new test file outside every lock package is free" ok "" other_test_file_edited

echo
echo "== toolchain-only: a Go toolchain bump is recognised from content and waives Rule 1 only =="
tc_bump() {
  sedi 's#^go 1.22$#go 1.22.5#' go.mod
  sedi 's#golang.org/x/oauth2 v1.0.0#golang.org/x/oauth2 v1.2.3#' go.mod
  printf 'golang.org/x/net v0.60.0 h1:aaa=\n' >> go.sum
  sedi 's#golang:1.22.0-alpine3.20#golang:1.22.5-alpine3.20#' Dockerfile.logs
  sedi 's#golang:1.22.0#golang:1.22.5#' tests/parity/docker-compose.yml
  printf '### Security\n' > /dev/null
}
tc_bump_base_image() { tc_bump; sedi 's#golang:1.22.5-alpine3.20#debian:12#' Dockerfile.logs; }
tc_bump_flag() { tc_bump; sedi 's#^RUN make$#RUN make FIPS=1#' Dockerfile.logs; }
tc_bump_env() { tc_bump; sedi 's#LH_BASE_URL: lh#LH_BASE_URL: vl#' tests/parity/docker-compose.yml; }
tc_bump_floor() { tc_bump; sedi 's/TestOldParity  5/TestOldParity  4/' tests/parity/lock_cells.txt; }
tc_bump_code() { tc_bump; product_change; }
tc_bump_critical() { tc_bump; sedi 's#github.com/VictoriaMetrics/c v1.0.0#github.com/VictoriaMetrics/c v1.1.0#' go.mod; }
tc_bump_drop_lock_ref() { tc_bump; sedi 's#refs: {tests: \[tests/parity/parity_test.go\#TestOldParity\]}#refs: {tests: []}#' $ROWS; }
tc_bump_gate_file() { tc_bump; printf '#!/bin/sh\n' > scripts/ci/new_helper.sh; }
tc_bump_replace() { tc_bump; printf '\nreplace golang.org/x/oauth2 => github.com/evil/b v0.0.1\n' >> go.mod; }
tc_docs_ride_along() { tc_bump; printf '# n\n' > docs/note.md; printf 'name: s\n' > .github/workflows/security.yaml; }
COMMIT_MSG='feat: not a deps commit, the content decides' run_case "a Go toolchain bump is toolchain-only whatever the commit subject says" ok "PR kind: \`toolchain-only\`" tc_bump
run_case "toolchain-only waives Rule 1" ok "Rule 1: a product change changes the registry | n/a" tc_bump
run_case "toolchain-only still runs Rules 2 and 3" ok "Rule 3: parity fixes ship locks, locks are not weakened | PASS" tc_bump
run_case "docs, workflows and scripts may ride along" ok "toolchain-only" tc_docs_ride_along
run_case "a different base image in the same PR is a product change (Rule 1)" fail "makes no real content change" tc_bump_base_image
run_case "an added flag in the Dockerfile is a product change (Rule 1)" fail "makes no real content change" tc_bump_flag
run_case "a changed env var in the parity compose file is not toolchain-only" fail "makes no real content change" tc_bump_env
run_case "a replace directive is not toolchain-only" fail "makes no real content change" tc_bump_replace
run_case "a storage-critical module bump is not toolchain-only" fail "makes no real content change" tc_bump_critical
run_case "product code in the same PR is not toolchain-only" fail "makes no real content change" tc_bump_code
run_case "a gate file in the same PR is not toolchain-only" fail "makes no real content change" tc_bump_gate_file
run_case "a toolchain bump cannot carry a lowered lock floor (Rule 3 has no bypass)" fail "cell floor of TestOldParity removed or lowered (5 -> 4)" tc_bump_floor
run_case "a toolchain bump cannot carry a deleted lock reference (Rule 3 has no bypass)" fail "lost its test reference" tc_bump_drop_lock_ref
tc_bump_unlinked_test() { tc_bump; new_test_unreferenced; }
run_case "a toolchain bump cannot carry an unlinked new test (Rule 2 still runs)" fail "tests/parity/new_parity_test.go#TestNewParity" tc_bump_unlinked_test

echo
echo "== one report: every rule gets a row, failures say how to fix =="
run_case "a passing run says PASS" ok "## Registry gate: PASS" product_and_row
run_case "a failing run says FAIL" fail "## Registry gate: FAIL" product_change
run_case "a failure carries a how-to-fix line" fail "### How to fix" product_change
run_case "the kind of PR is named" fail "PR kind: product change (product code: internal/x/x.go)" product_change
run_case "a release-metadata-free docs PR is named as not a product change" ok "PR kind: not a product change" docs_only
run_case "a route without rows does not stop the other rules from reporting" fail "| Rule 3: parity fixes ship locks, locks are not weakened |" add_route
run_case "the route rule has its own FAIL row" fail "| Routes, handlers, pins | FAIL |" add_route

report_once_case() {
  local dir out sum n
  dir="$(new_repo)"; sum="$(mktemp)"
  ( cd "$dir" && product_change && git add -A && git commit -q -m change ) >/dev/null 2>&1
  out="$(cd "$dir" && GITHUB_STEP_SUMMARY="$sum" bash "$CHECKER" base 2>&1)"
  n=$(grep -c '^## Registry gate: FAIL' "$sum")
  if [[ "$n" == 1 ]] && ! grep -q '^## Registry gate' <<<"$out" && grep -q '^| Rule 1:' "$sum"; then
    echo "ok   - the report is written once to the job summary and not repeated on stdout"; pass=$((pass + 1))
  else
    echo "FAIL - report once (summary copies: $n)"; cat "$sum"; echo "$out" | sed 's/^/       /'; fail=$((fail + 1))
  fi
  rm -rf "$dir" "$sum"
}
report_once_case

absent_case() {
  local name=$1 absent=$2; shift 2
  local dir out
  dir="$(new_repo)"
  ( cd "$dir" && "$@" && git add -A && git commit -q -m change ) >/dev/null 2>&1
  out="$(cd "$dir" && bash "$CHECKER" base 2>&1)"
  if ! grep -qF -- "$absent" <<<"$out"; then
    echo "ok   - $name"; pass=$((pass + 1))
  else
    echo "FAIL - $name (found '$absent')"; echo "$out" | sed 's/^/       /'; fail=$((fail + 1))
  fi
  rm -rf "$dir"
}
absent_case "a golang image tag bump of the parity compose file is not listed as lock-affecting" "docker-compose.yml: runtime configuration" parity_compose_toolchain_tag

echo
echo "== B1/M2: a line break that the classifier's reader sees and Docker/Go do not =="
# The fixture Dockerfile.logs holds "# FIPS is set per deployment." above "ARG FIPS=0". With the LF between
# them replaced by a CR, Docker reads ONE comment line (the build argument is gone); a reader that turns a
# lone CR into a line break saw an unchanged file and approved the Go bump as toolchain-only.
swap_bytes() { python3 -c 'import sys; f,a,b=sys.argv[1:4]; s=open(f,"rb").read(); a=a.encode().decode("unicode_escape").encode("latin-1"); b=b.encode().decode("unicode_escape").encode("utf-8"); n=s.replace(a,b,1); assert n!=s, "nothing replaced"; open(f,"wb").write(n)' "$@"; }
rebase_here() { git add -A; git commit -q -m "base2"; git branch -f base HEAD; }
cr_fips() { tc_bump; swap_bytes Dockerfile.logs 'deployment.\nARG FIPS=0' 'deployment.\rARG FIPS=0'; }
run_case "cr_fips: a CR between a comment and ARG FIPS=0 is not toolchain-only (it was approved)" fail "shipped build file: Dockerfile.logs" cr_fips
cr_fips_lf() { tc_bump; }
run_case "cr_fips control: the same bump without the CR is toolchain-only" ok "PR kind: \`toolchain-only\`" cr_fips_lf
cr_replace_base() { printf '\nreplace golang.org/x/oauth2 => ./deps/b\n' >> go.mod; rebase_here; }
cr_replace_tc() { cr_replace_base; tc_bump; swap_bytes go.mod '\nreplace golang.org/x/oauth2' '\n// local tree\rreplace golang.org/x/oauth2'; }
run_case "cr_replace_tc: a CR hiding a replace directive is not toolchain-only (the patched tree was dropped)" fail "makes no real content change" cr_replace_tc
u2028_replace_tc() { cr_replace_base; tc_bump; swap_bytes go.mod '\nreplace golang.org/x/oauth2' '\n// local tree replace golang.org/x/oauth2'; }
run_case "u2028_replace_tc: so does U+2028" fail "makes no real content change" u2028_replace_tc
dep_bump_only() { sedi 's#golang.org/x/oauth2 v1.0.0#golang.org/x/oauth2 v1.2.3#' go.mod; printf 'golang.org/x/oauth2 v1.2.3 h1:aaa=\n' >> go.sum; }
cr_replace_dep() { cr_replace_base; dep_bump_only; swap_bytes go.mod '\nreplace golang.org/x/oauth2' '\n// local tree\rreplace golang.org/x/oauth2'; }
u2028_replace_dep() { cr_replace_base; dep_bump_only; swap_bytes go.mod '\nreplace golang.org/x/oauth2' '\n// local tree replace golang.org/x/oauth2'; }
COMMIT_MSG='build(deps): bump a/b' run_case "cr_replace_dep: a CR hiding a replace directive is not dependency-only (the gate skipped Rules 1-3)" fail "shipped build file: go.mod" cr_replace_dep
COMMIT_MSG='build(deps): bump a/b' run_case "u2028_replace_dep: so does U+2028" fail "shipped build file: go.mod" u2028_replace_dep
dep_bump_with_replace_kept() { cr_replace_base; dep_bump_only; }
COMMIT_MSG='build(deps): bump a/b' run_case "control: the same bump with the replace line visible and unchanged is dependency-only" ok "PR kind: \`dependency-only\`" dep_bump_with_replace_kept
tc_unidigit() { tc_bump; swap_bytes Dockerfile.logs 'golang:1.22.5' 'golang:1.22.٣'; }
run_case "tc_unidigit: an Arabic-Indic digit in the golang tag is not a version" fail "shipped build file: Dockerfile.logs" tc_unidigit
crlf_base() { python3 -c 'for f in ("go.mod", "Dockerfile.logs"):
    data = open(f, "rb").read()
    assert data
    open(f, "wb").write(data.replace(b"\n", b"\r\n"))'; rebase_here; }
crlf_whole_file() { crlf_base; tc_bump; }
run_case "a whole file with CRLF endings in base and head is never verified (it stays a product change)" fail "makes no real content change" crlf_whole_file
tc_xxhash() { tc_bump; sedi 's#xxhash/v2 v2.3.0#xxhash/v2 v2.4.0#' go.mod; }
run_case "tc_xxhash: the stream-id hash is a storage-critical module, not a toolchain bump" fail "makes no real content change" tc_xxhash
dep_xxhash() { sedi 's#xxhash/v2 v2.3.0#xxhash/v2 v2.4.0#' go.mod; }
COMMIT_MSG='build(deps): bump xxhash' run_case "tc_xxhash: nor dependency-only" fail "shipped build file: go.mod" dep_xxhash
tc_alpinever() { sedi 's#alpine3.20#alpine3.21#' Dockerfile.logs; }
run_case "tc_alpinever: an alpine-release-only change is toolchain-only by design (documented)" ok "PR kind: \`toolchain-only\`" tc_alpinever

echo
echo "== M4: the control that replaces the old failures is visible =="
two_lock_files() { registry_row_change; printf '\nfunc judge() bool { return true }\n' >> tests/parity/parity_test.go; printf '\nfunc helperEdited() {}\n' >> tests/parity/helpers.go; }
run_case "a lock-affecting file is an inline annotation" ok "::warning file=tests/parity/parity_test.go::lock-affecting file: the owner's approval is the control" lock_helper_added
run_case "the Rule 3 verdict says how many files need review" ok "| Rule 3: parity fixes ship locks, locks are not weakened | PASS - 2 lock-affecting files need review (listed below) |" two_lock_files
run_case "the report header says it too, instead of a bare PASS" ok "## Registry gate: PASS - 2 lock-affecting files need review (listed below)" two_lock_files
run_case "one file reads 'needs review'" ok "PASS - 1 lock-affecting file needs review (listed below)" lock_helper_added
run_case "a gate file is an inline annotation" ok "::warning file=scripts/ci/pr_classify.py::gate file: the owner's approval is the control" gate_file_change
run_case "a gate file shows in the header count" ok "## Registry gate: PASS - 1 gate file needs review (listed below)" gate_file_change
lock_and_gate() { two_lock_files; gate_file_change; }
run_case "lock-affecting and gate files are counted together" ok "## Registry gate: PASS - 2 lock-affecting files and 1 gate file need review (listed below)" lock_and_gate
absent_case "a PR with nothing to review keeps the bare PASS" "need review" product_and_row

many_gate_files() { local i; for i in 01 02 03 04 05 06 07 08 09 10 11 12; do printf '#!/bin/sh\n' > "scripts/ci/g$i.sh"; done; }
cap_case() {
  local name=$1 want=$2 fn=$3 dir out n
  dir="$(new_repo)"
  ( cd "$dir" && "$fn" && git add -A && git commit -q -m change ) >/dev/null 2>&1
  out="$(cd "$dir" && bash "$CHECKER" base 2>&1)"
  n=$(grep -c '^::warning file=scripts/ci/g' <<<"$out")
  if [[ "$n" == 10 ]] && grep -qF -- "$want" <<<"$out"; then echo "ok   - $name"; pass=$((pass + 1)); else echo "FAIL - $name ($n inline annotations)"; echo "$out" | sed 's/^/       /' | head -30; fail=$((fail + 1)); fi
  rm -rf "$dir"
}
cap_case "annotations are capped at 10 per kind, then one '... and N more' line" "::warning::... and 2 more gate files (see the job summary)" many_gate_files

echo
echo "== untrusted strings never reach the report or the log raw =="
hostile_case() {
  local dir out sum ok=1 why=""
  dir="$(new_repo)"; sum="$(mktemp)"
  (
    cd "$dir" || exit 1
    printf '#!/bin/sh\n' > 'scripts/ci/a`<!--x'
    printf '#!/bin/sh\n' > 'scripts/ci/::error::boom'
    printf '#!/bin/sh\n' > 'scripts/ci/p%q,r:s'
    printf '#!/bin/sh\n' > 'scripts/ci/pipe|x'
    printf -- '- {id: "lh.x\\n::error::forged`<!--|", title: t}\n' >> tests/conformance/registry/rows/lh/endpoints.yaml
    git add -A && git commit -q -m change
  ) >/dev/null 2>&1
  out="$(cd "$dir" && GITHUB_STEP_SUMMARY="$sum" bash "$CHECKER" base 2>&1)"
  if grep -q '<!--' "$sum"; then ok=0; why="$why raw <!-- in the report;"; fi
  grep -q 'scripts/ci/a'"'"'&lt;!--x' "$sum" || { ok=0; why="$why escaped name missing from the report;"; }
  grep -q 'scripts/ci/pipe\\|x' "$sum" || { ok=0; why="$why pipe not escaped;"; }
  if sed 's/^[[:space:]]*//' <<<"$out" | grep -qE '^::error::(boom|forged)'; then ok=0; why="$why a hostile string started a workflow command in the log;"; fi
  grep -q '^::warning file=scripts/ci/p%25q%2Cr%3As::gate file' <<<"$out" || { ok=0; why="$why property not escaped;"; }
  grep -q 'lh.x ::error::forged' "$sum" || { ok=0; why="$why row id not flattened in the report;"; }
  if [[ $ok -eq 1 ]]; then echo "ok   - hostile file names and row ids are escaped (report, annotations, log)"; pass=$((pass + 1)); else echo "FAIL - hostile strings:$why"; sed 's/^/       summary: /' "$sum" | head -40; echo "$out" | sed 's/^/       log: /' | head -40; fail=$((fail + 1)); fi
  rm -rf "$dir" "$sum"
}
hostile_case

echo
echo "== paths git would quote or split: spaces and non-ASCII names =="
# git prints a path with a space unquoted (white-space splitting loses the first half) and a non-ASCII
# path in octal quotes (an anchored pattern misses it): the gate reads paths NUL-separated.
space_test_name() {
  registry_row_change
  printf 'package parity\n\nimport "testing"\n\nfunc TestSpaceName(t *testing.T) {}\n' > 'tests/parity/new space_test.go'
}
run_case "a new test in a file whose name has a space is still seen by Rule 2" fail "tests/parity/new space_test.go#TestSpaceName" space_test_name
utf8_route_file() {
  printf 'package selectapi\n\nfunc register() { mux.HandleFunc("/lakehouse/api/v1/utf8", nil) }\n' > $'internal/selectapi/caf\303\251.go'
}
utf8_route_setup() { mkdir -p internal/selectapi; utf8_route_file; }
run_case "a route registered in a non-ASCII file name still needs registry rows" fail "but not tests/conformance/registry/rows/" utf8_route_setup

echo
echo "== the release-metadata approvers file was renamed: the merge base decides which name is read =="
approvers_new_only() { git mv .github/registry-exempt-approvers .github/release-metadata-approvers; rebase_here; chart_version_bump_with_changelog; }
approvers_both_new_wins() { printf '# approvers\nalice\n' > .github/release-metadata-approvers; rebase_here; chart_version_bump_with_changelog; }
approvers_added_by_pr() { printf 'mallory\n' > .github/release-metadata-approvers; chart_version_bump_with_changelog; }
PR_AUTHOR=szibis run_case "the new name alone is read" ok "skipped (release-metadata PR)" approvers_new_only
PR_AUTHOR=mallory run_case "the new name alone: a stranger is still no approver" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" approvers_new_only
PR_AUTHOR=alice run_case "both names at the merge base: the new one wins" ok "skipped (release-metadata PR)" approvers_both_new_wins
PR_AUTHOR=szibis run_case "both names: a login only in the old file is not an approver" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" approvers_both_new_wins
PR_AUTHOR=szibis run_case "the old name alone (a merge base before the rename) is the fallback" ok "skipped (release-metadata PR)" chart_version_bump_with_changelog
PR_AUTHOR=mallory run_case "a PR cannot approve itself by adding the new file" fail "packaged/patched: charts/victoria-lakehouse/Chart.yaml" approvers_added_by_pr

echo
echo "== the hardening of issue #502: the gaps of the second independent review =="
# M-A: a new func-only test file in a lock package is listed with a warning annotation, not silently free
lock_pkg_warmup() {
  registry_row_change
  printf 'package parity\n\nimport "testing"\n\nfunc TestAAAWarmup(t *testing.T) {\n\tvlBaseURL = lhBaseURL\n}\n' > tests/parity/aaa_warmup_test.go
  printf -- '  refs:\n    tests:\n      - tests/parity/aaa_warmup_test.go#TestAAAWarmup\n' >> tests/conformance/registry/features/storage.yaml
}
run_case "M-A: a new func-only test in a lock package is listed in the report" ok "tests/parity/aaa_warmup_test.go: a new Go file in a package that holds a registry lock" lock_pkg_warmup
run_case "M-A: ... with an inline annotation for the Files tab" ok "::warning file=tests/parity/aaa_warmup_test.go::lock-affecting file" lock_pkg_warmup
lock_pkg_forged_exit() {
  registry_row_change
  printf 'package parity\n\nimport (\n\t"fmt"\n\t"syscall"\n\t"testing"\n)\n\nfunc TestZZForged(t *testing.T) {\n\tfmt.Println("lock-cells nonce TestOldParity 50")\n\tsyscall.Exit(0)\n}\n' > tests/parity/zz_forged_test.go
  printf -- '  refs:\n    tests:\n      - tests/parity/zz_forged_test.go#TestZZForged\n' >> tests/conformance/registry/features/storage.yaml
}
run_case "M-A: a test that prints a forged transcript and exits is listed" ok "tests/parity/zz_forged_test.go: a new Go file in a package that holds a registry lock" lock_pkg_forged_exit
lock_pkg_blank_import() {
  registry_row_change
  mkdir -p tests/parityhook
  printf 'package parityhook\n\nimport "os"\n\nfunc init() { os.Exit(0) }\n' > tests/parityhook/hook.go
  printf 'package parity\n\nimport _ "example.com/x/tests/parityhook"\n' > tests/parity/zz_hook_test.go
}
run_case "M-A: a blank import of a new package is listed and the import is named" ok 'blank-imports "example.com/x/tests/parityhook"' lock_pkg_blank_import
lock_pkg_cgo() {
  registry_row_change
  printf 'package parity\n\n/*\nvoid __attribute__((constructor)) boot(void) {}\n*/\nimport "C"\n' > tests/parity/zz_cgo.go
}
run_case "M-A: import \"C\" in a lock package is a Rule 3 failure" fail 'tests/parity/zz_cgo.go: imports "C" (cgo) in a package that holds a registry lock' lock_pkg_cgo
run_case "M-A: ... on Rule 3's row" fail "| Rule 3: parity fixes ship locks, locks are not weakened | FAIL" lock_pkg_cgo
lock_pkg_native_source() { registry_row_change; printf 'void f(void) {}\n' > tests/parity/ctor.c; }
run_case "M-A: a new C source file next to the lock tests is listed" ok "tests/parity/ctor.c: a new native source file" lock_pkg_native_source

# M-B: a line separator inside an allowlist line hides an entry from the Go gate (LF only) and not from the
# Python ratchet (splitlines); the gate refuses the bytes
append_to_line2() { # append_to_line2 <file> <bytes in printf escapes>
  awk -v tail="$(printf "$2")" 'NR == 2 { printf "%s%sTestSmuggled  # B9: behind a separator\n", $0, tail; next } { print }' "$1" > "$1.tmp" && mv "$1.tmp" "$1"
}
allowlist_u2028() { append_to_line2 tests/parity/known_failures.txt '\342\200\250'; }
allowlist_lone_cr() { append_to_line2 tests/parity/known_failures.txt '\r'; }
lock_cells_nel() { append_to_line2 tests/parity/lock_cells.txt '\302\205'; }
allowlist_comment_edit() { printf '# a note on the allowlist\n' >> tests/parity/known_failures.txt; }
PRECONDITION='grep -q "TestSmuggled" tests/parity/known_failures.txt' run_case "M-B: a U+2028 inside an allowlist line is a Rule 3 failure" fail "tests/parity/known_failures.txt: U+2028 at line 2" allowlist_u2028
PRECONDITION='grep -q "TestSmuggled" tests/parity/known_failures.txt' run_case "M-B: a lone CR inside an allowlist line is a Rule 3 failure" fail "tests/parity/known_failures.txt: U+000D at line 2" allowlist_lone_cr
PRECONDITION='grep -q "TestSmuggled" tests/parity/lock_cells.txt' run_case "M-B: a NEL inside lock_cells.txt is a Rule 3 failure" fail "tests/parity/lock_cells.txt: U+0085 at line 2" lock_cells_nel
run_case "M-B: a clean edit of the allowlist is no error, and the file is listed as a gate file" ok "tests/parity/known_failures.txt" allowlist_comment_edit

# M-C: valyala/fastjson and yaml.v3 parse ingest bytes; a module the gate does not know is critical
COMMIT_MSG='build(deps): bump fastjson' run_case "M-C: a valyala/fastjson bump is not dependency-only" fail "shipped build file: go.mod" x9_bump github.com/valyala/fastjson
COMMIT_MSG='build(deps): bump yaml' run_case "M-C: a gopkg.in/yaml.v3 bump is not dependency-only" fail "shipped build file: go.mod" x9_bump gopkg.in/yaml.v3
COMMIT_MSG='build(deps): bump x/net' run_case "M-C: a golang.org/x/net bump (PR #491) stays dependency-only" ok "PR kind: \`dependency-only\`" x9_bump golang.org/x/net
x_unknown_module() { sedi 's#^)#\tgithub.com/never/heard-of-it v1.0.0\n)#' go.mod; }
COMMIT_MSG='build(deps): add a module' run_case "M-C: a module the gate has never heard of is critical (and new)" fail "shipped build file: go.mod" x_unknown_module
tc_bump_fastjson() { tc_bump; sedi 's#github.com/valyala/fastjson v1.6.10#github.com/valyala/fastjson v1.6.11#' go.mod; }
run_case "M-C: a fastjson bump riding with a Go toolchain bump is a product change" fail "makes no real content change" tc_bump_fastjson

# minor 4: a toolchain bump moves forward
tc_downgrade() {
  sedi 's#^go 1.22$#go 1.21#' go.mod
  sedi 's#golang:1.22.0-alpine3.20#golang:1.21.0-alpine3.20#' Dockerfile.logs
}
tc_alpine_downgrade() { sedi 's#golang:1.22.0-alpine3.20#golang:1.22.0-alpine3.9#' Dockerfile.logs; }
tc_alpine_upgrade_numeric() { sedi 's#golang:1.22.0-alpine3.20#golang:1.22.0-alpine3.21#' Dockerfile.logs; }
run_case "minor 4: a Go downgrade is not toolchain-only (Rule 1 applies)" fail "makes no real content change" tc_downgrade
run_case "minor 4: an alpine release going back (3.20 to 3.9) is not toolchain-only" fail "makes no real content change" tc_alpine_downgrade
run_case "minor 4: an alpine release going forward (3.20 to 3.21) is toolchain-only" ok "PR kind: \`toolchain-only\`" tc_alpine_upgrade_numeric

# minor 1: a path with a line break raised no route or feature signal
newline_path_route() {
  printf 'package x\n\nfunc registerZZ(mux Mux) { mux.HandleFunc("/lakehouse/api/v1/zz", nil) }\n' > "internal/x/zz"$'\n'"route.go"
}
run_case "minor 1: a path with a line break is a hard failure of its own row" fail "| Changed paths | FAIL |" newline_path_route
run_case "minor 1: ... with the offending path shown inert" fail "internal/x/zz?route.go" newline_path_route
cr_path() { printf 'package x\n' > "internal/x/a"$'\r'"b.go"; }
run_case "minor 1: a path with a CR is refused too" fail "| Changed paths | FAIL |" cr_path

# minor 5: load-time declarations are a multiset per package (tests/parity/vars.go holds a called var and an init at the base)
loadtime_move_to_new_file() {
  registry_row_change
  printf 'package parity\n' > tests/parity/vars.go
  printf 'package parity\n\nvar cfg = loadCfg()\n\nfunc init() { warm() }\n' > tests/parity/setup_moved.go
}
loadtime_rename_file() { registry_row_change; git mv tests/parity/vars.go tests/parity/renamed_vars.go; }
loadtime_move_and_change() {
  registry_row_change
  printf 'package parity\n' > tests/parity/vars.go
  printf 'package parity\n\nvar cfg = loadCfg()\n\nfunc init() { os.Exit(0) }\n' > tests/parity/setup_moved.go
}
loadtime_copy_added() { registry_row_change; printf 'package parity\n\nvar cfg = loadCfg()\n' > tests/parity/setup_copy.go; }
run_case "minor 5: a load-time var and init() moved to a new file of the package are listed, not failed" ok "tests/parity/setup_moved.go: a new file in the package of a lock declares a package-level var" loadtime_move_to_new_file
run_case "minor 5: renaming the file that holds them is listed, not failed" ok "tests/parity/renamed_vars.go: a new file in the package of a lock declares a package-level var" loadtime_rename_file
run_case "minor 5: a move that also changes the init() body is still an addition (Rule 3)" fail "tests/parity/setup_moved.go: adds 1 new init() function(s) to a package that holds a registry lock" loadtime_move_and_change
run_case "minor 5: one more copy of an existing called var in a new file is an addition (Rule 3)" fail "tests/parity/setup_copy.go: adds package-level var whose initializer calls a function: cfg = loadCfg()" loadtime_copy_added

echo
echo "== the third review of the gate: symlinks, alias swaps, packages a lock imports, min-pass =="
# MAJOR-1: `go test` compiles a symlinked .go file, and no lock rule can read a link
link_in_parity() { ln -s ../../README.md tests/parity/zz_link.go; }
link_in_product_lock_pkg() { ln -s ../../README.md internal/x/zz_link.go; }
link_replaces_lock_file() { rm tests/parity/helpers.go; ln -s ../../README.md tests/parity/helpers.go; }
link_test_go() { ln -s ../../README.md tests/parity/zz_link_test.go; }
link_to_dir() { ln -s ../../docs tests/parity/zzdir; }
link_docs_only() { ln -s ../README.md docs/zz_link.md; }
link_go_in_docs() { ln -s ../README.md docs/zz.go; }
link_replaces_runtime_config() { mkdir -p deployment/docker; printf 'x: 1\n' > deployment/docker/docker-compose-e2e.yml; rebase_here; rm deployment/docker/docker-compose-e2e.yml; ln -s ../../README.md deployment/docker/docker-compose-e2e.yml; }
run_case "s_newlink_parity: a new .go symlink in a lock package is a Rule 3 failure" fail "tests/parity/zz_link.go: a symbolic link added" link_in_parity
run_case "s_newlink_product_lockpkg: ... in a product lock package too" fail "internal/x/zz_link.go: a symbolic link added: the go tool compiles a linked .go file" link_in_product_lock_pkg
run_case "s_replace_lockfile: a symlink replacing an existing lock-package file is a failure, not a deletion" fail "tests/parity/helpers.go: a regular file replaced by a symbolic link" link_replaces_lock_file
run_case "s_symlinked_test: a symlinked _test.go is a Rule 3 failure, not a tool error" fail "| Rule 3: parity fixes ship locks, locks are not weakened | FAIL |" link_test_go
run_case "s_symlink_dir: a symlink to a directory under a lock dir is refused" fail "tests/parity/zzdir: a symbolic link added in a package that holds a registry lock" link_to_dir
run_case "s_symlink_docs_only: a docs-only symlink outside every lock dir stays a pass" ok "registry-touch check OK" link_docs_only
run_case "s_symlink_go_anywhere: a .go symlink anywhere is refused" fail "docs/zz.go: a symbolic link added" link_go_in_docs
run_case "s_symlink_runtime_config: the lock suites' compose file replaced by a symlink is refused" fail "deployment/docker/docker-compose-e2e.yml" link_replaces_runtime_config

# MAJOR-2: a moved load-time declaration keeps its meaning (imports behind its qualifiers, literals, constraints)
alias_base() { printf 'package parity\n\nimport schema "example.com/real/schema"\n\nvar fixed = schema.Estimate(1)\n' > tests/parity/est.go; rebase_here; }
alias_pure_move() { alias_base; printf 'package parity\n' > tests/parity/est.go; printf 'package parity\n\nimport schema "example.com/real/schema"\n\nvar fixed = schema.Estimate(1)\n' > tests/parity/est_moved.go; }
alias_swap() { alias_base; printf 'package parity\n' > tests/parity/est.go; printf 'package parity\n\nimport schema "example.com/evil/schema"\n\nvar fixed = schema.Estimate(1)\n' > tests/parity/est_moved.go; }
alias_noalias_clause() {
  printf 'package parity\n\nimport "example.com/real/schema"\n\nvar fixed = schema.Estimate(1)\n' > tests/parity/est.go; rebase_here
  mkdir -p tests/evilpkg; printf 'package schema\n\nfunc Estimate(int) int { return 0 }\n' > tests/evilpkg/e.go
  printf 'package parity\n' > tests/parity/est.go; printf 'package parity\n\nimport "x/tests/evilpkg"\n\nvar fixed = schema.Estimate(1)\n' > tests/parity/est_moved.go
}
literal_whitespace() { printf 'package parity\n\nvar s = strings.ToUpper("a b")\n' > tests/parity/lit.go; rebase_here; printf 'package parity\n' > tests/parity/lit.go; printf 'package parity\n\nvar s = strings.ToUpper("a  b")\n' > tests/parity/lit_moved.go; }
constraint_change() { alias_base; printf 'package parity\n' > tests/parity/est.go; printf '//go:build ignore\n\npackage parity\n\nimport schema "example.com/real/schema"\n\nvar fixed = schema.Estimate(1)\n' > tests/parity/est_moved.go; }
move_and_change() { alias_base; printf 'package parity\n' > tests/parity/est.go; printf 'package parity\n\nimport schema "example.com/real/schema"\n\nvar fixed = schema.Estimate(2)\n' > tests/parity/est_moved.go; }
run_case "m_pure_move: a pure move with identical imports is listed, not failed" ok "tests/parity/est_moved.go" alias_pure_move
run_case "m_alias_swap: the same declaration under an alias for another package is an addition" fail "tests/parity/est_moved.go: adds package-level var whose initializer calls a function: fixed = schema.Estimate(1)" alias_swap
run_case "m_noalias_clause: an unaliased import whose package clause is chosen by the author is an addition" fail "tests/parity/est_moved.go: adds package-level var" alias_noalias_clause
alias_same_clause() {
  mkdir -p tests/realpkg; printf 'package schema\n\nfunc Estimate(int) int { return 1 }\n' > tests/realpkg/r.go
  printf 'package parity\n\nimport "x/tests/realpkg"\n\nvar fixed = schema.Estimate(1)\n' > tests/parity/est.go; rebase_here
  mkdir -p tests/evilpkg; printf 'package schema\n\nfunc Estimate(int) int { return 0 }\n' > tests/evilpkg/e.go
  printf 'package parity\n' > tests/parity/est.go; printf 'package parity\n\nimport "x/tests/evilpkg"\n\nvar fixed = schema.Estimate(1)\n' > tests/parity/est_moved.go
}
run_case "m_same_clause: two repository packages that both declare package schema are different packages" fail "tests/parity/est_moved.go: adds package-level var" alias_same_clause
run_case "m_literal_whitespace: white space inside a string literal is not collapsed" fail "tests/parity/lit_moved.go: adds package-level var" literal_whitespace
run_case "m_constraint_change: a move into a file with a build constraint is an addition" fail "tests/parity/est_moved.go: adds package-level var" constraint_change
run_case "m_move_changed: a move that also changes the initializer is an addition" fail "fixed = schema.Estimate(2)" move_and_change

# MAJOR-3: packages a lock package imports (tests/ingestmatrix is imported by tests/e2e and is neither lock nor product)
adj_base() {
  mkdir -p tests/ingestmatrix
  printf 'package ingestmatrix\n\nfunc A() {}\n' > tests/ingestmatrix/a.go
  printf 'package parity\n\nimport (\n\t"testing"\n\n\t_ "x/tests/ingestmatrix"\n)\n\nfunc TestOldParity(t *testing.T) {}\n' > tests/parity/parity_test.go
  rebase_here
}
adj_init_existing() { adj_base; printf '\nfunc init() { os.Exit(0) }\n' >> tests/ingestmatrix/a.go; }
adj_newfile() { adj_base; printf 'package ingestmatrix\n\nfunc N() {}\n' > tests/ingestmatrix/new.go; }
adj_newfile_init() { adj_base; printf 'package ingestmatrix\n\nfunc init() {}\n' > tests/ingestmatrix/new.go; }
adj_cgo() { adj_base; printf 'package ingestmatrix\n\n/*\nvoid f(void) {}\n*/\nimport "C"\n' > tests/ingestmatrix/c.go; }
adj_native() { adj_base; printf 'int x;\n' > tests/ingestmatrix/c.c; }
adj_hook() {
  mkdir -p tests/hook; printf 'package hook\n\nimport "os"\n\nfunc init() { os.Exit(0) }\n' > tests/hook/hook.go
  printf 'package parity\n\nimport _ "x/tests/hook"\n' > tests/parity/zz_hook_test.go
}
run_case "x_ingestmatrix_init: an init() added to a package a lock test imports is a Rule 3 failure" fail "tests/ingestmatrix/a.go: adds 1 new init() function(s) to a package that a lock package imports" adj_init_existing
run_case "x_ingestmatrix_newfile: a new file there is listed (inline warning), not failed" ok "tests/ingestmatrix/new.go: a new Go file in a test-support package that a lock package imports" adj_newfile
run_case "x_ingestmatrix_newfile_init: ... with an init() it fails" fail "tests/ingestmatrix/new.go: adds 1 new init() function(s)" adj_newfile_init
run_case "x_ingestmatrix_cgo: import \"C\" there is a Rule 3 failure" fail 'tests/ingestmatrix/c.go: imports "C" (cgo) in a package that a lock package imports' adj_cgo
run_case "x_ingestmatrix_native: a native source file there is a Rule 3 failure" fail "tests/ingestmatrix/c.c: a native source file" adj_native
run_case "x_hook_package: a new package a lock test blank-imports, with an init(), fails from its first commit" fail "tests/hook/hook.go: adds 1 new init() function(s)" adj_hook

# MINOR 3: the expected number of passing parity tests may only grow
minpass_base() { printf '# min-pass: 341\n# allowlist\nTestFixParity  # B1: x\nTestParity_B  # B2: y\n' > tests/parity/known_failures.txt; rebase_here; }
minpass_lowered() { minpass_base; sedi 's/min-pass: 341/min-pass: 340/' tests/parity/known_failures.txt; }
minpass_raised() { minpass_base; sedi 's/min-pass: 341/min-pass: 342/' tests/parity/known_failures.txt; }
minpass_removed() { minpass_base; sedi '/min-pass/d' tests/parity/known_failures.txt; }
minpass_unicode() { minpass_base; sedi 's/# min-pass: 341/#  min-pass: ٣/' tests/parity/known_failures.txt; }
run_case "min-pass: lowering it is a Rule 3 failure" fail "min-pass:\` lowered from 341 to 340" minpass_lowered
run_case "min-pass: raising it is free" ok "registry-touch check OK" minpass_raised
run_case "min-pass: removing the directive is a Rule 3 failure" fail "the \`# min-pass: 341\` directive was removed" minpass_removed
run_case "min-pass: an Arabic-Indic value is unreadable and refused" fail "unreadable min-pass value" minpass_unicode

run_unreleased_bullet_case

echo
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
