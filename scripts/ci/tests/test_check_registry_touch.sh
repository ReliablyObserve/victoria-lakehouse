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
[[ -f "$CHECKER" ]] || { echo "checker not found at $CHECKER" >&2; exit 2; }

pass=0
fail=0

# confgen cannot run inside a fixture repo (no Go module), and the fixtures
# deliberately have no tests/conformance tree; skip that part of the check.
export SKIP_CONFGEN_CHECK=1

# new_repo creates a repo with an initial commit and echoes its path.
new_repo() {
  local dir
  dir="$(mktemp -d)"
  (
    cd "$dir" || exit 1
    git init -q -b main
    git config user.email t@example.com
    git config user.name t
    mkdir -p internal/x cmd/lakehouse-logs tests/conformance/registry/rows/lh tests/conformance/registry/features
    printf '# Changelog\n\n## [1.0.0]\n\n### Added\n\n- **Existing thing.** text.\n' > CHANGELOG.md
    printf 'package x\n\nfunc F() {}\n' > internal/x/x.go
    printf -- '- id: lh.existing.row\n  title: existing\n' > tests/conformance/registry/rows/lh/endpoints.yaml
    printf -- '- id: lh.feature.storage.existing\n  title: existing\n' > tests/conformance/registry/features/storage.yaml
    git add -A
    git commit -q -m base
    git branch -q base
  ) >/dev/null 2>&1
  echo "$dir"
}

# run_case <name> <want: ok|fail> <expected message fragment or ""> -- commands...
run_case() {
  local name="$1" want="$2" fragment="$3"
  shift 3
  local dir out rc
  dir="$(new_repo)"
  (
    cd "$dir" || exit 1
    "$@"
    git add -A
    git commit -q -m change
  ) >/dev/null 2>&1
  out="$(cd "$dir" && bash "$CHECKER" base 2>&1)"
  rc=$?
  rm -rf "$dir"

  local ok=1
  if [[ "$want" == ok && $rc -ne 0 ]]; then ok=0; fi
  if [[ "$want" == fail && $rc -eq 0 ]]; then ok=0; fi
  if [[ -n "$fragment" ]] && ! grep -qF -- "$fragment" <<<"$out"; then ok=0; fi

  if [[ $ok -eq 1 ]]; then
    echo "ok   - $name"
    pass=$((pass + 1))
  else
    echo "FAIL - $name (rc=$rc, want $want)"
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
add_flag() {
  printf 'package main\n\nvar f = flag.String("lakehouse.new.knob", "", "doc")\n' > cmd/lakehouse-logs/flags.go
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
  printf 'package x\n\nfunc TestF() { mux.HandleFunc("/x", nil) }\n' > internal/x/x_test.go
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
echo "== the pre-existing row rule still holds =="
run_case "route change without rows fails" fail \
  "but not tests/conformance/registry/rows/" add_route
run_case "route change with rows and catalog passes" ok "" \
  bash -c 'printf "package main\n\nfunc main() { mux.HandleFunc(\"/x\", nil) }\n" > cmd/lakehouse-logs/main.go; printf -- "  notes: touched\n" >> tests/conformance/registry/rows/lh/endpoints.yaml; printf -- "- id: lh.feature.storage.new\n  title: new\n" >> tests/conformance/registry/features/storage.yaml'

echo
echo "== the generated documents must be current on a feature PR =="

# The cases above set SKIP_CONFGEN_CHECK=1, because a synthetic fixture repo
# has no Go module to run confgen in. This last case exercises the part they
# cannot: the checker's own `confgen -check` sub-step, against a real clone of
# this repository, in both directions — stale documents must fail, and the
# same tree must pass once regenerated.
run_confgen_case() {
  local name="the checker fails a feature PR with stale generated docs, and passes once regenerated"
  local repo_root
  repo_root="$(cd "$SCRIPT_DIR/../../.." && pwd)"

  if ! command -v go >/dev/null 2>&1; then
    echo "skip - $name (no go toolchain)"
    return
  fi
  if [[ ! -f "$repo_root/deps/VictoriaLogs/go.mod" ]]; then
    echo "skip - $name (upstream deps missing; run: make deps-logs deps-traces deps-vt)"
    return
  fi

  local tmp
  tmp="$(mktemp -d)"
  if ! git clone --quiet --shared "$repo_root" "$tmp/repo" 2>/dev/null; then
    rm -rf "$tmp"
    echo "skip - $name (cannot clone the repository)"
    return
  fi

  # confgen extracts the upstream inventory from the vendored sources, which
  # are .gitignored and therefore absent from the clone: link them in.
  ln -s "$repo_root/deps" "$tmp/repo/deps"
  ln -s "$repo_root/lakehouse-traces/deps" "$tmp/repo/lakehouse-traces/deps"

  local out rc stale_out stale_rc
  (
    cd "$tmp/repo" || exit 1
    git config user.email t@example.com
    git config user.name t
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
  since: unreleased
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
echo "$pass passed, $fail failed"
[[ $fail -eq 0 ]]
