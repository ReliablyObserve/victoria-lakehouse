#!/usr/bin/env bash
# Self-test for scripts/ci/upstream_sync_probe.sh against a FAKE upstream: two
# local git repositories stand in for VictoriaLogs and VictoriaTraces (real
# tags, real commits, real patches applied with real `git apply`), a fake
# repository carries the pin sites and the Makefile deps recipe, and `gh` and
# `go` are shimmed on PATH so the releases API and the Go toolchain are
# scripted rather than reached.
#
# What is real here, and therefore actually tested: pin parsing off the
# Makefile, the numeric version comparison, the derivation of VL_COMMIT_TRACES
# out of the candidate VictoriaTraces' own go.mod, rewriting every pin site,
# executing the Makefile's own deps recipe through `make -n`, `git apply
# --check` per patch, the branch and its commit, the Markdown matrix, the
# key=value facts the workflow consumes, and the exit code for each verdict.
#
# Cases:
#   1  no newer release                     -> exit 0,  "up to date"
#   2  an older "latest" is not newer       -> exit 0   (numeric, not textual, compare)
#   3  clean bump                           -> exit 0,  matrix + branch + commit + pins
#   4  a patch no longer applies            -> exit 10, failing hunk in the matrix
#   5  a module no longer builds            -> exit 20, first build error in the matrix
#   5b go mod tidy cannot resolve           -> exit 20, tidy error, build not attempted
#   6  drift needs registry rows            -> exit 30, failing gate named
#   6b a reported-version gate fails        -> exit 30, gate named
#   6c a reported-version gate vanished     -> exit 30, never a silent pass
#   7  --no-pr                              -> exit 0,  open_pr=false
#   8  a deps step fails; numeric commit    -> exit 1,  step named; YAML pin quoted
#   9  bad invocation                       -> exit 2
#  10  version comparison table            (the probe's own version_gt, extracted)
#
# The missing-token path is a workflow-level behaviour and cannot be exercised
# here: the probe itself never opens a pull request. It reports open_pr through
# --env-out (case 7), and .github/workflows/upstream-check.yaml turns a missing
# UPSTREAM_SYNC_TOKEN into a step-summary line and a non-zero job.
#
# Usage: scripts/ci/tests/upstream_sync_probe_test.sh
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1

PROBE="$(pwd)/scripts/ci/upstream_sync_probe.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0 fail=0

check_rc() {
	local desc="$1" actual="$2" expected="$3"
	if [[ "$actual" == "$expected" ]]; then
		pass=$((pass + 1))
		return
	fi
	fail=$((fail + 1))
	printf 'FAIL: %s\n  expected exit: %s\n  actual exit:   %s\n  output:\n%s\n' \
		"$desc" "$expected" "$actual" "${out:-}" >&2
}

check_contains() {
	local desc="$1" haystack="$2" needle="$3"
	if [[ "$haystack" == *"$needle"* ]]; then
		pass=$((pass + 1))
		return
	fi
	fail=$((fail + 1))
	printf 'FAIL: %s\n  expected substring: %s\n  actual:\n%s\n' "$desc" "$needle" "$haystack" >&2
}

check_not_contains() {
	local desc="$1" haystack="$2" needle="$3"
	if [[ "$haystack" != *"$needle"* ]]; then
		pass=$((pass + 1))
		return
	fi
	fail=$((fail + 1))
	printf 'FAIL: %s\n  unexpected substring: %s\n  actual:\n%s\n' "$desc" "$needle" "$haystack" >&2
}

# Every git invocation in this test is hermetic: a fixed identity, and signing
# turned off, so a developer machine that signs commits and annotates tags by
# configuration does not change what the fixture looks like.
git_q() {
	git -c user.name=probe-test -c user.email=probe-test@example.invalid \
		-c commit.gpgsign=false -c tag.gpgSign=false -c tag.forceSignAnnotated=false \
		-c init.defaultBranch=main "$@"
}

# ---------------------------------------------------------------------------
# Fake upstream: VictoriaLogs with three release tags, VictoriaTraces with two.
# main.go is identical at v1.50.0/v1.51.0/v1.52.0 (so one patch applies to all
# three) and changed at v1.53.0 (so the same patch cannot apply there).
# ---------------------------------------------------------------------------

VL_UP="$TMP/upstream/VictoriaLogs"
VT_UP="$TMP/upstream/VictoriaTraces"
mkdir -p "$VL_UP/app/vlstorage" "$VL_UP/lib/logstorage" "$VT_UP/app/vtstorage"

cat > "$VL_UP/app/vlstorage/main.go" <<'EOF'
package vlstorage

// dispatch routes a query to the local storage.
func dispatch(q string) string {
	return local(q)
}

func local(q string) string { return q }
EOF
cat > "$VL_UP/app/vlstorage/external.go" <<'EOF'
package vlstorage

// placeholder replaced by the Lakehouse overlay
EOF
cat > "$VL_UP/lib/logstorage/external_query.go" <<'EOF'
package logstorage

// placeholder replaced by the Lakehouse overlay
EOF
printf 'module github.com/VictoriaMetrics/VictoriaLogs\n\ngo 1.26\n' > "$VL_UP/go.mod"

(
	cd "$VL_UP" || exit 1
	git_q init -q .
	git_q add -A
	git_q commit -qm "VictoriaLogs v1.50.0"
	git_q tag -m "VictoriaLogs v1.50.0" v1.50.0
	printf 'package logstorage\n\n// added in v1.51.0\n' > lib/logstorage/coalesce.go
	git_q add -A
	git_q commit -qm "VictoriaLogs v1.51.0"
	git_q tag -m "VictoriaLogs v1.51.0" v1.51.0
	printf 'package logstorage\n\n// added in v1.52.0\n' > lib/logstorage/json_array_concat.go
	git_q add -A
	git_q commit -qm "VictoriaLogs v1.52.0"
	git_q tag -m "VictoriaLogs v1.52.0" v1.52.0
	# v1.53.0 moves the very line the dispatch patch anchors on.
	cat > app/vlstorage/main.go <<'EOF'
package vlstorage

// dispatch routes a query to the local storage or a remote peer.
func dispatch(q string, peer string) string {
	return local(q)
}

func local(q string) string { return q }
EOF
	git_q add -A
	git_q commit -qm "VictoriaLogs v1.53.0"
	git_q tag -m "VictoriaLogs v1.53.0" v1.53.0
)

VL_COMMIT_151="$(git -C "$VL_UP" rev-parse --short=12 v1.51.0)"

cat > "$VT_UP/app/vtstorage/main.go" <<'EOF'
package vtstorage

func dispatch(q string) string { return q }
EOF
cat > "$VT_UP/app/vtstorage/external.go" <<'EOF'
package vtstorage

// placeholder replaced by the Lakehouse overlay
EOF
(
	cd "$VT_UP" || exit 1
	git_q init -q .
	printf 'module github.com/VictoriaMetrics/VictoriaTraces\n\ngo 1.26\n\nrequire github.com/VictoriaMetrics/VictoriaLogs v1.121.1-0.20260101000000-%s\n' \
		"$(git -C "$VL_UP" rev-parse --short=12 v1.50.0)" > go.mod
	git_q add -A
	git_q commit -qm "VictoriaTraces v0.9.2"
	git_q tag -m "VictoriaTraces v0.9.2" v0.9.2
	# v0.11.0 is what makes the derived traces pin move to VictoriaLogs v1.51.0.
	printf 'module github.com/VictoriaMetrics/VictoriaTraces\n\ngo 1.26\n\nrequire github.com/VictoriaMetrics/VictoriaLogs v1.121.1-0.20260617051904-%s\n' \
		"$VL_COMMIT_151" > go.mod
	git_q add -A
	git_q commit -qm "VictoriaTraces v0.11.0"
	git_q tag -m "VictoriaTraces v0.11.0" v0.11.0
	# v0.12.0 names a VictoriaLogs commit whose 12-character prefix is all
	# digits — rare for a real hash, and exactly the value a YAML parser would
	# read as a number. It does not exist upstream, so the traces checkout fails.
	printf 'module github.com/VictoriaMetrics/VictoriaTraces\n\ngo 1.26\n\nrequire github.com/VictoriaMetrics/VictoriaLogs v1.121.1-0.20260901000000-012345678901\n' > go.mod
	git_q add -A
	git_q commit -qm "VictoriaTraces v0.12.0"
	git_q tag -m "VictoriaTraces v0.12.0" v0.12.0
)

# ---------------------------------------------------------------------------
# Fake repository: the pin sites, the patches, the Makefile deps recipe.
# ---------------------------------------------------------------------------

REPO="$TMP/repo"
mkdir -p "$REPO"/{patches/vl-logs,patches/vl-traces,patches/vt-traces,deployment/docker,.github/workflows,lakehouse-traces,tests/conformance/cmd/confgen}

# The patches are generated against the fake upstream, so `git apply` decides
# whether they apply — the test never asserts on a hand-written diff.
(
	cd "$VL_UP" || exit 1
	git_q checkout -q v1.52.0
	cat > app/vlstorage/main.go <<'EOF'
package vlstorage

// dispatch routes a query to the local storage.
func dispatch(q string) string {
	if External != nil {
		return External(q)
	}
	return local(q)
}

func local(q string) string { return q }
EOF
	git diff > "$REPO/patches/vl-logs/vlstorage-dispatch.patch"
	git_q checkout -q -- app/vlstorage/main.go
	git_q checkout -q main 2> /dev/null || git_q checkout -q master 2> /dev/null || true
)
cp "$REPO/patches/vl-logs/vlstorage-dispatch.patch" "$REPO/patches/vl-traces/vlstorage-dispatch.patch"
(
	cd "$VT_UP" || exit 1
	git_q checkout -q v0.11.0
	cat > app/vtstorage/main.go <<'EOF'
package vtstorage

func dispatch(q string) string {
	if External != nil {
		return External(q)
	}
	return q
}
EOF
	git diff > "$REPO/patches/vt-traces/vtstorage-dispatch.patch"
	git_q checkout -q -- app/vtstorage/main.go
	git_q checkout -q main 2> /dev/null || git_q checkout -q master 2> /dev/null || true
)

cat > "$REPO/patches/vl-logs/external.go.src" <<'EOF'
package vlstorage

var External func(string) string
EOF
cp "$REPO/patches/vl-logs/external.go.src" "$REPO/patches/vl-traces/external.go.src"
cat > "$REPO/patches/vt-traces/external.go.src" <<'EOF'
package vtstorage

var External func(string) string
EOF

cat > "$REPO/Makefile" <<EOF
export GOWORK=off

VL_VERSION_LOGS := v1.50.0 # the logs binary's VictoriaLogs release
VL_COMMIT_TRACES := $(git -C "$VL_UP" rev-parse --short=12 v1.50.0)
VL_REPO := $VL_UP
VL_DIR_LOGS := deps/VictoriaLogs
VL_DIR_TRACES := lakehouse-traces/deps/VictoriaLogs

VT_VERSION := v0.9.2
VT_REPO := $VT_UP
VT_DIR := lakehouse-traces/deps/VictoriaTraces

.PHONY: deps-logs deps-traces deps-vt

deps-logs: \$(VL_DIR_LOGS)/go.mod

\$(VL_DIR_LOGS)/go.mod:
	@mkdir -p deps
	git clone --quiet --branch \$(VL_VERSION_LOGS) \$(VL_REPO) \$(VL_DIR_LOGS)
	cp patches/vl-logs/external.go.src \$(VL_DIR_LOGS)/app/vlstorage/external.go
	cd \$(VL_DIR_LOGS) && git apply ../../patches/vl-logs/vlstorage-dispatch.patch

deps-traces: \$(VL_DIR_TRACES)/go.mod

\$(VL_DIR_TRACES)/go.mod:
	@mkdir -p lakehouse-traces/deps
	git clone --quiet \$(VL_REPO) \$(VL_DIR_TRACES)
	cd \$(VL_DIR_TRACES) && git checkout --quiet \$(VL_COMMIT_TRACES)
	cp patches/vl-traces/external.go.src \$(VL_DIR_TRACES)/app/vlstorage/external.go
	cd \$(VL_DIR_TRACES) && git apply ../../../patches/vl-traces/vlstorage-dispatch.patch

deps-vt: \$(VT_DIR)/go.mod

\$(VT_DIR)/go.mod:
	@mkdir -p lakehouse-traces/deps
	git clone --quiet --branch \$(VT_VERSION) \$(VT_REPO) \$(VT_DIR)
	cp patches/vt-traces/external.go.src \$(VT_DIR)/app/vtstorage/external.go
	cd \$(VT_DIR) && git apply ../../../patches/vt-traces/vtstorage-dispatch.patch
	cd \$(VT_DIR) && go mod edit -replace github.com/VictoriaMetrics/VictoriaLogs=../VictoriaLogs
EOF

cat > "$REPO/Dockerfile.logs" <<'EOF'
ARG VL_VERSION=v1.50.0
ARG VL_COMMIT=deadbeefcafe
ARG VT_VERSION=v0.9.2
FROM scratch
EOF

cat > "$REPO/deployment/docker/docker-compose-e2e.yml" <<'EOF'
services:
  victoria-logs:
    image: victoriametrics/victoria-logs:v1.50.0
  victoria-traces:
    image: victoriametrics/victoria-traces:v0.9.2
EOF

cat > "$REPO/.github/workflows/ci.yaml" <<'EOF'
name: CI
env:
  VL_VERSION_LOGS: v1.50.0
  VL_COMMIT_TRACES: deadbeefcafe
  VT_VERSION: v0.9.2
jobs:
  build:
    steps:
      - run: echo "${{ env.VL_VERSION_LOGS }}"
EOF

cat > "$REPO/.github/workflows/release.yaml" <<'EOF'
name: Release
env:
  VL_VERSION_LOGS: "v1.50.0" # quoted on purpose
  VL_COMMIT_TRACES: "deadbeefcafe"
  VT_VERSION: 'v0.9.2'
jobs:
  build:
    env:
      VT_VERSION: ${{ inputs.vt_version }}
EOF

cat > "$REPO/tests/conformance/inventory.generated.yaml" <<EOF
# GENERATED by \`make conformance-gen\` from the vendored upstream sources — do not edit.
vl_version: v1.50.0
vt_version: v0.9.2
vl_commit_traces: $(git -C "$VL_UP" rev-parse --short=12 v1.50.0)
protocol:
    vl:
        select: v4
        delete: v1
    vt:
        select: v4
        delete: v1
items:
    - kind: route
      surface: vl
      name: /select/logsql/query
    - kind: pipe
      surface: vl
      name: unpack_json
    - kind: route
      surface: vt
      name: /select/jaeger/api/traces
EOF
printf 'package main\n' > "$REPO/tests/conformance/cmd/confgen/main.go"
cat > "$REPO/go.mod" <<'EOF'
module github.com/ReliablyObserve/victoria-lakehouse

go 1.26

require (
	github.com/VictoriaMetrics/VictoriaLogs v1.50.0
	github.com/VictoriaMetrics/VictoriaMetrics v1.140.1-0.20260414051809-8a20ccf21db7
)

replace github.com/VictoriaMetrics/VictoriaLogs => ./deps/VictoriaLogs
EOF
cat > "$REPO/lakehouse-traces/go.mod" <<'EOF'
module github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces

go 1.26
EOF
mkdir -p "$REPO/cmd/lakehouse-logs"
printf 'package main\n\n// vlCompat is reported on /lakehouse/info.\nconst vlCompat = "1.50.0"\n' > "$REPO/cmd/lakehouse-logs/main.go"
printf 'package main\n\n// vtCompat is reported on /lakehouse/info.\nconst vtCompat = "0.9.2"\n' > "$REPO/lakehouse-traces/main.go"
cat > "$REPO/.gitignore" <<'EOF'
/deps/
/lakehouse-traces/deps/
EOF

(
	cd "$REPO" || exit 1
	git_q init -q .
	git_q add -A
	git_q commit -qm "fake repository"
)

# ---------------------------------------------------------------------------
# Shims: the releases API and the Go toolchain.
# ---------------------------------------------------------------------------

BIN="$TMP/bin"
mkdir -p "$BIN"

cat > "$BIN/gh" <<'EOF'
#!/usr/bin/env bash
# gh api repos/<owner>/<name>/releases/latest --jq .tag_name
case "$*" in
*VictoriaLogs*) printf '%s\n' "${FAKE_LATEST_VL:-v1.50.0}" ;;
*VictoriaTraces*) printf '%s\n' "${FAKE_LATEST_VT:-v0.9.2}" ;;
*) exit 1 ;;
esac
EOF

cat > "$BIN/go" <<'EOF'
#!/usr/bin/env bash
# Scripted Go toolchain. FAKE_GO_FAIL selects which step fails; FAKE_GO_LOG
# records every `go mod` call with the module directory it ran in.
case "${1:-}" in
build | vet)
	if [[ "${FAKE_GO_FAIL:-}" == "build" ]]; then
		printf '%s\n' "internal/selectapi/dispatch.go:118:31: undefined: logstorage.ExternalQuery" >&2
		exit 1
	fi
	exit 0
	;;
mod)
	dir="$(basename "$PWD")"
	[[ -f go.mod && "$(sed -n 's/^module //p' go.mod)" != */lakehouse-traces ]] && dir=root
	[[ -n "${FAKE_GO_LOG:-}" ]] && printf '%s: %s\n' "$dir" "$*" >> "$FAKE_GO_LOG"
	if [[ "${2:-}" == "tidy" ]]; then
		if [[ "${FAKE_GO_FAIL:-}" == "tidy" ]]; then
			printf '%s\n' "go: github.com/VictoriaMetrics/VictoriaLogs@v1.52.0 requires github.com/example/gone@v9.9.9: reading github.com/example/gone/go.mod at revision v9.9.9: unknown revision v9.9.9" >&2
			exit 1
		fi
		# Tidy moves a shared library, as a real bump does.
		if [[ -f go.mod ]]; then
			sed 's|VictoriaMetrics/VictoriaMetrics v1.140.1-0.20260414051809-8a20ccf21db7|VictoriaMetrics/VictoriaMetrics v1.149.1-0.20260811205936-d4a40004ef28|' go.mod > go.mod.tmp
			mv go.mod.tmp go.mod
		fi
	fi
	exit 0
	;;
run)
	if [[ "$*" == *-write* ]]; then
		inv=tests/conformance/inventory.generated.yaml
		[[ -f "$inv" ]] || exit 2
		sed -e "s|^vl_version: .*|vl_version: ${FAKE_VL:-v1.52.0}|" \
			-e "s|^vt_version: .*|vt_version: ${FAKE_VT:-v0.11.0}|" \
			-e "s|^        select: v4|        select: v5|" "$inv" > "$inv.tmp"
		mv "$inv.tmp" "$inv"
		if ! grep -q 'new_thing' "$inv"; then
			printf '    - kind: route\n      surface: vl\n      name: /select/logsql/new_thing\n' >> "$inv"
		fi
		printf 'wrote %s\n' "$inv"
	fi
	exit 0
	;;
test)
	if [[ "$*" == *Compat* ]]; then
		gate="$(printf '%s' "$*" | sed -n 's/.*\^\(Test[A-Za-z]*\).*/\1/p')"
		case "${FAKE_GO_FAIL:-}" in
		compat)
			printf '=== RUN   %s\n    compat_test.go:31: vlCompat = "1.50.0" but go.mod requires VictoriaLogs v1.52.0.\n--- FAIL: %s (0.00s)\nFAIL\n' "$gate" "$gate"
			exit 1
			;;
		nocompat)
			printf 'testing: warning: no tests to run\nPASS\nok\tgithub.com/ReliablyObserve/victoria-lakehouse/cmd/lakehouse-logs\t0.01s [no tests to run]\n'
			exit 0
			;;
		esac
		printf '=== RUN   %s\n--- PASS: %s (0.00s)\nPASS\n' "$gate" "$gate"
		exit 0
	fi
	printf '=== RUN   TestDrift_RealRegistry\n'
	if [[ "${FAKE_GO_FAIL:-}" == "drift" ]]; then
		printf '    drift_test.go:293: registry drift:\n'
		printf '        upstream route "/select/logsql/new_thing" (vl) has no registry row — add to tests/conformance/registry/rows/\n'
		printf -- '--- FAIL: TestDrift_RealRegistry (0.04s)\n'
		printf 'FAIL\tgithub.com/ReliablyObserve/victoria-lakehouse/tests/conformance\t0.3s\n'
		exit 1
	fi
	printf '    drift_test.go:297: Summary: 0 unmapped, 0 stale, 0 pending-bump, 155 flag warnings\n'
	printf -- '--- PASS: TestDrift_RealRegistry (0.04s)\n'
	printf 'ok\tgithub.com/ReliablyObserve/victoria-lakehouse/tests/conformance\t0.3s\n'
	exit 0
	;;
esac
exit 0
EOF
chmod +x "$BIN/gh" "$BIN/go"

# run <case> [probe args...] — runs the probe with the shims on PATH, keeping
# the work directory so the clone can be inspected afterwards.
run() {
	local name="$1"
	shift
	work="$TMP/work-$name"
	matrix="$TMP/matrix-$name.md"
	envfile="$TMP/env-$name"
	mkdir -p "$work"
	golog="$TMP/golog-$name"
	out="$(PATH="$BIN:$PATH" FAKE_GO_LOG="$golog" "$PROBE" --repo "$REPO" --workdir "$work" \
		--out "$matrix" --env-out "$envfile" "$@" 2>&1)"
	rc=$?
	mtx="$(cat "$matrix" 2> /dev/null)"
	envv="$(cat "$envfile" 2> /dev/null)"
	gomod="$(cat "$golog" 2> /dev/null)"
}

# --- 1. nothing newer upstream -----------------------------------------
FAKE_LATEST_VL=v1.50.0 FAKE_LATEST_VT=v0.9.2 run uptodate
check_rc "no newer release exits 0" "$rc" 0
check_contains "no newer release reports up to date" "$mtx" "up to date"
check_contains "no newer release sets the status" "$envv" "status=up-to-date"
check_contains "no newer release asks for no pull request" "$envv" "open_pr=false"

# --- 2. an older "latest" is not newer (numeric compare) ---------------
# v1.5.0 sorts AFTER v1.50.0 textually; only a numeric comparison gets this right.
FAKE_LATEST_VL=v1.5.0 FAKE_LATEST_VT=v0.9.2 run older
check_rc "an older latest release is not treated as newer" "$rc" 0
check_contains "an older latest release reports up to date" "$mtx" "up to date"

# --- 3. clean bump ------------------------------------------------------
FAKE_LATEST_VL=v1.52.0 FAKE_LATEST_VT=v0.11.0 run clean
check_rc "a clean bump exits 0" "$rc" 0
check_contains "a clean bump reports the status" "$envv" "status=clean"
check_contains "a clean bump names the branch" "$envv" "branch=upstream-sync/vl-v1.52.0-vt-v0.11.0"
check_contains "a clean bump derives the traces pin from VictoriaTraces' go.mod" \
	"$envv" "vl_commit_traces=$VL_COMMIT_151"
check_contains "a clean bump says a clean bump is possible" "$mtx" "a clean bump is possible"
check_contains "every patch is reported as applying" "$mtx" "| ✅ |"
# The manual-steps checklist mentions ❌ in prose, so the assertion is on a
# table cell, not on the character.
check_not_contains "no patch is reported as failing" "$mtx" "| ❌ |"
check_contains "the matrix links the VictoriaLogs comparison" "$mtx" "compare/v1.50.0...v1.52.0"
check_contains "the matrix lists the added upstream items" "$mtx" "/select/logsql/new_thing"
check_contains "the matrix reports the moved protocol version" "$mtx" "peer-protocol versions moved"
check_contains "the matrix carries the drift summary" "$mtx" "0 unmapped, 0 stale, 0 pending-bump"
check_contains "the matrix keeps the manual steps" "$mtx" "Regenerate every patch marked"

clone="$TMP/work-clean/repo"
check_contains "the root go.mod requirement moves to the VictoriaLogs release" \
	"$gomod" "root: mod edit -require=github.com/VictoriaMetrics/VictoriaLogs@v1.52.0"
check_contains "the traces go.mod takes VictoriaTraces' own VictoriaLogs requirement verbatim" \
	"$gomod" "lakehouse-traces: mod edit -require=github.com/VictoriaMetrics/VictoriaLogs@v1.121.1-0.20260617051904-$VL_COMMIT_151 -require=github.com/VictoriaMetrics/VictoriaTraces@v0.11.0"
check_contains "the root module is tidied" "$gomod" "root: mod tidy"
check_contains "the traces module is tidied" "$gomod" "lakehouse-traces: mod tidy"
check_contains "the logs binary reports the new VictoriaLogs release" \
	"$(cat "$clone/cmd/lakehouse-logs/main.go")" 'const vlCompat = "1.52.0"'
check_contains "the traces binary reports the new VictoriaTraces release" \
	"$(cat "$clone/lakehouse-traces/main.go")" 'const vtCompat = "0.11.0"'
check_contains "the matrix lists what tidy moved besides the pins" "$mtx" "VictoriaMetrics/VictoriaMetrics v1.149.1-0.20260811205936-d4a40004ef28"
check_contains "the reported-version gates ran" "$mtx" "Reported-version gate \`TestVTCompatMatchesGoMod\`: ✅"
check_contains "the tidied go.mod is committed" \
	"$(git -C "$clone" show --stat --name-only --pretty=format: HEAD)" "go.mod"
check_contains "the Makefile pin moves" "$(cat "$clone/Makefile")" "VL_VERSION_LOGS := v1.52.0"
check_contains "the derived Makefile pin moves" "$(cat "$clone/Makefile")" "VL_COMMIT_TRACES := $VL_COMMIT_151"
check_contains "the Dockerfile ARG default moves" "$(cat "$clone/Dockerfile.logs")" "ARG VL_VERSION=v1.52.0"
check_contains "the Dockerfile commit ARG moves" "$(cat "$clone/Dockerfile.logs")" "ARG VL_COMMIT=$VL_COMMIT_151"
check_contains "the Compose image tag moves" \
	"$(cat "$clone/deployment/docker/docker-compose-e2e.yml")" "victoria-logs:v1.52.0"
check_contains "the Compose traces image tag moves" \
	"$(cat "$clone/deployment/docker/docker-compose-e2e.yml")" "victoria-traces:v0.11.0"
check_contains "the workflow env pin moves" \
	"$(cat "$clone/.github/workflows/ci.yaml")" "VL_VERSION_LOGS: v1.52.0"
# shellcheck disable=SC2016 # the ${{ }} is the literal workflow text under test
check_contains "the Makefile pin keeps its trailing comment" "$(cat "$clone/Makefile")" \
	"VL_VERSION_LOGS := v1.52.0 # the logs binary's VictoriaLogs release"
check_contains "a quoted workflow pin keeps its quotes and comment" \
	"$(cat "$clone/.github/workflows/release.yaml")" 'VL_VERSION_LOGS: "v1.52.0" # quoted on purpose'
check_contains "a double-quoted workflow commit pin keeps its quotes" \
	"$(cat "$clone/.github/workflows/release.yaml")" "VL_COMMIT_TRACES: \"$VL_COMMIT_151\""
check_contains "a single-quoted workflow pin keeps its quotes" \
	"$(cat "$clone/.github/workflows/release.yaml")" "VT_VERSION: 'v0.11.0'"
# shellcheck disable=SC2016 # the ${{ }} is the literal workflow text under test
check_contains "a pin declared from an expression is left alone" \
	"$(cat "$clone/.github/workflows/release.yaml")" 'VT_VERSION: ${{ inputs.vt_version }}'
# shellcheck disable=SC2016 # the ${{ }} is the literal workflow text under test
check_contains "a workflow env reference is left alone" \
	"$(cat "$clone/.github/workflows/ci.yaml")" 'echo "${{ env.VL_VERSION_LOGS }}"'
check_contains "the branch is checked out in the clone" \
	"$(git -C "$clone" rev-parse --abbrev-ref HEAD)" "upstream-sync/vl-v1.52.0-vt-v0.11.0"
check_contains "the pin bump is committed" \
	"$(git -C "$clone" log -1 --pretty=%s)" "probe upstream sync VL v1.52.0 / VT v0.11.0"
check_not_contains "the commit carries no upstream checkout" \
	"$(git -C "$clone" show --stat --name-only --pretty=format: HEAD)" "deps/VictoriaLogs"
check_contains "the regenerated inventory is committed" \
	"$(git -C "$clone" show --stat --name-only --pretty=format: HEAD)" "inventory.generated.yaml"

# --- 4. a patch no longer applies --------------------------------------
FAKE_LATEST_VT=v0.11.0 run patchfail --vl v1.53.0
check_rc "a patch that no longer applies exits 10" "$rc" 10
check_contains "the patch failure sets the status" "$envv" "status=patches"
check_contains "the failing patch is named" "$mtx" "vlstorage-dispatch.patch"
check_contains "the failing patch is marked" "$mtx" "❌"
check_contains "the matrix carries the failing hunk" "$mtx" "patch failed: app/vlstorage/main.go"
check_contains "the matrix carries the context the patch searched for" "$mtx" "while searching for"
check_contains "the verdict names patch regeneration" "$mtx" "patches need regeneration"
check_contains "the build is reported as not run" "$mtx" "_Not run: while a patch does not apply"
check_contains "the traces-side patch still applies" "$mtx" "✅"

# --- 5. a module no longer builds --------------------------------------
FAKE_LATEST_VL=v1.52.0 FAKE_LATEST_VT=v0.11.0 FAKE_GO_FAIL=build run buildfail
check_rc "a build failure exits 20" "$rc" 20
check_contains "the build failure sets the status" "$envv" "status=build"
check_contains "the first build error is quoted" "$mtx" "undefined: logstorage.ExternalQuery"
check_contains "the verdict names the build" "$mtx" "the bump does not build"
check_contains "conformance is reported as not run" "$mtx" "_Not run: the tree does not build"

# --- 5b. tidy cannot resolve the new trees' requirements ----------------
FAKE_LATEST_VL=v1.52.0 FAKE_LATEST_VT=v0.11.0 FAKE_GO_FAIL=tidy run tidyfail
check_rc "a go mod tidy failure exits 20" "$rc" 20
check_contains "the tidy failure sets the status" "$envv" "status=build"
check_contains "the tidy error is quoted" "$mtx" "unknown revision v9.9.9"
check_contains "the tidy failure is attributed to tidy" "$mtx" "first go mod tidy error"
check_contains "a module that does not tidy is not built" "$mtx" "| root module (lakehouse-logs) | ❌ | — | — |"

# --- 6. drift needs registry rows --------------------------------------
FAKE_LATEST_VL=v1.52.0 FAKE_LATEST_VT=v0.11.0 FAKE_GO_FAIL=drift run drift
check_rc "unmapped upstream items exit 30" "$rc" 30
check_contains "the drift failure sets the status" "$envv" "status=conformance"
check_contains "the failing gate is named" "$mtx" "TestDrift_RealRegistry"
check_contains "the drift detail is quoted" "$mtx" "has no registry row"
check_contains "the verdict names the registry" "$mtx" "conformance gates need a human"

# --- 6b. a reported-version gate fails -------------------------------
FAKE_LATEST_VL=v1.52.0 FAKE_LATEST_VT=v0.11.0 FAKE_GO_FAIL=compat run compat
check_rc "a failing reported-version gate exits 30" "$rc" 30
check_contains "the failing reported-version gate is named" "$mtx" "Reported-version gate \`TestVLCompatMatchesGoMod\`: ❌"
check_contains "the reported-version failure is quoted" "$mtx" "but go.mod requires VictoriaLogs"

# --- 6c. a renamed reported-version gate is not a silent pass ---------
FAKE_LATEST_VL=v1.52.0 FAKE_LATEST_VT=v0.11.0 FAKE_GO_FAIL=nocompat run nocompat
check_rc "a reported-version gate that matches no test exits 30" "$rc" 30
check_contains "a vanished gate is explained" "$mtx" "no test by that name any more"

# --- 7. --no-pr reports open_pr=false ----------------------------------
FAKE_LATEST_VL=v1.52.0 FAKE_LATEST_VT=v0.11.0 run nopr --no-pr
check_rc "--no-pr still probes and exits on the verdict" "$rc" 0
check_contains "--no-pr asks for no pull request" "$envv" "open_pr=false"
check_contains "--no-pr still produces the matrix" "$mtx" "a clean bump is possible"

# --- 8. a deps step fails; an all-digit commit is quoted in YAML ------
# VictoriaTraces v0.12.0 requires a VictoriaLogs commit that does not exist, so
# the traces checkout fails: the probe cannot judge anything and must say so
# with exit 1, not with a verdict. The pins are rewritten before that step, so
# the clone still shows how an all-digit commit lands in a workflow env block.
FAKE_LATEST_VL=v1.52.0 FAKE_LATEST_VT=v0.12.0 run depsfail
check_rc "a failing deps step exits 1" "$rc" 1
check_contains "a failing deps step sets the status" "$envv" "status=error"
check_contains "a failing deps step asks for no pull request" "$envv" "open_pr=false"
check_contains "the failing deps step is named" "$mtx" "deps-traces\` step failed"
check_contains "the matrix says nothing else was judged" "$mtx" "build and conformance were not run"
clone="$TMP/work-depsfail/repo"
check_contains "an unquoted all-digit commit is quoted" \
	"$(cat "$clone/.github/workflows/ci.yaml")" 'VL_COMMIT_TRACES: "012345678901"'
check_contains "an already-quoted all-digit commit is not double-quoted" \
	"$(cat "$clone/.github/workflows/release.yaml")" 'VL_COMMIT_TRACES: "012345678901"'
check_not_contains "no value is double-quoted" \
	"$(cat "$clone/.github/workflows/release.yaml")" '""'
check_contains "a version pin that is not numeric stays unquoted" \
	"$(cat "$clone/.github/workflows/ci.yaml")" "VT_VERSION: v0.12.0"

# --- 9. bad invocation --------------------------------------------------
out="$(PATH="$BIN:$PATH" "$PROBE" --repo "$REPO" --nonsense 2>&1)"
rc=$?
check_rc "an unknown argument exits 2" "$rc" 2
out="$("$PROBE" --repo "$TMP" 2>&1)"
rc=$?
check_rc "a directory without a Makefile exits 2" "$rc" 2

# --- 10. version comparison, straight off the probe's own function ------
# Extracted rather than re-implemented, so the table tests the code that runs.
eval "$(sed -n '/^version_gt() {/,/^}/p' "$PROBE")"
if ! declare -F version_gt > /dev/null; then
	fail=$((fail + 1))
	printf 'FAIL: version_gt could not be extracted from %s\n' "$PROBE" >&2
else
	while read -r a b want; do
		[[ -z "$a" ]] && continue
		if version_gt "$a" "$b"; then got=newer; else got=not-newer; fi
		if [[ "$got" == "$want" ]]; then
			pass=$((pass + 1))
		else
			fail=$((fail + 1))
			printf 'FAIL: version_gt %s %s = %s, want %s\n' "$a" "$b" "$got" "$want" >&2
		fi
	done <<'EOF'
v1.53.0 v1.52.0 newer
v1.10.0 v1.9.0 newer
v1.100.0 v1.50.0 newer
v2.0.0 v1.99.99 newer
v1.52.1 v1.52.0 newer
v0.11.0 v0.9.2 newer
v1.53.0 v1.53.0-rc.1 newer
v1.52.0 v1.52.0 not-newer
v1.5.0 v1.50.0 not-newer
v1.9.0 v1.10.0 not-newer
v1.52.0-rc.2 v1.52.0 not-newer
v0.9.2 v0.11.0 not-newer
EOF
fi

printf '%d checks passed, %d failed\n' "$pass" "$fail"
[[ $fail -eq 0 ]]
