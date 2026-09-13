#!/usr/bin/env bash
# Upstream sync probe: answer "what would it cost to move to the newest
# VictoriaLogs / VictoriaTraces release?" before anyone spends a day finding out
# by hand.
#
# The probe never decides to bump. It performs the mechanical half of a bump on
# a throwaway clone and reports what broke:
#
#   1. read the CURRENT pins from the Makefile — the single source of truth for
#      VL_VERSION_LOGS, VL_COMMIT_TRACES and VT_VERSION (not a side manifest,
#      which is how the previous version of this check ended up comparing the
#      newest release against a string that matched no tag at all);
#   2. resolve the CANDIDATE releases (explicit --vl / --vt, else the newest
#      non-prerelease GitHub release of each project) and stop with "up to date"
#      when neither is newer;
#   3. clone the repository into a work directory, branch
#      upstream-sync/vl-<ver>-vt-<ver>, and write the candidate pins into every
#      place the tooling reads them (Makefile, Dockerfile ARG defaults, Compose
#      image tags, workflow env blocks) — the same set tests/conformance/pins_test.go
#      gates, so a missed place fails a test instead of silently mismatching;
#   4. derive VL_COMMIT_TRACES the way the Makefile documents it: it is never
#      chosen, it is the VictoriaLogs commit VictoriaTraces' own go.mod requires
#      at the candidate VT_VERSION;
#   5. run the Makefile's own deps targets, command by command (read out of
#      `make -n`, so the probe cannot drift from the recipe), checking every
#      `git apply` with `git apply --check --verbose` first and recording the
#      verdict — plus the context the patch searched for when it fails;
#   6. move the go.mod requirements (root: the VictoriaLogs release; traces:
#      VictoriaTraces' own VictoriaLogs requirement verbatim, and VictoriaTraces)
#      and the versions each binary reports, `go mod tidy`, then build and vet
#      both modules with GOWORK=off — and list what else tidy moved;
#   7. regenerate the conformance inventory, diff the upstream surface (items
#      added and removed, protocol versions moved) and run the conformance
#      gates — drift, pins, query grammar — plus the reported-version gates.
#
# Everything lands in one Markdown matrix (--out), which is the artifact, the
# step summary and the body of the sync pull request. Exit codes are the
# verdict:
#
#     0   clean bump possible (or already up to date)
#    10   a patch no longer applies — regenerate it against the new tree
#    20   a module no longer tidies, builds or vets
#    30   the conformance gates fail — registry rows and/or a pin site need a human
#     1   the probe itself could not run (clone, network, malformed pins)
#     2   bad invocation
#
# A step whose prerequisite failed is reported as "not run" rather than guessed,
# so the matrix never shows a green cell the probe did not observe.
#
# Usage: scripts/ci/upstream_sync_probe.sh [options]
#   --vl <tag>        candidate VictoriaLogs release (default: newest release)
#   --vt <tag>        candidate VictoriaTraces release (default: newest release)
#   --repo <dir>      repository to probe (default: the one this script is in)
#   --workdir <dir>   where the throwaway clone lives (default: a temp dir)
#   --out <file>      Markdown result matrix (default: <workdir>/upstream-sync-matrix.md)
#   --env-out <file>  key=value facts for the calling workflow (branch, status, ...)
#   --no-pr           record open_pr=false in --env-out: probe and report only
#   --keep            keep the work directory (it is removed by default)
set -euo pipefail

PROG=upstream_sync_probe

VL_CANDIDATE=""
VT_CANDIDATE=""
REPO=""
WORKDIR=""
WORKDIR_IS_TEMP=0
OUT=""
ENV_OUT=""
OPEN_PR=true
KEEP=0

# Release sources. Derived from the Makefile's clone URLs when those are GitHub
# URLs, so a fork only has to change the Makefile; these are the fallback.
VL_GH_REPO="VictoriaMetrics/VictoriaLogs"
VT_GH_REPO="VictoriaMetrics/VictoriaTraces"

SYNC_GIT_NAME="${SYNC_GIT_NAME:-github-actions[bot]}"
SYNC_GIT_EMAIL="${SYNC_GIT_EMAIL:-github-actions[bot]@users.noreply.github.com}"

die() {
	printf '%s: %s\n' "$PROG" "$1" >&2
	exit "${2:-1}"
}

usage() {
	sed -n '/^# Usage:/,/^set -euo/p' "$0" | sed 's/^#\{1,\} \{0,1\}//; $d'
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--vl)
		VL_CANDIDATE="${2:-}"
		shift 2
		;;
	--vt)
		VT_CANDIDATE="${2:-}"
		shift 2
		;;
	--repo)
		REPO="${2:-}"
		shift 2
		;;
	--workdir)
		WORKDIR="${2:-}"
		shift 2
		;;
	--out)
		OUT="${2:-}"
		shift 2
		;;
	--env-out)
		ENV_OUT="${2:-}"
		shift 2
		;;
	--no-pr)
		OPEN_PR=false
		shift
		;;
	--keep)
		KEEP=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage >&2
		die "unknown argument: $1" 2
		;;
	esac
done

# ---------------------------------------------------------------------------
# Repository and work area
# ---------------------------------------------------------------------------

if [[ -z "$REPO" ]]; then
	script_dir="$(cd "$(dirname "$0")" && pwd)"
	REPO="$(cd "$script_dir/../.." && pwd)"
fi
[[ -f "$REPO/Makefile" ]] || die "no Makefile in $REPO — not a repository root" 2

if [[ -z "$WORKDIR" ]]; then
	WORKDIR="$(mktemp -d)"
	WORKDIR_IS_TEMP=1
fi
mkdir -p "$WORKDIR"
WORKDIR="$(cd "$WORKDIR" && pwd)"
CLONE="$WORKDIR/repo"
LOGS_DIR="$WORKDIR/probe-logs"
mkdir -p "$LOGS_DIR"

[[ -n "$OUT" ]] || OUT="$WORKDIR/upstream-sync-matrix.md"
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

# shellcheck disable=SC2329 # invoked through the EXIT trap below
cleanup() {
	if [[ $KEEP -eq 0 && $WORKDIR_IS_TEMP -eq 1 ]]; then
		rm -rf "$WORKDIR"
	fi
}
trap cleanup EXIT

FENCE='```'

say() { printf '%s\n' "$*"; }
m() { printf '%s\n' "$*" >> "$OUT"; }

# emit_env writes the facts the calling workflow needs. No value contains a
# newline, so the file can be appended to $GITHUB_OUTPUT verbatim.
emit_env() { # emit_env <status> <exit code> <open_pr>
	[[ -n "$ENV_OUT" ]] || return 0
	{
		printf 'status=%s\n' "$1"
		printf 'exit_code=%s\n' "$2"
		printf 'vl_current=%s\n' "$VL_CURRENT"
		printf 'vt_current=%s\n' "$VT_CURRENT"
		printf 'vl_candidate=%s\n' "${VL_CANDIDATE:-$VL_CURRENT}"
		printf 'vt_candidate=%s\n' "${VT_CANDIDATE:-$VT_CURRENT}"
		printf 'vl_commit_traces=%s\n' "${VL_COMMIT_NEW:-$VL_COMMIT_CURRENT}"
		printf 'branch=%s\n' "${BRANCH:-}"
		printf 'open_pr=%s\n' "$3"
		printf 'matrix=%s\n' "$OUT"
	} > "$ENV_OUT"
}

finish() { # finish <status> <exit code> <open_pr>
	emit_env "$1" "$2" "$3"
	say "$PROG: $1 (exit $2); matrix: $OUT"
	exit "$2"
}

# ---------------------------------------------------------------------------
# Pins: the Makefile is the source of truth
# ---------------------------------------------------------------------------

makefile_var() { # makefile_var <name> <makefile>
	# make ends a value at `#`, so a trailing comment is not part of the pin.
	sed -n "s/^$1[[:space:]]*:=[[:space:]]*//p" "$2" | head -1 | sed -e 's/#.*$//' -e 's/[[:space:]]*$//'
}

VL_CURRENT="$(makefile_var VL_VERSION_LOGS "$REPO/Makefile")"
VT_CURRENT="$(makefile_var VT_VERSION "$REPO/Makefile")"
VL_COMMIT_CURRENT="$(makefile_var VL_COMMIT_TRACES "$REPO/Makefile")"
VL_REPO_URL="$(makefile_var VL_REPO "$REPO/Makefile")"
VT_REPO_URL="$(makefile_var VT_REPO "$REPO/Makefile")"
VT_DIR="$(makefile_var VT_DIR "$REPO/Makefile")"

if [[ -z "$VL_CURRENT" || -z "$VT_CURRENT" || -z "$VL_COMMIT_CURRENT" ]]; then
	die "Makefile does not declare all three pins (VL_VERSION_LOGS=$VL_CURRENT VT_VERSION=$VT_CURRENT VL_COMMIT_TRACES=$VL_COMMIT_CURRENT)"
fi

gh_slug() { # gh_slug <clone url> <fallback>
	case "$1" in
	*github.com[:/]*) printf '%s\n' "$1" | sed -e 's|^.*github\.com[:/]||' -e 's|\.git$||' ;;
	*) printf '%s\n' "$2" ;;
	esac
}
VL_GH_REPO="$(gh_slug "$VL_REPO_URL" "$VL_GH_REPO")"
VT_GH_REPO="$(gh_slug "$VT_REPO_URL" "$VT_GH_REPO")"

# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

# version_gt <a> <b> — true when release a is newer than release b. Compares
# dotted numeric components; a release always beats a prerelease of the same
# numbers (v1.53.0 > v1.53.0-rc.1), which is also why the release query below
# asks for /releases/latest — GitHub excludes prereleases from it.
version_gt() {
	local a="${1#v}" b="${2#v}" apre="" bpre="" i x y
	case "$a" in *-*)
		apre="${a#*-}"
		a="${a%%-*}"
		;;
	esac
	case "$b" in *-*)
		bpre="${b#*-}"
		b="${b%%-*}"
		;;
	esac
	local an bn
	IFS='.' read -r -a an <<< "$a"
	IFS='.' read -r -a bn <<< "$b"
	for i in 0 1 2 3; do
		x="${an[i]:-0}"
		y="${bn[i]:-0}"
		if [[ ! "$x" =~ ^[0-9]+$ || ! "$y" =~ ^[0-9]+$ ]]; then
			# A non-numeric component is compared as text rather than silently
			# read as zero, so a tag shaped unexpectedly never looks "older".
			if [[ "$x" > "$y" ]]; then return 0; fi
			if [[ "$x" < "$y" ]]; then return 1; fi
			continue
		fi
		if ((10#$x > 10#$y)); then return 0; fi
		if ((10#$x < 10#$y)); then return 1; fi
	done
	if [[ -z "$apre" && -n "$bpre" ]]; then return 0; fi
	return 1
}

# latest_release <owner/name> — newest non-prerelease tag, via gh when it is
# available and authenticated, else the public REST API through curl.
latest_release() {
	local slug="$1" tag=""
	if command -v gh > /dev/null 2>&1; then
		tag="$(gh api "repos/$slug/releases/latest" --jq .tag_name 2> /dev/null || true)"
	fi
	if [[ -z "$tag" || "$tag" == "null" ]] && command -v curl > /dev/null 2>&1; then
		tag="$(curl -sSfL -H 'Accept: application/vnd.github+json' \
			"https://api.github.com/repos/$slug/releases/latest" 2> /dev/null |
			sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1 || true)"
	fi
	printf '%s\n' "$tag"
}

VL_EXPLICIT=0
VT_EXPLICIT=0
if [[ -n "$VL_CANDIDATE" ]]; then VL_EXPLICIT=1; fi
if [[ -n "$VT_CANDIDATE" ]]; then VT_EXPLICIT=1; fi

if [[ -z "$VL_CANDIDATE" ]]; then
	VL_CANDIDATE="$(latest_release "$VL_GH_REPO")"
	[[ -n "$VL_CANDIDATE" ]] || die "could not resolve the newest $VL_GH_REPO release (no gh, no network?)"
fi
if [[ -z "$VT_CANDIDATE" ]]; then
	VT_CANDIDATE="$(latest_release "$VT_GH_REPO")"
	[[ -n "$VT_CANDIDATE" ]] || die "could not resolve the newest $VT_GH_REPO release (no gh, no network?)"
fi

# An explicitly requested version is always probed, newer or not: that is how a
# maintainer asks "what would going back to X cost?".
VL_NEWER=0
VT_NEWER=0
if [[ $VL_EXPLICIT -eq 1 && "$VL_CANDIDATE" != "$VL_CURRENT" ]] || version_gt "$VL_CANDIDATE" "$VL_CURRENT"; then
	VL_NEWER=1
fi
if [[ $VT_EXPLICIT -eq 1 && "$VT_CANDIDATE" != "$VT_CURRENT" ]] || version_gt "$VT_CANDIDATE" "$VT_CURRENT"; then
	VT_NEWER=1
fi

if [[ $VL_NEWER -eq 0 && $VT_NEWER -eq 0 ]]; then
	m "## Upstream sync probe — up to date"
	m ""
	m "| Pin | Pinned | Newest upstream release |"
	m "| --- | --- | --- |"
	m "| \`VL_VERSION_LOGS\` | $VL_CURRENT | $VL_CANDIDATE |"
	m "| \`VT_VERSION\` | $VT_CURRENT | $VT_CANDIDATE |"
	m "| \`VL_COMMIT_TRACES\` | $VL_COMMIT_CURRENT | derived from VictoriaTraces' \`go.mod\` |"
	m ""
	m "Nothing to do: the Makefile already pins the newest release of both projects."
	finish "up-to-date" 0 false
fi

BRANCH="upstream-sync/vl-$VL_CANDIDATE-vt-$VT_CANDIDATE"
TITLE="Upstream sync probe — VictoriaLogs $VL_CURRENT → $VL_CANDIDATE, VictoriaTraces $VT_CURRENT → $VT_CANDIDATE"

# ---------------------------------------------------------------------------
# Clean clone + branch
# ---------------------------------------------------------------------------

say "$PROG: cloning $REPO into $CLONE"
git clone --quiet --no-hardlinks "$REPO" "$CLONE" 2> "$LOGS_DIR/clone.log" ||
	die "clone failed: $(tail -3 "$LOGS_DIR/clone.log" | tr '\n' ' ')"
git -C "$CLONE" checkout --quiet -B "$BRANCH"

edit() { # edit <sed expression> <file>
	[[ -f "$2" ]] || return 0
	sed "$1" "$2" > "$2.probe.tmp" && mv "$2.probe.tmp" "$2"
}

# Every pin rewrite below replaces the value token and nothing else: spacing,
# quoting and a trailing comment stay exactly as they were, so a sync branch
# never carries a diff beyond the version itself.
set_makefile_pin() { # set_makefile_pin <name> <value>
	edit "s|^\($1[[:space:]]*:=[[:space:]]*\)[^[:space:]#]*|\1$2|" "$CLONE/Makefile"
}

# yaml_scalar_needs_quotes — true for a value a YAML 1.2 parser would read as a
# number rather than a string: an all-digit commit prefix (leading zeros lost)
# or a digits-e-digits one (a float). 12 hex characters are all digits about
# once in 280 commits, so this is rare, but it silently corrupts a pin.
yaml_scalar_needs_quotes() {
	[[ "$1" =~ ^[0-9]+([eE][0-9]+)?$ ]]
}

set_yaml_env_pin() { # set_yaml_env_pin <name> <value> <file>
	local value="$2"
	if yaml_scalar_needs_quotes "$value"; then
		# Quote only where the current value is unquoted; an already-quoted
		# value keeps its own quotes through the token-only rewrite.
		edit "/\\\$/!s|^\([[:space:]]*$1:[[:space:]]*\)\([^\"'#[:space:]][^#[:space:]]*\)|\1\"$value\"|" "$3"
	fi
	# The `/\$/!` address leaves `${{ env.X }}` references alone: those are
	# uses of a pin, not declarations of one.
	edit "/\\\$/!s|^\([[:space:]]*$1:[[:space:]]*\)\([\"']\{0,1\}\)[^\"'#[:space:]]*|\1\2$value|" "$3"
}

commit_branch() {
	(
		cd "$CLONE"
		git add -A
		if git diff --cached --quiet; then
			exit 0
		fi
		# Never sign: a bot commit on a throwaway clone has no key to sign
		# with, and a developer machine that signs by configuration would
		# otherwise stop here waiting for one.
		git -c "user.name=$SYNC_GIT_NAME" -c "user.email=$SYNC_GIT_EMAIL" -c commit.gpgsign=false \
			commit --quiet -m "deps: probe upstream sync VL $VL_CANDIDATE / VT $VT_CANDIDATE"
	)
}

# ---------------------------------------------------------------------------
# Candidate VT pin first, then the derived VictoriaLogs commit the traces
# binary must use. Order matters: deps-vt needs only VT_VERSION, and its clone
# is where the derivation reads from — one clone, not two.
# ---------------------------------------------------------------------------

set_makefile_pin VT_VERSION "$VT_CANDIDATE"
set_makefile_pin VL_VERSION_LOGS "$VL_CANDIDATE"

PATCH_ROWS=()
PATCH_COUNT=0
PATCH_FAILED=0

record_patch() { # record_patch <name> <tree> <ok|fail> [detail]
	local detail_file=""
	if [[ -n "${4:-}" ]]; then
		detail_file="$LOGS_DIR/patch-$PATCH_COUNT.detail"
		printf '%s\n' "$4" > "$detail_file"
	fi
	PATCH_ROWS[PATCH_COUNT]="$1|$2|$3|$detail_file"
	PATCH_COUNT=$((PATCH_COUNT + 1))
}

# run_deps_target executes the Makefile's own recipe for a deps target, command
# by command, as printed by `make -n`. Reading the recipe instead of copying it
# keeps the probe honest: when the Makefile learns a new step, so does this.
# Every `git apply` is checked before it runs, so a patch that no longer applies
# is a recorded verdict rather than an aborted run.
run_deps_target() { # run_deps_target <target> <tree label>
	local target="$1" tree="$2" cmds line patch check out
	if ! cmds="$(cd "$CLONE" && make -n "$target" 2> "$LOGS_DIR/$target.make.log")"; then
		m "- \`make -n $target\` failed: \`$(tail -2 "$LOGS_DIR/$target.make.log" | tr '\n' ' ')\`"
		return 1
	fi
	while IFS= read -r line; do
		line="${line#@}"
		line="${line#-}"
		line="${line#+}"
		[[ -z "${line// /}" ]] && continue
		# Comment lines inside a recipe are echoed by `make -n` too.
		[[ "$line" =~ ^[[:space:]]*# ]] && continue
		if [[ "$line" == *"git apply "* ]]; then
			patch="${line##* }"
			check="${line/git apply /git apply --check --verbose }"
			if out="$(cd "$CLONE" && bash -c "$check" 2>&1)"; then
				if (cd "$CLONE" && bash -c "$line" > /dev/null 2>&1); then
					record_patch "$(basename "$patch")" "$tree" ok
				else
					record_patch "$(basename "$patch")" "$tree" fail "passed --check but failed to apply"
					PATCH_FAILED=1
				fi
			else
				# `git apply --verbose` prints the context the patch searched
				# for, which is the only thing that tells a maintainer which
				# hunk moved and how far.
				record_patch "$(basename "$patch")" "$tree" fail \
					"$(printf '%s' "$out" | grep -v '^Checking patch' | head -20)"
				PATCH_FAILED=1
			fi
			continue
		fi
		if ! out="$(cd "$CLONE" && bash -c "$line" 2>&1)"; then
			printf '%s\n' "$out" > "$LOGS_DIR/$target.step.log"
			m "- \`$target\` step failed: \`${line:0:120}\` — \`$(printf '%s' "$out" | tail -2 | tr '\n' ' ')\`"
			return 1
		fi
	done <<< "$cmds"
	return 0
}

m "## $TITLE"
m ""
m "| Pin | Current | Candidate |"
m "| --- | --- | --- |"
m "| \`VL_VERSION_LOGS\` | $VL_CURRENT | **$VL_CANDIDATE** |"
m "| \`VT_VERSION\` | $VT_CURRENT | **$VT_CANDIDATE** |"

say "$PROG: preparing VictoriaTraces $VT_CANDIDATE"
if ! run_deps_target deps-vt "VictoriaTraces $VT_CANDIDATE"; then
	m ""
	m "The probe could not prepare the VictoriaTraces tree, so nothing else could be checked."
	finish "error" 1 false
fi

# The traces-side VictoriaLogs pin is DERIVED, never chosen: it is the commit
# VictoriaTraces' own go.mod requires.
#
# vt_vl_requirement prints that requirement verbatim — a pseudo-version such as
# v1.121.1-0.20260617051904-6ae2da3c11f3, or a plain release tag. Both go.mod
# shapes count: a line inside a `require (` block and a single-line `require`.
# A `replace` line matches neither, and must not: it names no version.
vt_vl_requirement() { # vt_vl_requirement <vt tree>
	[[ -f "$1/go.mod" ]] || return 0
	awk '
		$1 == "require" && $2 ~ /VictoriaMetrics\/VictoriaLogs$/ { print $3; exit }
		$1 ~ /^github\.com\/VictoriaMetrics\/VictoriaLogs$/ { print $2; exit }
	' "$1/go.mod"
}

# commit_of_requirement turns the requirement into the 12-character commit the
# Makefile pins: a pseudo-version carries it as its last component; a release
# tag has to be resolved against VictoriaLogs itself.
commit_of_requirement() { # commit_of_requirement <requirement>
	local req="$1" commit="${1##*-}"
	if [[ "$commit" =~ ^[0-9a-f]{12}$ ]]; then
		printf '%s\n' "$commit"
		return 0
	fi
	commit="$(git ls-remote "$VL_REPO_URL" "refs/tags/$req^{}" 2> /dev/null | awk '{print substr($1, 1, 12)}' | head -1)"
	if [[ -z "$commit" ]]; then
		commit="$(git ls-remote "$VL_REPO_URL" "refs/tags/$req" 2> /dev/null | awk '{print substr($1, 1, 12)}' | head -1)"
	fi
	[[ -n "$commit" ]] || return 1
	printf '%s\n' "$commit"
}

VL_TRACES_REQUIRE="$(vt_vl_requirement "$CLONE/$VT_DIR")"
if [[ -z "$VL_TRACES_REQUIRE" ]]; then
	m ""
	m "VictoriaTraces $VT_CANDIDATE's \`go.mod\` requires no VictoriaLogs version, so \`VL_COMMIT_TRACES\` cannot be derived — the two-pin rule needs revisiting before anything else can be judged."
	finish "error" 1 false
fi
if ! VL_COMMIT_NEW="$(commit_of_requirement "$VL_TRACES_REQUIRE")"; then
	m ""
	m "VictoriaTraces $VT_CANDIDATE requires VictoriaLogs \`$VL_TRACES_REQUIRE\`, which is neither a pseudo-version nor a tag of \`$VL_REPO_URL\` — \`VL_COMMIT_TRACES\` cannot be derived."
	finish "error" 1 false
fi
set_makefile_pin VL_COMMIT_TRACES "$VL_COMMIT_NEW"
m "| \`VL_COMMIT_TRACES\` | $VL_COMMIT_CURRENT | **$VL_COMMIT_NEW** (derived from VictoriaTraces $VT_CANDIDATE \`go.mod\`) |"
m ""

# ---------------------------------------------------------------------------
# The pins outside the Makefile, which pins_test.go requires to be equal to it
# ---------------------------------------------------------------------------

bump_pins_outside_makefile() {
	local f count=0
	for f in "$CLONE"/Dockerfile.*; do
		[[ -f "$f" ]] || continue
		edit "s|^\(ARG VL_VERSION=\)[^[:space:]]*|\1$VL_CANDIDATE|" "$f"
		edit "s|^\(ARG VL_COMMIT=\)[^[:space:]]*|\1$VL_COMMIT_NEW|" "$f"
		edit "s|^\(ARG VT_VERSION=\)[^[:space:]]*|\1$VT_CANDIDATE|" "$f"
		count=$((count + 1))
	done
	while IFS= read -r f; do
		[[ -n "$f" ]] || continue
		edit "s|victoriametrics/victoria-logs:[^\"'[:space:]]*|victoriametrics/victoria-logs:$VL_CANDIDATE|g" "$CLONE/$f"
		edit "s|victoriametrics/victoria-traces:[^\"'[:space:]]*|victoriametrics/victoria-traces:$VT_CANDIDATE|g" "$CLONE/$f"
		count=$((count + 1))
	done < <(cd "$CLONE" && git grep -l 'victoriametrics/victoria-' -- '*docker-compose*.yml' '*docker-compose*.yaml' 2> /dev/null || true)
	for f in "$CLONE"/.github/workflows/*.yaml "$CLONE"/.github/workflows/*.yml; do
		[[ -f "$f" ]] || continue
		set_yaml_env_pin VL_VERSION_LOGS "$VL_CANDIDATE" "$f"
		set_yaml_env_pin VL_COMMIT_TRACES "$VL_COMMIT_NEW" "$f"
		set_yaml_env_pin VT_VERSION "$VT_CANDIDATE" "$f"
		count=$((count + 1))
	done
	say "$PROG: rewrote pins in $count file(s) outside the Makefile"
}
bump_pins_outside_makefile

# ---------------------------------------------------------------------------
# The two VictoriaLogs trees and their patches
# ---------------------------------------------------------------------------

say "$PROG: preparing VictoriaLogs $VL_CANDIDATE (logs) and $VL_COMMIT_NEW (traces)"
deps_ok=1
if ! run_deps_target deps-logs "VictoriaLogs $VL_CANDIDATE (logs)"; then deps_ok=0; fi
if [[ $deps_ok -eq 1 ]]; then
	if ! run_deps_target deps-traces "VictoriaLogs $VL_COMMIT_NEW (traces)"; then deps_ok=0; fi
fi

m "### Patches"
m ""
if [[ $PATCH_COUNT -eq 0 ]]; then
	m "_No \`git apply\` step was found in the deps targets — the recipe changed shape and the probe checked nothing._"
	PATCH_FAILED=1
else
	m "| Patch | Upstream tree | \`git apply --check\` |"
	m "| --- | --- | --- |"
	for row in "${PATCH_ROWS[@]}"; do
		IFS='|' read -r p_name p_tree p_verdict p_detail <<< "$row"
		if [[ "$p_verdict" == "ok" ]]; then
			m "| \`$p_name\` | $p_tree | ✅ |"
		else
			m "| \`$p_name\` | $p_tree | ❌ |"
		fi
	done
	m ""
	for row in "${PATCH_ROWS[@]}"; do
		IFS='|' read -r p_name p_tree p_verdict p_detail <<< "$row"
		[[ "$p_verdict" == "ok" ]] && continue
		m "<details><summary>❌ <code>$p_name</code> — $p_tree</summary>"
		m ""
		m '```'
		if [[ -n "$p_detail" && -f "$p_detail" ]]; then m "$(cat "$p_detail")"; fi
		m '```'
		m ""
		m "</details>"
		m ""
	done
fi

if [[ $deps_ok -eq 0 ]]; then
	m "The upstream trees could not be prepared; build and conformance were not run."
	commit_branch
	finish "error" 1 false
fi

# ---------------------------------------------------------------------------
# Build + vet, both modules
# ---------------------------------------------------------------------------

BUILD_FAILED=0
m "### Build"
m ""

# set_compat_constant moves the upstream version a binary reports on
# /lakehouse/info. Each module has a test holding the constant equal to its
# go.mod requirement, so moving one without the other is a red pull request
# for a purely mechanical reason.
set_compat_constant() { # set_compat_constant <file> <const> <version>
	local f="$CLONE/$1"
	if [[ -f "$f" ]] && grep -qE "^const $2 = \"[^\"]*\"" "$f"; then
		edit "s|^const $2 = \"[^\"]*\"|const $2 = \"${3#v}\"|" "$f"
		return 0
	fi
	return 1
}

fenced() { # fenced <heading> <text> — one collapsible-free error excerpt
	printf '\n**%s**\n\n%s\n%s\n%s\n' "$1" "$FENCE" "$(printf '%s' "$2" | head -12)" "$FENCE"
}

if [[ $PATCH_FAILED -eq 1 ]]; then
	m "_Not run: while a patch does not apply the upstream trees are incomplete, so a build failure here would say nothing._"
	m ""
else
	# The replace directives already point both modules at the checkouts above,
	# so a build would compile without these edits — and then fail the moment a
	# new tree requires a newer dependency than go.sum lists. Moving the
	# requirements and tidying is the first thing a maintainer does, so the
	# verdict below is about the code rather than about bookkeeping.
	build_details=""
	go_edits_ok=1
	if ! out="$(cd "$CLONE" && GOWORK=off go mod edit \
		-require="github.com/VictoriaMetrics/VictoriaLogs@$VL_CANDIDATE" 2>&1)"; then
		go_edits_ok=0
		build_details="$build_details$(fenced "root module — go mod edit failed" "$out")"
	fi
	if ! out="$(cd "$CLONE/lakehouse-traces" && GOWORK=off go mod edit \
		-require="github.com/VictoriaMetrics/VictoriaLogs@$VL_TRACES_REQUIRE" \
		-require="github.com/VictoriaMetrics/VictoriaTraces@$VT_CANDIDATE" 2>&1)"; then
		go_edits_ok=0
		build_details="$build_details$(fenced "lakehouse-traces module — go mod edit failed" "$out")"
	fi
	compat_note=""
	if ! set_compat_constant cmd/lakehouse-logs/main.go vlCompat "$VL_CANDIDATE"; then
		compat_note="$compat_note \`vlCompat\` not found in \`cmd/lakehouse-logs/main.go\`."
	fi
	if ! set_compat_constant lakehouse-traces/main.go vtCompat "$VT_CANDIDATE"; then
		compat_note="$compat_note \`vtCompat\` not found in \`lakehouse-traces/main.go\`."
	fi

	m "Requirements moved: root \`VictoriaLogs $VL_CANDIDATE\`; lakehouse-traces \`VictoriaLogs $VL_TRACES_REQUIRE\` (VictoriaTraces' own requirement, verbatim) and \`VictoriaTraces $VT_CANDIDATE\`. Reported versions: \`vlCompat ${VL_CANDIDATE#v}\`, \`vtCompat ${VT_CANDIDATE#v}\`.${compat_note:+ ⚠️$compat_note}"
	m ""
	m "| Module | \`go mod tidy\` | \`go build ./...\` | \`go vet ./...\` |"
	m "| --- | --- | --- | --- |"
	for mod in "." "lakehouse-traces"; do
		label="root module (lakehouse-logs)"
		[[ "$mod" == "lakehouse-traces" ]] && label="lakehouse-traces module"
		tidy_verdict="✅"
		build_verdict="✅"
		vet_verdict="✅"
		if [[ $go_edits_ok -eq 0 ]]; then
			tidy_verdict="—"
			build_verdict="—"
			vet_verdict="—"
			BUILD_FAILED=1
		elif ! out="$(cd "$CLONE/$mod" && GOWORK=off go mod tidy 2>&1)"; then
			tidy_verdict="❌"
			build_verdict="—"
			vet_verdict="—"
			BUILD_FAILED=1
			build_details="$build_details$(fenced "$label — first go mod tidy error" "$out")"
		elif ! out="$(cd "$CLONE/$mod" && GOWORK=off go build ./... 2>&1)"; then
			build_verdict="❌"
			vet_verdict="—"
			BUILD_FAILED=1
			build_details="$build_details$(fenced "$label — first build error" "$out")"
		elif ! out="$(cd "$CLONE/$mod" && GOWORK=off go vet ./... 2>&1)"; then
			vet_verdict="❌"
			BUILD_FAILED=1
			build_details="$build_details$(fenced "$label — first vet error" "$out")"
		fi
		m "| $label | $tidy_verdict | $build_verdict | $vet_verdict |"
	done
	m ""
	if [[ -n "$build_details" ]]; then
		m "$build_details"
		m ""
	fi

	# What tidy pulled in besides the two pins: a moved shared library (the
	# VictoriaMetrics lib carries security fixes), a raised `go` directive, a new
	# toolchain line. Every one of them lands on this branch.
	churn="$(cd "$CLONE" && git diff -U0 -- go.mod lakehouse-traces/go.mod |
		grep -E '^[+-]([[:space:]]+[^[:space:]]+ v|(go|toolchain) )' |
		grep -vE 'github\.com/VictoriaMetrics/Victoria(Logs|Traces) ' || true)"
	if [[ -n "$churn" ]]; then
		m "<details><summary>Other go.mod changes the new trees pulled in</summary>"
		m ""
		m '```'
		m "$churn"
		m '```'
		m ""
		m "</details>"
		m ""
	fi
fi

# ---------------------------------------------------------------------------
# Conformance: the upstream surface diff and the gates
# ---------------------------------------------------------------------------

# inventory_items prints "kind<TAB>surface<TAB>name" for every item in the
# generated inventory, so two snapshots can be compared without depending on
# diff context.
inventory_items() { # inventory_items <inventory.yaml>
	[[ -f "$1" ]] || return 0
	awk '
		/^[[:space:]]*- kind:/ { kind = $3; surface = ""; next }
		/^[[:space:]]*surface:/ { surface = $2; next }
		/^[[:space:]]*name:/ {
			sub(/^[[:space:]]*name:[[:space:]]*/, "")
			if (kind != "") { print kind "\t" surface "\t" $0 }
			kind = ""
		}
	' "$1" | sort -u
}

protocol_block() { # protocol_block <inventory.yaml>
	[[ -f "$1" ]] || return 0
	sed -n '/^protocol:/,/^items:/p' "$1" | grep -E '^[[:space:]]+(vl|vt|select|delete):' || true
}

CONFORMANCE_FAILED=0
m "### Conformance"
m ""
if [[ $PATCH_FAILED -eq 1 ]]; then
	m "_Not run (see Patches)._"
	m ""
elif [[ $BUILD_FAILED -eq 1 ]]; then
	m "_Not run: the tree does not build, so the generated inventory would describe nothing real._"
	m ""
else
	INV="$CLONE/tests/conformance/inventory.generated.yaml"
	inventory_items "$INV" > "$LOGS_DIR/inventory.before"
	protocol_block "$INV" > "$LOGS_DIR/protocol.before"

	if ! out="$(cd "$CLONE" && GOWORK=off go run ./tests/conformance/cmd/confgen -write 2>&1)"; then
		CONFORMANCE_FAILED=1
		m "Inventory regeneration (\`confgen -write\`) failed:"
		m ""
		m '```'
		m "$(printf '%s' "$out" | head -12)"
		m '```'
		m ""
	else
		inventory_items "$INV" > "$LOGS_DIR/inventory.after"
		protocol_block "$INV" > "$LOGS_DIR/protocol.after"
		added="$(comm -13 "$LOGS_DIR/inventory.before" "$LOGS_DIR/inventory.after" || true)"
		removed="$(comm -23 "$LOGS_DIR/inventory.before" "$LOGS_DIR/inventory.after" || true)"

		m "| Upstream surface | Items |"
		m "| --- | --- |"
		m "| at $VL_CURRENT / $VT_CURRENT | $(wc -l < "$LOGS_DIR/inventory.before" | tr -d ' ') |"
		m "| at $VL_CANDIDATE / $VT_CANDIDATE | $(wc -l < "$LOGS_DIR/inventory.after" | tr -d ' ') |"
		m "| added | $(printf '%s' "$added" | grep -c . || true) |"
		m "| removed | $(printf '%s' "$removed" | grep -c . || true) |"
		m ""
		if [[ -n "$added" ]]; then
			m "<details><summary>Added by the candidate releases — each one needs a registry row</summary>"
			m ""
			m '```'
			m "$added"
			m '```'
			m ""
			m "</details>"
			m ""
		fi
		if [[ -n "$removed" ]]; then
			m "<details><summary>Removed by the candidate releases — each one needs a row decision</summary>"
			m ""
			m '```'
			m "$removed"
			m '```'
			m ""
			m "</details>"
			m ""
		fi
		if ! diff -q "$LOGS_DIR/protocol.before" "$LOGS_DIR/protocol.after" > /dev/null 2>&1; then
			m "Internal peer-protocol versions moved — peers of different versions stop talking to each other:"
			m ""
			m '```'
			m "$(diff "$LOGS_DIR/protocol.before" "$LOGS_DIR/protocol.after" || true)"
			m '```'
			m ""
		fi

		if ! out="$(cd "$CLONE" && GOWORK=off go run ./tests/conformance/cmd/confgen -check 2>&1)"; then
			CONFORMANCE_FAILED=1
			m "\`confgen -check\` is still stale right after \`-write\` — the generator is not idempotent on this upstream:"
			m ""
			m '```'
			m "$(printf '%s' "$out" | head -12)"
			m '```'
			m ""
		fi

		gates_log="$LOGS_DIR/conformance.log"
		if (cd "$CLONE" && CONFORMANCE_REQUIRE_DEPS=1 GOWORK=off \
			go test ./tests/conformance/... -count=1 -timeout=10m -v) > "$gates_log" 2>&1; then
			m "Conformance gates (drift, pins, query grammar): ✅"
			m ""
		else
			CONFORMANCE_FAILED=1
			m "Conformance gates (drift, pins, query grammar): ❌"
			m ""
			failed_tests="$(grep -E '^[[:space:]]*--- FAIL: ' "$gates_log" | awk '{print $3}' | sort -u || true)"
			if [[ -n "$failed_tests" ]]; then
				m "| Failing test |"
				m "| --- |"
				while IFS= read -r t; do
					[[ -n "$t" ]] && m "| \`$t\` |"
				done <<< "$failed_tests"
				m ""
			fi
			m "<details><summary>Gate output</summary>"
			m ""
			m '```'
			if [[ -n "$failed_tests" ]]; then
				# `go test` prints a failure's detail BEFORE its `--- FAIL:`
				# line, so the context that explains it is above, not below.
				m "$(grep -B14 -A4 -E '^[[:space:]]*--- FAIL: ' "$gates_log" | head -80 || true)"
			else
				m "$(tail -30 "$gates_log")"
			fi
			m '```'
			m ""
			m "</details>"
			m ""
		fi
		drift_summary="$(grep -o 'Summary: .*' "$gates_log" | head -1 || true)"
		if [[ -n "$drift_summary" ]]; then
			m "Drift: ${drift_summary#Summary: }"
			m ""
		fi

		# The reported-version gates live next to each binary, outside the
		# conformance package. `-run` matching nothing passes silently, so
		# "no tests to run" is a failure here: a renamed gate must not turn
		# into a green cell nobody observed.
		for gate in ".|./cmd/lakehouse-logs/|TestVLCompatMatchesGoMod" "lakehouse-traces|.|TestVTCompatMatchesGoMod"; do
			IFS='|' read -r g_mod g_pkg g_test <<< "$gate"
			g_log="$LOGS_DIR/$g_test.log"
			g_ok=1
			if ! (cd "$CLONE/$g_mod" && GOWORK=off go test "$g_pkg" -run "^$g_test\$" -count=1 -v) > "$g_log" 2>&1; then
				g_ok=0
			elif grep -q 'no tests to run' "$g_log"; then
				g_ok=0
				printf '%s: no test by that name any more — the gate was renamed or removed\n' "$g_test" >> "$g_log"
			fi
			if [[ $g_ok -eq 1 ]]; then
				m "Reported-version gate \`$g_test\`: ✅"
			else
				CONFORMANCE_FAILED=1
				m "Reported-version gate \`$g_test\`: ❌"
				m ""
				m '```'
				m "$(tail -12 "$g_log")"
				m '```'
			fi
			m ""
		done
	fi
fi

# ---------------------------------------------------------------------------
# Changelogs, and what a human still has to do
# ---------------------------------------------------------------------------

m "### Upstream changelogs"
m ""
m "- VictoriaLogs $VL_CURRENT → $VL_CANDIDATE: <https://github.com/$VL_GH_REPO/compare/$VL_CURRENT...$VL_CANDIDATE> · [releases](https://github.com/$VL_GH_REPO/releases)"
m "- VictoriaTraces $VT_CURRENT → $VT_CANDIDATE: <https://github.com/$VT_GH_REPO/compare/$VT_CURRENT...$VT_CANDIDATE> · [releases](https://github.com/$VT_GH_REPO/releases)"
m ""
m "### What the probe did not do"
m ""
m "The probe performs the mechanical half of a bump. The rest is judgement, and"
m "belongs on this branch before it can merge — the recipe is \`docs/upstream-sync.md\`:"
m ""
m "- [ ] Regenerate every patch marked ❌ against the new tree, and re-diff the full-file \`*.src\` replacements against their new upstream originals — that is where an upstream behaviour change hides."
m "- [ ] Review every other \`go.mod\` change \`go mod tidy\` pulled in — a moved shared library is an upstream change of its own."
m "- [ ] Re-sync the embedded web UI (\`make sync-vmui sync-vmui-traces\`) and re-run its drift gate."
m "- [ ] Add a registry row for every added route, pipe, filter or flag that changes behaviour, decide \`expect\` for every removed one, and keep \`since:\` accurate."
m "- [ ] Re-run the query-grammar sweep over the repository against the new parser."
m "- [ ] Run the parity and end-to-end suites against the new hot-tier images and diff the outcomes row by row against the current release."
m "- [ ] Re-measure the benchmark cells and compare them with the recorded baseline."
m ""

STATUS=clean
CODE=0
if [[ $PATCH_FAILED -eq 1 ]]; then
	STATUS=patches
	CODE=10
elif [[ $BUILD_FAILED -eq 1 ]]; then
	STATUS=build
	CODE=20
elif [[ $CONFORMANCE_FAILED -eq 1 ]]; then
	STATUS=conformance
	CODE=30
fi

case "$STATUS" in
clean) m "**Verdict: a clean bump is possible.** Every patch applies, both modules tidy, build and vet, and the conformance and reported-version gates pass." ;;
patches) m "**Verdict: patches need regeneration** before anything else can be judged." ;;
build) m "**Verdict: the bump does not build.** Upstream changed something the Lakehouse code calls." ;;
conformance) m "**Verdict: the conformance gates need a human** — registry rows and/or a pin site." ;;
esac

commit_branch
finish "$STATUS" "$CODE" "$OPEN_PR"
