#!/usr/bin/env bash
# Self-test for scripts/ci/loki_vl_proxy_bump.sh against a local fake checkout
# (no network: --latest stands in for the releases API).
#
# Cases:
#   1  latest == pin                 -> up-to-date, open_pr=false, no branch
#   2  latest older than pin         -> up-to-date (numeric compare 2.10.0 vs 2.9.0)
#   3  newer release                 -> bump-available, open_pr=true, probe commit, only the pin changed
#   4  newer minor 2.10.0 vs 2.9.0   -> numeric, not textual, compare
#   5  --no-pr                       -> open_pr=false
#   6  malformed latest / no pin     -> non-zero
#   7  bad invocation                -> exit 2
#
# Usage: scripts/ci/tests/loki_vl_proxy_bump_test.sh
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1
BUMP="$(pwd)/scripts/ci/loki_vl_proxy_bump.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
pass=0 fail=0
check() { # check <desc> <actual> <expected>
	if [[ "$2" == "$3" ]]; then pass=$((pass + 1)); return; fi
	fail=$((fail + 1))
	printf 'FAIL: %s\n  expected: %s\n  actual:   %s\n' "$1" "$3" "$2" >&2
}
g() { git -c commit.gpgsign=false -c init.defaultBranch=main -c user.name=t -c user.email=t@example.invalid "$@"; }

mkrepo() { # mkrepo <pin>
	rm -rf "$TMP/repo"
	mkdir -p "$TMP/repo/deployment/docker"
	printf 'FROM alpine:3.21\n# note ARG VERSION=9.9.9 in a comment is ignored\nARG VERSION=%s\nARG TARGETARCH=amd64\n' "$1" > "$TMP/repo/deployment/docker/Dockerfile.loki-vl-proxy"
	g -C "$TMP/repo" init -q
	g -C "$TMP/repo" add -A
	g -C "$TMP/repo" commit -qm base
}
fact() { sed -n "s/^$1=//p" "$TMP/facts.env"; }
run() { "$BUMP" --repo "$TMP/repo" --workdir "$TMP/work" --out "$TMP/out.md" --env-out "$TMP/facts.env" "$@" > "$TMP/log" 2>&1; echo $?; }

mkrepo 2.3.0
check "1 rc" "$(run --latest 2.3.0)" 0
check "1 status" "$(fact status)" up-to-date
check "1 open_pr" "$(fact open_pr)" false
check "1 no clone" "$([[ -d $TMP/work/repo ]] && echo yes || echo no)" no

mkrepo 2.10.0
check "2 rc" "$(run --latest 2.9.0)" 0
check "2 status" "$(fact status)" up-to-date

mkrepo 2.3.0
check "3 rc" "$(run --latest 2.4.1)" 0
check "3 status" "$(fact status)" bump-available
check "3 open_pr" "$(fact open_pr)" true
check "3 branch" "$(fact branch)" deps/loki-vl-proxy-latest
check "3 title" "$(fact title)" "deps: loki-vl-proxy v2.4.1 for the e2e/proof stack [skip release]"
check "3 subject" "$(g -C "$TMP/work/repo" log -1 --format=%s)" "deps: probe upstream sync loki-vl-proxy v2.4.1"
check "3 email" "$(g -C "$TMP/work/repo" log -1 --format=%ce)" "github-actions[bot]@users.noreply.github.com"
check "3 pin" "$(sed -n 's/^ARG VERSION=//p' "$TMP/work/repo/deployment/docker/Dockerfile.loki-vl-proxy")" 2.4.1
check "3 only pin changed" "$(g -C "$TMP/work/repo" diff --stat HEAD~1 | tail -1 | tr -s ' ')" " 1 file changed, 1 insertion(+), 1 deletion(-)"
check "3 comment kept" "$(grep -c '9.9.9' "$TMP/work/repo/deployment/docker/Dockerfile.loki-vl-proxy")" 1
check "3 body names jump" "$(grep -c 'v2.3.0 -> v2.4.1' "$TMP/out.md")" 1

mkrepo 2.9.0
check "4 rc" "$(run --latest 2.10.0)" 0
check "4 status" "$(fact status)" bump-available

mkrepo 2.3.0
check "5 rc" "$(run --latest 2.4.0 --no-pr)" 0
check "5 open_pr" "$(fact open_pr)" false

check "6 malformed" "$(run --latest v2.x)" 1
rm -rf "$TMP/repo/deployment/docker/Dockerfile.loki-vl-proxy"
printf 'FROM alpine\n' > "$TMP/repo/deployment/docker/Dockerfile.loki-vl-proxy"
check "6 no pin" "$(run --latest 2.4.0)" 1

check "7 bad use" "$("$BUMP" --bogus >/dev/null 2>&1; echo $?)" 2

printf 'loki_vl_proxy_bump_test: %d passed, %d failed\n' "$pass" "$fail"
[[ $fail -eq 0 ]]
