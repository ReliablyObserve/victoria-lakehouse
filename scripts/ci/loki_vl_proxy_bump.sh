#!/usr/bin/env bash
# Daily loki-vl-proxy bump: find the newest loki-vl-proxy release and, when it is
# newer than the pin in deployment/docker/Dockerfile.loki-vl-proxy, prepare a
# one-line bump on a throwaway clone with a probe commit that
# scripts/ci/upstream_sync_publish.sh then pushes and turns into the ONE open
# pull request (same token rules, lease and takeover handling as the upstream
# sync probe; it is reused unchanged).
#
# The proxy is used only by the e2e / proof / benchmark compose stacks and is
# never shipped, so the pull request title carries [skip release].
#
# Usage: scripts/ci/loki_vl_proxy_bump.sh --repo <checkout> --workdir <dir>
#          --out <matrix.md> --env-out <facts.env> [--latest <X.Y.Z>] [--no-pr]
#   --latest   skip the releases API and use this version (self-test, manual run)
# Facts written to --env-out (key=value): status, current, latest, open_pr,
#   branch, title.
# Exit: 0 ok (up to date or bump prepared), 1 the probe could not run, 2 bad use.
set -euo pipefail

PROG=loki_vl_proxy_bump
DOCKERFILE="deployment/docker/Dockerfile.loki-vl-proxy"
SLUG="ReliablyObserve/loki-vl-proxy"
BRANCH="deps/loki-vl-proxy-latest"
SYNC_GIT_EMAIL="${SYNC_GIT_EMAIL:-github-actions[bot]@users.noreply.github.com}"
SYNC_GIT_NAME="${SYNC_GIT_NAME:-github-actions[bot]}"

REPO="" WORKDIR="" OUT="" ENV_OUT="" LATEST="" NO_PR=0

die() {
	printf '%s: %s\n' "$PROG" "$1" >&2
	exit "${2:-1}"
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--repo) REPO="${2:-}"; shift 2 ;;
	--workdir) WORKDIR="${2:-}"; shift 2 ;;
	--out) OUT="${2:-}"; shift 2 ;;
	--env-out) ENV_OUT="${2:-}"; shift 2 ;;
	--latest) LATEST="${2:-}"; shift 2 ;;
	--no-pr) NO_PR=1; shift ;;
	*) die "unknown argument: $1" 2 ;;
	esac
done
[[ -n "$REPO" && -n "$WORKDIR" && -n "$OUT" && -n "$ENV_OUT" ]] ||
	die "--repo, --workdir, --out and --env-out are required" 2
[[ -f "$REPO/$DOCKERFILE" ]] || die "no $DOCKERFILE in $REPO" 2

# version_gt A B: true when A is a strictly higher X.Y.Z than B (numeric).
version_gt() {
	local a="$1" b="$2"
	[[ "$a" != "$b" ]] && [[ "$(printf '%s\n%s\n' "$a" "$b" | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)" == "$a" ]]
}

current="$(sed -n 's/^ARG VERSION=\([0-9][0-9.]*\)$/\1/p' "$REPO/$DOCKERFILE" | head -1)"
[[ -n "$current" ]] || die "cannot read the pinned ARG VERSION from $DOCKERFILE"

if [[ -z "$LATEST" ]]; then
	tag="$(gh release view -R "$SLUG" --json tagName --jq .tagName)" ||
		die "cannot read the latest $SLUG release"
	LATEST="${tag#v}"
fi
[[ "$LATEST" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "latest release is not X.Y.Z: $LATEST"

emit() { # emit <status> <open_pr>
	{
		printf 'status=%s\ncurrent=%s\nlatest=%s\nopen_pr=%s\nbranch=%s\n' "$1" "$current" "$LATEST" "$2" "$BRANCH"
		printf 'title=deps: loki-vl-proxy v%s for the e2e/proof stack [skip release]\n' "$LATEST"
	} > "$ENV_OUT"
}

if ! version_gt "$LATEST" "$current"; then
	printf 'loki-vl-proxy is up to date: pinned v%s, latest v%s.\n' "$current" "$LATEST" > "$OUT"
	emit up-to-date false
	cat "$OUT"
	exit 0
fi

rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"
clone="$WORKDIR/repo"
git clone --quiet --local "$REPO" "$clone"
git -C "$clone" checkout --quiet -B "$BRANCH"
tmp="$clone/$DOCKERFILE.new"
sed "s/^ARG VERSION=.*/ARG VERSION=$LATEST/" "$clone/$DOCKERFILE" > "$tmp"
mv "$tmp" "$clone/$DOCKERFILE"
git -C "$clone" diff --quiet && die "the pin was not rewritten"

# The commit subject must start with the prefix upstream_sync_publish.sh
# recognises as a probe commit, so a maintainer's follow-up commit on the branch
# is never overwritten.
GIT_COMMITTER_NAME="$SYNC_GIT_NAME" GIT_COMMITTER_EMAIL="$SYNC_GIT_EMAIL" \
	GIT_AUTHOR_NAME="$SYNC_GIT_NAME" GIT_AUTHOR_EMAIL="$SYNC_GIT_EMAIL" \
	git -C "$clone" -c commit.gpgsign=false -c core.hooksPath=/dev/null \
	commit --quiet -am "deps: probe upstream sync loki-vl-proxy v$LATEST"

{
	printf '## loki-vl-proxy bump\n\n'
	printf 'Pin `ARG VERSION` in `%s`: **v%s -> v%s**.\n\n' "$DOCKERFILE" "$current" "$LATEST"
	printf 'Release notes: https://github.com/%s/releases/tag/v%s (all releases since v%s: https://github.com/%s/releases)\n\n' "$SLUG" "$LATEST" "$current" "$SLUG"
	printf 'The proxy is used only by the e2e, proof and benchmark compose stacks and is not shipped, so this is a `[skip release]` change.\n\n'
	printf 'Opened by the daily bump job and never auto-merged. Before merging: read the release notes for breaking changes,\n'
	printf 'confirm every flag the compose files pass (`deployment/docker/docker-compose-e2e.yml`, `docker-compose-benchmark.yml`) still exists, and let the e2e job exercise the Loki-facing cases.\n'
} > "$OUT"

if [[ $NO_PR -eq 1 ]]; then emit bump-available false; else emit bump-available true; fi
cat "$OUT"
