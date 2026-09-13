#!/usr/bin/env bash
# Publish an upstream sync probe result: push the probe's branch and open, or
# update, the ONE pull request for that version pair.
#
# Kept out of the workflow YAML so the rules that can destroy work are tested
# (scripts/ci/tests/upstream_sync_publish_test.sh):
#
#   * no token, no push. The pull request must be created with
#     UPSTREAM_SYNC_TOKEN (a fine-grained token scoped to this repository), never
#     with GITHUB_TOKEN — the repository keeps "Allow GitHub Actions to create
#     and approve pull requests" off. A missing token is reported in the summary
#     and exits 3, so the scheduled run turns red instead of doing nothing;
#   * the branch belongs to the probe only while the probe is the last to write
#     it. When the remote branch tip is a probe commit, it is replaced (with a
#     lease on exactly the commit just observed); when anyone else has committed
#     on top — a maintainer regenerating patches, say — the branch is left alone
#     and only the pull request body is refreshed, with a note saying so;
#   * a daily run that found nothing new does not push at all, so an unchanged
#     bump does not re-trigger the pull request's CI every morning;
#   * one pull request per version pair: an open one on the branch is edited,
#     never duplicated.
#
# The token is handed to git per command as an HTTP header scoped to
# https://github.com/, never written to a remote URL or a config file, and
# repository hooks are disabled for the push.
#
# Usage: scripts/ci/upstream_sync_publish.sh --clone <dir> --branch <name>
#          --base <branch> --matrix <file> --title <text>
#          [--summary <file>] [--remote-url <url>]
#   GH_TOKEN   the sync token (required); GH_REPO  owner/name (default for the remote)
# Exit: 0 published, 3 no token, 1 failure, 2 bad invocation.
set -euo pipefail

PROG=upstream_sync_publish

CLONE=""
BRANCH=""
BASE=""
MATRIX=""
TITLE=""
SUMMARY=""
REMOTE_URL=""

SYNC_GIT_EMAIL="${SYNC_GIT_EMAIL:-github-actions[bot]@users.noreply.github.com}"
PROBE_SUBJECT_PREFIX="deps: probe upstream sync "
BODY_LIMIT=60000

die() {
	printf '%s: %s\n' "$PROG" "$1" >&2
	exit "${2:-1}"
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--clone)
		CLONE="${2:-}"
		shift 2
		;;
	--branch)
		BRANCH="${2:-}"
		shift 2
		;;
	--base)
		BASE="${2:-}"
		shift 2
		;;
	--matrix)
		MATRIX="${2:-}"
		shift 2
		;;
	--title)
		TITLE="${2:-}"
		shift 2
		;;
	--summary)
		SUMMARY="${2:-}"
		shift 2
		;;
	--remote-url)
		REMOTE_URL="${2:-}"
		shift 2
		;;
	*) die "unknown argument: $1" 2 ;;
	esac
done

[[ -n "$CLONE" && -n "$BRANCH" && -n "$BASE" && -n "$MATRIX" && -n "$TITLE" ]] ||
	die "--clone, --branch, --base, --matrix and --title are required" 2
[[ -d "$CLONE/.git" ]] || die "not a git clone: $CLONE" 2
[[ -f "$MATRIX" ]] || die "no result matrix at $MATRIX" 2

summary() {
	if [[ -n "$SUMMARY" ]]; then
		printf '%s\n' "$*" >> "$SUMMARY"
	fi
	printf '%s\n' "$*"
}

if [[ -z "${GH_TOKEN:-}" ]]; then
	summary ""
	summary "### No sync pull request was opened"
	summary ""
	summary "The probe found an upstream release worth probing and its matrix is complete, but the"
	summary "repository secret \`UPSTREAM_SYNC_TOKEN\` is not set, so nothing was pushed."
	summary ""
	summary "Grant it as a fine-grained personal access token limited to this repository with"
	summary "**Contents: read and write** and **Pull requests: read and write**, or as a GitHub App"
	summary "installation token with the same two permissions — see \`docs/upstream-sync.md\`."
	exit 3
fi

if [[ -z "$REMOTE_URL" ]]; then
	[[ -n "${GH_REPO:-}" ]] || die "GH_REPO is required when --remote-url is not given" 2
	REMOTE_URL="https://github.com/${GH_REPO}.git"
fi

# rgit runs git in the clone with the token as a header for github.com only, and
# with hooks disabled: the clone was produced by a job that compiled and ran
# third-party code, and nothing in it may run with the token in reach.
rgit() {
	local header=""
	header="AUTHORIZATION: basic $(printf 'x-access-token:%s' "$GH_TOKEN" | base64 | tr -d '\n')"
	git -C "$CLONE" -c core.hooksPath=/dev/null \
		-c "http.https://github.com/.extraheader=$header" "$@"
}

remote_sha="$(rgit ls-remote "$REMOTE_URL" "refs/heads/$BRANCH" | awk '{print $1}' | head -1)"
local_sha="$(git -C "$CLONE" rev-parse HEAD)"

pushed=0
takeover=0
if [[ -z "$remote_sha" ]]; then
	rgit push --quiet "$REMOTE_URL" "HEAD:refs/heads/$BRANCH"
	pushed=1
else
	rgit fetch --quiet "$REMOTE_URL" "refs/heads/$BRANCH"
	tip_email="$(git -C "$CLONE" log -1 --format=%ce FETCH_HEAD)"
	tip_subject="$(git -C "$CLONE" log -1 --format=%s FETCH_HEAD)"
	if [[ "$tip_email" != "$SYNC_GIT_EMAIL" || "$tip_subject" != "$PROBE_SUBJECT_PREFIX"* ]]; then
		takeover=1
	elif [[ "$(git -C "$CLONE" rev-parse "FETCH_HEAD^{tree}")" == "$(git -C "$CLONE" rev-parse "HEAD^{tree}")" &&
		"$(git -C "$CLONE" rev-parse "FETCH_HEAD^")" == "$(git -C "$CLONE" rev-parse "HEAD^")" ]]; then
		: # same content on the same base: nothing to push
	else
		rgit push --quiet --force-with-lease="refs/heads/$BRANCH:$remote_sha" \
			"$REMOTE_URL" "HEAD:refs/heads/$BRANCH"
		pushed=1
	fi
fi

# The pull request body: the matrix, capped below GitHub's 65536-character
# limit, with the takeover note first when the branch is no longer the probe's.
body="$(mktemp)"
trap 'rm -f "$body"' EXIT
if [[ $takeover -eq 1 ]]; then
	{
		printf '> **The probe no longer rewrites this branch.** Its tip is a commit the probe did not make, so\n'
		printf '> the branch is left exactly as it is. The matrix below is from a fresh probe of the same\n'
		printf '> releases against %s and describes that, not this branch.\n\n' "\`$BASE\`"
	} > "$body"
else
	: > "$body"
fi
head -c "$BODY_LIMIT" "$MATRIX" >> "$body"
if [[ "$(wc -c < "$MATRIX" | tr -d ' ')" -gt "$BODY_LIMIT" ]]; then
	printf '\n\n_Truncated — the complete matrix is the %s artifact of this run._\n' "\`upstream-sync-matrix\`" >> "$body"
fi

number="$(gh pr list --head "$BRANCH" --state open --json number --jq '.[0].number // empty')"
if [[ -n "$number" ]]; then
	gh pr edit "$number" --title "$TITLE" --body-file "$body" > /dev/null
	action="updated"
else
	url="$(gh pr create --base "$BASE" --head "$BRANCH" --title "$TITLE" --body-file "$body")"
	number="${url##*/}"
	action="opened"
fi

# Labels are best-effort and applied one at a time: a label that does not exist
# yet (the token cannot create one) must neither cost the pull request nor keep
# the other label off it.
for label in dependencies upstream-sync; do
	if ! gh pr edit "$number" --add-label "$label" > /dev/null 2>&1; then
		summary "note: label \`$label\` could not be applied to #$number — create it once with \`gh label create $label\`"
	fi
done

if [[ $takeover -eq 1 ]]; then
	branch_note="left untouched: its tip ($remote_sha) is not a probe commit"
elif [[ $pushed -eq 1 ]]; then
	branch_note="pushed $local_sha"
else
	branch_note="unchanged since the last probe, not pushed"
fi
summary ""
summary "Sync pull request #$number $action; branch \`$BRANCH\` $branch_note."
