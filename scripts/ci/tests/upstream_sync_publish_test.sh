#!/usr/bin/env bash
# Self-test for scripts/ci/upstream_sync_publish.sh. A local bare repository
# stands in for GitHub (so every push is real and inspectable) and `gh` is a
# shim that records the pull-request calls.
#
# Cases:
#   1  no token                          -> exit 3, summary explains, nothing pushed, gh never called
#   2  first publish                     -> branch pushed, pull request created and labelled
#   3  same result again                 -> not pushed, the open pull request is edited, not duplicated
#   4  a new probe result                -> pushed over the previous probe commit
#   5  the base branch moved             -> pushed, even with identical content
#   6  a maintainer committed on top     -> branch left untouched, body carries the note
#   7  an oversized matrix               -> body capped, truncation noted
#   8  a label cannot be applied         -> still published, the other label applied, noted
#   9  repository hooks                  -> never run during the push
#  10  bad invocation                    -> exit 2
#
# Usage: scripts/ci/tests/upstream_sync_publish_test.sh
set -uo pipefail
cd "$(dirname "$0")/../../.." || exit 1

PUBLISH="$(pwd)/scripts/ci/upstream_sync_publish.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0 fail=0

check() { # check <desc> <actual> <expected>
	if [[ "$2" == "$3" ]]; then
		pass=$((pass + 1))
		return
	fi
	fail=$((fail + 1))
	printf 'FAIL: %s\n  expected: %s\n  actual:   %s\n  output:\n%s\n' "$1" "$3" "$2" "${out:-}" >&2
}

check_contains() {
	if [[ "$2" == *"$3"* ]]; then
		pass=$((pass + 1))
		return
	fi
	fail=$((fail + 1))
	printf 'FAIL: %s\n  expected substring: %s\n  actual:\n%s\n' "$1" "$3" "$2" >&2
}

check_not_contains() {
	if [[ "$2" != *"$3"* ]]; then
		pass=$((pass + 1))
		return
	fi
	fail=$((fail + 1))
	printf 'FAIL: %s\n  unexpected substring: %s\n  actual:\n%s\n' "$1" "$3" "$2" >&2
}

BOT_NAME='github-actions[bot]'
BOT_EMAIL='github-actions[bot]@users.noreply.github.com'

# Hermetic git: fixed identities, no signing, whatever the machine's config.
git_as() { # git_as <name> <email> <git args...>
	local name="$1" email="$2"
	shift 2
	GIT_COMMITTER_NAME="$name" GIT_COMMITTER_EMAIL="$email" \
		GIT_AUTHOR_NAME="$name" GIT_AUTHOR_EMAIL="$email" \
		git -c commit.gpgsign=false -c tag.gpgSign=false -c init.defaultBranch=main "$@"
}
git_bot() { git_as "$BOT_NAME" "$BOT_EMAIL" "$@"; }
git_dev() { git_as "Maintainer" "maintainer@example.invalid" "$@"; }

ORIGIN="$TMP/origin.git"
SEED="$TMP/seed"
CLONE="$TMP/clone"
BRANCH="upstream-sync/vl-v1.53.0-vt-v0.12.0"
MATRIX="$TMP/matrix.md"
SUMMARY="$TMP/summary.md"

git_dev init -q --bare "$ORIGIN"
git_dev init -q "$SEED"
printf 'VL_VERSION_LOGS := v1.52.0\n' > "$SEED/Makefile"
git_dev -C "$SEED" add -A
git_dev -C "$SEED" commit -qm "base"
git_dev -C "$SEED" push -q "$ORIGIN" HEAD:refs/heads/main

git_dev clone -q "$ORIGIN" "$CLONE"

# probe_commit <makefile content> — a fresh probe commit on the sync branch,
# built from the clone's origin/main, the way the probe builds it.
probe_commit() {
	git_bot -C "$CLONE" fetch -q origin
	git_bot -C "$CLONE" checkout -q -B "$BRANCH" origin/main
	printf '%s\n' "$1" > "$CLONE/Makefile"
	git_bot -C "$CLONE" add -A
	git_bot -C "$CLONE" commit -qm "deps: probe upstream sync VL v1.53.0 / VT v0.12.0"
}

printf '## Upstream sync probe\n\nVerdict: a clean bump is possible.\n' > "$MATRIX"

BIN="$TMP/bin"
GHSTATE="$TMP/gh"
mkdir -p "$BIN" "$GHSTATE"
cat > "$BIN/gh" <<'EOF'
#!/usr/bin/env bash
# Records every call; keeps one pull request's state in $GHSTATE.
printf '%s\n' "$*" >> "$GHSTATE/calls"
body_from_args() {
	while [[ $# -gt 0 ]]; do
		if [[ "$1" == "--body-file" ]]; then cp "$2" "$GHSTATE/body"; fi
		shift
	done
}
case "$1 $2" in
"pr list")
	if [[ -f "$GHSTATE/open" ]]; then cat "$GHSTATE/open"; fi
	;;
"pr create")
	body_from_args "$@"
	printf '42\n' > "$GHSTATE/open"
	printf 'https://github.com/example/repo/pull/42\n'
	;;
"pr edit")
	if [[ -n "${FAKE_GH_LABEL_FAIL:-}" && "$*" == *"--add-label $FAKE_GH_LABEL_FAIL"* ]]; then
		printf 'could not add label: not found\n' >&2
		exit 1
	fi
	body_from_args "$@"
	;;
*) exit 1 ;;
esac
EOF
chmod +x "$BIN/gh"

origin_sha() { git -C "$ORIGIN" rev-parse --verify --quiet "refs/heads/$BRANCH" || true; }

# publish [extra env...] — runs the script with the shim first on PATH.
publish() {
	: > "$SUMMARY"
	: > "$GHSTATE/calls"
	out="$(env PATH="$BIN:$PATH" GHSTATE="$GHSTATE" "$@" "$PUBLISH" \
		--clone "$CLONE" --branch "$BRANCH" --base main --matrix "$MATRIX" \
		--title "Upstream sync: VictoriaLogs v1.53.0, VictoriaTraces v0.12.0" \
		--summary "$SUMMARY" --remote-url "$ORIGIN" 2>&1)"
	rc=$?
	calls="$(cat "$GHSTATE/calls")"
	summ="$(cat "$SUMMARY")"
}

# --- 1. no token ---------------------------------------------------------
probe_commit "VL_VERSION_LOGS := v1.53.0"
publish GH_TOKEN=
check "no token exits 3" "$rc" 3
check_contains "no token names the secret" "$summ" "UPSTREAM_SYNC_TOKEN"
check_contains "no token names the permissions" "$summ" "Pull requests: read and write"
check "no token pushes nothing" "$(origin_sha)" ""
check "no token never calls gh" "$calls" ""

# --- 2. first publish ------------------------------------------------------
publish GH_TOKEN=test-token
check "first publish exits 0" "$rc" 0
check "first publish pushes the probe commit" "$(origin_sha)" "$(git -C "$CLONE" rev-parse HEAD)"
check_contains "first publish creates the pull request against the base" "$calls" "pr create --base main --head $BRANCH"
check_contains "first publish applies the dependencies label" "$calls" "pr edit 42 --add-label dependencies"
check_contains "first publish applies the upstream-sync label" "$calls" "pr edit 42 --add-label upstream-sync"
check_contains "the body is the matrix" "$(cat "$GHSTATE/body")" "a clean bump is possible"
check_contains "the summary names the pull request" "$summ" "#42 opened"
first_sha="$(origin_sha)"

# --- 3. the same result again ---------------------------------------------
# A new commit object with the same tree on the same base: nothing changed.
sleep 1
probe_commit "VL_VERSION_LOGS := v1.53.0"
check_not_contains "the fixture really made a new commit" "$(git -C "$CLONE" rev-parse HEAD)" "$first_sha"
publish GH_TOKEN=test-token
check "an unchanged result exits 0" "$rc" 0
check "an unchanged result is not pushed" "$(origin_sha)" "$first_sha"
check_contains "an unchanged result edits the open pull request" "$calls" "pr edit 42 --title"
check_not_contains "an unchanged result never creates a second pull request" "$calls" "pr create"
check_contains "the summary says it was not pushed" "$summ" "not pushed"

# --- 4. a new probe result -------------------------------------------------
probe_commit "VL_VERSION_LOGS := v1.53.0
VL_COMMIT_TRACES := 0123456789ab"
publish GH_TOKEN=test-token
check "a new result exits 0" "$rc" 0
check "a new result replaces the previous probe commit" "$(origin_sha)" "$(git -C "$CLONE" rev-parse HEAD)"
check_contains "a new result edits, not creates" "$calls" "pr edit 42 --title"
check_not_contains "a new result never creates a second pull request" "$calls" "pr create"

# --- 5. the base branch moved ----------------------------------------------
# An empty commit on the base: the probe's tree is byte-identical to the one
# already pushed, and only the parent differs — which must still be pushed, or
# the pull request keeps diffing against a stale base.
git_dev -C "$SEED" commit -q --allow-empty -m "base moves on"
git_dev -C "$SEED" push -q "$ORIGIN" HEAD:refs/heads/main
before="$(origin_sha)"
probe_commit "VL_VERSION_LOGS := v1.53.0
VL_COMMIT_TRACES := 0123456789ab"
check "the fixture keeps the tree identical" "$(git -C "$CLONE" rev-parse "HEAD^{tree}")" "$(git -C "$ORIGIN" rev-parse "$before^{tree}")"
publish GH_TOKEN=test-token
check "a moved base exits 0" "$rc" 0
check_not_contains "a moved base is pushed even with identical files" "$(origin_sha)" "$before"
check "a moved base lands the fresh commit" "$(origin_sha)" "$(git -C "$CLONE" rev-parse HEAD)"

# A base that moves with real changes alters the tree as well: pushed too.
printf 'README\n' > "$SEED/README"
git_dev -C "$SEED" add -A
git_dev -C "$SEED" commit -qm "base moves on with a file"
git_dev -C "$SEED" push -q "$ORIGIN" HEAD:refs/heads/main
before="$(origin_sha)"
probe_commit "VL_VERSION_LOGS := v1.53.0
VL_COMMIT_TRACES := 0123456789ab"
publish GH_TOKEN=test-token
check "a base moved with files lands the fresh commit" "$(origin_sha)" "$(git -C "$CLONE" rev-parse HEAD)"

# --- 6. a maintainer committed on top --------------------------------------
DEV="$TMP/dev"
git_dev clone -q --branch "$BRANCH" "$ORIGIN" "$DEV"
printf 'regenerated\n' > "$DEV/vlstorage-dispatch.patch"
git_dev -C "$DEV" add -A
git_dev -C "$DEV" commit -qm "patches: regenerate against v1.53.0"
git_dev -C "$DEV" push -q origin "HEAD:refs/heads/$BRANCH"
maintainer_sha="$(origin_sha)"
probe_commit "VL_VERSION_LOGS := v1.53.0
VL_COMMIT_TRACES := ba9876543210"
publish GH_TOKEN=test-token
check "a maintained branch exits 0" "$rc" 0
check "a maintained branch is left untouched" "$(origin_sha)" "$maintainer_sha"
check_contains "the body says the probe no longer rewrites the branch" "$(cat "$GHSTATE/body")" "The probe no longer rewrites this branch"
check_contains "the body still carries the fresh matrix" "$(cat "$GHSTATE/body")" "a clean bump is possible"
check_contains "the summary says the branch was left alone" "$summ" "left untouched"

# A probe commit amended by someone else is not the probe's either.
git_dev -C "$DEV" fetch -q origin
git_dev -C "$DEV" reset -q --hard "origin/$BRANCH"
git_dev -C "$DEV" commit -q --amend -m "deps: probe upstream sync VL v1.53.0 / VT v0.12.0"
git_dev -C "$DEV" push -q --force origin "HEAD:refs/heads/$BRANCH"
amended_sha="$(origin_sha)"
publish GH_TOKEN=test-token
check "a probe subject committed by someone else is not overwritten" "$(origin_sha)" "$amended_sha"

# --- 7. an oversized matrix ------------------------------------------------
# Hand the branch back to the probe first.
git_bot -C "$DEV" reset -q --hard "$first_sha"
git_bot -C "$DEV" push -q --force origin "HEAD:refs/heads/$BRANCH"
awk 'BEGIN { for (i = 0; i < 2000; i++) printf "| row %05d | %s |\n", i, "0123456789012345678901234567890123456789" }' > "$MATRIX"
publish GH_TOKEN=test-token
check "an oversized matrix exits 0" "$rc" 0
body_size="$(wc -c < "$GHSTATE/body" | tr -d ' ')"
if [[ "$body_size" -le 60200 ]]; then pass=$((pass + 1)); else
	fail=$((fail + 1))
	printf 'FAIL: the body is capped (got %s bytes)\n' "$body_size" >&2
fi
check_contains "the cap is noted in the body" "$(cat "$GHSTATE/body")" "Truncated"
printf '## Upstream sync probe\n\nVerdict: a clean bump is possible.\n' > "$MATRIX"

# --- 8. a label cannot be applied ------------------------------------------
publish GH_TOKEN=test-token FAKE_GH_LABEL_FAIL=upstream-sync
check "a label failure still publishes" "$rc" 0
check_contains "the missing label is named" "$summ" "label \`upstream-sync\` could not be applied"
check_not_contains "the other label is not reported" "$summ" "label \`dependencies\` could not"
check_contains "the other label is still applied" "$calls" "pr edit 42 --add-label dependencies"

# --- 9. repository hooks never run -----------------------------------------
mkdir -p "$CLONE/.git/hooks"
printf '#!/bin/sh\ntouch "%s/hook-ran"\n' "$TMP" > "$CLONE/.git/hooks/pre-push"
chmod +x "$CLONE/.git/hooks/pre-push"
probe_commit "VL_VERSION_LOGS := v1.53.1"
publish GH_TOKEN=test-token
check "a publish with a hook present exits 0" "$rc" 0
check "the push happened" "$(origin_sha)" "$(git -C "$CLONE" rev-parse HEAD)"
if [[ -e "$TMP/hook-ran" ]]; then
	fail=$((fail + 1))
	printf 'FAIL: the pre-push hook ran during the publish\n' >&2
else
	pass=$((pass + 1))
fi

# --- 10. bad invocation ----------------------------------------------------
out="$(env PATH="$BIN:$PATH" GH_TOKEN=x "$PUBLISH" --clone "$CLONE" 2>&1)"
rc=$?
check "missing arguments exit 2" "$rc" 2
out="$(env PATH="$BIN:$PATH" GH_TOKEN=x "$PUBLISH" --bogus 2>&1)"
rc=$?
check "an unknown argument exits 2" "$rc" 2

printf '%d checks passed, %d failed\n' "$pass" "$fail"
[[ $fail -eq 0 ]]
