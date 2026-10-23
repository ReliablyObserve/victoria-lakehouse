#!/usr/bin/env python3
"""Decide whether a push to main owes a release: is any commit since the last release tag a product change?

Owner rule (2026-10-22): a merge that rebuilds neither the binaries nor the images
does not cut a release. The `[skip release]` marker keeps working; this check makes
the decision automatic for the commits that carry no marker (a Dependabot rebase
resets a title, a human forgets it).

THE RANGE, NOT THE HEAD (#756). The decision is made on the commits `last-tag..HEAD`, not
on the pushed commit alone. Auto Release runs are serialised by a concurrency group, and
GitHub keeps ONE pending run per group: when merges land in a burst, the runs of the middle
merges are cancelled. Deciding on the head commit alone then loses a product merge whenever
a later, non-product merge (docs, CI, a test, release metadata) is the one whose run
completes (#736, 2026-10-23). With the range, every run answers "is anything since the last
release owed a release?", so the lost change is released by the next run that completes,
whatever that run's own commit is. A release is owed when ANY commit in the range:

  * has no `[skip release]` marker in its subject or in the title of the pull request that
    produced it (the title is read, through release_skip.py, only for a commit that would
    otherwise release), AND
  * is a product change per the per-commit rules below (decide()).

Release-metadata commits ("chore: release metadata for vX [skip release]") carry the marker, so
the push of a release's own metadata PR never releases and never loops. The marker of the
HEAD commit does not hide an earlier owed commit. The tag is the highest vMAJOR.MINOR.PATCH
tag (LATEST_TAG, computed by the workflow's version step). If HEAD is that tag's commit or an
ancestor of it (a re-run of an old run, the run right after a release) the range is empty and
nothing is released; a re-run is therefore idempotent. A manual run (workflow_dispatch) is
never skipped, as before. A revert of a product commit is itself a product commit and is
released: an extra release, never a lost one.

If the title of an owed commit cannot be read the decision is: no release, the run turns red
(fail closed, as release_skip.py always did); a maintainer starts the workflow by hand. Another
owed commit in the range still releases.

The per-commit question "is this a product change" is answered by the registry gate's own
classifier, scripts/ci/pr_classify.py (no second classifier here), run on the commit against
its parent. The classifier answers the gate's question (does the change need registry
coverage); a release asks a different one (are the shipped bytes different), so four things
the gate waives are released here:

  * go.mod / go.sum of the root or the lakehouse-traces module, go.work and
    go.work.sum: a Go toolchain bump or a dependency bump rebuilds the binaries
    (the gate calls them toolchain-only / dependency-only);
  * a change of a base-image digest or tag in Dockerfile, Dockerfile.logs or
    Dockerfile.traces, except ADDING a digest to a `FROM name:tag` line that had none;
  * the upstream pins (VL_VERSION_LOGS, VL_COMMIT_TRACES, VT_VERSION) in the env of
    .github/workflows/auto-release.yaml, which override the Dockerfile ARGs of the
    release images;
  * everything the classifier calls a product change.

Digest added vs digest changed. A digest added where none existed pins what the
tag already resolves to: the image bytes are unchanged, so no release (#687). A
digest that changes from one value to another means the tag moved: the image is
rebuilt on different base layers, so it releases. The two are told apart here
because the gate's image_pin_only accepts add, remove and change alike. That the added
digest is the one the tag resolves to is assumed, not verified (no network here).

Not released: docs, CI workflows, tests, scripts, the test stacks' compose files and
Dockerfiles (Dockerfile.datagen, deployment/), tooling locks (pip, npm), Dependabot
bumps of those, release metadata, registry-only changes.

FAIL SAFE TOWARD RELEASING. Every doubt releases: a manual run, a merge commit, an empty diff,
an unreadable file, a classifier that fails, times out or prints something unexpected, no
usable release tag, a tag that is not an ancestor of HEAD, a range of more than MAX_RANGE
commits. A missed release is worse than an extra one. The one fail-closed case is the
unreadable pull request title above, which keeps the contract release_skip.py always had.

The classifier is the one in the PARENT commit (extracted with `git archive`), the
version the pull request's own gate ran with, so a change to the classifier cannot
vote on its own merge.

Env: EVENT_NAME, GITHUB_REF, GITHUB_SHA, LATEST_TAG, GITHUB_TOKEN, GITHUB_REPOSITORY,
GITHUB_OUTPUT, GITHUB_STEP_SUMMARY.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from typing import Callable, NamedTuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # also under `python3 -I`
import release_skip  # noqa: E402

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
# The same pattern as TAG_RE in the workflow's version step: only plain release tags take part.
TAG_RE = re.compile(r"v(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})")
# A range longer than this is a doubt (a long-lost tag, history drift): release.
MAX_RANGE = 100

# The paths the workflow skipped on before this check existed (docs, CI, metadata only).
# Kept as a skip rule so behaviour only grows; none of these paths ships.
LEGACY_SKIP = re.compile(
    r"^(docs/|\.github/|README\.md|CHANGELOG\.md|CONTRIBUTING\.md|SECURITY\.md|CODE_OF_CONDUCT\.md|LICENSE|website/|scripts/ci/"
    r"|charts/victoria-lakehouse/Chart\.yaml|tests/verification/)"
)
# Files whose change rebuilds the binaries: released whatever the classifier says.
BUILD_INPUTS = frozenset({
    "go.mod", "go.sum", "go.work", "go.work.sum",
    "lakehouse-traces/go.mod", "lakehouse-traces/go.sum",
})
# Dockerfiles of the released images (Dockerfile.datagen is a test tool: not here).
RELEASE_DOCKERFILES = frozenset({"Dockerfile", "Dockerfile.logs", "Dockerfile.traces"})
AUTO_RELEASE = ".github/workflows/auto-release.yaml"
PIN_LINE = re.compile(r"^\s*(VL_VERSION_LOGS|VL_COMMIT_TRACES|VT_VERSION):")
# FROM [--platform=..] name:tag[@sha256:<64 hex>] [AS stage]; tag required, ASCII only.
FROM_LINE = re.compile(
    r"^(?P<pre>FROM(?: --platform=[!-~]+)? )(?P<ref>[a-z0-9][a-z0-9._/-]*:[A-Za-z0-9_][A-Za-z0-9._-]*)"
    r"(?:@(?P<digest>sha256:[0-9a-f]{64}))?(?P<post>(?: AS [!-~]+)?)$"
)
CLASSIFY_TIMEOUT = 120


class Verdict(NamedTuple):
    skip: bool
    reason: str
    failed: bool = False  # fail closed: skip, and the step must turn the run red


def release(reason: str) -> Verdict:
    return Verdict(False, reason)


def skip(reason: str) -> Verdict:
    return Verdict(True, reason)


def git(repo: str, *args: str) -> bytes:
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, timeout=60).stdout


def show_text(repo: str, rev: str, path: str) -> str | None:
    """The file at rev as text; None when it does not exist there or is not valid UTF-8."""
    try:
        return git(repo, "show", f"{rev}:{path}").decode("utf-8")
    except (subprocess.CalledProcessError, UnicodeDecodeError):
        return None


def digest_added_only(base: str, head: str) -> bool:
    """True when head differs from base only by `@sha256:<digest>` ADDED to FROM lines that had none.

    Compared by position (same line count). Every differing pair is a FROM line in both revisions
    with the same platform, image name, tag and stage; the base line has no digest, the head line
    has one. Changing, removing or adding to a line that already had a digest is False, and so is
    any other edit, a heredoc or an `escape=` parser directive anywhere in the file.
    """
    for text in (base, head):
        if "<<" in text or re.search(r"^#[ \t]*escape[ \t]*=", text, re.M | re.I):
            return False
    bl, hl = base.split("\n"), head.split("\n")
    if len(bl) != len(hl):
        return False
    added = False
    for b, h in zip(bl, hl):
        if b == h:
            continue
        bm, hm = FROM_LINE.match(b), FROM_LINE.match(h)
        if not bm or not hm or bm.group("digest") is not None or hm.group("digest") is None:
            return False
        if any(bm.group(g) != hm.group(g) for g in ("pre", "ref", "post")):
            return False
        added = True
    return added


def pins_changed(base: str, head: str) -> bool:
    """The upstream pin lines of auto-release.yaml's env differ between the two revisions."""
    pins = lambda t: sorted(line.strip() for line in t.split("\n") if PIN_LINE.match(line))  # noqa: E731
    return pins(base) != pins(head)


def run_classifier(repo: str, base: str, head: str, classifier: str | None = None) -> dict[str, str]:
    """pr_classify.py's key=value output for base..head, from the parent's copy of the script.

    Raises on a non-zero exit, a timeout, or output without product=0|1.
    """
    with tempfile.TemporaryDirectory(prefix="release-classify-") as tmp:
        script = classifier
        if script is None:
            tar = subprocess.run(["git", "archive", "--format=tar", base, "scripts/ci"], cwd=repo, check=True,
                                 capture_output=True, timeout=60).stdout
            subprocess.run(["tar", "-x", "-C", tmp, "-f", "-"], input=tar, check=True, capture_output=True, timeout=60)
            script = os.path.join(tmp, "scripts", "ci", "pr_classify.py")
        out = subprocess.run([sys.executable, "-I", script, "--base", base, "--head", head], cwd=repo, check=True,
                             capture_output=True, text=True, timeout=CLASSIFY_TIMEOUT).stdout
    kv: dict[str, str] = {}
    for line in out.split("\n"):
        if "=" in line:
            k, _, v = line.partition("=")
            kv.setdefault(k, v)
    if kv.get("product") not in ("0", "1"):
        raise RuntimeError("the classifier printed no product=0|1 line")
    return kv


def decide(repo: str, sha: str, classifier: str | None = None,
           classify: Callable[[str, str, str, str | None], dict[str, str]] = run_classifier) -> Verdict:
    """Does the single commit `sha` (against its parent) change anything that ships?"""
    try:
        if not SHA_RE.match(sha):
            return release(f"no usable commit SHA ({sha!r})")
        parents = git(repo, "rev-list", "--parents", "-n", "1", sha).decode().split()[1:]
        if len(parents) != 1:
            return release(f"the commit has {len(parents)} parents, not one")
        parent = parents[0]
        files = [f for f in git(repo, "diff", "--no-renames", "--name-only", "-z", parent, sha).decode("utf-8").split("\0") if f]
        if not files:
            return release("empty diff")

        built = sorted(f for f in files if f in BUILD_INPUTS)
        if built:
            return release("the Go module or toolchain files change the built binaries: " + ", ".join(built))
        for f in sorted(RELEASE_DOCKERFILES.intersection(files)):
            b, h = show_text(repo, parent, f), show_text(repo, sha, f)
            if b is None or h is None or not digest_added_only(b, h):
                return release(f"{f} changes the built image (anything but a digest added to a FROM line that had none)")
        if AUTO_RELEASE in files:
            b, h = show_text(repo, parent, AUTO_RELEASE), show_text(repo, sha, AUTO_RELEASE)
            if b is None or h is None or pins_changed(b, h):
                return release("the upstream pins in auto-release.yaml change the release builds")

        if all(LEGACY_SKIP.match(f) for f in files):
            return skip("only docs, CI, metadata or release-tooling files changed")

        kv = classify(repo, parent, sha, classifier)
        if kv["product"] == "1":
            return release("product change: " + kv.get("reason", ""))
        detail = kv.get("detail") or "no product change"
        return skip(f"the registry-gate classifier finds no product change ({detail})")
    except Exception as exc:  # fail safe toward releasing
        return release(f"cannot decide ({type(exc).__name__}: {exc}); releasing")


def is_ancestor(repo: str, a: str, b: str) -> bool:
    """True when commit a is b or an ancestor of b."""
    return subprocess.run(["git", "merge-base", "--is-ancestor", a, b], cwd=repo, capture_output=True, timeout=60).returncode == 0


def decide_range(repo: str, head: str, tag: str, event: str = "push", ref: str = release_skip.MAIN_REF,
                 titles_for: Callable[[str], Callable[[str | None], list[str]]] | None = None,
                 classifier: str | None = None,
                 classify: Callable[[str, str, str, str | None], dict[str, str]] = run_classifier) -> Verdict:
    """The release decision for `head` given the last release tag `tag`: is any commit in tag..head owed a release?

    `titles_for(sha)` returns the lookup of the titles of the pull request(s) that produced `sha`
    (release_skip.github_titles); it is called only for a commit that would otherwise release.
    """
    if event == "workflow_dispatch":
        return release("manual run: never skipped")
    if ref != release_skip.MAIN_REF:
        return Verdict(True, f"refusing to release from {ref or 'an unknown ref'}: releases are cut from main only", True)
    try:
        if not SHA_RE.match(head):
            return release(f"no usable commit SHA ({head!r})")
        if not TAG_RE.fullmatch(tag):
            return release(f"no release tag to start from ({tag!r})")
        tag_sha = git(repo, "rev-parse", "--verify", "-q", f"refs/tags/{tag}^{{commit}}").decode().strip()
        if is_ancestor(repo, head, tag_sha):
            return skip(f"{tag} already contains this commit: nothing since the last release")
        if not is_ancestor(repo, tag_sha, head):
            return release(f"{tag} is not an ancestor of this commit (release-history drift)")
        commits = git(repo, "rev-list", "--reverse", f"{tag_sha}..{head}").decode().split()
        if len(commits) > MAX_RANGE:
            return release(f"{len(commits)} commits since {tag}, more than {MAX_RANGE}: releasing")
        unreadable: Verdict | None = None
        for sha in commits:
            message = git(repo, "log", "-n", "1", "--format=%B", sha).decode("utf-8", "replace")
            subject = message.splitlines()[0] if message.strip() else ""
            if release_skip.MARKER in subject:
                continue
            v = decide(repo, sha, classifier, classify)
            if v.skip:
                continue
            # owed unless the title of its pull request carries the marker (the subject of a squash
            # commit of a one-commit PR is the commit subject, not the PR title)
            if titles_for is None:
                return release(f"{sha[:12]} is owed a release: {v.reason}")
            d = release_skip.decide(message, titles_for(sha), "push", release_skip.MAIN_REF)
            if d.failed:
                unreadable = unreadable or Verdict(True, f"{sha[:12]}: {d.reason}", True)
            elif not d.skip:
                return release(f"{sha[:12]} since {tag} is owed a release: {v.reason}")
        if unreadable is not None:
            return unreadable
        return skip(f"none of the {len(commits)} commit(s) since {tag} is a product change without a [skip release] marker")
    except Exception as exc:  # fail safe toward releasing
        return release(f"cannot decide ({type(exc).__name__}: {exc}); releasing")


def main() -> int:
    env = os.environ
    repo, head = os.getcwd(), env.get("GITHUB_SHA", "")
    try:
        v = decide_range(repo, head, env.get("LATEST_TAG", ""), env.get("EVENT_NAME", "push"), env.get("GITHUB_REF", release_skip.MAIN_REF),
                         lambda sha: release_skip.github_titles(env.get("GITHUB_REPOSITORY", ""), env.get("GITHUB_TOKEN", ""), sha))
    except Exception as exc:  # decide_range already catches; belt and braces: a doubt releases
        v = release(f"cannot decide ({type(exc).__name__}: {exc}); releasing")
    verdict = "skip (no release owed)" if v.skip else "release"
    print(f"release range check: {verdict}: {v.reason}")
    if v.failed or (not v.skip and "cannot decide" in v.reason):
        print(f"::warning::Auto Release: {v.reason}")
    for path_key, text in (("GITHUB_OUTPUT", f"skip={str(v.skip).lower()}\n"),
                           ("GITHUB_STEP_SUMMARY", f"### Auto Release decision\n\n**{verdict}**: {v.reason}\n\n")):
        if env.get(path_key):
            with open(env[path_key], "a", encoding="utf-8") as fh:
                fh.write(text)
    return 1 if v.failed else 0


if __name__ == "__main__":
    sys.exit(main())
