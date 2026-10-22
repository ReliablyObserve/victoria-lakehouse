#!/usr/bin/env python3
"""Decide whether a push to main changes anything that ships; if not, it is not released.

Owner rule (2026-10-22): a merge that rebuilds neither the binaries nor the images
does not cut a release. The `[skip release]` marker (scripts/ci/release_skip.py)
keeps working as before; this check makes the decision automatic for the pushes
that carry no marker (a Dependabot rebase resets a title, a human forgets it).

The question "is this a product change" is answered by the registry gate's own
classifier, scripts/ci/pr_classify.py (no second classifier here), run on the
pushed commit against its parent. The classifier answers the gate's question (does
the change need registry coverage); a release asks a different one (are the shipped
bytes different), so four things the gate waives are released here:

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

FAIL SAFE TOWARD RELEASING. Every doubt releases: a manual run, a push that is not
exactly one commit on top of the previous main head (a rebase merge or a multi-commit
push would hide the earlier commits), a merge commit, an empty diff, an unreadable
file, a classifier that fails, times out or prints something unexpected. This script
never exits non-zero: a failing step would stop the release job (fail closed), the
opposite of what a doubt must do. A missed release is worse than an extra one.

The classifier is the one in the PARENT commit (extracted with `git archive`), the
version the pull request's own gate ran with, so a change to the classifier cannot
vote on its own merge.

Env: EVENT_NAME, BEFORE_SHA (github.event.before), GITHUB_SHA, GITHUB_OUTPUT,
GITHUB_STEP_SUMMARY.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from typing import Callable, NamedTuple

ZERO_SHA = "0" * 40
SHA_RE = re.compile(r"^[0-9a-f]{40}$")

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


def decide(repo: str, sha: str, before: str = "", event: str = "push", classifier: str | None = None,
           classify: Callable[[str, str, str, str | None], dict[str, str]] = run_classifier) -> Verdict:
    """The release decision for the commit `sha` of a push whose previous head was `before`."""
    if event == "workflow_dispatch":
        return release("manual run: never skipped")
    try:
        if not SHA_RE.match(sha):
            return release(f"no usable commit SHA ({sha!r})")
        parents = git(repo, "rev-list", "--parents", "-n", "1", sha).decode().split()[1:]
        if len(parents) != 1:
            return release(f"the pushed commit has {len(parents)} parents, not one")
        parent = parents[0]
        if before and before != ZERO_SHA and before != parent:
            return release("the push is not exactly one commit on top of the previous head (a rebase merge or a multi-commit push)")
        if before == ZERO_SHA:
            return release("the push created the branch")
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


def main() -> int:
    env = os.environ
    try:
        v = decide(os.getcwd(), env.get("GITHUB_SHA", ""), env.get("BEFORE_SHA", ""), env.get("EVENT_NAME", "push"))
    except Exception as exc:  # decide() already catches; belt and braces: never fail the step
        v = release(f"cannot decide ({type(exc).__name__}: {exc}); releasing")
    verdict = "skip (no product change)" if v.skip else "release"
    print(f"product check: {verdict}: {v.reason}")
    if not v.skip and "cannot decide" in v.reason:
        print(f"::warning::Auto Release product check: {v.reason}")
    for path_key, text in (("GITHUB_OUTPUT", f"skip={str(v.skip).lower()}\n"),
                           ("GITHUB_STEP_SUMMARY", f"### Auto Release product check\n\n**{verdict}**: {v.reason}\n\n")):
        if env.get(path_key):
            with open(env[path_key], "a", encoding="utf-8") as fh:
                fh.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
