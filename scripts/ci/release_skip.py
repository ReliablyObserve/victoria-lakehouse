#!/usr/bin/env python3
"""Decide whether a push to main must NOT cut a release.

A push is skipped when the head commit's subject contains `[skip release]`, or
when the pull request(s) that produced the pushed commit have it in their TITLE:
GitHub's default squash subject for a one-commit PR is the commit subject, not
the PR title, so a marker in the title alone is lost from the commit. The PR is
found from the pushed SHA (GET /commits/{sha}/pulls), so squash, merge and
rebase merges and backports whose subject quotes another PR all look up the
right one. A job re-run re-reads the title, so editing it and re-running works.

Fails closed and loudly. When the PR title cannot be read after 3 attempts the
push is NOT released: the decision is written to the run summary, an annotation
is printed, `skip=true` is written, and the step exits 1 so the run turns red;
a maintainer then starts the workflow by hand (workflow_dispatch).

A manual run (GITHUB_EVENT_NAME=workflow_dispatch) is never skipped, and only
runs on main: any other ref is refused.

Env: COMMIT_MSG, GITHUB_EVENT_NAME, GITHUB_REF, GITHUB_SHA, GITHUB_TOKEN,
GITHUB_REPOSITORY, GITHUB_OUTPUT, GITHUB_STEP_SUMMARY.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.request
from typing import Callable

MARKER = "[skip release]"
SQUASH_SUFFIX = re.compile(r"\(#(\d+)\)\s*$")
MAIN_REF = "refs/heads/main"
ATTEMPTS = 3


class Decision:
    def __init__(self, skip: bool, reason: str, failed: bool = False):
        self.skip, self.reason, self.failed = skip, reason, failed

    def __iter__(self):  # (skip, reason) for callers that only need those
        return iter((self.skip, self.reason))


def decide(message: str, fetch_titles: Callable[[str | None], list[str]], event: str = "push", ref: str = MAIN_REF) -> Decision:
    if ref != MAIN_REF:
        return Decision(True, f"refusing to release from {ref or 'an unknown ref'}: releases are cut from main only", failed=True)
    if event == "workflow_dispatch":
        return Decision(False, "manual run: never skipped")
    first = message.splitlines()[0] if message.strip() else ""
    if MARKER in first:
        return Decision(True, "commit subject contains [skip release]")
    m = SQUASH_SUFFIX.search(first)
    try:
        titles = fetch_titles(m.group(1) if m else None)
    except Exception as exc:  # fail closed
        return Decision(True, (
            f"cannot read the pull request title for the pushed commit ({type(exc).__name__}: {exc}); not releasing. "
            "Run the Auto Release workflow by hand (workflow_dispatch) if this push must release"), failed=True)
    for title in titles:
        if MARKER in title:
            return Decision(True, f"the title of the merged pull request contains [skip release]: {title!r}")
    if titles:
        return Decision(False, f"the merged pull request title has no [skip release]: {titles[0]!r}")
    return Decision(False, "no pull request is associated with the pushed commit (a direct push): the commit subject is the whole signal")


def github_titles(repo: str, token: str, sha: str, sleep: Callable[[float], None] = time.sleep) -> Callable[[str | None], list[str]]:
    """Titles of the PR that produced the pushed commit, with retries and backoff.

    Only a PR whose merge_commit_sha is the pushed SHA counts: an unrelated open
    PR must never decide a direct push. When the commit has no associated PR but
    its subject ends in "(#N)", PR N is read and must have been merged as this
    very commit, otherwise the lookup fails (closed) instead of releasing blindly.
    """

    def get(path: str):
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo}/{path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28"},
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)

    def once(number: str | None) -> list[str]:
        prs = get(f"commits/{sha}/pulls")
        matched = [p["title"] for p in prs if p.get("merge_commit_sha") == sha]
        if matched or not number:
            return matched
        pr = get(f"pulls/{number}")
        if pr.get("merge_commit_sha") != sha:
            raise RuntimeError(f"PR #{number} was not merged as commit {sha[:12]}")
        return [pr["title"]]

    def fetch(number: str | None) -> list[str]:
        if not (repo and token and sha):  # a missing setting is not transient: no retries
            raise RuntimeError("no GITHUB_REPOSITORY/GITHUB_TOKEN/GITHUB_SHA")
        last: Exception | None = None
        for attempt in range(ATTEMPTS):
            try:
                return once(number)
            except Exception as exc:
                last = exc
                if attempt < ATTEMPTS - 1:
                    sleep(2 ** attempt)
        assert last is not None
        raise last

    return fetch


def main() -> int:
    env = os.environ
    d = decide(env.get("COMMIT_MSG", ""),
               github_titles(env.get("GITHUB_REPOSITORY", ""), env.get("GITHUB_TOKEN", ""), env.get("GITHUB_SHA", "")),
               env.get("GITHUB_EVENT_NAME", "push"), env.get("GITHUB_REF", MAIN_REF))
    verdict = "skip (no release)" if d.skip else "release"
    print(f"release decision: {verdict}: {d.reason}")
    if d.failed:
        print(f"::warning::Auto Release: {d.reason}")
    if env.get("GITHUB_OUTPUT"):
        with open(env["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
            fh.write(f"skip={str(d.skip).lower()}\n")
    if env.get("GITHUB_STEP_SUMMARY"):
        with open(env["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as fh:
            fh.write(f"### Auto Release decision\n\n**{verdict}**: {d.reason}\n\n")
    return 1 if d.failed else 0


if __name__ == "__main__":
    sys.exit(main())
