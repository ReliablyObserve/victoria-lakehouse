#!/usr/bin/env python3
"""Decide whether a push to main must NOT cut a release.

A push is skipped when the head commit's subject contains `[skip release]`, or
when it is a squash merge "(#N)" whose PULL REQUEST TITLE contains the marker:
GitHub's default squash subject for a single-commit PR is the commit subject,
not the PR title, so a marker in the title alone is lost from the commit.

Fails closed. When the PR title cannot be read (API error, missing token) the
push is NOT released: the run warns and a maintainer starts the workflow by
hand (workflow_dispatch). A run without a commit message is a manual run and
is never skipped.

Env: COMMIT_MSG, GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_OUTPUT (optional).
Writes `skip=true|false` to GITHUB_OUTPUT and the reason to stdout.
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.request
from typing import Callable

MARKER = "[skip release]"
SQUASH_SUFFIX = re.compile(r"\(#(\d+)\)\s*$")


def decide(message: str, fetch_title: Callable[[str], str]) -> tuple[bool, str]:
    """(skip, reason)."""
    if not message.strip():
        return False, "manual run: no commit message"
    first = message.splitlines()[0]
    if MARKER in first:
        return True, "commit subject contains [skip release]"
    m = SQUASH_SUFFIX.search(first)
    if not m:
        return False, "not a squash merge: the commit subject is the whole signal"
    number = m.group(1)
    try:
        title = fetch_title(number)
    except Exception as exc:  # fail closed
        return True, (
            f"::warning::cannot read the title of PR #{number} ({type(exc).__name__}: {exc}); "
            "not releasing. Run the Auto Release workflow by hand (workflow_dispatch) if this push must release"
        )
    if MARKER in title:
        return True, f"the title of PR #{number} contains [skip release]"
    return False, f"PR #{number} title has no [skip release]"


def github_title(repo: str, token: str) -> Callable[[str], str]:
    def fetch(number: str) -> str:
        if not (repo and token):
            raise RuntimeError("no GITHUB_REPOSITORY/GITHUB_TOKEN")
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo}/pulls/{number}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28"},
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)["title"]
    return fetch


def main() -> int:
    skip, reason = decide(os.environ.get("COMMIT_MSG", ""),
                          github_title(os.environ.get("GITHUB_REPOSITORY", ""), os.environ.get("GITHUB_TOKEN", "")))
    print(f"release skip={str(skip).lower()}: {reason}")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"skip={str(skip).lower()}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
