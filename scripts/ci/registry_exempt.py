#!/usr/bin/env python3
"""Decide whether the `registry-exempt` label on a PR was applied by an approver.

The label bypasses the registry coverage gate, so it counts only when the
LATEST "labeled" event for it was made by a login listed in the approvers file
(read from the merge base, never from the PR, so a PR cannot approve itself).
Fails closed: an API error, an unknown actor, a missing approvers file or a
label that is not currently applied all deny.

Usage: registry_exempt.py --approvers FILE [--events-file JSON | (env GITHUB_TOKEN,
       GITHUB_REPOSITORY, PR_NUMBER)]
Exit 0 approved, 1 denied (message on stdout).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

LABEL = "registry-exempt"


def parse_approvers(text: str) -> set[str]:
    out = set()
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line.lower())
    return out


def decide(events: list[dict], approvers: set[str]) -> tuple[bool, str]:
    """events: the issue events in API order (oldest first)."""
    if not approvers:
        return False, "no approvers are configured (.github/registry-exempt-approvers is empty or missing at the merge base)"
    last = None
    for e in events:
        if e.get("event") in ("labeled", "unlabeled") and (e.get("label") or {}).get("name") == LABEL:
            last = e
    if last is None:
        return False, f"no '{LABEL}' label event found in the PR's history"
    if last["event"] != "labeled":
        return False, f"the '{LABEL}' label was removed (latest event is 'unlabeled')"
    actor = ((last.get("actor") or {}).get("login") or "").lower()
    if not actor:
        return False, f"the actor who applied '{LABEL}' is unknown"
    if actor not in approvers:
        return False, f"'{LABEL}' was last applied by '{actor}', who is not an approver; only the owner may apply it (remove and have an approver re-apply it)"
    return True, f"'{LABEL}' applied by approver '{actor}'"


def fetch_events(repo: str, pr: str, token: str) -> list[dict]:
    events: list[dict] = []
    page = 1
    while True:
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo}/issues/{pr}/events?per_page=100&page={page}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2022-11-28"},
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            batch = json.load(r)
        events.extend(batch)
        if len(batch) < 100:
            return events
        page += 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--approvers", required=True, help="file with one login per line ('-' for none)")
    ap.add_argument("--events-file")
    a = ap.parse_args()
    try:
        approvers = parse_approvers(open(a.approvers).read()) if a.approvers != "-" and os.path.exists(a.approvers) else set()
        if a.events_file:
            events = json.load(open(a.events_file))
        else:
            token, repo, pr = (os.environ.get(k, "") for k in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "PR_NUMBER"))
            if not (token and repo and pr):
                print(f"cannot verify who applied '{LABEL}': no GITHUB_TOKEN/GITHUB_REPOSITORY/PR_NUMBER (the label only works in CI)")
                return 1
            events = fetch_events(repo, pr, token)
        ok, msg = decide(events, approvers)
    except Exception as exc:  # fail closed
        print(f"cannot verify who applied '{LABEL}': {type(exc).__name__}: {exc}")
        return 1
    print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
