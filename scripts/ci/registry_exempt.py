#!/usr/bin/env python3
"""Decide whether the `registry-exempt` label on a PR may bypass the registry gate.

The label counts only when ALL hold:
  - its LATEST "labeled" event was made by a login in the approvers file (read
    from the merge base, so a PR cannot approve itself);
  - the label is still applied (no later "unlabeled");
  - nothing was pushed after it: no PR commit (authored/committed date) and no
    head-ref force push is later than the label event, so the owner approved
    the head that is being merged;
  - the gate run was not triggered by a push (opened / synchronize / reopened).
Fails closed: an API error, an unknown actor, a missing approvers file or a
malformed event all deny.

Events are normalised to {kind, actor, at, id}, kind one of labeled, unlabeled,
commit, force_push, and sorted by (at, id).

Usage: registry_exempt.py --approvers FILE [--events-file JSON | (env GITHUB_TOKEN,
       GITHUB_REPOSITORY, PR_NUMBER)] [--action EVENT_ACTION]
Exit 0 approved, 1 denied (message on stdout).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime

LABEL = "registry-exempt"
PUSH_ACTIONS = ("opened", "synchronize", "reopened")

QUERY = """
query($owner:String!,$name:String!,$pr:Int!,$after:String){
 repository(owner:$owner,name:$name){pullRequest(number:$pr){
  timelineItems(first:100,after:$after,itemTypes:[LABELED_EVENT,UNLABELED_EVENT,PULL_REQUEST_COMMIT,HEAD_REF_FORCE_PUSHED_EVENT]){
   pageInfo{hasNextPage endCursor}
   nodes{__typename
    ... on LabeledEvent{createdAt actor{login} label{name}}
    ... on UnlabeledEvent{createdAt actor{login} label{name}}
    ... on PullRequestCommit{commit{committedDate authoredDate}}
    ... on HeadRefForcePushedEvent{createdAt}}}}}}
"""


def parse_approvers(text: str) -> set[str]:
    out = set()
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line.lower())
    return out


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def normalise(nodes: list[dict]) -> list[dict]:
    """GraphQL timeline nodes to {kind, actor, at, id} events."""
    events = []
    for i, n in enumerate(nodes):
        t = n.get("__typename")
        if t in ("LabeledEvent", "UnlabeledEvent"):
            if (n.get("label") or {}).get("name") != LABEL:
                continue
            events.append({"kind": "labeled" if t == "LabeledEvent" else "unlabeled",
                           "actor": ((n.get("actor") or {}).get("login") or ""),
                           "at": n["createdAt"], "id": i})
        elif t == "PullRequestCommit":
            c = n["commit"]
            events.append({"kind": "commit", "actor": "", "at": max(c["committedDate"], c["authoredDate"], key=ts), "id": i})
        elif t == "HeadRefForcePushedEvent":
            events.append({"kind": "force_push", "actor": "", "at": n["createdAt"], "id": i})
    return events


def decide(events: list[dict], approvers: set[str], action: str = "") -> tuple[bool, str]:
    if not approvers:
        return False, "no approvers are configured (.github/registry-exempt-approvers is empty or missing at the merge base)"
    if action in PUSH_ACTIONS:
        return False, f"this run was triggered by a push ('{action}'); the label must be applied after the final push, then the gate re-runs"
    try:
        ordered = sorted(events, key=lambda e: (ts(e["at"]), e.get("id", 0)))
    except (KeyError, ValueError, TypeError) as exc:
        return False, f"malformed event list: {exc}"
    last = None
    for e in ordered:
        if e.get("kind") in ("labeled", "unlabeled"):
            last = e
    if last is None:
        return False, f"no '{LABEL}' label event found in the PR's history"
    if last["kind"] != "labeled":
        return False, f"the '{LABEL}' label was removed (latest event is 'unlabeled')"
    actor = (last.get("actor") or "").lower()
    if not actor:
        return False, f"the actor who applied '{LABEL}' is unknown"
    if actor not in approvers:
        return False, f"'{LABEL}' was last applied by '{actor}', who is not an approver; only the owner may apply it (remove and have an approver re-apply it)"
    at = ts(last["at"])
    later = [e for e in ordered if e.get("kind") in ("commit", "force_push") and ts(e["at"]) > at]
    if later:
        return False, f"{len(later)} commit/force-push event(s) are later than the '{LABEL}' label ({last['at']}); the owner approved an older head — remove and re-apply the label"
    return True, f"'{LABEL}' applied by approver '{actor}' after the last push"


def fetch_events(repo: str, pr: str, token: str) -> list[dict]:
    owner, name = repo.split("/", 1)
    nodes: list[dict] = []
    after = None
    while True:
        body = json.dumps({"query": QUERY, "variables": {"owner": owner, "name": name, "pr": int(pr), "after": after}}).encode()
        req = urllib.request.Request("https://api.github.com/graphql", data=body, headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
        if data.get("errors"):
            raise RuntimeError(f"graphql: {data['errors']}")
        items = data["data"]["repository"]["pullRequest"]["timelineItems"]
        nodes.extend(items["nodes"])
        if not items["pageInfo"]["hasNextPage"]:
            return normalise(nodes)
        after = items["pageInfo"]["endCursor"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--approvers", required=True, help="file with one login per line")
    ap.add_argument("--events-file", help="normalised events JSON (tests)")
    ap.add_argument("--action", default=os.environ.get("EVENT_ACTION", ""))
    a = ap.parse_args()
    try:
        approvers = set()
        if os.path.exists(a.approvers):
            with open(a.approvers, encoding="utf-8") as fh:
                approvers = parse_approvers(fh.read())
        if a.events_file:
            with open(a.events_file, encoding="utf-8") as fh:
                events = json.load(fh)
        else:
            token, repo, pr = (os.environ.get(k, "") for k in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "PR_NUMBER"))
            if not (token and repo and pr):
                print(f"cannot verify who applied '{LABEL}': no GITHUB_TOKEN/GITHUB_REPOSITORY/PR_NUMBER (the label only works in CI)")
                return 1
            events = fetch_events(repo, pr, token)
        if not isinstance(events, list):
            raise ValueError("events must be a list")
        ok, msg = decide(events, approvers, a.action)
    except Exception as exc:  # fail closed
        print(f"cannot verify who applied '{LABEL}': {type(exc).__name__}: {exc}")
        return 1
    print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
