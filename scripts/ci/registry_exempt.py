#!/usr/bin/env python3
"""Decide whether the `registry-exempt` label may bypass the registry gate.

The label is honoured in exactly one run: the one its own `labeled` event
triggers, when the label is `registry-exempt` and the person who applied it
(`github.event.sender.login`) is listed in the approvers file (read from the
merge base, so a PR cannot approve itself). Every other run of the gate denies
it: a push, an edit, a re-run on another label, or a later event. The owner
re-applies the label after the last push, so an approval can never outlive the
head it was given for, and the gate needs no timeline lookup.

Fails closed: a missing approvers file, an unknown actor or a missing field
denies.

Usage: registry_exempt.py --approvers FILE [--action A] [--label L] [--sender S]
       (defaults from the env: EVENT_ACTION, LABEL_NAME, SENDER)
Exit 0 approved, 1 denied (message on stdout).
"""
from __future__ import annotations

import argparse
import os
import sys

LABEL = "registry-exempt"
ACTION = "labeled"


def parse_approvers(text: str) -> set[str]:
    out = set()
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line.lower())
    return out


def decide(action: str, label: str, sender: str, approvers: set[str]) -> tuple[bool, str]:
    if not approvers:
        return False, "no approvers are configured (.github/registry-exempt-approvers is empty or missing at the merge base)"
    if action != ACTION:
        return False, (f"this run was triggered by '{action or 'an unknown event'}', not by the '{LABEL}' label being applied; "
                       f"the owner applies the label after the last push and that run honours it")
    if label != LABEL:
        return False, f"this run was triggered by the label '{label}', not '{LABEL}'"
    who = (sender or "").lower()
    if not who:
        return False, f"the actor who applied '{LABEL}' is unknown"
    if who not in approvers:
        return False, f"'{LABEL}' was applied by '{who}', who is not an approver; only the owner may apply it"
    return True, f"'{LABEL}' applied by approver '{who}' in this run"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--approvers", required=True, help="file with one login per line")
    ap.add_argument("--action", default=os.environ.get("EVENT_ACTION", ""))
    ap.add_argument("--label", default=os.environ.get("LABEL_NAME", ""))
    ap.add_argument("--sender", default=os.environ.get("SENDER", ""))
    a = ap.parse_args()
    try:
        approvers = set()
        if os.path.exists(a.approvers):
            with open(a.approvers, encoding="utf-8") as fh:
                approvers = parse_approvers(fh.read())
        ok, msg = decide(a.action, a.label, a.sender, approvers)
    except Exception as exc:  # fail closed
        print(f"cannot verify who applied '{LABEL}': {type(exc).__name__}: {exc}")
        return 1
    print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
