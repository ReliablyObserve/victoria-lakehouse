#!/usr/bin/env python3
"""Replay pr_classify.py --ci-scope over the recorded merged pull requests, on the real git objects.

Run from the repository root of a full clone (fetch-depth: 0). It is not discovered by `unittest discover`
(the name does not start with test), because the shallow checkout of the changelog-tests job has no history;
gate-selftest.yaml (gate-unit) runs it. For each case of testdata/ci_scope_replay.json it checks, with the
squash commit and its parent as the diff:

  1. the commit exists (a missing commit is a FAILURE, never a skip) and its file list is the recorded one;
  2. the registry-gate keys are exactly the ones the classifier printed before --ci-scope existed;
  3. with --ci-scope the output starts with those same bytes and then holds the seven scope keys;
  4. the scope equals the recorded one.

Regenerate the file with scripts/ci/ci_scope_replay_gen.py. A deliberate change to the registry-gate keys of
the classifier (step 2) or to the scope rules (step 4) needs the recorded file regenerated and reviewed in the same change.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "pr_classify.py")
FIXTURE = os.path.join(HERE, "testdata", "ci_scope_replay.json")
SCOPE_KEYS = ("ci_class", "skip_go", "skip_image", "skip_parity", "force", "ci_reason", "ci_detail")


def run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True)


def fail(failures: "list[str]", case: dict, msg: str) -> None:
    failures.append(f"#{case['pr']} {case['sha'][:10]}: {msg}")


def main() -> int:
    with open(FIXTURE, encoding="utf-8") as fh:
        data = json.load(fh)
    failures: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        approvers = os.path.join(tmp, "approvers")
        with open(approvers, "w", encoding="utf-8") as fh:
            fh.write("\n".join(data["approvers"]) + "\n")
        for c in data["cases"]:
            sha = c["sha"]

            if run("git", "cat-file", "-e", sha + "^{commit}").returncode != 0 or run("git", "cat-file", "-e", sha + "^").returncode != 0:
                fail(failures, c, "commit or parent missing from this clone (fetch the full history)")
                continue
            names = run("git", "diff", "--no-renames", "--name-only", "-z", sha + "^", sha).stdout.decode("utf-8", "surrogateescape")
            if [f for f in names.split("\0") if f] != c["files"]:
                fail(failures, c, "the changed files differ from the recorded list")
                continue
            base = [sys.executable, SCRIPT, "--base", sha + "^", "--head", sha, "--author", c["author"], "--approvers", approvers]
            plain = run(*base)
            scoped = run(*base, "--ci-scope", "--event", "pull_request")
            if plain.returncode != 0 or scoped.returncode != 0:
                fail(failures, c, f"classifier exit {plain.returncode}/{scoped.returncode}: {scoped.stderr.decode()[-200:]}")
                continue
            p_out, s_out = plain.stdout.decode("utf-8", "surrogateescape"), scoped.stdout.decode("utf-8", "surrogateescape")
            if p_out.splitlines() != c["legacy"]:
                fail(failures, c, "the registry-gate keys differ from the recorded ones")
            if not s_out.startswith(p_out):
                fail(failures, c, "--ci-scope changed the registry-gate keys")
                continue
            extra = s_out[len(p_out):].splitlines()
            if [ln.split("=", 1)[0] for ln in extra] != list(SCOPE_KEYS):
                fail(failures, c, "the scope keys are not " + ",".join(SCOPE_KEYS))
                continue
            got = dict(ln.split("=", 1) for ln in extra)
            if {k: got[k] for k in c["expected"]} != c["expected"]:
                fail(failures, c, f"scope {({k: got[k] for k in c['expected']})} != recorded {c['expected']}")
    for f in failures:
        print("FAIL " + f)
    print(f"ci-scope replay: {len(data['cases']) - len(failures)} of {len(data['cases'])} recorded pull requests replayed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
