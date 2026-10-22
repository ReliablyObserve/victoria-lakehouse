#!/usr/bin/env python3
"""Regenerate scripts/ci/tests/testdata/ci_scope_replay.json, the recorded pull requests that
scripts/ci/tests/test_pr_classify_ci_scope.py and scripts/ci/tests/ci_scope_replay_check.py replay.

Usage (from a full clone with the merged history, `gh` signed in; the unit tests never call the API):

    env -u GITHUB_TOKEN python3 scripts/ci/ci_scope_replay_gen.py --first 491 --last 686 \\
        --legacy-rev <commit before the change that added --ci-scope> \\
        > scripts/ci/tests/testdata/ci_scope_replay.json

For every merged pull request numbered first..last it records: the squash commit, its title and author
(public data), the changed files (`git diff --name-only <sha>^ <sha>`), the registry-gate keys of the
classifier as it was at --legacy-rev (so a test can prove they are unchanged), and what the scope rules
read from the diff itself (digest-only Dockerfile edits, the per-job verdict of every heavy workflow edit)
so the unit test can replay them without the git objects. `expected` is the scope the current classifier
prints. Review it by hand against the change classes before committing a regenerated file: it is a
regression lock, not an oracle.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import pr_classify as pc  # noqa: E402

SCOPE_KEYS = ("ci_class", "skip_go", "skip_image", "skip_parity", "force")


def run(*args: str, cwd: "str | None" = None, env: "dict[str, str] | None" = None) -> str:
    out = subprocess.run(args, check=True, capture_output=True, cwd=cwd, env=env).stdout
    return out.decode("utf-8", errors="surrogateescape")


def merged_prs(first: int, last: int) -> "list[dict]":
    raw = run("gh", "pr", "list", "--state", "merged", "--limit", "500", "--json", "number,title,author,mergeCommit")
    prs = [p for p in json.loads(raw) if first <= p["number"] <= last]
    return sorted(prs, key=lambda p: p["number"])


def classify(script_dir: str, base: str, head: str, author: str, approvers: str, extra: "list[str]") -> "list[str]":
    out = run("python3", os.path.join(script_dir, "pr_classify.py"), "--base", base, "--head", head,
              "--author", author, "--approvers", approvers, *extra)
    return out.splitlines()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--first", type=int, required=True)
    ap.add_argument("--last", type=int, required=True)
    ap.add_argument("--legacy-rev", required=True, help="revision whose scripts/ci/pr_classify.py predates --ci-scope")
    a = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        legacy = os.path.join(tmp, "legacy")
        os.makedirs(legacy)
        for name in ("pr_classify.py", "check_changelog_pr.py"):
            with open(os.path.join(legacy, name), "wb") as fh:
                fh.write(subprocess.run(["git", "show", f"{a.legacy_rev}:scripts/ci/{name}"], check=True, capture_output=True).stdout)
        approvers_file = os.path.join(tmp, "approvers")
        approvers = pc.show_text(a.legacy_rev, ".github/release-metadata-approvers") or ""
        with open(approvers_file, "w", encoding="utf-8") as fh:
            fh.write(approvers)

        cases = []
        for pr in merged_prs(a.first, a.last):
            sha = pr["mergeCommit"]["oid"]
            base = sha + "^"
            author = pr["author"]["login"]
            if author.startswith("app/"):  # gh prints an app as app/<name>, the event payload as <name>[bot]
                author = author[4:] + "[bot]"
            files = [f for f in pc.git("diff", "--no-renames", "--name-only", "-z", base, sha).split("\0") if f]
            ev = pc.GitEvidence(base, sha)
            pins = [f for f in files if "/" not in f and f.startswith("Dockerfile") and ev.image_pins_only(f)]
            workflows = {}
            for f in files:
                if f.startswith(".github/workflows/") and os.path.basename(f) in pc.CI_HEAVY_WORKFLOWS:
                    v = ev.workflow_jobs(f)
                    workflows[f] = None if v is None else {"jobs": list(v[0]), "detail": v[1]}
            legacy_lines = classify(legacy, base, sha, author, approvers_file, [])
            lines = classify(HERE, base, sha, author, approvers_file, ["--ci-scope", "--event", "pull_request"])
            scope = dict(ln.split("=", 1) for ln in lines[len(legacy_lines):])
            if lines[:len(legacy_lines)] != legacy_lines:
                sys.exit(f"PR {pr['number']}: the registry-gate keys differ from the legacy classifier")
            cases.append({
                "pr": pr["number"], "sha": sha, "title": pr["title"], "author": author, "files": files,
                "evidence": {"image_pins_only": pins, "workflows": workflows},
                "legacy": legacy_lines,
                "expected": {k: scope[k] for k in SCOPE_KEYS},
            })
        json.dump({"legacy_rev": a.legacy_rev, "approvers": sorted(ln.strip() for ln in approvers.split("\n") if ln.strip() and not ln.startswith("#")),
                   "cases": cases}, sys.stdout, indent=1, sort_keys=True)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
