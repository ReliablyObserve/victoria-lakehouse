#!/usr/bin/env python3
"""Classify a PR for the registry gate (scripts/ci/check_registry_touch.sh).

Reuses the changelog gate's own classification (check_changelog_pr.py) so the
two gates never disagree about what a release-impacting change is.

Prints `key=value` lines:
  exempt=release-metadata|dependency-only|none
  product=1|0
  reason=<first path or rule that made it product-changing>
"""
from __future__ import annotations

import argparse
import fnmatch
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_changelog_pr as cc  # noqa: E402

PRODUCT_GO_PREFIXES = ("internal/", "cmd/", "lakehouse-traces/")
PRODUCT_DIR_PREFIXES = ("patches/", "charts/")
DEP_FILE_GLOBS = ("go.mod", "go.sum", "requirements*.txt")
DEP_COMMIT_PREFIXES = ("build(deps", "chore(deps")
GENERATED_NAME_GLOBS = ("*.pb.go", "*_gen.go", "*_generated.go", "zz_generated*.go")


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def is_generated_go(path: str, head: str) -> bool:
    base = os.path.basename(path)
    if any(fnmatch.fnmatch(base, g) for g in GENERATED_NAME_GLOBS):
        return True
    try:
        top = git("show", f"{head}:{path}")[:2048]
    except subprocess.CalledProcessError:
        return False  # deleted: a removed product file is a product change
    return "Code generated" in top and "DO NOT EDIT" in top


def is_test_artifact(path: str) -> bool:
    base = os.path.basename(path)
    return (
        path.endswith("_test.go")
        or "/testdata/" in "/" + path
        or base.startswith("test_")
        or base.endswith("_test.sh")
        or base.endswith("_test.py")
    )


def product_reason(path: str, head: str) -> str | None:
    if is_test_artifact(path):
        return None
    if path.endswith(".go") and path.startswith(PRODUCT_GO_PREFIXES):
        return None if is_generated_go(path, head) else "non-test Go: " + path
    if path.startswith(PRODUCT_DIR_PREFIXES):
        if path.endswith((".md", ".txt")) and "/templates/" not in path:
            return None
        return "packaged/patched: " + path
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="merge base")
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--title", default="")
    a = ap.parse_args()

    files = [f for f in git("diff", "--no-renames", "--name-only", a.base, a.head).splitlines() if f]
    commits = git("log", "--pretty=format:%s", f"{a.base}..{a.head}").splitlines()
    if a.title:
        commits.append(a.title)

    exempt = "none"
    if cc.is_release_metadata_sync(files):
        exempt = "release-metadata"
    elif files and all(
        any(fnmatch.fnmatch(os.path.basename(f), g) for g in DEP_FILE_GLOBS) for f in files
    ) and commits and all(c.strip().lower().startswith(DEP_COMMIT_PREFIXES) for c in commits
                           if not c.startswith("Merge ")):
        exempt = "dependency-only"

    reason = ""
    if exempt == "none":
        if cc.should_require_changelog(commits, files):
            reason = "release-impacting per the changelog gate"
        for f in files:
            r = product_reason(f, a.head)
            if r:
                reason = r if not reason else f"{reason}; {r}"
                break
    print(f"exempt={exempt}")
    print(f"product={1 if reason else 0}")
    print(f"reason={reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
