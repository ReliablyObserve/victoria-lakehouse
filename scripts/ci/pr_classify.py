#!/usr/bin/env python3
"""Classify a PR for the registry gate (scripts/ci/check_registry_touch.sh).

Reuses the changelog gate's own release-metadata test (check_changelog_pr.py)
and tightens it, so the two gates never disagree about what a release-metadata
sync is.

Prints `key=value` lines:
  exempt=release-metadata|dependency-only|none
  product=1|0
  reason=<first path that made it product-changing>

A path is PRODUCT when it is not a test artifact and is
  - anything under internal/, cmd/, lakehouse-traces/ (Go, embedded UI assets,
    SQL, YAML ...) except README.md / RUNBOOK.md;
  - under patches/ or charts/ (docs excluded, templates included);
  - a shipped build file: Dockerfile, Dockerfile.logs, Dockerfile.traces, and
    go.mod / go.sum at the root.
Test artifacts: *_test.go, anything under testdata/, and test_*.{sh,py} /
*_test.{sh,py} scripts. There is no "generated file" exclusion: the product
trees hold no generated files, and a marker comment must not switch the gate off.
"""
from __future__ import annotations

import argparse
import fnmatch
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_changelog_pr as cc  # noqa: E402

PRODUCT_TREES = ("internal/", "cmd/", "lakehouse-traces/")
PRODUCT_DIR_PREFIXES = ("patches/", "charts/")
PRODUCT_DOC_NAMES = ("README.md", "RUNBOOK.md")
SHIPPED_BUILD_FILES = ("Dockerfile", "Dockerfile.logs", "Dockerfile.traces")
ROOT_MODULE_FILES = ("go.mod", "go.sum")
DEP_COMMIT_PREFIXES = ("build(deps", "chore(deps")
RELEASE_BOT = "github-actions[bot]"
SEMVER = re.compile(r"\d+\.\d+\.\d+")
VERSION_HEADING = re.compile(r"^## \[\d+\.\d+\.\d+\] - \d{4}-\d{2}-\d{2}")
REQUIRE_ENTRY = re.compile(r"^(?:require\s+)?(\S+)\s+(v\S+?)(?:\s*//.*)?$")


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def show(rev: str, path: str) -> str:
    try:
        return git("show", f"{rev}:{path}")
    except subprocess.CalledProcessError:
        return ""


def is_test_artifact(path: str) -> bool:
    base = os.path.basename(path)
    if path.endswith("_test.go") or "/testdata/" in "/" + path:
        return True
    if base.endswith((".sh", ".py")):
        return base.startswith("test_") or base.rsplit(".", 1)[0].endswith("_test")
    return False


def product_reason(path: str) -> str | None:
    if is_test_artifact(path):
        return None
    base = os.path.basename(path)
    if path.startswith(PRODUCT_TREES):
        return None if base in PRODUCT_DOC_NAMES else "product code: " + path
    if path.startswith(PRODUCT_DIR_PREFIXES):
        if path.endswith((".md", ".txt")) and "/templates/" not in path:
            return None
        return "packaged/patched: " + path
    if base in SHIPPED_BUILD_FILES or path in ROOT_MODULE_FILES:
        return "shipped build file: " + path
    return None


# ---------------------------------------------------------------- go.mod --

def parse_go_mod(text: str) -> tuple[dict[str, str], list[str]]:
    """(module -> version for every require entry, every other line)."""
    requires: dict[str, str] = {}
    others: list[str] = []
    in_block = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("//"):
            continue
        if in_block:
            if line == ")":
                in_block = False
                continue
            m = REQUIRE_ENTRY.match(line)
            if m:
                requires[m.group(1)] = m.group(2)
            else:
                others.append(line)
            continue
        if re.fullmatch(r"require\s*\(", line):
            in_block = True
            continue
        if line.startswith("require "):
            m = REQUIRE_ENTRY.match(line)
            if m:
                requires[m.group(1)] = m.group(2)
                continue
        others.append(line)
    return requires, others


def go_mod_dependency_only(base: str, head: str) -> bool:
    """Only `require` version lines changed, none of them VictoriaMetrics/*."""
    b_req, b_other = parse_go_mod(base)
    h_req, h_other = parse_go_mod(head)
    if b_other != h_other:  # replace / go / toolchain / module / exclude / retract edits
        return False
    for mod in set(b_req) | set(h_req):
        if b_req.get(mod) != h_req.get(mod) and mod.startswith("github.com/VictoriaMetrics/"):
            return False
    return True


def is_dependency_manifest(path: str) -> bool:
    b = os.path.basename(path)
    return b in ("go.mod", "go.sum") or fnmatch.fnmatch(b, "requirements*.txt")


def dependency_only(files: list[str], commits: list[str], base: str, head: str) -> bool:
    if not files or not all(is_dependency_manifest(f) for f in files):
        return False
    subjects = [c for c in commits if c.strip() and not c.startswith("Merge ")]
    if not subjects or not all(c.strip().lower().startswith(DEP_COMMIT_PREFIXES) for c in subjects):
        return False
    for f in files:
        if os.path.basename(f) == "go.mod" and not go_mod_dependency_only(show(base, f), show(head, f)):
            return False
    return True


# ------------------------------------------------------ release metadata --

def changed_lines(base_text: str, head_text: str) -> tuple[list[str], list[str]]:
    """(removed lines, added lines) by multiset difference of stripped lines."""
    from collections import Counter
    b = Counter(l.strip() for l in base_text.splitlines() if l.strip())
    h = Counter(l.strip() for l in head_text.splitlines() if l.strip())
    return list((b - h).elements()), list((h - b).elements())


def release_metadata(files: list[str], base: str, head: str, author: str, approvers: set[str]) -> bool:
    if not cc.is_release_metadata_sync(files):
        return False
    if not author or author.lower() not in approvers | {RELEASE_BOT}:
        return False
    chart = "charts/victoria-lakehouse/Chart.yaml"
    if chart in files:
        removed, added = changed_lines(show(base, chart), show(head, chart))
        if not all(re.match(r"^(version|appVersion):", l) for l in removed + added):
            return False
    if "README.md" in files:
        removed, added = changed_lines(show(base, "README.md"), show(head, "README.md"))
        if not all(SEMVER.search(l) for l in removed + added):
            return False
    # CHANGELOG: a release only moves existing text under a new version heading.
    base_lines = {l.strip() for l in show(base, "CHANGELOG.md").splitlines()}
    _, added = changed_lines(show(base, "CHANGELOG.md"), show(head, "CHANGELOG.md"))
    return all(VERSION_HEADING.match(l) or l in base_lines for l in added)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="merge base")
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--title", default="")
    ap.add_argument("--author", default="", help="PR author login (release-metadata exemption)")
    ap.add_argument("--approvers", default="", help="file with approver logins, read from the merge base")
    a = ap.parse_args()

    files = [f for f in git("diff", "--no-renames", "--name-only", a.base, a.head).splitlines() if f]
    commits = git("log", "--pretty=format:%s", f"{a.base}..{a.head}").splitlines()
    if a.title:
        commits.append(a.title)
    approvers = set()
    if a.approvers and os.path.exists(a.approvers):
        for line in open(a.approvers).read().splitlines():
            line = line.split("#", 1)[0].strip().lower()
            if line:
                approvers.add(line)

    exempt = "none"
    if release_metadata(files, a.base, a.head, a.author, approvers):
        exempt = "release-metadata"
    elif dependency_only(files, commits, a.base, a.head):
        exempt = "dependency-only"

    reason = ""
    if exempt == "none":
        for f in files:
            r = product_reason(f)
            if r:
                reason = r
                break
    print(f"exempt={exempt}")
    print(f"product={1 if reason else 0}")
    print(f"reason={reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
