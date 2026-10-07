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

CHART_PATH = "charts/victoria-lakehouse/Chart.yaml"
_CHART_VERSION = re.compile(r"^version: (\d+\.\d+\.\d+)$")
_CHART_APPVERSION = re.compile(r'^appVersion: "?(\d+\.\d+\.\d+)"?$')


def chart_versions(text: str) -> tuple[str, str, list[str]] | None:
    """(version, appVersion, every other line) from the TOP-LEVEL lines, unstripped.

    None when either key is missing or repeated. A nested `version:` (indented)
    is not a chart version and stays in the rest.
    """
    version, app, rest = [], [], []
    for line in text.split("\n"):
        mv, ma = _CHART_VERSION.match(line), _CHART_APPVERSION.match(line)
        if mv:
            version.append(mv.group(1))
        elif ma:
            app.append(ma.group(1))
        else:
            rest.append(line)
    if len(version) != 1 or len(app) != 1:
        return None
    return version[0], app[0], rest


def chart_ok(base_text: str, head_text: str, new: str) -> bool:
    """Chart.yaml changes only `version` and `appVersion`, both to the released version."""
    b, h = chart_versions(base_text), chart_versions(head_text)
    if b is None or h is None:
        return False
    if h[0] != new or h[1] != new or b[2] != h[2]:
        return False
    return tuple(map(int, new.split("."))) > tuple(map(int, b[0].split(".")))


def release_metadata(files: list[str], base: str, head: str, author: str, approvers: set[str]) -> bool:
    """The shape of a release-metadata PR, exactly.

    CHANGELOG moves text only between [Unreleased], the new section and the newest
    section (cc.changelog_release_shape); Chart.yaml changes only version and
    appVersion, both to the new release; README only its version numbers; the
    regenerated docs/features.md only the naming of that release; the author is
    the release bot or an approver; and when the repository has tags, the release
    has its tag.
    """
    shape = cc.changelog_release_shape(show(base, "CHANGELOG.md"), show(head, "CHANGELOG.md"))
    if shape is None:
        return False
    old, new = shape
    # (a release without a Chart.yaml change fails chart_ok: the version is not newer)
    generated_ok = all(
        cc.generated_docs_version_naming_only(show(base, f), show(head, f), old, new)
        for f in files
        if f in cc.RELEASE_METADATA_GENERATED_FILES
    )
    if not cc.is_release_metadata_sync(files, generated_ok=generated_ok):
        return False
    if not author or author.lower() not in approvers | {RELEASE_BOT}:
        return False
    if not chart_ok(show(base, CHART_PATH), show(head, CHART_PATH), new):
        return False
    if "README.md" in files and SEMVER.sub("VER", show(base, "README.md")) != SEMVER.sub("VER", show(head, "README.md")):
        return False
    tags = git("tag", "-l", "v*").split()
    return not tags or f"v{new}" in tags


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
        with open(a.approvers, encoding="utf-8") as fh:
            for line in fh.read().splitlines():
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
