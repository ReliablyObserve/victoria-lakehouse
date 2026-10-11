#!/usr/bin/env python3
"""Classify a PR for the registry gate (scripts/ci/check_registry_touch.sh).

Reuses the changelog gate's own release-metadata test (check_changelog_pr.py)
and tightens it, so the two gates never disagree about what a release-metadata
sync is.

Prints `key=value` lines:
  exempt=release-metadata|dependency-only|toolchain-only|none
  product=1|0
  reason=<first path that made it product-changing>
  detail=<what was verified, for the report>
  gate_file=<path>        (repeated: the changed files that make up the gate itself)

There is no exemption by label or by PR text: every kind is decided from the content
of the diff. `toolchain-only` is a Go toolchain / dependency-version bump whose every
changed line is verified (see toolchain_only): it waives Rule 1 only.

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
# Modules whose bump changes what the product stores or answers: never
# dependency-only, they need registry coverage or the owner's exemption.
CRITICAL_MODULE_PREFIXES = (
    "github.com/VictoriaMetrics/",
    "github.com/parquet-go/",
    "github.com/aws/",
    # codecs and wire formats: they decide what bytes mean
    "github.com/klauspost/",
    "github.com/golang/snappy",
    "github.com/pierrec/",
    "github.com/andybalholm/brotli",
    "github.com/valyala/gozstd",
    "github.com/DataDog/zstd",
    "github.com/gogo/protobuf",
    "github.com/apache/arrow",
    "go.opentelemetry.io/proto/",
    "google.golang.org/",
)
RELEASE_BOT = "github-actions[bot]"

# The files that make up the gate itself. A change to one is listed in the report for
# the reviewer; it is not a failure (the pull request's required approval covers it).
GATE_FILES_RE = re.compile(
    r"^(scripts/ci/[^/]+$|\.gitattributes$|tests/parity/lock_cells_test\.go$|tests/conformance/cmd/testlinks/"
    r"|tests/conformance/registry/(testlinks|paritygate|lockcode)\.go"
    r"|\.github/workflows/(conformance|registry-gate|registry-gate-base|gate-selftest|parity)\.yaml"
    r"|\.github/registry-exempt-approvers)"
)
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
                requires[m.group(1).strip('"`')] = m.group(2)
            else:
                others.append(line)
            continue
        if re.fullmatch(r"require\s*\(", line):
            in_block = True
            continue
        if line.startswith("require "):
            m = REQUIRE_ENTRY.match(line)
            if m:
                requires[m.group(1).strip('"`')] = m.group(2)
                continue
        others.append(line)
    return requires, others


PIN_RE = re.compile(r"^(VL_VERSION_LOGS|VL_COMMIT_TRACES|VT_VERSION)\s*:?=\s*(\S*)", re.M)


def makefile_pins(text: str) -> dict[str, str]:
    """The upstream pins a Makefile sets: changing one changes what the product embeds."""
    return {m.group(1): m.group(2) for m in PIN_RE.finditer(text)}


def go_mod_dependency_only(base: str, head: str) -> bool:
    """Only `require` version lines changed, none of them a storage-critical module."""
    b_req, b_other = parse_go_mod(base)
    h_req, h_other = parse_go_mod(head)
    if b_other != h_other:  # replace / go / toolchain / module / exclude / retract edits
        return False
    for mod in set(b_req) | set(h_req):
        if b_req.get(mod) != h_req.get(mod) and mod.startswith(CRITICAL_MODULE_PREFIXES):
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


# ---------------------------------------------------------- toolchain-only --
#
# A Go toolchain bump touches a fixed, small set of files and changes nothing but
# version numbers in them. The kind is decided from the content of every changed
# file (base blob against head blob), never from a commit message or a title:
#
#   go.mod, lakehouse-traces/go.mod   the `go` and `toolchain` directive lines, and
#                                     the version of `require` entries (indirect
#                                     markers included), none of them a storage-critical
#                                     module (CRITICAL_MODULE_PREFIXES); the set of
#                                     required modules and every other line unchanged
#   go.work                           the `go` and `toolchain` lines
#   go.sum, lakehouse-traces/go.sum   hash lines, none of a storage-critical module
#   Dockerfile, Dockerfile.logs,      `FROM [--platform=..] golang:<version>[-alpine[<ver>]] [AS x]`
#   Dockerfile.traces,                lines where only the versions change
#   Dockerfile.datagen
#   tests/parity/docker-compose.yml   `image: golang:<version>[-alpine[<ver>]]` lines, same
#
# plus files that are plainly not product: docs and Markdown, CI workflows and scripts
# that are not gate files. Tests, registry rows and the parity locks are NOT in that
# set, so a "toolchain" PR cannot carry a lock change through this kind.
GO_MOD_FILES = ("go.mod", "lakehouse-traces/go.mod")
GO_SUM_FILES = ("go.sum", "lakehouse-traces/go.sum")
GO_WORK_FILE = "go.work"
GOLANG_DOCKERFILES = ("Dockerfile", "Dockerfile.logs", "Dockerfile.traces", "Dockerfile.datagen")
GOLANG_COMPOSE_FILES = ("tests/parity/docker-compose.yml",)

GO_DIRECTIVE = re.compile(r"^go \d+\.\d+(?:\.\d+)?$")
TOOLCHAIN_DIRECTIVE = re.compile(r"^toolchain go\d+\.\d+(?:\.\d+)?$")
GO_SUM_LINE = re.compile(r"^(\S+) (v\S+?)(?:/go\.mod)? h1:\S+$")
_VER = r"(?P<ver>\d+\.\d+(?:\.\d+)?)"
_ALP = r"(?P<alp>-alpine(?P<alpver>\d+\.\d+)?)?"
FROM_GOLANG = re.compile(
    r"^(?P<pre>FROM(?:[ \t]+--platform=\S+)?[ \t]+golang:)" + _VER + _ALP + r"(?P<post>(?:[ \t]+AS[ \t]+\S+)?[ \t]*)$"
)
IMAGE_GOLANG = re.compile(r"^(?P<pre>[ \t]+image:[ \t]+golang:)" + _VER + _ALP + r"(?P<post>[ \t]*)$")


def _image_shape(m: "re.Match[str]") -> tuple[str, str, str]:
    """Everything of a golang image line that must not change in a bump."""
    alp = m.group("alp")
    return m.group("pre"), ("" if alp is None else ("-alpineV" if m.group("alpver") else "-alpine")), m.group("post")


def golang_image_bump_only(base: str, head: str, line_re: "re.Pattern[str]") -> bool:
    """True when head differs from base only in the version of golang image lines.

    Compared by position: the same number of lines, every differing pair matching
    `line_re` in both with the same prefix (platform, indentation), the same suffix shape
    (`-alpine` stays `-alpine`; its release number may move) and the same trailer (AS name);
    every other line byte-identical. The Go twin is registry.GolangImageBumpOnly; both are
    tested against one fixture.
    """
    bl, hl = base.split("\n"), head.split("\n")
    if len(bl) != len(hl):
        return False
    changed = False
    for b, h in zip(bl, hl):
        if b == h:
            continue
        bm, hm = line_re.match(b), line_re.match(h)
        if not bm or not hm or _image_shape(bm) != _image_shape(hm):
            return False
        changed = True
    return changed


def _is_directive(line: str) -> bool:
    return bool(GO_DIRECTIVE.match(line) or TOOLCHAIN_DIRECTIVE.match(line))


def go_mod_toolchain_only(base: str, head: str) -> bool:
    """go.mod: the go/toolchain directives and non-critical require versions only."""
    b_req, b_other = parse_go_mod(base)
    h_req, h_other = parse_go_mod(head)
    b_rest = [x for x in b_other if not _is_directive(x)]
    h_rest = [x for x in h_other if not _is_directive(x)]
    if b_rest != h_rest or set(b_req) != set(h_req):
        return False
    for other in (b_other, h_other):
        if sum(1 for x in other if GO_DIRECTIVE.match(x)) != 1:
            return False
    return not any(b_req[m] != h_req[m] and m.startswith(CRITICAL_MODULE_PREFIXES) for m in b_req)


def go_work_toolchain_only(base: str, head: str) -> bool:
    def split(text: str) -> tuple[list[str], list[str]]:
        lines = [x.strip() for x in text.splitlines() if x.strip()]
        return [x for x in lines if _is_directive(x)], [x for x in lines if not _is_directive(x)]

    _, b_rest = split(base)
    h_dir, h_rest = split(head)
    return b_rest == h_rest and any(GO_DIRECTIVE.match(x) for x in h_dir)


def go_sum_toolchain_only(base: str, head: str) -> bool:
    """go.sum: only well-formed hash lines differ, none for a storage-critical module."""
    for line in set(base.splitlines()) ^ set(head.splitlines()):
        m = GO_SUM_LINE.match(line)
        if not m or m.group(1).startswith(CRITICAL_MODULE_PREFIXES):
            return False
    return True


def toolchain_extra_ok(path: str) -> bool:
    """A file that rides along with a toolchain bump without needing a content check:
    documentation, CI workflows and scripts that are not part of the gate. Never a test,
    a registry row, a parity lock or anything product."""
    if GATE_FILES_RE.match(path) or is_test_artifact(path) or product_reason(path):
        return False
    if path.startswith("docs/") or path.endswith(".md"):
        return True
    if path.startswith(".github/workflows/") and path.endswith((".yaml", ".yml")):
        return True
    return path.startswith("scripts/")


def toolchain_only(files: list[str], base: str, head: str) -> str | None:
    """A description of what was verified when the PR is toolchain-only, else None."""
    verified = []
    for f in files:
        b, h = show(base, f), show(head, f)
        if f in GO_MOD_FILES:
            ok = go_mod_toolchain_only(b, h)
        elif f in GO_SUM_FILES:
            ok = go_sum_toolchain_only(b, h)
        elif f == GO_WORK_FILE:
            ok = go_work_toolchain_only(b, h)
        elif f in GOLANG_DOCKERFILES:
            ok = golang_image_bump_only(b, h, FROM_GOLANG)
        elif f in GOLANG_COMPOSE_FILES:
            ok = golang_image_bump_only(b, h, IMAGE_GOLANG)
        else:
            if not toolchain_extra_ok(f):
                return None
            continue
        if not ok:
            return None
        verified.append(f)
    if not verified:
        return None
    return "only Go toolchain and dependency versions changed in " + ", ".join(verified)


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

    exempt, detail = "none", ""
    if release_metadata(files, a.base, a.head, a.author, approvers):
        exempt = "release-metadata"
        detail = "exact release-metadata shape (CHANGELOG, Chart.yaml, regenerated docs)"
    elif dependency_only(files, commits, a.base, a.head):
        exempt = "dependency-only"
        detail = "only go.mod require versions and go.sum changed, every commit is build(deps)/chore(deps)"
    elif (tc := toolchain_only(files, a.base, a.head)) is not None:
        exempt = "toolchain-only"
        detail = tc

    reason = ""
    if exempt == "none":
        for f in files:
            r = product_reason(f)
            if r:
                reason = r
                break
            if f == "Makefile" and makefile_pins(show(a.base, f)) != makefile_pins(show(a.head, f)):
                reason = "upstream pin changed: " + f
                break
    print(f"exempt={exempt}")
    print(f"product={1 if reason else 0}")
    print(f"reason={reason}")
    print(f"detail={detail}")
    for f in files:
        if GATE_FILES_RE.match(f):
            print(f"gate_file={f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
