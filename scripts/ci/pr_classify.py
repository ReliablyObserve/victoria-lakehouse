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

With --ci-scope (and --event, see ci_scope_decide) it appends seven more keys that say which CI groups
the change needs (ci_class, skip_go, skip_image, skip_parity, force, ci_reason, ci_detail); the five above
are printed exactly as without the flag. See the "CI scope" section below.

There is no exemption by label or by PR text: every kind is decided from the content
of the diff. `toolchain-only` is a Go toolchain / dependency-version bump whose every
changed line is verified (see toolchain_only): it waives Rule 1 only.

Every file the classifier interprets is read as BYTES and split only on "\n", the way Docker,
Go and git split it (`str.splitlines()` and text-mode reads also split on a lone CR and on
U+0085/U+2028/U+2029, which lets a build change hide inside a "comment" line). A covered file
that holds a control byte (CR included), U+0085/U+2028/U+2029, a BOM or invalid UTF-8 is
never classified as a verified kind: it falls through to the path rules. Every line a kind
interprets is matched with ASCII-only character classes, and every line that CHANGES in a
covered file must be printable ASCII (an em dash in an unchanged comment is fine).

A path is PRODUCT when it is not a test artifact and is
  - anything under internal/, cmd/, lakehouse-traces/ (Go, embedded UI assets,
    SQL, YAML ...) except README.md / RUNBOOK.md;
  - under patches/ or charts/ (docs excluded, templates included);
  - a shipped build file: Dockerfile, Dockerfile.logs, Dockerfile.traces, and
    go.mod / go.sum at the root (a Dockerfile whose only change is the digest of its FROM
    images is not: image_pin_only).
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
# Dependency modules: INERT ones are an allowlist, everything else is CRITICAL.
#
# A module whose bump can change what the product stores or answers must never be bumped under a
# dependency-only or toolchain-only exemption: it needs registry coverage. The first version of this
# gate listed the critical modules and missed some (valyala/fastjson, quicktemplate, yaml.v3, ...): a
# list of what is dangerous is always one module short. So the list is INVERTED. A module is critical
# unless it is named below, with the reason it cannot change a stored or answered byte; a new module,
# or one this table does not know, is critical until a reviewer decides otherwise here.
#
# NOT inert, on purpose (third review of the gate): golang.org/x/sync is the singleflight of
# internal/s3reader/dedup.go, which hands ONE result to every concurrent cold read of the same range (a
# bump can change which bytes concurrent readers share), and golang.org/x/sys/cpu is what parquet-go
# consults to pick its SIMD encoders (a bump can change which encoder writes a page). Both can change
# a stored or answered byte, so a bump of either needs registry coverage. golang.org/x/net stays inert.
#
# Every entry is an exact module path (go.mod and go.sum spell modules the same way), or a path
# prefix ending in "/" for a family that is one project (the OpenTelemetry SDK). Evidence for the
# entries that need it: scripts/ci/tests/test_pr_classify_critical.py scans the import lines of the
# product's .go files with a regular expression (it does not run `go list -deps`, so it sees neither
# build-tag-only imports nor generated files); when deciding a new entry, cross-check by hand with
# `go list -deps -f '{{.ImportPath}} {{join .Imports " "}}'` in both modules.
INERT_MODULES = {
    # test-only: linked into no binary
    "go.uber.org/goleak": "goroutine-leak detector, imported by tests only",
    "github.com/kr/text": "indirect of the test-only pretty printers, linked into no binary",
    "golang.org/x/oauth2": "the OAuth2 client-credentials token source of VictoriaMetrics lib/promauth (outbound auth headers)",
    # HTTP/2 transport: imported only through google.golang.org/grpc (the OTLP trace exporter client in
    # internal/telemetry, the tests/ingestmatrix client). net/http itself uses the copy vendored into
    # GOROOT. No repo or upstream package imports x/net directly, and nothing it provides (http2 framing,
    # hpack, idna host names, the debug trace pages grpc registers) shapes a stored byte or an API payload.
    "golang.org/x/net": "HTTP/2 framing for grpc only; no direct importer in this repo or in deps/",
    # the logging facade and retry timer of the OpenTelemetry SDK
    "github.com/go-logr/logr": "logging facade used by the OpenTelemetry SDK",
    "github.com/go-logr/stdr": "stdlib backend of the logging facade",
    "github.com/cenkalti/backoff/v5": "retry timing of the OTLP trace exporter",
}
# Families that are one project. The OpenTelemetry SDK and its exporters trace the process itself and ship
# spans out of band to a collector; they do not touch stored data. NOT included: go.opentelemetry.io/proto/
# (the OTLP wire format: what ingest bytes mean), go.opentelemetry.io/contrib/ (otelhttp wraps every
# handler's ResponseWriter) and go.opentelemetry.io/auto/.
INERT_MODULE_PREFIXES = ("go.opentelemetry.io/otel/",)
INERT_MODULES["go.opentelemetry.io/otel"] = "the OpenTelemetry API: traces the process, ships spans out of band"


def is_inert(module: str) -> bool:
    """True when a bump of the module cannot change a stored or answered byte (see INERT_MODULES)."""
    return module in INERT_MODULES or module.startswith(INERT_MODULE_PREFIXES)


def is_critical(module: str) -> bool:
    """Everything that is not known to be inert."""
    return not is_inert(module)


RELEASE_BOT = "github-actions[bot]"

# The files that make up the gate itself. A change to one is listed in the report for
# the reviewer; it is not a failure (the pull request's required approval covers it).
# Files that shape the gate's verdict without being its code: the parity allowlist and the lock floors
# (the gate reads them as data, the runtime ratchet enforces them), the two registry-parsing helpers
# (escape.go, canonical.go), the gate's own tests and fixtures, and CODEOWNERS (who must approve).
GATE_FILES_RE = re.compile(
    r"^(scripts/ci/|\.gitattributes$|tests/parity/lock_cells_test\.go$|tests/conformance/cmd/testlinks/"
    r"|tests/parity/(known_failures|lock_cells)\.txt$"
    r"|tests/conformance/registry/(testlinks|paritygate|perfgate|lockcode|escape|canonical)\.go"
    r"|\.github/CODEOWNERS$"
    r"|\.github/workflows/(conformance|registry-gate|registry-gate-base|gate-selftest|parity)\.yaml"
    r"|\.github/(registry-exempt-approvers|release-metadata-approvers))"
)
SEMVER = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
REQUIRE_ENTRY = re.compile(r"^(?:require[ \t]+)?([!-~]+)[ \t]+(v[!-~]+?)(?:[ \t]*//.*)?$")


def git(*args: str) -> str:
    """git's output as str, decoded without newline translation. A path that is not UTF-8 survives as
    surrogate escapes (it is a path, not text: `show` hands it back to git byte for byte), where
    text=True would translate CR LF and raise on the first undecodable byte."""
    raw = subprocess.run(["git", *args], check=True, capture_output=True).stdout
    return raw.decode("utf-8", errors="surrogateescape")


def show(rev: str, path: str) -> bytes:
    """The blob at rev:path as bytes ("" bytes when the path does not exist there)."""
    try:
        return subprocess.run(["git", "show", f"{rev}:{path}"], check=True, capture_output=True).stdout
    except subprocess.CalledProcessError:
        return b""


# Characters that make a line splitter disagree with Docker, Go and git, or that no reviewed
# file of this repository holds: control bytes other than TAB and LF (CR, VT, FF, FS, GS, RS
# ...), DEL, NEL, LS, PS and a BOM.
_BAD_TEXT = re.compile("[\x00-\x08\x0b-\x1f\x7f\x85\u2028\u2029\ufeff]")


def clean_text(raw: bytes) -> "str | None":
    """raw decoded as UTF-8, or None when it cannot be read the way Docker and Go read it:
    invalid UTF-8, or a character from _BAD_TEXT. Newlines are not translated."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return None if _BAD_TEXT.search(text) else text


def show_text(rev: str, path: str) -> "str | None":
    return clean_text(show(rev, path))


def changed_lines_ascii(base: str, head: str) -> bool:
    """Every line that differs between the two texts is printable ASCII (TAB allowed)."""
    return all(re.fullmatch(r"[\t -~]*", ln) for ln in set(base.split("\n")) ^ set(head.split("\n")))


def printable(s: str) -> str:
    """s with every character that could break a key=value line made visible."""
    return "".join(c if c.isprintable() else (f"\\x{ord(c):02x}" if ord(c) < 256 else f"\\u{ord(c):04x}") for c in s)


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
    for raw in text.split("\n"):
        line = raw.strip(" \t")
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
        if re.fullmatch(r"require[ \t]*\(", line):
            in_block = True
            continue
        if line.startswith("require "):
            m = REQUIRE_ENTRY.match(line)
            if m:
                requires[m.group(1).strip('"`')] = m.group(2)
                continue
        others.append(line)
    return requires, others


PIN_RE = re.compile(r"^(VL_VERSION_LOGS|VL_COMMIT_TRACES|VT_VERSION)[ \t]*:?=[ \t]*([!-~]*)", re.M | re.A)


def makefile_pins(text: str) -> dict[str, str]:
    """The upstream pins a Makefile sets: changing one changes what the product embeds."""
    return {m.group(1): m.group(2) for m in PIN_RE.finditer(text)}


def go_mod_dependency_only(base: str, head: str) -> bool:
    """Only `require` version lines changed, none of them a critical module (see INERT_MODULES).

    The set of required modules is equal at both revisions, as for toolchain-only: a newly added
    `require` is a new dependency (it may be critical, or the build may start importing it), and a
    removed one changes what the build links."""
    b_req, b_other = parse_go_mod(base)
    h_req, h_other = parse_go_mod(head)
    if b_other != h_other:  # replace / go / toolchain / module / exclude / retract edits
        return False
    if set(b_req) != set(h_req):
        return False
    return not any(b_req[mod] != h_req[mod] and is_critical(mod) for mod in b_req)


# requirements*.txt: a pinned or ranged package line (extras and environment markers allowed),
# a comment or a blank line. No URLs, `-r`/`-e`/index options or hashes.
_OP = r"(?:==|>=|<=|~=|!=|<|>)"
_VERS = r"[A-Za-z0-9][A-Za-z0-9.*+!_-]*"
REQ_LINE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]*(?:\[[A-Za-z0-9._,-]+\])?[ \t]*" + _OP + r"[ \t]*" + _VERS
    + r"(?:[ \t]*,[ \t]*" + _OP + r"[ \t]*" + _VERS + r")*(?:[ \t]*;[ -~]*)?[ \t]*$"
)


def requirements_only(base: str, head: str) -> bool:
    """requirements*.txt: every line that changed is a package requirement, a comment or blank."""
    for line in set(base.split("\n")) ^ set(head.split("\n")):
        if line.strip(" \t") and not line.lstrip(" \t").startswith("#") and not REQ_LINE.match(line):
            return False
    return True


def is_dependency_manifest(path: str) -> bool:
    """The manifests a dependency-only PR may change: the two Go modules' go.mod/go.sum, and
    a Python requirements file that is not itself product code."""
    if path in GO_MOD_FILES or path in GO_SUM_FILES:
        return True
    return fnmatch.fnmatch(os.path.basename(path), "requirements*.txt") and product_reason(path) is None


def dependency_only(files: list[str], base: str, head: str) -> bool:
    """A dependency-version PR, decided from the content of every changed file alone (never from a
    commit subject or a title): go.mod `require` versions outside the storage-critical modules,
    go.sum hash lines of non-critical modules, requirements*.txt package lines. Waives Rule 1 only."""
    if not files or not all(is_dependency_manifest(f) for f in files):
        return False
    for f in files:
        b, h = show_text(base, f), show_text(head, f)
        if b is None or h is None or not changed_lines_ascii(b, h):
            return False
        if f in GO_MOD_FILES:
            ok = go_mod_dependency_only(b, h)
        elif f in GO_SUM_FILES:
            ok = go_sum_toolchain_only(b, h)
        else:
            ok = requirements_only(b, h)
        if not ok:
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
#                                     module (see INERT_MODULES); the set of
#                                     required modules and every other line unchanged
#   go.work                           the `go` and `toolchain` lines
#   go.sum, lakehouse-traces/go.sum   hash lines, none of a storage-critical module
#   Dockerfile, Dockerfile.logs,      `FROM [--platform=..] golang:<version>[-alpine[<ver>]][@sha256:<64 hex>] [AS x]`
#   Dockerfile.traces,                lines where only the versions change (and the digest, which may be
#   Dockerfile.datagen                missing on either side)
#   tests/parity/docker-compose.yml   `image: golang:<version>[-alpine[<ver>]][@sha256:<64 hex>]` lines, same
#
# Separately from the toolchain kind, a change of only the `@sha256:` digest of the `FROM` lines of those
# Dockerfiles (any base image, the image name and tag equal on both sides, see image_pin_only) is not a
# product change: main() skips the shipped-build-file rule for such a file.
#
# plus files that are plainly not product: docs and Markdown, CI workflows and scripts
# that are not gate files. Tests, registry rows and the parity locks are NOT in that
# set, so a "toolchain" PR cannot carry a lock change through this kind.
GO_MOD_FILES = ("go.mod", "lakehouse-traces/go.mod")
GO_SUM_FILES = ("go.sum", "lakehouse-traces/go.sum")
GO_WORK_FILE = "go.work"
GOLANG_DOCKERFILES = ("Dockerfile", "Dockerfile.logs", "Dockerfile.traces", "Dockerfile.datagen")
GOLANG_COMPOSE_FILES = ("tests/parity/docker-compose.yml",)

# ASCII digits only ([0-9], not \d: Python's \d also matches Arabic-Indic and other Unicode
# digits, which the Go twin and Docker do not).
GO_DIRECTIVE = re.compile(r"^go [0-9]+\.[0-9]+(?:\.[0-9]+)?$")
TOOLCHAIN_DIRECTIVE = re.compile(r"^toolchain go[0-9]+\.[0-9]+(?:\.[0-9]+)?$")
GO_SUM_LINE = re.compile(r"^([!-~]+) (v[!-~]+?)(?:/go\.mod)? h1:[!-~]+$")
_VER = r"(?P<ver>[0-9]+\.[0-9]+(?:\.[0-9]+)?)"
_ALP = r"(?P<alp>-alpine(?P<alpver>[0-9]+\.[0-9]+)?)?"
# An optional content digest after the tag: exactly 64 lowercase hex characters, sha256 only. Anything else
# (short, uppercase, another algorithm) leaves the line unmatched, so it is never verified. It is deliberately
# not captured: the toolchain rule ignores which digest a golang line carries.
_DIGEST = r"(?:@sha256:[0-9a-f]{64})?"
FROM_GOLANG = re.compile(
    r"^(?P<pre>FROM(?:[ \t]+--platform=[!-~]+)?[ \t]+golang:)" + _VER + _ALP + _DIGEST + r"(?P<post>(?:[ \t]+AS[ \t]+[!-~]+)?[ \t]*)$"
)
IMAGE_GOLANG = re.compile(r"^(?P<pre>[ \t]+image:[ \t]+golang:)" + _VER + _ALP + _DIGEST + r"(?P<post>[ \t]*)$")
# Any base image of a release Dockerfile, tag REQUIRED (`image@sha256:..` without a tag never matches), optional digest.
# Single spaces only and no trailing blank (every release Dockerfile is written that way), and the name has no
# colon, so `registry:port/name` and `${VAR}` references are not matched either.
FROM_IMAGE = re.compile(
    r"^(?P<pre>FROM(?: --platform=[!-~]+)? )(?P<name>[a-z0-9][a-z0-9._/-]*):(?P<tag>[A-Za-z0-9_][A-Za-z0-9._-]*)"
    + _DIGEST + r"(?P<post>(?: AS [!-~]+)?)$"
)
_DOCKERFILE_ESCAPE = re.compile(r"^#[ \t]*escape[ \t]*=", re.M | re.I)


def _image_shape(m: "re.Match[str]") -> tuple[str, str, str]:
    """Everything of a golang image line that must not change in a bump."""
    alp = m.group("alp")
    return m.group("pre"), ("" if alp is None else ("-alpineV" if m.group("alpver") else "-alpine")), m.group("post")


def golang_image_bump_only(base: str, head: str, line_re: "re.Pattern[str]") -> bool:
    """True when head differs from base only in the version of golang image lines.

    Compared by position: the same number of lines, every differing pair matching
    `line_re` in both with the same prefix (platform, indentation), the same suffix shape
    (`-alpine` stays `-alpine`; its release number may move forward) and the same trailer (AS name);
    the Go version and the alpine release never go back; every other line byte-identical. Either side of a
    pair may carry a `@sha256:<64 hex>` digest, and the two digests may differ (or one may be missing). The
    Go twin is registry.GolangImageBumpOnly (compose lines only); the compose cases of one shared fixture
    are tested against both.
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
        # a bump moves forward: neither the Go version nor the alpine release may go back; a number
        # that does not fit 31 bits is refused (the Go twin refuses it as well)
        try:
            if _vtuple(hm.group("ver")) < _vtuple(bm.group("ver")):
                return False
            if bm.group("alpver") and _vtuple(hm.group("alpver")) < _vtuple(bm.group("alpver")):
                return False
        except ValueError:
            return False
        changed = True
    return changed


def _continued(lines: list[str], i: int) -> bool:
    """True when line i is the continuation of an instruction: the nearest line above it that is neither
    blank nor a comment (Docker drops those inside a continuation) ends in a backslash."""
    j = i - 1
    while j >= 0 and (not lines[j].strip(" \t") or lines[j].lstrip(" \t").startswith("#")):
        j -= 1
    return j >= 0 and lines[j].rstrip(" \t").endswith("\\")


def image_pin_only(base: str, head: str) -> bool:
    """True when head differs from base only in the `@sha256:<64 hex>` digest of `FROM` lines.

    Compared by position: the same number of lines, every differing pair a `FROM [--platform=..]
    <name>:<tag>[@sha256:<64 hex>] [AS stage]` line in both revisions whose platform, image name, TAG
    and stage clause are byte-equal (the digest may be added, removed or changed; a digest that is not
    exactly 64 lowercase hex characters leaves the line unmatched), every other line byte-identical.
    An image reference with a digest but no tag never matches. Fail closed on anything that could make
    a `FROM`-looking line not an instruction: a `<<` heredoc anywhere, an `escape=` parser directive, or a
    line continuation in front of a changed line (blank and comment lines in between included). The Go twin has no Dockerfile reader (no Go consumer
    reads one), so these cases are Python-only in the shared fixture."""
    for text in (base, head):
        if "<<" in text or _DOCKERFILE_ESCAPE.search(text):
            return False
    bl, hl = base.split("\n"), head.split("\n")
    if len(bl) != len(hl):
        return False
    changed = False
    for i, (b, h) in enumerate(zip(bl, hl)):
        if b == h:
            continue
        bm, hm = FROM_IMAGE.match(b), FROM_IMAGE.match(h)
        if not bm or not hm or any(bm.group(g) != hm.group(g) for g in ("pre", "name", "tag", "post")):
            return False
        if _continued(bl, i) or _continued(hl, i):
            return False
        changed = True
    return changed


def dockerfile_pins_only(base_rev: str, head_rev: str, path: str) -> bool:
    """The release Dockerfile at path differs between the two revisions only in `FROM` image digests
    (image_pin_only), and both revisions are clean ASCII-changed text (see clean_text)."""
    if path not in GOLANG_DOCKERFILES:
        return False
    b, h = show_text(base_rev, path), show_text(head_rev, path)
    return b is not None and h is not None and changed_lines_ascii(b, h) and image_pin_only(b, h)


def _is_directive(line: str) -> bool:
    return bool(GO_DIRECTIVE.match(line) or TOOLCHAIN_DIRECTIVE.match(line))


VERSION_PART_MAX = 2**31 - 1  # the Go twin (registry.versionBefore) parses components as 31-bit numbers


def _vtuple(version: str) -> tuple[int, ...]:
    """A dotted numeric version as a tuple padded to three parts: "1.27" == "1.27.0" < "1.27.1" and
    "3.9" < "3.23" (numeric, not lexical). ASCII digits only: the callers' regexes guarantee it.
    A component above 31 bits raises ValueError: the Go twin refuses it too, and an overflowing number
    is never a bump (the callers treat the error as "not a bump")."""
    parts = [int(x) for x in version.split(".")]
    if any(x > VERSION_PART_MAX for x in parts):
        raise ValueError(f"version component out of range in {version!r}")
    return tuple(parts + [0] * (3 - len(parts)))


def _directives_not_older(base_lines: list[str], head_lines: list[str]) -> bool:
    """The `go` directive and the `toolchain` directive of head are not older than base's. A toolchain
    line present at base may not disappear; one that appears is fine."""
    def pick(lines: list[str], regex: "re.Pattern[str]", prefix: str) -> "tuple[int, ...] | None":
        for x in lines:
            if regex.match(x):
                return _vtuple(x[len(prefix):])
        return None

    try:
        b_go, h_go = pick(base_lines, GO_DIRECTIVE, "go "), pick(head_lines, GO_DIRECTIVE, "go ")
        if b_go is None or h_go is None or h_go < b_go:
            return False
        b_tc, h_tc = pick(base_lines, TOOLCHAIN_DIRECTIVE, "toolchain go"), pick(head_lines, TOOLCHAIN_DIRECTIVE, "toolchain go")
    except ValueError:  # a version component that overflows is never a bump
        return False
    if b_tc is not None and (h_tc is None or h_tc < b_tc):
        return False
    return True


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
    # a bump moves forward: the go directive and the toolchain line may not go back, nor be dropped
    if not _directives_not_older(b_other, h_other):
        return False
    return not any(b_req[m] != h_req[m] and is_critical(m) for m in b_req)


def go_work_toolchain_only(base: str, head: str) -> bool:
    def split(text: str) -> tuple[list[str], list[str]]:
        lines = [x.strip(" \t") for x in text.split("\n") if x.strip(" \t")]
        return [x for x in lines if _is_directive(x)], [x for x in lines if not _is_directive(x)]

    b_dir, b_rest = split(base)
    h_dir, h_rest = split(head)
    return b_rest == h_rest and any(GO_DIRECTIVE.match(x) for x in h_dir) and _directives_not_older(b_dir, h_dir)


def go_sum_toolchain_only(base: str, head: str) -> bool:
    """go.sum: only well-formed hash lines differ, none for a critical module (see INERT_MODULES)."""
    for line in set(base.split("\n")) ^ set(head.split("\n")):
        m = GO_SUM_LINE.match(line)
        if not m or is_critical(m.group(1)):
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


def toolchain_only(files: list[str], base: str, head: str) -> "str | None":
    """A description of what was verified when the PR is toolchain-only, else None.

    Every covered file is read as bytes and must be clean text (see clean_text); a file that is
    not, or whose changed lines are not printable ASCII, makes the PR an ordinary product change."""
    verified = []
    for f in files:
        covered = f in GO_MOD_FILES or f in GO_SUM_FILES or f == GO_WORK_FILE or f in GOLANG_DOCKERFILES or f in GOLANG_COMPOSE_FILES
        if not covered:
            if not toolchain_extra_ok(f):
                return None
            continue
        b, h = show_text(base, f), show_text(head, f)
        if b is None or h is None or not changed_lines_ascii(b, h):
            return None
        if f in GO_MOD_FILES:
            ok = go_mod_toolchain_only(b, h)
        elif f in GO_SUM_FILES:
            ok = go_sum_toolchain_only(b, h)
        elif f == GO_WORK_FILE:
            ok = go_work_toolchain_only(b, h)
        elif f in GOLANG_DOCKERFILES:
            ok = golang_image_bump_only(b, h, FROM_GOLANG)
        else:
            ok = golang_image_bump_only(b, h, IMAGE_GOLANG)
        if not ok:
            return None
        verified.append(f)
    if not verified:
        return None
    return "only Go toolchain and dependency versions (and golang image digests) changed in " + ", ".join(verified)


# ------------------------------------------------------ release metadata --

CHART_PATH = "charts/victoria-lakehouse/Chart.yaml"
_CHART_VERSION = re.compile(r"^version: ([0-9]+\.[0-9]+\.[0-9]+)$")
_CHART_APPVERSION = re.compile(r'^appVersion: "?([0-9]+\.[0-9]+\.[0-9]+)"?$')


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
    texts: dict[tuple[str, str], str] = {}

    def tx(rev: str, path: str) -> "str | None":
        if (rev, path) not in texts:
            t = show_text(rev, path)
            if t is None:
                return None
            texts[(rev, path)] = t
        return texts[(rev, path)]

    # every file read below is clean text (bytes, "\n" lines only): a CR or a Unicode line
    # separator hidden in a CHANGELOG, Chart.yaml, README or generated document is no metadata
    base_changelog, head_changelog = tx(base, "CHANGELOG.md"), tx(head, "CHANGELOG.md")
    if base_changelog is None or head_changelog is None:
        return False
    shape = cc.changelog_release_shape(base_changelog, head_changelog)
    if shape is None:
        return False
    old, new = shape
    # (a release without a Chart.yaml change fails chart_ok: the version is not newer)
    generated_ok = True
    for f in files:
        if f in cc.RELEASE_METADATA_GENERATED_FILES:
            b, h = tx(base, f), tx(head, f)
            if b is None or h is None or not cc.generated_docs_version_naming_only(b, h, old, new):
                generated_ok = False
    if not cc.is_release_metadata_sync(files, generated_ok=generated_ok):
        return False
    if not author or author.lower() not in approvers | {RELEASE_BOT}:
        return False
    b_chart, h_chart = tx(base, CHART_PATH), tx(head, CHART_PATH)
    if b_chart is None or h_chart is None or not chart_ok(b_chart, h_chart, new):
        return False
    if "README.md" in files:
        b_readme, h_readme = tx(base, "README.md"), tx(head, "README.md")
        if b_readme is None or h_readme is None or SEMVER.sub("VER", b_readme) != SEMVER.sub("VER", h_readme):
            return False
    tags = git("tag", "-l", "v*").split()
    return not tags or f"v{new}" in tags


# ------------------------------------------------------------- CI scope --
#
# `--ci-scope` answers a second question, next to the registry gate's "is this a product change?":
# which CI groups does this change need? The answer is only an input for the workflows; nothing in this
# file skips a job. It is decided from the changed paths and, where a path alone cannot tell, from the
# content of the diff (never from a title, a label, a branch name or a commit subject).
#
# The rules, in order, for every changed path (the first match wins):
#   * a path that is not clean text, or a classifier or fixture path of this very scope logic -> everything;
#   * any Go file, go.mod or go.sum anywhere -> everything;
#   * the product trees, patches (all of them), charts (not their prose), the Makefile, go.work, the linter config, alerts, dashboards,
#     the shipped Dockerfiles (unless only the digests of their FROM images changed) and every test tree not
#     listed below -> everything;
#   * a documented infra path (docs, CI scripts, workflows, deployment, the non-Go test tooling) -> infra;
#   * any path not named in this table -> everything (fail open).
# A union of infra paths asks for the union of what they ask for; one product path asks for everything.
# Any error, an unreadable diff, an event other than `pull_request` -> everything (see ci_scope_full).
#
# Output (appended after the five registry-gate keys, which are unchanged):
#   ci_class=product|release-image-pin|stack|ci|deps|docs|release-metadata|mixed-infra
#   skip_go=true|false      the Go group (Go tests, lint, build, fuzz, vulnerability scans, ...) is not needed
#   skip_image=true|false   the release-image build and scan is not needed
#   skip_parity=true|false  the parity suite is not needed
#   force=<workflow>:<job>,...   jobs that run although their group is skipped (consumer map, workflow edits)
#   ci_reason=<first path that made it a full run>     (empty for a scoped run)
#   ci_detail=<what was verified>
# skip_go=false implies skip_image=false and skip_parity=false: a product change runs everything.

CI_EVENT_SCOPED = "pull_request"
CI_SCOPE_KEYS = ("ci_class", "skip_go", "skip_image", "skip_parity", "force", "ci_reason", "ci_detail")
# The heavy workflows whose jobs are gated by group: an edit to one is judged per job (workflow_jobs_touched).
CI_HEAVY_WORKFLOWS = ("ci.yaml", "fuzz-stress-memleak.yaml", "security.yaml", "codeql.yaml", "parity.yaml")
# The classifier, its wiring and its fixtures: a change to them never scopes itself.
CI_SCOPE_SELF = (
    "scripts/ci/pr_classify.py",
    "scripts/ci/check_changelog_pr.py",
    "scripts/ci/tests/ci_scope_replay_check.py",
    "scripts/ci/tests/testdata/ci_scope_replay.json",
)
CI_SCOPE_SELF_PREFIXES = (".github/actions/ci-scope/",)
# Paths of the parity group that are not product code.
CI_PARITY_TOOLS = (
    "Dockerfile.datagen",
    "tests/parity/docker-compose.yml",
    "deployment/docker/Dockerfile.upstream-probe",
    "scripts/ci/parity_ratchet.py",
    "scripts/ci/requirements.txt",
)
# Scripts that a gated job runs: a change forces those jobs (a static test checks the paths exist).
CI_CONSUMERS: "tuple[tuple[str, tuple[str, ...]], ...]" = (
    ("scripts/ci/sharded_go_test.sh", ("ci.yaml:test-logs", "ci.yaml:test-traces", "ci.yaml:test-parquets3-heavy")),
    ("scripts/ci/shard_tests.py", ("ci.yaml:test-logs", "ci.yaml:test-traces", "ci.yaml:test-parquets3-heavy")),
    ("scripts/ci/short_gated_tests.sh", ("ci.yaml:test-parquets3-heavy",)),
    ("scripts/ci/gotest_report.py", ("ci.yaml:test-logs", "ci.yaml:test-traces", "ci.yaml:benchmarks-logs", "ci.yaml:benchmarks-traces")),
    ("scripts/bench/field_metadata/*", ("ci.yaml:field-metadata-perf",)),
    ("scripts/bench/compaction/ab.py", ("ci.yaml:storage-health-compaction",)),
    ("scripts/bench/compaction/test_ab.py", ("ci.yaml:storage-health-compaction",)),
    ("scripts/ci/parquet-readback/*", ("ci.yaml:parquet-readback",)),
    ("scripts/ci/trivy_fetch_db.sh", ("security.yaml:trivy-image",)),
)

# `uses: owner/repo[/path]@<ref>` with any ref (a tag, a branch or a SHA) and an optional trailing comment.
# Local actions (`./...`) and `docker://` references carry no `@ref` and never match.
_USES_REF = re.compile(
    r"^(?:-[ \t]+)?uses:[ \t]*(?P<name>[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+)@(?P<ref>[A-Za-z0-9_./-]+)(?:[ \t]+#.*)?[ \t]*$"
)
_JOBS_KEY = re.compile(r"^jobs:[ \t]*(?:#.*)?$")
_JOB_KEY = re.compile(r"^  ([A-Za-z0-9_-]+):[ \t]*(?:#.*)?$")
_HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def workflow_job_owners(text: str) -> "tuple[list[str | None], list[str]] | None":
    """(owner, job keys). owner[n] is the job key line n (1-based; index 0 unused) belongs to, None
    outside `jobs:`. The scan is stdlib only: a two-space key under a top-level `jobs:` starts a job, a
    top-level key ends the block, comment and blank lines stay with the job above. None when the file has
    no `jobs:` block or no job."""
    owner: "list[str | None]" = [None]
    keys: list[str] = []
    in_jobs, cur = False, None
    for line in text.split("\n"):
        if not in_jobs:
            if _JOBS_KEY.match(line):
                in_jobs = True
            owner.append(None)
            continue
        if line and not line[0].isspace() and not line.startswith("#"):
            in_jobs, cur = False, None
            owner.append(None)
            continue
        m = _JOB_KEY.match(line)
        if m:
            cur = m.group(1)
            keys.append(cur)
        owner.append(cur)
    return (owner, keys) if keys else None


def workflow_jobs_touched(base_text: str, head_text: str, diff_text: str) -> "tuple[tuple[str, ...], str] | None":
    """The jobs of one workflow that a change to it forces, as (job keys, what was verified); None when it
    cannot be judged (the caller then runs everything).

    A changed line is judged by the job it lies in (old numbering for a removed line, new numbering for an
    added one). The `uses:` lines are a special case: when every removed `uses: <action>@<ref>` line has an
    added line naming the same action (and the other way round), the change is a ref bump and forces
    nothing. A line outside every job (`on:`, `env:`, `permissions:`, `concurrency:` ...) runs the whole
    workflow, i.e. every job of it."""
    base_map, head_map = workflow_job_owners(base_text), workflow_job_owners(head_text)
    if base_map is None or head_map is None:
        return None
    base_owner, _ = base_map
    head_owner, head_keys = head_map
    uses: "list[tuple[str, str, str | None]]" = []  # (sign, action name, owning job)
    forced: set[str] = set()
    whole = False
    old = new = 0
    changed = False
    for line in diff_text.split("\n"):
        m = _HUNK.match(line)
        if m:
            old, new = int(m.group(1)), int(m.group(3))
            continue
        # the file headers come before the first hunk (old and new are still 0 there)
        if not line or line[0] not in "+-" or not (old or new):
            continue
        sign = line[0]
        lineno = old if sign == "-" else new
        owners = base_owner if sign == "-" else head_owner
        job = owners[lineno] if 0 < lineno < len(owners) else None
        if sign == "-":
            old += 1
        else:
            new += 1
        changed = True
        u = _USES_REF.match(line[1:].lstrip(" \t"))
        if u:
            uses.append((sign, u.group("name"), job))
        elif job is None:
            whole = True
        else:
            forced.add(job)
    if not changed:
        return None
    # the removed and the added pins must name the same actions in the same jobs: a pin that moved to
    # another job is an added or removed step
    added = sorted((j or "", n) for s, n, j in uses if s == "+")
    removed = sorted((j or "", n) for s, n, j in uses if s == "-")
    if added != removed:  # a different action, an added or a removed step: judged like any other line
        for _, _, job in uses:
            if job is None:
                whole = True
            else:
                forced.add(job)
    if whole:
        return tuple(sorted(set(head_keys) | forced)), "a line outside every job changed: the whole workflow runs"
    if not forced:
        return (), f"only the ref of {len(added)} uses: pin(s) changed"
    return tuple(sorted(forced)), "lines changed inside the jobs " + ", ".join(sorted(forced))


class GitEvidence:
    """What the scope rules read from the diff of one change (git-backed)."""

    def __init__(self, base: str, head: str) -> None:
        self.base, self.head = base, head

    def image_pins_only(self, path: str) -> bool:
        return dockerfile_pins_only(self.base, self.head, path)

    def workflow_jobs(self, path: str) -> "tuple[tuple[str, ...], str] | None":
        b, h = show_text(self.base, path), show_text(self.head, path)
        if not b or not h:
            return None
        return workflow_jobs_touched(b, h, git("diff", "--no-renames", "--text", "--no-textconv", "--no-ext-diff", "--no-color", "-U0", self.base, self.head, "--", path))


def ci_scope_full(reason: str, detail: str = "") -> "dict[str, str]":
    """The fail-open answer: every group runs."""
    return {
        "ci_class": "product", "skip_go": "false", "skip_image": "false", "skip_parity": "false",
        "force": "", "ci_reason": reason, "ci_detail": detail,
    }


def _ci_label(path: str) -> str:
    """The class an infra path belongs to (for the report only; it decides nothing)."""
    name = os.path.basename(path)
    if path.endswith(".md") or path.startswith(("docs/", "website/")):
        return "docs"
    if path.startswith(".github/workflows/"):
        return "ci"
    if path.startswith("deployment/") or path.startswith(("tests/s3compat/compose/", "tests/readers/", "tests/parity/")):
        return "deps" if cc.dependency_file_kind(path) == "file" and cc._REQUIREMENTS_NAME.match(name) else "stack"
    if cc.dependency_file_kind(path) == "file":
        return "deps" if cc._REQUIREMENTS_NAME.match(name) or name in ("package.json", "package-lock.json") else "stack"
    return "ci"


def _ci_path(path: str, ev: "GitEvidence") -> "str | tuple[str, bool, bool, tuple[str, ...], str]":
    """For an infra path: (label, needs image, needs parity, forced jobs, note). For a path that needs
    everything: the reason, as a str."""
    name = os.path.basename(path)
    if clean_text(path.encode("utf-8", "surrogateescape")) is None or "\n" in path or path.startswith("/") or ".." in path.split("/"):
        return "path is not clean text: " + path
    if path in CI_SCOPE_SELF or path.startswith(CI_SCOPE_SELF_PREFIXES):
        return "the classifier or its wiring: " + path
    if path.endswith(".go") or name in ("go.mod", "go.sum", "go.work", "Makefile") or name.startswith(".golangci"):
        return "Go source, module or build file: " + path
    if path.startswith(("alerts/", "dashboards/")):
        return "shipped alert or dashboard: " + path
    # root build files: a digest-only change of the FROM images keeps the image build and its scan
    if path == "Dockerfile.datagen":
        return ("stack", False, True, (), "")
    if "/" not in path and name.startswith("Dockerfile"):
        if ev.image_pins_only(path):
            return ("release-image-pin", True, True, (), "only the digest pins of FROM images changed in " + path)
        return "shipped build file: " + path
    # product trees, patches, charts: only their prose is documentation
    if path.startswith(PRODUCT_TREES):
        return ("docs", False, False, (), "") if name in PRODUCT_DOC_NAMES else "product code: " + path
    if path.startswith("patches/"):  # its .md and .txt files are read by the build and by tests: no prose exception
        return "patched: " + path
    if path.startswith("charts/"):
        if path.endswith((".md", ".txt")) and "/templates/" not in path:
            return ("docs", False, False, (), "")
        return "packaged: " + path
    floor = product_reason(path)
    if floor:
        return floor
    if path in CI_PARITY_TOOLS:
        return (_ci_label(path), False, True, (), "")
    if path.startswith("tests/"):
        if path.endswith(".md"):
            return ("docs", False, False, (), "")
        if path.startswith(("tests/readers/", "tests/s3compat/compose/", "tests/playwright/", "tests/verification/")):
            return (_ci_label(path), False, False, (), "")
        return "test tree outside the infra allowlist: " + path
    if path.startswith(".github/workflows/") and name in CI_HEAVY_WORKFLOWS:
        verdict = ev.workflow_jobs(path)
        if verdict is None:
            return "workflow edit that cannot be judged per job: " + path
        jobs, note = verdict
        return (
            "deps" if not jobs else "ci", False, name == "parity.yaml" and bool(jobs),
            tuple(f"{name}:{j}" for j in jobs), f"{path}: {note}",
        )
    if path.startswith(".github/"):
        return ("ci", False, False, (), "")
    if path.startswith(("scripts/", "deployment/")):
        for pattern, jobs in CI_CONSUMERS:
            if fnmatch.fnmatchcase(path, pattern):
                return ("ci", False, False, jobs, "")
        return (_ci_label(path), False, False, (), "")
    if path.startswith(("docs/", "website/")) or ("/" not in path and path.endswith(".md")):
        return ("docs", False, False, (), "")
    return "path outside the infra allowlist: " + path


def ci_scope_decide(files: "list[str]", ev: "GitEvidence", release_meta: bool) -> "dict[str, str]":
    """The scope of one change. `ev` supplies the two content checks (image_pins_only, workflow_jobs);
    release_meta is the registry gate's exact release-metadata verdict."""
    if not files:
        return ci_scope_full("empty diff")
    if release_meta:
        return {
            "ci_class": "release-metadata", "skip_go": "true", "skip_image": "true", "skip_parity": "true",
            "force": "", "ci_reason": "", "ci_detail": "exact release-metadata shape (CHANGELOG, Chart.yaml, regenerated docs)",
        }
    labels: set[str] = set()
    image = parity = False
    force: set[str] = set()
    notes: list[str] = []
    for f in files:
        res = _ci_path(f, ev)
        if isinstance(res, str):
            return ci_scope_full(res)
        label, need_image, need_parity, jobs, note = res
        labels.add(label)
        image, parity = image or need_image, parity or need_parity
        force.update(jobs)
        if note:
            notes.append(note)
    cls = next(iter(labels)) if len(labels) == 1 else "mixed-infra"
    detail = f"{len(files)} path(s), all on the infra allowlist"
    if notes:
        detail += "; " + "; ".join(notes)
    return {
        "ci_class": cls, "skip_go": "true", "skip_image": "false" if image else "true",
        "skip_parity": "false" if parity else "true", "force": ",".join(sorted(force)),
        "ci_reason": "", "ci_detail": detail,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="merge base")
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--title", default="", help="ignored: no kind is decided from a title or a commit subject")
    ap.add_argument("--author", default="", help="PR author login (release-metadata exemption)")
    ap.add_argument("--approvers", default="", help="file with approver logins, read from the merge base")
    ap.add_argument("--ci-scope", action="store_true", help="after the registry-gate keys, print which CI groups the change needs")
    ap.add_argument("--event", default="", help="with --ci-scope: the workflow event; only pull_request is scoped, anything else is a full run")
    a = ap.parse_args()

    # NUL-separated: a path with a newline, a CR or a Unicode separator stays one path
    files = [f for f in git("diff", "--no-renames", "--name-only", "-z", a.base, a.head).split("\0") if f]
    approvers = set()
    if a.approvers and os.path.exists(a.approvers):
        with open(a.approvers, encoding="utf-8") as fh:
            for line in fh.read().split("\n"):
                line = line.split("#", 1)[0].strip().lower()
                if line:
                    approvers.add(line)

    exempt, detail = "none", ""
    if release_metadata(files, a.base, a.head, a.author, approvers):
        exempt = "release-metadata"
        detail = "exact release-metadata shape (CHANGELOG, Chart.yaml, regenerated docs)"
    elif dependency_only(files, a.base, a.head):
        exempt = "dependency-only"
        detail = "only go.mod require versions outside the storage-critical modules, go.sum hash lines and requirements files changed"
    elif (tc := toolchain_only(files, a.base, a.head)) is not None:
        exempt = "toolchain-only"
        detail = tc

    reason = ""
    if exempt == "none":
        pins = []
        for f in files:
            # a release Dockerfile whose only change is the digest of its FROM images is a pin refresh,
            # not a product change (image_pin_only); any other change to it stays a product change
            if dockerfile_pins_only(a.base, a.head, f):
                pins.append(f)
                continue
            r = product_reason(f)
            if r:
                reason = r
                break
            if f == "Makefile":
                b, h = show_text(a.base, f), show_text(a.head, f)
                if b is None or h is None:
                    reason = "the Makefile is not clean text: " + f
                    break
                if makefile_pins(b) != makefile_pins(h):
                    reason = "upstream pin changed: " + f
                    break
        if pins and not reason:
            detail = "only the digest pins of FROM images changed in " + ", ".join(pins)
    print(f"exempt={exempt}")
    print(f"product={1 if reason else 0}")
    print(f"reason={printable(reason)}")
    print(f"detail={printable(detail)}")
    for f in files:
        if GATE_FILES_RE.match(f):
            print(f"gate_file={printable(f)}")
    if a.ci_scope:
        # fail open: every error and every event but a pull request means "everything runs"
        try:
            if a.event != CI_EVENT_SCOPED:
                scope = ci_scope_full("event " + (printable(a.event) or "unset") + ": always a full run")
            else:
                scope = ci_scope_decide(files, GitEvidence(a.base, a.head), exempt == "release-metadata")
        except Exception as e:  # noqa: BLE001 - any failure of the scope logic must widen, never narrow, the run
            scope = ci_scope_full("classifier error: " + type(e).__name__)
        for key in CI_SCOPE_KEYS:
            print(f"{key}={printable(scope[key])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
