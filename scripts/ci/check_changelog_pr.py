#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import Counter
import os
import pathlib
import re
import subprocess
import sys
from typing import Callable, Iterable, NamedTuple


ROOT = pathlib.Path(__file__).resolve().parents[2]
CHANGELOG = ROOT / "CHANGELOG.md"

RELEASE_PREFIXES = (
    "feat",
    "fix",
    "perf",
    "revert",
)

NON_RELEASE_SCOPES = frozenset((
    "ci",
    "build",
    "deps",
    "deps-dev",
    "chore",
))

DEPENDENCY_UPDATE_PREFIXES = (
    "build(deps):",
    "build(deps-dev):",
)

IMPACTFUL_PATHS = (
    "cmd/",
    "internal/",
    "charts/",
    "deployment/",
)

UNIT_TEST_PATH_PREFIXES = (
    "cmd/",
    "internal/",
    # The traces binary is its own module with the same layout.
    "lakehouse-traces/internal/",
    "lakehouse-traces/cmd/",
)

IMPACTFUL_FILES = {
    "Dockerfile",
    "go.mod",
    "go.sum",
}

EXEMPT_PATH_PREFIXES = (
    ".github/",
    "website/",
    "scripts/ci/",
    "scripts/proof/",
)

NON_RELEASE_PATH_PREFIXES = (
    "docs/",
    # A changelog fragment documents a change; it is not one.
    "changelog.d/",
)

NON_RELEASE_FILES = {
    "README.md",
    "CHANGELOG.md",
    "LICENSE",
    # Test-stack only: the loki-vl-proxy image used by the e2e/proof/benchmark
    # compose files, never shipped. Exact file, not a prefix, so the shipped
    # Dockerfiles under deployment/ stay release-impacting.
    "deployment/docker/Dockerfile.loki-vl-proxy",
}

RELEASE_METADATA_FILES = {
    "CHANGELOG.md",
    "README.md",
    "charts/victoria-lakehouse/Chart.yaml",
}


def run_git(*args: str, cwd: pathlib.Path | None = None) -> str:
    cwd = cwd or ROOT
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def comparison_base(base: str, head: str, cwd: pathlib.Path | None = None) -> str:
    """The commit the PR's own changes start from: merge-base(base, head).

    ``pull_request.base.sha`` is the base branch tip, which moves on after the
    branch was cut. Diffing that tip against the head (two dots) attributes
    every commit that landed on the base since then to the PR, as reverts: a
    release-metadata PR cut before another PR merged then "touches" that PR's
    files, stops looking like a metadata sync and fails the gate (#312, #321).
    """
    return run_git("merge-base", base, head, cwd=cwd)


def extract_unreleased_section(text: str) -> str:
    match = re.search(
        r"^## \[Unreleased\]\s*\n(?P<body>.*?)(?=^## \[|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if not match:
        return ""
    return match.group("body").strip()


# A markdown heading is one to six '#' followed by whitespace. A bare '#' that
# starts a wrapped line is ordinary text — an issue reference such as `#502)` —
# and treating it as a heading would cut the bullet short there, losing
# everything after it from the bullet's identity.
HEADING_RE = re.compile(r"#{1,6}\s")


def logical_bullets(lines: list[str]) -> list[str]:
    """Bullets as whole units, one per ``- `` line plus its wrapped remainder.

    A changelog bullet here is a paragraph, not a line: the entries are long
    enough that leaving them on one physical line makes the file unreadable in
    a diff or an editor, so bodies are wrapped. Wrapping must not change what a
    bullet IS, or re-wrapping an existing entry would read as adding a new one
    and the release gates below would fire on a purely cosmetic edit.

    So a bullet runs from its ``- `` marker to the next marker, heading or
    blank line, and its text is whitespace-normalised. Two bullets that differ
    only in where their lines break are the same bullet.
    """
    bullets: list[str] = []
    current: list[str] | None = None

    def flush() -> None:
        nonlocal current
        if current:
            bullets.append(" ".join(" ".join(current).split()))
        current = None

    blanks = 0
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- "):
            flush()
            current = [stripped]
            blanks = 0
        elif not stripped:
            # A blank line does NOT end a bullet on its own: a long entry is
            # broken into paragraphs, and a paragraph break inside a list item
            # is a blank line followed by an INDENTED continuation. Only a
            # blank line followed by something unindented ends the item.
            blanks += 1
        elif current is not None and not HEADING_RE.match(stripped):
            if blanks and not line.startswith(" "):
                flush()
            else:
                current.append(stripped)
            blanks = 0
        else:
            flush()
            blanks = 0
    flush()
    return bullets


def extract_bullet_points(text: str) -> set[str]:
    return set(logical_bullets(text.splitlines()))


def has_genuinely_new_unreleased_entries(head_unreleased: str, base_full_changelog: str) -> bool:
    base_all_bullets = extract_bullet_points(base_full_changelog)
    head_new_bullets = extract_bullet_points(head_unreleased) - base_all_bullets
    return bool(head_new_bullets)


def versioned_bullets(text: str) -> set[str]:
    """Bullet points that live under a released ``## [x.y.z]`` section.

    Excludes the ``## [Unreleased]`` section so the two documentation paths
    (Unreleased vs. a materialized/backfilled version section) stay distinct.
    """
    versioned: list[str] = []
    in_versioned = False
    for line in text.splitlines():
        if line.startswith("## ["):
            in_versioned = not line.startswith("## [Unreleased]")
            continue
        if in_versioned:
            versioned.append(line)
    return set(logical_bullets(versioned))


def has_new_versioned_entries(head_full_changelog: str, base_full_changelog: str) -> bool:
    """True when the PR adds a bullet under a version section that the base
    changelog did not already contain anywhere.

    This is the backfill/release path: a PR may document its change directly
    under a newly added ``## [x.y.z]`` heading (e.g. materializing Unreleased
    into a cut release, or backfilling a previously-undocumented tag) instead
    of leaving it under ``## [Unreleased]``. Stale branches that only carry
    forward bullets already present in the base are still rejected, because
    those bullets are subtracted out.
    """
    base_all_bullets = extract_bullet_points(base_full_changelog)
    return bool(versioned_bullets(head_full_changelog) - base_all_bullets)


def version_headings(text: str) -> set[str]:
    """Released version headings (``## [x.y.z]``), excluding ``[Unreleased]``."""
    return {
        m.group(1)
        for m in re.finditer(r"^## \[([^\]]+)\]", text, re.MULTILINE)
        if m.group(1) != "Unreleased"
    }


def adds_version_section(head_full_changelog: str, base_full_changelog: str) -> bool:
    """True when the head changelog carries a version heading the base lacks.

    A release-metadata sync normally materializes ``[Unreleased]`` into the new
    version. When a release is cut out of order (two release runs raced, or a
    tag is backfilled) ``[Unreleased]`` may already be empty on the base, so the
    sync can only be recognized by the version section it adds.
    """
    return bool(version_headings(head_full_changelog) - version_headings(base_full_changelog))


def has_meaningful_changelog_content(section: str) -> bool:
    if not section.strip():
        return False
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("### "):
            continue
        if stripped.startswith("- "):
            return True
        return True
    return False


def is_release_commit(subject: str) -> bool:
    lowered = subject.strip().lower()
    if "breaking change" in lowered:
        return True
    for prefix in RELEASE_PREFIXES:
        if lowered.startswith(prefix + ":"):
            return True
        if lowered.startswith(prefix + "("):
            scope_end = lowered.find(")", len(prefix) + 1)
            if scope_end > 0:
                scope = lowered[len(prefix) + 1 : scope_end]
                if scope in NON_RELEASE_SCOPES:
                    continue
            return True
    return False


def is_release_path(path: str) -> bool:
    if is_unit_test_only_path(path) or path in NON_RELEASE_FILES:
        return False
    if path in IMPACTFUL_FILES:
        return True
    return any(path.startswith(prefix) for prefix in IMPACTFUL_PATHS)


def is_exempt_path(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in EXEMPT_PATH_PREFIXES)


def is_non_release_path(path: str) -> bool:
    if is_unit_test_only_path(path):
        return True
    if is_exempt_path(path):
        return True
    if path in NON_RELEASE_FILES:
        return True
    return any(path.startswith(prefix) for prefix in NON_RELEASE_PATH_PREFIXES)


def is_unit_test_only_path(path: str) -> bool:
    return path.endswith("_test.go") and any(
        path.startswith(prefix) for prefix in UNIT_TEST_PATH_PREFIXES
    )


def should_require_changelog(commits: Iterable[str], files: Iterable[str]) -> bool:
    commit_list = [c for c in commits if c.strip()]
    file_list = [f for f in files if f.strip()]

    if file_list and all(is_exempt_path(f) for f in file_list):
        return False

    if any(is_release_commit(subject) for subject in commit_list):
        return True

    impactful = [f for f in file_list if is_release_path(f)]
    if impactful:
        return True

    non_release = [f for f in file_list if is_non_release_path(f)]
    return len(file_list) > 0 and len(non_release) != len(file_list)


def is_dependency_only_pr(commits: Iterable[str], files: Iterable[str]) -> bool:
    commit_list = [c for c in commits if c.strip()]
    file_list = [f for f in files if f.strip()]
    if not commit_list or not file_list:
        return False
    all_dep_commits = all(
        any(c.strip().lower().startswith(p) for p in DEPENDENCY_UPDATE_PREFIXES)
        for c in commit_list
    )
    if not all_dep_commits:
        return False
    return all(
        f in IMPACTFUL_FILES
        or is_non_release_path(f)
        or f.startswith(".github/")
        for f in file_list
    )


DEPENDABOT_LOGIN = "dependabot[bot]"

# Tooling and test trees where a dependency manifest, lock or pin file can be bumped by
# Dependabot without any change to what ships. Product Go modules (go.mod, go.sum), the
# release Dockerfiles at the repository root, charts/ and every source tree are not listed
# on purpose: a bump there changes the shipped binary or image and still needs an entry.
DEPENDENCY_TOOLING_PREFIXES = (
    ".github/",
    "deployment/",
    "scripts/",
    "tests/",
    "website/",
)
DEPENDENCY_NPM_PREFIXES = ("tests/", "website/")
DEPENDENCY_DOCKER_PREFIXES = ("deployment/", "scripts/", "tests/")

_REQUIREMENTS_NAME = re.compile(r"^(?:[A-Za-z0-9_.]+-)*requirements[A-Za-z0-9_.-]*\.(?:txt|in)$")
_COMPOSE_NAME = re.compile(r"^[A-Za-z0-9_.-]*compose[A-Za-z0-9_.-]*\.ya?ml$")
_DOCKERFILE_NAME = re.compile(r"^(?:Dockerfile(?:\.[A-Za-z0-9_.-]+)?|[A-Za-z0-9_.-]+\.Dockerfile)$")
_WORKFLOW_PATH = re.compile(r"^\.github/workflows/[A-Za-z0-9_.-]+\.ya?ml$")
# `uses: owner/repo[/path]@<40 hex> # vX.Y.Z`, as a diff line. Nothing else counts as a pin line.
_USES_DIFF_LINE = re.compile(
    r"^[+-]\s*(?:-\s+)?uses:\s*(?P<name>[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+)"
    r"@(?P<sha>[0-9a-f]{40})(?:\s+#.*)?$"
)


def is_dependabot_pr(author_login: str, head_repo: str, base_repo: str) -> bool:
    """The PR was opened by Dependabot from a branch of this repository.

    The inputs are the pull_request event's ``user.login``, ``head.repo.full_name`` and the
    workflow's ``github.repository``. Branch names, titles and commit authors are free text
    that anyone can set to look like Dependabot's, so none of them is consulted. A fork has a
    different head repository and a deleted fork has none: neither qualifies.
    """
    return bool(
        author_login == DEPENDABOT_LOGIN
        and head_repo
        and base_repo
        and head_repo == base_repo
    )


def dependency_file_kind(path: str) -> str | None:
    """``"file"`` for a dependency manifest/lock/pin file of a tooling or test tree, ``"uses"``
    for a workflow (only its ``uses:`` pin lines may change), ``None`` for anything else."""
    parts = path.split("/")
    if not path or path.startswith("/") or "\\" in path or ".." in parts or "" in parts:
        return None
    name = parts[-1]
    if _WORKFLOW_PATH.match(path):
        return "uses"
    if not path.startswith(DEPENDENCY_TOOLING_PREFIXES) or len(parts) < 2:
        return None
    if _REQUIREMENTS_NAME.match(name):
        return "file"
    if name in ("package.json", "package-lock.json") and path.startswith(DEPENDENCY_NPM_PREFIXES):
        return "file"
    if path.startswith(DEPENDENCY_DOCKER_PREFIXES):
        if _DOCKERFILE_NAME.match(name):
            return "file"
        if _COMPOSE_NAME.match(name):
            return "file"
        # The S3 compatibility stacks are Compose files whose names carry no "compose".
        if len(parts) == 4 and path.startswith("tests/s3compat/compose/") and re.search(r"\.ya?ml$", name):
            return "file"
    return None


def diff_changes_only_uses_pins(diff_text: str) -> bool:
    """True when a ``git diff -U0`` of one workflow changes nothing but SHA-pinned ``uses:``
    lines, and every removed pin has an added pin of the same action."""
    removed: Counter = Counter()
    added: Counter = Counter()
    in_hunk = False
    for line in diff_text.split("\n"):
        if line.startswith("@@"):
            in_hunk = True
            continue
        if not in_hunk or not line or line.startswith("\\"):
            continue
        if line[0] not in "+-":
            return False
        m = _USES_DIFF_LINE.match(line)
        if not m:
            return False
        (added if line[0] == "+" else removed)[m.group("name")] += 1
    return bool(added) and added == removed


def dependabot_dependency_only_diff(base: str, head: str) -> bool:
    """Every file the PR changes is a modified dependency file of a tooling or test tree.

    Fails closed: an added, deleted, renamed, retyped or mode-changed file, a path outside the
    allowed classes, a workflow change beyond ``uses:`` pins, or a git error is "not exempt".
    """
    try:
        raw = run_git("diff", "--no-renames", "--raw", "-z", f"{base}..{head}").split("\0")
        entries: list[tuple[str, str]] = []
        i = 0
        while i < len(raw):
            meta = raw[i]
            if not meta:
                i += 1
                continue
            fields = meta.split()
            if not meta.startswith(":") or len(fields) != 5 or i + 1 >= len(raw):
                return False
            src_mode, dst_mode, _, _, status = fields
            src_mode = src_mode.lstrip(":")
            if status != "M" or src_mode != "100644" or dst_mode != "100644":
                return False
            entries.append((status, raw[i + 1]))
            i += 2
        if not entries:
            return False
        for _, path in entries:
            kind = dependency_file_kind(path)
            if kind is None:
                return False
            if kind == "uses" and not diff_changes_only_uses_pins(
                run_git("diff", "--no-renames", "-U0", f"{base}..{head}", "--", path)
            ):
                return False
        return True
    except (subprocess.CalledProcessError, ValueError):
        return False


# Generated files whose only release-time change is the NAME of the version a
# feature shipped in (`since: the release after v0.146.3` becomes
# `since: v0.146.4` or `the release after v0.146.4`). A release that leaves a
# feature with an [Unreleased] bullet renames it, so the regenerated file belongs
# in the release-metadata PR. Exact paths; README.md and UPSTREAM_COVERAGE.md
# carry no version naming and are not listed.
RELEASE_METADATA_GENERATED_FILES = {
    "docs/features.md",
}

SEMVER_RE = r"\d+\.\d+\.\d+"
_SECTION = re.compile(r"^## \[([^\]]+)\]")
_VERSION_SECTION = re.compile(r"^## \[(" + SEMVER_RE + r")\] - \d{4}-\d{2}-\d{2}$")
_OLD_SECTION = re.compile(r"^## \[(" + SEMVER_RE + r")\]")


def _semver(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in v.split("."))


def parse_changelog(text: str) -> list[tuple[str, list[str]]]:
    """Sections as (heading line, body lines); the first is the preamble ("")."""
    sections: list[tuple[str, list[str]]] = [("", [])]
    for line in text.split("\n"):
        if _SECTION.match(line):
            sections.append((line, []))
        else:
            sections[-1][1].append(line)
    return sections


def _zone(sections: list[tuple[str, list[str]]]) -> Counter:
    """Non-blank, non-subheading lines (unstripped) of the given sections."""
    return Counter(
        l for _, body in sections for l in body if l.strip() and not l.startswith("### ")
    )


KEEP_A_CHANGELOG = {"### " + n for n in ("Added", "Changed", "Deprecated", "Removed", "Fixed", "Security")}


def _subheadings(sections: list[tuple[str, list[str]]]) -> Counter:
    return Counter(l.rstrip() for _, body in sections for l in body if l.startswith("### "))


def _subheadings_ok(base: list, head: list, new_section, fragment_headings: frozenset = frozenset()) -> bool:
    """Subheadings are structure, and structure is release notes too.

    Only the Keep-a-Changelog set may appear; none may be lost; and the only new
    ones are copies of a heading the base already has, or the heading of a kind of
    a fragment the release consumed, each at most once, in the new section (a
    release splits the bullets of one section across two).
    """
    b, h = _subheadings(base), _subheadings(head)
    if not set(h) <= KEEP_A_CHANGELOG or (b - h):
        return False
    extra = h - b
    new_subs = _subheadings([new_section])
    return all(n == 1 and (k in b or k in fragment_headings) and k in new_subs for k, n in extra.items())


def changelog_release_shape(
    base_text: str, head_text: str, fragments: Iterable[Fragment] = ()
) -> tuple[str, str] | None:
    """(old, new) version when head is base plus exactly one new release, else None.

    The only change allowed is the one a release makes: a new version section
    directly below [Unreleased], greater than the newest one, and text moving
    between [Unreleased], the new section and the previously newest section (a
    merge of main into the metadata branch moves bullets between them). Older
    sections must be byte-identical; no line may be lost, added or duplicated.
    ``fragments`` are the changelog.d files the release removed: their lines join
    the text [Unreleased] held, so the new section is that text plus those lines.
    """
    fragments = list(fragments)
    b, h = parse_changelog(base_text), parse_changelog(head_text)
    if len(b) < 2 or len(h) != len(b) + 1 or b[1][0] != "## [Unreleased]" or h[1][0] != "## [Unreleased]":
        return None
    if b[0] != h[0]:
        return None
    new_m = _VERSION_SECTION.match(h[2][0])
    if not new_m:
        return None
    new = new_m.group(1)
    old = ""
    movable_base = [b[1]]
    movable_head = [h[1], h[2]]
    if len(b) > 2:
        old_m = _OLD_SECTION.match(b[2][0])
        if not old_m or h[3][0] != b[2][0]:
            return None
        old = old_m.group(1)
        if _semver(new) <= _semver(old):
            return None
        movable_base.append(b[2])
        movable_head.append(h[3])
        if h[4:] != b[3:]:
            return None
    if _zone(movable_base) + fragment_zone(fragments) != _zone(movable_head):
        return None
    if not _subheadings_ok(movable_base, movable_head, h[2], fragment_headings(fragments)):
        return None
    return old, new


_PLACEHOLDER = "\x00VER\x00"


def generated_docs_version_naming_only(base_text: str, head_text: str, old: str, new: str) -> bool:
    r"""True when the only differences are the renaming of version ``old`` to ``new``.

    Compared line by line, in order. A differing line must be a ``since:`` or
    ``Changelog:`` line of the same feature (same leading text up to ``since:``),
    and may only turn the reference to the previous newest release
    (``the release after vOLD``) into the release that shipped (``vNEW``,
    ``\`NEW\``) or into the release after it. Every other version, in particular
    a ``since:`` swapped between features, must stay byte-identical.
    """
    bl, hl = base_text.split("\n"), head_text.split("\n")
    if len(bl) != len(hl) or not old or not new or old == new:
        return False
    end = r"(?!\.?\d)"  # v1.2.3 is not v1.2.30
    after_old = re.compile(r"the release after v?`?" + re.escape(old) + r"`?" + end)
    head_forms = [
        re.compile(r"`" + re.escape(new) + r"`, the release after `" + re.escape(new) + r"`" + end),
        re.compile(r"the release after v?`?" + re.escape(new) + r"`?" + end),
        re.compile(r"v" + re.escape(new) + end),
        re.compile(r"`" + re.escape(new) + r"`" + end),
    ]
    for x, y in zip(bl, hl):
        if x == y:
            continue
        if not ("since:" in x or "Changelog:" in x) or not ("since:" in y or "Changelog:" in y):
            return False
        nx = after_old.sub(_PLACEHOLDER, x)
        ny = y
        for pat in head_forms:
            ny = pat.sub(_PLACEHOLDER, ny)
        if nx != ny:
            return False
    return True


def is_release_metadata_sync(
    files: Iterable[str], generated_ok: bool = False, fragment_paths: frozenset = frozenset()
) -> bool:
    """CHANGELOG plus release-metadata files only. Generated files count when
    ``generated_ok`` (their diff was checked to be version naming only); the
    changelog.d files in ``fragment_paths`` (checked to be plain deletions of
    valid fragments) count too."""
    file_list = [f for f in files if f.strip()]
    if not file_list or "CHANGELOG.md" not in file_list:
        return False
    return all(
        path in RELEASE_METADATA_FILES
        or path in fragment_paths
        or (generated_ok and path in RELEASE_METADATA_GENERATED_FILES)
        for path in file_list
    )


# ---------------------------------------------------------- fragments --
#
# A change is documented by ONE small file, changelog.d/<PR-or-slug>.<kind>.md, whose whole content
# is the bullet as it will read in CHANGELOG.md. Nothing but the release edits CHANGELOG.md: the
# release job (scripts/ci/changelog_fragments.py) turns the fragments present at the release commit
# into the new version section and removes them, so a fragment is released exactly once, in the
# release whose tag contains it, and two PRs never touch the same lines.

FRAGMENT_DIR = "changelog.d"
FRAGMENT_KINDS = ("added", "changed", "deprecated", "removed", "fixed", "security")
# Matched with fullmatch: `$` would also match before a trailing newline in a file name.
FRAGMENT_NAME_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,80}\.(" + "|".join(FRAGMENT_KINDS) + r")\.md")
FRAGMENT_README = "README.md"
# A GitHub release body is limited to about 125k characters; one fragment never needs more than this.
FRAGMENT_MAX_BYTES = 16 * 1024
# The same pattern as TAG_RE of the release workflow: only plain release tags take part.
RELEASE_TAG_RE = re.compile(r"v(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})")
_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
_LEAD_IN_RE = re.compile(r"^- \*\*[^*].*?\*\*")


class Fragment(NamedTuple):
    path: str  # changelog.d/<name>
    kind: str  # one of FRAGMENT_KINDS
    lines: tuple  # the bullet's lines, right-trimmed, no blank line at either end

    @property
    def text(self) -> str:
        return " ".join(" ".join(self.lines).split())

    @property
    def lead_in(self) -> str:
        parts = self.text.split("**")
        return parts[1] if len(parts) > 2 else ""


def bullet_lead_in(text: str) -> str:
    """The bold lead-in of a whitespace-normalised bullet (``- **Lead.** body``), "" when it has none."""
    parts = text.split("**")
    return parts[1] if text.startswith("- **") and len(parts) > 2 else ""


def released_lead_ins(changelog_text: str) -> set[str]:
    """The bold lead-ins of every bullet that sits under a released ``## [x.y.z]`` section."""
    return {bullet_lead_in(b) for b in versioned_bullets(changelog_text)} - {""}


def release_tags(rev: str, cwd: pathlib.Path | None = None) -> list[str]:
    """The plain ``vMAJOR.MINOR.PATCH`` tags reachable from ``rev``, oldest version first. A tag with
    any other name (``foo-stray``, ``v1abc``, ``v1.2.3-rc1``) is no release and is ignored."""
    try:
        out = run_git("tag", "--merged", rev, "-l", "v[0-9]*", cwd=cwd)
    except subprocess.CalledProcessError:
        return []
    tags = [t for t in out.splitlines() if RELEASE_TAG_RE.fullmatch(t)]
    return sorted(tags, key=lambda t: tuple(int(x) for x in t[1:].split(".")))


def latest_release_tag(rev: str, cwd: pathlib.Path | None = None) -> str:
    """The newest release tag reachable from ``rev``, "" when there is none."""
    tags = release_tags(rev, cwd)
    return tags[-1] if tags else ""


def fragment_kind(path: str) -> str | None:
    """The kind of ``changelog.d/<name>.<kind>.md``, None for any other path."""
    head, _, name = path.rpartition("/")
    if head != FRAGMENT_DIR:
        return None
    m = FRAGMENT_NAME_RE.fullmatch(name)
    return m.group(1) if m else None


def parse_fragment(path: str, text: str) -> tuple[Fragment | None, list[str]]:
    """(fragment, []) or (None, reasons). The content is exactly one changelog bullet."""
    kind = fragment_kind(path)
    if kind is None:
        return None, [
            f"{path}: not a fragment name; use {FRAGMENT_DIR}/<PR-or-slug>.<{'|'.join(FRAGMENT_KINDS)}>.md "
            "(lower-case letters, digits, '.', '_' and '-')"
        ]
    if len(text.encode("utf-8", "surrogateescape")) > FRAGMENT_MAX_BYTES:
        return None, [f"{path}: larger than {FRAGMENT_MAX_BYTES} bytes; a release note is not the place for it"]
    if "\r" in text or "\t" in text or "\x00" in text:
        return None, [f"{path}: carriage returns, tabs and NUL are not allowed"]
    if "<!--" in _COMMENT_RE.sub("", text):
        return None, [f"{path}: an HTML comment that is not closed would hide the rest of the changelog and the release notes"]
    lines = [l.rstrip() for l in text.split("\n")]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    if not lines:
        return None, [f"{path}: empty fragment"]
    flat = " ".join(" ".join(lines).split())
    if not _LEAD_IN_RE.match(flat):
        return None, [f"{path}: must start with '- **<bold lead-in>**' (the repository's bullet style)"]
    problem = _not_one_item(lines)
    if problem:
        return None, [f"{path}: must be exactly one bullet ({problem}); a second paragraph or a sub-list of the same item is indented"]
    return Fragment(path, kind, tuple(lines)), []


def _not_one_item(lines: list[str]) -> str:
    """Why the lines are not ONE changelog item, "" when they are: no second '- ' item and no heading at
    column 0, and a paragraph after a blank line is indented (the repository's long-entry style)."""
    blank = False
    for line in lines[1:]:
        if not line:
            blank = True
        elif line.startswith(("- ", "* ", "+ ")):
            return "a second item"
        elif HEADING_RE.match(line):
            return "a heading"
        elif blank and not line.startswith(" "):
            return "an unindented second paragraph"
        else:
            blank = False
    return ""


def fragment_zone(fragments: Iterable[Fragment]) -> Counter:
    """The non-blank lines the fragments contribute to a release section, as ``_zone`` counts them."""
    return Counter(l for fr in fragments for l in fr.lines if l.strip())


def fragment_headings(fragments: Iterable[Fragment]) -> frozenset:
    return frozenset("### " + fr.kind.capitalize() for fr in fragments)


def duplicate_fragment_problems(fragments: Iterable[Fragment]) -> list[str]:
    """Two fragments with the same bullet or the same bold lead-in."""
    out: list[str] = []
    by_text: dict[str, str] = {}
    by_lead: dict[str, str] = {}
    for fr in sorted(fragments, key=lambda f: f.path):
        if fr.text in by_text:
            out.append(f"{fr.path}: the same bullet as {by_text[fr.text]}")
            continue
        by_text[fr.text] = fr.path
        if fr.lead_in in by_lead:
            out.append(f"{fr.path}: the same bold lead-in as {by_lead[fr.lead_in]}")
        else:
            by_lead[fr.lead_in] = fr.path
    return out


def metadata_fragment_deletions(
    files: Iterable[str],
    read_base: Callable[[str], "str | None"],
    read_head: Callable[[str], "str | None"],
) -> list[Fragment] | None:
    """The fragments a release-metadata PR removes; None when a changed changelog.d path is not
    the plain deletion of a valid fragment (an addition, an edit, a stray file)."""
    out: list[Fragment] = []
    for f in files:
        if not f.startswith(FRAGMENT_DIR + "/"):
            continue
        base_text, head_text = read_base(f), read_head(f)
        if not base_text or head_text:
            return None
        fr, problems = parse_fragment(f, base_text)
        if fr is None:
            return None
        out.append(fr)
    return out


def legacy_unreleased_window_open(base: str, cwd: pathlib.Path | None = None) -> bool:
    """A hand-written ``[Unreleased]`` bullet is still accepted next to fragments.

    That is the transition for pull requests opened before fragments existed, and it ends with the
    first release whose tag contains changelog.d/README.md: the newest release tag reachable from
    the base commit decides. The caller passes the base BRANCH TIP, not the merge base, so a branch
    cut before that release cannot keep the window open; replaying an older base gives the old
    answer. No reachable tag, or one from before fragments, leaves it open.
    """
    tag = latest_release_tag(base, cwd)
    if not tag:
        return True
    try:
        run_git("cat-file", "-e", f"{tag}:{FRAGMENT_DIR}/{FRAGMENT_README}", cwd=cwd)
    except subprocess.CalledProcessError:
        return True
    return False


def fragment_changes(base: str, head: str, cwd: pathlib.Path | None = None) -> dict[str, str]:
    """path -> git status letter (A, M, D) of every changed path under changelog.d/."""
    out = run_git("diff", "--no-renames", "--name-status", "-z", f"{base}..{head}", "--", FRAGMENT_DIR, cwd=cwd)
    parts = [p for p in out.split("\0") if p]
    return {parts[i + 1]: parts[i][0] for i in range(0, len(parts) - 1, 2)}


def _ls_tree(rev: str, cwd: pathlib.Path | None = None) -> dict[str, tuple[str, str]]:
    """path -> (mode, blob id) of every file under changelog.d/ at ``rev``; NUL-separated, so a file
    name with a newline in it stays one name."""
    raw = subprocess.run(
        ["git", "ls-tree", "-r", "-z", rev, "--", FRAGMENT_DIR],
        cwd=cwd or ROOT, capture_output=True, check=True,
    ).stdout.decode("utf-8", "surrogateescape")
    out: dict[str, tuple[str, str]] = {}
    for entry in raw.split("\0"):
        meta, _, path = entry.partition("\t")
        if path:
            mode, _typ, blob = meta.split()
            out[path] = (mode, blob)
    return out


def fragment_tree_problems(
    head: str, changes: dict[str, str], cwd: pathlib.Path | None = None, released: Iterable[str] = ()
) -> list[str]:
    """Why the changelog.d/ files this PR adds or edits cannot be released, and any clash with the
    fragments already in the tree. ``released`` is the bold lead-ins of the released changelog
    sections: a fragment may not repeat one (it would be released twice)."""
    problems: list[str] = []
    parsed: list[Fragment] = []
    try:
        tree = _ls_tree(head, cwd)
    except subprocess.CalledProcessError:
        return ["changelog.d/: cannot be read"]
    released = set(released)
    for path in sorted(p for p, st in changes.items() if st in "AM"):
        if path == f"{FRAGMENT_DIR}/{FRAGMENT_README}":
            continue
        mode = tree.get(path, ("", ""))[0]
        if mode != "100644":
            problems.append(f"{path}: must be a regular file (git mode 100644), not a symlink, an executable or a directory")
            continue
        try:
            text = run_git("show", f"{head}:{path}", cwd=cwd)
        except subprocess.CalledProcessError:
            problems.append(f"{path}: unreadable")
            continue
        fr, why = parse_fragment(path, text)
        problems.extend(why)
        if fr is not None:
            parsed.append(fr)
            if fr.lead_in in released:
                problems.append(
                    f"{path}: its bold lead-in is already in a released section of CHANGELOG.md "
                    f"({fr.lead_in!r}); the change was released, so a new fragment would release it twice"
                )
    if parsed:
        everyone = list(parsed)
        seen = {fr.path for fr in parsed}
        for path, (mode, _blob) in sorted(tree.items()):
            if path in seen or fragment_kind(path) is None or mode != "100644":
                continue
            fr, _ = parse_fragment(path, run_git("show", f"{head}:{path}", cwd=cwd))
            if fr is not None:
                everyone.append(fr)
        mine = {fr.path for fr in parsed}
        problems.extend(p for p in duplicate_fragment_problems(everyone) if p.split(":")[0] in mine)
    return problems


def released_fragment_edits(base: str, merge_base: str, changes: dict[str, str], cwd: pathlib.Path | None = None) -> list[str]:
    """A PR edits or deletes a fragment that is byte-identical at the newest release tag reachable from
    ``base``: that release already took it, its metadata pull request removes it, and a change to it
    would be released again (reworded) or collide with that removal."""
    tag = latest_release_tag(base, cwd)
    if not tag:
        return []
    out: list[str] = []
    for path, st in sorted(changes.items()):
        if st not in "MD" or fragment_kind(path) is None:
            continue
        try:
            at_tag = run_git("rev-parse", f"{tag}:{path}", cwd=cwd)
            before = run_git("rev-parse", f"{merge_base}:{path}", cwd=cwd)
        except subprocess.CalledProcessError:
            continue
        if at_tag == before:
            out.append(
                f"{path}: already released in {tag} (unchanged since) and removed by that release's metadata "
                "pull request; add a new fragment instead of editing or deleting this one"
            )
    return out


def _show(rev: str, path: str) -> str:
    try:
        return run_git("show", f"{rev}:{path}")
    except subprocess.CalledProcessError:
        return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    args = parser.parse_args()

    base = comparison_base(args.base, args.head)
    files = [f for f in run_git("diff", "--name-only", "-z", f"{base}..{args.head}").split("\0") if f]
    commits = run_git("log", "--pretty=format:%s", f"{base}..{args.head}").splitlines()

    base_text = run_git("show", f"{base}:CHANGELOG.md")
    head_text = run_git("show", f"{args.head}:CHANGELOG.md")
    base_unreleased = extract_unreleased_section(base_text)
    head_unreleased = extract_unreleased_section(head_text)

    deleted = metadata_fragment_deletions(
        files, lambda f: _show(base, f) or None, lambda f: _show(args.head, f) or None
    )
    shape = changelog_release_shape(base_text, head_text, deleted or ())
    generated_ok = shape is not None and all(
        generated_docs_version_naming_only(
            _show(base, f), _show(args.head, f), shape[0], shape[1]
        )
        for f in files
        if f in RELEASE_METADATA_GENERATED_FILES
    )
    if deleted is not None and is_release_metadata_sync(
        files, generated_ok=generated_ok, fragment_paths=frozenset(fr.path for fr in deleted)
    ):
        if head_unreleased.strip() == base_unreleased.strip() and not adds_version_section(head_text, base_text):
            print(
                "changelog gate: release metadata sync must materialize Unreleased into a version section",
                file=sys.stderr,
            )
            return 1
        print("changelog gate: ok (release metadata sync)")
        return 0

    # A fragment that cannot be released must fail here, whatever else the PR changes and before any
    # of the skips below: the release job assembles every fragment present at the release commit, so
    # one bad file on main stops the release. A fragment the newest release already took is not edited.
    changes = fragment_changes(base, args.head)
    problems = fragment_tree_problems(args.head, changes, released=released_lead_ins(head_text))
    problems += released_fragment_edits(args.base, base, changes)
    if problems:
        for problem in problems:
            print(f"changelog gate: {problem}", file=sys.stderr)
        return 1
    added_fragments = sorted(
        p for p, st in changes.items() if st == "A" and fragment_kind(p) is not None
    )

    # Author identity comes from the pull_request event (PR_AUTHOR_LOGIN, PR_HEAD_REPO, PR_BASE_REPO),
    # never from the branch name, the title or a commit: those are free text.
    if is_dependabot_pr(
        os.environ.get("PR_AUTHOR_LOGIN", ""),
        os.environ.get("PR_HEAD_REPO", ""),
        os.environ.get("PR_BASE_REPO", ""),
    ) and dependabot_dependency_only_diff(base, args.head):
        print(
            "changelog gate: skipped (Dependabot PR that changes only dependency manifest, lock "
            "or pin files of tooling and test trees)"
        )
        return 0

    if is_dependency_only_pr(commits, files):
        print("changelog gate: skipped (dependency-only update)")
        return 0

    # The transition window is judged from the base branch tip, not the merge base.
    legacy_open = legacy_unreleased_window_open(args.base)
    if head_unreleased.strip() != base_unreleased.strip() and not legacy_open:
        print(
            "changelog gate: [Unreleased] in CHANGELOG.md stays empty; document the change in a fragment, "
            f"{FRAGMENT_DIR}/<PR-or-slug>.<{'|'.join(FRAGMENT_KINDS)}>.md (see {FRAGMENT_DIR}/{FRAGMENT_README})",
            file=sys.stderr,
        )
        return 1

    if not should_require_changelog(commits, files):
        print("changelog gate: skipped (no releasable changes detected)")
        return 0

    # A PR documents its change in a new fragment (the flow), or, only until the first release that
    # contains changelog.d/ (legacy_unreleased_window_open), under [Unreleased]; or directly under a
    # newly added/backfilled version section (the release flow).
    unreleased_has_new = (
        legacy_open
        and has_meaningful_changelog_content(head_unreleased)
        and has_genuinely_new_unreleased_entries(head_unreleased, base_text)
    )
    versioned_has_new = has_new_versioned_entries(head_text, base_text)

    if not (added_fragments or unreleased_has_new or versioned_has_new):
        print(
            "changelog gate: PR must add a changelog fragment, "
            f"{FRAGMENT_DIR}/<PR-or-slug>.<{'|'.join(FRAGMENT_KINDS)}>.md holding the one bullet "
            f"(see {FRAGMENT_DIR}/{FRAGMENT_README}); "
            "a fragment carried over from before a release does not count",
            file=sys.stderr,
        )
        return 1

    print("changelog gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
