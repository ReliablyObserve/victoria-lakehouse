#!/usr/bin/env python3

from __future__ import annotations

import argparse
from collections import Counter
import pathlib
import re
import subprocess
import sys
from typing import Iterable


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


def changelog_release_shape(base_text: str, head_text: str) -> tuple[str, str] | None:
    """(old, new) version when head is base plus exactly one new release, else None.

    The only change allowed is the one a release makes: a new version section
    directly below [Unreleased], greater than the newest one, and text moving
    between [Unreleased], the new section and the previously newest section (a
    merge of main into the metadata branch moves bullets between them). Older
    sections must be byte-identical; no line may be lost, added or duplicated.
    """
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
    if _zone(movable_base) != _zone(movable_head):
        return None
    return old, new


_PLACEHOLDER = "@@"


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
    after_old = re.compile(r"the release after v?`?" + re.escape(old) + r"`?")
    head_forms = [
        re.compile(r"`" + re.escape(new) + r"`, the release after `" + re.escape(new) + r"`"),
        re.compile(r"the release after v?`?" + re.escape(new) + r"`?"),
        re.compile(r"v" + re.escape(new) + r"(?![\d.])"),
        re.compile(r"`" + re.escape(new) + r"`"),
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
        if nx != ny or _PLACEHOLDER not in nx:
            return False
    return True


def is_release_metadata_sync(files: Iterable[str], generated_ok: bool = False) -> bool:
    """CHANGELOG plus release-metadata files only. Generated files count when
    ``generated_ok`` (their diff was checked to be version naming only)."""
    file_list = [f for f in files if f.strip()]
    if not file_list or "CHANGELOG.md" not in file_list:
        return False
    return all(
        path in RELEASE_METADATA_FILES
        or (generated_ok and path in RELEASE_METADATA_GENERATED_FILES)
        for path in file_list
    )


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
    files = run_git("diff", "--name-only", f"{base}..{args.head}").splitlines()
    commits = run_git("log", "--pretty=format:%s", f"{base}..{args.head}").splitlines()

    base_text = run_git("show", f"{base}:CHANGELOG.md")
    head_text = run_git("show", f"{args.head}:CHANGELOG.md")
    base_unreleased = extract_unreleased_section(base_text)
    head_unreleased = extract_unreleased_section(head_text)

    if is_dependency_only_pr(commits, files):
        print("changelog gate: skipped (dependency-only update)")
        return 0

    shape = changelog_release_shape(base_text, head_text)
    generated_ok = shape is not None and all(
        generated_docs_version_naming_only(
            _show(base, f), _show(args.head, f), shape[0], shape[1]
        )
        for f in files
        if f in RELEASE_METADATA_GENERATED_FILES
    )
    if is_release_metadata_sync(files, generated_ok=generated_ok):
        if head_unreleased.strip() == base_unreleased.strip() and not adds_version_section(head_text, base_text):
            print(
                "changelog gate: release metadata sync must materialize Unreleased into a version section",
                file=sys.stderr,
            )
            return 1
        print("changelog gate: ok (release metadata sync)")
        return 0

    if not should_require_changelog(commits, files):
        print("changelog gate: skipped (no releasable changes detected)")
        return 0

    if "CHANGELOG.md" not in files:
        print(
            "changelog gate: CHANGELOG.md must be updated for feature/fix/perf or release-impacting PRs",
            file=sys.stderr,
        )
        return 1

    # A PR documents its change either under [Unreleased] (the normal flow) or
    # directly under a newly added/backfilled version section (the release flow).
    # Both satisfy the "document your change" intent; either path is accepted.
    unreleased_has_new = (
        has_meaningful_changelog_content(head_unreleased)
        and has_genuinely_new_unreleased_entries(head_unreleased, base_text)
    )
    versioned_has_new = has_new_versioned_entries(head_text, base_text)

    if not (unreleased_has_new or versioned_has_new):
        print(
            "changelog gate: PR must add at least one new changelog entry under [Unreleased] "
            "or under a newly added version section "
            "(stale feature branch entries carried over from before a release do not count)",
            file=sys.stderr,
        )
        return 1

    print("changelog gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
