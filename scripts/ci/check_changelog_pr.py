#!/usr/bin/env python3

from __future__ import annotations

import argparse
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
)

NON_RELEASE_PATH_PREFIXES = (
    "docs/",
)

NON_RELEASE_FILES = {
    "README.md",
    "CHANGELOG.md",
    "LICENSE",
}

RELEASE_METADATA_FILES = {
    "CHANGELOG.md",
    "README.md",
    "charts/victoria-lakehouse/Chart.yaml",
}


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def extract_unreleased_section(text: str) -> str:
    match = re.search(
        r"^## \[Unreleased\]\s*\n(?P<body>.*?)(?=^## \[|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if not match:
        return ""
    return match.group("body").strip()


def extract_bullet_points(text: str) -> set[str]:
    return {line.strip() for line in text.splitlines() if line.strip().startswith("- ")}


def has_genuinely_new_unreleased_entries(head_unreleased: str, base_full_changelog: str) -> bool:
    base_all_bullets = extract_bullet_points(base_full_changelog)
    head_new_bullets = extract_bullet_points(head_unreleased) - base_all_bullets
    return bool(head_new_bullets)


def versioned_bullets(text: str) -> set[str]:
    """Bullet points that live under a released ``## [x.y.z]`` section.

    Excludes the ``## [Unreleased]`` section so the two documentation paths
    (Unreleased vs. a materialized/backfilled version section) stay distinct.
    """
    bullets: set[str] = set()
    in_versioned = False
    for line in text.splitlines():
        if line.startswith("## ["):
            in_versioned = not line.startswith("## [Unreleased]")
            continue
        if in_versioned and line.strip().startswith("- "):
            bullets.add(line.strip())
    return bullets


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
    if is_unit_test_only_path(path):
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


def is_release_metadata_sync(files: Iterable[str]) -> bool:
    file_list = [f for f in files if f.strip()]
    if not file_list or "CHANGELOG.md" not in file_list:
        return False
    return all(path in RELEASE_METADATA_FILES for path in file_list)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    args = parser.parse_args()

    files = run_git("diff", "--name-only", f"{args.base}..{args.head}").splitlines()
    commits = run_git("log", "--pretty=format:%s", f"{args.base}..{args.head}").splitlines()

    base_text = run_git("show", f"{args.base}:CHANGELOG.md")
    head_text = run_git("show", f"{args.head}:CHANGELOG.md")
    base_unreleased = extract_unreleased_section(base_text)
    head_unreleased = extract_unreleased_section(head_text)

    if is_dependency_only_pr(commits, files):
        print("changelog gate: skipped (dependency-only update)")
        return 0

    if is_release_metadata_sync(files):
        if head_unreleased.strip() == base_unreleased.strip():
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
