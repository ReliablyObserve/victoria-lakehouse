#!/usr/bin/env python3
"""Turn the changelog fragments of a release commit into the release's CHANGELOG.md section.

    changelog_fragments.py check
    changelog_fragments.py previous-tag --rev HEAD^
    changelog_fragments.py assemble --version 0.168.0 [--date 2026-10-23] [--after-tag v0.167.9]
                                    [--consumed-out PATH]

A change is documented by one file, changelog.d/<PR-or-slug>.<kind>.md, that holds the bullet
(see changelog.d/README.md and check_changelog_pr.py, which owns the format and the PR gate).

`assemble` runs in the release job on the release commit:

* The fragments it releases are the ones present in changelog.d/, minus the files (same path, same
  bytes) already present at --after-tag (the previous release): those were released by it even when its
  release-metadata pull request has not merged yet, so a fragment is released exactly once. A fragment merged after the
  release commit is not in this checkout and lands in the next release.
* `## [Unreleased]` is kept, empty, and the new `## [VERSION] - DATE` section follows it, built from the
  fragments only, grouped under the Keep a Changelog headings (Added, Changed, Deprecated, Removed,
  Fixed, Security), each group in file-name order. Text under `[Unreleased]` stops the release (it
  would be filed twice or not at all), as does a fragment that repeats the lead-in of a released
  section, a fragment a released section already describes that is still in the directory (left
  behind), and a section too large for a GitHub release body.
* The released fragments are removed from the working tree, so the tree is the one the release
  metadata pull request leaves behind, and their paths are written to --consumed-out, one per line,
  for the metadata job, which removes them from git.

Anything it cannot place (an invalid fragment, text under [Unreleased], a version that is already a
section) is an error; nothing is dropped and nothing is changed.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import os
import pathlib
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # also under `python3 -I`
import check_changelog_pr as cc  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]
SEMVER_RE = re.compile(r"^(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$")
KNOWN_HEADINGS = tuple(k.capitalize() for k in cc.FRAGMENT_KINDS)
# The release notes are the section plus the artifacts block, in a body GitHub limits to about 125000 characters.
SECTION_MAX_BYTES = 100_000


class AssembleError(Exception):
    pass


def load_fragments(directory: pathlib.Path, rel: str = cc.FRAGMENT_DIR) -> tuple[list[cc.Fragment], list[str]]:
    """Every fragment in `directory` (published as `rel/<name>`), and every reason one cannot be used."""
    fragments: list[cc.Fragment] = []
    problems: list[str] = []
    if not directory.is_dir():
        return fragments, problems
    for entry in sorted(directory.iterdir(), key=lambda e: e.name):
        path = f"{rel}/{entry.name}"
        if entry.name == cc.FRAGMENT_README and entry.is_file():
            continue
        if entry.is_symlink() or not entry.is_file():
            problems.append(f"{path}: not a regular file")
            continue
        fr, why = cc.parse_fragment(path, entry.read_text(encoding="utf-8"))
        problems.extend(why)
        if fr is not None:
            fragments.append(fr)
    problems.extend(cc.duplicate_fragment_problems(fragments))
    return fragments, problems


def blobs_at_tag(tag: str, rel: str = cc.FRAGMENT_DIR, cwd: pathlib.Path | None = None) -> dict[str, str]:
    """path -> git blob id of every file under `rel` at git tag `tag`; empty when the tag cannot be read
    (then every fragment is released, which can repeat the previous release's entries but never loses one)."""
    if not tag:
        return {}
    try:
        out = subprocess.run(
            ["git", "ls-tree", "-r", "-z", tag, "--", rel],
            cwd=cwd, capture_output=True, check=True,
        ).stdout.decode("utf-8", "surrogateescape")
    except (OSError, subprocess.CalledProcessError):
        return {}
    blobs = {}
    for entry in out.split("\0"):
        meta, _, path = entry.partition("\t")
        if path:
            blobs[path] = meta.split()[2]
    return blobs


def blob_id(path: pathlib.Path) -> str:
    """The git blob id of the file's bytes, as `git hash-object` computes it."""
    data = path.read_bytes()
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def split_unreleased(text: str) -> tuple[str, str, str]:
    """(everything up to and including the [Unreleased] heading line, its body, the rest)."""
    lines = text.split("\n")
    start = next((i for i, l in enumerate(lines) if l.startswith("## [Unreleased]")), None)
    if start is None:
        raise AssembleError("no '## [Unreleased]' section in the changelog")
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## [")), len(lines))
    return "\n".join(lines[: start + 1]), "\n".join(lines[start + 1 : end]), "\n".join(lines[end:])


def section_body(fragments: list[cc.Fragment]) -> str:
    groups: dict[str, list[str]] = {}
    for fr in sorted(fragments, key=lambda f: f.path):
        groups.setdefault(fr.kind.capitalize(), []).append("\n".join(fr.lines))
    return "\n\n".join(f"### {h}\n\n" + "\n\n".join(groups[h]) for h in KNOWN_HEADINGS if h in groups)


def assemble(text: str, fragments: list[cc.Fragment], version: str, date: str) -> str:
    """The changelog with `[Unreleased]` emptied and `## [version] - date` below it."""
    if not SEMVER_RE.match(version):
        raise AssembleError(f"'{version}' is not a release version (MAJOR.MINOR.PATCH)")
    try:
        datetime.date.fromisoformat(date)
    except ValueError as err:
        raise AssembleError(f"date '{date}' is not YYYY-MM-DD") from err
    if re.search(r"^## \[" + re.escape(version) + r"\]", text, re.MULTILINE):
        raise AssembleError(f"the changelog already has a section for {version}")
    head, body, rest = split_unreleased(text)
    if body.strip():
        raise AssembleError(
            "text under '## [Unreleased]' in CHANGELOG.md: changes are documented by fragments in changelog.d/ and "
            "[Unreleased] stays empty. Convert each bullet into changelog.d/<PR-or-slug>.<kind>.md "
            "(or delete it if a release already took it)"
        )
    section = f"## [{version}] - {date}\n\n"
    content = section_body(fragments)
    if len(content.encode("utf-8")) > SECTION_MAX_BYTES:
        sizes = ", ".join(
            f"{fr.path} ({len(chr(10).join(fr.lines).encode('utf-8'))} bytes)"
            for fr in sorted(fragments, key=lambda f: -len("\n".join(f.lines).encode("utf-8")))
        )
        raise AssembleError(
            f"the section would be {len(content.encode('utf-8'))} bytes; a GitHub release body holds about 125000 "
            f"characters, so a section is at most {SECTION_MAX_BYTES} bytes. Shorten the fragments, largest first: {sizes}"
        )
    if content:
        section += content + "\n\n"
    return head + "\n\n" + section + rest


def run(
    root: pathlib.Path, version: str, date: str, after_tag: str, consumed_out: pathlib.Path | None, dry_run: bool = False
) -> list[str]:
    fragments, problems = load_fragments(root / cc.FRAGMENT_DIR)
    if problems:
        raise AssembleError("cannot release these fragments:\n  " + "\n  ".join(problems))
    # A fragment is the previous release's only when the tag holds that very file: a later fragment that
    # reuses the name of a released (and since deleted) one has other bytes and is released.
    already = blobs_at_tag(after_tag, cwd=root)
    chosen = [fr for fr in fragments if already.get(fr.path) != blob_id(root / fr.path)]
    changelog = root / "CHANGELOG.md"
    text = changelog.read_text(encoding="utf-8")
    # A fragment the previous release took is skipped, because that release's metadata pull request
    # removes it. If the CHANGELOG.md already has that release's section, the pull request merged and the
    # file was left behind: it would be skipped for ever, so it stops the release instead.
    left: list[str] = []
    in_section = cc.released_lead_in_versions(text)
    for fr in fragments:
        if fr not in chosen:
            first = cc.first_release_tag_with_blob(after_tag, fr.path, already[fr.path], cwd=root)
            if first and re.search(r"^## \[" + re.escape(first[1:]) + r"\]", text, re.MULTILINE):
                holds = first[1:] in in_section.get(fr.lead_key, set())
                left.append(
                    f"{fr.path} (released in {first}; "
                    + ("its bullet is in the section: delete the file" if holds
                       else f"its bullet is NOT in the section: file it under [{first[1:]}] and delete the file")
                    + ")"
                )
    if left:
        raise AssembleError(
            "these fragments were released by a release whose section is already in CHANGELOG.md, but the files are "
            "still in changelog.d/. Clear each in a pull request with a docs: or chore: subject (a delete-only pull "
            "request is refused when the bullet is not in the section): " + "; ".join(left)
        )
    # A change that a released section already describes is released once: a fragment that repeats
    # its lead-in (a re-added copy, a rebase onto a release that took the change) stops the release
    # instead of publishing the change twice. The gate rejects such a PR; this is the backstop.
    released = cc.released_lead_ins(text)
    twice = [fr.path for fr in chosen if fr.lead_key in released]
    if twice:
        raise AssembleError(
            "these fragments repeat the bold lead-in of a released CHANGELOG.md section; remove them "
            "(the change is already released): " + ", ".join(twice)
        )
    result = assemble(text, chosen, version, date)
    if dry_run:
        return [fr.path for fr in chosen]
    changelog.write_text(result, encoding="utf-8")
    for fr in chosen:
        (root / fr.path).unlink()
    consumed = [fr.path for fr in chosen]
    if consumed_out is not None:
        consumed_out.write_text("".join(p + "\n" for p in consumed), encoding="utf-8")
    return consumed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", default=str(ROOT), help="repository root (default: this checkout)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="validate every fragment in changelog.d/")
    prev = sub.add_parser("previous-tag", help="print the newest vMAJOR.MINOR.PATCH tag reachable from --rev")
    prev.add_argument("--rev", default="HEAD^")
    asm = sub.add_parser("assemble", help="build the release section from the fragments and remove them")
    asm.add_argument("--version", required=True, help="MAJOR.MINOR.PATCH (no leading v)")
    asm.add_argument("--date", help="YYYY-MM-DD (default: today, UTC)")
    asm.add_argument("--after-tag", default="", help="previous release tag: its fragments are not released again")
    asm.add_argument("--consumed-out", help="file that receives the released fragment paths, one per line")
    asm.add_argument("--dry-run", action="store_true", help="run every check and change nothing")
    args = parser.parse_args(argv)
    root = pathlib.Path(args.root)
    try:
        if args.cmd == "check":
            fragments, problems = load_fragments(root / cc.FRAGMENT_DIR)
            if problems:
                raise AssembleError("\n  ".join(problems))
            print(f"{len(fragments)} changelog fragment(s), all valid")
            return 0
        if args.cmd == "previous-tag":
            print(cc.latest_release_tag(args.rev, cwd=root))
            return 0
        date = args.date or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        out = pathlib.Path(args.consumed_out) if args.consumed_out else None
        consumed = run(root, args.version, date, args.after_tag, out, args.dry_run)
        print(f"{'checked' if args.dry_run else 'assembled'} [{args.version}] - {date} from {len(consumed)} fragment(s)")
    except (AssembleError, OSError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
